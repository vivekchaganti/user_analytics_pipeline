import glob
import os
import sys
import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.trigger_rule import TriggerRule

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from api_client import process_and_save_parquet  # noqa: E402
from db_loader import load_parquet_to_staging  # noqa: E402

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")

TRANSFORM_SQL = """
INSERT INTO dim_users
(user_uuid, username, title, first_name, last_name, gender, email, phone,
 cell, nationality)
SELECT u.uuid, u.username, u.title, u.first_name, u.last_name, u.gender,
       u.email, u.phone, u.cell, u.nationality
FROM staging_users u
ON CONFLICT (user_uuid) DO UPDATE SET
    title = EXCLUDED.title,
    first_name = EXCLUDED.first_name,
    last_name = EXCLUDED.last_name,
    gender = EXCLUDED.gender,
    email = EXCLUDED.email,
    phone = EXCLUDED.phone,
    cell = EXCLUDED.cell,
    nationality = EXCLUDED.nationality,
    updated_at = CURRENT_TIMESTAMP;

INSERT INTO dim_locations
(street_number, street_name, city, state, country, postcode, latitude,
 longitude, timezone_offset, timezone_desc)
SELECT u.street_number, u.street_name, u.city, u.state, u.country,
       u.postcode, u.latitude::NUMERIC, u.longitude::NUMERIC,
       u.timezone_offset, u.timezone_desc
FROM staging_users u
ON CONFLICT (street_number, street_name, postcode) DO NOTHING;

INSERT INTO dim_dates
(date_key, full_date, year, month, day, quarter, day_of_week, is_weekend)
SELECT
    (EXTRACT(EPOCH FROM d)::INT) AS date_key,
    d AS full_date,
    EXTRACT(YEAR FROM d)::INT,
    EXTRACT(MONTH FROM d)::INT,
    EXTRACT(DAY FROM d)::INT,
    EXTRACT(QUARTER FROM d)::INT,
    TO_CHAR(d, 'Day') AS day_of_week,
    (EXTRACT(ISODOW FROM d) IN (6, 7))::BOOLEAN AS is_weekend
FROM generate_series(
    '2006-01-01'::date, '2026-12-31'::date, interval '1 day'
) AS d
ON CONFLICT (full_date) DO NOTHING;

INSERT INTO fact_user_registrations
(user_uuid, location_key, dob_date_key, reg_date_key, age_at_registration,
 current_age, registered_years)
SELECT u.uuid, l.location_key, d1.date_key, d2.date_key,
       (EXTRACT(YEAR FROM AGE(d2.full_date, d1.full_date)))::INT,
       (EXTRACT(YEAR FROM AGE(NOW(), d1.full_date)))::INT,
       (EXTRACT(YEAR FROM AGE(d2.full_date, d1.full_date)))::INT
FROM staging_users u
JOIN dim_locations l ON l.street_number = u.street_number
    AND l.street_name = u.street_name AND l.postcode = u.postcode
JOIN dim_dates d1 ON d1.full_date = u.dob_date::DATE
JOIN dim_dates d2 ON d2.full_date = u.registered_date::DATE
ON CONFLICT (user_uuid) DO NOTHING;
"""


def _cleanup_old_parquet_files(days_to_keep=3, **context):
    if not os.path.isdir(DATA_DIR):
        return
    cutoff = time.time() - (days_to_keep * 86400)
    removed = 0
    for fp in glob.glob(os.path.join(DATA_DIR, "*.parquet")):
        if os.path.getmtime(fp) < cutoff:
            try:
                os.remove(fp)
                removed += 1
            except OSError:
                pass
    print(f"Cleanup: removed {removed} files older than {days_to_keep} days")


default_args = {
    "owner": "vivekchaganti",
    "start_date": datetime(2026, 6, 8),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "user_analytics_etl_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
    catchup=False,
    tags=["user_analytics", "etl", "data_warehouse"],
    description="ETL pipeline: API -> Parquet -> Staging -> Star Schema",
) as dag:

    extract_api_task = PythonOperator(
        task_id="extract_api_to_parquet",
        python_callable=process_and_save_parquet,
        provide_context=True,
    )

    load_staging_task = PythonOperator(
        task_id="load_parquet_to_staging",
        python_callable=load_parquet_to_staging,
    )

    transform_task = SQLExecuteQueryOperator(
        task_id="transform_staging_to_warehouse",
        conn_id="user_analytics_postgres_conn",
        sql=TRANSFORM_SQL,
    )

    cleanup_task = PythonOperator(
        task_id="cleanup_old_parquet_files",
        python_callable=_cleanup_old_parquet_files,
        op_kwargs={"days_to_keep": 3},
        trigger_rule=TriggerRule.ALL_DONE,
    )

    extract_api_task >> load_staging_task >> transform_task >> cleanup_task
