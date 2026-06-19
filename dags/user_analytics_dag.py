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
from api_client import PARQUET_FILE_GLOB, process_and_save_parquet  # noqa: E402
from db_loader import load_parquet_to_staging  # noqa: E402
from dq_check_operator import DQSqlCheckOperator  # noqa: E402
from dq_checks_sql import STAGING_DQ_CHECKS  # noqa: E402
from warehouse_sql import (  # noqa: E402
    ENSURE_SCHEMA_SQL,
    UPSERT_DIM_DATES_SQL,
    UPSERT_DIM_LOCATIONS_SQL,
    UPSERT_DIM_USERS_SQL,
    UPSERT_FACT_USER_REGISTRATIONS_SQL,
)

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
POSTGRES_CONN_ID = "user_analytics_postgres_conn"


def _cleanup_old_parquet_files(days_to_keep=3, **context):
    if not os.path.isdir(DATA_DIR):
        return
    cutoff = time.time() - (days_to_keep * 86400)
    removed = 0
    for fp in glob.glob(os.path.join(DATA_DIR, PARQUET_FILE_GLOB)):
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
    description="ETL pipeline: API -> Parquet -> Staging -> DQ -> Star Schema",
) as dag:

    extract_api_task = PythonOperator(
        task_id="extract_api_to_parquet",
        python_callable=process_and_save_parquet,
        provide_context=True,
    )

    load_staging_task = PythonOperator(
        task_id="load_parquet_to_staging",
        python_callable=load_parquet_to_staging,
        provide_context=True,
    )

    dq_check_tasks = [
        DQSqlCheckOperator(
            task_id=task_id,
            conn_id=POSTGRES_CONN_ID,
            sql=sql,
        )
        for task_id, sql in STAGING_DQ_CHECKS.items()
    ]

    ensure_schema_task = SQLExecuteQueryOperator(
        task_id="ensure_warehouse_schema",
        conn_id=POSTGRES_CONN_ID,
        sql=ENSURE_SCHEMA_SQL,
        split_statements=True,
    )

    update_dim_users_task = SQLExecuteQueryOperator(
        task_id="update_dim_users",
        conn_id=POSTGRES_CONN_ID,
        sql=UPSERT_DIM_USERS_SQL,
    )

    update_dim_locations_task = SQLExecuteQueryOperator(
        task_id="update_dim_locations",
        conn_id=POSTGRES_CONN_ID,
        sql=UPSERT_DIM_LOCATIONS_SQL,
    )

    update_dim_dates_task = SQLExecuteQueryOperator(
        task_id="update_dim_dates",
        conn_id=POSTGRES_CONN_ID,
        sql=UPSERT_DIM_DATES_SQL,
    )

    update_fact_task = SQLExecuteQueryOperator(
        task_id="update_fact_user_registrations",
        conn_id=POSTGRES_CONN_ID,
        sql=UPSERT_FACT_USER_REGISTRATIONS_SQL,
    )

    cleanup_task = PythonOperator(
        task_id="cleanup_old_parquet_files",
        python_callable=_cleanup_old_parquet_files,
        op_kwargs={"days_to_keep": 3},
        trigger_rule=TriggerRule.ALL_DONE,
    )

    extract_api_task >> load_staging_task >> dq_check_tasks >> ensure_schema_task
    ensure_schema_task >> [
        update_dim_users_task,
        update_dim_locations_task,
        update_dim_dates_task,
    ]
    (
        [
            update_dim_users_task,
            update_dim_locations_task,
            update_dim_dates_task,
        ]
        >> update_fact_task
        >> cleanup_task
    )
