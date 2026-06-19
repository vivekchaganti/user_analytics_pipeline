import pandas as pd
from sqlalchemy import create_engine

from api_client import parquet_file_path

# Database connection details - these would typically be in .env or Airflow connections
DB_USER = "airflow"
DB_PASS = "airflow"
DB_HOST = "postgres"  # Use 'localhost' if running outside Docker
DB_PORT = "5432"
DB_NAME = "user_analytics_dw"


def load_parquet_to_staging(execution_date=None, **context):
    """Loads the Parquet file into the Postgres staging table."""
    if execution_date is None and context:
        execution_date = context.get("ds")

    if not execution_date:
        raise ValueError("execution_date is required to locate the parquet file")

    input_file = parquet_file_path(execution_date)
    print(f"Reading data from {input_file}...")
    try:
        df = pd.read_parquet(input_file)
    except Exception as e:
        print(f"Error reading Parquet file: {e}")
        raise

    print(f"Connecting to Postgres database {DB_NAME}...")
    engine = create_engine(
        f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )

    try:
        # Load into staging_users.
        # We use 'replace' for staging as it's a truncate-and-load pattern.
        print("Loading data into staging_users table...")
        df.to_sql(
            name="staging_users",
            con=engine,
            if_exists="replace",
            index=False,
            method="multi",
            chunksize=1000,
        )
        print("Successfully loaded data into staging!")
    except Exception as e:
        print(f"Error loading data to Postgres: {e}")
        raise
    finally:
        engine.dispose()


if __name__ == "__main__":
    load_parquet_to_staging()
