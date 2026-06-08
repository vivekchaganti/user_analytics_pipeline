import os
import pandas as pd
from sqlalchemy import create_engine

# Database connection details - these would typically be in .env or Airflow connections
DB_USER = "airflow"
DB_PASS = "airflow"
DB_HOST = "postgres"  # Use 'localhost' if running outside Docker
DB_PORT = "5432"
DB_NAME = "user_analytics_dw"

# Paths
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
INPUT_FILE = os.path.join(DATA_DIR, "users_raw.parquet")


def load_parquet_to_staging():
    """Loads the Parquet file into the Postgres staging table."""
    print(f"Reading data from {INPUT_FILE}...")
    try:
        df = pd.read_parquet(INPUT_FILE)
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
