# User Analytics ETL Pipeline

This repository contains an Airflow‑based ETL pipeline that extracts user data from an external API, stores it as Parquet files, loads it into staging tables in PostgreSQL, and transforms the data into a star schema (dimension and fact tables).

## Quick Start

1. **Clone the repo**  
   ```bash
   git clone https://github.com/vivekchaganti/user_analytics_pipeline.git
   cd user_analytics_pipeline
   ```

2. **Start services**  
   ```bash
   docker-compose down && docker-compose up -d
   ```

3. **Trigger the DAG** (once Airflow UI is available at `http://localhost:8080`):  
   ```bash
   docker exec user_analytics_airflow_webserver airflow dags trigger user_analytics_etl_pipeline
   ```

## Architecture

- **PostgreSQL** – stores the dimensional tables (`dim_users`, `dim_locations`, `dim_dates`) and the fact table (`fact_user_registrations`).
- **Airflow** – orchestrates the ETL workflow:
  - `extract_api_to_parquet` – pulls data from the API and writes Parquet files.
  - `load_parquet_to_staging` – loads Parquet files into staging tables.
  - `transform_staging_to_warehouse` – runs SQL to populate the star schema.
  - `cleanup_old_parquet_files` – removes Parquet files older than 3 days.
- **Python modules** (`src/`) – contain the API client and DB loader logic.

## Schema Overview

| Table                     | Primary Key | Important Columns |
|---------------------------|-------------|-------------------|
| `dim_users`               | `user_uuid` | `username`, `title`, `first_name`, `last_name`, `gender`, `email`, `phone`, `cell`, `nationality` |
| `dim_locations`           | composite (`street_number`, `street_name`, `postcode`) | `city`, `state`, `country`, `latitude`, `longitude`, `timezone_offset`, `timezone_desc` |
| `dim_dates`               | `date_key`  | `full_date`, `year`, `month`, `day`, `quarter`, `day_of_week`, `is_weekend` |
| `fact_user_registrations` | `user_uuid` | `location_key`, `dob_date_key`, `reg_date_key`, `age_at_registration`, `current_age`, `registered_years` |

## CI/CD

The GitHub Actions workflow runs:

- **Black** – code formatting check.
- **Flake8** – linting.
- **Pytest** – unit tests (see `tests/test_dag.py`).

All checks must pass before merging.

## Troubleshooting

- **Worker timeouts** – ensure sufficient resources for the Airflow scheduler.
- **Unique constraint errors** – the `username` unique constraint was removed to allow duplicate usernames.
- **Date generation errors** – fixed by using `generate_series` with proper type casts.

## License

MIT License

--- 

*Repository URL:* https://github.com/vivekchaganti/user_analytics_pipeline