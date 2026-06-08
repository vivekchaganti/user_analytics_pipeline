import os
from sqlalchemy import create_engine

DB_USER = 'airflow'
DB_PASS = 'airflow'
DB_HOST = 'postgres'
DB_PORT = '5432'
DB_NAME = 'user_analytics_dw'

def run_sql_file(filepath):
    if not os.path.exists(filepath):
        raise FileNotFoundError(f'SQL file not found: {filepath}')
    with open(filepath, 'r') as f:
        sql_content = f.read()
    engine = create_engine(f'postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
    with engine.begin() as conn:
        conn.exec_driver_sql(sql_content)
    engine.dispose()
    return f'Executed: {filepath}'
