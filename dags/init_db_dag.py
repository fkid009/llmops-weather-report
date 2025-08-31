from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

DEFAULT_ARGS = {
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# Airflow Connections에서 conn_id="postgres" 로 설정해둔 Postgres를 사용
CONN_ID = "postgres"

DDL_SQL = """
CREATE TABLE IF NOT EXISTS cities (
  city_id SERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  country TEXT,
  lat DOUBLE PRECISION,
  lon DOUBLE PRECISION,
  UNIQUE (name, country)
);

CREATE TABLE IF NOT EXISTS weather_raw (
  id BIGSERIAL PRIMARY KEY,
  city_id INT REFERENCES cities(city_id),
  ts TIMESTAMPTZ NOT NULL,
  source TEXT DEFAULT 'openweather',
  kind TEXT NOT NULL, -- 'current' or 'forecast'
  payload JSONB NOT NULL,
  UNIQUE (city_id, ts, source, kind)
);
"""

with DAG(
    dag_id="init_db_dag",
    description="Initialize cities & weather_raw tables",
    start_date=datetime(2025, 8, 31),
    schedule_interval=None,   # 필요 시 수동 실행
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["init", "db"],
) as dag:

    create_tables = SQLExecuteQueryOperator(
        task_id="create_tables",
        conn_id=CONN_ID,
        sql=DDL_SQL,
    )