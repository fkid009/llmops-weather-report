import sys
sys.path.append("/opt/project")

from datetime import datetime, timedelta

from utils.data_utils import (
        load_weather_data, 
        choose_initial_or_daily, 
        prepare_init_files, 
        save_raw_to_db_from_csv,
        save_today_csv_to_db,
        build_today_csv_from_tmp
)
from utils.path import TMP_DIR

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule


with DAG(
        "data_pipeline",
        description = "A data pipeline dag including data loading, preprocessing and saving",
        schedule_interval = "0 6 * * *", # 매일 06:00
        start_date = datetime(2025, 8, 31),
        catchup = False
    ) as dag:

        branch = BranchPythonOperator(
            task_id="branch_initial_or_daily",
            python_callable = choose_initial_or_daily,
        )

        prepare_init_csv = PythonOperator(
            task_id="prepare_init_files",
            python_callable=prepare_init_files,
        )
        
        save_to_db = PythonOperator(
            task_id="save_raw_to_db",
            python_callable=lambda **ctx: save_raw_to_db_from_csv(
                csv_path=ctx["ti"].xcom_pull(task_ids="prepare_init_files"),
                postgres_conn_id="postgres",
                source="bootstrap",
                kind="forecast",
            ),
        )

        skip_init = EmptyOperator(task_id="skip_init")

        join_init_or_skip = EmptyOperator(
            task_id="join_init_or_skip",
            trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        )
        
        daily_ingest = PythonOperator(
            task_id="daily_ingest",
            python_callable=load_weather_data,
            op_kwargs={
                "cities_yaml": "Seoul",
                "save_tmp_dir": TMP_DIR,
                "archive": False,           # 공간 아끼기 (원하면 True로 병행 아카이브)
                "lang": "en",
                "units": "metric",
                "include_forecast": True,
            },
        )

        build_daily_csv = PythonOperator(
            task_id="build_daily_csv",
            python_callable=build_today_csv_from_tmp,
            op_kwargs={"tmp_dir": TMP_DIR},
        )
        
        save_today_to_db = PythonOperator(
            task_id="save_today_to_db",
            python_callable=lambda **ctx: save_today_csv_to_db(
                csv_path=ctx["ti"].xcom_pull(task_ids="build_daily_csv"),  # ← CSV 만든 태스크 ID로 교체
                postgres_conn_id="postgres",
            ),
        )


branch >> [prepare_init_csv, skip_init]
prepare_init_csv  >> save_to_db >> join_init_or_skip
skip_init >> join_init_or_skip
join_init_or_skip >> daily_ingest >> build_daily_csv >> save_today_to_db