from datetime import datetime, timedelta

from utils.data_utils import load_weather_data, choose_initial_or_daily
# from utils.path import 

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

        prepare_init_files = PythonOperator(
            task_id="prepare_init_files",
            python_callable=_prepare_init_files,
        )
        
        initial_load = PythonOperator(
            task_id="initial_load",
            python_callable=save_raw_to_db,
            op_kwargs={
                "date_dir": INIT_DIR,   # data/init 안의 *_forecast.json / *_current.json 사용
                "dsn": PG_DSN,
            },
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
                "cities_yaml": str(path.CITIES_YAML),
                "save_tmp_dir": TMP_DIR,
                "archive": False,           # 공간 아끼기 (원하면 True로 병행 아카이브)
                "lang": "en",
                "units": "metric",
                "include_forecast": True,
            },
        )
        
        save_today_to_db = PythonOperator(
            task_id="save_raw_to_db",
            python_callable=save_raw_to_db,
            op_kwargs={
                "raw_root": TMP_DIR,        # tmp에 저장된 *_current/_forecast.json 읽어 DB로
                "dsn": PG_DSN,
            },
        )


branch >> [prepare_init_files, skip_init]
prepare_init_files >> initial_load >> join_init_or_skip
skip_init >> join_init_or_skip
join_init_or_skip >> daily_ingest >> save_today_to_db