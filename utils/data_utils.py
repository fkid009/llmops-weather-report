import os, json, time, logging, requests, yaml, psycopg2
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Dict

from utils.path import CITIES_YAML, RAW_DIR




def load_weather_data(
    cities_yaml: str = str(CITIES_YAML),
    city: Optional[str] = None,
    lang: str = "en",
    units: str = "metric",
    include_forecast: bool = True,
    save_raw_dir: str = str(RAW_DIR),
    **kwargs,
) -> Dict[str, int]:
    """
    OpenWeather에서 현재날씨(+옵션: 예보) 수집 → 날짜 폴더에 raw JSON 저장만 수행.
    Returns: {"cities_processed": n, "current_saved": n, "forecast_saved": m}
    """
    log = logging.getLogger("airflow.task")
    api_key = os.getenv("OPENWEATHER_API_KEY")
    if not api_key:
        raise RuntimeError("OPENWEATHER_API_KEY not set")

    tmp_output_dir = Path()
    

PG_DSN   = "dbname=postgres user=postgres password=password1234 host=db port=5432"

def choose_initial_or_daily():
    """
    weather_raw 테이블이 없거나, 행이 0개면 'initial_load'로, 있으면 'daily_ingest'로 분기.
    """
    conn = psycopg2.connect(PG_DSN)
    cur = conn.cursor()
    cur.execute("""
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema='public' AND table_name='weather_raw'
        );
    """)
    has_table = cur.fetchone()[0]
    is_empty = True
    if has_table:
        cur.execute("SELECT EXISTS (SELECT 1 FROM weather_raw);")
        has_rows = cur.fetchone()[0]
        is_empty = not has_rows
    cur.close(); conn.close()
    return "initial_load" if is_empty else "daily_ingest"


def _prepare_init_files():
    """
    data/init 내 JSON 파일을 save_raw_to_db가 읽을 수 있도록 *_forecast.json으로 변환.
    (current는 고려하지 않음)
    """
    p = Path(INIT_DIR)
    p.mkdir(parents=True, exist_ok=True)
    for f in p.glob("*.json"):
        lower = f.name.lower()
        if lower.endswith("_forecast.json"):
            continue  # 이미 OK
        # *_init.json 같은 건 forecast.json으로 변환
        stem = f.stem.replace("_init", "")
        new = f.with_name(stem + "_forecast.json")
        if new.exists():
            new.unlink()
        f.rename(new)