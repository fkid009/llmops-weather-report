import os, json, time, logging, requests, yaml
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
    