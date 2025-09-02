import os, json, time, logging, requests, yaml, psycopg2, csv
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Dict

from utils.path import CITIES_YAML, RAW_DIR, INIT_DATA_DIR, INIT_TMP_DIR, TMP_DIR, UTILS_DIR
from utils.utils import load_yaml

from airflow.providers.postgres.hooks.postgres import PostgresHook




def load_weather_data(
    cities_yaml: str,
    save_tmp_dir: str,
    archive: bool = False,
    lang: str = "en",
    units: str = "metric",
    include_forecast: bool = True,
) -> dict:
    """
    OpenWeather에서 현재날씨(+옵션: 예보) 수집 → 날짜 폴더에 raw JSON 저장만 수행.
    Returns: {"cities_processed": n, "current_saved": n, "forecast_saved": m}
    """

    cities = load_yaml(UTILS_DIR / "cities.yaml")
    log = logging.getLogger("airflow.task")
    api_key = os.getenv("OPENWEATHER_API_KEY")
    if not api_key:
        raise RuntimeError("OPENWEATHER_API_KEY not set")

    base = Path(save_tmp_dir)
    if archive:
        datedir = base / datetime.utcnow().strftime("%Y%m%d")
    else:
        datedir = base

    saved = []

    for city in cities:
        name = city["name"]
        lat = city["lat"]
        lon = city["lon"]

        # --- current 날씨 ---
        url_current = (
            f"https://api.openweathermap.org/data/2.5/weather?"
            f"lat={lat}&lon={lon}&appid={api_key}&units={units}&lang={lang}"
        )
        resp = requests.get(url_current, timeout=30)
        resp.raise_for_status()
        current_data = resp.json()

        fcur = datedir / f"{name}_current.json"
        fcur.write_text(json.dumps(current_data, ensure_ascii=False), encoding="utf-8")
        saved.append(str(fcur))

        # --- forecast 날씨 ---
        if include_forecast:
            url_forecast = (
                f"https://api.openweathermap.org/data/2.5/forecast?"
                f"lat={lat}&lon={lon}&appid={api_key}&units={units}&lang={lang}"
            )
            resp = requests.get(url_forecast, timeout=30)
            resp.raise_for_status()
            forecast_data = resp.json()

            ffor = datedir / f"{name}_forecast.json"
            ffor.write_text(json.dumps(forecast_data, ensure_ascii=False), encoding="utf-8")
            saved.append(str(ffor))

    return {"saved_files": saved}

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
    return "prepare_init_files" if is_empty else "skip_init"


def prepare_init_files():
    """
    data/init/*_init.json -> (표 형태로 변환) -> data/tmp/init/*.csv 저장
    - 각 *_init.json 은 forecast-like 구조라고 가정:
      {"city": {...}, "list": [ { "dt": ..., "main": {...}, "wind": {...}, "weather": [...] }, ... ]}
    - 생성한 CSV 경로를 리턴 (Airflow에선 XCom으로 자동 저장됨)
    """
    base = INIT_DATA_DIR
    out = INIT_TMP_DIR

    fname = "init_db.csv"
    csv_path = out / fname

    header = [
        "name", "country", "lat", "lon",
        "ts", "temp", "feels_like", "humidity", "wind_speed", "description"
    ]

    with csv_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)

        for p in sorted(base.glob("*_init.json")):
            obj = json.loads(p.read_text(encoding="utf-8"))

            city = (obj or {}).get("city") or {}
            lst = (obj or {}).get("list") or []
            if not city or not lst:
                continue

            name = city.get("name")
            country = city.get("country")
            coord = city.get("coord") or {}
            lat, lon = coord.get("lat"), coord.get("lon")

            for item in lst:
                # 시각
                ts = item.get("dt")
                ts_iso = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat() if isinstance(ts, (int, float)) else None

                main = item.get("main") or {}
                wind = item.get("wind") or {}
                weather0 = (item.get("weather") or [{}])[0] or {}

                row = [
                    name,
                    country,
                    lat,
                    lon,
                    ts_iso,
                    main.get("temp"),
                    main.get("feels_like"),
                    main.get("humidity"),
                    wind.get("speed"),
                    weather0.get("description"),
                ]
                w.writerow(row)

    return str(csv_path)


def save_raw_to_db_from_csv(
    csv_path: str,
    postgres_conn_id: str = "postgres",
    source: str = "bootstrap",
    kind: str = "forecast",
) -> None:
    """
    CSV -> TEMP stage -> cities UPSERT -> weather_raw UPSERT (모두 한 커넥션에서)
    """
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    conn = hook.get_conn()
    try:
        with conn.cursor() as cur:
            # 1) TEMP stage (ON COMMIT DROP이므로 커밋 시 자동 삭제)
            cur.execute("""
                CREATE TEMP TABLE weather_init_stage (
                  name        TEXT,
                  country     TEXT,
                  lat         DOUBLE PRECISION,
                  lon         DOUBLE PRECISION,
                  ts          TIMESTAMPTZ,
                  temp        DOUBLE PRECISION,
                  feels_like  DOUBLE PRECISION,
                  humidity    INT,
                  wind_speed  DOUBLE PRECISION,
                  description TEXT
                ) ON COMMIT DROP;
            """)

            # 2) COPY (같은 커넥션의 cursor로 실행)
            with open(csv_path, "r", encoding="utf-8") as f:
                cur.copy_expert("""
                    COPY weather_init_stage(name, country, lat, lon, ts, temp, feels_like, humidity, wind_speed, description)
                    FROM STDIN WITH CSV HEADER DELIMITER ',';
                """, f)

            # 3) cities UPSERT
            cur.execute("""
                INSERT INTO cities (name, country, lat, lon)
                SELECT DISTINCT s.name, s.country, s.lat, s.lon
                FROM weather_init_stage s
                WHERE s.name IS NOT NULL
                ON CONFLICT (name, country) DO UPDATE
                SET lat = EXCLUDED.lat, lon = EXCLUDED.lon;
            """)

            # 4) weather_raw UPSERT
            cur.execute("""
                INSERT INTO weather_raw (city_id, ts, source, kind, payload)
                SELECT
                    c.city_id,
                    s.ts,
                    %s AS source,
                    %s AS kind,
                    jsonb_build_object(
                        'dt', EXTRACT(EPOCH FROM s.ts)::bigint,
                        'main', jsonb_build_object(
                            'temp', s.temp,
                            'feels_like', s.feels_like,
                            'humidity', s.humidity
                        ),
                        'wind', jsonb_build_object('speed', s.wind_speed),
                        'weather', jsonb_build_array(jsonb_build_object('description', s.description))
                    )
                FROM weather_init_stage s
                JOIN cities c
                  ON c.name = s.name
                 AND ((c.country IS NULL AND s.country IS NULL) OR c.country = s.country)
                ON CONFLICT (city_id, ts, source, kind) DO NOTHING;
            """, (source, kind))

        conn.commit()   # ← 여기서 커밋하면 TEMP 테이블도 드롭됨
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

def save_today_csv_to_db(csv_path: str, postgres_conn_id: str = "postgres"):
    """
    같은 커넥션에서 TEMP 생성 -> COPY -> UPSERT -> COMMIT
    DDL 없음(이미 init_db_dag에서 테이블 생성)
    """
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    conn = hook.get_conn()
    try:
        with conn.cursor() as cur:
            # 1) TEMP 테이블 (COMMIT 시 자동 드롭)
            cur.execute("""
                CREATE TEMP TABLE stage_today (
                  name        TEXT,
                  country     TEXT,
                  lat         DOUBLE PRECISION,
                  lon         DOUBLE PRECISION,
                  ts          TIMESTAMPTZ,
                  temp        DOUBLE PRECISION,
                  feels_like  DOUBLE PRECISION,
                  humidity    INT,
                  wind_speed  DOUBLE PRECISION,
                  description TEXT
                ) ON COMMIT DROP;
            """)

            # 2) 같은 커넥션으로 COPY
            with open(csv_path, "r", encoding="utf-8") as f:
                cur.copy_expert("""
                    COPY stage_today(name,country,lat,lon,ts,temp,feels_like,humidity,wind_speed,description)
                    FROM STDIN WITH CSV HEADER DELIMITER ',';
                """, f)

            # 3) cities 업서트
            cur.execute("""
                INSERT INTO cities (name, country, lat, lon)
                SELECT DISTINCT s.name, s.country, s.lat, s.lon
                FROM stage_today s
                WHERE s.name IS NOT NULL
                ON CONFLICT (name, country) DO UPDATE
                SET lat = EXCLUDED.lat, lon = EXCLUDED.lon;
            """)

            # 4) 오늘(current)만 적재
            cur.execute("""
                INSERT INTO weather_raw (city_id, ts, source, kind, payload)
                SELECT
                    c.city_id,
                    s.ts,
                    'openweather' AS source,
                    'current'     AS kind,
                    jsonb_build_object(
                        'dt', EXTRACT(EPOCH FROM s.ts)::bigint,
                        'main', jsonb_build_object(
                            'temp', s.temp,
                            'feels_like', s.feels_like,
                            'humidity', s.humidity
                        ),
                        'wind', jsonb_build_object('speed', s.wind_speed),
                        'weather', jsonb_build_array(jsonb_build_object('description', s.description))
                    )
                FROM stage_today s
                JOIN cities c
                  ON c.name = s.name
                 AND ((c.country IS NULL AND s.country IS NULL) OR c.country = s.country)
                WHERE s.ts::date = CURRENT_DATE
                ON CONFLICT (city_id, ts, source, kind) DO NOTHING;
            """)

        conn.commit()   # 여기서 TEMP 자동 드롭
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

def build_today_csv_from_tmp(tmp_dir: str, out_csv: str | None = None) -> str:
    """
    tmp_dir의 *_current.json들을 모아 CSV 생성 후 경로 반환
    컬럼: name,country,lat,lon,ts,temp,feels_like,humidity,wind_speed,description
    """
    p = Path(tmp_dir)
    p.mkdir(parents=True, exist_ok=True)
    if out_csv is None:
        out_csv = str(p / f"today_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.csv")

    header = ["name","country","lat","lon","ts","temp","feels_like","humidity","wind_speed","description"]
    with open(out_csv, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f); w.writerow(header)
        for jf in sorted(p.glob("*_current.json")):
            obj = json.loads(jf.read_text(encoding="utf-8"))
            name = obj.get("name")
            country = (obj.get("sys") or {}).get("country")
            coord = obj.get("coord") or {}
            lat, lon = coord.get("lat"), coord.get("lon")
            ts = obj.get("dt")
            ts_iso = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat() if isinstance(ts,(int,float)) else None
            main = obj.get("main") or {}
            wind = obj.get("wind") or {}
            weather0 = (obj.get("weather") or [{}])[0] or {}
            w.writerow([name, country, lat, lon, ts_iso,
                        main.get("temp"), main.get("feels_like"), main.get("humidity"),
                        wind.get("speed"), weather0.get("description")])
    return out_csv  # ← 반드시 문자열 경로 반환!