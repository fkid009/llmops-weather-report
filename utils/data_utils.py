import os, json, time, logging, requests, yaml, psycopg2, csv
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Dict

from utils.path import CITIES_YAML, RAW_DIR, INIT_DATA_DIR, INIT_TMP_DIR

from airflow.providers.postgres.hooks.postgres import PostgresHook




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
    source: str = "bootstrap",   # init에서 온 데이터 표시
    kind: str = "forecast",      # init은 전부 forecast로 넣기로 합의
) -> None:
    """
    _prepare_init_files 가 만든 CSV를 DB에 반영:
      CSV -> (TEMP stage COPY) -> cities UPSERT -> weather_raw UPSERT
    ※ TEMP 테이블은 세션 종료 시 자동 삭제 (영구 DDL 필요 없음)
    """
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)

    # 1) TEMP stage 생성 (세션 한정, on commit drop)
    hook.run("""
        BEGIN;
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

    # 2) COPY to TEMP stage (헤더 포함 CSV)
    hook.copy_expert(
        filename=csv_path,
        sql="""
            COPY weather_init_stage(name, country, lat, lon, ts, temp, feels_like, humidity, wind_speed, description)
            FROM STDIN WITH CSV HEADER DELIMITER ',';
        """,
    )

    # 3) cities UPSERT (영구 테이블로)
    hook.run("""
        INSERT INTO cities (name, country, lat, lon)
        SELECT DISTINCT s.name, s.country, s.lat, s.lon
        FROM weather_init_stage s
        WHERE s.name IS NOT NULL
        ON CONFLICT (name, country) DO UPDATE
        SET lat = EXCLUDED.lat,
            lon = EXCLUDED.lon;
    """)

    # 4) weather_raw UPSERT (payload JSONB를 SQL에서 구성)
    hook.run(
        """
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
            ) AS payload
        FROM weather_init_stage s
        JOIN cities c
          ON c.name = s.name
         AND ((c.country IS NULL AND s.country IS NULL) OR c.country = s.country)
        ON CONFLICT (city_id, ts, source, kind) DO NOTHING;
        """,
        parameters=(source, kind),
    )

    # 5) 커밋 (TEMP 테이블은 ON COMMIT DROP)
    hook.run("COMMIT;")

def save_today_csv_to_db(csv_path: str, postgres_conn_id: str = "postgres"):
    """
    csv_path: 이전 태스크가 만든 '오늘' 스냅샷 CSV 경로
    CSV 헤더 가정:
      name,country,lat,lon,ts,temp,feels_like,humidity,wind_speed,description
    → cities 업서트 후, weather_raw에 kind='current' 로 '오늘'만 INSERT
    """

    hook = PostgresHook(postgres_conn_id=postgres_conn_id)

    hook.run("""
        BEGIN;
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

    # CSV → stage
    hook.copy_expert(
        filename=csv_path,
        sql="""
            COPY stage_today(name,country,lat,lon,ts,temp,feels_like,humidity,wind_speed,description)
            FROM STDIN WITH CSV HEADER DELIMITER ',';
        """,
    )

    # cities 업서트
    hook.run("""
        INSERT INTO cities (name, country, lat, lon)
        SELECT DISTINCT s.name, s.country, s.lat, s.lon
        FROM stage_today s
        WHERE s.name IS NOT NULL
        ON CONFLICT (name, country) DO UPDATE
        SET lat = EXCLUDED.lat, lon = EXCLUDED.lon;
    """)

    # 오늘(current)만 적재 (forecast 무시)
    hook.run("""
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
        WHERE s.ts::date = CURRENT_DATE             -- ← 딱 '오늘'만
        ON CONFLICT (city_id, ts, source, kind) DO NOTHING;
        COMMIT;
    """)