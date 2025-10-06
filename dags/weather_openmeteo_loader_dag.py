from __future__ import annotations
from datetime import datetime, date, timedelta
import logging
from pathlib import Path

import requests

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

# ====== LOGGER ======
logger = logging.getLogger("airflow.task")

# ====== CONFIG / LOCATIONS ======
LOCATIONS = [
    { "district": "Stare Miasto (I)",             "latitude": 50.061389, "longitude": 19.937222 },
    { "district": "Grzegórzki (II)",               "latitude": 50.051389, "longitude": 19.957778 },
    { "district": "Prądnik Czerwony (III)",        "latitude": 50.079167, "longitude": 19.961667 },
    { "district": "Prądnik Biały (IV)",            "latitude": 50.085278, "longitude": 19.884444 },
    { "district": "Krowodrza (V)",                 "latitude": 50.063611, "longitude": 19.924722 },
    { "district": "Bronowice (VI)",                "latitude": 50.085833, "longitude": 19.908333 },
    { "district": "Zwierzyniec (VII)",             "latitude": 50.056944, "longitude": 19.905556 },
    { "district": "Dębniki (VIII)",                "latitude": 50.021111, "longitude": 19.893333 },
    { "district": "Łagiewniki-Borek Fałęcki (IX)", "latitude": 50.013333, "longitude": 19.951111 },
    { "district": "Swoszowice (X)",                "latitude": 49.972778, "longitude": 19.937778 },
    { "district": "Podgórze Duchackie (XI)",       "latitude": 50.024167, "longitude": 19.939444 },
    { "district": "Bieżanów-Prokocim (XII)",       "latitude": 50.018056, "longitude": 19.969167 },
    { "district": "Podgórze (XIII)",               "latitude": 50.042500, "longitude": 19.948056 },
    { "district": "Czyżyny (XIV)",                 "latitude": 50.080556, "longitude": 20.015556 },
    { "district": "Mistrzejowice (XV)",            "latitude": 50.085556, "longitude": 20.071111 },
    { "district": "Bieńczyce (XVI)",               "latitude": 50.108333, "longitude": 20.081389 },
    { "district": "Wzgórza Krzesławickie (XVII)",  "latitude": 50.098056, "longitude": 20.106667 },
    { "district": "Nowa Huta (XVIII)",             "latitude": 50.085556, "longitude": 20.190556 }
]

API_URL = "https://api.open-meteo.com/v1/forecast"
HOURLY_FIELDS = [
    "temperature_2m", "wind_speed_10m", "relative_humidity_2m",
    "precipitation", "weather_code", "pressure_msl",
    "cloud_cover", "visibility", "is_day"
]
MIN15_FIELDS = [
    "temperature_2m", "precipitation", "is_day",
    "wind_gusts_10m", "wind_speed_10m", "wind_direction_10m"
]

# Snowflake target
DB = "GTFS_TEST"
SCHEMA = "WEATHER_API_STAGING"
HOURLY_TBL = f"{DB}.{SCHEMA}.HOURLY_WEATHER"
MIN15_TBL  = f"{DB}.{SCHEMA}.MINUTELY_15_WEATHER"

# Snowflake Airflow connection id
SNOWFLAKE_CONN_ID = "my_snowflake_conn"

# ====== HELPERS ======
def ymd(d: date) -> str:
    return d.strftime("%Y-%m-%d")

def create_schema_and_tables(conn):
    logger.info("Ensuring schema and tables exist: %s.%s", DB, SCHEMA)
    cur = conn.cursor()
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {DB}.{SCHEMA};")
    cur.execute(f"""
      CREATE TABLE IF NOT EXISTS {HOURLY_TBL} (
        district_name       STRING,
        latitude            FLOAT,
        longitude           FLOAT,
        timestamp           TIMESTAMP_NTZ,
        temperature_2m      FLOAT,
        wind_speed_10m      FLOAT,
        relative_humidity_2m FLOAT,
        precipitation       FLOAT,
        weather_code        INT,
        pressure_msl        FLOAT,
        cloud_cover         FLOAT,
        visibility          FLOAT,
        is_day              BOOLEAN
      );
    """)
    cur.execute(f"""
      CREATE TABLE IF NOT EXISTS {MIN15_TBL} (
        district_name    STRING,
        latitude         FLOAT,
        longitude        FLOAT,
        timestamp        TIMESTAMP_NTZ,
        temperature_2m   FLOAT,
        precipitation    FLOAT,
        is_day           BOOLEAN,
        wind_gusts_10m   FLOAT,
        wind_speed_10m   FLOAT,
        wind_direction_10m FLOAT
      );
    """)
    cur.close()
    logger.info("Schema and tables ready")

def fetch_open_meteo(lat: float, lon: float, start_date: str, end_date: str):
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": ",".join(HOURLY_FIELDS),
        "minutely_15": ",".join(MIN15_FIELDS),
        "start_date": start_date,
        "end_date": end_date,
        "models": "best_match"
    }
    logger.info("Requesting Open-Meteo for %s,%s from %s to %s", lat, lon, start_date, end_date)
    r = requests.get(API_URL, params=params, timeout=60)
    r.raise_for_status()
    logger.info("Open-Meteo response OK: %s rows (hourly time length)", len(r.json().get("hourly", {}).get("time", [])))
    return r.json()

def build_hourly_records(name: str, lat: float, lon: float, api_json: dict):
    hrs = api_json.get("hourly", {})
    times = hrs.get("time", [])
    records = []
    for i, t in enumerate(times):
        ts = datetime.fromisoformat(t)
        rec = {
            "district_name": name,
            "latitude": lat,
            "longitude": lon,
            "timestamp": ts,
        }
        for fld in HOURLY_FIELDS:
            arr = hrs.get(fld, [])
            rec[fld] = arr[i] if i < len(arr) else None
        records.append(rec)
    return records

def build_min15_records(name: str, lat: float, lon: float, api_json: dict):
    mins = api_json.get("minutely_15", {})
    times = mins.get("time", [])
    records = []
    for i, t in enumerate(times):
        ts = datetime.fromisoformat(t)
        rec = {
            "district_name": name,
            "latitude": lat,
            "longitude": lon,
            "timestamp": ts,
        }
        for fld in MIN15_FIELDS:
            arr = mins.get(fld, [])
            rec[fld] = arr[i] if i < len(arr) else None
        records.append(rec)
    return records

def insert_many_snowflake(conn, table: str, cols: list[str], records: list[dict]):
    if not records:
        logger.info("No records to insert into %s", table)
        return
    logger.info("Inserting %s records into %s", len(records), table)
    cur = conn.cursor()
    cols_str = ", ".join(cols)
    placeholders = ", ".join(["%s"] * len(cols))
    sql = f"INSERT INTO {table} ({cols_str}) VALUES ({placeholders})"
    tuples = []
    for r in records:
        tup = tuple(r.get(c) for c in cols)
        tuples.append(tup)
    cur.executemany(sql, tuples)
    cur.close()
    logger.info("Inserted %s records into %s", len(records), table)

# ====== DAG TASK ======
def run_fetch_and_load(**context):
    """
    - Compute global start_date = two months ago, end_date = today
    - For each location, check max existing day in HOURLY_WEATHER safely
    - Fetch missing data from Open-Meteo and insert
    """
    today = date.today()
    two_months_ago = today - timedelta(days=60)
    global_start = two_months_ago
    global_end = today

    logger.info("DAG run date: %s. Global window: %s -> %s", today, global_start, global_end)

    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()

    # Ensure schema and tables exist before any SELECTs
    create_schema_and_tables(conn)

    hourly_cols = ["district_name", "latitude", "longitude", "timestamp"] + HOURLY_FIELDS
    min15_cols  = ["district_name", "latitude", "longitude", "timestamp"] + MIN15_FIELDS

    total_hourly = 0
    total_min15 = 0

    for loc in LOCATIONS:
        name = loc["district"]
        lat = loc["latitude"]
        lon = loc["longitude"]

        logger.info("Processing location: %s (%s,%s)", name, lat, lon)

        # Safe attempt to get latest existing day; if it fails treat as no rows
        existing_max_day = None
        try:
            cur = conn.cursor()
            cur.execute(
                f"SELECT MAX(DATE_TRUNC('day', timestamp)) FROM {HOURLY_TBL} WHERE district_name = %s",
                (name,)
            )
            row = cur.fetchone()
            cur.close()
            if row and row[0]:
                existing_max = row[0]
                existing_max_day = existing_max.date() if isinstance(existing_max, datetime) else existing_max
                logger.info("Existing max day for %s: %s", name, existing_max_day)
            else:
                logger.info("No existing rows found for %s", name)
        except Exception as e:
            logger.warning("Could not read existing max day for %s: %s", name, e)
            existing_max_day = None

        if existing_max_day:
            fetch_start = max(global_start, existing_max_day + timedelta(days=1))
        else:
            fetch_start = global_start

        fetch_end = global_end

        if fetch_start > fetch_end:
            logger.info("No new data to fetch for %s (fetch_start %s > fetch_end %s)", name, fetch_start, fetch_end)
            continue

        start_str = ymd(fetch_start)
        end_str = ymd(fetch_end)
        logger.info("Fetching data for %s: %s -> %s", name, start_str, end_str)

        try:
            api_json = fetch_open_meteo(lat, lon, start_str, end_str)
        except Exception as e:
            logger.error("Failed to fetch Open-Meteo for %s: %s", name, e)
            continue

        hourly_recs = build_hourly_records(name, lat, lon, api_json)
        min15_recs = build_min15_records(name, lat, lon, api_json)

        logger.info("Fetched %s hourly rows and %s min15 rows for %s", len(hourly_recs), len(min15_recs), name)

        try:
            insert_many_snowflake(conn, HOURLY_TBL, hourly_cols, hourly_recs)
            insert_many_snowflake(conn, MIN15_TBL, min15_cols, min15_recs)
        except Exception as e:
            logger.error("Failed to insert data for %s into Snowflake: %s", name, e)
            continue

        total_hourly += len(hourly_recs)
        total_min15 += len(min15_recs)

    conn.close()
    logger.info("Total hourly rows inserted: %s", total_hourly)
    logger.info("Total min15 rows inserted: %s", total_min15)
    return {"hourly_rows": total_hourly, "min15_rows": total_min15}

# ====== DAG DEFINITION ======
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="openmeteo_weather_loader",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["open-meteo", "weather", "snowflake"],
) as dag:

    task_fetch_and_load = PythonOperator(
        task_id="fetch_and_load_openmeteo",
        python_callable=run_fetch_and_load,
        provide_context=True,
    )

    task_fetch_and_load
