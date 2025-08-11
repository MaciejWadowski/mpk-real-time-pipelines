#!/usr/bin/env python3
"""
weather_api_loader.py

Downloads historical weather data from Open-Meteo for a list of locations
and loads it into Snowflake under GTFS_TEST.WEATHER_API_STAGING.

- Start/end dates are declared at the top.
- Reads coords & district names from input_values.json (same folder).
- Creates two tables: HOURLY_WEATHER & MINUTELY_15_WEATHER.
"""

import json
from datetime import datetime
from pathlib import Path

import requests
import yaml
import snowflake.connector

# ──────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ──────────────────────────────────────────────────────────────────────────────

# date range for API calls
START_DATE = "2025-05-21"
END_DATE   = "2025-07-21"

# paths to credential/config & input file
BASE_DIR    = Path(__file__).parent
CONFIG_YML  = BASE_DIR / "config.yml"
INPUT_JSON  = BASE_DIR / "input_values.json"

# Snowflake destination
DB         = "GTFS_TEST"
SCHEMA     = "WEATHER_API_STAGING"
HOURLY_TBL = f"{DB}.{SCHEMA}.HOURLY_WEATHER"
MIN15_TBL  = f"{DB}.{SCHEMA}.MINUTELY_15_WEATHER"

# API endpoint & query fields
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

# ──────────────────────────────────────────────────────────────────────────────
# UTILITIES
# ──────────────────────────────────────────────────────────────────────────────

def load_sf_config(cfg_path: Path):
    with open(cfg_path, "r", encoding="utf-8") as f:
        conf = yaml.safe_load(f)
    return conf["snowflake"]

def init_snowflake(sf):
    """
    Connect, create schema & tables if missing.
    """
    conn = snowflake.connector.connect(
        user=sf["user"],
        password=sf["password"],
        account=sf["account"],
        database=sf["database"],
        warehouse=sf["warehouse"],
        autocommit=True
    )
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
    return conn

def fetch_weather(name, lat, lon):
    """
    Call Open-Meteo and return two lists of dicts:
      hourly_records, minutely_records
    """
    params = {
        "latitude":    lat,
        "longitude":   lon,
        "hourly":      ",".join(HOURLY_FIELDS),
        "minutely_15": ",".join(MIN15_FIELDS),
        "start_date":  START_DATE,
        "end_date":    END_DATE,
        "models":      "best_match"
    }
    resp = requests.get(API_URL, params=params)
    resp.raise_for_status()
    data = resp.json()

    # hourly
    hrs = data.get("hourly", {})
    times_h = hrs.get("time", [])
    hourly = []
    for i, t in enumerate(times_h):
        rec = {
            "district_name": name,
            "latitude":      lat,
            "longitude":     lon,
            "timestamp":     datetime.fromisoformat(t)
        }
        for fld in HOURLY_FIELDS:
            rec[fld] = hrs.get(fld, [None]*len(times_h))[i]
        hourly.append(rec)

    # minutely_15
    mins = data.get("minutely_15", {})
    times_m = mins.get("time", [])
    minutely = []
    for i, t in enumerate(times_m):
        rec = {
            "district_name": name,
            "latitude":      lat,
            "longitude":     lon,
            "timestamp":     datetime.fromisoformat(t)
        }
        for fld in MIN15_FIELDS:
            rec[fld] = mins.get(fld, [None]*len(times_m))[i]
        minutely.append(rec)

    return hourly, minutely

def insert_records(conn, table, columns, records):
    if not records:
        return
    cols_str = ", ".join(columns)
    vals_tpl = ", ".join([f"%({c})s" for c in columns])
    sql = f"INSERT INTO {table} ({cols_str}) VALUES ({vals_tpl})"
    cur = conn.cursor()
    cur.executemany(sql, records)

# ──────────────────────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────────────────────

def main():
    # load config & initialize
    sf_cfg = load_sf_config(CONFIG_YML)
    conn   = init_snowflake(sf_cfg)

    # load locations
    inp = json.loads(INPUT_JSON.read_text(encoding="utf-8"))
    locations = inp.get("locations", [])

    # define columns for insertion
    hourly_cols = ["district_name", "latitude", "longitude", "timestamp"] + HOURLY_FIELDS
    min15_cols  = ["district_name", "latitude", "longitude", "timestamp"] + MIN15_FIELDS

    for loc in locations:
        name = loc["district"]
        lat  = loc["latitude"]
        lon  = loc["longitude"]
        print(f"Fetching for {name} @ ({lat},{lon})…")
        hourly, minutely = fetch_weather(name, lat, lon)

        print(f"  → inserting {len(hourly)} hourly rows")
        insert_records(conn, HOURLY_TBL, hourly_cols, hourly)

        print(f"  → inserting {len(minutely)} minutely rows")
        insert_records(conn, MIN15_TBL, min15_cols, minutely)

    print("All data loaded successfully.")

if __name__ == "__main__":
    main()
