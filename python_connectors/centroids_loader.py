#!/usr/bin/env python3
"""
centroids_loader.py

Reads district centroid data from input_values.json (in script folder),
reads Snowflake creds from config.yml (in script folder),
creates WEATHER_API_STAGING.DISTRICT_CENTROIDS,
and inserts via INSERT…SELECT…UNION:
  - district_number INT or NULL
  - district_name   STRING
  - latitude        FLOAT
  - longitude       FLOAT
  - location        GEOGRAPHY
"""

import json
import re
from pathlib import Path

import yaml
import snowflake.connector

# ──────────────────────────────────────────────────────────────────────────────
# PATHS
# ──────────────────────────────────────────────────────────────────────────────

BASE_DIR    = Path(__file__).parent
INPUT_JSON  = BASE_DIR / "input_values.json"
CONFIG_YML  = BASE_DIR / "config.yml"

# ──────────────────────────────────────────────────────────────────────────────
# SNOWFLAKE TARGET
# ──────────────────────────────────────────────────────────────────────────────

DB         = "GTFS_TEST"
SCHEMA     = "WEATHER_API_STAGING"
TABLE      = "DISTRICT_CENTROIDS"
FULL_TABLE = f"{DB}.{SCHEMA}.{TABLE}"

# ──────────────────────────────────────────────────────────────────────────────
# ROMAN MAP
# ──────────────────────────────────────────────────────────────────────────────

_ROMAN_MAP = {
    'I':1, 'IV':4, 'V':5, 'IX':9,
    'X':10, 'XL':40,'L':50,'XC':90,
    'C':100,'D':500,'M':1000
}

def roman_to_int(r: str) -> int:
    i, val = 0, 0
    while i < len(r):
        if i+1<len(r) and r[i:i+2] in _ROMAN_MAP:
            val += _ROMAN_MAP[r[i:i+2]]; i+=2
        else:
            val += _ROMAN_MAP[r[i]]; i+=1
    return val

# ──────────────────────────────────────────────────────────────────────────────
# LOAD CONFIG & INIT
# ──────────────────────────────────────────────────────────────────────────────

def load_sf_config(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)["snowflake"]

def init_sf(cfg: dict):
    ctx = snowflake.connector.connect(
        user=cfg["user"], password=cfg["password"],
        account=cfg["account"], database=cfg["database"],
        warehouse=cfg["warehouse"], autocommit=True
    )
    cur = ctx.cursor()
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {DB}.{SCHEMA};")
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {FULL_TABLE} (
            district_number INT,
            district_name   STRING,
            latitude        FLOAT,
            longitude       FLOAT,
            location        GEOGRAPHY
        );
    """)
    return ctx

# ──────────────────────────────────────────────────────────────────────────────
# LOAD CENTROIDS
# ──────────────────────────────────────────────────────────────────────────────

def load_centroids(path: Path):
    data = json.loads(path.read_text(encoding="utf-8"))
    recs = []
    for loc in data["locations"]:
        name = loc["district"]
        # Match the Roman numeral inside parentheses, e.g. "(VII)"
        m = re.search(r"\(([IVXLCDM]+)\)", name)
        num = roman_to_int(m.group(1)) if m else None
        lat, lon = loc["latitude"], loc["longitude"]
        safe_name = name.replace("'", "''")
        wkt = f"POINT({lon} {lat})"
        recs.append({"num":num,"name":safe_name,"lat":lat,"lon":lon,"wkt":wkt})
    return recs

# ──────────────────────────────────────────────────────────────────────────────
# INSERT VIA SELECT…UNION
# ──────────────────────────────────────────────────────────────────────────────

def insert_recs(ctx, table: str, recs: list):
    if not recs: return
    selects = []
    for r in recs:
        num_lit = "NULL" if r["num"] is None else str(r["num"])
        selects.append(
            f"SELECT {num_lit} AS district_number, "
            f"'{r['name']}' AS district_name, "
            f"{r['lat']} AS latitude, "
            f"{r['lon']} AS longitude, "
            f"ST_GEOGFROMTEXT('{r['wkt']}') AS location"
        )
    sql = (
        f"INSERT INTO {table} "
        "(district_number, district_name, latitude, longitude, location)\n"
        + "\nUNION ALL\n".join(selects)
        + ";"
    )
    ctx.cursor().execute(sql)

# ──────────────────────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────────────────────

def main():
    cfg       = load_sf_config(CONFIG_YML)
    ctx       = init_sf(cfg)
    centroids = load_centroids(INPUT_JSON)
    print(f"Loaded {len(centroids)} centroids")
    insert_recs(ctx, FULL_TABLE, centroids)
    print(f"Inserted {len(centroids)} rows into {FULL_TABLE}")

if __name__ == "__main__":
    main()