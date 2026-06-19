# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Hobby data engineering project analyzing Kraków public transit (MPK) delays using GTFS data. A team of 5 developers collects real-time vehicle positions (every minute), static schedules, weather data, and traffic intensity to measure average delays, find correlations, and identify root causes. Future goal: public interactive dashboard.

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Orchestration | Apache Airflow 2.10.5 (CeleryExecutor) |
| Data Warehouse | Snowflake (database: `GTFS_TEST`, account migrates monthly) |
| Transformation | dbt-snowflake 1.9.4 |
| Ingestion | Python (requests, pandas, protobuf) |
| Message Queue | Redis 7.2 |
| Metadata DB | PostgreSQL 15 (Airflow only) |
| APIs | TomTom (traffic), Open-Meteo (weather), ZTP Kraków (GTFS) |
| Containerization | Docker Compose |
| Testing | pytest + unittest.mock |
| CI/CD | GitHub Actions (self-hosted runner for deploy) |

## Development Commands

```bash
# Start Airflow stack
docker compose up -d

# Rebuild after Dockerfile changes
docker compose up -d --build

# Run dbt models (from inside container, or locally with dbt/gtfs_project/profiles.yml)
cd dbt/gtfs_project && dbt run

# Run all tests
pytest tests/

# Run a single test file
pytest tests/traffic_intensity_test.py

# Run a single test
pytest tests/traffic_intensity_test.py::test_fetch_tomtom_traffic_intensity_success

# First-time setup
cp .env-sample .env  # then edit secrets
bash init.sh
```

## Repository Structure

```
├── dags/
│   ├── download_gtfs_static.py          # @daily: downloads GTFS feeds → Snowflake SCHEDULE schema (with SCD2 for ROUTES)
│   ├── download_gtfs_realtime.py        # */1 * * * *: fetches protobuf → parquet cache at /tmp/gtfs_cache/
│   ├── upload_gtfs_realtime.py          # */30 * * * *: aggregates cache → Snowflake TRIP_UPDATES schema
│   ├── traffic_intensity_ingestion.py   # */15 7-20 * * *: TomTom API → Snowflake SCHEDULE.TRAFFIC_INTENSITY
│   ├── weather_openmeteo_loader_dag.py  # @daily: Open-Meteo → Snowflake WEATHER_API_STAGING
│   ├── traffic_intensity.py             # Logic module (imported by traffic_intensity_ingestion.py)
│   ├── gtfs_static_config.json          # SCD2/insert config for each static GTFS table
│   └── gtfs_realtime_pb2.py             # Generated protobuf bindings (do not edit)
├── dbt/gtfs_project/models/
│   ├── sources.yml                      # Source definitions (3 schemas, gtfs_test database)
│   ├── trips_schedule_table.sql         # Joins stop_times + trips + stops + routes_scd2 + calendar
│   ├── aggregated_calendar_table.sql    # Date series with day-of-week expansion
│   ├── delays_table.sql                 # Actual vs planned arrival; deduped by load_timestamp
│   ├── delays_table_vectors.sql         # Stop-pair segments for vehicle matching
│   ├── stops_weather.sql                # Nearest-district weather join (ST_DISTANCE)
│   └── vechicle_position.sql            # Vehicle positions (typo in filename — do not rename, referenced by alias)
├── python_connectors/                   # Standalone loaders run outside Airflow (migration, centroids, weather)
├── tests/                               # pytest; currently only covers traffic_intensity logic
└── notebooks/                           # Jupyter EDA notebooks
```

## Data Flow

```
ZTP GTFS static feeds (A/M/T zips)
  └─ download_gtfs_static DAG ──→ SCHEDULE.*_STG tables
                                 └─ SCD2 merge (ROUTES_SCD2) or simple INSERT
                                    └─ dbt run ──→ SCHEDULE_DATA_MARTS.*

ZTP GTFS realtime protobuf feeds (every 1 min)
  └─ download_gtfs_realtime DAG ──→ /tmp/gtfs_cache/{trip_updates,vehicle_positions}/*.parquet
                                    └─ upload_gtfs_realtime DAG (every 30 min) ──→ TRIP_UPDATES.*

TomTom API (every 15 min, 7am–8pm)
  └─ traffic_intensity_ingestion DAG ──→ SCHEDULE.TRAFFIC_INTENSITY

Open-Meteo API (daily)
  └─ openmeteo_weather_loader DAG ──→ WEATHER_API_STAGING.{HOURLY_WEATHER,MINUTELY_15_WEATHER}
```

## Snowflake Schemas (database: GTFS_TEST)

| Schema | Purpose |
|--------|---------|
| SCHEDULE | Static GTFS staging + SCD2 tables + traffic_intensity |
| TRIP_UPDATES | Real-time TRIP_UPDATES and VEHICLE_POSITIONS tables |
| WEATHER_API_STAGING | HOURLY_WEATHER, MINUTELY_15_WEATHER, DISTRICT_LOCATIONS |
| SCHEDULE_DATA_MARTS | dbt output models |

## Key Patterns

- **Airflow connection ID**: `my_snowflake_conn` (SnowflakeHook, used in every DAG)
- **Airflow Variable**: `tom_tom_api_keys` — JSON list of keys for round-robin batching
- **Transit mode codes**: A (tram), M (bus), T (light rail), TR (combined — filtered out in all dbt queries)
- **Timestamps**: stored UTC as `TIMESTAMP_NTZ`, converted to `Europe/Warsaw` in dbt
- **Date format**: `YYYYMMDD` strings used for joins between GTFS tables
- **Deduplication**: `QUALIFY ROW_NUMBER() OVER (PARTITION BY ... ORDER BY LOAD_TIMESTAMP DESC) = 1`
- **Midnight handling**: trips with arrival `HH > 23` in GTFS are handled by subtracting 24h and shifting the date; the join logic in `delays_table.sql` explicitly covers the 23:00/00:00 boundary
- **SCD2**: only ROUTES table uses SCD2 (via `_apply_scd2` in `download_gtfs_static.py`); config in `dags/gtfs_static_config.json`
- **Static staging pattern**: data lands in `TABLE_STG`, then merged to the production table; `dbt run` is triggered as the final step of `gtfs_download_static`
- **dbt invocation**: called programmatically via `dbtRunner` inside a `PythonVirtualenvOperator` (dbt-snowflake==1.9.4) at end of static download DAG
- **dbt profiles**: `dbt/gtfs_project/profiles.yml` for local dev; CI uses `dbt/test-profiles.yml` with `${VARIABLE}` substitution via `envsubst`

## Conventions

- Commit messages in English
- DAG IDs: `snake_case` matching filename (e.g., `gtfs_download_static`)
- Snowflake table names: UPPERCASE; dbt models use `alias` config to set the target name
- DAG defaults: `catchup=False`, `depends_on_past=False`, 1–2 retries
- Airflow ports: 8080 (webserver), 5555 (Flower)

## When Making Changes

- **DAGs**: auto-refresh in Airflow every 60 seconds; no restart needed
- **dbt models**: run `dbt run` to rebuild; also triggered automatically at end of `gtfs_download_static`
- **New dbt sources**: update `dbt/gtfs_project/models/sources.yml`
- **Dockerfile**: rebuild with `docker compose up -d --build`
- **Python dependencies**: add to both `requirements.txt` and `dockerfiles/Dockerfile`
- **Static table schema change**: update `dags/gtfs_static_config.json` if PKs or SCD2 settings change

## Known Issues / Technical Debt

- `dbt/gtfs_project/dbt_project.yml` sets `materialized: view` only for the `example` subfolder — actual models each set their own `materialized: 'table'` via `config()` block
- `dbt_project.yml` version field is `1.9.6` (matches sources.yml version, not a package version)
- No dbt tests defined (schema or data tests)
- Test coverage limited to `traffic_intensity.py` logic only
- `sf.py` is a legacy 63KB monolith — all functionality has been migrated to DAGs
- `dbt/gtfs_project/profiles.yml` and `python_connectors/config.yml` may contain real credentials — never commit
- Snowflake account identifier changes monthly (coordinate with team before running migration scripts)

## CI/CD

- **PR checks** (`.github/workflows/pull_request.yml`): runs on every push/PR, Ubuntu + Python 3.11, installs `requirements.txt`, runs `pytest`
- **Deploy** (`.github/workflows/deploy.yml`): manual trigger only, self-hosted runner, generates `profiles.yml` from GitHub secrets via `envsubst`, rebuilds Docker stack with 5-min health check timeout
