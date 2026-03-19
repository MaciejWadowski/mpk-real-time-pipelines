# CLAUDE.md - MPK Real-Time Pipelines

## Project Overview

Hobby data engineering project analyzing Kraków public transit (MPK) delays using GTFS data. A team of 5 developers collects real-time vehicle positions (every minute), static schedules, weather data, and traffic intensity to measure average delays, find correlations, and identify root causes. Future goal: public interactive dashboard.

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Orchestration | Apache Airflow 2.10.5 (CeleryExecutor) |
| Data Warehouse | Snowflake (account migrates monthly) |
| Transformation | dbt-snowflake 1.9.4 |
| Ingestion | Python (requests, pandas, protobuf) |
| Message Queue | Redis 7.2 |
| Metadata DB | PostgreSQL 15 (Airflow only) |
| APIs | TomTom (traffic), Open-Meteo (weather), ZTP Kraków (GTFS) |
| Containerization | Docker Compose |
| Testing | pytest + unittest.mock |
| CI/CD | GitHub Actions (self-hosted runner for deploy) |
| Dev Environment | Jupyter (datascience-notebook, Python 3.12) |

## Repository Structure

```
├── dags/                    # Airflow DAG definitions
│   ├── gtfs_download_static.py          # Daily static GTFS download
│   ├── gtfs_download_realtime_cache.py  # Every 1 min: cache vehicle positions & trip updates
│   ├── gtfs_aggregate_realtime_upload.py # Every 30 min: aggregate & upload to Snowflake
│   ├── traffic_intensity_ingestion.py    # Every 15 min (7am-8pm): TomTom traffic data
│   └── openmeteo_weather_loader.py       # Weather data for 18 Kraków districts
├── dbt/gtfs_project/        # dbt project
│   ├── dbt_project.yml      # Project config (version 1.9.6)
│   ├── profiles.yml         # Local dev Snowflake connection (DO NOT COMMIT secrets)
│   ├── test-profiles.yml    # CI/CD template with ${VARIABLE} substitution
│   └── models/
│       ├── sources.yml                   # Source definitions (3 schemas)
│       ├── trips_schedule_table.sql      # Static schedule + stops + routes
│       ├── aggregated_calendar_table.sql # Date series with day-of-week logic
│       ├── delays_table.sql             # Actual vs planned arrival comparison
│       ├── delays_table_vectors.sql     # Stop-pair segments for vehicle matching
│       ├── stops_weather.sql            # Nearest-district weather join (ST_DISTANCE)
│       └── vechicle_position.sql        # Vehicle positions (NOTE: typo in filename)
├── python_connectors/       # Standalone data loaders (outside Airflow)
│   ├── weather_api_loader.py
│   ├── centroids_loader.py
│   ├── snowflake_migration_uploader.py
│   ├── snowflake_migration_downloader.py
│   └── krakow_districts/    # GeoJSON + centroid data for 18 districts
├── dockerfiles/Dockerfile   # Extends airflow:2.10.5 with gtfs-kit, snowflake, protobuf
├── docker-compose.yml       # Full Airflow stack (webserver, scheduler, worker, triggerer, redis, postgres)
├── config/                  # Airflow configuration
├── plugins/                 # Empty (unused)
├── notebooks/               # Jupyter notebooks for EDA and analysis
├── tests/                   # pytest tests (currently only traffic_intensity)
├── .github/workflows/       # CI: pytest on PR; CD: manual deploy via self-hosted runner
├── sf.py                    # Legacy standalone data loading script (63KB)
├── gtfs-realtime.proto      # GTFS-RT protobuf schema definition
├── init.sh                  # Quick setup: webserver + db init + admin user
└── shapes.txt               # Route geometry data (14MB)
```

## Snowflake Schemas

| Schema | Purpose |
|--------|---------|
| SCHEDULE | Static GTFS (routes, stops, trips, calendar, shapes, traffic_intensity) |
| TRIP_UPDATES | Real-time trip updates and vehicle positions |
| WEATHER_API_STAGING | Hourly weather + district locations |
| SCHEDULE_DATA_MARTS | dbt-transformed models (output) |

All dbt models use `materialized: 'table'` (full refresh). No incremental models yet.

## Data Sources

- **Static GTFS**: `https://gtfs.ztp.krakow.pl/GTFS_KRK_{A,M,T}.zip` (A=tram, M=bus, T=light rail)
- **Vehicle Positions**: `https://gtfs.ztp.krakow.pl/VehiclePositions_{T,A,M}.pb` (protobuf)
- **Trip Updates**: `https://gtfs.ztp.krakow.pl/TripUpdates_{T,A,M}.pb` (protobuf)
- **Weather**: Open-Meteo API (free, no auth)
- **Traffic**: TomTom API (key required, stored as Airflow Variable)

## Key Patterns

- **Airflow connection ID**: `my_snowflake_conn` (used everywhere via SnowflakeHook)
- **Mode column**: A (tram), M (bus), T (light rail), TR (combined - filtered out in queries)
- **Timestamps**: UTC stored, converted to `Europe/Warsaw` in dbt models
- **Date format**: YYYYMMDD strings for joins between GTFS tables
- **Deduplication**: Uses `load_timestamp` - keeps latest per partition
- **Midnight handling**: Special logic for overnight trips (arrival > 23:59)
- **Real-time caching**: `/tmp/gtfs_cache/` → parquet files → 60-min retention
- **TomTom keys**: Multiple keys with batch round-robin (Airflow Variable, JSON)

## Development Commands

```bash
# Start Airflow stack
docker compose up -d

# Stop
docker compose down

# Rebuild after Dockerfile changes
docker compose up -d --build

# Run dbt models (from inside container or locally with profiles.yml)
cd dbt/gtfs_project
dbt run

# Run tests
pytest tests/

# First-time setup
cp .env-sample .env  # then edit secrets
docker compose up airflow-webserver -d
docker compose run airflow-webserver airflow db init
docker compose run airflow-webserver airflow users create \
  --username admin --password admin \
  --firstname Admin --lastname User \
  --role Admin --email admin@example.com
docker compose up -d
```

## Conventions

- **Commits**: English language
- **DAG IDs**: snake_case (e.g., `gtfs_download_static`)
- **Python files**: snake_case
- **Snowflake tables**: UPPERCASE (with explicit `alias` in dbt)
- **DAG defaults**: `catchup=False`, `depends_on_past=False`, 1-2 retries
- **Airflow port**: 8080 (webserver), 5555 (Flower monitoring)

## Known Issues / Technical Debt

- `vechicle_position.sql` has a typo (should be `vehicle`) - referenced by alias so renaming file is safe
- Hardcoded Snowflake credentials in `dbt/gtfs_project/profiles.yml` and `python_connectors/config.yml` - sensitive, never commit real passwords
- `sf.py` is a legacy monolith (63KB) - functionality has been split into DAGs
- No dbt tests defined (no schema tests, no data tests)
- Test coverage limited to traffic_intensity DAG only
- Deploy uses self-hosted runner on local machine (temporary)
- Single dbt target (dev) - no staging/prod separation

## CI/CD

- **PR checks** (`pull_request.yml`): Ubuntu, Python 3.11, pytest
- **Deploy** (`deploy.yml`): Manual trigger, self-hosted runner, generates profiles.yml from secrets via envsubst, rebuilds Docker stack with health checks (5-min timeout)

## When Making Changes

- After modifying DAGs: they auto-refresh every 60 seconds in Airflow
- After modifying dbt models: run `dbt run` to rebuild tables
- After modifying Dockerfile: rebuild with `docker compose up -d --build`
- After adding Python dependencies: add to both `requirements.txt` and `dockerfiles/Dockerfile`
- New dbt sources: update `dbt/gtfs_project/models/sources.yml`
- Keep `.env` out of git (it's in `.gitignore`)
