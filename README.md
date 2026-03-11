# MPK Real-Time Pipelines

Real-time data pipeline for analyzing public transit delays in Krakow (MPK). Collects GTFS vehicle positions every minute, static schedules, weather data, and traffic intensity to measure average delays, find correlations, and identify root causes.

## Architecture

```
ZTP Krakow (GTFS)  ──┐
Open-Meteo (weather) ─┤──→  Airflow DAGs  ──→  Snowflake  ──→  dbt models
TomTom (traffic)   ───┘     (Docker)           (warehouse)     (transforms)
```

### Data Sources

| Source | Data | Frequency |
|--------|------|-----------|
| [ZTP Krakow](https://gtfs.ztp.krakow.pl) | Static GTFS (routes, stops, schedules) | Daily |
| [ZTP Krakow](https://gtfs.ztp.krakow.pl) | Real-time vehicle positions & trip updates (protobuf) | Every 1 min |
| [Open-Meteo](https://open-meteo.com) | Hourly weather for 18 Krakow districts | Periodic |
| [TomTom](https://developer.tomtom.com) | Traffic intensity at 150+ stops | Every 15 min (7am-8pm) |

### Tech Stack

- **Orchestration**: Apache Airflow 2.10.5 (CeleryExecutor)
- **Data Warehouse**: Snowflake
- **Transformation**: dbt-snowflake 1.9.4
- **Ingestion**: Python (requests, pandas, gtfs-realtime-bindings)
- **Infrastructure**: Docker Compose, Redis, PostgreSQL
- **CI/CD**: GitHub Actions
- **Analysis**: Jupyter Notebooks

## Project Structure

```
dags/                        # Airflow DAG definitions
  gtfs_download_static.py            # Daily static GTFS download & upload
  gtfs_download_realtime_cache.py    # Cache real-time protobuf data every minute
  gtfs_aggregate_realtime_upload.py  # Aggregate & upload cached data every 30 min
  traffic_intensity_ingestion.py     # TomTom traffic data ingestion
  openmeteo_weather_loader.py        # Weather data for Krakow districts
dbt/gtfs_project/            # dbt transformation project
  models/                        # SQL models (delays, schedules, weather joins)
  profiles.yml                   # Local Snowflake connection (not committed)
  test-profiles.yml              # CI/CD template
python_connectors/           # Standalone data loaders (outside Airflow)
dockerfiles/                 # Custom Airflow Docker image
notebooks/                   # Jupyter notebooks for EDA and analysis
tests/                       # pytest test suite
.github/workflows/           # CI (pytest on PR) and CD (manual deploy)
```

## Getting Started

### Prerequisites

- Docker & Docker Compose
- Snowflake account with `GTFS_UPLOADER_ROLE`
- TomTom API key (for traffic data)

### First-Time Setup

1. **Create environment file**:
   ```bash
   cp .env-sample .env
   # Edit .env - set POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB, AIRFLOW_FERNET_KEY
   ```

2. **Start Airflow and initialize the database**:
   ```bash
   docker compose up airflow-webserver -d
   docker compose run airflow-webserver airflow db init
   ```

3. **Create admin user**:
   ```bash
   docker compose run airflow-webserver airflow users create \
     --username admin \
     --password admin \
     --firstname Admin \
     --lastname User \
     --role Admin \
     --email admin@example.com
   ```

4. **Add Snowflake connection**:
   ```bash
   docker compose run airflow-webserver airflow connections add my_snowflake_conn \
     --conn-type snowflake \
     --conn-login "<your_username>" \
     --conn-password "<your_password>" \
     --conn-schema "SCHEDULE" \
     --conn-extra '{
       "account": "<your_snowflake_account>",
       "warehouse": "COMPUTE_WH",
       "database": "GTFS_TEST",
       "role": "GTFS_UPLOADER_ROLE",
       "insecure_mode": false
     }'
   ```

5. **Start all services**:
   ```bash
   docker compose up -d
   ```

6. **Access Airflow UI**: [http://localhost:8080](http://localhost:8080)

### dbt Setup

```bash
cd dbt/gtfs_project
cp test-profiles.yml profiles.yml
# Edit profiles.yml - replace placeholders with your Snowflake credentials
dbt run
```

### Daily Usage

```bash
docker compose up -d          # Start Airflow
docker compose down           # Stop Airflow
docker compose up -d --build  # Rebuild after Dockerfile changes
```

## Airflow DAGs

| DAG | Schedule | Description |
|-----|----------|-------------|
| `gtfs_download_static` | Daily | Downloads GTFS feeds (bus/tram/light rail), merges, uploads to Snowflake |
| `gtfs_download_realtime_cache` | Every 1 min | Fetches vehicle positions & trip updates, caches as parquet |
| `gtfs_aggregate_realtime_upload` | Every 30 min | Aggregates cached real-time data, uploads to Snowflake |
| `traffic_intensity_ingestion` | Every 15 min (7-20h) | Fetches traffic flow data from TomTom for 150+ stops |
| `openmeteo_weather_loader` | Periodic | Loads hourly weather for 18 Krakow districts |

## dbt Models

All models live in `dbt/gtfs_project/models/` and are materialized as full-refresh tables in the `SCHEDULE_DATA_MARTS` schema.

| Model | Description |
|-------|-------------|
| `aggregated_calendar_table` | Generates a date series from GTFS calendar data, applying day-of-week service rules and calendar_dates exceptions |
| `trips_schedule_table` | Joins stop_times, trips, stops, routes, and calendar into a single planned schedule table with geographic coordinates |
| `delays_table` | Compares real-time trip updates against the planned schedule to calculate delay in minutes per stop |
| `delays_table_vectors` | Creates stop-pair segments (departure stop -> arrival stop) with planned and actual timing for route-level analysis |
| `vehicle_position` | Matches real-time vehicle positions (lat/lon, speed, bearing) to delay vectors for spatial analysis |
| `stops_weather` | Joins each stop to its nearest Krakow district using geographic distance, then enriches with hourly weather data |

## Snowflake Schemas

| Schema | Content |
|--------|---------|
| `SCHEDULE` | Static GTFS data (routes, stops, trips, calendar, shapes, traffic_intensity) |
| `TRIP_UPDATES` | Real-time trip updates and vehicle positions |
| `WEATHER_API_STAGING` | Hourly weather and district locations |
| `SCHEDULE_DATA_MARTS` | dbt-transformed analytical models |

## Running Tests

```bash
pytest tests/
```

## CI/CD

- **Pull requests**: Automatically runs pytest on Ubuntu with Python 3.11
- **Deploy**: Manual trigger via GitHub Actions, uses self-hosted runner to rebuild Docker stack

## Contributing

1. Create a feature branch
2. Make changes and add tests
3. Open a pull request - CI will run pytest automatically
4. Commits in English
