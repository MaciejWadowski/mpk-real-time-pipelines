from typing import Any
import time
import requests
import datetime
from pendulum import DateTime
from airflow import DAG
from airflow.decorators import task  # Import the task decorator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.models import Variable
from math import ceil

SNOWFLAKE_CONN_ID = "my_snowflake_conn"
TOMTOM_API_KEY_VAR = "tomtom_api_key_secret"
BATCH_SIZE = 100

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2023, 10, 1),
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

def fetch_tomtom_traffic_intensity(lat: float, lon: float, api_key: str) -> dict[str, Any]:
    """
    Fetches traffic intensity from TomTom API for a given latitude and longitude. Retries up to 3 times on failure.
    """
    url = (
        f"https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json"
        f"?point={lat},{lon}&unit=KMPH&key={api_key}"
    )
    for i in range(3):
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            return {
                "current_speed": data['flowSegmentData']['currentSpeed'],
                "free_flow_speed": data['flowSegmentData']['freeFlowSpeed'],
                "current_travel_time": data['flowSegmentData']['currentTravelTime'],
                "free_flow_travel_time": data['flowSegmentData']['freeFlowTravelTime'],
                "confidence": data['flowSegmentData']['confidence']
            }
        except Exception as e:
            if i < 2:
                time.sleep(2 ** (i + 1))
            else:
                print(f"Failed to fetch TomTom data for ({lat}, {lon}): {e}")
                raise

def get_gtfs_stops_from_snowflake(logical_date: DateTime) -> list[dict[str, Any]]:
    query_date = logical_date.format("YYYY-MM-DD")
    query = f"""
        SELECT DISTINCT STOP_NAME, max(stop_lat) as stop_lat, min(stop_lon) as stop_lon
        FROM GTFS_TEST.SCHEDULE.STOPS
        WHERE to_date(load_timestamp) = '{query_date}'
        GROUP BY STOP_NAME
    """
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    results = hook.get_records(query)
    return [
        {"STOP_NAME": row[0], "stop_lat": row[1], "stop_lon": row[2]}
        for row in results
    ]

def create_traffic_intensity_table_if_not_exists() -> None:
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    table = "GTFS_TEST.SCHEDULE.TRAFFIC_INTENSITY"
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {table} (
        STOP_NAME STRING,
        stop_lat FLOAT,
        stop_lon FLOAT,
        current_speed FLOAT,
        free_flow_speed FLOAT,
        current_travel_time FLOAT,
        free_flow_travel_time FLOAT,
        confidence FLOAT,
        load_timestamp DATE
    )
    """
    hook.run(create_table_sql)

def insert_traffic_data_to_snowflake(traffic_data: list[dict[str, Any]]) -> None:
    if not traffic_data:
        return
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    insert_sql = """
    INSERT INTO GTFS_TEST.SCHEDULE.TRAFFIC_INTENSITY (
        STOP_NAME, stop_lat, stop_lon, current_speed, free_flow_speed,
        current_travel_time, free_flow_travel_time, confidence, load_timestamp
    ) VALUES
    """
    values = [
        f"""(
            '{row["STOP_NAME"].replace("'", "''")}',
            {row["stop_lat"]},
            {row["stop_lon"]},
            {row["current_speed"]},
            {row["free_flow_speed"]},
            {row["current_travel_time"]},
            {row["free_flow_travel_time"]},
            {row["confidence"]},
            '{row["load_timestamp"]}'
        )"""
        for row in traffic_data
    ]
    insert_sql += ",\n".join(values)
    hook.run(insert_sql)

def process_traffic_intensity(stops: list[dict[str, Any]], logical_date: DateTime) -> None:
    """
    Processes stops in batches, fetches traffic intensity, and inserts into Snowflake.
    """
    total_stops = len(stops)
    num_batches = ceil(total_stops / BATCH_SIZE)
    create_traffic_intensity_table_if_not_exists()
    api_key = Variable.get(TOMTOM_API_KEY_VAR)
    for batch_num in range(num_batches):
        batch_stops = stops[batch_num * BATCH_SIZE : (batch_num + 1) * BATCH_SIZE]
        traffic_data = [
            {
                "STOP_NAME": stop["STOP_NAME"],
                "stop_lat": stop["stop_lat"],
                "stop_lon": stop["stop_lon"],
                **fetch_tomtom_traffic_intensity(
                    lat=stop["stop_lat"],
                    lon=stop["stop_lon"],
                    api_key=api_key
                ),
                "load_timestamp": logical_date.format("YYYY-MM-DD")
            }
            for stop in batch_stops
        ]
        insert_traffic_data_to_snowflake(traffic_data)
        print(f"Inserted batch {batch_num + 1}/{num_batches} ({len(batch_stops)} stops)")


with DAG(
    dag_id='traffic_intensity_ingestion',
    default_args=default_args,
    description='Ingest traffic intensity data from TomTom API',
    schedule_interval="@daily",
    catchup=False,
    params={"example_param": "default_value"}
) as dag:
    
    @task
    def fetch_gtfs_stops(logical_date: DateTime) -> list[dict[str, Any]]:
        return get_gtfs_stops_from_snowflake(logical_date=logical_date)

    @task
    def ingest_traffic_intensity(stops: list[dict[str, Any]], logical_date: DateTime) -> None:
        return process_traffic_intensity(stops=stops, logical_date=logical_date)
        
    stops = fetch_gtfs_stops()
    ingest_traffic_intensity(stops=stops, logical_date="{{ logical_date }}")