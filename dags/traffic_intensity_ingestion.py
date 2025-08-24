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



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2023, 10, 1),
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

def fetch_tomtom_traffic_intensity(lat: float, lon: float, api_key: str) -> dict[str, Any]:  
    """
    Fetches traffic intensity from TomTom API for a given latitude and longitude. 
    
    This service provides information about the speeds and travel times of the road fragment closest to the given coordinates. 
    Refernce to documentation: https://developer.tomtom.com/traffic-api/documentation/traffic-flow/flow-segment-data

    Returns:
    dict[str, Any]: Dictionary with current speed, free flow speed, and confidence.

    """
    url = (
        f"https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json" 
        f"?point={lat},{lon}&unit=KMPH&key={api_key}"
    )
    
    response = None

    for i in range (3):  # Retry up to 3 times
        try:
            response = requests.get(url)
            response.raise_for_status()
            break # Exit loop if successful
        except Exception as e:
            if i < 2:  # If not the last attempt
                time.sleep(2 ** (i+1))  # Exponential backoff
            else:
                raise e
            
    data = response.json()
    
    current_speed = data['flowSegmentData']['currentSpeed'] # Current average speed in km/h on given road segment
    free_flow_speed = data['flowSegmentData']['freeFlowSpeed'] # Ideal speed in km/h on a given road segment
    current_travel_time = data['flowSegmentData']['currentTravelTime'] # Current travel time in seconds on a given road segment
    free_flow_travel_time = data['flowSegmentData']['freeFlowTravelTime'] # Ideal travel time in seconds on a given road segment
    confidence = data['flowSegmentData']['confidence'] # The confidence is a measure of the quality of the provided travel time and speed. A value ranges between 0 and 1 where 1 means full confidence, meaning that the response contains the highest quality data
    
    return {
        "current_speed": current_speed,
        "free_flow_speed": free_flow_speed,
        "current_travel_time": current_travel_time,
        "free_flow_travel_time": free_flow_travel_time,
        "confidence": confidence 
    }

def fetch_gtfs_stops_from_snowflake(logical_date: DateTime) -> list[dict[str, Any]]:
    query_date = logical_date.format("YYYY-MM-DD")
    query = f"""
        SELECT DISTINCT STOP_NAME, max(stop_lat) as stop_lat, min(stop_lon) as stop_lon
        FROM GTFS_TEST.SCHEDULE.STOPS
        WHERE to_date(load_timestamp) = '{query_date}'
        GROUP BY STOP_NAME
    """
    hook = SnowflakeHook(snowflake_conn_id='my_snowflake_conn')
    results = hook.get_records(query)

    return [
        {"STOP_NAME": row[0], "stop_lat": row[1], "stop_lon": row[2]}
        for row in results
    ]


def create_table_if_not_exists() -> None:
    hook = SnowflakeHook(snowflake_conn_id='my_snowflake_conn')
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
    if traffic_data:
        hook = SnowflakeHook(snowflake_conn_id='my_snowflake_conn')

        insert_sql = f"""
        INSERT INTO GTFS_TEST.SCHEDULE.TRAFFIC_INTENSITY (
            STOP_NAME, stop_lat, stop_lon, current_speed, free_flow_speed,
            current_travel_time, free_flow_travel_time, confidence, load_timestamp
        ) VALUES
        """

        values = [
            f"""(
                    '{row["STOP_NAME"]}',
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
        

def process_traffic_indensity(stops: list[dict[str, Any]], logical_date: DateTime) -> None:
    batch_size = 100
    total_stops = len(stops)
    num_batches = ceil(total_stops / batch_size)

    create_table_if_not_exists()

    for batch_num in range(num_batches):
        batch_stops = stops[batch_num * batch_size : (batch_num + 1) * batch_size]
        traffic_data = [
            {
                "STOP_NAME": stop["STOP_NAME"],
                "stop_lat": stop["stop_lat"],
                "stop_lon": stop["stop_lon"],
                **fetch_tomtom_traffic_intensity(
                    lat=stop["stop_lat"],
                    lon=stop["stop_lon"],
                    api_key=Variable.get("tomtom_api_key_secret")
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
        return fetch_gtfs_stops_from_snowflake(logical_date=logical_date)

    @task
    def ingest_traffic_intensity(stops: list[dict[str, Any]], logical_date: DateTime) -> None:
        return process_traffic_indensity(stops=stops, logical_date=logical_date)
        
    stops = fetch_gtfs_stops()
    ingest_traffic_intensity(stops=stops, logical_date="{{ logical_date }}")