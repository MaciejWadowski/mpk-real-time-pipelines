from typing import Any
import datetime
from pendulum import DateTime
from airflow import DAG
from airflow.decorators import task  # Import the task decorator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.models import Variable
from math import ceil
from traffic_intensity import *

SNOWFLAKE_CONN_ID = "my_snowflake_conn"
TOMTOM_API_KEY_VAR = "tom_tom_api_keys"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2023, 10, 1),
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

with DAG(
    dag_id='traffic_intensity_ingestion',
    default_args=default_args,
    description='Ingest traffic intensity data from TomTom API',
    schedule_interval=" */15 7-20 * * *",
    catchup=False,
    params={"example_param": "default_value"}
) as dag:
    
    @task
    def fetch_gtfs_stops(logical_date: DateTime) -> list[dict[str, Any]]:
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        return get_gtfs_stops_from_snowflake(logical_date=logical_date, hook=hook)

    @task
    def ingest_traffic_intensity(stops: list[dict[str, Any]], logical_date: DateTime) -> None:
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        api_keys = Variable.get(TOMTOM_API_KEY_VAR, deserialize_json=True)
        return process_traffic_intensity(stops=stops, logical_date=logical_date, hook=hook, api_keys=api_keys)
        
    stops = fetch_gtfs_stops()
    ingest_traffic_intensity(stops=stops, logical_date="{{ logical_date }}")