import datetime
from typing import Any
from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from traffic_intensity import (
    load_traffic_data_from_disk,
    insert_traffic_data_to_snowflake,
    purge_traffic_data_from_disk,
)

SNOWFLAKE_CONN_ID = "my_snowflake_conn"
TRAFFIC_DATA_DIR = "/tmp/traffic_intensity_data"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2023, 10, 1),
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

with DAG(
    dag_id='traffic_intensity_ingest',
    default_args=default_args,
    description='Read traffic intensity data from disk and ingest to Snowflake',
    schedule_interval="0 * * * *",  # Hourly
    catchup=False,
) as dag:

    @task
    def ingest_and_purge_traffic_intensity() -> None:
        traffic_data = load_traffic_data_from_disk(TRAFFIC_DATA_DIR)
        
        if not traffic_data:
            return None
        
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        insert_traffic_data_to_snowflake(traffic_data, hook)
        purge_traffic_data_from_disk(TRAFFIC_DATA_DIR)

    ingest_and_purge_traffic_intensity()
