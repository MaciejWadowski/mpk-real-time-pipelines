import datetime
from typing import Any
from pendulum import DateTime
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from traffic_intensity import process_traffic_intensity, save_traffic_data_to_disk

TOMTOM_API_KEY_VAR = "tom_tom_api_keys"
TRAFFIC_DATA_DIR = "/tmp/traffic_intensity_data"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2023, 10, 1),
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

with DAG(
    dag_id='traffic_intensity_fetch',
    default_args=default_args,
    description='Fetch TomTom traffic intensity every 15 minutes and save to disk',
    schedule_interval="*/15 7-20 * * *",
    catchup=False,
) as dag:

    @task
    def fetch_and_save_traffic_intensity(logical_date: str | DateTime) -> str:
        if isinstance(logical_date, str):
            logical_date = DateTime.parse(logical_date)
        api_keys = Variable.get(TOMTOM_API_KEY_VAR, deserialize_json=True)
        traffic_data = process_traffic_intensity(logical_date=logical_date, api_keys=api_keys)
        file_path = save_traffic_data_to_disk(traffic_data, logical_date, TRAFFIC_DATA_DIR)
        return file_path

    fetch_and_save_traffic_intensity(logical_date="{{ logical_date }}")
