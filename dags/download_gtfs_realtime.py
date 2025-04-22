import os
import datetime
import requests
import logging

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from snowflake.connector.pandas_tools import write_pandas

from download_gtfs_static import create_table_in_snowflake

# default DAG settings
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2023, 10, 1),
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=1),
}

logger = logging.getLogger(__file__)

# Create Kafka producer# download GTFS-Realtime data and save it to file
def fetch_real_time_data(url):
    """
    Downloads GTFS‑Realtime data (protobuf) from the provided URL.
    Returns the raw bytes if successful.
    """
    print(f"Downloading real‑time data from: {url}")
    response = requests.get(url)
    if response.status_code == 200:
        print("Real‑time data downloaded successfully.")
        return response.content
    else:
        print(f"Failed to download real‑time data. Status code: {response.status_code}")
        return None


def parse_vehicle_positions(pb_data, logical_date):
    """
    Parses GTFS‑Realtime protobuf data and returns a list of dictionaries containing:
    trip_id, vehicle_id, latitude, longitude, and timestamp.
    """
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(pb_data)
    vehicles = []
    for entity in feed.entity:
        if entity.HasField('vehicle'):
            vehicle = entity.vehicle
            if vehicle.HasField('trip'):
                vehicles.append({
                    'trip_id': vehicle.trip.trip_id,
                    'vehicle_id': vehicle.vehicle.id,
                    'latitude': vehicle.position.latitude,
                    'longitude': vehicle.position.longitude,
                    'timestamp': vehicle.timestamp,
                    'load_timestamp': logical_date
                })
    return vehicles


def save_realtime_data_to_snowflake(ctx, vehicles):
    """
    Creates the VEHICLE_POSITIONS table in the REALTIME schema and uploads real‑time data.
    Before uploading, truncates the target table.
    """
    cs = ctx.cursor()
    df = pd.DataFrame(vehicles).reset_index(drop=True)
    if df.empty:
        print("No real‑time data available to upload.")
        return
    # Convert dataframe column names to uppercase.
    df = df.rename(columns=lambda x: x.upper())
    table_name = "VEHICLE_POSITIONS"
    create_table_in_snowflake(ctx, table_name, df, "REALTIME")
    cs.execute(f"USE SCHEMA REALTIME")
    cs.execute(f"TRUNCATE TABLE REALTIME.{table_name}")
    print(f"Uploading real‑time data to table REALTIME.{table_name} ...")
    success, nchunks, nrows, _ = write_pandas(ctx, df, table_name, auto_create_table=False)
    if success:
        print(f"Real‑time data uploaded successfully: {nrows} rows in {nchunks} chunks.\n")
    else:
        print(f"Failed to upload real‑time data to table {table_name}.\n")


def fetch_gtfs_realtime(**kwargs):
    # PART 2: PROCESS GTFS‑Realtime VEHICLE DATA
    print("\n=== Processing GTFS‑Realtime vehicle data ===")
    realtime_sources = [
        ("T", "https://gtfs.ztp.krakow.pl/VehiclePositions_T.pb"),
        ("A", "https://gtfs.ztp.krakow.pl/VehiclePositions_A.pb"),
        ("M", "https://gtfs.ztp.krakow.pl/VehiclePositions_M.pb"),
        ("TR", "https://gtfs.ztp.krakow.pl/VehiclePositions.pb")
    ]

    all_vehicles = []
    for mode, url in realtime_sources:
        pb_data = fetch_real_time_data(url)
        if pb_data:
            vehicles = parse_vehicle_positions(pb_data, kwargs['logical_date'])
            # Add mode identifier to each record.
            for v in vehicles:
                v['mode'] = mode
            print(f"Parsed {len(vehicles)} real‑time vehicle records from mode {mode}.")
            all_vehicles.extend(vehicles)

    if all_vehicles:
        hook = SnowflakeHook(snowflake_conn_id='my_snowflake_conn')
        ctx = hook.get_conn()
        save_realtime_data_to_snowflake(ctx, all_vehicles)
    else:
        print("No real‑time vehicle data available from any source.")


# DAG definition with 5 mintues interval
with DAG(
        dag_id="gtfs_realtime_fetch_dag",
        default_args=default_args,
        description="Task downloading GTFS-Realtime data",
        schedule_interval="*/5 * * * *",  # 5 minutes
        catchup=False,
) as dag:
    fetch_realtime_task = PythonOperator(
        task_id="fetch_gtfs_realtime",
        python_callable=fetch_gtfs_realtime,
    )

    fetch_realtime_task
