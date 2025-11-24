import os
import datetime
import logging
import pandas as pd
import requests

from google.transit import gtfs_realtime_pb2
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from snowflake.connector.pandas_tools import write_pandas

# Destination schema for trip updates (historical table resides here)
SF_SCHEMA_TRIP_UPDATES = "TRIP_UPDATES"  # Under GTFS_TEST.TRIP_UPDATES

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2023, 10, 1),
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

def create_table_in_snowflake(ctx, table_name, df, schema):
    """
    Creates a table in Snowflake (in the given schema) based on the DataFrame's structure.
    """
    ddl = generate_create_table_ddl(table_name, df, schema)
    print(f"Creating table {schema}.{table_name.upper()} ...")
    cs_local = ctx.cursor()
    cs_local.execute(ddl)
    cs_local.close()
    print(f"Table {schema}.{table_name.upper()} created.")

def generate_create_table_ddl(table_name, df, schema):
    """
    Generates a CREATE OR REPLACE TABLE DDL command based on the DataFrame's structure.
    Mapping:
      - integer   -> NUMBER,
      - float     -> FLOAT,
      - datetime  -> TIMESTAMP_NTZ,
      - others    -> VARCHAR.
    Column names are converted to uppercase.
    Special case: the "load_timestamp" column is always mapped to TIMESTAMP_NTZ.
    """
    col_defs = []
    for col, dtype in df.dtypes.items():
        if col.lower() == "load_timestamp":
            sf_type = "TIMESTAMP_NTZ"
        else:
            dtype_str = str(dtype).lower()
            if "int" in dtype_str:
                sf_type = "NUMBER"
            elif "float" in dtype_str:
                sf_type = "FLOAT"
            elif "datetime" in dtype_str:
                sf_type = "TIMESTAMP_NTZ"
            else:
                sf_type = "VARCHAR"
        col_defs.append(f'"{col.upper()}" {sf_type}')
    ddl = (f"CREATE TABLE IF NOT EXISTS {schema}.{table_name.upper()} (\n  " +
           ",\n  ".join(col_defs) + "\n);")
    return ddl

def fetch_real_time_data(url):
    """
    Uses the requests library to fetch GTFS‑Realtime protobuf data from a URL.
    Returns the content if successful; otherwise, returns None.
    """
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            return response.content
        else:
            print(f"Failed to fetch data from {url}. Status code: {response.status_code}")
            return None
    except Exception as ex:
        print(f"Error fetching data from {url}: {ex}")
        return None

def parse_trip_updates(pb_data, load_timestamp):
    """
    Parses GTFS‑Realtime Trip Updates protobuf data and returns a list of dictionaries containing:
      trip_id, stop_id, stop_sequence, arrival, departure, schedule_relationship, load_timestamp.
    """
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(pb_data)
    updates = []
    for entity in feed.entity:
        if entity.HasField('trip_update'):
            tu = entity.trip_update
            trip_id = tu.trip.trip_id if tu.trip.HasField("trip_id") else None
            for stu in tu.stop_time_update:
                update_dict = {
                    'trip_id': trip_id,
                    'stop_id': stu.stop_id if stu.HasField("stop_id") else None,
                    'stop_sequence': stu.stop_sequence if stu.HasField("stop_sequence") else None,
                    'arrival': stu.arrival.time if stu.HasField("arrival") else None,
                    'departure': stu.departure.time if stu.HasField("departure") else None,
                    'schedule_relationship': stu.schedule_relationship if stu.HasField("schedule_relationship") else None,
                    'load_timestamp': load_timestamp
                }
                updates.append(update_dict)
    return updates

def parse_vehicle_positions(pb_data, load_timestamp):
    """
    Parsuje GTFS‑Realtime Vehicle Positions protobuf i zwraca listę słowników
    z polami: bearing, congestion_level, current_status, current_stop_sequence,
    direction_id, id, label, latitude, license_plate, longitude,
    multi_carriage_details, occupancy_percentage, occupancy_status,
    odometer, position, route_id, schedule_relationship, speed,
    start_date, start_time, stop_id, timestamp, trip, trip_id, vehicle,
    oraz load_timestamp i mode.
    """
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(pb_data)
    positions = []
    for entity in feed.entity:
        if entity.HasField("vehicle"):
            v = entity.vehicle
            pos = v.position if v.HasField("position") else None
            record = {
                'bearing': pos.bearing if pos and pos.HasField("bearing") else None,
                'congestion_level': v.congestion_level if v.HasField("congestion_level") else None,
                'current_status': v.current_status if v.HasField("current_status") else None,
                'current_stop_sequence': v.current_stop_sequence if v.HasField("current_stop_sequence") else None,
                'direction_id': v.trip.direction_id if v.HasField("trip") and v.trip.HasField("direction_id") else None,
                'id': v.vehicle.id if v.HasField("vehicle") and v.vehicle.HasField("id") else None,
                'label': v.vehicle.label if v.HasField("vehicle") and v.vehicle.HasField("label") else None,
                'latitude': pos.latitude if pos and pos.HasField("latitude") else None,
                'license_plate': v.vehicle.license_plate if v.HasField("vehicle") and v.vehicle.HasField("license_plate") else None,
                'longitude': pos.longitude if pos and pos.HasField("longitude") else None,
                'multi_carriage_details': None,  # złożone pole, można rozwinąć jeśli potrzebne
                'occupancy_percentage': v.occupancy_percentage if v.HasField("occupancy_percentage") else None,
                'occupancy_status': v.occupancy_status if v.HasField("occupancy_status") else None,
                'odometer': pos.odometer if pos and pos.HasField("odometer") else None,
                'position': None,  # całe pole position jako obiekt – zwykle nie zapisujemy
                'route_id': v.trip.route_id if v.HasField("trip") and v.trip.HasField("route_id") else None,
                'schedule_relationship': v.trip.schedule_relationship if v.HasField("trip") and v.trip.HasField("schedule_relationship") else None,
                'speed': pos.speed if pos and pos.HasField("speed") else None,
                'start_date': v.trip.start_date if v.HasField("trip") and v.trip.HasField("start_date") else None,
                'start_time': v.trip.start_time if v.HasField("trip") and v.trip.HasField("start_time") else None,
                'stop_id': v.stop_id if v.HasField("stop_id") else None,
                'timestamp': v.timestamp if v.HasField("timestamp") else None,
                'trip': None,  # całe pole trip jako obiekt – zwykle nie zapisujemy
                'trip_id': v.trip.trip_id if v.HasField("trip") and v.trip.HasField("trip_id") else None,
                'vehicle': None,  # całe pole vehicle jako obiekt – zwykle nie zapisujemy
                'load_timestamp': load_timestamp
            }
            positions.append(record)
    return positions

def save_trip_updates_to_snowflake(ctx, updates):
    """
    Creates (if necessary) and appends trip updates to the historical TRIP_UPDATES table.
    """
    df = pd.DataFrame(updates).reset_index(drop=True)
    if df.empty:
        print("No trip updates data available to upload.")
        return

    # Convert column names to uppercase.
    df = df.rename(columns=lambda x: x.upper())
    table_name = "TRIP_UPDATES"
    
    # Create the historical table if it doesn't exist.
    create_table_in_snowflake(ctx, table_name, df, SF_SCHEMA_TRIP_UPDATES)
    
    cs = ctx.cursor()
    cs.execute(f"USE SCHEMA {SF_SCHEMA_TRIP_UPDATES}")
    
    # Appending new rows for historical SCD.
    print(f"Uploading trip updates data to table {SF_SCHEMA_TRIP_UPDATES}.{table_name} ...")
    success, nchunks, nrows, _ = write_pandas(
        ctx, df, table_name, auto_create_table=False, use_logical_type=True
    )
    if success:
        print(f"Trip updates data uploaded successfully: {nrows} rows in {nchunks} chunks.\n")
    else:
        print(f"Failed to upload trip updates data to table {table_name}.\n")
    cs.close()

def save_vehicle_positions_to_snowflake(ctx, positions):
    df = pd.DataFrame(positions).reset_index(drop=True)
    if df.empty:
        print("No vehicle positions data available to upload.")
        return

    df = df.rename(columns=lambda x: x.upper())
    table_name = "VEHICLE_POSITIONS"

    create_table_in_snowflake(ctx, table_name, df, SF_SCHEMA_TRIP_UPDATES)

    cs = ctx.cursor()
    cs.execute(f"USE SCHEMA {SF_SCHEMA_TRIP_UPDATES}")

    print(f"Uploading vehicle positions data to table {SF_SCHEMA_TRIP_UPDATES}.{table_name} ...")
    success, nchunks, nrows, _ = write_pandas(
        ctx, df, table_name, auto_create_table=False, use_logical_type=True
    )
    if success:
        print(f"Vehicle positions uploaded successfully: {nrows} rows in {nchunks} chunks.\n")
    else:
        print(f"Failed to upload vehicle positions data.\n")
    cs.close()


def ingest_trip_updates_to_snowflake(**kwargs):
    """
    Downloads and processes GTFS‑Realtime trip updates from various sources,
    then appends the data to the historical table.
    """
    # Use the Airflow logical_date if available, otherwise fall back to current time.
    load_timestamp = pd.to_datetime(kwargs.get('logical_date', datetime.datetime.utcnow())).replace(tzinfo=None)
    
    # List of trip updates sources as tuples: (mode, URL)
    trip_updates_sources = [
        ("T", "https://gtfs.ztp.krakow.pl/TripUpdates_T.pb"),
        ("A", "https://gtfs.ztp.krakow.pl/TripUpdates_A.pb"),
        ("M", "https://gtfs.ztp.krakow.pl/TripUpdates_M.pb"),
        ("TR", "https://gtfs.ztp.krakow.pl/TripUpdates.pb")
    ]
    
    all_trip_updates = []
    for mode, url in trip_updates_sources:
        print(f"Fetching trip updates from {url} for mode {mode} ...")
        pb_data = fetch_real_time_data(url)
        if pb_data:
            updates = parse_trip_updates(pb_data, load_timestamp)
            # Add mode identifier to each record.
            for u in updates:
                u['mode'] = mode
            print(f"Parsed {len(updates)} trip updates records from mode {mode}.")
            all_trip_updates.extend(updates)
        else:
            print(f"No data fetched from {url} for mode {mode}.")
    
    if all_trip_updates:
        hook = SnowflakeHook(snowflake_conn_id='my_snowflake_conn')
        ctx = hook.get_conn()
        save_trip_updates_to_snowflake(ctx, all_trip_updates)
    else:
        print("No trip updates data available from any source.")

def ingest_vehicle_positions_to_snowflake(**kwargs):
    load_timestamp = pd.to_datetime(kwargs.get('logical_date', datetime.datetime.utcnow())).replace(tzinfo=None)

    vehicle_positions_sources = [
        ("T", "https://gtfs.ztp.krakow.pl/VehiclePositions_T.pb"),
        ("A", "https://gtfs.ztp.krakow.pl/VehiclePositions_A.pb"),
        ("M", "https://gtfs.ztp.krakow.pl/VehiclePositions_M.pb"),
        ("TR", "https://gtfs.ztp.krakow.pl/VehiclePositions.pb")
    ]

    all_positions = []
    for mode, url in vehicle_positions_sources:
        print(f"Fetching vehicle positions from {url} for mode {mode} ...")
        pb_data = fetch_real_time_data(url)
        if pb_data:
            positions = parse_vehicle_positions(pb_data, load_timestamp)
            for p in positions:
                p['mode'] = mode
                p['load_timestamp'] = load_timestamp
            print(f"Parsed {len(positions)} vehicle positions records from mode {mode}.")
            all_positions.extend(positions)
        else:
            print(f"No data fetched from {url} for mode {mode}.")

    if all_positions:
        hook = SnowflakeHook(snowflake_conn_id='my_snowflake_conn')
        ctx = hook.get_conn()
        save_vehicle_positions_to_snowflake(ctx, all_positions)
    else:
        print("No vehicle positions data available from any source.")

with DAG(
    dag_id='gtfs_load_gtfs_realtime',
    default_args=default_args,
    description='Load GTFS‑Realtime trip updates and vehicle positions',
    schedule_interval="*/5 * * * *",  # Run every 5 minutes
    catchup=False,
) as dag:
    
    load_trip_updates_task = PythonOperator(
        task_id='load_trip_updates_data',
        python_callable=ingest_trip_updates_to_snowflake,
        provide_context=True
    )

    load_vehicle_positions_task = PythonOperator(
        task_id='load_vehicle_positions_data',
        python_callable=ingest_vehicle_positions_to_snowflake,
        provide_context=True
    )
    
    [load_trip_updates_task, load_vehicle_positions_task]
