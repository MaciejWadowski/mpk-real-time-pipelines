import os
import datetime
import logging
import pandas as pd
import requests
import tempfile
import shutil
import time
from pathlib import Path

from google.transit import gtfs_realtime_pb2
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from snowflake.connector.pandas_tools import write_pandas

# Configuration variables
DOWNLOAD_INTERVAL_MINUTES = 1  # Download every 1 minute
AGGREGATION_INTERVAL_MINUTES = 30  # Upload aggregated batch every 30 minutes

# Destination schema for trip updates (historical table resides here)
SF_SCHEMA_TRIP_UPDATES = "TRIP_UPDATES"
LOCAL_CACHE_DIR = "/tmp/gtfs_cache"  # Local directory for caching data

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2023, 10, 1),
    'retries': 0,
}

def setup_cache_directory():
    """Create and setup cache directory if it doesn't exist"""
    os.makedirs(LOCAL_CACHE_DIR, exist_ok=True)
    os.makedirs(os.path.join(LOCAL_CACHE_DIR, "trip_updates"), exist_ok=True)
    os.makedirs(os.path.join(LOCAL_CACHE_DIR, "vehicle_positions"), exist_ok=True)

def clean_old_cache_files(directory, retention_minutes=60):
    """Remove cache files older than retention_minutes"""
    current_time = time.time()
    cutoff_time = current_time - (retention_minutes * 60)
    
    for filename in os.listdir(directory):
        filepath = os.path.join(directory, filename)
        if os.path.isfile(filepath):
            if os.path.getmtime(filepath) < cutoff_time:
                os.remove(filepath)

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
    Parses GTFS‑Realtime Vehicle Positions protobuf and returns list of dictionaries
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
                'multi_carriage_details': None,
                'occupancy_percentage': v.occupancy_percentage if v.HasField("occupancy_percentage") else None,
                'occupancy_status': v.occupancy_status if v.HasField("occupancy_status") else None,
                'odometer': pos.odometer if pos and pos.HasField("odometer") else None,
                'position': None,
                'route_id': v.trip.route_id if v.HasField("trip") and v.trip.HasField("route_id") else None,
                'schedule_relationship': v.trip.schedule_relationship if v.HasField("trip") and v.trip.HasField("schedule_relationship") else None,
                'speed': pos.speed if pos and pos.HasField("speed") else None,
                'start_date': v.trip.start_date if v.HasField("trip") and v.trip.HasField("start_date") else None,
                'start_time': v.trip.start_time if v.HasField("trip") and v.trip.HasField("start_time") else None,
                'stop_id': v.stop_id if v.HasField("stop_id") else None,
                'timestamp': v.timestamp if v.HasField("timestamp") else None,
                'trip': None,
                'trip_id': v.trip.trip_id if v.HasField("trip") and v.trip.HasField("trip_id") else None,
                'vehicle': None,
                'load_timestamp': load_timestamp
            }
            positions.append(record)
    return positions

def download_and_cache_data(**kwargs):
    """
    Downloads GTFS data every minute and saves it to local cache files
    """
    setup_cache_directory()
    
    # Use the Airflow logical_date if available, otherwise fall back to current time
    load_timestamp = pd.to_datetime(kwargs.get('logical_date', datetime.datetime.utcnow())).replace(tzinfo=None)
    timestamp_str = load_timestamp.strftime("%Y%m%d_%H%M%S")
    
    # Trip updates sources
    trip_updates_sources = [
        ("T", "https://gtfs.ztp.krakow.pl/TripUpdates_T.pb"),
        ("A", "https://gtfs.ztp.krakow.pl/TripUpdates_A.pb"),
        ("M", "https://gtfs.ztp.krakow.pl/TripUpdates_M.pb")
    ]
    
    # Vehicle positions sources
    vehicle_positions_sources = [
        ("T", "https://gtfs.ztp.krakow.pl/VehiclePositions_T.pb"),
        ("A", "https://gtfs.ztp.krakow.pl/VehiclePositions_A.pb"),
        ("M", "https://gtfs.ztp.krakow.pl/VehiclePositions_M.pb")
    ]
    
    # Process and cache trip updates
    all_trip_updates = []
    for mode, url in trip_updates_sources:
        print(f"Fetching trip updates from {url} for mode {mode} ...")
        pb_data = fetch_real_time_data(url)
        if pb_data:
            updates = parse_trip_updates(pb_data, load_timestamp)
            for u in updates:
                u['mode'] = mode
            print(f"Parsed {len(updates)} trip updates records from mode {mode}.")
            all_trip_updates.extend(updates)
    
    if all_trip_updates:
        # Save to cache file
        cache_file = os.path.join(LOCAL_CACHE_DIR, "trip_updates", f"trip_updates_{timestamp_str}.parquet")
        df = pd.DataFrame(all_trip_updates)
        df.to_parquet(cache_file, index=False)
        print(f"Cached {len(all_trip_updates)} trip updates to {cache_file}")
    else:
        print("No trip updates data available to cache.")
    
    # Process and cache vehicle positions
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
    
    if all_positions:
        # Save to cache file
        cache_file = os.path.join(LOCAL_CACHE_DIR, "vehicle_positions", f"vehicle_positions_{timestamp_str}.parquet")
        df = pd.DataFrame(all_positions)
        df.to_parquet(cache_file, index=False)
        print(f"Cached {len(all_positions)} vehicle positions to {cache_file}")
    else:
        print("No vehicle positions data available to cache.")
    
    # Clean old cache files (keep last 60 minutes as buffer)
    clean_old_cache_files(os.path.join(LOCAL_CACHE_DIR, "trip_updates"), retention_minutes=60)
    clean_old_cache_files(os.path.join(LOCAL_CACHE_DIR, "vehicle_positions"), retention_minutes=60)

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

def aggregate_and_upload_to_snowflake(**kwargs):
    """
    Aggregates cached data from the last X minutes and uploads to Snowflake
    """
    setup_cache_directory()
    
    # Calculate time window for aggregation
    current_time = pd.to_datetime(kwargs.get('logical_date', datetime.datetime.utcnow())).replace(tzinfo=None)
    window_start = current_time - datetime.timedelta(minutes=AGGREGATION_INTERVAL_MINUTES)
    
    print(f"Aggregating data from {window_start} to {current_time}")
    
    hook = SnowflakeHook(snowflake_conn_id='my_snowflake_conn')
    ctx = hook.get_conn()
    
    # Process trip updates
    trip_updates_files = sorted(
        Path(os.path.join(LOCAL_CACHE_DIR, "trip_updates")).glob("*.parquet")
    )
    
    trip_updates_data = []
    uploaded_trip_files = []
    
    for file_path in trip_updates_files:
        file_time_str = file_path.stem.replace("trip_updates_", "")
        try:
            file_time = datetime.datetime.strptime(file_time_str, "%Y%m%d_%H%M%S")
            if window_start <= file_time <= current_time:
                df = pd.read_parquet(file_path)
                trip_updates_data.append(df)
                uploaded_trip_files.append(file_path)
                print(f"Included trip updates from {file_path.name}")
        except Exception as e:
            print(f"Error processing file {file_path}: {e}")
    
    if trip_updates_data:
        combined_trip_df = pd.concat(trip_updates_data, ignore_index=True)
        print(f"Aggregated {len(combined_trip_df)} trip updates records")
        
        # Upload to Snowflake
        combined_trip_df = combined_trip_df.rename(columns=lambda x: x.upper())
        table_name = "TRIP_UPDATES"
        
        create_table_in_snowflake(ctx, table_name, combined_trip_df, SF_SCHEMA_TRIP_UPDATES)
        
        cs = ctx.cursor()
        cs.execute(f"USE SCHEMA {SF_SCHEMA_TRIP_UPDATES}")
        
        print(f"Uploading aggregated trip updates to {SF_SCHEMA_TRIP_UPDATES}.{table_name} ...")
        success, nchunks, nrows, _ = write_pandas(
            ctx, combined_trip_df, table_name, auto_create_table=False, use_logical_type=True
        )
        if success:
            print(f"Trip updates uploaded successfully: {nrows} rows in {nchunks} chunks.")
            
            # Remove uploaded files
            for file_path in uploaded_trip_files:
                try:
                    os.remove(file_path)
                    print(f"Removed cached file: {file_path}")
                except Exception as e:
                    print(f"Error removing file {file_path}: {e}")
        else:
            print(f"Failed to upload trip updates data.")
        cs.close()
    else:
        print("No trip updates data to aggregate for this window.")
    
    # Process vehicle positions
    vehicle_position_files = sorted(
        Path(os.path.join(LOCAL_CACHE_DIR, "vehicle_positions")).glob("*.parquet")
    )
    
    vehicle_data = []
    uploaded_vehicle_files = []
    
    for file_path in vehicle_position_files:
        file_time_str = file_path.stem.replace("vehicle_positions_", "")
        try:
            file_time = datetime.datetime.strptime(file_time_str, "%Y%m%d_%H%M%S")
            if window_start <= file_time <= current_time:
                df = pd.read_parquet(file_path)
                vehicle_data.append(df)
                uploaded_vehicle_files.append(file_path)
                print(f"Included vehicle positions from {file_path.name}")
        except Exception as e:
            print(f"Error processing file {file_path}: {e}")
    
    if vehicle_data:
        combined_vehicle_df = pd.concat(vehicle_data, ignore_index=True)
        print(f"Aggregated {len(combined_vehicle_df)} vehicle positions records")
        
        # Upload to Snowflake
        combined_vehicle_df = combined_vehicle_df.rename(columns=lambda x: x.upper())
        table_name = "VEHICLE_POSITIONS"
        
        create_table_in_snowflake(ctx, table_name, combined_vehicle_df, SF_SCHEMA_TRIP_UPDATES)
        
        cs = ctx.cursor()
        cs.execute(f"USE SCHEMA {SF_SCHEMA_TRIP_UPDATES}")
        
        print(f"Uploading aggregated vehicle positions to {SF_SCHEMA_TRIP_UPDATES}.{table_name} ...")
        success, nchunks, nrows, _ = write_pandas(
            ctx, combined_vehicle_df, table_name, auto_create_table=False, use_logical_type=True
        )
        if success:
            print(f"Vehicle positions uploaded successfully: {nrows} rows in {nchunks} chunks.")
            
            # Remove uploaded files
            for file_path in uploaded_vehicle_files:
                try:
                    os.remove(file_path)
                    print(f"Removed cached file: {file_path}")
                except Exception as e:
                    print(f"Error removing file {file_path}: {e}")
        else:
            print(f"Failed to upload vehicle positions data.")
        cs.close()
    else:
        print("No vehicle positions data to aggregate for this window.")

# Create two separate DAGs with different schedules

# DAG for frequent downloads (every 1 minute)
with DAG(
    dag_id='gtfs_download_realtime_cache',
    default_args=default_args,
    description='Download GTFS data every minute and cache locally',
    schedule_interval=f"*/{DOWNLOAD_INTERVAL_MINUTES} * * * *",  # Every X minutes
    catchup=False,
) as download_dag:
    
    download_task = PythonOperator(
        task_id='download_and_cache_gtfs_data',
        python_callable=download_and_cache_data,
        provide_context=True
    )

# DAG for periodic aggregation and upload (every 30 minutes)
with DAG(
    dag_id='gtfs_aggregate_realtime_upload',
    default_args=default_args,
    description='Aggregate cached GTFS data and upload to Snowflake periodically',
    schedule_interval=f"*/{AGGREGATION_INTERVAL_MINUTES} * * * *",  # Every X minutes
    catchup=False,
) as upload_dag:
    
    upload_task = PythonOperator(
        task_id='aggregate_and_upload_to_snowflake',
        python_callable=aggregate_and_upload_to_snowflake,
        provide_context=True
    )