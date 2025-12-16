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
    'retries': 1,
    'retry_delay': datetime.timedelta(seconds=20),
}

def setup_cache_directory():
    """Create and setup cache directory if it doesn't exist"""
    os.makedirs(LOCAL_CACHE_DIR, exist_ok=True)
    os.makedirs(os.path.join(LOCAL_CACHE_DIR, "trip_updates"), exist_ok=True)
    os.makedirs(os.path.join(LOCAL_CACHE_DIR, "vehicle_positions"), exist_ok=True)

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