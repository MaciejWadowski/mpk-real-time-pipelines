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