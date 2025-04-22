import os
import datetime
import math
import pandas as pd
import numpy as np
import requests
import gtfs_kit as gk
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from google.transit import gtfs_realtime_pb2

# -------------------------------------------------------------------
# SNOWFLAKE CONNECTION CONFIGURATION
# -------------------------------------------------------------------
# Use the following connection parameters.
SF_USER = 'GTFS_UPLOADER'
SF_PASSWORD = 'PSWD124'
SF_ACCOUNT = 'FIJHPBP-OQ13375'
SF_DATABASE = 'GTFS'
SF_SCHEMA_STATIC = 'SCHEDULE'       # For static (schedule) data
SF_SCHEMA_REALTIME = 'REALTIME'      # For real‑time vehicle data
SF_SCHEMA_TRIP_UPDATES = 'TRIP_UPDATES'  # For trip updates data
SF_WAREHOUSE = 'COMPUTE_WH'

# Establish connection to Snowflake
print("Connecting to Snowflake...")
ctx = snowflake.connector.connect(
    user=SF_USER,
    password=SF_PASSWORD,
    account=SF_ACCOUNT,
    warehouse=SF_WAREHOUSE,
    database=SF_DATABASE,
    schema=SF_SCHEMA_STATIC  # initially set to SCHEDULE schema (static data)
)
cs = ctx.cursor()
print("Connected to Snowflake.")

# Set role first, then set the current database.
cs.execute("USE ROLE GTFS_UPLOADER_ROLE")
cs.execute(f"USE DATABASE {SF_DATABASE}")
print(f"Current database set to {SF_DATABASE} and role set to GTFS_UPLOADER_ROLE.\n")

# Ensure that all target schemas exist.
for schema in [SF_SCHEMA_STATIC, SF_SCHEMA_REALTIME, SF_SCHEMA_TRIP_UPDATES]:
    cs.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    print(f"Schema {schema} ensured to exist.")

# Optionally, create the stage (if needed for file loading)
cs.execute("CREATE OR REPLACE STAGE GTFS.SCHEDULE.GTFS_STAGE")
print("Stage GTFS.SCHEDULE.GTFS_STAGE created or replaced.\n")

# -------------------------------------------------------------------
# FUNCTIONS TO CREATE TABLES IN SNOWFLAKE
# -------------------------------------------------------------------
def generate_create_table_ddl(table_name, df, schema):
    """
    Generates a CREATE OR REPLACE TABLE DDL command based on a DataFrame's structure.
    Mapping: integer -> NUMBER, float -> FLOAT, datetime -> TIMESTAMP_NTZ, others -> VARCHAR.
    Column names are converted to uppercase.
    
    This version converts the dtype to a string then checks for keywords ("int", "float", "datetime").
    """
    col_defs = []
    for col, dtype in df.dtypes.items():
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
    ddl = f'CREATE OR REPLACE TABLE {schema}.{table_name.upper()} (\n  ' + ',\n  '.join(col_defs) + '\n);'
    return ddl

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

# -------------------------------------------------------------------
# STATIC GTFS FUNCTIONS
# -------------------------------------------------------------------
# List of static GTFS feed URLs for different modes in Kraków.
GTFS_URLS = [
    "https://gtfs.ztp.krakow.pl/GTFS_KRK_A.zip",
    "https://gtfs.ztp.krakow.pl/GTFS_KRK_M.zip",
    "https://gtfs.ztp.krakow.pl/GTFS_KRK_T.zip",
    "https://gtfs.ztp.krakow.pl/GTFS_KRK_TR.zip"
]

def merge_gtfs_feeds(urls):
    """
    Downloads static GTFS feeds using gtfs_kit and merges common tables:
    routes, trips, stop_times, stops, and calendar (if available).
    Removes duplicates.
    """
    feeds = []
    for url in urls:
        print(f"Downloading static GTFS data from: {url}")
        feed = gk.read_feed(url, dist_units="km")
        feeds.append(feed)
    
    merged_feed = feeds[0]
    for feed in feeds[1:]:
        merged_feed.routes = pd.concat([merged_feed.routes, feed.routes], ignore_index=True).drop_duplicates()
        merged_feed.trips = pd.concat([merged_feed.trips, feed.trips], ignore_index=True).drop_duplicates()
        merged_feed.stop_times = pd.concat([merged_feed.stop_times, feed.stop_times], ignore_index=True).drop_duplicates()
        merged_feed.stops = pd.concat([merged_feed.stops, feed.stops], ignore_index=True).drop_duplicates()
        if hasattr(merged_feed, "calendar") and hasattr(feed, "calendar"):
            merged_feed.calendar = pd.concat([merged_feed.calendar, feed.calendar], ignore_index=True).drop_duplicates()
        elif not hasattr(merged_feed, "calendar") and hasattr(feed, "calendar"):
            merged_feed.calendar = feed.calendar.copy().drop_duplicates()
    print("Static GTFS feeds merged successfully.\n")
    return merged_feed

def save_static_data_to_snowflake(ctx, merged_feed):
    """
    Creates tables in the SCHEDULE schema and uploads static GTFS data.
    The tables are: ROUTES, TRIPS, STOP_TIMES, STOPS and CALENDAR (if available).
    Before uploading, the tables are truncated.
    """
    tables = ["routes", "trips", "stop_times", "stops"]
    if hasattr(merged_feed, "calendar"):
        tables.append("calendar")
    for table in tables:
        df = getattr(merged_feed, table).reset_index(drop=True)
        # Ensure all column names are uppercase.
        df = df.rename(columns=lambda x: x.upper())
        # Create the table in the SCHEDULE schema.
        create_table_in_snowflake(ctx, table, df, SF_SCHEMA_STATIC)
        # Set the current schema to SCHEDULE before uploading data.
        cs.execute(f"USE SCHEMA {SF_SCHEMA_STATIC}")
        # Truncate the table before upload.
        cs.execute(f"TRUNCATE TABLE {SF_SCHEMA_STATIC}.{table.upper()}")
        print(f"Uploading static data for table {table.upper()} to Snowflake...")
        success, nchunks, nrows, _ = write_pandas(ctx, df, table.upper(), auto_create_table=False)
        if success:
            print(f"Table {table.upper()} uploaded successfully: {nrows} rows in {nchunks} chunks.\n")
        else:
            print(f"Upload failed for table {table.upper()}.\n")

# -------------------------------------------------------------------
# REAL‑TIME GTFS‑Realtime VEHICLE FUNCTIONS
# -------------------------------------------------------------------
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

def parse_vehicle_positions(pb_data):
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
                    'timestamp': vehicle.timestamp
                })
    return vehicles

def save_realtime_data_to_snowflake(ctx, vehicles):
    """
    Creates the VEHICLE_POSITIONS table in the REALTIME schema and uploads real‑time data.
    Before uploading, truncates the target table.
    """
    df = pd.DataFrame(vehicles).reset_index(drop=True)
    if df.empty:
        print("No real‑time data available to upload.")
        return
    # Convert dataframe column names to uppercase.
    df = df.rename(columns=lambda x: x.upper())
    table_name = "VEHICLE_POSITIONS"
    create_table_in_snowflake(ctx, table_name, df, SF_SCHEMA_REALTIME)
    cs.execute(f"USE SCHEMA {SF_SCHEMA_REALTIME}")
    cs.execute(f"TRUNCATE TABLE {SF_SCHEMA_REALTIME}.{table_name}")
    print(f"Uploading real‑time data to table {SF_SCHEMA_REALTIME}.{table_name} ...")
    success, nchunks, nrows, _ = write_pandas(ctx, df, table_name, auto_create_table=False)
    if success:
        print(f"Real‑time data uploaded successfully: {nrows} rows in {nchunks} chunks.\n")
    else:
        print(f"Failed to upload real‑time data to table {table_name}.\n")

# -------------------------------------------------------------------
# REAL‑TIME GTFS‑Realtime TRIP UPDATES FUNCTIONS
# -------------------------------------------------------------------
def parse_trip_updates(pb_data):
    """
    Parses GTFS‑Realtime Trip Updates protobuf data and returns a list of dictionaries containing:
    trip_id, stop_id, stop_sequence, arrival, departure, schedule_relationship.
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
                    'schedule_relationship': stu.schedule_relationship if stu.HasField("schedule_relationship") else None
                }
                updates.append(update_dict)
    return updates

def save_trip_updates_to_snowflake(ctx, updates):
    """
    Creates the TRIP_UPDATES table in the TRIP_UPDATES schema and uploads trip updates data.
    Before uploading, truncates the target table.
    """
    df = pd.DataFrame(updates).reset_index(drop=True)
    if df.empty:
        print("No trip updates data available to upload.")
        return
    # Convert dataframe column names to uppercase.
    df = df.rename(columns=lambda x: x.upper())
    table_name = "TRIP_UPDATES"
    create_table_in_snowflake(ctx, table_name, df, SF_SCHEMA_TRIP_UPDATES)
    cs.execute(f"USE SCHEMA {SF_SCHEMA_TRIP_UPDATES}")
    cs.execute(f"TRUNCATE TABLE {SF_SCHEMA_TRIP_UPDATES}.{table_name}")
    print(f"Uploading trip updates data to table {SF_SCHEMA_TRIP_UPDATES}.{table_name} ...")
    success, nchunks, nrows, _ = write_pandas(ctx, df, table_name, auto_create_table=False)
    if success:
        print(f"Trip updates data uploaded successfully: {nrows} rows in {nchunks} chunks.\n")
    else:
        print(f"Failed to upload trip updates data to table {table_name}.\n")

# -------------------------------------------------------------------
# MAIN EXECUTION BLOCK
# -------------------------------------------------------------------
if __name__ == '__main__':
    
    # PART 1: PROCESS STATIC GTFS DATA
    print("=== Processing static GTFS data ===\n")
    merged_feed = merge_gtfs_feeds(GTFS_URLS)
    save_static_data_to_snowflake(ctx, merged_feed)
    
    # PART 2: PROCESS GTFS‑Realtime VEHICLE DATA
    print("\n=== Processing GTFS‑Realtime vehicle data ===")
    realtime_sources = [
        ("T", "https://gtfs.ztp.krakow.pl/VehiclePositions_T.pb"),
        ("A", "https://gtfs.ztp.krakow.pl/VehiclePositions_A.pb"),
        ("M", "https://gtfs.ztp.krakow.pl/VehiclePositions_M.pb"),
        ("TR", "https://gtfs.ztp.krakow.pl/VehiclePositions_TR.pb")
    ]
    
    all_vehicles = []
    for mode, url in realtime_sources:
        pb_data = fetch_real_time_data(url)
        if pb_data:
            vehicles = parse_vehicle_positions(pb_data)
            # Add mode identifier to each record.
            for v in vehicles:
                v['mode'] = mode
            print(f"Parsed {len(vehicles)} real‑time vehicle records from mode {mode}.")
            all_vehicles.extend(vehicles)
    
    if all_vehicles:
        save_realtime_data_to_snowflake(ctx, all_vehicles)
    else:
        print("No real‑time vehicle data available from any source.")
    
    # PART 3: PROCESS GTFS‑Realtime TRIP UPDATES DATA
    print("\n=== Processing GTFS‑Realtime trip updates data ===")
    trip_updates_sources = [
        ("T", "https://gtfs.ztp.krakow.pl/TripUpdates_T.pb"),
        ("A", "https://gtfs.ztp.krakow.pl/TripUpdates_A.pb"),
        ("M", "https://gtfs.ztp.krakow.pl/TripUpdates_M.pb"),
        ("TR", "https://gtfs.ztp.krakow.pl/TripUpdates_TR.pb")
    ]
    
    all_trip_updates = []
    for mode, url in trip_updates_sources:
        pb_data = fetch_real_time_data(url)
        if pb_data:
            updates = parse_trip_updates(pb_data)
            # Add mode identifier to each record.
            for u in updates:
                u['mode'] = mode
            print(f"Parsed {len(updates)} trip updates records from mode {mode}.")
            all_trip_updates.extend(updates)
    
    if all_trip_updates:
        save_trip_updates_to_snowflake(ctx, all_trip_updates)
    else:
        print("No trip updates data available from any source.")
    
    # Close the Snowflake connection.
    cs.close()
    ctx.close()
    print("\nProgram finished.")
