import os
import datetime
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from snowflake.connector.pandas_tools import write_pandas


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


with DAG(
    dag_id='gtfs_download_static',
    default_args=default_args,
    description='Download GTFS data for Kraków and save tables as CSV files',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    def merge_gtfs_feeds(logical_date):
        import pandas as pd
        import gtfs_kit as gk
        """
        Downloads GTFS feeds from the provided URLs using gtfs_kit
        and merges the common tables: routes, trips, stop_times, stops,
        calendar (if available) and calendar_dates (if available).
        Additionally, each feed is tagged with a "mode" field using a hardcoded letter.
        """
        GTFS_FEEDS = [
            ("https://gtfs.ztp.krakow.pl/GTFS_KRK_A.zip", "A"),
            ("https://gtfs.ztp.krakow.pl/GTFS_KRK_M.zip", "M"),
            ("https://gtfs.ztp.krakow.pl/GTFS_KRK_T.zip", "T")
        ]
        feeds = []
        for url, mode_letter in GTFS_FEEDS:
            print(f"Downloading data from: {url}")
            feed = gk.read_feed(url, dist_units="km")
            
            # Add a new column "mode" to each table if present.
            if hasattr(feed, "routes"):
                feed.routes["mode"] = mode_letter
            if hasattr(feed, "trips"):
                feed.trips["mode"] = mode_letter
            if hasattr(feed, "stop_times"):
                feed.stop_times["mode"] = mode_letter
            if hasattr(feed, "stops"):
                feed.stops["mode"] = mode_letter
            if hasattr(feed, "calendar"):
                feed.calendar["mode"] = mode_letter
            if hasattr(feed, "calendar_dates"):
                feed.calendar_dates["mode"] = mode_letter

            feeds.append(feed)

        # Start merging from the first feed
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

            if hasattr(feed, "calendar_dates"):
                if hasattr(merged_feed, "calendar_dates"):
                    merged_feed.calendar_dates = pd.concat([merged_feed.calendar_dates, feed.calendar_dates],
                                                           ignore_index=True).drop_duplicates()
                else:
                    merged_feed.calendar_dates = feed.calendar_dates.copy().drop_duplicates()

        # Add a load timestamp to each table
        merged_feed.routes['load_timestamp'] = logical_date
        merged_feed.trips['load_timestamp'] = logical_date
        merged_feed.stop_times['load_timestamp'] = logical_date
        merged_feed.stops['load_timestamp'] = logical_date
        
        if hasattr(merged_feed, "calendar"):
            merged_feed.calendar['load_timestamp'] = logical_date
        
        if hasattr(merged_feed, "calendar_dates"):
            merged_feed.calendar_dates['load_timestamp'] = logical_date

        return merged_feed


    def generate_create_table_ddl(table_name, df, schema):
        """
        Generates a CREATE OR REPLACE TABLE DDL command based on a DataFrame's structure.
        Mapping: integer -> NUMBER, float -> FLOAT, datetime -> TIMESTAMP_NTZ, others -> VARCHAR.
        Column names are converted to uppercase.
        Special case: the "load_timestamp" column is always mapped to TIMESTAMP_NTZ.
        """
        col_defs = []
        for col, dtype in df.dtypes.items():
            # Check if the column is "load_timestamp" (case-insensitive) and force TIMESTAMP_NTZ.
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
        ddl = f'CREATE TABLE IF NOT EXISTS {schema}.{table_name.upper()} (\n  ' + ',\n  '.join(col_defs) + '\n);'
        return ddl


    def save_static_data_to_snowflake(ctx, merged_feed):
        """
        Creates tables in the SCHEDULE schema and uploads static GTFS data.
        The tables are: ROUTES, TRIPS, STOP_TIMES, STOPS,
        CALENDAR (if available) and CALENDAR_DATES (if available).
        Before uploading, the tables are truncated.
        """
        cs = ctx.cursor()
        tables = ["routes", "trips", "stop_times", "stops"]
        if hasattr(merged_feed, "calendar"):
            tables.append("calendar")
        if hasattr(merged_feed, "calendar_dates"):
            tables.append("calendar_dates")
            
        for table in tables:
            df = getattr(merged_feed, table).reset_index(drop=True)
            # Convert all column names to uppercase.
            df = df.rename(columns=lambda x: x.upper())
            
            # Optionally remove timezone from any datetime columns.
            # This ensures no column is timezone-aware.
            for col in df.select_dtypes(include=['datetimetz']).columns:
                df[col] = df[col].dt.tz_localize(None)
            
            create_table_in_snowflake(ctx, table, df, "SCHEDULE")
            cs.execute("USE SCHEMA SCHEDULE")
            print(f"Uploading static data for table {table.upper()} to Snowflake...")
            
            # Pass use_logical_type=True to help the connector correctly convert times.
            success, nchunks, nrows, _ = write_pandas(
                ctx, df, table.upper(), auto_create_table=False, use_logical_type=True
            )
            
            if success:
                print(f"Table {table.upper()} uploaded successfully: {nrows} rows in {nchunks} chunks.\n")
            else:
                print(f"Upload failed for table {table.upper()}.\n")


    def ingest_static_data_to_snowflake(**kwargs):
        """
        Downloads and merges the GTFS feeds and uploads the merged static data to Snowflake.
        """
        # Merge all feeds using the provided logical_date argument.
        merged_feed = merge_gtfs_feeds(kwargs['logical_date'])
        hook = SnowflakeHook(snowflake_conn_id='my_snowflake_conn')
        conn = hook.get_conn()
        save_static_data_to_snowflake(conn, merged_feed)


    download_gtfs_task = PythonOperator(
        task_id='download_gtfs_data',
        python_callable=ingest_static_data_to_snowflake,
    )

    download_gtfs_task
