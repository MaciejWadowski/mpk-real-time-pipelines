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
        and merges common tables: routes, trips, stop_times, stops,
        and calendar (if available).
        """
        GTFS_URLS = [
            "https://gtfs.ztp.krakow.pl/GTFS_KRK_A.zip",
            "https://gtfs.ztp.krakow.pl/GTFS_KRK_M.zip",
            "https://gtfs.ztp.krakow.pl/GTFS_KRK_T.zip"
        ]
        feeds = []
        for url in GTFS_URLS:
            print(f"Downloading data from: {url}")
            feed = gk.read_feed(url, dist_units="km")
            feeds.append(feed)

        # Merge the feeds; for tables present in more than one feed, concatenate the rows.
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
        merged_feed.routes['load_timestamp'] = logical_date
        merged_feed.trips['load_timestamp'] = logical_date
        merged_feed.stop_times['load_timestamp'] = logical_date
        merged_feed.stops['load_timestamp'] = logical_date
        merged_feed.calendar['load_timestamp'] = logical_date

        return merged_feed


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
        ddl = f'CREATE TABLE IF NOT EXISTS {schema}.{table_name.upper()} (\n  ' + ',\n  '.join(col_defs) + '\n);'
        return ddl




    def save_static_data_to_snowflake(ctx, merged_feed):
        """
        Creates tables in the SCHEDULE schema and uploads static GTFS data.
        The tables are: ROUTES, TRIPS, STOP_TIMES, STOPS and CALENDAR (if available).
        Before uploading, the tables are truncated.
        """
        cs = ctx.cursor()
        tables = ["routes", "trips", "stop_times", "stops"]
        if hasattr(merged_feed, "calendar"):
            tables.append("calendar")
        for table in tables:
            df = getattr(merged_feed, table).reset_index(drop=True)
            # Ensure all column names are uppercase.
            df = df.rename(columns=lambda x: x.upper())
            # Create the table in the SCHEDULE schema.
            create_table_in_snowflake(ctx, table, df, "SCHEDULE")
            # Set the current schema to SCHEDULE before uploading data.
            cs.execute(f"USE SCHEMA SCHEDULE")
            print(f"Uploading static data for table {table.upper()} to Snowflake...")
            success, nchunks, nrows, _ = write_pandas(ctx, df, table.upper(), auto_create_table=False)
            if success:
                print(f"Table {table.upper()} uploaded successfully: {nrows} rows in {nchunks} chunks.\n")
            else:
                print(f"Upload failed for table {table.upper()}.\n")

    def ingest_static_data_to_snowflake(**kwargs):
        """
        Downloads and merges the GTFS feeds and saves each merged table as a CSV file.
        The files are stored in the output directory 'data_csv'.
        """
        # Merge all feeds
        merged_feed = merge_gtfs_feeds(kwargs['logical_date'])
        hook = SnowflakeHook(snowflake_conn_id='my_snowflake_conn')
        conn = hook.get_conn()
        save_static_data_to_snowflake(conn, merged_feed)
        


    download_gtfs_task = PythonOperator(
        task_id='download_gtfs_data',
        python_callable=ingest_static_data_to_snowflake,
    )

    download_gtfs_task