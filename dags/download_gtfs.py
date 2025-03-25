import os
import datetime
import pandas as pd
import gtfs_kit as gk

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2023, 10, 1),
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

with DAG(
    dag_id='gtfs_download_to_csv',
    default_args=default_args,
    description='Download GTFS data for Kraków and save tables as CSV files',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    def merge_gtfs_feeds():
        """
        Downloads GTFS feeds from the provided URLs using gtfs_kit
        and merges common tables: routes, trips, stop_times, stops,
        and calendar (if available).
        """
        GTFS_URLS = [
            "https://gtfs.ztp.krakow.pl/GTFS_KRK_A.zip",
            "https://gtfs.ztp.krakow.pl/GTFS_KRK_M.zip",
            "https://gtfs.ztp.krakow.pl/GTFS_KRK_T.zip",
            "https://gtfs.ztp.krakow.pl/GTFS_KRK_TR.zip"
        ]
        feeds = []
        for url in GTFS_URLS:
            print(f"Downloading data from: {url}")
            feed = gk.read_feed(url, dist_units="km")
            feeds.append(feed)

        # Merge the feeds; for tables present in more than one feed, concatenate the rows.
        merged_feed = feeds[0]
        for feed in feeds[1:]:
            merged_feed.routes = pd.concat([merged_feed.routes, feed.routes], ignore_index=True)
            merged_feed.trips = pd.concat([merged_feed.trips, feed.trips], ignore_index=True)
            merged_feed.stop_times = pd.concat([merged_feed.stop_times, feed.stop_times], ignore_index=True)
            merged_feed.stops = pd.concat([merged_feed.stops, feed.stops], ignore_index=True)
            if hasattr(merged_feed, "calendar") and hasattr(feed, "calendar"):
                merged_feed.calendar = pd.concat([merged_feed.calendar, feed.calendar], ignore_index=True)
            elif not hasattr(merged_feed, "calendar") and hasattr(feed, "calendar"):
                merged_feed.calendar = feed.calendar.copy()
        return merged_feed

    def download_and_save_csv():
        """
        Downloads and merges the GTFS feeds and saves each merged table as a CSV file.
        The files are stored in the output directory 'data_csv'.
        """
        # Merge all feeds
        merged_feed = merge_gtfs_feeds()
        
        # Define the output directory (adjust if needed)
        output_dir = 'data_csv'
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        # Save each GTFS table as a CSV file
        merged_feed.routes.to_csv(os.path.join(output_dir, 'routes.csv'), index=False)
        merged_feed.trips.to_csv(os.path.join(output_dir, 'trips.csv'), index=False)
        merged_feed.stop_times.to_csv(os.path.join(output_dir, 'stop_times.csv'), index=False)
        merged_feed.stops.to_csv(os.path.join(output_dir, 'stops.csv'), index=False)
        if hasattr(merged_feed, "calendar"):
            merged_feed.calendar.to_csv(os.path.join(output_dir, 'calendar.csv'), index=False)
        
        print(f"CSV files saved to directory: {output_dir}")

    download_gtfs_task = PythonOperator(
        task_id='download_gtfs_data',
        python_callable=download_and_save_csv,
    )

    download_gtfs_task