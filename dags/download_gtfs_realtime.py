import os
import datetime
import requests

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# default DAG settings
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2023, 10, 1),
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=1),
}

# download GTFS-Realtime data and save it to file
def fetch_gtfs_realtime():
    url = "https://gtfs.ztp.krakow.pl/VehiclePositions_T.pb"
    print(f"Download real‑time data from: {url}")
    response = requests.get(url)
    
    if response.status_code == 200:
        # Create output catalog if it not exists
        output_dir = "/path/to/your/output/directory"  # change to proper
        os.makedirs(output_dir, exist_ok=True)
        
        # Filename with current timestamp
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        file_path = os.path.join(output_dir, f"VehiclePositions_T_{timestamp}.pb")
        
        with open(file_path, "wb") as f:
            f.write(response.content)
        
        print(f"Real‑time data are saved to: {file_path}")
    else:
        print(f"Error status code: {response.status_code}")

# DAG definition with 5 mintues interval
with DAG(
    dag_id="gtfs_realtime_fetch_dag",
    default_args=default_args,
    description="Task downloading GTFS-Realtime data",
    schedule_interval="*/5 * * * *",   # 5 minutes
    catchup=False,
) as dag:

    fetch_realtime_task = PythonOperator(
        task_id="fetch_gtfs_realtime",
        python_callable=fetch_gtfs_realtime,
    )

    fetch_realtime_task