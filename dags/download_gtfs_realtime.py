import os
import datetime
import requests
import logging

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

logger = logging.getLogger(__file__)


# Kafka configuration
conf = {
    'bootstrap.servers': 'kafka:9092',  # Kafka broker (adjust if using remote broker)
    'client.id': 'mpk-real-time-producer'
}

# Create Kafka producer

# Callback to handle delivery report
def delivery_report(err, msg):
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.info(f"Message delivered to {msg.topic()} partition {msg.partition()}")

# Produce a message to a Kafka topic
def produce_message(topic, message):
    from confluent_kafka import Producer
    producer = Producer(conf)
    producer.produce(topic, message.encode('utf-8'), callback=delivery_report)
    producer.flush()

# download GTFS-Realtime data and save it to file
def fetch_gtfs_realtime():
    import gtfs_realtime_pb2 as pb
    from google.protobuf.json_format import MessageToJson

    url = "https://gtfs.ztp.krakow.pl/VehiclePositions_T.pb"
    logger.info(f"Download real‑time data from: {url}")
    response = requests.get(url)
    if response.ok:
        feed = pb.FeedMessage()
        feed.ParseFromString(response.content)
        message = MessageToJson(feed)
        produce_message("mpk-real-time-feed", message)
    else:
        logger.error(f"Error status code: {response.status_code}")
        response.raise_for_status()

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