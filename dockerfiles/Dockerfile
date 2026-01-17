FROM apache/airflow:2.10.5

COPY constraints.txt constraints.txt
RUN pip install --no-cache-dir gtfs_kit apache-airflow-providers-snowflake gtfs-realtime-bindings --constraint constraints.txt