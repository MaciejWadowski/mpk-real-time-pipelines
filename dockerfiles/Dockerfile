FROM apache/airflow:2.10.5

RUN pip install --no-cache-dir gtfs_kit apache-airflow-providers-snowflake apache-airflow-providers-docker gtfs-realtime-bindings --constraint "${HOME}/constraints.txt"
