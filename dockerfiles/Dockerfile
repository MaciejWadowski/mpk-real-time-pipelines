FROM apache/airflow:2.10.5

ARG AIRFLOW_UID=1000
ARG AIRFLOW_GID=0


RUN pip install --no-cache-dir gtfs_kit apache-airflow-providers-snowflake gtfs-realtime-bindings

USER airflow