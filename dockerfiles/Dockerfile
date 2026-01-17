FROM apache/airflow:2.10.5

RUN pip install --no-cache-dir gtfs_kit apache-airflow-providers-snowflake gtfs-realtime-bindings --constraint "${HOME}/constraints.txt"
# Copy DAGs, plugins, and logs into the container

COPY --chown=airflow:root dags /opt/airflow/dags
COPY --chown=airflow:root plugins /opt/airflow/plugins
RUN mkdir -p /opt/airflow/logs && chmod 777 /opt/airflow/logs