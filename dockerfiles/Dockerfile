FROM apache/airflow:2.10.5

ARG AIRFLOW_UID=1000
ARG AIRFLOW_GID=1000

# Ensure the 'airflow' user exists with the requested IDs (works whether it already exists or not)
RUN set -eux; \
    if id -u airflow >/dev/null 2>&1; then \
      usermod -u "${AIRFLOW_UID}" airflow || true; \
      groupmod -g "${AIRFLOW_GID}" airflow || true; \
    else \
      groupadd -g "${AIRFLOW_GID}" airflow || true; \
      useradd -m -u "${AIRFLOW_UID}" -g "${AIRFLOW_GID}" -s /bin/bash airflow || true; \
    fi

RUN pip install --no-cache-dir gtfs_kit apache-airflow-providers-snowflake gtfs-realtime-bindings

USER airflow