#!/bin/bash
# Usage: ./tf.sh plan | ./tf.sh apply | ./tf.sh destroy
set -euo pipefail

set -a
source ../.env
set +a

MY_IP=$(curl -s ifconfig.me)/32
echo "Using IP: $MY_IP"

terraform "$@" \
  -var="my_ip=$MY_IP" \
  -var="postgres_user=${POSTGRES_USER}" \
  -var="postgres_password=${POSTGRES_PASSWORD}" \
  -var="postgres_db=${POSTGRES_DB}" \
  -var="airflow_fernet_key=${AIRFLOW_FERNET_KEY}" \
  -var="dbt_user=${DBT_USER}" \
  -var="dbt_password=${DBT_PASSWORD}" \
  -var="dbt_account=${DBT_ACCOUNT}" \
  -var="dbt_database=${DBT_DATABASE}" \
  -var="dbt_schema=${DBT_SCHEMA}" \
  -var="dbt_warehouse=${DBT_WAREHOUSE}" \
  -var="dbt_warehouse_stronk=${DBT_WAREHOUSE_STRONK:-${DBT_WAREHOUSE}}" \
  -var="dbt_role=${DBT_ROLE}" \
  -var="tomtom_api_key=${TOMTOM_API_KEY}" \
  -var="iam_initial_password=${IAM_INITIAL_PASSWORD}" \
  -var="iam_users_csv=${IAM_USERS}"
