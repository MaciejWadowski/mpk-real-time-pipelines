docker compose up airflow-webserver -d
docker compose run airflow-webserver airflow db init
docker compose run airflow-webserver airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com
docker compose up -d
