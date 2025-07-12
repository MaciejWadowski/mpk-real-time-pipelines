### Setting up ariflow enviroment for the first time

- Copy the content of .env-sample file and name it .env file. Change the fernet key and sample postgres_db user and password.
- `docker compose up airflow-webserver -d`
- `docker compose run airflow-webserver airflow db init`
- ```shell
    docker compose run airflow-webserver airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com
- `docker compose up -d`


### Using airflow in the future


```shell
docker compse up -d # starting airflow
docker compose down # stopping airflow

docker compose run airflow-webserver airflow connections add my_snowflake_conn \
  --conn-type snowflake \
  --conn-login "<add username here>" \
  --conn-password "<add password here> \
  --conn-schema "SCHEDULE" \
  --conn-extra '{
    "account": "MARCQSC-WM98819",
    "warehouse": "COMPUTE_WH",
    "database": "GTFS_TEST",
    "role": "GTFS_UPLOADER_ROLE",
    "insecure_mode": false
}'
```

Airflow folders:
- `/dags` - a folder that contains all of the DAG's for an airflow. It's scheduled to refresh content of dags folder every 60 seconds, but can be modified with `AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL` env variable
- `/plugins` - a folder that will contains some custom plugins, probably won't be used ever
- `/logs` - a folder to logs from airflow(scheduler, dag-processor)

### profiles.yml
Copy file in /dbt/gtfs_project/test-profiles.yml and name it profiles.yml
Replace placeholders with your personal values.