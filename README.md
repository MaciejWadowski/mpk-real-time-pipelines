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
```

Airflow folders:
- `/dags` - a folder that contains all of the DAG's for an airflow. It's scheduled to refresh content of dags folder every 60 seconds, but can be modified with `AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL` env variable
- `/plugins` - a folder that will contains some custom plugins, probably won't be used ever
- `/logs` - a folder to logs from airflow(scheduler, dag-processor)


### Generate gtfs_realtime_pb2.py script

```shell
brew install protoc # for mac only
protoc --python_out=. gtfs-realtime.proto
```


### Kafka

Create kafka topic with below comamnd
```shell
docker exec -it kafka kafka-topics --create --topic mpk-real-time-feed --partitions 1 --replication-factor 1 --bootstrap-server localhost:9093
```

* docker exec -it kafka: This runs the kafka-topics command inside your running Kafka container.
* --topic mpk-real-time-feed: Specifies the name of the topic to create (in this example, mpk-real-time-feed).
* --partitions 1: Specifies the number of partitions for the topic. You can change this number depending on how many partitions you want.
* --replication-factor 1: Specifies the replication factor for the topic. This should be 1 in a local setup, but in a production environment, you'd typically set this to 2 or more, depending on the number of Kafka brokers.
* --bootstrap-server localhost:9093: The bootstrap server to connect to, in this case, it's localhost:9093 because we're running Kafka in a Docker container.


### Kafka UI

kafka ui is accessible from localhost:8083. You can see the messages in json format that are processed by download_gtfs_realtime dag.
