import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="my_dag_name",
    start_date=datetime.datetime(2021, 1, 1),
    schedule="@daily",
    catchup=False
) as dag:
    @task.bash(env={"message": '{{ dag_run.conf["message"] if dag_run.conf else "no message" }}'}, dag=dag)
    def bash_task():
        return "echo \"here is the message: '$message'\""

    basic_operator = EmptyOperator(task_id="task", dag=dag)
    basic_operator >> bash_task()