import requests
from airflow.decorators import task, dag
from datetime import datetime
from airflow.utils.dates import days_ago
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


# Define your DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 8, 21),
    'depends_on_past': False,
    'retries': 1,
}

@dag(schedule_interval=None, start_date=days_ago(1), default_args=default_args, catchup=False)
def room_for_rent_stream():

    @task
    def create_big_query_table():
        pass

    realtime_task = SparkSubmitOperator(
        application="/opt/airflow/jobs/room_rent_stream_job.py",
        conn_id="spark_local",
        task_id="spark_realtime_task",
        packages="org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1"
    )

    create_big_query_table_task = create_big_query_table()

    create_big_query_table_task >> realtime_task

room_for_rent_stream()
