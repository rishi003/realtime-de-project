# import airflow
# from datetime import timedelta
# from airflow import DAG
# from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# default_args = {
#     "owner": "airflow",
#     "retry_delay": timedelta(minutes=5),
# }

# spark_dag = DAG(
#     dag_id="apartment_ratings_dag",
#     default_args=default_args,
#     schedule_interval=None,
#     dagrun_timeout=timedelta(minutes=60),
#     description="Fetches and stores apartment ratings from Toronto Open Datasets",
#     start_date=airflow.utils.dates.days_ago(1),
# )

# Extract = SparkSubmitOperator(
#     application="/opt/airflow/dags/spark_etl_script_docker.py",
#     conn_id="spark_local",
#     task_id="spark_submit_task",
#     dag=spark_dag,
# )

# Extract
