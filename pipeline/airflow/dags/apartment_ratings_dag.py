import requests
from airflow.decorators import task, dag
from datetime import datetime
from airflow.utils.dates import days_ago
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from minio import Minio
from io import BytesIO



# Define your DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 8, 21),
    'depends_on_past': False,
    'retries': 1,
}

@dag(schedule_interval=None, start_date=days_ago(1), default_args=default_args, catchup=False)
def load_and_clean_ratings_data():

    @task
    def download_data():
        url = 'https://ckan0.cf.opendata.inter.prod-toronto.ca/api/3/action/package_show'
        querystring = {'id':'apartment-building-evaluation'}
        response = requests.request('GET', url, params=querystring)
        resources = response.json()['result']['resources']

        client = Minio('miniio:9000', access_key='atFOQ6sYmKMua2fZjdDt', secret_key='SidZ8OkenMAsxM4aTPMZl20NUYzWx1Q4vjhLsQ5T', secure=False)

        for resource in resources:
            if '.csv' in resource['url']:
                data = requests.request('GET', resource['url'])
                client.put_object('raw', resource['name'], BytesIO(data.content), len(data.content))
        
        return

    clean_task = SparkSubmitOperator(
        application = "/opt/airflow/jobs/apartment_rating_job.py",
        conn_id="spark_local",
        task_id="spark_clenaing_task",
    )
    
    download_task = download_data()

    download_task >> clean_task

load_and_clean_ratings_data()

# spark = SparkSession.builder.appName("ApartmentRatings").getOrCreate()

# response = requests.get("https://api.mfapi.in/mf/118550")
# data = response.text
# sparkContext = spark.sparkContext
# RDD = sparkContext.parallelize([data])
# raw_json_dataframe = spark.read.json(RDD)

# raw_json_dataframe.printSchema()
# raw_json_dataframe.createOrReplaceTempView("Mutual_benefit")

# dataframe = (
#     raw_json_dataframe.withColumn("data", F.explode(F.col("data")))
#     .withColumn("meta", F.expr("meta"))
#     .select("data.*", "meta.*")
# )

# dataframe.show(100, False)
# dataframe.toPandas().to_csv("dataframe.csv")

# NOTE This line requires Java 8 instead of Java 11 work it to work on Airflow
# We are saving locally for now.
# dataframe.write.parquet('s3a://sparkjobresult/output',mode='overwrite')
# dataframe.write.format('csv').option('header','true').save('s3a://sparkjobresult/output',mode='overwrite')
