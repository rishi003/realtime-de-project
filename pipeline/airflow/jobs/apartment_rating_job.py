import logging
import os
from pyspark import SparkContext
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T

logger = logging.getLogger("MinioSparkJob")

spark = SparkSession.builder.appName("ApartmentRatings").getOrCreate()

def load_config(spark_context: SparkContext):
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.access.key", "atFOQ6sYmKMua2fZjdDt")
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "SidZ8OkenMAsxM4aTPMZl20NUYzWx1Q4vjhLsQ5T")
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://miniio:9000")
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "true")
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.attempts.maximum", "1")
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.connection.establish.timeout", "5000")
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.connection.timeout", "10000")

load_config(spark)

df = spark.read.csv("s3a://raw/*.csv", header=True)

df.show()