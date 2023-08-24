import logging
from pyspark import SparkContext
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T

logger = logging.getLogger("RoomRentStreamJob")

spark = SparkSession.builder.appName("RoomRentStreamJob").getOrCreate()

df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "broker:9092") \
    .option("subscribe", "room-for-rent") \
    .option("startingOffsets", "earliest") \
    .load()

df.printSchema()