import logging
from pyspark import SparkContext
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T

logger = logging.getLogger("ApartmentRatingsJob")

spark = SparkSession.builder.appName("ApartmentRatings").getOrCreate()

def iqr_outlier_treatment(dataframe, columns, factor=1.5):
    """
    Detects and treats outliers using IQR for multiple variables in a PySpark DataFrame.

    :param dataframe: The input PySpark DataFrame
    :param columns: A list of columns to apply IQR outlier treatment
    :param factor: The IQR factor to use for detecting outliers (default is 1.5)
    :return: The processed DataFrame with outliers treated
    """
    for column in columns:
        # Calculate Q1, Q3, and IQR
        quantiles = dataframe.approxQuantile(column, [0.25, 0.75], 0.5)
        q1, q3 = quantiles[0], quantiles[1]
        iqr = q3 - q1

        # Define the upper and lower bounds for outliers
        lower_bound = q1 - factor * iqr
        upper_bound = q3 + factor * iqr

        # Filter outliers and update the DataFrame
        dataframe = dataframe.filter((F.col(column) >= lower_bound) & (F.col(column) <= upper_bound))

    return dataframe

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

df = spark.read.csv("s3a://raw/apartment_ratings/*.csv", header=True)

# Select only the required columns
df = df.select('_id', 'RSN', 'YEAR_BUILT', 'PROPERTY_TYPE', 'WARDNAME', 'SITE_ADDRESS', 'CONFIRMED_STOREYS', 'CONFIRMED_UNITS', 'SCORE', 'LATITUDE', 'LONGITUDE')

# Print number of null values in each column
df.select([F.count(F.when(F.isnan(c) | F.col(c).isNull(), c)).alias(c) for c in df.columns]).show()

# Drop rows with null values
df = df.dropna()

# Clean site address from unwanted characters
df = df.withColumn("SITE_ADDRESS", F.regexp_replace("SITE_ADDRESS", "\\*\\* CREATED IN ERROR \\*\\* ", ""))

# Cast columns to the correct data types
df = df.withColumn("YEAR_BUILT", F.col("YEAR_BUILT").cast("string"))
df = df.withColumn("CONFIRMED_STOREYS", F.col("CONFIRMED_STOREYS").cast("int"))
df = df.withColumn("CONFIRMED_UNITS", F.col("CONFIRMED_UNITS").cast("int"))
df = df.withColumn("SCORE", F.col("SCORE").cast("double"))
df = df.withColumn("LATITUDE", F.col("LATITUDE").cast("double"))
df = df.withColumn("LONGITUDE", F.col("LONGITUDE").cast("double"))
df.dtypes

# Detect and treat outliers
numericColumns = [c for c in df.columns if isinstance(df.schema[c].dataType, (T.DoubleType, T.IntegerType))]
df = iqr_outlier_treatment(df, numericColumns)

# Write to a new CSV file
df.coalesce(1).write.mode("overwrite").csv("s3a://processed/apartment-ratings", )

df.show()