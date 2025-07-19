from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id
import pandas as pd

# Initialize Spark Session
spark = SparkSession.builder \
    .appName('alibabaScrape') \
    .getOrCreate()

# Optional: Set log level to reduce verbosity
spark.sparkContext.setLogLevel("WARN")

# Confirm the session is active
print(f"Spark session started with app name: {spark.sparkContext.appName}")

df = spark.createDataFrame([(1, 'apple'), (2, 'banana')], ['id', 'fruit'])
df.show()

# spark.stop()