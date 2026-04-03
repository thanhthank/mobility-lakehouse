from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("SilverJob") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

df = spark.read.format("delta").load("/opt/spark-data/bronze")

df_silver = df.filter(
    col("vehicle_id").isNotNull() &
    col("event_time").isNotNull() &
    col("latitude").between(-90, 90) &
    col("longitude").between(-180, 180)
)

df_silver.write.format("delta") \
    .mode("overwrite") \
    .save("/opt/spark-data/silver")

print("Silver layer written successfully.")