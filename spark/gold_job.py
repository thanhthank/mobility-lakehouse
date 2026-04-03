from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count

spark = SparkSession.builder \
    .appName("GoldJob") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# đọc từ Silver
df = spark.read.format("delta").load("/opt/spark-data/silver")

# ví dụ aggregation: tốc độ trung bình theo vehicle
df_gold = df.groupBy("vehicle_id").agg(
    avg("speed").alias("avg_speed"),
    count("*").alias("trip_count")
)

# ghi ra Gold
df_gold.write \
    .format("delta") \
    .mode("overwrite") \
    .save("/opt/spark-data/gold")

print("Gold layer written successfully!")