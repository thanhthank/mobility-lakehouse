from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, to_date, to_timestamp
from schemas import gps_schema

spark = SparkSession.builder \
    .appName("BronzeStream") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "gps_raw") \
    .option("startingOffsets", "earliest") \
    .load()

df_raw = df_kafka.selectExpr("CAST(value AS STRING) as json_str")

df_parsed = df_raw.select(
    from_json(col("json_str"), gps_schema).alias("data")
).select("data.*")

df_bronze = df_parsed \
    .withColumn("event_time", to_timestamp("event_time")) \
    .withColumn("ingestion_time", current_timestamp()) \
    .withColumn("event_date", to_date("event_time"))

query = df_bronze.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/opt/spark-data/checkpoint/bronze") \
    .start("/opt/spark-data/bronze")

query.awaitTermination()