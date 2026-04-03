from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("WriteToPostGIS") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

df = spark.read.format("delta").load("/opt/spark-data/silver")

# Chỉ ghi các cột đang khớp với bảng gps_points
df_out = df.select(
    "vehicle_id",
    "event_time",
    "latitude",
    "longitude",
    "speed"
)

df_out.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgis:5432/mobility") \
    .option("dbtable", "gps_points") \
    .option("user", "postgres") \
    .option("password", "postgres") \
    .option("driver", "org.postgresql.Driver") \
    .mode("append") \
    .save()

print("DONE WRITE TO POSTGIS")