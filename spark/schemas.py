from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

gps_schema = StructType([
    StructField("vehicle_id", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("pickup_location_id", IntegerType(), True),
    StructField("dropoff_location_id", IntegerType(), True),
    StructField("trip_miles", DoubleType(), True),
    StructField("trip_time", DoubleType(), True),
    StructField("base_passenger_fare", DoubleType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("speed", DoubleType(), True),
])