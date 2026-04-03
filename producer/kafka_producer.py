import json
import time
import random
import pandas as pd
from kafka import KafkaProducer

CSV_FILE = "data/raw/gps.csv"
TOPIC = "gps_raw"

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

df = pd.read_csv(CSV_FILE)

print("Total rows:", len(df))

for _, row in df.iterrows():
    message = {
        "vehicle_id": str(row["hvfhs_license_num"]),
        "event_time": str(row["pickup_datetime"]),
        "pickup_location_id": int(row["PULocationID"]) if pd.notna(row["PULocationID"]) else -1,
        "dropoff_location_id": int(row["DOLocationID"]) if pd.notna(row["DOLocationID"]) else -1,
        "trip_miles": float(row["trip_miles"]) if pd.notna(row["trip_miles"]) else 0.0,
        "trip_time": float(row["trip_time"]) if pd.notna(row["trip_time"]) else 0.0,
        "base_passenger_fare": float(row["base_passenger_fare"]) if pd.notna(row["base_passenger_fare"]) else 0.0,
        "latitude": 21.0 + random.random(),
        "longitude": 105.8 + random.random(),
        "speed": 20 + random.random() * 40
    }

    producer.send(TOPIC, value=message)
    print("Sent:", message)
    time.sleep(0.01)

producer.flush()
producer.close()
print("Done sending data.")