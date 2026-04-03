import pandas as pd

# đọc file parquet (đang nằm trong data/raw)
df = pd.read_parquet("data/raw/fhvhv_tripdata_2026-01.parquet")

print("Columns:")
print(df.columns)

# lấy sample để nhẹ máy
df_sample = df.head(50000)

# lưu CSV
df_sample.to_csv("data/raw/gps.csv", index=False)

print("✅ Done! File saved to data/raw/gps.csv")
