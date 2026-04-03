Mobility Data Lakehouse
  Hệ thống xử lý dữ liệu GPS theo kiến trúc Data Lakehouse, sử dụng Kafka + Spark + Delta Lake + PostGIS + Streamlit.

Kiến trúc hệ thống
          Kafka Producer
              ↓
             Kafka
              ↓
         Spark Streaming
              ↓
         Bronze → Silver → Gold (Delta Lake)
              ↓
           PostgreSQL + PostGIS
              ↓
           Streamlit Dashboard

Hướng dẫn chạy
  1. Clone repo: git clone https://github.com/thanhtank/mobility-lakehouse.git
            cd mobility-lakehouse
  2. Khởi động hệ thống (Kafka + Spark + PostGIS)
            docker compose up -d
            Kiểm tra container: docker compose ps
  3. Chạy Kafka Producer: python producer/kafka_producer.py
  4. Chạy Spark Streaming
        docker exec -it spark-master bash
     
      /opt/spark/bin/spark-submit \
      --master spark://spark-master:7077 \
      --packages io.delta:delta-spark_2.12:3.2.0 \
      /opt/spark-apps/bronze_stream.py

  Chạy tiếp:
    silver_job.py
    gold_job.py
    to_postgis.py
  5. Kiểm tra dữ liệu trong PostgreSQL
      docker exec -it postgis psql -U postgres -d mobility
      SELECT COUNT(*) FROM gps_points;
      SELECT * FROM gps_points LIMIT 5;
  6. Chạy Dashboard
      cd dashboard
      python -m streamlit run app.py
      Mở trình duyệt: http://localhost:8501
