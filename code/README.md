# ==========================================
# HƯỚNG DẪN CHẠY HỆ THỐNG
# ==========================================

## 1. KHỞI ĐỘNG HỆ THỐNG DOCKER

# Khởi động tất cả services
docker-compose up -d

# Kiểm tra trạng thái
docker-compose ps

# Xem logs
docker-compose logs -f


## 2. CÀI ĐẶT PYTHON DEPENDENCIES (trên máy host)

pip install -r code/requirements.txt


## 3. CHẠY PRODUCER (Thu thập dữ liệu từ Steam API)

python code/produce.py


## 4. CHẠY CONSUMER CCU (2 tùy chọn)

### Tùy chọn A: Consumer đơn giản (Khuyến nghị cho dashboard)
python code/simple_consumer_ccu.py

### Tùy chọn B: Spark Streaming Consumer (cho big data thực sự)
# Copy file vào Spark master container
docker cp code/spark_consumer.py spark-master-streaming:/opt/bitnami/spark/

# Exec vào container
docker exec -it spark-master-streaming bash

# Chạy Spark submit
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,org.mongodb.spark:mongo-spark-connector_2.12:10.2.0 \
  /opt/bitnami/spark/spark_consumer.py


## 5. TRUY CẬP CÁC GIAO DIỆN WEB

- Kafka UI: http://localhost:8080
- Spark Master UI: http://localhost:8081
- Spark Worker UI: http://localhost:8082
- Mongo Express: http://localhost:8083


## 6. KIỂM TRA DỮ LIỆU TRONG MONGODB

### Từ command line
docker exec -it mongodb mongosh

use steam_db
db.player_ccu.find().sort({timestamp: -1}).limit(5).pretty()
db.player_details.countDocuments()

### Hoặc dùng Mongo Express: http://localhost:8083


## 7. DỪNG HỆ THỐNG

docker-compose down

# Xóa cả dữ liệu (cẩn thận!)
docker-compose down -v


## 8. CẤU TRÚC DỮ LIỆU

### Collection: player_ccu
{
  "timestamp": ISODate("2025-10-09T14:30:00Z"),
  "total_players": 929,
  "online_players": 245,
  "offline_players": 684,
  "country_stats": {
    "VN": 450,
    "US": 200,
    "CN": 150,
    ...
  },
  "top_countries": [
    ["VN", 450],
    ["US", 200],
    ["CN", 150]
  ]
}

### Collection: player_details
{
  "steamid": "76561197993609350",
  "personaname": "❤ Mixi ❤",
  "personastate": 0,
  "loccountrycode": "VN",
  "avatar": "https://...",
  "profileurl": "https://...",
  "last_seen": ISODate("2025-10-09T14:30:00Z")
}


## 9. TROUBLESHOOTING

### Kafka không kết nối được
- Kiểm tra port: docker-compose ps
- Xem logs: docker-compose logs kafka1 kafka2

### MongoDB không ghi được
- Kiểm tra connection: docker exec -it mongodb mongosh
- Xem permissions

### Producer báo lỗi 403/401
- Kiểm tra Steam API key còn hạn không
- Một số Steam ID có thể là private profile
