# 🎮 Real-time Steam Game Analytics Platform

**Nhóm 6 - Big Data & Real-time Processing**

Hệ thống phân tích dữ liệu game real-time từ Steam API sử dụng Kafka, Spark Structured Streaming và MongoDB.

## 📋 Table of Contents

- [Tổng Quan](#-tổng-quan)
- [Kiến Trúc Hệ Thống](#-kiến-trúc-hệ-thống)
- [Công Nghệ Sử Dụng](#-công-nghệ-sử-dụng)
- [Yêu Cầu Hệ Thống](#-yêu-cầu-hệ-thống)
- [Cài Đặt và Khởi Động](#-cài-đặt-và-khởi-động)
- [Sử Dụng](#-sử-dụng)
- [Cấu Trúc Dữ Liệu](#-cấu-trúc-dữ-liệu)
- [Monitoring & Visualization](#-monitoring--visualization)
- [Troubleshooting](#-troubleshooting)

---

## 🎯 Tổng Quan

Hệ thống streaming real-time phân tích dữ liệu người chơi Steam, bao gồm:

- **Player Status Tracking**: Theo dõi trạng thái online/offline của người chơi
- **Game Statistics**: Phân tích thống kê game chi tiết (kills, deaths, win rate, KD ratio)
- **CCU Analytics**: Tính toán Concurrent Users theo country và time windows
- **Real-time Dashboard**: Visualization với Grafana

### ✨ Tính Năng Chính

- ⚡ Real-time streaming với Kafka
- 🔥 Processing với Spark Structured Streaming
- 📊 Windowed aggregations (5-minute windows)
- 🌍 Geographic analytics (CCU by country)
- 📈 Player performance metrics (KD ratio, win rate)
- 🎯 Upsert mechanism để tránh duplicate
- 📉 Time-series data với watermark support

---

## 🏗️ Kiến Trúc Hệ Thống

```
┌─────────────────┐
│   Steam API     │
└────────┬────────┘
         │
         ▼
┌─────────────────┐      ┌──────────────────┐
│  Producer       │─────▶│  Kafka Cluster   │
│  (Python)       │      │  (2 brokers)     │
└─────────────────┘      └────────┬─────────┘
                                  │
                                  ▼
                         ┌──────────────────┐
                         │ Spark Streaming  │
                         │ (Master+Worker)  │
                         └────────┬─────────┘
                                  │
                                  ▼
                         ┌──────────────────┐
                         │    MongoDB       │
                         └────────┬─────────┘
                                  │
                    ┌─────────────┴─────────────┐
                    ▼                           ▼
           ┌────────────────┐         ┌──────────────┐
           │    Grafana     │         │ Mongo Express│
           └────────────────┘         └──────────────┘
```

### Data Flow

1. **Producer** (`produce_v2.py`) gọi Steam API → Kafka
2. **Kafka** (2 brokers) lưu messages vào 2 topics:
   - `steam_players` - Thông tin người chơi
   - `steam_userstats` - Thống kê game
3. **Spark Streaming** (`spark_consumer.py`) xử lý và aggregate
4. **MongoDB** lưu 4 collections:
   - `player_details` - Chi tiết người chơi
   - `user_stats` - Thống kê game
   - `ccu_windowed` - CCU theo country/time
   - `ccu_global` - CCU toàn hệ thống
5. **Grafana** + **Mongo Express** để visualization

---

## 🛠️ Công Nghệ Sử Dụng

| Component | Technology | Version |
|-----------|-----------|---------|
| **Message Queue** | Apache Kafka | 3.7.0 |
| **Stream Processing** | Apache Spark | 3.5.5 |
| **Database** | MongoDB | 6.0 |
| **Visualization** | Grafana | 11.1.4 |
| **API Integration** | Python + aiohttp | 3.10 |
| **Container** | Docker + Docker Compose | Latest |

### Python Libraries

- `kafka-python` - Kafka producer
- `pyspark` - Spark Structured Streaming
- `pymongo` - MongoDB driver
- `aiohttp` - Async HTTP client
- `pandas`, `numpy` - Data processing

---

## 💻 Yêu Cầu Hệ Thống

### Hardware
- **RAM**: Tối thiểu 8GB (khuyến nghị 16GB)
- **CPU**: 4 cores trở lên
- **Disk**: 10GB trống
- **Network**: Kết nối internet ổn định

### Software
- **OS**: Windows 10/11, Linux, macOS
- **Docker Desktop**: Version 20.10+
- **Docker Compose**: Version 2.0+
- **Python**: 3.10+ (cho producer)
- **Git**: Để clone repository

### Steam API Key
- Đăng ký tại: https://steamcommunity.com/dev/apikey
- Lưu key vào file `code/produce_v2.py`

---

## 🚀 Cài Đặt và Khởi Động

### Bước 1: Clone Repository

```bash
git clone https://github.com/imvhuy/Nhom6_Realtime_game_Analytics.git
cd Nhom6_Realtime_game_Analytics
```

### Bước 2: Cấu Hình Steam API Key

Mở file `code/produce_v2.py` và thay thế API key:

```python
STEAM_API_KEY = "YOUR_STEAM_API_KEY_HERE"
```

### Bước 3: Build và Khởi Động Docker Containers

```bash
# Build custom Spark image với PyMongo
docker-compose build

# Khởi động tất cả services
docker-compose up -d

# Kiểm tra containers
docker ps
```

**Expected containers**:
- `kafka1` - Kafka broker 1
- `kafka2` - Kafka broker 2
- `kafka-ui` - Kafka web UI
- `spark-master-streaming` - Spark master
- `spark-worker-streaming` - Spark worker
- `mongodb` - MongoDB database
- `mongo-express` - MongoDB web UI
- `grafana` - Grafana dashboard
- `mongo-api` - REST API cho Grafana

### Bước 4: Tạo Kafka Topics (nếu chưa có)

```bash
# Tạo topic cho player info
docker exec -it kafka1 /opt/kafka/bin/kafka-topics.sh \
  --create --topic steam_players \
  --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 2

# Tạo topic cho user stats
docker exec -it kafka1 /opt/kafka/bin/kafka-topics.sh \
  --create --topic steam_userstats \
  --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 2

# List topics
docker exec -it kafka1 /opt/kafka/bin/kafka-topics.sh \
  --list --bootstrap-server localhost:9092
```


### Bước 5: Chạy Producer

```bash
# Từ thư mục code
python produce_v2.py
```

Bạn sẽ thấy:
```
Đã nạp X steamid từ file steam_ids.csv
=== BẮT ĐẦU LẤY DỮ LIỆU ===
Đã gửi thông tin người chơi ... vào topic steam_players
Đã gửi thống kê game ... vào topic steam_userstats
```

### Bước 6: Chạy Spark Consumer



**Chạy trên docker**:
**Copy file spark_consumer.py vào docker**:
```bash
docker cp code\spark_consumer.py spark-master-streaming:/opt/bitnami/spark/
```
```bash
docker exec -u 0 -it spark-master-streaming bash -c "export HOME=/root && spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,org.mongodb.spark:mongo-spark-connector_2.12:10.2.0 --master local[*] /opt/bitnami/spark/spark_consumer.py" 
```

### Bước 8: Verify Data Flow

```bash
vào mongo-express http://localhost:8083 để check data
```

---

## 📊 Sử Dụng

### Web UIs

| Service | URL | Credentials |
|---------|-----|-------------|
| **Kafka UI** | http://localhost:8080 | None |
| **Spark Master** | http://localhost:8081 | None |
| **Spark Worker** | http://localhost:8082 | None |
| **MongoDB Express** | http://localhost:8083 | admin/admin |
| **Grafana** | http://localhost:3000 | admin/admin |
| **Mongo API** | http://localhost:5000 | None |

### Quick Commands

```bash
# Xem logs
docker logs kafka1 -f
docker logs spark-master-streaming -f
docker logs mongodb -f

# Restart services
docker-compose restart spark-master-streaming
docker-compose restart spark-worker-streaming

# Stop all
docker-compose down

# Clean volumes (⚠️ Xóa data)
docker-compose down -v
```

### Scripts Tiện Ích

| Script | Mô Tả |
|--------|-------|
| `run_spark_consumer.bat` | Chạy Spark consumer |
| `restart_spark_clean.bat` | Restart với clean checkpoint |
| `clean_userstats.bat` | Clean user_stats collection |
| `verify_setup.bat` | Verify toàn bộ setup |
| `test_producer.bat` | Test producer |

---

## 🗄️ Cấu Trúc Dữ Liệu

### 1. Collection: `player_details`

Lưu thông tin và trạng thái của từng người chơi.

```javascript
{
  "_id": ObjectId("..."),
  "steamid": "76561198012345678",
  "personaname": "PlayerName",
  "personastate": 1,
  "state_description": "Online",
  "country": "US",
  "state_code": "CA",
  "avatar": "https://...",
  "profileurl": "https://...",
  "timecreated": 1234567890,
  "last_seen": ISODate("2025-10-22T..."),
  "processed_time": ISODate("2025-10-22T..."),
  "_inserted_at": ISODate("2025-10-22T...")
}
```

**Indexes**:
```javascript
db.player_details.createIndex({"steamid": 1})
db.player_details.createIndex({"last_seen": -1})
db.player_details.createIndex({"country": 1})
```

### 2. Collection: `user_stats`

Thống kê game chi tiết của người chơi.

```javascript
{
  "_id": ObjectId("..."),
  "steamid": "76561198012345678",
  "game_name": "ValveTestApp260",
  "stats": {
    "total_kills": 62101,
    "total_deaths": 49242,
    "total_wins": 24605,
    "total_mvps": 6805,
    "total_kills_awp": 15498,
    "total_kills_ak47": 13509,
    // ... 160+ more stats
  },
  "achievements": {
    "PLAY_CS2": 1
  },
  "total_kills": 62101,
  "total_deaths": 49242,
  "total_wins": 24605,
  "total_mvps": 6805,
  "total_matches_played": 2657,
  "total_matches_won": 949,
  "kd_ratio": 1.26,
  "win_rate": 35.72,
  "event_time": ISODate("2025-10-22T..."),
  "_updated_at": ISODate("2025-10-22T...")
}
```

**Indexes**:
```javascript
db.user_stats.createIndex({"steamid": 1}, {unique: true})
db.user_stats.createIndex({"kd_ratio": -1})
db.user_stats.createIndex({"win_rate": -1})
db.user_stats.createIndex({"total_kills": -1})
```

### 3. Collection: `ccu_windowed`

CCU (Concurrent Users) theo country và time windows.

```javascript
{
  "_id": ObjectId("..."),
  "window_start": ISODate("2025-10-22T15:00:00Z"),
  "window_end": ISODate("2025-10-22T15:05:00Z"),
  "country": "US",
  "unique_players": 150,
  "online_count": 120,
  "offline_count": 20,
  "away_count": 10,
  "avg_persona_state": 1.2,
  "computation_time": ISODate("2025-10-22T..."),
  "_updated_at": ISODate("2025-10-22T...")
}
```

**Indexes**:
```javascript
db.ccu_windowed.createIndex({"window_start": 1, "country": 1}, {unique: true})
db.ccu_windowed.createIndex({"window_start": -1})
```

### 4. Collection: `ccu_global`

CCU toàn hệ thống theo time windows.

```javascript
{
  "_id": ObjectId("..."),
  "window_start": ISODate("2025-10-22T15:00:00Z"),
  "window_end": ISODate("2025-10-22T15:05:00Z"),
  "total_unique_players": 500,
  "total_online": 400,
  "total_offline": 80,
  "total_away": 20,
  "online_percentage": 80.0,
  "active_countries": 25,
  "computation_time": ISODate("2025-10-22T..."),
  "_updated_at": ISODate("2025-10-22T...")
}
```

**Indexes**:
```javascript
db.ccu_global.createIndex({"window_start": 1}, {unique: true})
db.ccu_global.createIndex({"window_start": -1})
```

---

## 📈 Monitoring & Visualization

### Grafana Setup

1. **Truy cập Grafana**: http://localhost:3000
2. **Login**: admin/admin
3. **Add Data Source**:
   - Type: **Infinity** (JSON/CSV/GraphQL)
   - URL: `http://mongo-api:5000`

### Sample Queries

**MongoDB Queries** (xem file `MONGODB_QUERIES.md`):

```javascript
// Top KD Ratio
db.user_stats.find().sort({"kd_ratio": -1}).limit(10)

// CCU Trend (last 24 hours)
db.ccu_global.find({
  "window_start": {$gte: new Date(Date.now() - 24*60*60*1000)}
}).sort({"window_start": 1})

// CCU by Country
db.ccu_windowed.aggregate([
  {$group: {
    _id: "$country",
    avg_players: {$avg: "$unique_players"}
  }},
  {$sort: {"avg_players": -1}}
])
```

### Dashboard Panels

1. **Player Count Trend** - Line chart
2. **CCU by Country** - Bar chart
3. **Top Players by KD** - Table
4. **Win Rate Distribution** - Histogram
5. **Active Countries Map** - Geo map

---

## 🔧 Troubleshooting

### Producer Issues

**Problem**: `Connection refused to Kafka`
```bash
# Check Kafka brokers
docker ps | grep kafka

# Verify ports
netstat -an | findstr "19092"
```

**Solution**: Ensure Kafka containers running và ports exposed.

---

### Spark Consumer Issues

**Problem**: `MongoDB connection error`
```bash
# Test connection from Spark container
docker exec -it spark-master-streaming ping mongodb

# Test MongoDB directly
docker exec -it mongodb mongosh --eval "db.version()"
```

**Solution**: 
- Ensure MongoDB container running
- Use `mongodb:27017` (service name), NOT `localhost:27017`

---

**Problem**: `Only player_details has data, others empty`

**Solution**: 
- Windowed queries need `outputMode("update")`, not `append`
- Check file `WINDOWED_FIX.md` for details

---

**Problem**: `User stats not parsing correctly`

**Solution**:
```bash
# Test parsing logic
cd code
python test_userstats_parsing.py

# Clean and restart
cd ..
clean_userstats.bat
```

---

### Docker Issues

**Problem**: `Out of memory`
```bash
# Increase Docker memory
# Docker Desktop → Settings → Resources → Memory: 8GB+
```

---

**Problem**: `Port already in use`
```bash
# Find process using port
netstat -ano | findstr "8080"

# Kill process (Windows)
taskkill /F /PID <PID>

# Or change port in docker-compose.yaml
```

---

## 📚 Documentation

- `FIX_GUIDE.md` - Fix MongoDB Connector issue
- `WINDOWED_FIX.md` - Fix windowed aggregation
- `DOCKER_NETWORK_FIX.md` - Docker networking guide
- `USERSTATS_GUIDE.md` - User stats implementation
- `MONGODB_QUERIES.md` - MongoDB query examples

---

## 👥 Team Members

- **Nhóm 6** - Big Data & Real-time Processing
- Repository: [Nhom6_Realtime_game_Analytics](https://github.com/imvhuy/Nhom6_Realtime_game_Analytics)

---

## 📝 License

This project is for educational purposes.

---

## 🙏 Acknowledgments

- Steam API Documentation
- Apache Kafka & Spark Communities
- MongoDB Documentation
- Grafana Community

---

## 📞 Support

Nếu gặp vấn đề, vui lòng:
1. Check `Troubleshooting` section
2. Review relevant `.md` files
3. Check Docker logs
4. Create issue on GitHub

**Happy Streaming! 🚀🎮**

Bước 1:
dit me m thang thao ngu
buoc 2:
c