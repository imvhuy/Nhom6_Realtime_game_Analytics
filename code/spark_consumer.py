"""
Spark Structured Streaming với Windowed + Stateful Processing
- Windowing: Time-based windows (5 minutes)
- Watermark: Xử lý late-arriving data (10 minutes)
- Stateful: Theo dõi trạng thái từng người chơi
- Aggregation: Tính toán CCU theo nhiều chiều
- MongoDB: Dùng PyMongo thay vì Spark Connector (fix version mismatch)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, current_timestamp, window, count, sum as _sum,
    avg, max as _max, min as _min, approx_count_distinct, expr, 
    when, lit, to_timestamp, unix_timestamp, struct, to_json
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
import os
import json
import time
import logging
from datetime import datetime

# ======================
#  CẤU HÌNH
# ======================
KAFKA_BROKER = "kafka1:9092,kafka2:9093"
KAFKA_TOPIC_PLAYERS = "steam_players"
KAFKA_TOPIC_USERSTATS = "steam_userstats"
MONGO_URI = "mongodb://mongodb:27017"
MONGO_DB = "steam_db"
CHECKPOINT_DIR = "/tmp/spark_checkpoint_ccu"

# ======================
#  MONGODB HELPER FUNCTIONS (định nghĩa trước)
# ======================
def get_mongo_connection():
    """Tạo kết nối MongoDB"""
    try:
        from pymongo import MongoClient
        client = MongoClient(
            "mongodb://mongodb:27017", 
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=10000,
            socketTimeoutMS=20000,
            maxPoolSize=10
        )
        client.admin.command('ping')
        return client["steam_db"]
    except Exception as e:
        print(f"❌ MongoDB connection error: {e}")
        return None

def write_to_mongodb(collection_name, documents):
    """Ghi batch documents vào MongoDB"""
    try:
        db = get_mongo_connection()
        if db is None:
            print(f"❌ Cannot connect to MongoDB for {collection_name}")
            return False
        
        collection = db[collection_name]
        
        if isinstance(documents, list):
            if len(documents) > 0:
                # Thêm timestamp cho mỗi document
                for doc in documents:
                    doc['_inserted_at'] = datetime.now()
                
                result = collection.insert_many(documents, ordered=False)
                print(f"✓ Inserted {len(result.inserted_ids)} documents into {collection_name}")
        else:
            documents['_inserted_at'] = datetime.now()
            result = collection.insert_one(documents)
            print(f"✓ Inserted 1 document into {collection_name}")
        
        return True
    except Exception as e:
        print(f"⚠ Write error to {collection_name}: {e}")
        return False

def write_to_mongodb_upsert(collection_name, documents, unique_keys):
    """
    Ghi documents vào MongoDB với upsert (update nếu tồn tại, insert nếu mới)
    
    Args:
        collection_name: Tên collection
        documents: List of documents hoặc single document
        unique_keys: List các field dùng làm unique key để check duplicate
    """
    try:
        db = get_mongo_connection()
        if db is None:
            print(f"❌ Cannot connect to MongoDB for {collection_name}")
            return False
        
        collection = db[collection_name]
        
        doc_list = documents if isinstance(documents, list) else [documents]
        
        if len(doc_list) == 0:
            return True
        
        upserted_count = 0
        updated_count = 0
        
        for doc in doc_list:
            # Tạo filter từ unique keys
            filter_dict = {key: doc[key] for key in unique_keys if key in doc}
            
            # Thêm timestamp
            doc['_updated_at'] = datetime.now()
            
            # Upsert: update nếu tồn tại, insert nếu không
            result = collection.update_one(
                filter_dict,
                {"$set": doc},
                upsert=True
            )
            
            if result.upserted_id:
                upserted_count += 1
            elif result.modified_count > 0:
                updated_count += 1
        
        print(f"✓ {collection_name}: Inserted {upserted_count}, Updated {updated_count} documents")
        return True
        
    except Exception as e:
        print(f"⚠ Upsert error to {collection_name}: {e}")
        return False

def df_to_documents(df):
    """Chuyển DataFrame thành list of dicts"""
    return df.toJSON().map(lambda x: json.loads(x)).collect()

# ======================
#  KHỞI TẠO SPARK SESSION
# ======================
spark = SparkSession.builder \
    .appName("SteamCCU_Windowed_Stateful_Streaming") \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5") \
    .config("spark.sql.streaming.stateStore.stateSchemaCheck", "false") \
    .config("spark.python.profile", "false") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

print("="*80)
print("SPARK STRUCTURED STREAMING - WINDOWED + STATEFUL CCU PROCESSING")
print("="*80)

# Test MongoDB connection trước khi bắt đầu
print("\n🔍 Testing MongoDB connection...")
test_db = get_mongo_connection()
if test_db is None:
    print("❌ Cannot connect to MongoDB. Exiting...")
    spark.stop()
    exit(1)
else:
    print("✅ MongoDB connection successful!")

print("\n🔍 Testing Kafka connection...")
time.sleep(2)  # Give some time for Kafka to be ready



# ======================
#  SCHEMA DEFINITION
# ======================
player_schema = StructType([
    StructField("steamid", StringType(), True),
    StructField("communityvisibilitystate", IntegerType(), True),
    StructField("profilestate", IntegerType(), True),
    StructField("personaname", StringType(), True),
    StructField("commentpermission", IntegerType(), True),
    StructField("profileurl", StringType(), True),
    StructField("avatar", StringType(), True),
    StructField("avatarmedium", StringType(), True),
    StructField("avatarfull", StringType(), True),
    StructField("avatarhash", StringType(), True),
    StructField("personastate", IntegerType(), True),
    StructField("realname", StringType(), True),
    StructField("primaryclanid", StringType(), True),
    StructField("timecreated", LongType(), True),
    StructField("personastateflags", IntegerType(), True),
    StructField("loccountrycode", StringType(), True),
    StructField("locstatecode", StringType(), True)
])

# User Stats Schema - Nested structure từ API Steam
# Structure: {"playerstats": {"steamID": "...", "gameName": "...", "stats": [...], "achievements": [...]}}
stat_item_schema = StructType([
    StructField("name", StringType(), True),
    StructField("value", IntegerType(), True)
])

achievement_item_schema = StructType([
    StructField("name", StringType(), True),
    StructField("achieved", IntegerType(), True)
])

playerstats_schema = StructType([
    StructField("steamID", StringType(), True),
    StructField("gameName", StringType(), True),
    StructField("stats", StringType(), True),  # Lưu array dưới dạng JSON string vì có thể rất lớn
    StructField("achievements", StringType(), True)  # Lưu array dưới dạng JSON string
])

userstats_schema = StructType([
    StructField("playerstats", playerstats_schema, True)
])

# ======================
#  ĐỌC STREAMING DATA TỪ KAFKA - PLAYER INFO
# ======================
df_kafka_players = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", KAFKA_TOPIC_PLAYERS) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

print("✓ Kết nối Kafka streaming (players) thành công")

# ======================
#  PARSE & TRANSFORM DATA - PLAYERS
# ======================
df_parsed = df_kafka_players.select(
    col("key").cast("string").alias("steamid_key"),
    from_json(col("value").cast("string"), player_schema).alias("data"),
    col("timestamp").alias("event_time")
)

df_players = df_parsed.select(
    col("data.steamid").alias("steamid"),
    col("data.personaname").alias("personaname"),
    col("data.personastate").alias("personastate"),
    col("data.loccountrycode").alias("country"),
    col("data.locstatecode").alias("state_code"),
    col("data.avatar").alias("avatar"),
    col("data.profileurl").alias("profileurl"),
    col("data.timecreated").alias("timecreated"),
    col("event_time"),
    current_timestamp().alias("processed_time")
)

# ======================
#  STREAMING QUERY: UNIFIED WINDOWED + STATEFUL PROCESSING
#  Kết hợp windowing với stateful tracking trạng thái người chơi
# ======================
print("\nQuery: Unified Windowed + Stateful CCU Analysis")

# Step 1: Stateful - Lưu trạng thái mới nhất của từng người chơi
df_player_state_desc = df_players \
    .select(
        col("steamid"),
        col("personaname"),
        col("personastate"),
        when(col("personastate") == 0, "Offline")
            .when(col("personastate") == 1, "Online")
            .when(col("personastate") == 2, "Busy")
            .when(col("personastate") == 3, "Away")
            .when(col("personastate") == 4, "Snooze")
            .when(col("personastate") == 5, "Looking to trade")
            .when(col("personastate") == 6, "Looking to play")
            .otherwise("Unknown").alias("state_description"),
        col("country"),
        col("state_code"),
        col("avatar"),
        col("profileurl"),
        col("timecreated"),
        col("event_time").alias("last_seen"),
        col("processed_time")
    )

# Write stateful player details to MongoDB using foreach
def write_player_state_batch(df, batch_id):
    """Write player state batch to MongoDB"""
    try:
        print(f"Processing batch {batch_id} with {df.count()} records for player_details")
        if df.count() > 0:
            documents = df_to_documents(df)
            write_to_mongodb("player_details", documents)
    except Exception as e:
        print(f"❌ Error writing player state batch {batch_id}: {e}")

query_player_state = df_player_state_desc.writeStream \
    .outputMode("append") \
    .foreachBatch(write_player_state_batch) \
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/player_details") \
    .trigger(processingTime="30 seconds") \
    .start()

print("  ✓ Stateful Player Details query started")
print(f"    - Track: Individual player state changes")
print(f"    - Output: MongoDB collection 'player_details'")

# ======================
#  STREAMING QUERY: USER STATS FROM KAFKA
# ======================
print("\nQuery: User Stats Streaming")

# Đọc streaming data từ Kafka - User Stats
df_kafka_userstats = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", KAFKA_TOPIC_USERSTATS) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

print("✓ Kết nối Kafka streaming (userstats) thành công")

# Parse JSON từ Kafka - vì structure phức tạp, lưu raw JSON
df_userstats_parsed = df_kafka_userstats.select(
    col("key").cast("string").alias("steamid"),
    col("value").cast("string").alias("raw_data"),
    col("timestamp").alias("event_time"),
    current_timestamp().alias("processed_time")
)

# Write user stats to MongoDB
def write_userstats_batch(df, batch_id):
    """Write user stats batch to MongoDB with enhanced parsing"""
    try:
        row_count = df.count()
        print(f"📈 Processing batch {batch_id} with {row_count} records for user_stats")
        if row_count > 0:
            # Convert to documents và parse JSON
            rows = df.collect()
            documents = []
            for row in rows:
                try:
                    # Parse raw JSON data
                    stats_data = json.loads(row.raw_data)
                    
                    # Check structure: có thể là {"playerstats": {...}} hoặc direct {...}
                    if "playerstats" in stats_data:
                        playerstats = stats_data.get("playerstats", {})
                    else:
                        # Nếu không có wrapper, data chính là playerstats
                        playerstats = stats_data
                    
                    # Debug: print structure
                    if batch_id == 0:
                        print(f"  [DEBUG] Sample structure keys: {list(playerstats.keys())[:5]}")
                    
                    # Parse stats array để dễ query
                    stats_dict = {}
                    stats_array = playerstats.get("stats", [])
                    if isinstance(stats_array, list):
                        for stat in stats_array:
                            if isinstance(stat, dict):
                                stats_dict[stat.get("name")] = stat.get("value")
                    
                    # Parse achievements array
                    achievements_dict = {}
                    achievements_array = playerstats.get("achievements", [])
                    if isinstance(achievements_array, list):
                        for achievement in achievements_array:
                            if isinstance(achievement, dict):
                                achievements_dict[achievement.get("name")] = achievement.get("achieved", 0)
                    
                    # Extract key stats với fallback về 0
                    total_kills = stats_dict.get("total_kills", 0) or 0
                    total_deaths = stats_dict.get("total_deaths", 0) or 0
                    total_wins = stats_dict.get("total_wins", 0) or 0
                    total_mvps = stats_dict.get("total_mvps", 0) or 0
                    total_matches_played = stats_dict.get("total_matches_played", 0) or 0
                    total_matches_won = stats_dict.get("total_matches_won", 0) or 0
                    
                    # Calculate ratios với error handling
                    kd_ratio = round(total_kills / max(total_deaths, 1), 2) if total_deaths > 0 else total_kills
                    win_rate = round(total_matches_won / max(total_matches_played, 1) * 100, 2) if total_matches_played > 0 else 0
                    
                    # Tạo document với structure cải tiến
                    doc = {
                        "steamid": row.steamid,
                        "game_name": playerstats.get("gameName"),
                        "stats": stats_dict,  # Dictionary thay vì array để query dễ hơn
                        "achievements": achievements_dict,
                        "raw_data": playerstats,  # Lưu playerstats object, không có wrapper
                        "total_kills": total_kills,
                        "total_deaths": total_deaths,
                        "total_wins": total_wins,
                        "total_mvps": total_mvps,
                        "total_matches_played": total_matches_played,
                        "total_matches_won": total_matches_won,
                        "kd_ratio": kd_ratio,
                        "win_rate": win_rate,
                        "event_time": row.event_time,
                        "processed_time": row.processed_time,
                        "_inserted_at": datetime.now()
                    }
                    documents.append(doc)
                    
                    # Debug first document
                    if batch_id == 0 and len(documents) == 1:
                        print(f"  [DEBUG] Stats count: {len(stats_dict)}, Achievements: {len(achievements_dict)}")
                        print(f"  [DEBUG] Sample stats: kills={total_kills}, deaths={total_deaths}, KD={kd_ratio}")
                        
                except Exception as e:
                    print(f"⚠ Error parsing stats for {row.steamid}: {e}")
                    import traceback
                    traceback.print_exc()
            
            if documents:
                # Upsert based on steamid to keep latest stats
                write_to_mongodb_upsert("user_stats", documents, ["steamid"])
                print(f"  → Parsed {len(documents)} player stats with KD ratio and win rate")
    except Exception as e:
        print(f"❌ Error writing userstats batch {batch_id}: {e}")
        import traceback
        traceback.print_exc()

query_userstats = df_userstats_parsed.writeStream \
    .outputMode("append") \
    .foreachBatch(write_userstats_batch) \
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/user_stats") \
    .trigger(processingTime="30 seconds") \
    .start()

print("  ✓ User Stats query started")
print(f"    - Track: Game statistics for each player")
print(f"    - Output: MongoDB collection 'user_stats'")

# Step 2: Windowed Aggregation - CCU theo country và time windows
df_windowed_ccu = df_players \
    .withWatermark("event_time", "10 minutes") \
    .groupBy(
        window(col("event_time"), "5 minutes", "5 minutes"),
        col("country")
    ) \
    .agg(
        approx_count_distinct("steamid").alias("unique_players"),
        _sum(when(col("personastate") == 1, 1).otherwise(0)).alias("online_count"),
        _sum(when(col("personastate") == 0, 1).otherwise(0)).alias("offline_count"),
        _sum(when(col("personastate") >= 2, 1).otherwise(0)).alias("away_count"),
        avg("personastate").alias("avg_persona_state"),
        current_timestamp().alias("computation_time")
    ) \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("country"),
        col("unique_players"),
        col("online_count"),
        col("offline_count"),
        col("away_count"),
        col("avg_persona_state"),
        col("computation_time")
    )

# Write windowed CCU to MongoDB using foreach
def write_windowed_ccu_batch(df, batch_id):
    """Write windowed CCU batch to MongoDB - with upsert for update mode"""
    try:
        row_count = df.count()
        print(f"📊 Processing batch {batch_id} with {row_count} records for ccu_windowed")
        if row_count > 0:
            documents = df_to_documents(df)
            # Upsert để tránh duplicate khi update
            write_to_mongodb_upsert("ccu_windowed", documents, ["window_start", "window_end", "country"])
    except Exception as e:
        print(f"❌ Error writing windowed CCU batch {batch_id}: {e}")

query_windowed = df_windowed_ccu.writeStream \
    .outputMode("update") \
    .foreachBatch(write_windowed_ccu_batch) \
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/ccu_windowed") \
    .trigger(processingTime="30 seconds") \
    .start()

print("  ✓ Windowed CCU by Country query started")
print(f"    - Window: 5 minutes, Watermark: 10 minutes")
print(f"    - Output Mode: UPDATE (for windowed aggregation)")
print(f"    - Output: MongoDB collection 'ccu_windowed'")

# Step 3: Global CCU Metrics - Tính tổng CCU toàn hệ thống
df_global_ccu = df_players \
    .withWatermark("event_time", "10 minutes") \
    .groupBy(
        window(col("event_time"), "5 minutes", "5 minutes")
    ) \
    .agg(
        approx_count_distinct("steamid").alias("total_unique_players"),
        _sum(when(col("personastate") == 1, 1).otherwise(0)).alias("total_online"),
        _sum(when(col("personastate") == 0, 1).otherwise(0)).alias("total_offline"),
        _sum(when(col("personastate") >= 2, 1).otherwise(0)).alias("total_away"),
        approx_count_distinct("country").alias("active_countries"),
        current_timestamp().alias("computation_time")
    ) \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("total_unique_players"),
        col("total_online"),
        col("total_offline"),
        col("total_away"),
        when(col("total_unique_players") > 0, 
             expr("total_online * 100.0 / total_unique_players"))
            .otherwise(lit(0)).alias("online_percentage"),
        col("active_countries"),
        col("computation_time")
    )

# Write global CCU to MongoDB using foreach
def write_global_ccu_batch(df, batch_id):
    """Write global CCU batch to MongoDB - with upsert for update mode"""
    try:
        row_count = df.count()
        print(f"🌍 Processing batch {batch_id} with {row_count} records for ccu_global")
        if row_count > 0:
            documents = df_to_documents(df)
            # Upsert để tránh duplicate khi update
            write_to_mongodb_upsert("ccu_global", documents, ["window_start", "window_end"])
    except Exception as e:
        print(f"❌ Error writing global CCU batch {batch_id}: {e}")

query_global = df_global_ccu.writeStream \
    .outputMode("update") \
    .foreachBatch(write_global_ccu_batch) \
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/ccu_global") \
    .trigger(processingTime="30 seconds") \
    .start()

print("  ✓ Global CCU Metrics query started")
print(f"    - Aggregation: System-wide windowed metrics")
print(f"    - Output Mode: UPDATE (for windowed aggregation)")
print(f"    - Output: MongoDB collection 'ccu_global'")

# ==============================================
#  MONITORING & AWAIT TERMINATION
# ==============================================
print("\n" + "="*80)
print("STREAMING QUERIES SUMMARY")
print("="*80)
print(f"1. Player Details (Stateful)          → player_details")
print(f"2. User Game Statistics               → user_stats")
print(f"3. Windowed CCU by Country            → ccu_windowed")
print(f"4. Global System Metrics (Windowed)   → ccu_global")
print("="*80)
print("\n✅ All queries are running in parallel...")
print("📊 Data is being written to MongoDB in real-time")
print("🔍 Check MongoDB collections for results")
print("\n  Press Ctrl+C to stop all queries\n")

try:
    # Await all queries
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    print("\n\n Stopping all streaming queries...")
    for query in spark.streams.active:
        query.stop()
    print("✓ All queries stopped")
    spark.stop()
    print("✓ Spark session closed")
