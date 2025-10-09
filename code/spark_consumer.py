"""
Spark Structured Streaming với Stateful Processing
- Windowing: Time-based windows
- Watermark: Xử lý late-arriving data
- Stateful: Theo dõi trạng thái từng người chơi qua thời gian
- Aggregation: Tính toán CCU theo nhiều chiều
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, current_timestamp, window, count, sum as _sum,
    avg, max as _max, min as _min, countDistinct, expr, 
    when, lit, to_timestamp, unix_timestamp, struct
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
from pyspark.sql.streaming import GroupState, GroupStateTimeout
import os

# ======================
#  CẤU HÌNH
# ======================
KAFKA_BROKER = "kafka1:9092,kafka2:9093"
KAFKA_TOPIC = "steam_players"
MONGO_URI = "mongodb://mongodb:27017"
MONGO_DB = "steam_db"
CHECKPOINT_DIR = "/tmp/spark_checkpoint_ccu"

# ======================
#  KHỞI TẠO SPARK SESSION
# ======================
spark = SparkSession.builder \
    .appName("SteamCCU_Stateful_Streaming") \
    .config("spark.mongodb.output.uri", f"{MONGO_URI}/{MONGO_DB}") \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,"
            "org.mongodb.spark:mongo-spark-connector_2.12:10.2.0") \
    .config("spark.sql.streaming.stateStore.stateSchemaCheck", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("="*80)
print("SPARK STRUCTURED STREAMING - STATEFUL CCU PROCESSING")
print("="*80)

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

# ======================
#  ĐỌC STREAMING DATA TỪ KAFKA
# ======================
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

print("Kết nối Kafka streaming thành công")

# ======================
#  PARSE & TRANSFORM DATA
# ======================
df_parsed = df_kafka.select(
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
    col("event_time"),
    current_timestamp().alias("processed_time")
)

# ======================
#  STREAMING QUERY 1: WINDOWED AGGREGATION
#  Tính CCU theo time windows với watermark
# ======================
print("\nQuery 1: Windowed CCU Aggregation")

df_windowed_ccu = df_players \
    .withWatermark("event_time", "10 minutes") \
    .groupBy(
        window(col("event_time"), "5 minutes", "5 minutes"),
        col("country")
    ) \
    .agg(
        countDistinct("steamid").alias("unique_players"),
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

# Write windowed CCU to MongoDB
query_windowed = df_windowed_ccu.writeStream \
    .outputMode("append") \
    .format("mongodb") \
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/windowed_ccu") \
    .option("database", MONGO_DB) \
    .option("collection", "ccu_windowed") \
    .trigger(processingTime="30 seconds") \
    .start()

print("  Windowed CCU query started")
print(f"    - Window: 5 minutes")
print(f"    - Watermark: 10 minutes")
print(f"    - Output: MongoDB collection 'ccu_windowed'")

# ======================
#  STREAMING QUERY 2: GLOBAL AGGREGATION
#  Tính tổng CCU toàn hệ thống
# ======================
print("\nQuery 2: Global CCU Metrics")

df_global_ccu = df_players \
    .withWatermark("event_time", "10 minutes") \
    .groupBy(
        window(col("event_time"), "5 minutes", "5 minutes")
    ) \
    .agg(
        countDistinct("steamid").alias("total_unique_players"),
        _sum(when(col("personastate") == 1, 1).otherwise(0)).alias("total_online"),
        _sum(when(col("personastate") == 0, 1).otherwise(0)).alias("total_offline"),
        _sum(when(col("personastate") >= 2, 1).otherwise(0)).alias("total_away"),
        countDistinct("country").alias("active_countries"),
        current_timestamp().alias("computation_time")
    ) \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("total_unique_players"),
        col("total_online"),
        col("total_offline"),
        col("total_away"),
        expr("total_online * 100.0 / total_unique_players").alias("online_percentage"),
        col("active_countries"),
        col("computation_time")
    )

query_global = df_global_ccu.writeStream \
    .outputMode("append") \
    .format("mongodb") \
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/global_ccu") \
    .option("database", MONGO_DB) \
    .option("collection", "ccu_global") \
    .trigger(processingTime="30 seconds") \
    .start()

print("  ✓ Global CCU query started")
print(f"    - Aggregation: System-wide metrics")
print(f"    - Output: MongoDB collection 'ccu_global'")

# ======================
#  STREAMING QUERY 3: PLAYER DETAILS (Latest State)
#  Lưu trạng thái mới nhất của từng người chơi
# ======================
print("\nQuery 3: Player Latest State (Stateful)")

df_player_state = df_players \
    .withWatermark("event_time", "15 minutes") \
    .dropDuplicates(["steamid", "event_time"]) \
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
        col("event_time").alias("last_seen"),
        col("processed_time")
    )

query_player_state = df_player_state.writeStream \
    .outputMode("append") \
    .format("mongodb") \
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/player_state") \
    .option("database", MONGO_DB) \
    .option("collection", "player_state_history") \
    .trigger(processingTime="30 seconds") \
    .start()

print("  ✓ Player state tracking query started")
print(f"    - Track: Individual player state changes")
print(f"    - Output: MongoDB collection 'player_state_history'")

# ======================
#  STREAMING QUERY 4: SLIDING WINDOW ANALYSIS
#  Phân tích xu hướng với sliding windows
# ======================
print("\nQuery 4: Sliding Window Trend Analysis")

df_sliding = df_players \
    .withWatermark("event_time", "20 minutes") \
    .groupBy(
        window(col("event_time"), "10 minutes", "2 minutes")  # 10min window, slide 2min
    ) \
    .agg(
        countDistinct("steamid").alias("unique_players"),
        _sum(when(col("personastate") == 1, 1).otherwise(0)).alias("online"),
        _max(when(col("personastate") == 1, 1).otherwise(0)).alias("peak_online"),
        current_timestamp().alias("computation_time")
    ) \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("unique_players"),
        col("online"),
        col("peak_online"),
        col("computation_time")
    )

query_sliding = df_sliding.writeStream \
    .outputMode("append") \
    .format("mongodb") \
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/sliding_window") \
    .option("database", MONGO_DB) \
    .option("collection", "ccu_trends") \
    .trigger(processingTime="1 minute") \
    .start()

print("  ✓ Sliding window analysis query started")
print(f"    - Window: 10 minutes, Slide: 2 minutes")
print(f"    - Output: MongoDB collection 'ccu_trends'")

# ======================
#  MONITORING & AWAIT TERMINATION
# ======================
print("\n" + "="*80)
print("STREAMING QUERIES SUMMARY")
print("="*80)
print(f"1. Windowed CCU by Country    → ccu_windowed")
print(f"2. Global System Metrics       → ccu_global")
print(f"3. Player State History        → player_state_history")
print(f"4. Sliding Window Trends       → ccu_trends")
print("="*80)
print("\n All queries are running in parallel...")
print(" Data is being written to MongoDB in real-time")
print("Check MongoDB collections for results")
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
