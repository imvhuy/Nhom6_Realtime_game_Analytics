"""
Simple Spark Streaming CCU (Concurrent Users) Analysis
Chỉ tính toán số người chơi đồng thời theo thời gian
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, current_timestamp, window, count, sum as _sum,
    approx_count_distinct, when, lit
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
import os

# ======================
#  CẤU HÌNH
# ======================
KAFKA_BROKER = "kafka1:9092,kafka2:9093"
KAFKA_TOPIC = "steam_players"
CHECKPOINT_DIR = "/tmp/spark_checkpoint_ccu"

# ======================
#  KHỞI TẠO SPARK SESSION
# ======================
spark = SparkSession.builder \
    .appName("SteamCCU_Simple") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5") \
    .config("spark.sql.streaming.stateStore.stateSchemaCheck", "false") \
    .config("spark.jars.ivy", "/opt/bitnami/spark/.ivy2") \
    .config("spark.driver.extraJavaOptions", "-Duser.home=/opt/bitnami/spark -Divy.home=/opt/bitnami/spark/.ivy2") \
    .config("spark.executor.extraJavaOptions", "-Duser.home=/opt/bitnami/spark -Divy.home=/opt/bitnami/spark/.ivy2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("="*60)
print("SPARK STREAMING - CCU ANALYSIS")
print("="*60)

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
#  ĐỌC DỮ LIỆU TỪ KAFKA
# ======================
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

print("Kết nối Kafka streaming thành công")

# Parse JSON data
df_parsed = df_kafka \
    .select(
        col("key").cast("string").alias("steamid_key"),
        from_json(col("value").cast("string"), player_schema).alias("data"),
        col("timestamp").alias("event_time")
    ) \
    .select(
        col("data.steamid"),
        col("data.personaname"),
        col("data.personastate"),
        col("data.loccountrycode").alias("country"),
        col("data.locstatecode").alias("state_code"),
        col("event_time"),
        current_timestamp().alias("processed_time")
    ) \
    .filter(col("steamid").isNotNull())

# ======================
#  CCU ANALYSIS - 5 PHÚT WINDOW
# ======================
df_ccu = df_parsed \
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
        count("*").alias("total_records"),
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
        col("total_records"),
        col("computation_time")
    )

print("Query: CCU Analysis by Country (5-minute windows)")

# ======================
#  OUTPUT TO CONSOLE
# ======================
query_ccu = df_ccu.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/ccu_analysis") \
    .option("truncate", "false") \
    .trigger(processingTime="30 seconds") \
    .start()

print("✓ CCU query started")
print("  - Window: 5 minutes")
print("  - Watermark: 10 minutes") 
print("  - Output: Console")
print("  - Processing: Every 30 seconds")

# ======================
#  GLOBAL CCU (tổng hệ thống)
# ======================
df_global_ccu = df_parsed \
    .withWatermark("event_time", "10 minutes") \
    .groupBy(
        window(col("event_time"), "5 minutes", "5 minutes")
    ) \
    .agg(
        approx_count_distinct("steamid").alias("total_unique_players"),
        _sum(when(col("personastate") == 1, 1).otherwise(0)).alias("total_online"),
        _sum(when(col("personastate") == 0, 1).otherwise(0)).alias("total_offline"),
        _sum(when(col("personastate") >= 2, 1).otherwise(0)).alias("total_away"),
        count("*").alias("total_records"),
        current_timestamp().alias("computation_time")
    ) \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("total_unique_players"),
        col("total_online"),
        col("total_offline"),
        col("total_away"),
        ((col("total_online") * 100.0) / col("total_unique_players")).alias("online_percentage"),
        col("total_records"),
        col("computation_time")
    )

query_global = df_global_ccu.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/global_ccu") \
    .option("truncate", "false") \
    .trigger(processingTime="30 seconds") \
    .start()

print("✓ Global CCU query started")
print("  - Aggregation: System-wide metrics")
print("  - Output: Console")

# ======================
#  CHẠY STREAMING
# ======================
print("\n" + "="*60)
print("🚀 STARTING STREAM PROCESSING...")
print("="*60)
print("📊 Monitoring Steam CCU in real-time")
print("⏱️  Processing every 30 seconds")
print("🔄 Press Ctrl+C to stop")
print("="*60)

try:
    # Chờ cả 2 queries
    query_ccu.awaitTermination()
except KeyboardInterrupt:
    print("\n⚠️  Stopping queries...")
    query_ccu.stop()
    query_global.stop()
    
print("✅ CCU Analysis completed")
spark.stop()