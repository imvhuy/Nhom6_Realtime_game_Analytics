"""
Script đơn giản để tính CCU và lưu vào MongoDB
Không cần Spark - chỉ dùng Kafka Consumer thuần
"""

import json
import time
from datetime import datetime
from kafka import KafkaConsumer
from pymongo import MongoClient
from collections import defaultdict

# ======================
#  CẤU HÌNH
# ======================
KAFKA_BROKER = ["localhost:19092", "localhost:19093"]
KAFKA_TOPIC = "steam_players"
MONGO_URI = "mongodb://localhost:27017"
MONGO_DB = "steam_db"
MONGO_COLLECTION = "player_ccu"

# ======================
#  KẾT NỐI KAFKA & MONGODB
# ======================
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',  # Đọc từ đầu nếu chưa có offset
    enable_auto_commit=True,
    group_id='ccu-calculator-group-v2'  # Đổi group_id để reset offset
)

mongo_client = MongoClient(MONGO_URI)
db = mongo_client[MONGO_DB]
collection = db[MONGO_COLLECTION]
player_collection = db["player_details"]

print(f"✓ Đã kết nối Kafka topic: {KAFKA_TOPIC}")
print(f"✓ Đã kết nối MongoDB: {MONGO_URI}/{MONGO_DB}")
print("=" * 60)

# ======================
#  BIẾN ĐẾM CCU
# ======================
player_states = {}  # {steamid: {"personastate": state, "data": full_data}}
last_save_time = time.time()
SAVE_INTERVAL = 30  # Lưu CCU mỗi 5 phút

def calculate_ccu():
    """
    Tính số người chơi online/offline
    personastate: 0 = Offline, 1 = Online, 2 = Away/Busy/etc.
    """
    total = len(player_states)
    online = sum(1 for p in player_states.values() if p.get("personastate", 0) == 1)
    offline = sum(1 for p in player_states.values() if p.get("personastate", 0) == 0)
    away = total - online - offline  # Trạng thái khác (Away, Busy, Snooze...)
    
    # Thống kê theo quốc gia
    country_stats = defaultdict(int)
    country_online = defaultdict(int)
    for p in player_states.values():
        country = p.get("loccountrycode", "Unknown")
        country_stats[country] += 1
        if p.get("personastate", 0) == 1:
            country_online[country] += 1
    
    return {
        "timestamp": datetime.now(),
        "total_players": total,
        "online_players": online,
        "offline_players": offline,
        "away_players": away,
        "country_stats": dict(country_stats),
        "country_online": dict(country_online),
        "top_countries": sorted(country_stats.items(), key=lambda x: x[1], reverse=True)[:10]
    }

def save_ccu_to_mongo():
    """
    Lưu dữ liệu CCU vào MongoDB
    """
    ccu_data = calculate_ccu()
    result = collection.insert_one(ccu_data)
    
    print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] CCU Statistics:")
    print(f"  Tổng: {ccu_data['total_players']} người")
    print(f"  Online (state=1): {ccu_data['online_players']} người")
    print(f"  Offline (state=0): {ccu_data['offline_players']} người")
    print(f"  Away/Busy (state=2+): {ccu_data['away_players']} người")
    print(f"  Top quốc gia: {ccu_data['top_countries'][:3]}")
    print(f"  Đã lưu vào MongoDB (ID: {result.inserted_id})")
    print("=" * 60)

# ======================
#  LUỒNG CHÍNH
# ======================
print("Đang lắng nghe dữ liệu từ Kafka...")
print(f"Sẽ tính và lưu CCU mỗi {SAVE_INTERVAL} giây\n")

try:
    for message in consumer:
        player_data = message.value
        steamid = player_data.get("steamid")
        
        if steamid:
            # Cập nhật trạng thái người chơi
            player_states[steamid] = {
                "personastate": player_data.get("personastate", 0),
                "personaname": player_data.get("personaname", "Unknown"),
                "loccountrycode": player_data.get("loccountrycode", "Unknown"),
                "avatar": player_data.get("avatar", ""),
                "profileurl": player_data.get("profileurl", ""),
                "last_updated": datetime.now()
            }
            
            # Lưu chi tiết người chơi vào collection riêng
            player_collection.update_one(
                {"steamid": steamid},
                {"$set": {
                    **player_data,
                    "last_seen": datetime.now()
                }},
                upsert=True
            )
            
            state = player_data.get('personastate', 0)
            state_text = {0: "Offline", 1: "Online", 2: "Busy", 3: "Away", 4: "Snooze", 5: "Looking to trade", 6: "Looking to play"}.get(state, "Unknown")
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Cập nhật: {player_data.get('personaname', 'Unknown')} "
                  f"({steamid}) - {state_text} (state={state})")
        
        # Kiểm tra xem đã đến lúc lưu CCU chưa
        current_time = time.time()
        if current_time - last_save_time >= SAVE_INTERVAL:
            save_ccu_to_mongo()
            last_save_time = current_time

except KeyboardInterrupt:
    print("\n\nĐang dừng consumer...")
    # Lưu CCU lần cuối trước khi thoát
    save_ccu_to_mongo()
    consumer.close()
    mongo_client.close()
    print("✓ Đã đóng kết nối. Tạm biệt!")

except Exception as e:
    print(f"\nLỗi: {e}")
    consumer.close()
    mongo_client.close()
