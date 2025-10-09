import json
import time
import requests
import csv
from kafka import KafkaProducer

# ======================
#  CẤU HÌNH
# ======================
STEAM_API_KEY = "9D834392B85C588E56E887E7C28FED23"
APP_ID = 730  # CS:GO
KAFKA_BROKER = ["localhost:19092", "localhost:19093"]
CSV_PATH = r"d:\4th\StreamingBigdata\final\CK\code\steam_ids.csv"

# ======================
#  ĐỌC DANH SÁCH STEAM ID
# ======================
def load_steam_ids(csv_file):
    steam_ids = []
    with open(csv_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            steam_ids.append(row["steamid"].strip())
    return steam_ids

STEAM_IDS = load_steam_ids(CSV_PATH)
print(f"Đã nạp {len(STEAM_IDS)} steamid từ file {CSV_PATH}")

# ======================
#  KHAI BÁO PRODUCER
# ======================
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: str(k).encode("utf-8")
)

# ======================
#  GỌI API AN TOÀN
# ======================
def get_player_summaries(steam_id):
    url = f"https://api.steampowered.com/ISteamUser/GetPlayerSummaries/v2/?key={STEAM_API_KEY}&steamids={steam_id}"
    r = requests.get(url)
    r.raise_for_status()
    data = r.json()
    players = data.get("response", {}).get("players", [])
    if not players:
        print(f"[!] Người chơi {steam_id} là private hoặc không tồn tại.")
        return None
    return players[0]

def get_user_stats(steam_id, appid):
    url = f"https://api.steampowered.com/ISteamUserStats/GetUserStatsForGame/v2/?key={STEAM_API_KEY}&steamid={steam_id}&appid={appid}"
    r = requests.get(url)
    data = r.json()
    stats = data.get("playerstats", {})
    if not stats:  # Nếu người chơi private stats
        print(f"[!] Không thể lấy thống kê game của {steam_id} (có thể là private).")
        return None
    return stats

# ======================
#  LUỒNG CHÍNH
# ======================
while True:
    for sid in STEAM_IDS:
        try:
            # ---- LẤY VÀ GỬI THÔNG TIN NGƯỜI CHƠI ----
            player_info = get_player_summaries(sid)
            if player_info:  # Chỉ gửi nếu có dữ liệu
                producer.send("steam_players", key=sid, value=player_info)
                print(f"Đã gửi thông tin người chơi {sid} vào topic steam_players")

            # ---- LẤY VÀ GỬI THỐNG KÊ GAME ----
            user_stats = get_user_stats(sid, APP_ID)
            if user_stats:  # Chỉ gửi nếu có dữ liệu
                producer.send("steam_userstats", key=sid, value=user_stats)
                print(f"Đã gửi thống kê game {sid} vào topic steam_userstats")

            producer.flush()

        except requests.exceptions.HTTPError as http_err:
            print(f"[HTTP Error] {sid}: {http_err}")
        except Exception as e:
            print(f"[Lỗi khác] {sid}: {e}")

    time.sleep(300)
