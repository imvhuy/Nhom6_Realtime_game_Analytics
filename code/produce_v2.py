import asyncio
import aiohttp
import json
import csv
from kafka import KafkaProducer

# ======================
#  CẤU HÌNH
# ======================
STEAM_API_KEY = "9D834392B85C588E56E887E7C28FED23"
APP_ID = 730  # ví dụ CS:GO
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
#  KAFKA PRODUCER
# ======================
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: str(k).encode("utf-8")
)

# ======================
#  CÁC HÀM BẤT ĐỒNG BỘ
# ======================
async def get_player_summaries(session, steam_id):
    url = f"https://api.steampowered.com/ISteamUser/GetPlayerSummaries/v2/?key={STEAM_API_KEY}&steamids={steam_id}"
    try:
        async with session.get(url, timeout=10) as resp:
            data = await resp.json()
            players = data.get("response", {}).get("players", [])
            if not players:
                print(f"[!] Người chơi {steam_id} là private hoặc không tồn tại.")
                return None
            return players[0]
    except Exception as e:
        print(f"[Lỗi PlayerSummary] {steam_id}: {e}")
        return None

async def get_user_stats(session, steam_id):
    url = f"https://api.steampowered.com/ISteamUserStats/GetUserStatsForGame/v2/?key={STEAM_API_KEY}&steamid={steam_id}&appid={APP_ID}"
    try:
        async with session.get(url, timeout=10) as resp:
            data = await resp.json()
            stats = data.get("playerstats", {})
            if not stats:
                print(f"[!] Không thể lấy thống kê game của {steam_id} (có thể là private).")
                return None
            return stats
    except Exception as e:
        print(f"[Lỗi UserStats] {steam_id}: {e}")
        return None

async def process_steam_id(session, sid):
    player_info = await get_player_summaries(session, sid)
    if player_info:
        producer.send("steam_players", key=sid, value=player_info)
        print(f"Đã gửi thông tin người chơi {sid} vào topic steam_players")

    user_stats = await get_user_stats(session, sid)
    if user_stats:
        producer.send("steam_userstats", key=sid, value=user_stats)
        print(f"Đã gửi thống kê game {sid} vào topic steam_userstats")

# ======================
#  LUỒNG CHÍNH BẤT ĐỒNG BỘ
# ======================
async def main():
    while True:
        print("\n=== BẮT ĐẦU LẤY DỮ LIỆU ===")
        async with aiohttp.ClientSession() as session:
            # Giới hạn số lượng request song song để tránh bị chặn (rate limit)
            SEMAPHORE = asyncio.Semaphore(20)

            async def limited_process(sid):
                async with SEMAPHORE:
                    await process_steam_id(session, sid)

            tasks = [limited_process(sid) for sid in STEAM_IDS]
            await asyncio.gather(*tasks)

        producer.flush()
        print("=== Hoàn thành một vòng, nghỉ 5 phút ===")
        await asyncio.sleep(300)

if __name__ == "__main__":
    asyncio.run(main())
