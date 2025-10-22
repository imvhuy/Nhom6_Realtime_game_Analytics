# api.py
from flask import Flask, jsonify, request
from pymongo import MongoClient, DESCENDING
from datetime import datetime, timedelta
import os

MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongodb:27017/")
DB_NAME   = os.getenv("DB_NAME", "steam_db")

app = Flask(__name__)
client = MongoClient(MONGO_URI)
db = client[DB_NAME]

def int_q(name, default):
    try:
        return int(request.args.get(name, default))
    except:
        return default

@app.route("/health")
def health():
    return {"status": "ok", "time": datetime.utcnow().isoformat()}

# CCU theo cửa sổ (lấy mới nhất)
@app.route("/ccu/latest")
def ccu_latest():
    limit = int_q("limit", 30)
    docs = list(db.player_ccu.find({}, {"_id": 0})
                .sort("window_start", DESCENDING)
                .limit(limit))
    docs.reverse()  # để line chart đi từ cũ -> mới
    return jsonify(docs)

# ==============================================
# INSIGHTS & ANALYTICS ENDPOINTS
# ==============================================

# 1.Phân tích trạng thái người chơi (persona state distribution)
@app.route("/insights/player-states")
def insights_player_states():
    """
    Thống kê phân bố trạng thái người chơi
    0=Offline, 1=Online, 2=Busy, 3=Away, 4=Snooze, 5=Looking to trade, 6=Looking to play
    """
    pipeline = [
        {"$group": {
            "_id": "$personastate",
            "count": {"$sum": 1},
            "players": {"$push": "$personaname"}
        }},
        {"$sort": {"_id": 1}}
    ]
    
    result = list(db.player_details.aggregate(pipeline))
    
    state_names = {
        0: "Offline", 1: "Online", 2: "Busy", 3: "Away",
        4: "Snooze", 5: "Looking to trade", 6: "Looking to play"
    }
    
    formatted = [{
        "state": state_names.get(doc["_id"], "Unknown"),
        "state_code": doc["_id"],
        "count": doc["count"],
        "percentage": round(doc["count"] / sum(d["count"] for d in result) * 100, 2),
        "sample_players": doc["players"][:5]  # Chỉ lấy 5 người mẫu
    } for doc in result]
    
    return jsonify(formatted)


# 2.Top quốc gia với nhiều người chơi nhất
@app.route("/insights/top-countries")
def insights_top_countries():
    """
    Top countries với breakdown online/offline và state code
    """
    k = int_q("k", 10)
    
    pipeline = [
        {"$group": {
            "_id": {
                "country": "$loccountrycode",
                "state_code": "$locstatecode"
            },
            "total": {"$sum": 1},
            "online": {"$sum": {"$cond": [{"$eq": ["$personastate", 1]}, 1, 0]}},
            "avg_lastlogoff": {"$avg": "$lastlogoff"}
        }},
        {"$group": {
            "_id": "$_id.country",
            "total_players": {"$sum": "$total"},
            "total_online": {"$sum": "$online"},
            "regions": {"$push": {
                "state_code": "$_id.state_code",
                "count": "$total"
            }}
        }},
        {"$sort": {"total_players": -1}},
        {"$limit": k}
    ]
    
    result = list(db.player_details.aggregate(pipeline))
    
    formatted = [{
        "country": doc["_id"] or "Unknown",
        "total_players": doc["total_players"],
        "online_players": doc["total_online"],
        "offline_players": doc["total_players"] - doc["total_online"],
        "online_rate": round(doc["total_online"] / doc["total_players"] * 100, 2) if doc["total_players"] > 0 else 0,
        "top_regions": sorted(doc["regions"], key=lambda x: x["count"], reverse=True)[:3]
    } for doc in result]
    
    return jsonify(formatted)

# 3. Phân tích thời gian tạo tài khoản (account age)
@app.route("/insights/account-age")
def insights_account_age():
    """
    Phân tích độ tuổi tài khoản Steam
    """
    from datetime import datetime
    
    pipeline = [
        {"$match": {"timecreated": {"$exists": True, "$ne": None}}},
        {"$project": {
            "steamid": 1,
            "personaname": 1,
            "timecreated": 1,
            "age_years": {
                "$divide": [
                    {"$subtract": [datetime.now().timestamp(), "$timecreated"]},
                    31536000  # seconds in a year
                ]
            }
        }},
        {"$bucket": {
            "groupBy": "$age_years",
            "boundaries": [0, 1, 3, 5, 10, 15, 20],
            "default": "20+",
            "output": {
                "count": {"$sum": 1},
                "avg_age": {"$avg": "$age_years"}
            }
        }}
    ]
    
    result = list(db.player_details.aggregate(pipeline))
    
    age_labels = {
        0: "< 1 year",
        1: "1-3 years",
        3: "3-5 years",
        5: "5-10 years",
        10: "10-15 years",
        15: "15-20 years",
        "20+": "20+ years"
    }
    
    formatted = [{
        "age_group": age_labels.get(doc["_id"], str(doc["_id"])),
        "count": doc["count"],
        "avg_age_years": round(doc["avg_age"], 1)
    } for doc in result]
    
    return jsonify(formatted)

# ==============================================
# GAME STATS ENDPOINTS (K/D, Items)
# ==============================================

# 4. Top người chơi theo K/D ratio
@app.route("/stats/top-kd")
def stats_top_kd():
    """
    Top người chơi với tỉ lệ K/D cao nhất
    """
    limit = int_q("limit", 20)
    
    pipeline = [
        {"$match": {
            "total_kills": {"$exists": True, "$gt": 0},
            "total_deaths": {"$exists": True, "$gt": 0}
        }},
        {"$project": {
            "steamid": 1,
            "personaname": 1,
            "total_kills": 1,
            "total_deaths": 1,
            "kd_ratio": {
                "$cond": [
                    {"$gt": ["$total_deaths", 0]},
                    {"$divide": ["$total_kills", "$total_deaths"]},
                    "$total_kills"
                ]
            },
            "total_rounds_played": 1,
            "total_mvps": 1,
            "last_updated": 1
        }},
        {"$sort": {"kd_ratio": -1}},
        {"$limit": limit}
    ]
    
    result = list(db.player_game_stats.aggregate(pipeline))
    
    formatted = [{
        "rank": idx + 1,
        "steamid": doc.get("steamid"),
        "player_name": doc.get("personaname", "Unknown"),
        "kills": doc.get("total_kills", 0),
        "deaths": doc.get("total_deaths", 0),
        "kd_ratio": round(doc.get("kd_ratio", 0), 2),
        "rounds_played": doc.get("total_rounds_played", 0),
        "mvps": doc.get("total_mvps", 0),
        "last_updated": doc.get("last_updated")
    } for idx, doc in enumerate(result)]
    
    return jsonify(formatted)

# 5. Thống kê K/D theo khoảng
@app.route("/stats/kd-distribution")
def stats_kd_distribution():
    """
    Phân bố người chơi theo khoảng K/D ratio
    """
    pipeline = [
        {"$match": {
            "total_kills": {"$exists": True},
            "total_deaths": {"$exists": True, "$gt": 0}
        }},
        {"$project": {
            "kd_ratio": {
                "$cond": [
                    {"$gt": ["$total_deaths", 0]},
                    {"$divide": ["$total_kills", "$total_deaths"]},
                    "$total_kills"
                ]
            }
        }},
        {"$bucket": {
            "groupBy": "$kd_ratio",
            "boundaries": [0, 0.5, 1.0, 1.5, 2.0, 3.0, 5.0],
            "default": "5.0+",
            "output": {
                "count": {"$sum": 1},
                "avg_kd": {"$avg": "$kd_ratio"}
            }
        }}
    ]
    
    result = list(db.player_game_stats.aggregate(pipeline))
    
    kd_labels = {
        0: "0.0 - 0.5 (Beginner)",
        0.5: "0.5 - 1.0 (Below Average)",
        1.0: "1.0 - 1.5 (Average)",
        1.5: "1.5 - 2.0 (Good)",
        2.0: "2.0 - 3.0 (Very Good)",
        3.0: "3.0 - 5.0 (Excellent)",
        "5.0+": "5.0+ (Pro)"
    }
    
    formatted = [{
        "kd_range": kd_labels.get(doc["_id"], str(doc["_id"])),
        "player_count": doc["count"],
        "avg_kd": round(doc["avg_kd"], 2)
    } for doc in result]
    
    return jsonify(formatted)

# 6. Top vật phẩm phổ biến nhất (weapons)
@app.route("/stats/top-weapons")
def stats_top_weapons():
    """
    Top vũ khí được sử dụng nhiều nhất (dựa trên kills)
    """
    limit = int_q("limit", 15)
    
    pipeline = [
        {"$match": {"weapon_stats": {"$exists": True}}},
        {"$unwind": "$weapon_stats"},
        {"$group": {
            "_id": "$weapon_stats.weapon_name",
            "total_kills": {"$sum": "$weapon_stats.kills"},
            "total_shots": {"$sum": "$weapon_stats.shots"},
            "total_hits": {"$sum": "$weapon_stats.hits"},
            "users_count": {"$sum": 1}
        }},
        {"$project": {
            "weapon_name": "$_id",
            "total_kills": 1,
            "total_shots": 1,
            "total_hits": 1,
            "users_count": 1,
            "accuracy": {
                "$cond": [
                    {"$gt": ["$total_shots", 0]},
                    {"$multiply": [
                        {"$divide": ["$total_hits", "$total_shots"]},
                        100
                    ]},
                    0
                ]
            },
            "avg_kills_per_user": {
                "$divide": ["$total_kills", "$users_count"]
            }
        }},
        {"$sort": {"total_kills": -1}},
        {"$limit": limit}
    ]
    
    result = list(db.player_game_stats.aggregate(pipeline))
    
    formatted = [{
        "rank": idx + 1,
        "weapon": doc.get("weapon_name", "Unknown"),
        "total_kills": doc.get("total_kills", 0),
        "total_shots": doc.get("total_shots", 0),
        "total_hits": doc.get("total_hits", 0),
        "accuracy": round(doc.get("accuracy", 0), 2),
        "users": doc.get("users_count", 0),
        "avg_kills_per_user": round(doc.get("avg_kills_per_user", 0), 1)
    } for idx, doc in enumerate(result)]
    
    return jsonify(formatted)

# 7. Top map phổ biến
@app.route("/stats/top-maps")
def stats_top_maps():
    """
    Top bản đồ được chơi nhiều nhất
    """
    limit = int_q("limit", 10)
    
    pipeline = [
        {"$match": {"map_stats": {"$exists": True}}},
        {"$unwind": "$map_stats"},
        {"$group": {
            "_id": "$map_stats.map_name",
            "total_rounds": {"$sum": "$map_stats.rounds_played"},
            "total_wins": {"$sum": "$map_stats.rounds_won"},
            "players_count": {"$sum": 1}
        }},
        {"$project": {
            "map_name": "$_id",
            "total_rounds": 1,
            "total_wins": 1,
            "players_count": 1,
            "win_rate": {
                "$cond": [
                    {"$gt": ["$total_rounds", 0]},
                    {"$multiply": [
                        {"$divide": ["$total_wins", "$total_rounds"]},
                        100
                    ]},
                    0
                ]
            },
            "avg_rounds_per_player": {
                "$divide": ["$total_rounds", "$players_count"]
            }
        }},
        {"$sort": {"total_rounds": -1}},
        {"$limit": limit}
    ]
    
    result = list(db.player_game_stats.aggregate(pipeline))
    
    formatted = [{
        "rank": idx + 1,
        "map": doc.get("map_name", "Unknown"),
        "total_rounds": doc.get("total_rounds", 0),
        "total_wins": doc.get("total_wins", 0),
        "win_rate": round(doc.get("win_rate", 0), 2),
        "players": doc.get("players_count", 0),
        "avg_rounds_per_player": round(doc.get("avg_rounds_per_player", 0), 1)
    } for idx, doc in enumerate(result)]
    
    return jsonify(formatted)

# 8. Player profile với full stats
@app.route("/stats/player/<steamid>")
def stats_player_profile(steamid):
    """
    Thông tin chi tiết về một người chơi cụ thể
    """
    player = db.player_game_stats.find_one({"steamid": steamid}, {"_id": 0})
    
    if not player:
        return jsonify({"error": "Player not found"}), 404
    
    # Tính toán thêm các metrics
    kills = player.get("total_kills", 0)
    deaths = player.get("total_deaths", 1)
    kd_ratio = round(kills / deaths if deaths > 0 else kills, 2)
    
    headshot_kills = player.get("total_kills_headshot", 0)
    headshot_rate = round(headshot_kills / kills * 100 if kills > 0 else 0, 2)
    
    profile = {
        "steamid": steamid,
        "player_name": player.get("personaname", "Unknown"),
        "overall_stats": {
            "kills": kills,
            "deaths": deaths,
            "kd_ratio": kd_ratio,
            "headshot_kills": headshot_kills,
            "headshot_rate": headshot_rate,
            "rounds_played": player.get("total_rounds_played", 0),
            "mvps": player.get("total_mvps", 0),
            "wins": player.get("total_wins", 0),
            "damage": player.get("total_damage_done", 0)
        },
        "weapon_stats": player.get("weapon_stats", []),
        "map_stats": player.get("map_stats", []),
        "last_updated": player.get("last_updated")
    }
    
    return jsonify(profile)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
