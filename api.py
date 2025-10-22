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

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
