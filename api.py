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

## một số querry mẫu khi xài nhập URL là http://mongo-api:5000/players/list
# Thêm vào sau endpoint /ccu/latest (sau dòng 32)

# Lấy danh sách người chơi (có phân trang)
# @app.route("/players/list")
# def players_list():
#     limit = int_q("limit", 50)
#     skip = int_q("skip", 0)
    
#     # Lọc theo trạng thái (optional)
#     state = request.args.get("state")  # 0=offline, 1=online
#     query = {}
#     if state is not None:
#         query["personastate"] = int(state)
    
#     docs = list(db.player_details.find(query, {"_id": 0})
#                 .sort("last_seen", DESCENDING)
#                 .skip(skip)
#                 .limit(limit))
    
#     total = db.player_details.count_documents(query)
    
#     return jsonify({
#         "total": total,
#         "limit": limit,
#         "skip": skip,
#         "data": docs
#     })

# # Lấy thông tin 1 người chơi cụ thể
# @app.route("/players/<steamid>")
# def player_detail(steamid):
#     player = db.player_details.find_one({"steamid": steamid}, {"_id": 0})
#     if not player:
#         return jsonify({"error": "Player not found"}), 404
#     return jsonify(player)

# # Thống kê người chơi theo quốc gia
# @app.route("/players/by-country")
# def players_by_country():
#     pipeline = [
#         {"$group": {
#             "_id": "$loccountrycode",
#             "count": {"$sum": 1},
#             "online": {
#                 "$sum": {"$cond": [{"$eq": ["$personastate", 1]}, 1, 0]}
#             },
#             "offline": {
#                 "$sum": {"$cond": [{"$eq": ["$personastate", 0]}, 1, 0]}
#             }
#         }},
#         {"$sort": {"count": -1}},
#         {"$limit": 20}
#     ]
    
#     result = list(db.player_details.aggregate(pipeline))
    
#     # Format lại output
#     formatted = [{
#         "country": doc["_id"] or "Unknown",
#         "total": doc["count"],
#         "online": doc["online"],
#         "offline": doc["offline"]
#     } for doc in result]
    
#     return jsonify(formatted)

# # Top người chơi online gần đây
# @app.route("/players/online")
# def players_online():
#     limit = int_q("limit", 20)
    
#     docs = list(db.player_details.find(
#         {"personastate": 1},  # chỉ lấy online
#         {"_id": 0}
#     ).sort("last_seen", DESCENDING).limit(limit))
    
#     return jsonify(docs)

# # Search người chơi theo tên
# @app.route("/players/search")
# def players_search():
#     name = request.args.get("name", "")
#     limit = int_q("limit", 20)
    
#     if not name:
#         return jsonify({"error": "Missing 'name' parameter"}), 400
    
#     # Case-insensitive search
#     docs = list(db.player_details.find(
#         {"personaname": {"$regex": name, "$options": "i"}},
#         {"_id": 0}
#     ).limit(limit))
    
#     return jsonify(docs)



# # Top quốc gia ở cửa sổ mới nhất
# @app.route("/country/top")
# def country_top():
#     k = int_q("k", 10)
#     latest_win = db.country_stats.find_one(sort=[("window_start", DESCENDING)])
#     if not latest_win:
#         return jsonify([])
#     win_start = latest_win["window_start"]
#     docs = list(db.country_stats.find(
#         {"window_start": win_start}, {"_id": 0}
#     ).sort("cnt", DESCENDING).limit(k))
#     return jsonify(docs)

# # Doanh thu theo giờ (nếu bạn có/sim item_events)
# @app.route("/revenue/hourly")
# def revenue_hourly():
#     limit = int_q("limit", 24)
#     docs = list(db.item_hourly_rev.find({}, {"_id": 0})
#                 .sort("hour", DESCENDING)
#                 .limit(limit))
#     docs.reverse()
#     return jsonify(docs)

# # Top items gần đây
# @app.route("/items/top")
# def items_top():
#     k = int_q("k", 10)
#     pipeline = [
#         {"$sort": {"hour": -1}},
#         {"$limit": 6},  # gom vài giờ gần nhất
#         {"$unwind": "$top_items"},
#         {"$project": {
#             "hour": 1,
#             "item": {"$arrayElemAt": ["$top_items", 0]},
#             "value": {"$arrayElemAt": ["$top_items", 1]}
#         }},
#         {"$sort": {"value": -1}},
#         {"$limit": k}
#     ]
#     docs = list(db.item_hourly_rev.aggregate(pipeline))
#     return jsonify(docs)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
