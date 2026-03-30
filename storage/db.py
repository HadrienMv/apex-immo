"""
MongoDB storage — biens scrapés, historique prix, transactions DVF.
"""
from pymongo import MongoClient, DESCENDING
from datetime import datetime
import os

MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb+srv://hadrien:Hadrien123@cluster0.qaa6v36.mongodb.net"
)
DB_NAME = "apex_scanner"

_client = None


def get_db():
    global _client
    if _client is None:
        _client = MongoClient(MONGO_URI)
    return _client[DB_NAME]


def init_db():
    db = get_db()
    # Indexes
    db.biens.create_index("url", unique=True)
    db.biens.create_index([("score_arbitrage", DESCENDING)])
    db.biens.create_index("verdict")
    db.biens.create_index("commune")
    db.biens.create_index("code_insee")
    db.dvf_cache.create_index([("code_insee", 1), ("annee_min", 1)], unique=True)


def upsert_bien(bien: dict) -> str:
    """Insert or update a bien. Returns the id."""
    db = get_db()
    now = datetime.now().isoformat()

    existing = db.biens.find_one({"url": bien.get("url")})

    if existing:
        # Track price drops
        old_prix = existing.get("prix")
        new_prix = bien.get("prix")
        historique = existing.get("historique_prix", [])
        nb_baisses = existing.get("nb_baisses", 0)

        if new_prix and old_prix and new_prix < old_prix:
            nb_baisses += 1
            historique.append({"date": now, "prix": new_prix})

        bien["historique_prix"] = historique
        bien["nb_baisses"] = nb_baisses
        bien["derniere_vue"] = now
        bien["updated_at"] = now

        db.biens.update_one({"url": bien["url"]}, {"$set": bien})
        return str(existing["_id"])
    else:
        bien.setdefault("premiere_vue", now)
        bien.setdefault("derniere_vue", now)
        bien.setdefault("historique_prix", [])
        bien.setdefault("nb_baisses", 0)
        bien.setdefault("statut", "nouveau")
        bien.setdefault("created_at", now)
        result = db.biens.insert_one(bien)
        return str(result.inserted_id)


def get_biens(verdict: str = None, limit: int = 50) -> list[dict]:
    db = get_db()
    query = {"verdict": verdict} if verdict else {}
    cursor = db.biens.find(query).sort("score_arbitrage", DESCENDING).limit(limit)
    results = []
    for doc in cursor:
        doc["_id"] = str(doc["_id"])
        results.append(doc)
    return results


def get_stats() -> dict:
    db = get_db()
    total = db.biens.count_documents({})
    pipeline_verdict = [{"$group": {"_id": "$verdict", "count": {"$sum": 1}}}]
    pipeline_source = [{"$group": {"_id": "$source", "count": {"$sum": 1}}}]
    by_verdict = {r["_id"]: r["count"] for r in db.biens.aggregate(pipeline_verdict)}
    by_source = {r["_id"]: r["count"] for r in db.biens.aggregate(pipeline_source)}
    return {"total": total, "by_verdict": by_verdict, "by_source": by_source}


def cache_dvf(code_insee: str, annee_min: int, data: dict):
    db = get_db()
    db.dvf_cache.update_one(
        {"code_insee": code_insee, "annee_min": annee_min},
        {"$set": {"data": data, "fetched_at": datetime.now().isoformat()}},
        upsert=True,
    )


def get_dvf_cache(code_insee: str, annee_min: int) -> dict | None:
    db = get_db()
    doc = db.dvf_cache.find_one({"code_insee": code_insee, "annee_min": annee_min})
    return doc.get("data") if doc else None


# Init on import
init_db()
