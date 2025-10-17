from pathlib import Path
import os
import argparse

import pandas as pd
from pymongo import MongoClient

SCRIPT_DIR = Path(__file__).resolve().parent.parent / "data"
CLEAN_CSV = SCRIPT_DIR / "cyberbullying_clean.csv"
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = os.environ.get("MONGO_DB", "harassment")
COLLECTION_NAME = os.environ.get("MONGO_COLLECTION", "posts")


def load_csv():
    if not CLEAN_CSV.exists():
        raise FileNotFoundError(f"{CLEAN_CSV} not found. Run preprocess.py first.")
    df = pd.read_csv(CLEAN_CSV, encoding="utf-8", low_memory=False)
    return df


def connect_mongo(uri):
    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    client.server_info()
    return client


def ensure_unique_index(coll, field):
    """
    Remove exact duplicates on `field` (keep first) then create a unique index.
    Safe to call even if duplicates exist.
    """
    pipeline = [
        {"$group": {"_id": f"${field}", "count": {"$sum": 1}, "ids": {"$push": "$_id"}}},
        {"$match": {"_id": {"$ne": None}, "count": {"$gt": 1}}}
    ]
    try:
        duplicates = list(coll.aggregate(pipeline))
    except Exception as e:
        print(f"Warning: error while detecting duplicates for field '{field}': {e}")
        duplicates = []

    if duplicates:
        removed = 0
        for d in duplicates:
            ids = d.get("ids", [])
            keep = ids[0]
            remove = ids[1:]
            if remove:
                res = coll.delete_many({"_id": {"$in": remove}})
                removed += res.deleted_count
        print(f"Info: {removed} documents removed to deduplicate on '{field}'")

    try:
        coll.create_index([(field, 1)], unique=True)
        print(f"Success: created unique index on '{field}'")
    except Exception as e:
        print(f"Warning: could not create unique index on '{field}': {e}")


def upsert_records(df, by="texte"):
    client = connect_mongo(MONGO_URI)
    db = client[DB_NAME]
    coll = db[COLLECTION_NAME]

    ensure_unique_index(coll, "id_post")
    try:
        coll.create_index([("texte", 1)])
    except Exception:
        pass

    records = df[["id_post", "texte", "Type", "Label"]].to_dict(orient="records")
    count = 0
    for r in records:
        key = {by: r.get(by)}
        if by == "id_post" and key.get("id_post") is not None:
            try:
                key["id_post"] = int(key["id_post"])
            except Exception:
                pass
        try:
            coll.replace_one(key, r, upsert=True)
            count += 1
        except Exception as e:
            print(f"Error upserting key {key}: {e}")

    print(f"Success: {count} documents upserted into {DB_NAME}.{COLLECTION_NAME} (MONGO_URI={MONGO_URI})")
    client.close()


def main():
    parser = argparse.ArgumentParser(description="Load cyberbullying_clean.csv into MongoDB")
    parser.add_argument("--by", choices=["texte", "id_post"], default="texte",
                        help="upsert key: 'texte' (default) or 'id_post'")
    args = parser.parse_args()

    df = load_csv()
    upsert_records(df, by=args.by)


if __name__ == "__main__":
    main()
