import os
from pymongo import MongoClient
from elasticsearch import Elasticsearch, helpers
from datetime import datetime
import sys
import json

# Config via env
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.environ.get("MONGO_DB", "harassment")  # adjust if your DB is 'harassment'
MONGO_COLL = os.environ.get("MONGO_COLLECTION", "posts")

ES_HOST = os.environ.get("ES_HOST", "http://localhost:9200")
ES_USER = os.environ.get("ES_USER", "elastic")
ES_PASS = os.environ.get("ES_PASS", "changeme123")
INDEX_NAME = os.environ.get("ES_INDEX", "harcelement_posts")

# Connect
mongo_client = MongoClient(MONGO_URI)
db = mongo_client[MONGO_DB]
collection = db[MONGO_COLL]

# Elasticsearch using basic_auth (http_auth is deprecated)
es = Elasticsearch(ES_HOST, basic_auth=(ES_USER, ES_PASS))

def gen_actions():
    for doc in collection.find():
        yield {
            "_index": INDEX_NAME,
            "_id": str(doc.get("id_post") or doc.get("_id")),
            "_source": {
                "id_post": doc.get("id_post"),
                "titre": doc.get("titre", ""),
                "texte": doc.get("texte", ""),
                "language": doc.get("language", ""),
                "type": doc.get("Type", ""),
                "sentiment": doc.get("sentiment", ""),
                "score": float(doc.get("sentiment_scores", {}).get("compound", 0.0)),
                "date": doc.get("date", datetime.now().isoformat()),
                "source": "dataset_kaggle"
            }
        }

if __name__ == "__main__":
    # sanity checks
    try:
        count = collection.count_documents({})
    except Exception as e:
        print("Error counting documents in Mongo:", e)
        sys.exit(1)

    print(f"MongoDB: {MONGO_URI}  DB: {MONGO_DB}  Collection: {MONGO_COLL}  -> documents={count}")

    if count == 0:
        sample_doc = collection.find_one()
        print("No documents to import. sample find_one =>", json.dumps(sample_doc, default=str, indent=2))
        print("Check DB/collection name or run the loader script to populate Mongo.")
        sys.exit(0)

    # ensure ES reachable
    if not es.ping():
        print("Elasticsearch not reachable at", ES_HOST)
        sys.exit(1)

    # create index if missing (ignore if exists)
    try:
        es.indices.create(index=INDEX_NAME, ignore=400)
    except Exception as e:
        print("Warning: could not create index:", e)

    try:
        success, errors = helpers.bulk(es, gen_actions(), chunk_size=500, request_timeout=60)
        print("Documents imported:", success)
        if errors:
            print("Sample errors:", errors[:3])
    except Exception as e:
        print("Bulk import failed:", e)
        sys.exit(1)
