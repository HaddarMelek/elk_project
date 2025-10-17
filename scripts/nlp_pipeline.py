"""
nlp_pipeline.py

Purpose:
 - Detect language and analyze sentiment for documents stored in MongoDB (harassment.posts)
 - Or process the cleaned CSV (data/cyberbullying_clean.csv) and optionally upsert results to MongoDB

Usage:
  python3 scripts/nlp_pipeline.py                # process MongoDB (default)
  python3 scripts/nlp_pipeline.py --source csv   # process CSV and upsert results
  python3 scripts/nlp_pipeline.py --force        # recompute even if fields exist
  python3 scripts/nlp_pipeline.py --sample 100   # process only 100 docs (for testing)
"""
from pathlib import Path
import os
import argparse
import logging
from typing import Dict, Any, List

# Optional deps
try:
    from langdetect import detect, LangDetectException
except Exception:
    detect = None
    LangDetectException = Exception

try:
    import nltk
    from nltk.sentiment import SentimentIntensityAnalyzer
except Exception:
    nltk = None
    SentimentIntensityAnalyzer = None

try:
    import pandas as pd
except Exception:
    pd = None

try:
    from pymongo import MongoClient
except Exception:
    MongoClient = None

# Config
SCRIPT_DIR = Path(__file__).resolve().parent.parent / "data"
CLEAN_CSV = SCRIPT_DIR / "cyberbullying_clean.csv"

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.environ.get("MONGO_DB", "harassment")
MONGO_COLL = os.environ.get("MONGO_COLLECTION", "posts")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("nlp_pipeline")


def ensure_nlp_resources():
    if nltk is None or SentimentIntensityAnalyzer is None:
        raise RuntimeError("nltk not installed. Install with: pip install nltk")
    try:
        nltk.download("vader_lexicon", quiet=True)
    except Exception:
        logger.warning("Could not auto-download vader_lexicon.")
    return SentimentIntensityAnalyzer()


def detect_language(text: str) -> str:
    if detect is None:
        return "unknown"
    try:
        if not text or not str(text).strip():
            return "unknown"
        return detect(text)
    except LangDetectException:
        return "unknown"
    except Exception:
        return "unknown"


def sentiment_vader(sid, text: str):
    if not text or not str(text).strip():
        scores = {"neg": 0.0, "neu": 1.0, "pos": 0.0, "compound": 0.0}
        return "neutral", scores, []
    scores = sid.polarity_scores(str(text))
    comp = scores.get("compound", 0.0)
    if comp >= 0.05:
        label = "positive"
    elif comp <= -0.05:
        label = "negative"
    else:
        label = "neutral"

    # token contributions via lexicon lookup (top contributors)
    tokens: List[Dict[str, Any]] = []
    try:
        lex = getattr(sid, "lexicon", {}) or {}
        token_vals = []
        for t in str(text).split():
            tt = t.lower().strip(".,!?;:'\"()[]{}")
            if tt in lex:
                token_vals.append((tt, lex[tt]))
        token_vals = sorted(token_vals, key=lambda x: -abs(x[1]))[:8]
        tokens = [{"token": k, "value": v} for k, v in token_vals]
    except Exception:
        tokens = []

    return label, scores, tokens


def connect_mongo(uri: str):
    if MongoClient is None:
        raise RuntimeError("pymongo not installed. Install with: pip install pymongo")
    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    client.server_info()
    return client


def process_from_csv(sid, force=False, sample=None, upsert=False):
    if pd is None:
        raise RuntimeError("pandas not installed. Install with: pip install pandas")
    if not CLEAN_CSV.exists():
        raise FileNotFoundError(f"{CLEAN_CSV} not found. Run preprocess.py first.")
    df = pd.read_csv(CLEAN_CSV, encoding="utf-8", low_memory=False)
    if sample:
        df = df.head(sample)

    results = []
    for _, row in df.iterrows():
        texte = row.get("texte", "") or ""
        lang = detect_language(texte)
        sentiment, scores, tokens = sentiment_vader(sid, texte)
        doc = {
            "id_post": int(row.get("id_post")) if pd.notna(row.get("id_post")) else None,
            "texte": texte,
            "Type": row.get("Type"),
            "Label": row.get("Label"),
            "language": lang,
            "sentiment": sentiment,
            "sentiment_scores": scores,
            "sentiment_tokens": tokens,
        }
        results.append(doc)

    if upsert:
        client = connect_mongo(MONGO_URI)
        db = client[MONGO_DB]
        coll = db[MONGO_COLL]
        count = 0
        for r in results:
            key = {"id_post": r.get("id_post")} if r.get("id_post") is not None else {"texte": r.get("texte")}
            coll.replace_one(key, r, upsert=True)
            count += 1
        client.close()
        logger.info(f"{count} documents upserted into {MONGO_DB}.{MONGO_COLL}")

    return results


def process_from_mongo(sid, force=False, batch=500, sample=None):
    client = connect_mongo(MONGO_URI)
    db = client[MONGO_DB]
    coll = db[MONGO_COLL]

    cursor = coll.find({}, no_cursor_timeout=True).batch_size(batch)
    total = 0
    updated = 0
    for doc in cursor:
        total += 1
        if sample and total > sample:
            break
        need = force or ("language" not in doc) or ("sentiment" not in doc)
        if not need:
            continue
        texte = doc.get("texte", "") or ""
        lang = detect_language(texte)
        sentiment, scores, tokens = sentiment_vader(sid, texte)
        update = {
            "language": lang,
            "sentiment": sentiment,
            "sentiment_scores": scores,
            "sentiment_tokens": tokens,
        }
        try:
            coll.update_one({"_id": doc["_id"]}, {"$set": update})
            updated += 1
        except Exception as e:
            logger.warning(f"Error updating _id={doc.get('_id')}: {e}")

    cursor.close()
    client.close()
    return {"scanned": total, "updated": updated}


def main():
    parser = argparse.ArgumentParser(description="NLP pipeline: language detection + sentiment (VADER)")
    parser.add_argument("--source", choices=["mongo", "csv"], default="mongo",
                        help="source to process: 'mongo' (default) or 'csv'")
    parser.add_argument("--force", action="store_true", help="recompute even if fields already present")
    parser.add_argument("--batch", type=int, default=500, help="MongoDB cursor batch size")
    parser.add_argument("--sample", type=int, default=0, help="process only N documents (for testing)")
    parser.add_argument("--upsert", action="store_true",
                        help="when --source csv: upsert results to MongoDB after processing")
    args = parser.parse_args()

    try:
        sid = ensure_nlp_resources()
    except Exception as e:
        logger.error(f"NLP resources missing: {e}")
        return

    if args.source == "csv":
        results = process_from_csv(sid, force=args.force, sample=(args.sample or None), upsert=args.upsert)
        logger.info(f"{len(results)} rows processed from CSV")
    else:
        stats = process_from_mongo(sid, force=args.force, batch=args.batch, sample=(args.sample or None))
        logger.info(f"Mongo processing finished — scanned={stats['scanned']} updated={stats['updated']}")


if __name__ == "__main__":
    main()