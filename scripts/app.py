from fastapi import FastAPI
from pydantic import BaseModel
from scripts.nlp_pipeline import ensure_nlp_resources, detect_language, sentiment_vader

sid = ensure_nlp_resources()

app = FastAPI(title="NLP Pipeline API", version="1.0")

class TextItem(BaseModel):
    texte: str

@app.post("/analyze")
def analyze_text(item: TextItem):
    texte = item.texte
    lang = detect_language(texte)
    sentiment, scores, tokens = sentiment_vader(sid, texte)
    return {
        "texte": texte,
        "language": lang,
        "sentiment": sentiment,
        "sentiment_scores": scores,
        "sentiment_tokens": tokens
    }

@app.get("/")
def root():
    return {"message": "NLP Pipeline API is running"}
