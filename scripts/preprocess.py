import re
from pathlib import Path
import os
import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
CSV_PATH = SCRIPT_DIR / "cyberbullying.csv"
OUT_PATH = SCRIPT_DIR / "cyberbullying_clean.csv"

if not CSV_PATH.exists():
    raise FileNotFoundError(f"File not found: {CSV_PATH}. Place the CSV here or update CSV_PATH.")

df = pd.read_csv(CSV_PATH, encoding="utf-8", low_memory=False, na_values=["", "NA", "NaN", None])

col_map = {c.lower(): c for c in df.columns}
if "text" not in col_map or "label" not in col_map:
    raise KeyError(f"Required columns missing. Found columns: {list(df.columns)}")

df = df.rename(columns={
    col_map.get("text"): "texte",
    col_map.get("label"): "Label",
    col_map.get("types"): "Type"
})

if "Type" not in df.columns:
    df["Type"] = None

df = df.reset_index(drop=True)
df["id_post"] = df.index + 1

_url_re = re.compile(r"https?://\S+|www\.\S+", flags=re.IGNORECASE)
_email_re = re.compile(r"\S+@\S+\.\S+", flags=re.IGNORECASE)
_ctrl_re = re.compile(r"[\r\n\t]+")
_multi_space_re = re.compile(r"\s{2,}")

def clean_text(s):
    if pd.isna(s):
        return ""
    s = str(s)
    s = _url_re.sub(" ", s)
    s = _email_re.sub(" ", s)
    s = _ctrl_re.sub(" ", s)
    s = _multi_space_re.sub(" ", s)
    return s.strip()

df["texte"] = df["texte"].apply(clean_text)

df["Label"] = df["Label"].astype(str).fillna("Unknown")
df["Label"] = df["Label"].str.strip().replace({"nan": "Unknown"})
df["Label"] = df["Label"].str.replace(r"\s+", "-", regex=True).str.replace(r"-{2,}", "-", regex=True)

df["Type"] = df["Type"].astype(str).fillna("Unknown")
df["Type"] = df["Type"].str.strip().replace({"": "Unknown", "nan": "Unknown"})

df = df.drop_duplicates(subset=["texte"], keep="first").reset_index(drop=True)

df.to_csv(OUT_PATH, index=False, encoding="utf-8")
print(f"Preprocessing complete — {len(df)} rows saved to {OUT_PATH}")
