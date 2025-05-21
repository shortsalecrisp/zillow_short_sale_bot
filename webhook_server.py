from fastapi import FastAPI, Request
import os, sqlite3, json

from apify_fetcher import fetch_rows
from bot_min import process_rows

REQUIRED_ENV_VARS = ["OPENAI_API_KEY", "SMSM_KEY", "SHEET_URL"]
missing = [v for v in REQUIRED_ENV_VARS if not os.getenv(v)]
if missing:
    raise RuntimeError(", ".join(missing))

app = FastAPI()
EXPORTED_ZPIDS: set[str] = set()

@app.get("/")
def root():
    return {"status": "ok"}

@app.get("/export-zpids")
def export_zpids():
    conn = sqlite3.connect("seen.db")
    conn.execute("CREATE TABLE IF NOT EXISTS listings (zpid TEXT PRIMARY KEY)")
    zpids = [row[0] for row in conn.execute("SELECT zpid FROM listings")]
    conn.close()
    EXPORTED_ZPIDS.update(zpids)
    return {"zpids": zpids}

@app.post("/apify-hook")
async def apify_hook(request: Request):
    payload = await request.json()
    dataset_id = payload.get("dataset_id") or request.query_params.get("dataset_id")
    if not dataset_id:
        return {"error": "dataset_id missing"}
    rows = fetch_rows(dataset_id)
    fresh_rows = [r for r in rows if r.get("zpid") not in EXPORTED_ZPIDS]
    if not fresh_rows:
        return {"status": "no new rows"}
    process_rows(fresh_rows)
    EXPORTED_ZPIDS.update(r.get("zpid") for r in fresh_rows)
    return {"status": "processed", "rows": len(fresh_rows)}

@app.get("/healthz")
def healthz():
    return {"status": "ok"}

