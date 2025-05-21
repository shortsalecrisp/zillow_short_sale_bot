from fastapi import FastAPI, Request, HTTPException
import os, sqlite3
from apify_fetcher import fetch_rows
from bot_min import process_rows

REQUIRED_ENV_VARS = ["OPENAI_API_KEY", "SMSM_KEY", "SHEET_URL"]
missing = [var for var in REQUIRED_ENV_VARS if not os.getenv(var)]
if missing:
    raise RuntimeError(f"Missing required env vars: {', '.join(missing)}")

app = FastAPI()
EXPORTED_ZPIDS: set[str] = set()

@app.get("/")
def root():
    return {"status": "ok"}

@app.get("/export-zpids")
def export_zpids():
    try:
        conn = sqlite3.connect("seen.db")
        rows = conn.execute("SELECT zpid FROM listings").fetchall()
        conn.close()
        return {"zpids": [row[0] for row in rows]}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))

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
def health_check():
    return {"status": "ok"}

