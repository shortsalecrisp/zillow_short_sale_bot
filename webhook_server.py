from fastapi import FastAPI, Request
import os


OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
SMSM_KEY       = os.getenv("SMSM_KEY")
SHEET_URL      = os.getenv("SHEET_URL")

if not (OPENAI_API_KEY and SMSM_KEY and SHEET_URL):
    missing = [
        name for name, val in [
            ("OPENAI_API_KEY", OPENAI_API_KEY),
            ("SMSM_KEY", SMSM_KEY),
            ("SHEET_URL", SHEET_URL),
        ]
        if not val
    ]
    raise RuntimeError(f"Missing required env vars: {', '.join(missing)}")

from fastapi import FastAPI, Request

from apify_fetcher import fetch_rows
from bot_min import process_rows

app = FastAPI()

EXPORTED_ZPIDS: set[str] = set()


@app.post("/apify-hook")
async def apify_hook(req: Request):
    """
    Apify → Render webhook.
    • Manual “Test” button – datasetId in JSON body
    • Scheduled run        – datasetId in query-string
    """
    payload = await req.json()

    dataset_id = (
        payload.get("datasetId") or
        req.query_params.get("datasetId")
    )
    if not dataset_id:
        return {"error": "datasetId missing"}

    rows = fetch_rows(dataset_id)

    # Skip rows we've already handled in this container
    fresh_rows = [r for r in rows if r.get("zpid") not in EXPORTED_ZPIDS]
    EXPORTED_ZPIDS.update(r.get("zpid") for r in fresh_rows)

    process_rows(fresh_rows)
    return {"status": "ok", "imported": len(fresh_rows)}


@app.get("/export-zpids")
async def export_zpids():
    """Runner Actor hits this to build excludeZpids."""
    return list(EXPORTED_ZPIDS)

