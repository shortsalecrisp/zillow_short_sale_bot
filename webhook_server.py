from fastapi import FastAPI, Request
import os

from apify_fetcher import fetch_rows
from bot_min import process_rows

# Environment‑variable guard
REQUIRED_ENV_VARS = [
    "OPENAI_API_KEY",
    "SMSM_KEY",
    "SHEET_URL",
]
missing = [var for var in REQUIRED_ENV_VARS if not os.getenv(var)]
if missing:
    raise RuntimeError(f"Missing required env vars: {', '.join(missing)}")

# FastAPI app
app = FastAPI()
EXPORTED_ZPIDS: set[str] = set()


@app.post("/apify-hook")
async def apify_hook(request: Request):
    """Webhook entry‑point for Apify dataset notifications.

    The dataset ID may arrive either in the JSON body (manual “test” button)
    or as a query‑string parameter during scheduled runs.
    """
    payload = await request.json()

    dataset_id = payload.get("dataset_id") or request.query_params.get("datasetId")
    if not dataset_id:
        return {"error": "dataset_id missing"}

    rows = fetch_rows(dataset_id)

    # Skip rows already processed in this container’s lifetime
    fresh_rows = [r for r in rows if r.get("zpid") not in EXPORTED_ZPIDS]
    if not fresh_rows:
        return {"status": "no new rows"}

    process_rows(fresh_rows)
    EXPORTED_ZPIDS.update(r.get("zpid") for r in fresh_rows)

    return {"status": "processed", "rows": len(fresh_rows)}


@app.get("/healthz")
def health_check():
    """Simple liveness probe for Render."""
    return {"status": "ok"}

