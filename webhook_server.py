from fastapi import FastAPI, Request
from apify_fetcher import fetch_rows   # your existing helper
from bot import process_rows           # just added

app = FastAPI()

@app.post("/apify-hook")
async def apify_hook(req: Request):
    """
    Apify → Render webhook.
    Works for both normal runs and the manual “Test” button because we
    look for datasetId in two places.
    """
    data = await req.json()
    dataset_id = (
        data.get("datasetId")              # manual “Test” button
        or req.query_params.get("datasetId")   # real run
    )
    if not dataset_id:
        return {"error": "datasetId missing"}

    rows = fetch_rows(dataset_id)
    process_rows(rows)
    return {"status": "ok", "imported": len(rows)}

