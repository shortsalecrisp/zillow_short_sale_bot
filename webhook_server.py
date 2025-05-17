from fastapi import FastAPI, Request
from apify_fetcher import fetch_rows
from bot import process_rows  # adjust if needed

app = FastAPI()

@app.post("/apify-hook")
async def apify_hook(req: Request):
    data = await req.json()
    dataset_id = (
        data.get("datasetId")                 # ← from JSON body
        or req.query_params.get("datasetId")  # ← fallback for query param
    )
    if not dataset_id:
        return {"error": "datasetId missing"}

    rows = fetch_rows(dataset_id)   # calls apify_fetcher
    process_rows(rows)              # your Sheets/SMS logic
    return {"status": "ok", "imported": len(rows)}

