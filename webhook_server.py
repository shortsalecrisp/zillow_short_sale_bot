from fastapi import FastAPI, Request
from apify_fetcher import fetch_rows
from bot import process_rows  # adjust if needed

app = FastAPI()

@app.post("/apify-hook")
async def apify_hook(req: Request):
    data = await req.json()
    dataset_id = (
        data.get("resource", {}).get("datasetId")
        or req.query_params.get("datasetId")
    )
    if not dataset_id:
        return {"error": "datasetId missing"}
    rows = fetch_rows(dataset_id)
    process_rows(rows)
    return {"status": "ok", "imported": len(rows)}
