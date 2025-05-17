from fastapi import FastAPI, Request

from apify_fetcher import fetch_rows     # your helper that pulls dataset 
rows
from bot import process_rows             # the pipeline we finished 
earlier

app = FastAPI()

# keep track of every ZPID we've already handled during this container’s 
life
EXPORTED_ZPIDS: set[str] = set()


@app.post("/apify-hook")
async def apify_hook(req: Request):
    """
    Entry-point for Apify webhooks.

    • Manual “Test” button ➜ datasetId is in the JSON body
    • Normal task run        ➜ datasetId is passed as a query-string 
param
    """
    payload = await req.json()
    dataset_id = payload.get("datasetId") or 
req.query_params.get("datasetId")
    if not dataset_id:
        return {"error": "datasetId missing"}

    rows = fetch_rows(dataset_id)

    # Deduplicate against zpids we've already processed in this pod.
    fresh_rows = [r for r in rows if r.get("zpid") not in EXPORTED_ZPIDS]
    EXPORTED_ZPIDS.update(r.get("zpid") for r in fresh_rows)

    process_rows(fresh_rows)
    return {"status": "ok", "imported": len(fresh_rows)}


@app.get("/export-zpids")
async def export_zpids():
    """
    Helper for the JS runner Actor.
    Returns all ZPIDs we’ve seen so far so Apify can set `excludeZpids`.
    """
    return list(EXPORTED_ZPIDS)

