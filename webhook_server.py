from fastapi import FastAPI, Request
from apify_fetcher import fetch_rows
from bot import process_rows, conn          # <- conn is the SQLite handle 
in bot.py

app = FastAPI()

# 
---------------------------------------------------------------------------
# 1) Main webhook: Apify → Render
# 
---------------------------------------------------------------------------
@app.post("/apify-hook")
async def apify_hook(req: Request):
    """
    Receives a POST from an Apify Actor/Task run.
    Works for both:
      • manual “Test” button  → `datasetId` is in JSON body
      • real run              → datasetId appears in query-string
    """
    data = await req.json()
    dataset_id = data.get("datasetId") or 
req.query_params.get("datasetId")
    if not dataset_id:
        return {"error": "datasetId missing"}

    rows = fetch_rows(dataset_id)      # pulls JSON rows from Apify 
dataset
    process_rows(rows)                 # your full pipeline in bot.py
    return {"status": "ok", "imported": len(rows)}


# 
---------------------------------------------------------------------------
# 2) Helper endpoint for Apify “excludeZpids”
# 
---------------------------------------------------------------------------
@app.get("/export-zpids")
def export_zpids():
    """
    Tiny JSON API the Apify Task calls *before* each run.
    It returns every zpid already processed so the Actor can skip them,
    saving Apify credits and GPT tokens.
    """
    zpids = [row[0] for row in conn.execute("SELECT zpid FROM listings")]
    return {"zpids": zpids}

