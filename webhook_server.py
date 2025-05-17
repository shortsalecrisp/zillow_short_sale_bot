# webhook_server.py
from fastapi import FastAPI, Request, HTTPException
from typing import List
from apify_fetcher import fetch_rows          # your helper that loads 
dataset rows
from bot import process_rows, load_seen_zpids # new helper in bot.py

app = FastAPI()

##########################################################################
# 1)  Webhook that Apify calls after every run (dataset → Google Sheet)  
#
##########################################################################
@app.post("/apify-hook")
async def apify_hook(req: Request):
    """
    Called by Apify's HTTP-webhook integration.
    Handles both the manual “Test” button and real runs.
    """
    data = await req.json()
    dataset_id = (
        data.get("datasetId")                  # manual “Test”
        or req.query_params.get("datasetId")   # normal run
    )
    if not dataset_id:
        raise HTTPException(status_code=400, detail="datasetId missing")

    rows = fetch_rows(dataset_id)              # list[dict] from Apify 
dataset
    imported = process_rows(rows)              # → bot handles filtering, 
sheet, sms
    return {"status": "ok", "imported": imported}


###########################################################
# 2)  Export endpoint for Apify helper (excludeZpids)      #
###########################################################
@app.get("/export-zpids", response_model=List[int])
async def export_zpids():
    """
    Returns the list of Zillow property IDs that have ALREADY been 
processed.
    Apify’s helper hits this endpoint before every run and passes the IDs 
in
    the Actor input as `excludeZpids`, so the scraper only fetches *new* 
homes.
    """
    return load_seen_zpids()       # simple list[int] that you save in 
bot.py

