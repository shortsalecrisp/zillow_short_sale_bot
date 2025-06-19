# webhook_server.py – receives listings from Apify, de-dupes, logs, sheets + SMS
from fastapi import FastAPI, Request
import os
import re
import json
import logging
import sqlite3
import requests

from apify_fetcher import fetch_rows          # unchanged helper
from bot_min       import process_rows        # unchanged helper

# 
──────────────────────────────────────────────────────────────────────────────
# Config & logging
# 
──────────────────────────────────────────────────────────────────────────────
DB_PATH     = "seen.db"
TABLE_SQL   = "CREATE TABLE IF NOT EXISTS listings (zpid TEXT PRIMARY KEY)"
SMS_API_URL = os.getenv("SMS_API_URL", "https://api.smsmobileapi.com/sendsms/")
SMS_API_KEY = os.getenv("SMSM_API_KEY")  # make sure this is set in Render

logging.basicConfig(
    level  = logging.INFO,
    format = "%(asctime)s %(levelname)s: %(message)s",
)
logger = logging.getLogger("webhook_server")

app            = FastAPI()
EXPORTED_ZPIDS = set()          # cache of already-handled zpids in memory


# 
──────────────────────────────────────────────────────────────────────────────
# tiny helpers
# 
──────────────────────────────────────────────────────────────────────────────
def ensure_table() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.execute(TABLE_SQL)
    return conn


def _digits_only(num: str) -> str:
    """Strip everything except digits and be sure it starts with a country code."""
    digits = re.sub(r"\D", "", num or "")
    if len(digits) == 10:       # assume US local -> prefix country code 1
        digits = "1" + digits
    return digits


def send_sms(phone: str, message: str) -> None:
    """
    Patch ►►  Correct parameter names for SMSMobileAPI:
        * apikey     – your key
        * recipients – digits only, country code included
        * message    – text
    """
    if not SMS_API_KEY:
        logger.warning("SMS_API_KEY missing – skipping SMS send")
        return

    digits = _digits_only(phone)
    if not digits:
        logger.warning("Bad phone number '%s' – skipping SMS send", phone)
        return

    payload = {
        "apikey":     SMS_API_KEY,
        "recipients": digits,
        "message":    message,
    }

    try:
        r = requests.post(SMS_API_URL, data=payload, timeout=15)
        r.raise_for_status()
        j = r.json()
        if j.get("result", {}).get("error") != "0":
            logger.error("SMS API error %s %s", j["result"].get("error"),
                         j["result"].get("sent"))
        else:
            logger.info("SMS sent OK to %s", digits)
    except Exception as exc:
        logger.exception("SMS send failed: %s", exc)


# 
──────────────────────────────────────────────────────────────────────────────
# Health check & export utilities
# 
──────────────────────────────────────────────────────────────────────────────
@app.get("/")
def root():
    return {"status": "ok"}


@app.get("/export-zpids")
def export_zpids():
    conn  = ensure_table()
    zpids = [row[0] for row in conn.execute("SELECT zpid FROM listings")]
    conn.close()
    EXPORTED_ZPIDS.update(zpids)
    return {"zpids": zpids}


@app.post("/reset-zpids")
def reset_zpids():
    conn = ensure_table()
    conn.execute("DELETE FROM listings")
    conn.commit()
    conn.close()
    EXPORTED_ZPIDS.clear()
    return {"status": "cleared"}


# 
──────────────────────────────────────────────────────────────────────────────
# Webhook – receives new listings from Apify
# 
──────────────────────────────────────────────────────────────────────────────
@app.post("/apify-hook")
async def apify_hook(request: Request):
    """
    Accepts either:
      • {"dataset_id": "..."} – fetch rows from that dataset
      • {"listings": [ {...}, ... ]} – rows already provided inline
    """
    body = await request.json()
    logger.debug("Incoming webhook payload: %s", json.dumps(body))

    # --- get raw rows ---------------------------------------------------------
    if isinstance(body.get("listings"), list):
        rows = body["listings"]
        logger.info("apify-hook: received %d listings directly in payload", len(rows))
    else:
        dataset_id = body.get("dataset_id") or request.query_params.get("dataset_id")
        if not dataset_id:
            logger.error("apify-hook: missing dataset_id and no listings array found")
            return {"error": "dataset_id missing and no listings provided"}
        rows = fetch_rows(dataset_id)
        logger.info("apify-hook: fetched %d rows from dataset %s", len(rows), dataset_id)

    # --- dedupe against memory cache & sqlite ---------------------------------
    fresh_rows = [r for r in rows if r.get("zpid") not in EXPORTED_ZPIDS]
    if not fresh_rows:
        logger.info("apify-hook: no fresh rows to process (all zpids already seen)")
        return {"status": "no new rows"}

    logger.debug("Sample fields on first fresh row: %s", list(fresh_rows[0].keys())[:15])

    # --- main processing (writes to sheet, etc.) ------------------------------
    # process_rows is expected to:
    #   • append to Google Sheet
    #   • return (address, phone, summary) so we can SMS afterwards
    sms_jobs = process_rows(fresh_rows) or []

    # --- send SMS for each processed listing ----------------------------------
    for job in sms_jobs:
        try:
            send_sms(job["phone"], job["message"])
        except Exception:
            logger.exception("send_sms failed for %s", job)

    # --- remember we've handled these zpids -----------------------------------
    EXPORTED_ZPIDS.update(r.get("zpid") for r in fresh_rows)

    # also persist in sqlite for durability
    conn = ensure_table()
    conn.executemany("INSERT OR IGNORE INTO listings (zpid) VALUES (?)",
                     [(r["zpid"],) for r in fresh_rows])
    conn.commit()
    conn.close()

    return {"status": "processed", "rows": len(fresh_rows)}


@app.get("/healthz")
def healthz():
    return {"status": "ok"}

