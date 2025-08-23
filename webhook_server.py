# webhook_server.py – receives listings from Apify, de-dupes, writes sheet & SMS,
#                     **and now records inbound SMS replies via a webhook**

from fastapi import FastAPI, Request, HTTPException
import os
import re
import json
import logging
import sqlite3
from datetime import datetime

import gspread
from google.oauth2.service_account import Credentials

from apify_fetcher import fetch_rows          # unchanged helper
from bot_min       import process_rows        # unchanged helper
from sms_providers import get_sender

# ──────────────────────────────────────────────────────────────────────
# Configuration & logging
# ──────────────────────────────────────────────────────────────────────
DB_PATH     = "seen.db"
TABLE_SQL   = "CREATE TABLE IF NOT EXISTS listings (zpid TEXT PRIMARY KEY)"
SMS_PROVIDER = os.getenv("SMS_PROVIDER", "android_gateway")
SMS_SENDER   = get_sender(SMS_PROVIDER)

# Google Sheets / Replies tab
GSHEET_ID   = os.environ["GSHEET_ID"]
SC_JSON     = json.loads(os.environ["GCP_SERVICE_ACCOUNT_JSON"])
SCOPES      = ["https://www.googleapis.com/auth/spreadsheets"]

# Shared-secret token for inbound-SMS webhook
WEBHOOK_TOKEN = os.environ["SMSM_WEBHOOK_TOKEN"]  # e.g. "65-g84-jfy7t"

logging.basicConfig(
    level  = logging.INFO,
    format = "%(asctime)s %(levelname)s: %(message)s",
)
logger = logging.getLogger("webhook_server")

# FastAPI app
app            = FastAPI()

# In-memory de-dupe cache of exported ZPIDs
EXPORTED_ZPIDS: set[str] = set()

# ──────────────────────────────────────────────────────────────────────
# Google Sheets helpers
# ──────────────────────────────────────────────────────────────────────
creds         = Credentials.from_service_account_info(SC_JSON, scopes=SCOPES)
gclient       = gspread.authorize(creds)

def get_replies_ws():
    """Ensure a 'Replies' sheet exists and return the worksheet handle."""
    try:
        return gclient.open_by_key(GSHEET_ID).worksheet("Replies")
    except gspread.WorksheetNotFound:
        ws = gclient.open_by_key(GSHEET_ID).add_worksheet(
            title="Replies", rows="1000", cols="3"
        )
        ws.append_row(["phone", "time_received", "message"])
        return ws

REPLIES_WS = get_replies_ws()

# ──────────────────────────────────────────────────────────────────────
# Local helpers
# ──────────────────────────────────────────────────────────────────────
def ensure_table() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.execute(TABLE_SQL)
    return conn


def _digits_only(num: str) -> str:
    """Keep digits, prefix 1 if US local (10 digits)."""
    digits = re.sub(r"\D", "", num or "")
    if len(digits) == 10:
        digits = "1" + digits
    return digits


BAD_AREA = {
    "800",
    "888",
    "877",
    "866",
    "855",
    "844",
    "833",
}  # reject toll-free & 1xx after leading '1' stripped

def fmt_phone(raw: str) -> str:
    """Return 123-456-7890 or '' if invalid/toll-free/1xx."""
    digits = re.sub(r"\D", "", raw)
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    if len(digits) != 10:
        return ""
    if digits[:3] in BAD_AREA or digits[:3].startswith("1"):
        return ""
    return f"{digits[:3]}-{digits[3:6]}-{digits[6:]}"


def send_sms(phone: str, message: str) -> None:
    """Send an SMS using the configured provider."""
    digits = _digits_only(phone)
    if not digits:
        logger.warning("Bad phone number '%s' – skipping SMS send", phone)
        return
    try:
        SMS_SENDER.send(digits, message)
        logger.info("SMS sent OK to %s", digits)
    except Exception as exc:
        logger.exception("SMS send failed: %s", exc)


# ──────────────────────────────────────────────────────────────────────
# Health check & export utilities
# ──────────────────────────────────────────────────────────────────────
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


# ──────────────────────────────────────────────────────────────────────
# Webhook – receives new listings from Apify
# ──────────────────────────────────────────────────────────────────────
@app.post("/apify-hook")
async def apify_hook(request: Request):
    """
    Accepts either:
      • {"dataset_id": "..."}       – fetch rows from that dataset
      • {"listings": [ {...}, ...]} – rows already provided inline
    """
    body = await request.json()
    logger.debug("Incoming webhook payload: %s", json.dumps(body))

    # --- obtain raw rows ------------------------------------------------------
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

    # --- dedupe ---------------------------------------------------------------
    conn = ensure_table()
    fresh_rows = []
    for r in rows:
        zpid = r.get("zpid")
        if zpid in EXPORTED_ZPIDS:
            continue
        if conn.execute("SELECT 1 FROM listings WHERE zpid=?", (zpid,)).fetchone():
            EXPORTED_ZPIDS.add(zpid)
            continue
        fresh_rows.append(r)

    if not fresh_rows:
        logger.info("apify-hook: no fresh rows to process (all zpids already seen)")
        conn.close()
        return {"status": "no new rows"}

    logger.debug("Sample fields on first fresh row: %s", list(fresh_rows[0].keys())[:15])

    # --- main processing ------------------------------------------------------
    # process_rows should append to the sheet and return SMS jobs
    sms_jobs = process_rows(fresh_rows) or []

    # --- send SMS -------------------------------------------------------------
    for job in sms_jobs:
        try:
            send_sms(job["phone"], job["message"])
        except Exception:
            logger.exception("send_sms failed for %s", job)

    # --- persist ZPIDs --------------------------------------------------------
    EXPORTED_ZPIDS.update(r.get("zpid") for r in fresh_rows)

    conn.executemany(
        "INSERT OR IGNORE INTO listings (zpid) VALUES (?)",
        [(r["zpid"],) for r in fresh_rows],
    )
    conn.commit()
    conn.close()

    return {"status": "processed", "rows": len(fresh_rows)}


# ──────────────────────────────────────────────────────────────────────
# NEW: Inbound-SMS webhook – records replies to Google Sheets
# ──────────────────────────────────────────────────────────────────────
@app.post("/sms-reply")
async def sms_reply(request: Request):
    """
    SMSMobileAPI (Android Gateway) will POST JSON like:
        {
          "number":  "+15558675309",
          "message": "Sure, let's talk!",
          "guid":    "...",
          "time_received": "2025-06-26 01:23:45"
        }

    The webhook URL must include ?token=<WEBHOOK_TOKEN>
    """
    token = request.query_params.get("token")
    if token != WEBHOOK_TOKEN:
        raise HTTPException(status_code=403, detail="bad token")

    data = await request.json()
    phone_raw = data.get("number") or data.get("phone") or ""
    msg       = data.get("message", "")
    ts        = data.get("time_received") or datetime.utcnow().isoformat(timespec="seconds")

    phone = fmt_phone(phone_raw)
    if not phone:
        logger.warning("Ignored inbound with unusable phone: %s", phone_raw)
        return {"status": "ignored"}

    try:
        REPLIES_WS.append_row([phone, ts, msg])
        logger.info("Recorded reply from %s", phone)
    except Exception as exc:
        logger.exception("Sheet append error: %s", exc)
        raise HTTPException(status_code=500, detail="sheet error")

    return {"status": "ok"}


@app.get("/healthz")
def healthz():
    return {"status": "ok"}

