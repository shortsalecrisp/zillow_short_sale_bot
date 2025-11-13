# webhook_server.py – receives listings from Apify, de-dupes, writes sheet & SMS,
#                     **and now records inbound SMS replies via a webhook**

from fastapi import FastAPI, Request, HTTPException
import os
import re
import json
import logging
import sqlite3
import threading
from datetime import datetime, timedelta
from typing import Optional

import gspread
from google.oauth2.service_account import Credentials
import requests

from apify_fetcher import fetch_rows          # unchanged helper
from bot_min       import TZ, process_rows, run_hourly_scheduler        # unchanged helper
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

# Shared-secret token for inbound-SMS webhook (optional)
WEBHOOK_TOKEN = os.getenv("SMSM_WEBHOOK_TOKEN")  # e.g. "65-g84-jfy7t"

logging.basicConfig(
    level  = logging.INFO,
    format = "%(asctime)s %(levelname)s: %(message)s",
)
logger = logging.getLogger("webhook_server")

# FastAPI app
app            = FastAPI()

# In-memory de-dupe cache of exported ZPIDs
EXPORTED_ZPIDS: set[str] = set()

_scheduler_thread: Optional[threading.Thread] = None
_scheduler_stop: Optional[threading.Event] = None

_ingest_thread: Optional[threading.Thread] = None
_ingest_stop: Optional[threading.Event] = None

APIFY_TOKEN = os.getenv("APIFY_API_TOKEN") or os.getenv("APIFY_TOKEN") or ""
APIFY_TASK_ID = os.getenv("APIFY_ZILLOW_TASK_ID") or os.getenv("APIFY_TASK_ID") or ""
APIFY_ACTOR_ID = os.getenv("APIFY_ZILLOW_ACTOR_ID") or ""
APIFY_WAIT_FOR_FINISH = int(os.getenv("APIFY_WAIT_FOR_FINISH", "240"))
APIFY_INPUT_RAW = os.getenv("APIFY_ZILLOW_INPUT", "").strip()
try:
    APIFY_RUN_INPUT = json.loads(APIFY_INPUT_RAW) if APIFY_INPUT_RAW else None
except json.JSONDecodeError:
    logger.warning("APIFY_ZILLOW_INPUT is not valid JSON – ignoring value")
    APIFY_RUN_INPUT = None

APIFY_RUN_START = int(os.getenv("APIFY_RUN_START_HOUR", "8"))
APIFY_RUN_END = int(os.getenv("APIFY_RUN_END_HOUR", "20"))  # inclusive (run at 8 pm)
APIFY_ENABLED = bool(APIFY_TOKEN and (APIFY_TASK_ID or APIFY_ACTOR_ID))


def _ensure_scheduler_thread() -> None:
    global _scheduler_thread, _scheduler_stop
    if _scheduler_thread and _scheduler_thread.is_alive():
        return

    _scheduler_stop = threading.Event()

    def _runner() -> None:
        logger.info("Background hourly scheduler thread starting")
        while not _scheduler_stop.is_set():
            try:
                run_hourly_scheduler(stop_event=_scheduler_stop)
                break
            except Exception:
                logger.exception(
                    "Background scheduler crashed; restarting in 30 seconds"
                )
                if _scheduler_stop.wait(30):
                    break
        logger.info("Background hourly scheduler thread stopped")

    _scheduler_thread = threading.Thread(
        target=_runner,
        name="hourly-scheduler",
        daemon=True,
    )
    _scheduler_thread.start()


def _ensure_ingest_thread() -> None:
    global _ingest_thread, _ingest_stop
    if not APIFY_ENABLED:
        logger.info("Hourly Apify ingestion disabled (missing token or actor/task id)")
        return
    if _ingest_thread and _ingest_thread.is_alive():
        return

    _ingest_stop = threading.Event()

    def _runner() -> None:
        logger.info("Hourly Apify ingestion thread starting")
        try:
            _ingest_loop(_ingest_stop)
        finally:
            logger.info("Hourly Apify ingestion thread stopped")

    _ingest_thread = threading.Thread(
        target=_runner,
        name="apify-hourly-ingest",
        daemon=True,
    )
    _ingest_thread.start()


@app.on_event("startup")
async def _start_scheduler() -> None:
    _ensure_scheduler_thread()
    _ensure_ingest_thread()


@app.on_event("shutdown")
async def _stop_scheduler() -> None:
    global _scheduler_thread, _scheduler_stop, _ingest_thread, _ingest_stop
    if _scheduler_stop:
        _scheduler_stop.set()
    if _scheduler_thread and _scheduler_thread.is_alive():
        _scheduler_thread.join(timeout=10)
    if _ingest_stop:
        _ingest_stop.set()
    if _ingest_thread and _ingest_thread.is_alive():
        _ingest_thread.join(timeout=10)

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


def _compute_next_ingest_run(ref: datetime) -> datetime:
    local = ref.astimezone(TZ)
    next_slot = local.replace(minute=0, second=0, microsecond=0)
    if local.minute or local.second or local.microsecond:
        next_slot += timedelta(hours=1)

    if next_slot.hour < APIFY_RUN_START:
        next_slot = next_slot.replace(hour=APIFY_RUN_START)
    elif next_slot.hour > APIFY_RUN_END:
        next_slot = (next_slot + timedelta(days=1)).replace(hour=APIFY_RUN_START)
    elif next_slot.hour == APIFY_RUN_END and next_slot <= local:
        next_slot = (next_slot + timedelta(days=1)).replace(hour=APIFY_RUN_START)

    return next_slot


def _trigger_apify_run() -> Optional[str]:
    if not APIFY_ENABLED:
        return None

    if APIFY_TASK_ID:
        endpoint = f"https://api.apify.com/v2/actor-tasks/{APIFY_TASK_ID}/runs"
        label = f"task {APIFY_TASK_ID}"
    else:
        endpoint = f"https://api.apify.com/v2/acts/{APIFY_ACTOR_ID}/runs"
        label = f"actor {APIFY_ACTOR_ID}"

    params = {"token": APIFY_TOKEN, "waitForFinish": APIFY_WAIT_FOR_FINISH}
    kwargs = {"timeout": APIFY_WAIT_FOR_FINISH + 60}
    if APIFY_RUN_INPUT is not None and not APIFY_TASK_ID:
        kwargs["json"] = APIFY_RUN_INPUT

    logger.info("Starting Apify %s run", label)
    try:
        resp = requests.post(endpoint, params=params, **kwargs)
        resp.raise_for_status()
    except requests.RequestException as exc:
        logger.exception("Apify %s run failed: %s", label, exc)
        return None

    payload = resp.json().get("data", {})
    status = payload.get("status") or payload.get("statusMessage")
    dataset_id = payload.get("defaultDatasetId")
    if not dataset_id:
        logger.warning("Apify %s run completed with status %s but no dataset id", label, status)
        return None

    logger.info("Apify %s run finished with status %s (dataset %s)", label, status, dataset_id)
    return dataset_id


def _ingest_loop(stop_event: threading.Event) -> None:
    next_run = _compute_next_ingest_run(datetime.now(tz=TZ))
    while not stop_event.is_set():
        now = datetime.now(tz=TZ)
        sleep_secs = max(0, (next_run - now).total_seconds())
        if sleep_secs:
            logger.debug(
                "Apify ingest sleeping %.0f seconds until %s",
                sleep_secs,
                next_run.isoformat(),
            )
        if stop_event.wait(timeout=sleep_secs):
            break

        if APIFY_RUN_START <= next_run.hour <= APIFY_RUN_END:
            logger.info("Triggering Apify scrape scheduled for %s", next_run.isoformat())
            dataset_id = _trigger_apify_run()
            if dataset_id:
                try:
                    _process_dataset(dataset_id)
                except Exception:
                    logger.exception("Processing dataset %s failed", dataset_id)
        else:
            logger.info(
                "Scheduled Apify scrape at %s skipped (outside configured hours)",
                next_run.isoformat(),
            )

        next_run = _compute_next_ingest_run(next_run + timedelta(seconds=1))


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
def _process_incoming_rows(rows: list[dict]) -> dict:
    conn = ensure_table()
    try:
        fresh_rows: list[dict] = []
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
            return {"status": "no new rows"}

        logger.debug("Sample fields on first fresh row: %s", list(fresh_rows[0].keys())[:15])

        sms_jobs = process_rows(fresh_rows) or []

        for job in sms_jobs:
            try:
                send_sms(job["phone"], job["message"])
            except Exception:
                logger.exception("send_sms failed for %s", job)

        EXPORTED_ZPIDS.update(r.get("zpid") for r in fresh_rows)

        conn.executemany(
            "INSERT OR IGNORE INTO listings (zpid) VALUES (?)",
            [(r["zpid"],) for r in fresh_rows],
        )
        conn.commit()
        return {"status": "processed", "rows": len(fresh_rows)}
    finally:
        conn.close()


def _process_dataset(dataset_id: str) -> dict:
    rows = fetch_rows(dataset_id)
    logger.info("apify-hook: fetched %d rows from dataset %s", len(rows), dataset_id)
    if not rows:
        return {"status": "no rows"}
    return _process_incoming_rows(rows)


@app.post("/apify-hook")
async def apify_hook(request: Request):
    """
    Accepts either:
      • {"dataset_id": "..."}       – fetch rows from that dataset
      • {"listings": [ {...}, ...]} – rows already provided inline
    """
    body = await request.json()
    logger.debug("Incoming webhook payload: %s", json.dumps(body))

    if isinstance(body.get("listings"), list):
        rows = body["listings"]
        logger.info("apify-hook: received %d listings directly in payload", len(rows))
        return _process_incoming_rows(rows)

    dataset_id = body.get("dataset_id") or request.query_params.get("dataset_id")
    if not dataset_id:
        logger.error("apify-hook: missing dataset_id and no listings array found")
        return {"error": "dataset_id missing and no listings provided"}

    return _process_dataset(dataset_id)


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

    If SMSM_WEBHOOK_TOKEN is configured, the webhook URL must include
    ?token=<WEBHOOK_TOKEN>.  If the env var is unset, the endpoint accepts
    requests without a token (useful for local dev / legacy deploys).
    """
    token = request.query_params.get("token")
    if WEBHOOK_TOKEN:
        if token != WEBHOOK_TOKEN:
            raise HTTPException(status_code=403, detail="bad token")
    elif token:
        logger.info("Ignoring unused token query param while token auth disabled")

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

