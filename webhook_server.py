# webhook_server.py – receives listings from Apify, de-dupes, writes sheet & SMS,
#                     **and now records inbound SMS replies via a webhook**

import asyncio
from datetime import datetime, timedelta
import json
import logging
import os
import re
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from fastapi import FastAPI, HTTPException, Request, Response

import gspread
from google.oauth2.service_account import Credentials

from apify_fetcher import fetch_rows  # unchanged helper
from bot_min import (
    TZ,
    WORK_END,
    WORK_START,
    _hour_floor,
    fetch_contact_page,
    process_rows,
    run_hourly_scheduler,
)
from sms_providers import get_sender

# ──────────────────────────────────────────────────────────────────────
# Configuration & logging
# ──────────────────────────────────────────────────────────────────────
DB_PATH     = "seen.db"
TABLE_SQL   = "CREATE TABLE IF NOT EXISTS listings (zpid TEXT PRIMARY KEY)"
SMS_PROVIDER = os.getenv("SMS_PROVIDER", "android_gateway")
SMS_SENDER   = get_sender(SMS_PROVIDER)
DISABLE_APIFY_SCHEDULER = os.getenv("DISABLE_APIFY_SCHEDULER", "false").lower() == "true"

# Google Sheets / Replies tab
GSHEET_ID   = os.environ["GSHEET_ID"]
SC_JSON     = json.loads(os.environ["GCP_SERVICE_ACCOUNT_JSON"])
SCOPES      = ["https://www.googleapis.com/auth/spreadsheets"]

# Shared-secret token for inbound-SMS webhook (optional)
WEBHOOK_TOKEN = os.getenv("SMSM_WEBHOOK_TOKEN")  # e.g. "65-g84-jfy7t"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
)
logger = logging.getLogger("webhook_server")

# FastAPI app
app            = FastAPI()

# In-memory de-dupe cache of exported ZPIDs
EXPORTED_ZPIDS: set[str] = set()

_scheduler_thread: Optional[threading.Thread] = None
_scheduler_stop: Optional[threading.Event] = None
_startup_task: Optional[asyncio.Task] = None

_APIFY_ACTOR_ID_RAW = os.getenv("APIFY_ZILLOW_ACTOR_ID") or os.getenv("APIFY_ACTOR_ID")
# Apify actor "unique names" use `user~actor`, but many configs still include
# a slash. Normalize here so `user/actor` works too and avoids 404s.
APIFY_ACTOR_ID = (
_APIFY_ACTOR_ID_RAW.replace("/", "~") if _APIFY_ACTOR_ID_RAW else None
)
if _APIFY_ACTOR_ID_RAW and "/" in _APIFY_ACTOR_ID_RAW and "~" not in _APIFY_ACTOR_ID_RAW:
    logger.info("Normalizing APIFY_ACTOR_ID from %s to %s", _APIFY_ACTOR_ID_RAW, APIFY_ACTOR_ID)
APIFY_TOKEN = os.getenv("APIFY_API_TOKEN") or os.getenv("APIFY_TOKEN")
APIFY_TIMEOUT = int(os.getenv("APIFY_ACTOR_TIMEOUT", "240"))
APIFY_MEMORY = int(os.getenv("APIFY_ACTOR_MEMORY", "2048"))
APIFY_MAX_RETRIES = int(os.getenv("APIFY_ACTOR_MAX_RETRIES", "3"))
APIFY_RETRY_BACKOFF = float(os.getenv("APIFY_ACTOR_RETRY_BACKOFF", "1.8"))
APIFY_STATUS_PATH = Path(os.getenv("APIFY_STATUS_PATH", "apify_status.json"))
APIFY_LAST_RUN_PATH = Path(os.getenv("APIFY_LAST_RUN_PATH", "apify_last_run.json"))
APIFY_MAX_ITEMS = int(os.getenv("APIFY_MAX_ITEMS", "5"))
_apify_input_raw = os.getenv("APIFY_ACTOR_INPUT", "").strip()
RUN_ON_DEPLOY = os.getenv("RUN_SCRAPE_ON_DEPLOY", "true").lower() == "true"

try:
    APIFY_INPUT: Optional[Dict[str, Any]] = (
        json.loads(_apify_input_raw) if _apify_input_raw else None
    )
except json.JSONDecodeError:
    logger.error("Invalid JSON in APIFY_ACTOR_INPUT – startup runs will omit custom input")
    APIFY_INPUT = None


def _ensure_scheduler_thread(
    hourly_callbacks: Optional[List] = None,
    *,
    initial_callbacks: bool = True,
) -> None:
    global _scheduler_thread, _scheduler_stop
    if _scheduler_thread and _scheduler_thread.is_alive():
        return

    _scheduler_stop = threading.Event()

    def _runner() -> None:
        logger.info("Background hourly scheduler thread starting")
        while not _scheduler_stop.is_set():
            try:
                run_hourly_scheduler(
                    stop_event=_scheduler_stop,
                    hourly_callbacks=hourly_callbacks,
                    run_immediately=True,
                    initial_callbacks=initial_callbacks,
                )
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


def _load_last_apify_run() -> Optional[datetime]:
    try:
        if not APIFY_LAST_RUN_PATH.exists():
            return None
        data = json.loads(APIFY_LAST_RUN_PATH.read_text())
        ts = data.get("ts")
        return datetime.fromisoformat(ts) if ts else None
    except Exception:
        logger.debug("Unable to read Apify last-run marker", exc_info=True)
        return None


def _write_last_apify_run(ts: datetime) -> None:
    try:
        APIFY_LAST_RUN_PATH.write_text(json.dumps({"ts": ts.isoformat()}))
    except Exception:
        logger.debug("Unable to persist Apify last-run marker", exc_info=True)


def _apify_hourly_task(run_time: datetime) -> None:
    current_slot = _hour_floor(run_time)
    last_run = _load_last_apify_run()
    if last_run and last_run >= current_slot:
        logger.debug(
            "Apify scrape already executed for slot %s; skipping", current_slot.isoformat()
        )
        return

    logger.info("Triggering hourly Apify scrape at %s", run_time.isoformat())
    rows = _run_apify_actor()
    if rows:
        _process_incoming_rows(rows)
    _write_last_apify_run(current_slot)


def _process_incoming_rows(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    conn = ensure_table()
    fresh_rows: List[Dict[str, Any]] = []
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

    try:
        sms_jobs = process_rows(fresh_rows) or []
    except Exception:
        logger.exception("process_rows failed; skipping batch to keep server alive")
        conn.close()
        return {"status": "error", "rows": len(fresh_rows)}

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
    conn.close()

    return {"status": "processed", "rows": len(fresh_rows)}


def _record_apify_degradation(reason: str, status: Optional[int] = None) -> None:
    payload = {
        "ts": datetime.utcnow().isoformat() + "Z",
        "reason": reason,
    }
    if status is not None:
        payload["status"] = status
    try:
        APIFY_STATUS_PATH.write_text(json.dumps(payload))
    except Exception:
        logger.debug("Unable to write Apify degradation marker", exc_info=True)


def _clear_apify_degradation() -> None:
    try:
        if APIFY_STATUS_PATH.exists():
            APIFY_STATUS_PATH.unlink()
    except Exception:
        logger.debug("Unable to clear Apify degradation marker", exc_info=True)


def _lightweight_apify_scrape() -> List[Dict[str, Any]]:
    """Fallback scraper that hits startUrls directly with proxy/headless helpers."""
    urls: List[str] = []
    if isinstance(APIFY_INPUT, dict):
        for item in APIFY_INPUT.get("startUrls", []) or []:
            if isinstance(item, dict) and item.get("url"):
                urls.append(item["url"])
            elif isinstance(item, str):
                urls.append(item)
    results: List[Dict[str, Any]] = []
    for url in urls:
        html, _ = fetch_contact_page(url)
        if html:
            results.append({"url": url, "html": html, "source": "apify_fallback"})
    if results:
        logger.info("Lightweight Apify fallback scraped %d urls", len(results))
    return results


def _run_apify_actor() -> List[Dict[str, Any]]:
    if not APIFY_ACTOR_ID or not APIFY_TOKEN:
        logger.info("Skipping startup Apify run – missing APIFY_ACTOR_ID or token")
        return []

    url = f"https://api.apify.com/v2/acts/{APIFY_ACTOR_ID}/run-sync-get-dataset-items"
    params = {
        "token": APIFY_TOKEN,
        "timeout": APIFY_TIMEOUT,
        "memory": APIFY_MEMORY,
        "clean": 1,
        "limit": APIFY_MAX_ITEMS,
        "desc": 1,
    }
    logger.info(
        "Triggering Apify actor %s for startup scrape (%s input)",
        APIFY_ACTOR_ID,
        "custom" if APIFY_INPUT is not None else "default",
    )
    req_kwargs: Dict[str, Any] = {"params": params, "timeout": APIFY_TIMEOUT + 30}
    if APIFY_INPUT is not None:
        req_kwargs["json"] = APIFY_INPUT

    for attempt in range(1, APIFY_MAX_RETRIES + 1):
        resp = requests.post(url, **req_kwargs)
        timed_out = False
        try:
            resp.raise_for_status()
        except requests.HTTPError:
            status = resp.status_code
            body_preview = resp.text[:500] if resp.text else "<no body>"
            timed_out = status in (408, 504) or "TIMED-OUT" in body_preview.upper()
            logger.error(
                "Apify actor call failed (attempt %s/%s) with status %s: %s",
                attempt,
                APIFY_MAX_RETRIES,
                status,
                body_preview,
            )
            if timed_out and attempt < APIFY_MAX_RETRIES:
                sleep_for = min(APIFY_TIMEOUT, (APIFY_RETRY_BACKOFF ** (attempt - 1)) * 5)
                logger.info("Retrying Apify actor after %.1fs due to timeout", sleep_for)
                time.sleep(sleep_for)
                continue
            _record_apify_degradation("timed_out", status=status)
            fallback_rows = _lightweight_apify_scrape()
            return fallback_rows
        data = resp.json()
        if not isinstance(data, list):
            logger.warning("Unexpected Apify response – expected list, got %s", type(data))
            _record_apify_degradation("invalid_response")
            return _lightweight_apify_scrape()
        _clear_apify_degradation()
        logger.info("Apify actor returned %d rows", len(data))
        return data
    _record_apify_degradation("exhausted_retries")
    return _lightweight_apify_scrape()


async def _maybe_run_startup_scrape() -> None:
    if not RUN_ON_DEPLOY:
        logger.info("RUN_SCRAPE_ON_DEPLOY disabled; skipping startup scrape")
        return

    current_slot = _hour_floor(datetime.now(tz=TZ))
    last_run = _load_last_apify_run()
    if last_run and last_run >= current_slot:
        logger.info(
            "Skipping startup Apify run – already executed at %s",
            last_run.isoformat(),
        )
        return

    try:
        rows = await asyncio.to_thread(_run_apify_actor)
        if not rows:
            return
        result = await asyncio.to_thread(_process_incoming_rows, rows)
        logger.info(
            "Startup scrape complete – %s",
            result.get("status", "no status"),
        )
        _write_last_apify_run(current_slot)
    except Exception:
        logger.exception("Startup Apify scrape failed")


@app.on_event("startup")
async def _start_scheduler() -> None:
    global _startup_task
    if DISABLE_APIFY_SCHEDULER:
        logger.info("DISABLE_APIFY_SCHEDULER enabled; skipping scheduler thread")
        return
    _ensure_scheduler_thread(
        hourly_callbacks=[_apify_hourly_task],
        initial_callbacks=False,
    )
    if _startup_task is None or _startup_task.done():
        _startup_task = asyncio.create_task(_maybe_run_startup_scrape())


@app.on_event("shutdown")
async def _stop_scheduler() -> None:
    global _scheduler_thread, _scheduler_stop
    if _scheduler_stop:
        _scheduler_stop.set()
    if _scheduler_thread and _scheduler_thread.is_alive():
        _scheduler_thread.join(timeout=10)

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


@app.head("/")
def root_head():
    return Response(status_code=200)


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

    return _process_incoming_rows(rows)


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
