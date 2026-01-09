# webhook_server.py – receives listings from Apify, de-dupes, writes sheet & SMS,
#                     **and now records inbound SMS replies via a webhook**

import asyncio
from datetime import datetime, timedelta, timezone
import json
import logging
import os
import re
import urllib.parse
import sqlite3
import threading
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
    SCHEDULER_TZ,
    apify_acquire_decision_slot,
    apify_hour_key,
    apify_work_hours_status,
    _hour_floor,
    dedupe_rows_by_zpid,
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
# Optional self-ping to keep Render (or other idle-suspending platforms) awake.
KEEPALIVE_URL = os.getenv("KEEPALIVE_URL")
KEEPALIVE_PERIOD_SECONDS = int(os.getenv("KEEPALIVE_PERIOD_SECONDS", "300"))
KEEPALIVE_TIMEOUT_SECONDS = float(os.getenv("KEEPALIVE_TIMEOUT_SECONDS", "8"))

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
_keepalive_thread: Optional[threading.Thread] = None
_keepalive_stop: Optional[threading.Event] = None

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
APIFY_LAST_RUN_PATH = Path(os.getenv("APIFY_LAST_RUN_PATH", "apify_last_run.txt"))
APIFY_FORCE_EVERY_HOUR = os.getenv("APIFY_FORCE_EVERY_HOUR", "false").lower() == "true"
APIFY_MAX_ITEMS = int(os.getenv("APIFY_MAX_ITEMS", "5"))
APIFY_FETCH_ATTEMPTS = int(os.getenv("APIFY_FETCH_ATTEMPTS", "6"))
APIFY_FETCH_BACKOFF_SECONDS = float(os.getenv("APIFY_FETCH_BACKOFF_SECONDS", "2.0"))
APIFY_FETCH_MAX_WAIT_SECONDS = float(os.getenv("APIFY_FETCH_MAX_WAIT_SECONDS", "300"))
_apify_input_raw = os.getenv("APIFY_ACTOR_INPUT", "").strip()

try:
    APIFY_INPUT: Optional[Dict[str, Any]] = (
        json.loads(_apify_input_raw) if _apify_input_raw else None
    )
except json.JSONDecodeError:
    logger.error("Invalid JSON in APIFY_ACTOR_INPUT – startup runs will omit custom input")
    APIFY_INPUT = None


def _should_run_immediately() -> bool:
    if os.getenv("SCHEDULER_RUN_IMMEDIATELY", "false").lower() == "true":
        return True
    now_local = datetime.now(SCHEDULER_TZ)
    return now_local.minute < 2


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
                    run_immediately=_should_run_immediately(),
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


def _ensure_keepalive_thread() -> None:
    """Periodically hit KEEPALIVE_URL so the platform sees traffic and stays warm."""

    global _keepalive_thread, _keepalive_stop
    if not KEEPALIVE_URL:
        return
    if _keepalive_thread and _keepalive_thread.is_alive():
        return

    _keepalive_stop = threading.Event()

    def _runner() -> None:
        logger.info(
            "Keepalive pinger enabled for %s (every %ss)",
            KEEPALIVE_URL,
            KEEPALIVE_PERIOD_SECONDS,
        )
        while not _keepalive_stop.wait(KEEPALIVE_PERIOD_SECONDS):
            try:
                resp = requests.get(
                    KEEPALIVE_URL,
                    timeout=KEEPALIVE_TIMEOUT_SECONDS,
                )
                logger.debug(
                    "Keepalive ping %s -> %s",
                    KEEPALIVE_URL,
                    resp.status_code,
                )
            except Exception:
                logger.warning("Keepalive ping failed", exc_info=True)
        logger.info("Keepalive pinger stopped")

    _keepalive_thread = threading.Thread(
        target=_runner,
        name="keepalive-pinger",
        daemon=True,
    )
    _keepalive_thread.start()


def extract_description(row: Dict[str, Any]) -> str:
    # Prefer top-level fields if present
    for key in ("description", "homeDescription", "remarks", "whatsSpecial"):
        value = row.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()

    # Fallback to nested hdpData.homeInfo if present
    home_info = (row.get("hdpData") or {}).get("homeInfo") or {}
    for key in ("description", "homeDescription", "whatsSpecialText", "whatsSpecial"):
        value = home_info.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()

    return ""


def _normalize_apify_row(row: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(row, dict):
        return row
    normalized = dict(row)

    agent_name = row.get("agentName")
    if not agent_name:
        for path in (
            ("listingAgent", "name"),
            ("agent", "name"),
            ("agentName",),
        ):
            current: Any = row
            for key in path:
                if not isinstance(current, dict):
                    current = None
                    break
                current = current.get(key)
            if isinstance(current, str) and current.strip():
                agent_name = current.strip()
                break
    if agent_name:
        normalized["agentName"] = agent_name

    address = row.get("address") or row.get("street")
    if isinstance(address, dict):
        street_parts = [
            address.get("street"),
            address.get("streetAddress"),
            address.get("streetAddress1"),
        ]
        street = next((p for p in street_parts if isinstance(p, str) and p.strip()), "")
        city = address.get("city") if isinstance(address.get("city"), str) else ""
        state = address.get("state") if isinstance(address.get("state"), str) else ""
        zip_code = address.get("zipcode") or address.get("zip")
        if street:
            normalized.setdefault("street", street)
            normalized.setdefault("address", street)
        if city:
            normalized.setdefault("city", city)
        if state:
            normalized.setdefault("state", state)
        if isinstance(zip_code, str):
            normalized.setdefault("zip", zip_code)
    elif isinstance(address, str) and address.strip():
        normalized.setdefault("address", address.strip())
        normalized.setdefault("street", address.strip())

    detail_url = row.get("detailUrl") or row.get("detailURL") or row.get("url")
    if isinstance(detail_url, str) and detail_url.strip():
        normalized["detailUrl"] = detail_url.strip()

    if not normalized.get("listing_description"):
        normalized["listing_description"] = extract_description(row)

    return normalized


def _row_has_listing_text(row: Dict[str, Any]) -> bool:
    if not isinstance(row, dict):
        return False
    for key in (
        "description",
        "listing_description",
        "openai_summary",
        "listingDescription",
        "homeDescription",
        "marketingDescription",
        "remarks",
        "publicRemarks",
        "brokerRemarks",
        "agentRemarks",
        "listingRemarks",
        "shortSaleDescription",
        "whatsSpecial",
        "whatsSpecialText",
    ):
        value = row.get(key)
        if isinstance(value, str) and value.strip():
            return True
    hdp_info = (row.get("hdpData") or {}).get("homeInfo") or {}
    for key in (
        "description",
        "homeDescription",
        "listingDescription",
        "whatsSpecial",
        "whatsSpecialText",
    ):
        value = hdp_info.get(key)
        if isinstance(value, str) and value.strip():
            return True
    for path in (("property", "description"), ("property", "remarks"), ("listing", "description"), ("listing", "remarks")):
        current: Any = row
        for key in path:
            if not isinstance(current, dict):
                current = None
                break
            current = current.get(key)
        if isinstance(current, str) and current.strip():
            return True
    return False


def _row_has_expected_fields(row: Dict[str, Any]) -> bool:
    if not isinstance(row, dict):
        return False
    address = row.get("address") or row.get("street")
    if isinstance(address, str) and address.strip():
        return True
    if isinstance(address, dict):
        for key in ("street", "streetAddress", "streetAddress1", "city", "state", "zip", "zipcode"):
            value = address.get(key)
            if isinstance(value, str) and value.strip():
                return True
    agent_name = row.get("agentName")
    if isinstance(agent_name, str) and agent_name.strip():
        return True
    for container_key in ("listingAgent", "agent"):
        agent = row.get(container_key)
        if isinstance(agent, dict):
            name = agent.get("name")
            if isinstance(name, str) and name.strip():
                return True
    if _row_has_listing_text(row):
        return True
    return False


def _load_last_apify_run() -> Optional[datetime]:
    try:
        if not APIFY_LAST_RUN_PATH.exists():
            return None
        raw = APIFY_LAST_RUN_PATH.read_text().strip()
        if not raw:
            return None
        try:
            parsed = datetime.strptime(raw, "%Y-%m-%d-%H")
            return SCHEDULER_TZ.localize(parsed)
        except ValueError:
            data = json.loads(raw)
            ts = data.get("ts")
            if not ts:
                return None
            parsed_dt = datetime.fromisoformat(ts)
            return (
                parsed_dt.astimezone(SCHEDULER_TZ)
                if parsed_dt.tzinfo
                else SCHEDULER_TZ.localize(parsed_dt)
            )
    except Exception:
        logger.debug("Unable to read Apify last-run marker", exc_info=True)
        return None


def _write_last_apify_run(ts: datetime) -> None:
    hour_key = apify_hour_key(ts)
    try:
        APIFY_LAST_RUN_PATH.parent.mkdir(parents=True, exist_ok=True)
        APIFY_LAST_RUN_PATH.write_text(hour_key)
    except Exception:
        logger.debug("Unable to persist Apify last-run marker", exc_info=True)


def _apify_hourly_task(run_time: datetime) -> None:
    try:
        current_slot = _hour_floor(run_time)
        hour_key = apify_hour_key(current_slot)
        if not apify_acquire_decision_slot(current_slot):
            logger.info("Apify hourly decision already handled for %s (decision lock)", hour_key)
            return

        local_hour, within_work_hours = apify_work_hours_status(current_slot)
        logger.info(
            "Apify work-hours check: local_hour=%s within_work_hours=%s",
            local_hour,
            within_work_hours,
        )
        if not within_work_hours and not APIFY_FORCE_EVERY_HOUR:
            logger.info("Apify hourly decision: skip %s (outside work hours)", hour_key)
            return
        if not within_work_hours and APIFY_FORCE_EVERY_HOUR:
            logger.info(
                "Apify hourly decision: outside work hours but APIFY_FORCE_EVERY_HOUR enabled; proceeding"
            )

        last_run = _load_last_apify_run()
        logger.info(
            "Apify last_run marker=%s force_every_hour=%s current_hour_key=%s",
            apify_hour_key(last_run) if last_run else None,
            APIFY_FORCE_EVERY_HOUR,
            hour_key,
        )
        if last_run and apify_hour_key(last_run) == hour_key:
            if APIFY_FORCE_EVERY_HOUR:
                logger.info(
                    "Apify hourly decision: already ran %s but APIFY_FORCE_EVERY_HOUR enabled; proceeding",
                    hour_key,
                )
            else:
                logger.info("Apify hourly decision: skip %s (already ran)", hour_key)
                return

        logger.info("Apify hourly decision: run %s", hour_key)
        triggered = _run_apify_actor()
        if not triggered:
            logger.warning("Apify actor did not start for hour %s", hour_key)
            return
        _write_last_apify_run(current_slot)
    except Exception:
        logger.exception("Apify hourly task failed for %s", run_time.isoformat())


def _process_incoming_rows(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    normalized_rows = [_normalize_apify_row(row) if isinstance(row, dict) else row for row in rows]
    fresh_rows = dedupe_rows_by_zpid(normalized_rows, logger)
    if not fresh_rows:
        logger.info("apify-hook: no fresh rows to process (all zpids already seen)")
        return {"status": "no new rows"}

    conn = ensure_table()
    db_filtered: List[Dict[str, Any]] = []
    for r in fresh_rows:
        zpid = r.get("zpid")
        if zpid in EXPORTED_ZPIDS:
            continue
        if conn.execute("SELECT 1 FROM listings WHERE zpid=?", (zpid,)).fetchone():
            EXPORTED_ZPIDS.add(zpid)
            continue
        db_filtered.append(r)

    if not db_filtered:
        logger.info("apify-hook: no fresh rows to process (all zpids already seen)")
        conn.close()
        return {"status": "no new rows"}

    rows_with_text = []
    missing_text = 0
    for row in db_filtered:
        if _row_has_listing_text(row):
            rows_with_text.append(row)
        else:
            missing_text += 1
            logger.warning(
                "apify-hook: zpid-only payload detected zpid=%s keys=%s",
                row.get("zpid"),
                list(row.keys()),
            )
    if missing_text:
        logger.warning(
            "apify-hook: filtered %d rows missing listing text (check webhook payload mapping)",
            missing_text,
        )
    db_filtered = rows_with_text
    if not db_filtered:
        logger.info("apify-hook: no rows with listing text after validation")
        conn.close()
        return {"status": "no rows", "reason": "missing listing text"}

    if APIFY_MAX_ITEMS and len(db_filtered) > APIFY_MAX_ITEMS:
        original_count = len(db_filtered)
        db_filtered = _select_recent_rows(db_filtered, APIFY_MAX_ITEMS)
        logger.info(
            "apify-hook: limiting to %d most recent listings (from %d)",
            len(db_filtered),
            original_count,
        )

    logger.debug("Sample fields on first fresh row: %s", list(db_filtered[0].keys())[:15])

    try:
        sms_jobs = process_rows(db_filtered, skip_dedupe=True) or []
    except Exception:
        logger.exception("process_rows failed; skipping batch to keep server alive")
        conn.close()
        return {"status": "error", "rows": len(db_filtered)}

    for job in sms_jobs:
        try:
            send_sms(job["phone"], job["message"])
        except Exception:
            logger.exception("send_sms failed for %s", job)

    EXPORTED_ZPIDS.update(r.get("zpid") for r in db_filtered)

    conn.executemany(
        "INSERT OR IGNORE INTO listings (zpid) VALUES (?)",
        [(r["zpid"],) for r in db_filtered],
    )
    conn.commit()
    conn.close()

    return {"status": "processed", "rows": len(db_filtered)}


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


def _run_apify_actor() -> bool:
    if not APIFY_ACTOR_ID or not APIFY_TOKEN:
        logger.info("Skipping Apify run – missing APIFY_ACTOR_ID or token")
        return False

    url = f"https://api.apify.com/v2/acts/{APIFY_ACTOR_ID}/runs"
    params = {
        "token": APIFY_TOKEN,
        "timeout": APIFY_TIMEOUT,
        "memory": APIFY_MEMORY,
        "clean": 1,
    }
    logger.info("Triggering Apify actor %s", APIFY_ACTOR_ID)
    req_kwargs: Dict[str, Any] = {"params": params, "timeout": APIFY_TIMEOUT + 30}
    if APIFY_INPUT is not None:
        req_kwargs["json"] = APIFY_INPUT

    resp: Optional[requests.Response] = None
    try:
        resp = requests.post(url, **req_kwargs)
        resp.raise_for_status()
    except requests.RequestException as exc:
        status = resp.status_code if resp is not None else None
        logger.error("Apify actor trigger failed with status %s: %s", status, exc)
        _record_apify_degradation("trigger_failed", status=status)
        return False

    run_info: Dict[str, Any] = {}
    try:
        run_info = resp.json() if resp.headers.get("content-type", "").startswith("application/json") else {}
    except ValueError:
        run_info = {}
    run_id = run_info.get("id")
    logger.info(
        "Apify actor run started (run_id=%s); awaiting listings POST",
        run_id or "unknown",
    )
    _clear_apify_degradation()
    return True


def _get_apify_run_status(run_id: str) -> Optional[str]:
    if not APIFY_TOKEN:
        return None
    url = f"https://api.apify.com/v2/actor-runs/{run_id}"
    try:
        resp = requests.get(url, params={"token": APIFY_TOKEN}, timeout=15)
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException:
        logger.debug("Failed to fetch Apify run status for %s", run_id, exc_info=True)
        return None
    except ValueError:
        return None
    status = data.get("data", {}).get("status")
    return status


@app.on_event("startup")
async def _start_scheduler() -> None:
    if DISABLE_APIFY_SCHEDULER:
        logger.info("DISABLE_APIFY_SCHEDULER enabled; skipping scheduler thread")
        return
    _ensure_scheduler_thread(
        hourly_callbacks=[_apify_hourly_task],
        initial_callbacks=True,
    )
    _ensure_keepalive_thread()


@app.on_event("shutdown")
async def _stop_scheduler() -> None:
    global _scheduler_thread, _scheduler_stop
    if _scheduler_stop:
        _scheduler_stop.set()
    if _scheduler_thread and _scheduler_thread.is_alive():
        _scheduler_thread.join(timeout=10)
    global _keepalive_thread, _keepalive_stop
    if _keepalive_stop:
        _keepalive_stop.set()
    if _keepalive_thread and _keepalive_thread.is_alive():
        _keepalive_thread.join(timeout=5)

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


_RELATIVE_TIME_RE = re.compile(
    r"(?P<count>\d+)\s*(?P<unit>minute|min|hour|hr|day|week|month|year)s?",
    re.IGNORECASE,
)


def _parse_relative_time(text: str) -> Optional[datetime]:
    matches = list(_RELATIVE_TIME_RE.finditer(text or ""))
    if not matches:
        return None
    now = datetime.utcnow()
    total = timedelta()
    for match in matches:
        count = int(match.group("count"))
        unit = match.group("unit").lower()
        if unit in {"minute", "min"}:
            total += timedelta(minutes=count)
        elif unit in {"hour", "hr"}:
            total += timedelta(hours=count)
        elif unit == "day":
            total += timedelta(days=count)
        elif unit == "week":
            total += timedelta(weeks=count)
        elif unit == "month":
            total += timedelta(days=30 * count)
        elif unit == "year":
            total += timedelta(days=365 * count)
    return now - total if total else None


def _parse_listing_timestamp(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        ts = float(value)
        if ts > 1e12:
            ts /= 1000.0
        try:
            return datetime.utcfromtimestamp(ts)
        except (OverflowError, OSError, ValueError):
            return None
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        if re.fullmatch(r"-?\d+(\.\d+)?", raw):
            try:
                ts = float(raw)
            except ValueError:
                return None
            if ts > 1e12:
                ts /= 1000.0
            try:
                return datetime.utcfromtimestamp(ts)
            except (OverflowError, OSError, ValueError):
                return None
        relative = _parse_relative_time(raw)
        if relative:
            return relative
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo:
            return parsed.astimezone(timezone.utc).replace(tzinfo=None)
        return parsed
    return None


def _extract_listing_timestamp(row: Dict[str, Any]) -> Optional[datetime]:
    for key in (
        "datePosted",
        "listedDate",
        "listingDate",
        "datePostedString",
        "timeOnZillowTimestamp",
        "timeOnZillow",
    ):
        if key in row:
            ts = _parse_listing_timestamp(row.get(key))
            if ts:
                return ts
    hdp_info = row.get("hdpData") or {}
    if isinstance(hdp_info, dict):
        home_info = hdp_info.get("homeInfo") or {}
        if isinstance(home_info, dict):
            for key in ("datePosted", "timeOnZillow", "timeOnZillowTimestamp"):
                ts = _parse_listing_timestamp(home_info.get(key))
                if ts:
                    return ts
    return None


def _select_recent_rows(rows: List[Dict[str, Any]], limit: int) -> List[Dict[str, Any]]:
    if limit <= 0 or len(rows) <= limit:
        return rows
    annotated: List[tuple] = []
    for idx, row in enumerate(rows):
        ts = _extract_listing_timestamp(row)
        annotated.append((ts, idx, row))
    if not any(ts for ts, _, _ in annotated):
        return rows[:limit]
    annotated.sort(
        key=lambda item: (
            item[0] is None,
            -(item[0].timestamp()) if item[0] else 0,
            item[1],
        )
    )
    return [row for _, _, row in annotated[:limit]]


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
    Accepts:
      • {"listings": [...]} – direct POST of listings (preferred)
      • {"items": [...]} or {"data": [...]} – alternate list payloads
      • {"datasetId": "..."} – fetch rows from that dataset (legacy webhooks)
    """
    body = await request.body()
    payload: Any = {}
    if body:
        try:
            payload = json.loads(body)
        except json.JSONDecodeError:
            content_type = request.headers.get("content-type", "")
            if "application/x-www-form-urlencoded" in content_type:
                parsed = urllib.parse.parse_qs(body.decode("utf-8", errors="ignore"))
                payload = {
                    key: values[0] if len(values) == 1 else values
                    for key, values in parsed.items()
                }
            else:
                payload = {}
    if payload == {}:
        logger.info("apify-hook: empty payload (ignored)")
        return {"status": "ignored", "reason": "empty payload"}
    try:
        logger.debug("Incoming webhook payload: %s", json.dumps(payload))
    except TypeError:
        logger.debug("Incoming webhook payload: %r", payload)

    dataset_id = None
    rows: Optional[List[Dict[str, Any]]] = None
    run_id = None
    row_source = "none"
    query_params = request.query_params

    if isinstance(payload, list):
        rows = payload
        row_source = "payload_list"
    elif isinstance(payload, dict):
        dataset_id = (
            payload.get("datasetId")
            or payload.get("dataset_id")
            or payload.get("datasetID")
            or payload.get("defaultDatasetId")
        )
        if isinstance(payload.get("items"), list):
            rows = payload.get("items")
            row_source = "payload.items"
        elif isinstance(payload.get("data"), list):
            rows = payload.get("data")
            row_source = "payload.data"
        elif isinstance(payload.get("listings"), list):
            rows = payload.get("listings")
            row_source = "payload.listings"

        run_id = payload.get("actorRunId") or payload.get("runId")
        resource = payload.get("resource")
        event_data = payload.get("eventData") if isinstance(payload.get("eventData"), dict) else None
        data_payload = payload.get("data") if isinstance(payload.get("data"), dict) else None
        if event_data and not resource:
            resource = event_data.get("resource")
        if data_payload and not resource:
            resource = data_payload.get("resource")
        if isinstance(resource, dict):
            dataset_id = dataset_id or resource.get("datasetId") or resource.get("defaultDatasetId")
            run_id = resource.get("id") or resource.get("runId") or run_id
        if not dataset_id and event_data:
            dataset_id = event_data.get("datasetId")
        if not dataset_id and data_payload:
            dataset_id = data_payload.get("datasetId")
        if event_data and not run_id:
            run_id = event_data.get("id") or event_data.get("runId")
        if data_payload and not run_id:
            run_id = data_payload.get("id") or data_payload.get("runId")

        if rows is None and event_data:
            if isinstance(event_data.get("items"), list):
                rows = event_data.get("items")
                row_source = "eventData.items"
            elif isinstance(event_data.get("item"), dict):
                rows = [event_data.get("item")]
                row_source = "eventData.item"
        if rows is None and data_payload:
            if isinstance(data_payload.get("items"), list):
                rows = data_payload.get("items")
                row_source = "data.items"
            elif isinstance(data_payload.get("item"), dict):
                rows = [data_payload.get("item")]
                row_source = "data.item"
            elif isinstance(data_payload.get("listings"), list):
                rows = data_payload.get("listings")
                row_source = "data.listings"

    dataset_id = dataset_id or query_params.get("datasetId") or query_params.get("dataset_id")
    if not dataset_id:
        if rows is not None:
            logger.info("apify-hook: processing %d rows included in webhook payload", len(rows))
        else:
            payload_keys = list(payload.keys()) if isinstance(payload, dict) else []
            logger.info(
                "apify-hook: ping received without datasetId or listings. Keys=%s",
                payload_keys,
            )
            return {"status": "ignored", "reason": "missing datasetId"}

    if rows is not None:
        normalized_rows: List[Dict[str, Any]] = []
        for row in rows:
            if isinstance(row, dict):
                normalized_rows.append(_normalize_apify_row(row))
            else:
                normalized_rows.append(row)
        rows = normalized_rows

        zpid_only_count = sum(1 for row in rows if not _row_has_expected_fields(row))
        if zpid_only_count == len(rows):
            logger.warning(
                "apify-hook: zpid-only payload received (rows=%d); %s",
                len(rows),
                "fetching dataset instead" if dataset_id else "rejecting payload",
            )
            if dataset_id:
                rows = None
                row_source = "none"
            else:
                return {"status": "rejected", "reason": "zpid-only payload"}
        elif zpid_only_count:
            logger.warning(
                "apify-hook: dropping %d rows missing address/agent/description fields",
                zpid_only_count,
            )
            rows = [row for row in rows if _row_has_expected_fields(row)]
            if not rows and dataset_id:
                rows = None
                row_source = "none"
            elif not rows:
                return {"status": "rejected", "reason": "missing required fields"}
    if rows is None:
        fetch_attempts = max(APIFY_FETCH_ATTEMPTS, 1)
        rows = []
        row_source = "dataset_fetch"
        deadline = datetime.utcnow() + timedelta(seconds=APIFY_FETCH_MAX_WAIT_SECONDS)
        attempt = 0
        last_status: Optional[str] = None
        while attempt < fetch_attempts and datetime.utcnow() <= deadline:
            attempt += 1
            try:
                rows = fetch_rows(dataset_id)
            except Exception:
                logger.exception("Failed to fetch dataset items for datasetId=%s", dataset_id)
                return {"status": "error", "reason": "fetch_rows_failed"}
            if rows:
                break
            if run_id:
                last_status = _get_apify_run_status(run_id)
                if last_status in {"SUCCEEDED", "FAILED", "ABORTED", "TIMED-OUT"}:
                    logger.info(
                        "apify-hook: run %s finished with status %s; dataset still empty",
                        run_id,
                        last_status,
                    )
                    break
            logger.debug(
                "apify-hook: dataset %s returned 0 rows on attempt %d/%d",
                dataset_id,
                attempt,
                fetch_attempts,
            )
            if attempt < fetch_attempts:
                await asyncio.sleep(APIFY_FETCH_BACKOFF_SECONDS * attempt)
        if rows:
            logger.info("apify-hook: fetched %d rows from dataset %s", len(rows), dataset_id)
        else:
            logger.info("apify-hook: dataset %s empty after retries", dataset_id)

    if rows:
        if row_source == "none":
            row_source = "payload"
        logger.info("apify-hook: row source=%s count=%d", row_source, len(rows))

    if not rows:
        logger.info("apify-hook: 0 listings received; no Apify retries scheduled")
        return {"status": "no rows"}

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
