# webhook_server.py – receives listings from Apify, de-dupes, writes sheet & SMS,
#                     **and now records inbound SMS replies via a webhook**

import asyncio
from datetime import datetime, timedelta, timezone
import json
import hashlib
import logging
import os
import re
import urllib.parse
import threading
from typing import Any, Dict, List, Optional

import requests
from fastapi import FastAPI, HTTPException, Request, Response
from starlette.requests import ClientDisconnect

import gspread
from google.oauth2.service_account import Credentials

from apify_fetcher import fetch_rows  # unchanged helper
from bot_min import (
    TZ,
    WORK_END,
    WORK_START,
    SCHEDULER_TZ,
    append_seen_zpids,
    dedupe_rows_by_zpid,
    fetch_contact_page,
    load_seen_zpids,
    log_headless_status,
    process_rows,
    run_hourly_scheduler,
)
from sms_providers import get_sender

# ──────────────────────────────────────────────────────────────────────
# Configuration & logging
# ──────────────────────────────────────────────────────────────────────
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


@app.on_event("startup")
async def _log_playwright_status() -> None:
    log_headless_status(logger)

# In-memory de-dupe cache of exported ZPIDs
EXPORTED_ZPIDS: set[str] = set()

_scheduler_thread: Optional[threading.Thread] = None
_scheduler_stop: Optional[threading.Event] = None
_keepalive_thread: Optional[threading.Thread] = None
_keepalive_stop: Optional[threading.Event] = None
_deferred_rows_lock = threading.Lock()
_deferred_rows: List[Dict[str, Any]] = []
_deferred_zpids: set[str] = set()
APIFY_TOKEN = os.getenv("APIFY_API_TOKEN") or os.getenv("APIFY_TOKEN")
APIFY_MAX_ITEMS = int(os.getenv("APIFY_MAX_ITEMS", "5"))
APIFY_FETCH_ATTEMPTS = int(os.getenv("APIFY_FETCH_ATTEMPTS", "6"))
APIFY_FETCH_BACKOFF_SECONDS = float(os.getenv("APIFY_FETCH_BACKOFF_SECONDS", "2.0"))
APIFY_FETCH_MAX_WAIT_SECONDS = float(os.getenv("APIFY_FETCH_MAX_WAIT_SECONDS", "300"))


def _should_run_immediately() -> bool:
    return os.getenv("SCHEDULER_RUN_IMMEDIATELY", "false").lower() == "true"


def _within_initial_hours(slot: datetime) -> bool:
    slot = slot.astimezone(SCHEDULER_TZ)
    return WORK_START <= slot.hour < WORK_END


def _next_initial_window(slot: datetime) -> datetime:
    slot = slot.astimezone(SCHEDULER_TZ)
    if slot.hour < WORK_START:
        return slot.replace(hour=WORK_START, minute=0, second=0, microsecond=0)
    if slot.hour >= WORK_END:
        next_day = slot + timedelta(days=1)
        return next_day.replace(hour=WORK_START, minute=0, second=0, microsecond=0)
    return slot


def _defer_rows(rows: List[Dict[str, Any]]) -> int:
    accepted = 0
    with _deferred_rows_lock:
        for row in rows:
            if not isinstance(row, dict):
                continue
            zpid = str(row.get("zpid", "")).strip()
            if zpid and zpid in _deferred_zpids:
                continue
            if zpid:
                _deferred_zpids.add(zpid)
            _deferred_rows.append(row)
            accepted += 1
    return accepted


def _drain_deferred_rows() -> List[Dict[str, Any]]:
    with _deferred_rows_lock:
        if not _deferred_rows:
            return []
        rows = list(_deferred_rows)
        _deferred_rows.clear()
        _deferred_zpids.clear()
    return rows


def _process_deferred_rows(run_time: datetime) -> None:
    if not _within_initial_hours(run_time):
        next_window = _next_initial_window(run_time)
        logger.info(
            "Deferred initial rows still outside work hours; next window at %s",
            next_window.isoformat(),
        )
        return
    rows = _drain_deferred_rows()
    if not rows:
        logger.info("No deferred initial rows to process")
        return
    logger.info("Processing %d deferred initial rows", len(rows))
    _process_incoming_rows(
        rows,
        skip_seen_dedupe=False,
        skip_seen_append=False,
        allow_deferred_drain=False,
    )


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
    for key in (
        "description",
        "listing_description",
        "listingDescription",
        "homeDescription",
        "marketingDescription",
        "openai_summary",
        "remarks",
        "publicRemarks",
        "brokerRemarks",
        "agentRemarks",
        "listingRemarks",
        "shortSaleDescription",
        "whatsSpecial",
        "whatsSpecialText",
        "listingText",
    ):
        value = row.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()

    # Fallback to nested hdpData.homeInfo if present
    home_info = (row.get("hdpData") or {}).get("homeInfo") or {}
    for key in (
        "description",
        "listingDescription",
        "homeDescription",
        "whatsSpecialText",
        "whatsSpecial",
    ):
        value = home_info.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()

    for path in (("property", "description"), ("property", "remarks"), ("listing", "description"), ("listing", "remarks")):
        current: Any = row
        for key in path:
            if not isinstance(current, dict):
                current = None
                break
            current = current.get(key)
        if isinstance(current, str) and current.strip():
            return current.strip()

    return ""


def _normalize_apify_row(row: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(row, dict):
        return row
    normalized = dict(row)

    agent_name = row.get("agentName")
    if not agent_name:
        for path in (
            ("attributionInfo", "agentName"),
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

    listing_text = extract_description(row)
    if listing_text:
        normalized.setdefault("listingText", listing_text)
        if not normalized.get("listing_description"):
            normalized["listing_description"] = listing_text
        normalized.setdefault(
            "listingTextHash",
            hashlib.sha256(listing_text.encode("utf-8")).hexdigest(),
        )
    elif not normalized.get("listing_description"):
        normalized["listing_description"] = ""

    return normalized


def _row_has_listing_text(row: Dict[str, Any]) -> bool:
    if not isinstance(row, dict):
        return False
    return bool(extract_description(row))


def _row_has_detail_marker(row: Dict[str, Any]) -> bool:
    if not isinstance(row, dict):
        return False
    if _row_has_listing_text(row):
        return True
    return bool(
        row.get("detailScrapedAt")
        or row.get("detail_scraped_at")
        or row.get("detailScrapeAt")
    )


def _prefer_detail_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    preferred: Dict[str, Dict[str, Any]] = {}
    extras: List[Dict[str, Any]] = []
    for row in rows:
        zpid = str(row.get("zpid", "")).strip() if isinstance(row, dict) else ""
        if not zpid:
            extras.append(row)
            continue
        existing = preferred.get(zpid)
        if not existing:
            preferred[zpid] = row
            continue
        if _row_has_detail_marker(existing):
            continue
        if _row_has_detail_marker(row):
            preferred[zpid] = row
    return list(preferred.values()) + extras


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


def _apify_run_source(run_id: Optional[str]) -> str:
    return "apify"


def _merge_rows_by_zpid(primary: List[Dict[str, Any]], secondary: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    merged: List[Dict[str, Any]] = []
    seen: dict[str, int] = {}
    for row in primary + secondary:
        if not isinstance(row, dict):
            merged.append(row)
            continue
        zpid = str(row.get("zpid", "")).strip()
        if zpid:
            existing_index = seen.get(zpid)
            if existing_index is None:
                seen[zpid] = len(merged)
                merged.append(row)
                continue
            existing = merged[existing_index]
            if not _row_has_detail_marker(existing) and _row_has_detail_marker(row):
                merged[existing_index] = row
            continue
        merged.append(row)
    return merged


def _format_listing_address(row: Dict[str, Any]) -> str:
    address = row.get("address") or row.get("street")
    if isinstance(address, str):
        return address.strip()
    if isinstance(address, dict):
        parts: List[str] = []
        street = (
            address.get("streetAddress")
            or address.get("streetAddress1")
            or address.get("street")
            or address.get("addressLine1")
        )
        if isinstance(street, str) and street.strip():
            parts.append(street.strip())
        city = address.get("city")
        state = address.get("state")
        zipcode = address.get("zip") or address.get("zipcode")
        locality = ", ".join(part.strip() for part in [city, state] if isinstance(part, str) and part.strip())
        if locality:
            parts.append(locality)
        if isinstance(zipcode, str) and zipcode.strip():
            parts.append(zipcode.strip())
        return ", ".join(parts)
    return ""


def _extract_hard_skip_zpids(payload: Dict[str, Any]) -> set[str]:
    candidates: List[Any] = []
    for key in ("hard_skip", "hardSkip", "hard_skip_zpids", "hardSkipZpids"):
        value = payload.get(key)
        if isinstance(value, list):
            candidates.extend(value)
        elif isinstance(value, str):
            candidates.append(value)
    return {str(val).strip() for val in candidates if str(val).strip()}


def _select_payload_listings(payload: Dict[str, Any]) -> Dict[str, Any]:
    received_rows = payload.get("listings")
    if not isinstance(received_rows, list):
        return {"rows": [], "received": 0, "hard_skipped": 0, "already_seen": 0, "selected": 0}
    seen_set = load_seen_zpids()
    hard_skip = _extract_hard_skip_zpids(payload)
    selected_rows: List[Dict[str, Any]] = []
    selected_zpids: List[str] = []
    selected_addresses: List[str] = []
    hard_skipped = 0
    already_seen = 0
    for row in received_rows:
        if not isinstance(row, dict):
            continue
        zpid = str(row.get("zpid", "")).strip()
        if not zpid:
            continue
        if zpid in hard_skip:
            hard_skipped += 1
            continue
        if zpid in seen_set:
            already_seen += 1
            continue
        if len(selected_rows) < 5:
            selected_rows.append(row)
            selected_zpids.append(zpid)
            selected_addresses.append(_format_listing_address(row))
    return {
        "rows": selected_rows,
        "received": len(received_rows),
        "hard_skipped": hard_skipped,
        "already_seen": already_seen,
        "selected": len(selected_rows),
        "selected_zpids": selected_zpids,
        "selected_addresses": [addr for addr in selected_addresses if addr],
    }


def _process_incoming_rows(
    rows: List[Dict[str, Any]],
    *,
    skip_seen_dedupe: bool = False,
    skip_seen_append: bool = False,
    allow_deferred_drain: bool = True,
) -> Dict[str, Any]:
    normalized_rows = [_normalize_apify_row(row) if isinstance(row, dict) else row for row in rows]
    normalized_rows = _prefer_detail_rows(normalized_rows)
    if skip_seen_dedupe:
        fresh_rows = normalized_rows
    else:
        fresh_rows = dedupe_rows_by_zpid(normalized_rows, logger, append_seen=False)
    if not fresh_rows:
        logger.info("apify-hook: no fresh rows to process (all zpids already seen)")
        return {"status": "no new rows"}

    db_filtered: List[Dict[str, Any]] = []
    for r in fresh_rows:
        zpid = r.get("zpid")
        if zpid in EXPORTED_ZPIDS:
            continue
        db_filtered.append(r)

    if not db_filtered:
        logger.info("apify-hook: no fresh rows to process (all zpids already seen)")
        return {"status": "no new rows"}

    missing_text = 0
    for row in db_filtered:
        if not _row_has_listing_text(row):
            missing_text += 1
            logger.warning(
                "apify-hook: listing text missing for zpid=%s keys=%s",
                row.get("zpid"),
                list(row.keys()),
            )
    if missing_text:
        logger.warning(
            "apify-hook: %d rows missing listing text; continuing to allow RapidAPI enrichment",
            missing_text,
        )

    if APIFY_MAX_ITEMS:
        original_count = len(db_filtered)
        db_filtered = _select_recent_rows(db_filtered, APIFY_MAX_ITEMS)
        logger.info(
            "apify-hook: selecting up to %d unseen listings (from %d)",
            len(db_filtered),
            original_count,
        )
        if not db_filtered:
            logger.info("apify-hook: no unseen rows to process after filter")
            return {"status": "no new rows"}

    now = datetime.now(tz=SCHEDULER_TZ)
    if allow_deferred_drain and _within_initial_hours(now):
        deferred_rows = _drain_deferred_rows()
        if deferred_rows:
            logger.info(
                "Draining %d deferred initial rows on webhook",
                len(deferred_rows),
            )
            _process_incoming_rows(
                deferred_rows,
                skip_seen_dedupe=False,
                skip_seen_append=False,
                allow_deferred_drain=False,
            )
    if not _within_initial_hours(now):
        deferred = _defer_rows(db_filtered)
        next_window = _next_initial_window(now)
        logger.info(
            "Initial processing outside work hours; deferred=%d next_window=%s now=%s",
            deferred,
            next_window.isoformat(),
            now.isoformat(),
        )
        return {"status": "deferred", "rows": deferred}

    if not skip_seen_append:
        append_seen_zpids(
            [
                str(row.get("zpid")).strip()
                for row in db_filtered
                if str(row.get("zpid", "")).strip()
            ]
        )

    logger.debug("Sample fields on first fresh row: %s", list(db_filtered[0].keys())[:15])

    try:
        sms_jobs = process_rows(db_filtered, skip_dedupe=True) or []
    except Exception:
        logger.exception("process_rows failed; skipping batch to keep server alive")
        return {"status": "error", "rows": len(db_filtered)}

    for job in sms_jobs:
        try:
            send_sms(job["phone"], job["message"])
        except Exception:
            logger.exception("send_sms failed for %s", job)

    detail_seen: List[str] = []
    for row in db_filtered:
        if _row_has_detail_marker(row):
            zpid = row.get("zpid")
            if zpid:
                detail_seen.append(zpid)

    EXPORTED_ZPIDS.update(detail_seen)

    return {"status": "processed", "rows": len(db_filtered)}


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
    logger.info("RENDER_APIFY_TRIGGER_DISABLED=true")
    if DISABLE_APIFY_SCHEDULER:
        logger.info("DISABLE_APIFY_SCHEDULER enabled; skipping scheduler thread")
        return
    _ensure_scheduler_thread(
        hourly_callbacks=[_process_deferred_rows],
        initial_callbacks=False,
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
    try:
        body = await request.body()
    except ClientDisconnect:
        logger.info("apify-hook: client disconnected while reading body")
        return Response(status_code=200)
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
        dataset_qs = request.query_params.get("datasetId") or request.query_params.get(
            "dataset_id"
        )
        if not dataset_qs:
            logger.info(
                "apify-hook: empty payload (ignored). content_type=%s content_length=%s",
                request.headers.get("content-type"),
                request.headers.get("content-length"),
            )
            return {"status": "ignored", "reason": "empty payload"}
        logger.info(
            "apify-hook: empty payload but datasetId provided via query params: %s",
            dataset_qs,
        )
    if isinstance(payload, dict):
        logger.debug("Incoming webhook payload keys=%s", list(payload.keys()))
    elif isinstance(payload, list):
        logger.debug("Incoming webhook payload list length=%s", len(payload))

    dataset_id = None
    rows: Optional[List[Dict[str, Any]]] = None
    run_id = None
    row_source = "none"
    query_params = request.query_params

    payload_listings = None
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
            payload_listings = rows

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
    if run_id:
        logger.info("apify-hook: run source=%s run_id=%s", _apify_run_source(run_id), run_id)
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

    if payload_listings is not None:
        selection = _select_payload_listings(payload)
        logger.info(
            "apify-hook: selection received=%s hard_skipped=%s already_seen=%s selected=%s",
            selection["received"],
            selection["hard_skipped"],
            selection["already_seen"],
            selection["selected"],
        )
        if selection.get("selected_zpids"):
            logger.info("apify-hook: selected zpids=%s", selection["selected_zpids"])
        if selection.get("selected_addresses"):
            logger.info("apify-hook: selected addresses=%s", selection["selected_addresses"])
        rows = selection["rows"]
        row_source = "payload.listings"

    if rows is not None:
        normalized_rows: List[Dict[str, Any]] = []
        for row in rows:
            if isinstance(row, dict):
                normalized_rows.append(_normalize_apify_row(row))
            else:
                normalized_rows.append(row)
        rows = normalized_rows
        detail_count = sum(1 for row in rows if _row_has_detail_marker(row))
        if detail_count == len(rows):
            logger.info("DETAIL_ENRICHED_PAYLOAD count=%d", detail_count)
        else:
            logger.warning(
                "DETAIL_ENRICHED_MISSING missing=%d total=%d",
                len(rows) - detail_count,
                len(rows),
            )

        if payload_listings is None:
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
    if dataset_id and rows is not None and payload_listings is None:
        try:
            fetched_rows = fetch_rows(dataset_id)
        except Exception:
            logger.exception("Failed to fetch dataset items for datasetId=%s", dataset_id)
            fetched_rows = []
        if fetched_rows:
            if rows:
                rows = _merge_rows_by_zpid(rows, fetched_rows)
                row_source = f"{row_source}+dataset_fetch"
            else:
                rows = fetched_rows
                row_source = "dataset_fetch"
            logger.info(
                "apify-hook: fetched %d rows from dataset %s (merged with payload)",
                len(fetched_rows),
                dataset_id,
            )
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

    return _process_incoming_rows(
        rows,
        skip_seen_dedupe=payload_listings is not None,
        skip_seen_append=False,
    )


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
