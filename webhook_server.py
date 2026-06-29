# webhook_server.py – receives listings from Apify, de-dupes, writes sheet & SMS,
#                     **and now records inbound SMS replies via a webhook**

import asyncio
from datetime import datetime, timedelta, timezone
import json
import hashlib
import logging
import os
import re
import subprocess
import sys
import time
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
    INITIAL_SMS_END,
    TZ,
    WORK_START,
    SCHEDULER_TZ,
    SMS_TEMPLATE,
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
SMS_API_KEY = os.getenv("SMS_GATEWAY_API_KEY") or os.getenv("SMS_API_KEY", "EhobscAL")
SMS_SENDER   = get_sender(SMS_PROVIDER)
DISABLE_APIFY_SCHEDULER = os.getenv("DISABLE_APIFY_SCHEDULER", "false").lower() == "true"
RENDER_APIFY_TRIGGER_DISABLED = (
    os.getenv("RENDER_APIFY_TRIGGER_DISABLED", "false").lower() == "true"
)
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
CODEX_AUTOMATION_TOKEN = os.getenv("CODEX_AUTOMATION_TOKEN", "").strip()
LEADS_SHEET_TAB = os.getenv("GSHEET_TAB", "Sheet1")
INITIAL_SMS_RETRY_ATTEMPTS = max(1, int(os.getenv("SMS_RETRY_ATTEMPTS", "3")))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
)
logger = logging.getLogger("webhook_server")
if not SMS_API_KEY:
    logger.warning("SMS gateway API key missing; outbound SMS will be skipped")

# FastAPI app
app            = FastAPI()


@app.on_event("startup")
async def _log_headless_status() -> None:
    async def _warm_headless_browser() -> None:
        try:
            await asyncio.to_thread(log_headless_status, logger)
        except Exception:
            logger.exception("HEADLESS_STATUS background warmup failed")

    asyncio.create_task(_warm_headless_browser())


@app.on_event("startup")
async def _recover_pending_queue() -> None:
    async def _recover_in_background() -> None:
        try:
            processed = await asyncio.to_thread(_process_pending_queue, startup=True)
            logger.info("queue: startup processed count=%d", processed)
        except Exception:
            logger.exception("queue: startup recovery failed")

    asyncio.create_task(_recover_in_background())

# In-memory de-dupe cache of exported ZPIDs
EXPORTED_ZPIDS: set[str] = set()

_scheduler_thread: Optional[threading.Thread] = None
_scheduler_stop: Optional[threading.Event] = None
_scheduler_start_lock = threading.Lock()
_scheduler_started = False
_keepalive_thread: Optional[threading.Thread] = None
_keepalive_stop: Optional[threading.Event] = None
_deferred_rows_lock = threading.Lock()
_deferred_rows: List[Dict[str, Any]] = []
_deferred_zpids: set[str] = set()
_queue_lock = threading.Lock()
_queue_worker_lock = threading.Lock()
_state_search_worker_lock = threading.Lock()
_apify_backstop_worker_lock = threading.Lock()
_free_source_pilot_worker_lock = threading.Lock()
_apify_backstop_day_lock = threading.Lock()
_original_payload_signature_lock = threading.Lock()
_previous_original_upstream_dataset_id: Optional[str] = None
_previous_original_ordered_zpids: List[str] = []
APIFY_TOKEN = os.getenv("APIFY_API_TOKEN") or os.getenv("APIFY_TOKEN")
APIFY_MAIN_TASK_ID = (
    os.getenv("APIFY_TASK_MAIN")
    or os.getenv("APIFY_MAIN_TASK_ID")
    or os.getenv("APIFY_SEARCH_TASK_ID")
    or "GPBSVcMBIK6CyJzBm"
).strip()
APIFY_MAX_ITEMS = int(os.getenv("APIFY_MAX_ITEMS", "5"))
APIFY_FETCH_ATTEMPTS = int(os.getenv("APIFY_FETCH_ATTEMPTS", "6"))
APIFY_FETCH_BACKOFF_SECONDS = float(os.getenv("APIFY_FETCH_BACKOFF_SECONDS", "2.0"))
APIFY_FETCH_MAX_WAIT_SECONDS = float(os.getenv("APIFY_FETCH_MAX_WAIT_SECONDS", "300"))
APIFY_STATE_SEARCH_ENABLED = os.getenv("APIFY_STATE_SEARCH_ENABLED", "true").lower() == "true"
APIFY_STATE_SEARCH_LIMIT = int(os.getenv("APIFY_STATE_SEARCH_LIMIT", "5"))
APIFY_STATE_SEARCH_FETCH_LIMIT = int(os.getenv("APIFY_STATE_SEARCH_FETCH_LIMIT", "25"))
APIFY_STATE_SEARCH_FETCH_LIMIT_DEFAULTS = {}
APIFY_STATE_SEARCH_TIMEOUT_SECONDS = float(os.getenv("APIFY_STATE_SEARCH_TIMEOUT_SECONDS", "60"))
APIFY_STATE_SEARCH_BACKGROUND = os.getenv("APIFY_STATE_SEARCH_BACKGROUND", "true").lower() == "true"
APIFY_STATE_DETAIL_TASK_ID = os.getenv("APIFY_STATE_DETAIL_TASK_ID", "VI5izq8RGAL14zM75").strip()
APIFY_STATE_DETAIL_TIMEOUT_SECONDS = float(os.getenv("APIFY_STATE_DETAIL_TIMEOUT_SECONDS", "240"))
PENDING_QUEUE_TAB = os.getenv("PENDING_QUEUE_TAB", "PendingQueue")
PENDING_QUEUE_STALE_MINUTES = int(os.getenv("PENDING_QUEUE_STALE_MINUTES", "30"))
APIFY_BACKSTOP_ENABLED = os.getenv("APIFY_BACKSTOP_ENABLED", "true").lower() == "true"
APIFY_BACKSTOP_HOUR = int(os.getenv("APIFY_BACKSTOP_HOUR", "18"))
APIFY_BACKSTOP_MAIN_FETCH_LIMIT = int(os.getenv("APIFY_BACKSTOP_MAIN_FETCH_LIMIT", "100"))
APIFY_BACKSTOP_MAIN_LIMIT = int(os.getenv("APIFY_BACKSTOP_MAIN_LIMIT", "10"))
APIFY_BACKSTOP_STATE_FETCH_LIMIT = int(os.getenv("APIFY_BACKSTOP_STATE_FETCH_LIMIT", "50"))
APIFY_BACKSTOP_STATE_FETCH_LIMIT_DEFAULTS = {"AK": 50, "HI": 50}
APIFY_BACKSTOP_STATE_LIMIT = int(os.getenv("APIFY_BACKSTOP_STATE_LIMIT", "10"))
APIFY_BACKSTOP_LOCK_PATH = os.getenv("APIFY_BACKSTOP_LOCK_PATH", "/tmp/apify_coverage_backstop.txt")
FREE_SOURCE_PILOT_ENABLED = os.getenv("FREE_SOURCE_PILOT_ENABLED", "true").lower() == "true"
FREE_SOURCE_PILOT_TAB = os.getenv("FREE_SOURCE_PILOT_TAB", "Lead Source Pilot")
FREE_SOURCE_PILOT_STATES = [
    state.strip().upper()
    for state in os.getenv("FREE_SOURCE_PILOT_STATES", "FL,CA,TX,WA,PA,HI,GA,MI").split(",")
    if state.strip()
]
FREE_SOURCE_PILOT_RESULTS_PER_QUERY = int(os.getenv("FREE_SOURCE_PILOT_RESULTS_PER_QUERY", "10"))
FREE_SOURCE_PILOT_SLEEP_SECONDS = float(os.getenv("FREE_SOURCE_PILOT_SLEEP_SECONDS", "1.0"))
_SENSITIVE_QUERY_PARAMS = {"token", "apikey", "api_key", "access_token", "authorization"}
_STATE_SEARCH_SOURCE_PRIORITY = {"ak": 0, "hi": 1}


def _redact_sensitive_url(url: str) -> str:
    if not isinstance(url, str) or not url:
        return url
    parsed = urllib.parse.urlsplit(url)
    if not parsed.query:
        return url
    query = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
    redacted_query = []
    for key, value in query:
        if key.lower() in _SENSITIVE_QUERY_PARAMS:
            redacted_query.append((key, "[REDACTED]"))
        else:
            redacted_query.append((key, value))
    return urllib.parse.urlunsplit(
        (parsed.scheme, parsed.netloc, parsed.path, urllib.parse.urlencode(redacted_query), parsed.fragment)
    )


def _format_request_exception(exc: requests.RequestException) -> str:
    message = str(exc)
    if message:
        return _redact_sensitive_url(message)
    req = getattr(exc, "request", None)
    if req is not None:
        req_url = getattr(req, "url", "")
        if req_url:
            return _redact_sensitive_url(req_url)
    return exc.__class__.__name__


def _task_enabled(task_id: Optional[str]) -> bool:
    return APIFY_STATE_SEARCH_ENABLED and bool(task_id)


def _state_search_fetch_limit(source: str) -> int:
    source_key = re.sub(r"[^A-Z0-9_]", "", str(source or "").upper())
    raw_value = os.getenv(f"APIFY_STATE_SEARCH_FETCH_LIMIT_{source_key}") if source_key else None
    if not raw_value:
        raw_value = os.getenv("APIFY_STATE_SEARCH_FETCH_LIMIT")
    if raw_value:
        try:
            return max(int(raw_value), 0)
        except ValueError:
            logger.warning("state-search: invalid fetch limit source=%s value=%s", source, raw_value)
    default_limit = APIFY_STATE_SEARCH_FETCH_LIMIT_DEFAULTS.get(source_key, APIFY_STATE_SEARCH_FETCH_LIMIT)
    return max(default_limit, 0)


def _backstop_state_fetch_limit(source: str) -> int:
    source_key = re.sub(r"[^A-Z0-9_]", "", str(source or "").upper())
    raw_value = os.getenv(f"APIFY_BACKSTOP_STATE_FETCH_LIMIT_{source_key}") if source_key else None
    if not raw_value:
        raw_value = os.getenv("APIFY_BACKSTOP_STATE_FETCH_LIMIT")
    if raw_value:
        try:
            return max(int(raw_value), 0)
        except ValueError:
            logger.warning("coverage-backstop: invalid fetch limit source=%s value=%s", source, raw_value)
    default_limit = APIFY_BACKSTOP_STATE_FETCH_LIMIT_DEFAULTS.get(
        source_key,
        APIFY_BACKSTOP_STATE_FETCH_LIMIT,
    )
    return max(default_limit, 0)


EXTRA_STATE_SEARCHES = [
    {"source": "ak", "task_id": os.getenv("APIFY_TASK_AK", "").strip(), "enabled": _task_enabled(os.getenv("APIFY_TASK_AK", "").strip())},
    {"source": "hi", "task_id": os.getenv("APIFY_TASK_HI", "").strip(), "enabled": _task_enabled(os.getenv("APIFY_TASK_HI", "").strip())},
]


def _prioritize_extra_state_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    indexed_rows = list(enumerate(rows))
    return [
        row
        for _, row in sorted(
            indexed_rows,
            key=lambda item: (
                _STATE_SEARCH_SOURCE_PRIORITY.get(
                    str(item[1].get("search_source") or item[1].get("source") or "").lower(),
                    99,
                ),
                item[0],
            ),
        )
    ]


def _run_state_detail_task_for_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not rows:
        return []
    if not APIFY_TOKEN:
        logger.warning("state-search: detail task skipped missing_apify_token")
        return []
    if not APIFY_STATE_DETAIL_TASK_ID:
        logger.warning("state-search: detail task skipped missing_task_id")
        return []

    selected_zpids: List[str] = []
    start_urls: List[Dict[str, str]] = []
    source_by_zpid: Dict[str, str] = {}
    seen_zpids: set[str] = set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        zpid = str(row.get("zpid", "")).strip()
        if not zpid or zpid in seen_zpids:
            continue
        detail_url = _extra_state_listing_url(row)
        if not detail_url:
            continue
        selected_zpids.append(zpid)
        start_urls.append({"url": detail_url})
        source_by_zpid[zpid] = str(row.get("search_source") or "state-search")
        seen_zpids.add(zpid)

    if not start_urls:
        return rows

    url = f"https://api.apify.com/v2/actor-tasks/{APIFY_STATE_DETAIL_TASK_ID}/run-sync-get-dataset-items"
    params = {
        "token": APIFY_TOKEN,
        "limit": len(start_urls),
        "clean": "true",
        "format": "json",
    }
    payload = {
        "startUrls": start_urls,
        "propertyStatus": "FOR_SALE",
        "extractBuildingUnits": "disabled",
        "maxConcurrency": min(max(len(start_urls), 1), 10),
    }
    try:
        resp = requests.post(
            url,
            params=params,
            json=payload,
            timeout=APIFY_STATE_DETAIL_TIMEOUT_SECONDS,
        )
        resp.raise_for_status()
        items = resp.json()
        if not isinstance(items, list):
            logger.warning(
                "state-search: detail_task_id=%s invalid_payload_type=%s",
                APIFY_STATE_DETAIL_TASK_ID,
                type(items).__name__,
            )
            return []

        detail_by_zpid: Dict[str, Dict[str, Any]] = {}
        dropped = 0
        for item in items:
            if not isinstance(item, dict):
                dropped += 1
                continue
            item_zpid = str(item.get("zpid", "")).strip()
            source = source_by_zpid.get(item_zpid, str(item.get("search_source") or "state-search"))
            normalized, drop_reason = _normalize_extra_state_row(item, source)
            if normalized is None:
                dropped += 1
                logger.info("state-search: detail dropped reason=%s", drop_reason)
                continue
            zpid = str(normalized.get("zpid", "")).strip()
            if not zpid or zpid not in source_by_zpid:
                dropped += 1
                continue
            normalized["search_source"] = source_by_zpid[zpid]
            detail_by_zpid[zpid] = normalized

        enriched: List[Dict[str, Any]] = []
        matched = 0
        for row in rows:
            if not isinstance(row, dict):
                enriched.append(row)
                continue
            zpid = str(row.get("zpid", "")).strip()
            detail = detail_by_zpid.get(zpid)
            if not detail:
                enriched.append(row)
                continue
            merged = dict(row)
            merged.update(detail)
            if row.get("search_source"):
                merged["search_source"] = row.get("search_source")
            enriched.append(merged)
            matched += 1

        logger.info(
            "state-search: detail_task_id=%s requested=%d returned=%d matched=%d dropped=%d",
            APIFY_STATE_DETAIL_TASK_ID,
            len(selected_zpids),
            len(items),
            matched,
            dropped,
        )
        return enriched
    except requests.Timeout:
        logger.warning("state-search: detail_task_id=%s timeout", APIFY_STATE_DETAIL_TASK_ID)
    except requests.RequestException as exc:
        logger.warning(
            "state-search: detail_task_id=%s request_error=%s",
            APIFY_STATE_DETAIL_TASK_ID,
            _format_request_exception(exc),
        )
    except ValueError:
        logger.warning("state-search: detail_task_id=%s invalid_json", APIFY_STATE_DETAIL_TASK_ID)
    return []


def _run_apify_search_task_sync_dataset_items(
    task_id: str,
    source: str,
    *,
    fetch_limit: int,
    log_prefix: str,
) -> List[Dict[str, Any]]:
    if not APIFY_TOKEN:
        logger.warning("%s: source=%s task_id=%s skipped missing_apify_token", log_prefix, source, task_id)
        return []
    limit = max(fetch_limit, 0)
    if limit <= 0:
        logger.info("%s: source=%s task_id=%s skipped limit=%s", log_prefix, source, task_id, fetch_limit)
        return []

    url = f"https://api.apify.com/v2/actor-tasks/{task_id}/run-sync-get-dataset-items"
    params = {
        "token": APIFY_TOKEN,
        "limit": limit,
        "maxItems": limit,
        "desc": "true",
        "clean": "true",
    }
    try:
        resp = requests.get(url, params=params, timeout=APIFY_STATE_SEARCH_TIMEOUT_SECONDS)
        resp.raise_for_status()
        payload = resp.json()
        if not isinstance(payload, list):
            logger.warning(
                "%s: source=%s task_id=%s invalid_payload_type=%s",
                log_prefix,
                source,
                task_id,
                type(payload).__name__,
            )
            return []
        raw_rows = [item for item in payload[:limit] if isinstance(item, dict)]
        prepared = _prepare_extra_state_rows(raw_rows, source, log_prefix=log_prefix)
        logger.info(
            "%s: source=%s task_id=%s search_only_prepared=%d",
            log_prefix,
            source,
            task_id,
            len(prepared["rows"]),
        )
        return prepared["rows"]
    except requests.Timeout:
        logger.warning("%s: source=%s task_id=%s timeout", log_prefix, source, task_id)
    except requests.RequestException as exc:
        logger.warning(
            "%s: source=%s task_id=%s request_error=%s",
            log_prefix,
            source,
            task_id,
            _format_request_exception(exc),
        )
    except ValueError:
        logger.warning("%s: source=%s task_id=%s invalid_json", log_prefix, source, task_id)
    return []


def _run_state_task_sync_dataset_items(task_id: str, source: str) -> List[Dict[str, Any]]:
    selection_limit = max(APIFY_STATE_SEARCH_LIMIT, 0)
    if selection_limit <= 0:
        logger.info("state-search: source=%s task_id=%s skipped limit=%s", source, task_id, APIFY_STATE_SEARCH_LIMIT)
        return []
    limit = max(_state_search_fetch_limit(source), selection_limit)
    return _run_apify_search_task_sync_dataset_items(
        task_id,
        source,
        fetch_limit=limit,
        log_prefix="state-search",
    )


def _fetch_extra_state_rows() -> List[Dict[str, Any]]:
    if not APIFY_STATE_SEARCH_ENABLED:
        return []
    collected: List[Dict[str, Any]] = []
    for cfg in EXTRA_STATE_SEARCHES:
        source = cfg["source"]
        task_id = cfg["task_id"]
        enabled = bool(cfg.get("enabled"))
        if not enabled:
            if task_id:
                logger.info("state-search: source=%s task_id=%s disabled", source, task_id)
            continue
        rows = _run_state_task_sync_dataset_items(task_id, source)
        collected.extend(rows)
    return collected


def _enqueue_extra_state_rows(payload: Dict[str, Any]) -> int:
    extra_state_rows = _fetch_extra_state_rows()
    logger.info(
        "state-search: fetch_once invoked enabled=%s fetched=%d",
        APIFY_STATE_SEARCH_ENABLED,
        len(extra_state_rows),
    )
    if not extra_state_rows:
        return 0

    extra_state_rows = _prioritize_extra_state_rows(extra_state_rows)
    extra_selection = _select_unseen_rows(
        extra_state_rows,
        _pending_queue_state_skip_zpids(),
        _extract_hard_skip_zpids(payload),
        max_rows=max(APIFY_STATE_SEARCH_LIMIT, 0),
    )
    logger.info(
        "state-search: unseen_filter received=%d hard_skipped=%d already_seen=%d invalid=%d kept=%d",
        extra_selection["received"],
        extra_selection["hard_skipped"],
        extra_selection["already_seen"],
        extra_selection.get("invalid_rows", 0),
        extra_selection["selected"],
    )
    if not extra_selection["rows"]:
        return 0
    logger.info("state-search: selected_for_detail=%d", len(extra_selection["rows"]))
    detail_rows = _run_state_detail_task_for_rows(extra_selection["rows"])
    extra_enqueued = _enqueue_pending_rows(detail_rows, source="state-search")
    logger.info("state-search: enqueued_extra=%d", extra_enqueued)
    return extra_enqueued


def _fetch_apify_backstop_main_rows() -> List[Dict[str, Any]]:
    if not APIFY_MAIN_TASK_ID:
        logger.info("coverage-backstop: main skipped missing_task_id")
        return []
    if APIFY_BACKSTOP_MAIN_FETCH_LIMIT <= 0:
        logger.info("coverage-backstop: main skipped fetch_limit=%s", APIFY_BACKSTOP_MAIN_FETCH_LIMIT)
        return []
    return _run_apify_search_task_sync_dataset_items(
        APIFY_MAIN_TASK_ID,
        "main",
        fetch_limit=max(APIFY_BACKSTOP_MAIN_FETCH_LIMIT, APIFY_BACKSTOP_MAIN_LIMIT),
        log_prefix="coverage-backstop",
    )


def _fetch_apify_backstop_state_rows() -> List[Dict[str, Any]]:
    if not APIFY_STATE_SEARCH_ENABLED:
        return []
    collected: List[Dict[str, Any]] = []
    for cfg in EXTRA_STATE_SEARCHES:
        source = cfg["source"]
        task_id = cfg["task_id"]
        enabled = bool(cfg.get("enabled"))
        if not enabled:
            if task_id:
                logger.info("coverage-backstop: source=%s task_id=%s disabled", source, task_id)
            continue
        fetch_limit = _backstop_state_fetch_limit(source)
        if fetch_limit <= 0:
            logger.info("coverage-backstop: source=%s task_id=%s skipped fetch_limit=%s", source, task_id, fetch_limit)
            continue
        rows = _run_apify_search_task_sync_dataset_items(
            task_id,
            source,
            fetch_limit=max(fetch_limit, APIFY_BACKSTOP_STATE_LIMIT),
            log_prefix="coverage-backstop",
        )
        collected.extend(rows)
    return _prioritize_extra_state_rows(collected)


def _coverage_backstop_skip_zpids() -> set[str]:
    skip: set[str] = set()
    try:
        skip.update(load_seen_zpids())
    except Exception:
        logger.warning("coverage-backstop: failed to load seen zpids", exc_info=True)
    skip.update(_pending_queue_state_skip_zpids())
    return skip


def _enqueue_apify_backstop_rows(
    rows: List[Dict[str, Any]],
    *,
    source: str,
    max_rows: int,
) -> int:
    if max_rows <= 0:
        logger.info("coverage-backstop: source=%s skipped limit=%s", source, max_rows)
        return 0
    selection = _select_unseen_rows(
        rows,
        _coverage_backstop_skip_zpids(),
        set(),
        max_rows=max(max_rows, 0),
    )
    logger.info(
        "coverage-backstop: source=%s unseen_filter received=%d hard_skipped=%d already_seen=%d invalid=%d kept=%d",
        source,
        selection["received"],
        selection["hard_skipped"],
        selection["already_seen"],
        selection.get("invalid_rows", 0),
        selection["selected"],
    )
    if not selection["rows"]:
        return 0
    logger.info("coverage-backstop: source=%s selected_for_detail=%d", source, len(selection["rows"]))
    detail_rows = _run_state_detail_task_for_rows(selection["rows"])
    enqueued = _enqueue_pending_rows(detail_rows, source=source)
    logger.info("coverage-backstop: source=%s enqueued=%d", source, enqueued)
    return enqueued


def _enqueue_apify_coverage_backstop() -> int:
    if not APIFY_BACKSTOP_ENABLED:
        logger.info("coverage-backstop: skipped disabled")
        return 0
    main_rows = _fetch_apify_backstop_main_rows()
    main_enqueued = _enqueue_apify_backstop_rows(
        main_rows,
        source="coverage-backstop-main",
        max_rows=max(APIFY_BACKSTOP_MAIN_LIMIT, 0),
    )
    state_rows = _fetch_apify_backstop_state_rows()
    state_enqueued = _enqueue_apify_backstop_rows(
        state_rows,
        source="coverage-backstop-state",
        max_rows=max(APIFY_BACKSTOP_STATE_LIMIT, 0),
    )
    total = main_enqueued + state_enqueued
    logger.info(
        "coverage-backstop: enqueued_total=%d main=%d state=%d",
        total,
        main_enqueued,
        state_enqueued,
    )
    return total


def _apify_backstop_due(run_time: datetime) -> bool:
    if not APIFY_BACKSTOP_ENABLED:
        return False
    if not APIFY_TOKEN:
        return False
    local_dt = run_time.astimezone(SCHEDULER_TZ)
    return local_dt.hour == APIFY_BACKSTOP_HOUR


def _apify_backstop_day_key(run_time: datetime) -> str:
    return run_time.astimezone(SCHEDULER_TZ).strftime("%Y-%m-%d")


def _acquire_apify_backstop_day(run_time: datetime) -> bool:
    day_key = _apify_backstop_day_key(run_time)
    lock_file = f"{APIFY_BACKSTOP_LOCK_PATH}.{day_key}"
    with _apify_backstop_day_lock:
        try:
            parent = os.path.dirname(lock_file)
            if parent:
                os.makedirs(parent, exist_ok=True)
            fd = os.open(lock_file, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            logger.info("coverage-backstop: skipped already_ran day=%s", day_key)
            return False
        except Exception:
            logger.warning("coverage-backstop: failed to acquire day lock day=%s", day_key, exc_info=True)
            return False
        try:
            os.close(fd)
        except Exception:
            logger.debug("coverage-backstop: failed to close day lock fd", exc_info=True)
    return True


def _start_apify_coverage_backstop(run_time: Optional[datetime] = None) -> None:
    run_time = run_time or datetime.now(tz=SCHEDULER_TZ)
    if not _apify_backstop_due(run_time):
        return
    if not _apify_backstop_worker_lock.acquire(blocking=False):
        logger.info("coverage-backstop: worker already running; skipped duplicate trigger")
        return
    if not _acquire_apify_backstop_day(run_time):
        _apify_backstop_worker_lock.release()
        return

    def _runner() -> None:
        try:
            enqueued = _enqueue_apify_coverage_backstop()
            if enqueued and _within_initial_hours(datetime.now(tz=SCHEDULER_TZ)):
                processed = _process_pending_queue()
                logger.info("coverage-backstop: processed count=%d", processed)
        except Exception:
            logger.exception("coverage-backstop: worker failed")
        finally:
            _apify_backstop_worker_lock.release()

    threading.Thread(target=_runner, name="apify-coverage-backstop", daemon=True).start()
    logger.info("coverage-backstop: background worker started")


def _process_apify_coverage_backstop_callback(run_time: datetime) -> None:
    _start_apify_coverage_backstop(run_time)


def _free_source_pilot_due(run_time: datetime) -> bool:
    if not FREE_SOURCE_PILOT_ENABLED:
        return False
    local_dt = run_time.astimezone(SCHEDULER_TZ)
    return WORK_START <= local_dt.hour <= WORK_END


def _run_free_source_pilot(run_time: datetime) -> None:
    if not _free_source_pilot_due(run_time):
        logger.info("free-source-pilot: skipped outside scheduled window run_time=%s", run_time.isoformat())
        return
    if not FREE_SOURCE_PILOT_STATES:
        logger.info("free-source-pilot: skipped no states configured")
        return
    if not _free_source_pilot_worker_lock.acquire(blocking=False):
        logger.info("free-source-pilot: skipped overlapping run")
        return

    script_path = os.path.join(os.path.dirname(__file__), "scripts", "free_short_sale_source_pilot.py")
    cmd = [
        sys.executable,
        script_path,
        "--spreadsheet-id",
        GSHEET_ID,
        "--main-tab",
        LEADS_SHEET_TAB,
        "--pilot-tab",
        FREE_SOURCE_PILOT_TAB,
        "--states",
        *FREE_SOURCE_PILOT_STATES,
        "--results-per-query",
        str(FREE_SOURCE_PILOT_RESULTS_PER_QUERY),
        "--sleep-seconds",
        str(FREE_SOURCE_PILOT_SLEEP_SECONDS),
    ]
    try:
        logger.info(
            "free-source-pilot: starting run_time=%s states=%s results_per_query=%s tab=%s",
            run_time.isoformat(),
            ",".join(FREE_SOURCE_PILOT_STATES),
            FREE_SOURCE_PILOT_RESULTS_PER_QUERY,
            FREE_SOURCE_PILOT_TAB,
        )
        completed = subprocess.run(
            cmd,
            cwd=os.path.dirname(__file__),
            capture_output=True,
            text=True,
            timeout=50 * 60,
            check=False,
        )
        if completed.returncode:
            logger.error(
                "free-source-pilot: failed returncode=%s stdout=%s stderr=%s",
                completed.returncode,
                completed.stdout[-4000:],
                completed.stderr[-4000:],
            )
            return
        logger.info("free-source-pilot: completed stdout=%s", completed.stdout[-4000:])
        if completed.stderr.strip():
            logger.warning("free-source-pilot: stderr=%s", completed.stderr[-4000:])
    except subprocess.TimeoutExpired as exc:
        logger.error("free-source-pilot: timed out after %.0fs", exc.timeout)
    except Exception:
        logger.exception("free-source-pilot: crashed")
    finally:
        _free_source_pilot_worker_lock.release()


def _process_free_source_pilot_callback(run_time: datetime) -> None:
    threading.Thread(
        target=_run_free_source_pilot,
        args=(run_time,),
        name="free-source-pilot",
        daemon=True,
    ).start()


def _start_extra_state_rows(payload: Dict[str, Any]) -> None:
    if not APIFY_STATE_SEARCH_BACKGROUND:
        _enqueue_extra_state_rows(payload)
        return

    if not _state_search_worker_lock.acquire(blocking=False):
        logger.info("state-search: background worker already running; skipped duplicate trigger")
        return

    payload_copy = dict(payload) if isinstance(payload, dict) else {}

    def _runner() -> None:
        try:
            enqueued = _enqueue_extra_state_rows(payload_copy)
            if enqueued and _within_initial_hours(datetime.now(tz=SCHEDULER_TZ)):
                processed = _process_pending_queue()
                logger.info("state-search: background processed count=%d", processed)
        except Exception:
            logger.exception("state-search: background worker failed")
        finally:
            _state_search_worker_lock.release()

    threading.Thread(target=_runner, name="state-search-worker", daemon=True).start()
    logger.info("state-search: background worker started")


def _should_run_immediately() -> bool:
    return os.getenv("SCHEDULER_RUN_IMMEDIATELY", "false").lower() == "true"


def _within_initial_hours(slot: datetime) -> bool:
    slot = slot.astimezone(SCHEDULER_TZ)
    return WORK_START <= slot.hour < INITIAL_SMS_END


def _next_initial_window(slot: datetime) -> datetime:
    slot = slot.astimezone(SCHEDULER_TZ)
    if slot.hour < WORK_START:
        return slot.replace(hour=WORK_START, minute=0, second=0, microsecond=0)
    if slot.hour >= INITIAL_SMS_END:
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
        source="deferred_scheduler",
        skip_seen_dedupe=False,
        skip_seen_append=False,
        allow_deferred_drain=False,
    )


def _process_pending_rows_callback(run_time: datetime) -> None:
    if not _within_initial_hours(run_time):
        return
    processed = _process_pending_queue()
    if processed:
        logger.info("queue: scheduler processed count=%d", processed)


def _ensure_scheduler_thread(
    hourly_callbacks: Optional[List] = None,
    *,
    initial_callbacks: bool = True,
) -> None:
    global _scheduler_thread, _scheduler_stop, _scheduler_started
    with _scheduler_start_lock:
        if _scheduler_started:
            logger.info("scheduler already started")
            return
        if _scheduler_thread and _scheduler_thread.is_alive():
            _scheduler_started = True
            logger.info("scheduler already started")
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
        _scheduler_started = True


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


def _street_only_address(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    text = value.strip()
    if not text:
        return ""
    return text.split(",", 1)[0].strip()


def _format_sms_listing_address(row: Dict[str, Any]) -> str:
    street = _street_only_address(row.get("street"))
    if street:
        return street

    address = row.get("address")
    if isinstance(address, dict):
        for key in ("streetAddress", "streetAddress1", "street", "addressLine1"):
            street = _street_only_address(address.get(key))
            if street:
                return street
        return ""

    return _street_only_address(address)


def _extract_hard_skip_zpids(payload: Dict[str, Any]) -> set[str]:
    candidates: List[Any] = []
    for key in ("hard_skip", "hardSkip", "hard_skip_zpids", "hardSkipZpids"):
        value = payload.get(key)
        if isinstance(value, list):
            candidates.extend(value)
        elif isinstance(value, str):
            candidates.append(value)
    return {str(val).strip() for val in candidates if str(val).strip()}


def _select_unseen_rows(
    received_rows: List[Dict[str, Any]],
    seen_set: set[str],
    hard_skip: set[str],
    *,
    max_rows: Optional[int] = None,
) -> Dict[str, Any]:
    selected_rows: List[Dict[str, Any]] = []
    selected_zpids: List[str] = []
    selected_addresses: List[str] = []
    hard_skipped = 0
    already_seen = 0
    invalid_rows = 0
    selected_zpid_set: set[str] = set()

    for row in received_rows:
        if not isinstance(row, dict):
            invalid_rows += 1
            continue
        zpid = str(row.get("zpid", "")).strip()
        if zpid and zpid in hard_skip:
            hard_skipped += 1
            continue
        if zpid and zpid in seen_set:
            already_seen += 1
            continue
        if zpid and zpid in selected_zpid_set:
            continue
        if max_rows is not None and len(selected_rows) >= max_rows:
            continue

        selected_rows.append(row)
        if zpid:
            selected_zpid_set.add(zpid)
            selected_zpids.append(zpid)
        selected_addresses.append(_format_listing_address(row))

    return {
        "rows": selected_rows,
        "received": len(received_rows),
        "hard_skipped": hard_skipped,
        "already_seen": already_seen,
        "invalid_rows": invalid_rows,
        "selected": len(selected_rows),
        "selected_zpids": selected_zpids,
        "selected_addresses": [addr for addr in selected_addresses if addr],
    }


_ZPID_IN_URL_RE = re.compile(r"/(\d+)_zpid", re.IGNORECASE)


def _extra_state_listing_url(row: Dict[str, Any]) -> str:
    for key in (
        "detailUrl",
        "detailURL",
        "propertyUrl",
        "propertyURL",
        "listingUrl",
        "listingURL",
        "url",
        "href",
    ):
        value = row.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def _normalize_extra_state_row(row: Dict[str, Any], source: str) -> tuple[Optional[Dict[str, Any]], Optional[str]]:
    if not isinstance(row, dict):
        return None, "invalid_type"
    if row.get("error"):
        return None, "error_field"

    candidate = dict(row)
    candidate["search_source"] = source

    detail_url = _extra_state_listing_url(candidate)
    if detail_url:
        candidate["detailUrl"] = detail_url
        candidate.setdefault("propertyUrl", detail_url)

    zpid_val = candidate.get("zpid")
    if not zpid_val:
        for path in (("property", "zpid"), ("listing", "zpid"), ("home", "zpid")):
            current: Any = candidate
            for key in path:
                if not isinstance(current, dict):
                    current = None
                    break
                current = current.get(key)
            if current:
                zpid_val = current
                break
    if not zpid_val and detail_url:
        match = _ZPID_IN_URL_RE.search(detail_url)
        if match:
            zpid_val = match.group(1)
    if zpid_val:
        candidate["zpid"] = str(zpid_val).strip()

    listing_text = extract_description(candidate)
    if listing_text:
        candidate.setdefault("listingText", listing_text)
        candidate.setdefault("description", listing_text)
        candidate["listing_description"] = listing_text

    normalized = _normalize_apify_row(candidate)
    has_zpid = bool(str(normalized.get("zpid", "")).strip())
    has_url = bool(_extra_state_listing_url(normalized))
    if not has_zpid and not has_url:
        return None, "missing_zpid_and_url"

    if not _row_has_expected_fields(normalized) and not (has_zpid and has_url):
        return None, "not_valid_candidate"

    return normalized, None


def _select_payload_listings(payload: Dict[str, Any]) -> Dict[str, Any]:
    received_rows = payload.get("listings")
    if not isinstance(received_rows, list):
        return {"rows": [], "received": 0, "hard_skipped": 0, "already_seen": 0, "invalid_rows": 0, "selected": 0}
    seen_set = load_seen_zpids()
    hard_skip = _extract_hard_skip_zpids(payload)
    return _select_unseen_rows(received_rows, seen_set, hard_skip, max_rows=5)


def _prepare_extra_state_rows(
    raw_rows: List[Dict[str, Any]],
    source: str,
    *,
    log_prefix: str = "state-search",
) -> Dict[str, Any]:
    fetched = len(raw_rows)
    kept: List[Dict[str, Any]] = []
    dropped_error = 0
    dropped_missing_id_url = 0
    dropped_invalid = 0

    for row in raw_rows:
        normalized, drop_reason = _normalize_extra_state_row(row, source)
        if normalized:
            kept.append(normalized)
            continue
        if drop_reason == "error_field":
            dropped_error += 1
        elif drop_reason == "missing_zpid_and_url":
            dropped_missing_id_url += 1
        else:
            dropped_invalid += 1

    logger.info(
        "%s: source=%s fetched=%d dropped_error=%d dropped_missing_id_url=%d dropped_invalid=%d normalized_kept=%d",
        log_prefix,
        source,
        fetched,
        dropped_error,
        dropped_missing_id_url,
        dropped_invalid,
        len(kept),
    )
    return {
        "rows": kept,
        "fetched": fetched,
        "dropped_error": dropped_error,
        "dropped_missing_id_url": dropped_missing_id_url,
        "dropped_invalid": dropped_invalid,
    }



def _process_incoming_rows(
    rows: List[Dict[str, Any]],
    *,
    source: str = "",
    skip_seen_dedupe: bool = False,
    skip_seen_append: bool = False,
    allow_deferred_drain: bool = True,
    skip_enqueue: bool = False,
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

    if not skip_enqueue:
        _enqueue_pending_rows(db_filtered, source=source)

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
                source="deferred_drain",
                skip_seen_dedupe=False,
                skip_seen_append=False,
                allow_deferred_drain=False,
                skip_enqueue=False,
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
    processed = _process_pending_queue()
    return {"status": "processed", "rows": len(db_filtered), "processed": processed}


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


def _job_message(job: Dict[str, Any]) -> str:
    raw_message = str(job.get("message") or "").strip()
    if raw_message:
        return raw_message
    first = str(job.get("first") or "").strip()
    address = _street_only_address(job.get("street")) or _street_only_address(job.get("address"))
    if first or address:
        return SMS_TEMPLATE.format(first=first, address=address).strip()
    return ""


@app.on_event("startup")
async def _start_scheduler() -> None:
    if RENDER_APIFY_TRIGGER_DISABLED:
        logger.info("RENDER_APIFY_TRIGGER_DISABLED=true")
        logger.info(
            "Apify trigger disabled; new listings will not be fetched unless another source provides them."
        )
    else:
        logger.info("RENDER_APIFY_TRIGGER_DISABLED=false")
        logger.info("Apify trigger enabled; new listings will be fetched via Apify.")
    if DISABLE_APIFY_SCHEDULER:
        logger.info("DISABLE_APIFY_SCHEDULER enabled; skipping scheduler thread")
        return
    _ensure_scheduler_thread(
        hourly_callbacks=[
            _process_deferred_rows,
            _process_pending_rows_callback,
            _process_apify_coverage_backstop_callback,
            _process_free_source_pilot_callback,
        ],
        initial_callbacks=False,
    )
    _ensure_keepalive_thread()


@app.on_event("shutdown")
async def _stop_scheduler() -> None:
    global _scheduler_thread, _scheduler_stop, _scheduler_started
    if _scheduler_stop:
        _scheduler_stop.set()
    if _scheduler_thread and _scheduler_thread.is_alive():
        _scheduler_thread.join(timeout=10)
    with _scheduler_start_lock:
        _scheduler_started = False
        _scheduler_thread = None
        _scheduler_stop = None
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
_GSHEET_RETRY_STATUS = {429, 500, 503}


def _should_retry_gspread_error(exc: Exception) -> bool:
    if isinstance(exc, gspread.exceptions.APIError):
        status = getattr(exc.response, "status_code", None)
        return status in _GSHEET_RETRY_STATUS
    return False


def _retry_gspread_call(label: str, func):
    delay = 1.0
    for attempt in range(1, 4):
        try:
            return func()
        except Exception as exc:
            if attempt >= 3 or not _should_retry_gspread_error(exc):
                raise
            logger.warning(
                "Google Sheets %s failed (%s); retrying in %.1fs (attempt %s/3)",
                label,
                exc,
                delay,
                attempt,
            )
            time.sleep(delay)
            delay *= 2

def get_replies_ws():
    """Ensure a 'Replies' sheet exists and return the worksheet handle."""
    try:
        workbook = _retry_gspread_call(
            "open workbook",
            lambda: gclient.open_by_key(GSHEET_ID),
        )
        return _retry_gspread_call(
            "open Replies worksheet",
            lambda: workbook.worksheet("Replies"),
        )
    except gspread.WorksheetNotFound:
        workbook = _retry_gspread_call(
            "open workbook",
            lambda: gclient.open_by_key(GSHEET_ID),
        )
        ws = _retry_gspread_call(
            "create Replies worksheet",
            lambda: workbook.add_worksheet(title="Replies", rows="1000", cols="3"),
        )
        ws.append_row(["phone", "time_received", "message"])
        return ws

REPLIES_WS = get_replies_ws()

QUEUE_HEADERS = [
    "zpid",
    "address",
    "source",
    "created_at",
    "status",
    "claimed_at",
    "processed_at",
    "result",
    "error",
    "listing_json",
]
GOOGLE_SHEETS_MAX_CELL_CHARS = 50_000
QUEUE_CELL_SAFE_LIMIT = GOOGLE_SHEETS_MAX_CELL_CHARS - 100
QUEUE_REQUIRED_PAYLOAD_KEYS = ("zpid", "source")
QUEUE_OPTIONAL_PAYLOAD_TRIM_ORDER = (
    "listing_description",
    "description",
    "listingText",
    "address",
    "street",
    "agentName",
    "url",
    "propertyUrl",
    "detailUrl",
)
FINAL_QUEUE_STATUSES = {
    "completed_short_sale",
    "completed_non_short_sale",
    "skipped_seen",
}
TERMINAL_QUEUE_RESULTS = {
    "skipped_already_contacted_agent",
    "skipped_agent_team",
    "skipped_stale_listing",
    "skipped_undisclosed_address",
}


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_iso_timestamp(value: str) -> Optional[datetime]:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _queue_row_values(record: Dict[str, Any]) -> List[str]:
    values: List[str] = []
    for col in QUEUE_HEADERS:
        raw_value = str(record.get(col, "") or "")
        if len(raw_value) > QUEUE_CELL_SAFE_LIMIT:
            trimmed = raw_value[:QUEUE_CELL_SAFE_LIMIT]
            logger.warning(
                "queue: truncated column=%s chars=%d->%d",
                col,
                len(raw_value),
                len(trimmed),
            )
            values.append(trimmed)
            continue
        values.append(raw_value)
    return values


def _extract_special_listing_conditions(row: Dict[str, Any]) -> str:
    if not isinstance(row, dict):
        return ""
    paths = (
        ("specialListingConditions",),
        ("specialConditions",),
        ("resoFacts", "specialListingConditions"),
        ("resoFacts", "specialConditions"),
        ("property", "specialListingConditions"),
        ("property", "resoFacts", "specialListingConditions"),
        ("listing", "specialListingConditions"),
    )
    parts: List[str] = []
    for path in paths:
        current: Any = row
        for key in path:
            if not isinstance(current, dict):
                current = None
                break
            current = current.get(key)
        if isinstance(current, str) and current.strip():
            parts.append(current.strip())
        elif isinstance(current, list):
            parts.extend(str(item).strip() for item in current if str(item).strip())
    seen: set[str] = set()
    deduped: List[str] = []
    for part in parts:
        normalized = re.sub(r"\s+", " ", part).strip()
        if normalized and normalized not in seen:
            seen.add(normalized)
            deduped.append(normalized)
    return "; ".join(deduped)


def _compact_queue_resume_payload(row: Dict[str, Any], source: str) -> Dict[str, Any]:
    zpid = str(row.get("zpid", "")).strip()
    listing_text = extract_description(row)
    special_conditions = _extract_special_listing_conditions(row)
    if special_conditions and special_conditions not in listing_text:
        listing_text = " ".join(part for part in (listing_text, special_conditions) if part)
    sms_address = _format_sms_listing_address(row)
    full_address = _format_listing_address(row)
    attribution = row.get("attributionInfo") if isinstance(row.get("attributionInfo"), dict) else {}
    broker_name = (
        row.get("brokerName")
        or row.get("brokerageName")
        or row.get("brokerage")
        or attribution.get("brokerName")
        or attribution.get("brokerageName")
        or ""
    )
    payload: Dict[str, Any] = {
        "zpid": zpid,
        "address": sms_address or full_address,
        "street": sms_address,
        "city": str(row.get("city") or "").strip(),
        "state": str(row.get("state") or "").strip(),
        "zip": str(row.get("zip") or row.get("zipcode") or "").strip(),
        "source": source,
        "search_source": str(row.get("search_source") or source or "").strip(),
        "agentName": str(row.get("agentName") or "").strip(),
        "brokerName": str(broker_name or "").strip(),
        "brokerageName": str(broker_name or "").strip(),
        "url": str(_extra_state_listing_url(row) or "").strip(),
        "detailUrl": str(row.get("detailUrl") or row.get("detailURL") or "").strip(),
        "propertyUrl": str(row.get("propertyUrl") or row.get("propertyURL") or "").strip(),
        "homeStatus": str(
            row.get("homeStatus")
            or row.get("status")
            or row.get("listingStatus")
            or row.get("home_status")
            or ""
        ).strip(),
        "detailScrapedAt": str(
            row.get("detailScrapedAt")
            or row.get("detail_scraped_at")
            or row.get("detailScrapeAt")
            or ""
        ).strip(),
        "specialListingConditions": special_conditions,
        "listing_description": listing_text,
        "description": listing_text,
        "listingText": listing_text,
    }
    if full_address and full_address != payload.get("address"):
        payload["full_address"] = full_address
    return {k: v for k, v in payload.items() if str(v or "").strip()}


def _serialize_queue_payload(payload: Dict[str, Any], zpid: str) -> str:
    compact_payload = dict(payload)
    serialized = json.dumps(compact_payload, separators=(",", ":"), ensure_ascii=False)
    if len(serialized) <= QUEUE_CELL_SAFE_LIMIT:
        return serialized

    for key in QUEUE_OPTIONAL_PAYLOAD_TRIM_ORDER:
        value = compact_payload.get(key)
        if not isinstance(value, str) or not value:
            continue
        new_value = value[:4_000].rstrip()
        if len(new_value) < len(value):
            compact_payload[key] = new_value
            logger.warning(
                "queue: truncated payload field zpid=%s field=%s chars=%d->%d",
                zpid,
                key,
                len(value),
                len(new_value),
            )
        serialized = json.dumps(compact_payload, separators=(",", ":"), ensure_ascii=False)
        if len(serialized) <= QUEUE_CELL_SAFE_LIMIT:
            return serialized

    for key in QUEUE_OPTIONAL_PAYLOAD_TRIM_ORDER:
        if key in compact_payload:
            compact_payload.pop(key, None)
            logger.warning("queue: dropped payload field zpid=%s field=%s to fit sheet cell", zpid, key)
            serialized = json.dumps(compact_payload, separators=(",", ":"), ensure_ascii=False)
            if len(serialized) <= QUEUE_CELL_SAFE_LIMIT:
                return serialized

    minimal_payload: Dict[str, str] = {}
    for key in QUEUE_REQUIRED_PAYLOAD_KEYS:
        value = str(compact_payload.get(key, "") or "").strip()
        if not value:
            continue
        if len(value) > 1_000:
            clipped = value[:1_000]
            logger.warning(
                "queue: truncated required payload field zpid=%s field=%s chars=%d->%d",
                zpid,
                key,
                len(value),
                len(clipped),
            )
            value = clipped
        minimal_payload[key] = value
    return json.dumps(minimal_payload, separators=(",", ":"), ensure_ascii=False)


def _pending_queue_state_skip_zpids() -> set[str]:
    skip_statuses = FINAL_QUEUE_STATUSES | {"pending", "in_progress"}
    with _queue_lock:
        records = _load_pending_queue_records(PENDING_QUEUE_WS)
    skip: set[str] = set()
    for rec in records:
        zpid = str(rec.get("zpid", "")).strip()
        status = str(rec.get("status", "")).strip()
        if zpid and status in skip_statuses:
            skip.add(zpid)
    return skip


def get_pending_queue_ws():
    try:
        workbook = _retry_gspread_call("open workbook", lambda: gclient.open_by_key(GSHEET_ID))
        ws = _retry_gspread_call(
            "open pending queue worksheet",
            lambda: workbook.worksheet(PENDING_QUEUE_TAB),
        )
    except gspread.WorksheetNotFound:
        workbook = _retry_gspread_call("open workbook", lambda: gclient.open_by_key(GSHEET_ID))
        ws = _retry_gspread_call(
            "create pending queue worksheet",
            lambda: workbook.add_worksheet(title=PENDING_QUEUE_TAB, rows="2000", cols=str(len(QUEUE_HEADERS))),
        )
        _retry_gspread_call("seed pending queue header", lambda: ws.append_row(QUEUE_HEADERS))
        return ws

    values = _retry_gspread_call("read pending queue header", lambda: ws.row_values(1))
    if values[: len(QUEUE_HEADERS)] != QUEUE_HEADERS:
        end_col = chr(ord("A") + len(QUEUE_HEADERS) - 1)
        _retry_gspread_call(
            "repair pending queue header",
            lambda: ws.update(f"A1:{end_col}1", [QUEUE_HEADERS], value_input_option="RAW"),
        )
    return ws


PENDING_QUEUE_WS = get_pending_queue_ws()


def _load_pending_queue_records(ws) -> List[Dict[str, Any]]:
    values = _retry_gspread_call("read pending queue rows", ws.get_all_values)
    if not values:
        return []
    header = list(values[0])
    if len(header) < len(QUEUE_HEADERS):
        header += QUEUE_HEADERS[len(header):]
    records: List[Dict[str, Any]] = []
    for row_num, row_vals in enumerate(values[1:], start=2):
        row = list(row_vals)
        if len(row) < len(header):
            row += [""] * (len(header) - len(row))
        record = {header[idx]: row[idx] for idx in range(len(header))}
        record["_row_num"] = row_num
        records.append(record)
    return records


def _update_pending_queue_row(ws, row_num: int, record: Dict[str, Any]) -> None:
    end_col = chr(ord("A") + len(QUEUE_HEADERS) - 1)
    values = _queue_row_values(record)
    _retry_gspread_call(
        "update pending queue row",
        lambda: ws.update(f"A{row_num}:{end_col}{row_num}", [values], value_input_option="RAW"),
    )


def _enqueue_pending_rows(rows: List[Dict[str, Any]], source: str) -> int:
    now_iso = _utcnow_iso()
    enqueued = 0
    enqueued_zpids: List[str] = []
    with _queue_lock:
        ws = PENDING_QUEUE_WS
        records = _load_pending_queue_records(ws)
        by_zpid: Dict[str, Dict[str, Any]] = {}
        for rec in records:
            zpid = str(rec.get("zpid", "")).strip()
            if zpid:
                current_status = str(rec.get("status", "")).strip()
                existing = by_zpid.get(zpid)
                if not existing:
                    by_zpid[zpid] = rec
                    continue
                existing_status = str(existing.get("status", "")).strip()
                if existing_status in FINAL_QUEUE_STATUSES:
                    continue
                if current_status in FINAL_QUEUE_STATUSES:
                    by_zpid[zpid] = rec
                    continue
                if current_status in {"pending", "in_progress"} and existing_status == "failed":
                    by_zpid[zpid] = rec

        for row in rows:
            zpid = str(row.get("zpid", "")).strip()
            if not zpid:
                continue
            existing = by_zpid.get(zpid)
            if existing:
                status = str(existing.get("status", "")).strip()
                if status in FINAL_QUEUE_STATUSES:
                    logger.info("queue: skipped duplicate completed zpid=%s", zpid)
                    continue
                if status == "failed" and existing.get("_row_num"):
                    address = _format_sms_listing_address(row) or _format_listing_address(row)
                    source_value = str(row.get("source") or source or "").strip()
                    existing.update(
                        {
                            "address": address,
                            "source": source_value,
                            "created_at": now_iso,
                            "status": "pending",
                            "claimed_at": "",
                            "processed_at": "",
                            "result": "",
                            "error": "",
                            "listing_json": _serialize_queue_payload(
                                _compact_queue_resume_payload(row, source_value),
                                zpid,
                            ),
                        }
                    )
                    _update_pending_queue_row(ws, int(existing["_row_num"]), existing)
                    enqueued += 1
                    enqueued_zpids.append(zpid)
                    by_zpid[zpid] = existing
                    continue
                continue

            address = _format_sms_listing_address(row) or _format_listing_address(row)
            source_value = str(row.get("source") or source or "").strip()
            payload_dict = _compact_queue_resume_payload(row, source_value)
            payload = _serialize_queue_payload(payload_dict, zpid)
            append_vals = _queue_row_values(
                {
                    "zpid": zpid,
                    "address": address,
                    "source": source_value,
                    "created_at": now_iso,
                    "status": "pending",
                    "claimed_at": "",
                    "processed_at": "",
                    "result": "",
                    "error": "",
                    "listing_json": payload,
                }
            )
            _retry_gspread_call("append pending queue row", lambda vals=append_vals: ws.append_row(vals))
            enqueued += 1
            enqueued_zpids.append(zpid)
            by_zpid[zpid] = {"zpid": zpid, "status": "pending"}

    logger.info("queue: enqueued count=%d zpids=%s source=%s", enqueued, enqueued_zpids, source)
    return enqueued


def _requeue_stale_in_progress_items(*, startup: bool = False) -> int:
    stale_cutoff = datetime.now(timezone.utc) - timedelta(minutes=max(PENDING_QUEUE_STALE_MINUTES, 1))
    requeued = 0
    with _queue_lock:
        ws = PENDING_QUEUE_WS
        records = _load_pending_queue_records(ws)
        for rec in records:
            status = str(rec.get("status", "")).strip()
            if status != "in_progress":
                continue
            claimed_at = _parse_iso_timestamp(str(rec.get("claimed_at", "")))
            if not claimed_at or claimed_at > stale_cutoff:
                continue
            rec["status"] = "pending"
            rec["claimed_at"] = ""
            rec["processed_at"] = ""
            rec["result"] = ""
            rec["error"] = ""
            _update_pending_queue_row(ws, int(rec["_row_num"]), rec)
            requeued += 1
            logger.info("queue: requeued stale item zpid=%s", str(rec.get("zpid", "")).strip())
    if startup and requeued == 0:
        logger.info("queue: startup recovery found no stale in_progress items")
    return requeued


def _claim_next_pending_item() -> Optional[Dict[str, Any]]:
    with _queue_lock:
        ws = PENDING_QUEUE_WS
        records = _load_pending_queue_records(ws)
        for rec in records:
            status = str(rec.get("status", "")).strip()
            if status != "pending":
                continue
            zpid = str(rec.get("zpid", "")).strip()
            if not zpid:
                continue
            rec["status"] = "in_progress"
            rec["claimed_at"] = _utcnow_iso()
            rec["error"] = ""
            _update_pending_queue_row(ws, int(rec["_row_num"]), rec)
            logger.info("queue: claimed zpid=%s", zpid)
            return rec
    return None


def _complete_queue_item(item: Dict[str, Any], status: str, result: str = "", error: str = "") -> None:
    row_num = int(item["_row_num"])
    item["status"] = status
    item["processed_at"] = _utcnow_iso()
    item["result"] = result
    item["error"] = error
    with _queue_lock:
        _update_pending_queue_row(PENDING_QUEUE_WS, row_num, item)
    logger.info("queue: completed zpid=%s result=%s", str(item.get("zpid", "")).strip(), status)


def _process_claimed_queue_item(item: Dict[str, Any]) -> None:
    zpid = str(item.get("zpid", "")).strip()
    listing_payload = str(item.get("listing_json", "")).strip()
    try:
        if listing_payload:
            row = json.loads(listing_payload)
            if not isinstance(row, dict):
                row = {}
        else:
            row = {}
    except ValueError:
        row = {}

    if not row:
        row = {
            "zpid": zpid,
            "address": item.get("address", ""),
            "source": item.get("source", ""),
        }
    if zpid and "zpid" not in row:
        row["zpid"] = zpid

    try:
        outcomes = process_rows([row], skip_dedupe=True, return_outcomes=True) or {}
        if _row_has_detail_marker(row) and zpid:
            EXPORTED_ZPIDS.add(zpid)
        result_status = outcomes.get(zpid)
        if result_status not in {"completed_short_sale", "completed_non_short_sale"}:
            if result_status in TERMINAL_QUEUE_RESULTS:
                _complete_queue_item(item, "completed_non_short_sale", result=result_status)
                return
            logger.warning(
                "queue: non-terminal outcome zpid=%s result=%s; marking failed",
                zpid,
                result_status or "missing_outcome",
            )
            _complete_queue_item(item, "failed", error=result_status or "missing_outcome")
            return
        _complete_queue_item(item, result_status)
    except Exception as exc:
        logger.exception("queue: failed zpid=%s", zpid)
        _complete_queue_item(item, "failed", error=str(exc))


def _process_pending_queue(*, startup: bool = False) -> int:
    processed = 0
    if not _queue_worker_lock.acquire(blocking=False):
        return 0
    try:
        if startup:
            _requeue_stale_in_progress_items(startup=True)
        while True:
            claimed = _claim_next_pending_item()
            if not claimed:
                break
            _process_claimed_queue_item(claimed)
            processed += 1
    finally:
        _queue_worker_lock.release()
    return processed

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


def send_sms(phone: str, message: str, sms_type: str = "initial") -> None:
    """Send an SMS using the configured provider."""
    if not SMS_API_KEY:
        logger.warning("SMS gateway API key missing; skipping SMS send to %s", phone)
        return
    digits = _digits_only(phone)
    if not digits:
        logger.warning("Bad phone number '%s' – skipping SMS send", phone)
        return
    try:
        SMS_SENDER.send(digits, message, sms_type=sms_type)
        if sms_type == "followup":
            logger.info("TASKER_SEND_FOLLOWUP to %s", digits)
        else:
            logger.info("TASKER_SEND_INITIAL to %s", digits)
    except Exception as exc:
        logger.exception("SMS send failed: %s", exc)


def _row_value(row: List[str], index: int) -> str:
    if index < len(row):
        return str(row[index] or "").strip()
    return ""


def _get_leads_ws():
    workbook = _retry_gspread_call("open workbook", lambda: gclient.open_by_key(GSHEET_ID))
    return _retry_gspread_call(
        "open leads worksheet",
        lambda: workbook.worksheet(LEADS_SHEET_TAB),
    )


def _auth_internal_request(request: Request) -> None:
    if not CODEX_AUTOMATION_TOKEN:
        raise HTTPException(status_code=503, detail="automation_token_not_configured")
    auth = request.headers.get("authorization", "")
    header_token = request.headers.get("x-codex-automation-token", "")
    expected_auth = f"Bearer {CODEX_AUTOMATION_TOKEN}"
    if auth == expected_auth or header_token == CODEX_AUTOMATION_TOKEN:
        return
    raise HTTPException(status_code=403, detail="bad token")


def _format_initial_message(payload: Dict[str, Any], row: List[str]) -> str:
    first = str(payload.get("first") or _row_value(row, 0)).strip()
    address = (
        _street_only_address(payload.get("street"))
        or _street_only_address(payload.get("address"))
        or _street_only_address(_row_value(row, 4))
    )
    return SMS_TEMPLATE.format(first=first, address=address).strip()


def _mark_initial_sms_sent(
    ws,
    *,
    row_idx: int,
    msg_id: str,
    mark_codex_verified: bool,
) -> str:
    ts = datetime.now(tz=TZ).isoformat()
    updates = [
        {"range": f"H{row_idx}", "values": [["x"]]},
        {"range": f"W{row_idx}", "values": [[ts]]},
        {"range": f"L{row_idx}", "values": [[msg_id]]},
    ]
    if mark_codex_verified:
        updates.append({"range": f"AQ{row_idx}", "values": [["x"]]})
    _retry_gspread_call(
        "mark internal initial SMS",
        lambda: ws.batch_update(updates, value_input_option="RAW"),
    )
    return ts


def _send_initial_sms_from_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    try:
        row_idx = int(payload.get("row"))
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="invalid_row")
    if row_idx < 2:
        raise HTTPException(status_code=400, detail="invalid_row")

    phone = fmt_phone(str(payload.get("phone") or ""))
    if not phone:
        raise HTTPException(status_code=400, detail="invalid_phone")
    digits = _digits_only(phone)

    ws = _get_leads_ws()
    row = _retry_gspread_call("read leads row", lambda: ws.row_values(row_idx))
    if not row:
        raise HTTPException(status_code=404, detail="row_not_found")

    force_resend = bool(payload.get("force_resend") or payload.get("force"))
    initial_marked = _row_value(row, 7).lower() == "x"
    initial_ts = _row_value(row, 22).strip()
    if _row_value(row, 42).lower() == "x" and initial_marked and initial_ts and not force_resend:
        return {"status": "already_verified", "row": row_idx}

    current_phone = fmt_phone(_row_value(row, 2))
    if not current_phone:
        raise HTTPException(status_code=409, detail="row_phone_missing")
    if _digits_only(current_phone) != digits:
        raise HTTPException(status_code=409, detail="row_phone_mismatch")

    if initial_marked and not force_resend:
        raise HTTPException(status_code=409, detail="initial_sms_already_marked")

    message = _format_initial_message(payload, row)
    if not message:
        raise HTTPException(status_code=400, detail="empty_message")

    final_result = None
    for attempt in range(1, INITIAL_SMS_RETRY_ATTEMPTS + 1):
        final_result = SMS_SENDER.send_with_diagnostics(
            digits,
            message,
            sms_type="initial",
            row_idx=row_idx,
            attempt=attempt,
        )
        if final_result.success:
            break
        if attempt < INITIAL_SMS_RETRY_ATTEMPTS:
            time.sleep(2)

    if not final_result or not final_result.success:
        logger.error(
            "INTERNAL_INITIAL_SMS_FAILED row=%s phone=%s http_status=%s response_body=%s exception_type=%s exception_message=%s",
            row_idx,
            digits,
            getattr(final_result, "status_code", None),
            getattr(final_result, "response_text", "") or "<empty>",
            getattr(final_result, "exception_type", ""),
            getattr(final_result, "exception_message", ""),
        )
        raise HTTPException(
            status_code=502,
            detail={
                "status": "gateway_failed",
                "gateway_status": getattr(final_result, "status_code", None),
                "gateway_response": getattr(final_result, "response_text", ""),
            },
        )

    msg_id = str(payload.get("message_id") or "")
    mark_codex_verified = payload.get("mark_codex_verified", True) is not False
    sent_at = _mark_initial_sms_sent(
        ws,
        row_idx=row_idx,
        msg_id=msg_id,
        mark_codex_verified=mark_codex_verified,
    )
    logger.info(
        "INTERNAL_INITIAL_SMS_SENT row=%s phone=%s http_status=%s response_body=%s codex_verified=%s",
        row_idx,
        digits,
        final_result.status_code,
        final_result.response_text or "<empty>",
        mark_codex_verified,
    )
    return {
        "status": "sent",
        "row": row_idx,
        "phone": digits,
        "sent_at": sent_at,
        "gateway_status": final_result.status_code,
        "gateway_response": final_result.response_text,
        "codex_verified": mark_codex_verified,
    }


def _send_followup_sms_from_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    phone = fmt_phone(str(payload.get("phone") or ""))
    if not phone:
        raise HTTPException(status_code=400, detail="invalid_phone")
    digits = _digits_only(phone)

    message = str(payload.get("message") or "").strip()
    if not message:
        raise HTTPException(status_code=400, detail="empty_message")
    if len(message) > 1600:
        raise HTTPException(status_code=400, detail="message_too_long")

    row_idx = None
    if payload.get("row") not in (None, ""):
        try:
            row_idx = int(payload.get("row"))
        except (TypeError, ValueError):
            raise HTTPException(status_code=400, detail="invalid_row")
        if row_idx < 2:
            raise HTTPException(status_code=400, detail="invalid_row")

    final_result = None
    for attempt in range(1, INITIAL_SMS_RETRY_ATTEMPTS + 1):
        final_result = SMS_SENDER.send_with_diagnostics(
            digits,
            message,
            sms_type="followup",
            row_idx=row_idx,
            attempt=attempt,
        )
        if final_result.success:
            break
        if attempt < INITIAL_SMS_RETRY_ATTEMPTS:
            time.sleep(2)

    if not final_result or not final_result.success:
        logger.error(
            "INTERNAL_FOLLOWUP_SMS_FAILED row=%s phone=%s http_status=%s response_body=%s exception_type=%s exception_message=%s",
            row_idx,
            digits,
            getattr(final_result, "status_code", None),
            getattr(final_result, "response_text", "") or "<empty>",
            getattr(final_result, "exception_type", ""),
            getattr(final_result, "exception_message", ""),
        )
        raise HTTPException(
            status_code=502,
            detail={
                "status": "gateway_failed",
                "gateway_status": getattr(final_result, "status_code", None),
                "gateway_response": getattr(final_result, "response_text", ""),
            },
        )

    sent_at = datetime.now(tz=TZ).isoformat()
    logger.info(
        "INTERNAL_FOLLOWUP_SMS_SENT row=%s phone=%s http_status=%s response_body=%s",
        row_idx,
        digits,
        final_result.status_code,
        final_result.response_text or "<empty>",
    )
    return {
        "status": "sent",
        "row": row_idx,
        "phone": digits,
        "sent_at": sent_at,
        "gateway_status": final_result.status_code,
        "gateway_response": final_result.response_text,
    }


@app.post("/internal/send-initial-sms")
async def internal_send_initial_sms(request: Request):
    """Send an initial SMS from the production bot using Render-side credentials."""
    _auth_internal_request(request)
    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="invalid_json")
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="invalid_payload")

    return await asyncio.to_thread(_send_initial_sms_from_payload, payload)


@app.post("/internal/send-followup-sms")
async def internal_send_followup_sms(request: Request):
    """Send a custom follow-up SMS from the production bot using Render-side credentials."""
    _auth_internal_request(request)
    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="invalid_json")
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="invalid_payload")

    return await asyncio.to_thread(_send_followup_sms_from_payload, payload)


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
            or payload.get("upstreamDatasetId")
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
        upstream_dataset_id = payload.get("upstreamDatasetId") if isinstance(payload, dict) else None
        if upstream_dataset_id is None:
            upstream_dataset_id = dataset_id
        total_scraped = payload.get("totalScraped") if isinstance(payload, dict) else None
        total_new = payload.get("totalNew") if isinstance(payload, dict) else None
        ordered_zpids = [
            str(row.get("zpid", "")).strip()
            for row in payload_listings
            if isinstance(row, dict)
        ]
        ordered_addresses = [
            _format_listing_address(row)
            for row in payload_listings
            if isinstance(row, dict)
        ]
        logger.info(
            "ORIGINAL_PAYLOAD_META upstreamDatasetId=%s totalScraped=%s totalNew=%s listingCount=%s",
            upstream_dataset_id,
            total_scraped,
            total_new,
            len(payload_listings),
        )
        logger.info("ORIGINAL_PAYLOAD_ZPIDS %s", ordered_zpids)
        logger.info("ORIGINAL_PAYLOAD_ADDRESSES %s", ordered_addresses)
        global _previous_original_upstream_dataset_id, _previous_original_ordered_zpids
        with _original_payload_signature_lock:
            prev_upstream_dataset_id = _previous_original_upstream_dataset_id
            prev_zpids = list(_previous_original_ordered_zpids)
            identical_to_previous = (
                upstream_dataset_id == prev_upstream_dataset_id and ordered_zpids == prev_zpids
            )
            _previous_original_upstream_dataset_id = upstream_dataset_id
            _previous_original_ordered_zpids = list(ordered_zpids)
        logger.info(
            "ORIGINAL_PAYLOAD_REPEAT_CHECK identical=%s previousUpstreamDatasetId=%s previousZpids=%s",
            identical_to_previous,
            prev_upstream_dataset_id,
            prev_zpids,
        )

        selection = _select_payload_listings(payload)
        logger.info(
            "apify-hook: selection received=%s hard_skipped=%s already_seen=%s invalid=%s selected=%s",
            selection["received"],
            selection["hard_skipped"],
            selection["already_seen"],
            selection.get("invalid_rows", 0),
            selection["selected"],
        )
        if selection.get("selected_zpids"):
            logger.info("apify-hook: selected zpids=%s", selection["selected_zpids"])
        if selection.get("selected_addresses"):
            logger.info("apify-hook: selected addresses=%s", selection["selected_addresses"])
        rows = selection["rows"]
        row_source = "payload.listings"
        _enqueue_pending_rows(rows, source=row_source)
        _start_extra_state_rows(payload)
        _start_apify_coverage_backstop(datetime.now(tz=SCHEDULER_TZ))

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

    if payload_listings is None:
        logger.debug("state-search: skipped extra fetch for non-primary webhook event source=%s", row_source)

    if rows:
        if row_source == "none":
            row_source = "payload"
        logger.info("apify-hook: row source=%s count=%d", row_source, len(rows))

    if not rows:
        logger.info("apify-hook: 0 listings received; no Apify retries scheduled")
        return {"status": "no rows"}

    return _process_incoming_rows(
        rows,
        source=row_source,
        skip_seen_dedupe=payload_listings is not None,
        skip_seen_append=False,
        skip_enqueue=payload_listings is not None,
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
