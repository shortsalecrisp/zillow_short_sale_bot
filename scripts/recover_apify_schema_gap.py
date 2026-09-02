#!/usr/bin/env python3
"""Recover Apify rows that were queued before the curated schema was normalized."""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List

import requests


WORKBOOK_ID = "12UzsoQCo4W0WB_lNl3BjKpQ_wXNhEH7xegkFRVu2M70"
PENDING_QUEUE_TAB = "PendingQueue"
DETAIL_TASK_FALLBACK = "VI5izq8RGAL14zM75"
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _configure_env() -> None:
    if not os.getenv("GSHEET_ID") and os.getenv("GOOGLE_SHEET_ID"):
        os.environ["GSHEET_ID"] = os.environ["GOOGLE_SHEET_ID"]
    os.environ.setdefault("GSHEET_ID", WORKBOOK_ID)
    os.environ.setdefault("GOOGLE_API_KEY", "missing")
    os.environ.setdefault("GOOGLE_CX", "missing")
    os.environ.setdefault("SMS_GATEWAY_API_KEY", "missing")
    path = os.getenv("GCP_SERVICE_ACCOUNT_JSON_PATH") or os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if not os.getenv("GCP_SERVICE_ACCOUNT_JSON") and path:
        with open(path, "r", encoding="utf-8") as fh:
            os.environ["GCP_SERVICE_ACCOUNT_JSON"] = fh.read()


def _load_url_map(raw: str) -> Dict[str, Dict[str, Any]]:
    if not raw:
        return {}
    loaded = json.loads(raw)
    if not isinstance(loaded, dict):
        raise ValueError("url map must be a JSON object keyed by zpid")
    result: Dict[str, Dict[str, Any]] = {}
    for zpid, entry in loaded.items():
        zpid_key = str(zpid).strip()
        if not zpid_key:
            continue
        if isinstance(entry, str):
            result[zpid_key] = {"propertyUrl": entry}
        elif isinstance(entry, dict):
            result[zpid_key] = dict(entry)
        else:
            raise ValueError(f"url map entry for {zpid_key} must be a string or object")
    return result


def _chunked(values: List[Dict[str, Any]], size: int) -> Iterable[List[Dict[str, Any]]]:
    for idx in range(0, len(values), size):
        yield values[idx : idx + size]


def _detail_url(zpid: str, patch: Dict[str, Any]) -> str:
    return str(
        patch.get("propertyUrl")
        or patch.get("detailUrl")
        or f"https://www.zillow.com/homedetails/{zpid}_zpid/"
    ).strip()


def _fetch_details(
    *,
    token: str,
    task_id: str,
    candidates: List[Dict[str, Any]],
    url_map: Dict[str, Dict[str, Any]],
    batch_size: int,
) -> Dict[str, Dict[str, Any]]:
    details: Dict[str, Dict[str, Any]] = {}
    if not token or not task_id or not candidates:
        return details
    endpoint = f"https://api.apify.com/v2/actor-tasks/{task_id}/run-sync-get-dataset-items"
    for batch in _chunked(candidates, max(batch_size, 1)):
        start_urls = []
        for candidate in batch:
            zpid = candidate["zpid"]
            patch = url_map.get(zpid, {})
            start_urls.append({"url": _detail_url(zpid, patch)})
        resp = requests.post(
            endpoint,
            params={
                "token": token,
                "limit": len(start_urls),
                "clean": "true",
                "format": "json",
            },
            json={
                "startUrls": start_urls,
                "propertyStatus": "FOR_SALE",
                "extractBuildingUnits": "disabled",
                "maxConcurrency": min(max(len(start_urls), 1), 10),
            },
            timeout=300,
        )
        if resp.status_code not in {200, 201}:
            print(
                json.dumps(
                    {
                        "event": "detail_fetch_failed",
                        "status": resp.status_code,
                        "body": resp.text[:500],
                    }
                )
            )
            continue
        for item in resp.json():
            if not isinstance(item, dict):
                continue
            zpid = str(item.get("zpid") or item.get("propertyId") or "").strip()
            if zpid:
                details[zpid] = item
    return details


def _queue_value(row: List[str], headers: Dict[str, int], key: str) -> str:
    idx = headers[key]
    return row[idx] if idx < len(row) else ""


def _set_queue_result(ws: Any, row_num: int, status: str, result: str, error: str = "") -> None:
    processed_at = datetime.now(timezone.utc).isoformat()
    ws.update(
        f"E{row_num}:I{row_num}",
        [[status, "", processed_at, result, error]],
        value_input_option="RAW",
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--since-row", type=int, default=5997)
    parser.add_argument("--limit", type=int, default=25)
    parser.add_argument("--batch-size", type=int, default=10)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--url-map-json", default=os.getenv("APIFY_SCHEMA_RECOVERY_URL_MAP", ""))
    parser.add_argument(
        "--detail-task-id",
        default=os.getenv("APIFY_STATE_DETAIL_TASK_ID") or os.getenv("APIFY_DETAIL_TASK_ID") or DETAIL_TASK_FALLBACK,
    )
    args = parser.parse_args()

    _configure_env()
    logging.disable(logging.DEBUG)

    import gspread
    from google.oauth2.service_account import Credentials

    import bot_min

    logging.disable(logging.DEBUG)

    service_account = json.loads(os.environ["GCP_SERVICE_ACCOUNT_JSON"])
    creds = Credentials.from_service_account_info(
        service_account,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    workbook = gspread.authorize(creds).open_by_key(os.getenv("GSHEET_ID", WORKBOOK_ID))
    queue_ws = workbook.worksheet(os.getenv("PENDING_QUEUE_TAB", PENDING_QUEUE_TAB))
    values = queue_ws.get_all_values()
    if not values:
        print(json.dumps({"event": "empty_queue"}))
        return 0
    headers = {name: idx for idx, name in enumerate(values[0])}
    url_map = _load_url_map(args.url_map_json)

    candidates: List[Dict[str, Any]] = []
    skipped: List[Dict[str, Any]] = []
    for row_num, row in enumerate(values[1:], start=2):
        if row_num < args.since_row:
            continue
        zpid = _queue_value(row, headers, "zpid").strip()
        if not zpid:
            continue
        status = _queue_value(row, headers, "status").strip()
        result = _queue_value(row, headers, "result").strip()
        error = _queue_value(row, headers, "error").strip()
        if status == "completed_short_sale" or result == "skipped_already_contacted_agent":
            continue
        try:
            payload = json.loads(_queue_value(row, headers, "listing_json") or "{}")
        except json.JSONDecodeError:
            skipped.append({"row": row_num, "zpid": zpid, "reason": "invalid_listing_json"})
            continue
        listing_text = bot_min._listing_text_from_payload(payload)
        if not bot_min.is_short_sale(listing_text or ""):
            continue
        exclusion = bot_min._short_sale_exclusion_reason(
            bot_min._short_sale_text_from_payload(listing_text)
        )
        if exclusion:
            skipped.append({"row": row_num, "zpid": zpid, "reason": f"excluded_{exclusion}"})
            continue
        missing_metadata = not payload.get("street") or not payload.get("agentName")
        if result not in {"skipped_undisclosed_address", ""} and error != "failed_missing_agent" and not missing_metadata:
            continue
        candidates.append(
            {
                "row": row_num,
                "zpid": zpid,
                "payload": payload,
                "queue_status": status,
                "queue_result": result,
                "queue_error": error,
            }
        )
        if len(candidates) >= args.limit:
            break

    token = os.getenv("APIFY_API_TOKEN") or os.getenv("APIFY_TOKEN") or ""
    details = _fetch_details(
        token=token,
        task_id=args.detail_task_id,
        candidates=candidates,
        url_map=url_map,
        batch_size=args.batch_size,
    )

    recoverable: List[Dict[str, Any]] = []
    unresolved: List[Dict[str, Any]] = []
    for candidate in candidates:
        zpid = candidate["zpid"]
        merged: Dict[str, Any] = {}
        merged.update(candidate["payload"])
        merged.update(url_map.get(zpid, {}))
        merged.update(details.get(zpid, {}))
        merged["zpid"] = zpid
        merged.setdefault("search_source", candidate["payload"].get("search_source") or "apify_schema_gap_recovery")
        if candidate["payload"].get("listing_description"):
            merged.setdefault("listing_description", candidate["payload"]["listing_description"])
            merged.setdefault("description", candidate["payload"]["listing_description"])
            merged.setdefault("listingText", candidate["payload"]["listing_description"])
        bot_min._normalize_listing_payload_aliases(merged)
        street = str(merged.get("street") or "").strip()
        agent = str(merged.get("agentName") or "").strip()
        if not street:
            unresolved.append({"row": candidate["row"], "zpid": zpid, "reason": "missing_address"})
            continue
        if not agent:
            unresolved.append({"row": candidate["row"], "zpid": zpid, "reason": "missing_agent"})
            continue
        recoverable.append(merged)

    summary: Dict[str, Any] = {
        "event": "apify_schema_gap_recovery",
        "apply": args.apply,
        "candidate_count": len(candidates),
        "detail_count": len(details),
        "recoverable_count": len(recoverable),
        "unresolved": unresolved,
        "skipped": skipped,
        "recoverable": [
            {
                "zpid": row.get("zpid"),
                "agentName": row.get("agentName"),
                "street": row.get("street"),
                "city": row.get("city"),
                "state": row.get("state"),
            }
            for row in recoverable
        ],
    }

    if args.apply and recoverable:
        outcomes = bot_min.process_rows(recoverable, skip_dedupe=True, return_outcomes=True) or {}
        summary["outcomes"] = outcomes
        for candidate in candidates:
            zpid = candidate["zpid"]
            if zpid in outcomes:
                outcome = outcomes[zpid]
                status = "completed_short_sale" if outcome == "completed_short_sale" else "completed_non_short_sale"
                _set_queue_result(
                    queue_ws,
                    int(candidate["row"]),
                    status,
                    f"recovered_schema_gap:{outcome}",
                )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
