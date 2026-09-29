"""Durable Google Sheets queue for Pilot verifier contract requests.

The scheduled verifier may be unable to resolve the Render hostname from its
execution sandbox. It can append the exact contract payload to this queue;
Render then executes the existing contract locally with production credentials.
The queue never bypasses ``pilot_verifier_contract.handle`` and never sends SMS.
"""
from __future__ import annotations

import datetime as dt
import json
import os
import threading
import time
import urllib.error
from typing import Any

from scripts import free_short_sale_source_pilot as pilot

VERSION = "pilot_verifier_queue_v2"
QUEUE_TAB = os.getenv("PILOT_VERIFIER_QUEUE_TAB", "Pilot Verifier Queue")
QUEUE_HEADERS = [
    "request_id",
    "submitted_at",
    "automation_id",
    "payload_json",
    "status",
    "claimed_at",
    "completed_at",
    "result_json",
    "error",
]
PENDING = "pending"
PROCESSING = "processing"
COMPLETED = "completed"
FAILED = "failed"
MAX_CELL_CHARS = 45_000
PROCESSING_STALE_SECONDS = max(
    120, int(os.getenv("PILOT_VERIFIER_QUEUE_STALE_SECONDS", "600"))
)
BOOKKEEPING_RETRY_DELAYS = (2, 5, 15, 30)

_PROCESS_LOCK = threading.Lock()
_STATE_LOCK = threading.Lock()
_STATE: dict[str, Any] = {
    "version": VERSION,
    "last_poll_at": "",
    "last_success_at": "",
    "last_error": "",
    "last_stats": {},
}


def _utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def _timestamp(value: dt.datetime) -> str:
    return value.astimezone(dt.timezone.utc).isoformat()


def _parse_timestamp(value: str) -> dt.datetime | None:
    try:
        parsed = dt.datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.astimezone(dt.timezone.utc) if parsed.tzinfo else None


def _is_transient_error(exc: BaseException) -> bool:
    if isinstance(exc, urllib.error.HTTPError):
        return exc.code in {429, 500, 502, 503, 504}
    if isinstance(exc, (TimeoutError, urllib.error.URLError)):
        return True
    text = f"{type(exc).__name__}: {exc}".lower()
    return any(marker in text for marker in (
        "too many requests", "timed out", "timeout", "temporarily unavailable",
        "http error 429", "http error 500", "http error 502",
        "http error 503", "http error 504",
    ))


def _bookkeeping_call(callback):
    """Retry queue bookkeeping; the contract itself remains state guarded."""
    for attempt in range(len(BOOKKEEPING_RETRY_DELAYS) + 1):
        try:
            return callback()
        except Exception as exc:
            if not _is_transient_error(exc) or attempt >= len(BOOKKEEPING_RETRY_DELAYS):
                raise
            delay = BOOKKEEPING_RETRY_DELAYS[attempt]
            if isinstance(exc, urllib.error.HTTPError) and exc.headers:
                try:
                    delay = max(delay, min(60, int(exc.headers.get("Retry-After", delay))))
                except (TypeError, ValueError):
                    pass
            time.sleep(delay)


def _quoted_tab() -> str:
    return "'" + QUEUE_TAB.replace("'", "''") + "'"


def _header_index(name: str) -> int:
    if name not in QUEUE_HEADERS:
        raise ValueError(f"unknown_queue_field:{name}")
    return QUEUE_HEADERS.index(name)


def _mapped_updates(row_number: int, fields: dict[str, Any]) -> list[dict[str, Any]]:
    if row_number < 2 or not fields:
        raise ValueError("invalid_queue_write")
    return [
        {
            "range": (
                f"{_quoted_tab()}!"
                f"{pilot.column_letter(_header_index(name) + 1)}{row_number}"
            ),
            "values": [[str(value)]],
        }
        for name, value in fields.items()
    ]


def _sheet_properties(token: str, spreadsheet_id: str) -> dict[str, Any] | None:
    result = pilot.sheets_request(
        token,
        "GET",
        f"{spreadsheet_id}?fields=sheets.properties",
    )
    matches = [
        sheet.get("properties", {})
        for sheet in result.get("sheets", [])
        if sheet.get("properties", {}).get("title") == QUEUE_TAB
    ]
    if len(matches) > 1:
        raise ValueError("ambiguous_pilot_verifier_queue_tab")
    return matches[0] if matches else None


def ensure_queue_tab(token: str, spreadsheet_id: str) -> None:
    """Create the hidden queue once and fail closed on later schema drift."""
    properties = _sheet_properties(token, spreadsheet_id)
    if properties is None:
        pilot.sheets_request(
            token,
            "POST",
            f"{spreadsheet_id}:batchUpdate",
            {
                "requests": [
                    {
                        "addSheet": {
                            "properties": {
                                "title": QUEUE_TAB,
                                "hidden": True,
                                "gridProperties": {
                                    "rowCount": 1000,
                                    "columnCount": len(QUEUE_HEADERS),
                                },
                            }
                        }
                    }
                ]
            },
        )
        pilot.update_values(
            token,
            spreadsheet_id,
            f"{_quoted_tab()}!A1:{pilot.column_letter(len(QUEUE_HEADERS))}1",
            [QUEUE_HEADERS],
        )
        return

    if not properties.get("hidden"):
        pilot.sheets_request(
            token,
            "POST",
            f"{spreadsheet_id}:batchUpdate",
            {
                "requests": [
                    {
                        "updateSheetProperties": {
                            "properties": {
                                "sheetId": properties["sheetId"],
                                "hidden": True,
                            },
                            "fields": "hidden",
                        }
                    }
                ]
            },
        )

    rows = pilot.get_values(
        token,
        spreadsheet_id,
        f"{_quoted_tab()}!A1:{pilot.column_letter(len(QUEUE_HEADERS))}1",
    )
    if not rows or rows[0] != QUEUE_HEADERS:
        raise ValueError("pilot_verifier_queue_schema_drift")


def _row_map(values: list[Any]) -> dict[str, str]:
    padded = [str(value) for value in values] + [""] * (len(QUEUE_HEADERS) - len(values))
    return dict(zip(QUEUE_HEADERS, padded[: len(QUEUE_HEADERS)]))


def read_queue(token: str, spreadsheet_id: str) -> list[tuple[int, dict[str, str]]]:
    rows = pilot.get_values(
        token,
        spreadsheet_id,
        f"{_quoted_tab()}!A:{pilot.column_letter(len(QUEUE_HEADERS))}",
    )
    if not rows or rows[0] != QUEUE_HEADERS:
        raise ValueError("pilot_verifier_queue_schema_drift")
    return [
        (number, _row_map(row))
        for number, row in enumerate(rows[1:], start=2)
        if any(str(value).strip() for value in row)
    ]


def _read_exact_row(token: str, spreadsheet_id: str, row_number: int) -> dict[str, str]:
    rows = pilot.get_values(
        token,
        spreadsheet_id,
        (
            f"{_quoted_tab()}!A{row_number}:"
            f"{pilot.column_letter(len(QUEUE_HEADERS))}{row_number}"
        ),
    )
    return _row_map(rows[0]) if rows else _row_map([])


def _claim_request(
    token: str,
    spreadsheet_id: str,
    row_number: int,
    request_id: str,
    claimed_at: str,
) -> bool:
    _bookkeeping_call(
        lambda: pilot.batch_update_values(
            token,
            spreadsheet_id,
            _mapped_updates(
                row_number,
                {"status": PROCESSING, "claimed_at": claimed_at, "error": ""},
            ),
        )
    )
    reread = _bookkeeping_call(
        lambda: _read_exact_row(token, spreadsheet_id, row_number)
    )
    return (
        reread.get("request_id") == request_id
        and reread.get("status") == PROCESSING
        and reread.get("claimed_at") == claimed_at
    )


def _finish_request(
    token: str,
    spreadsheet_id: str,
    row_number: int,
    *,
    status: str,
    completed_at: str,
    result: dict[str, Any] | None = None,
    error: str = "",
) -> None:
    if status not in {COMPLETED, FAILED}:
        raise ValueError("invalid_queue_terminal_status")
    result_json = json.dumps(result or {}, sort_keys=True, separators=(",", ":"))
    _bookkeeping_call(
        lambda: pilot.batch_update_values(
            token,
            spreadsheet_id,
            _mapped_updates(
                row_number,
                {
                    "status": status,
                    "completed_at": completed_at,
                    "result_json": result_json[:MAX_CELL_CHARS],
                    "error": str(error)[:MAX_CELL_CHARS],
                },
            ),
        )
    )
    reread = _bookkeeping_call(
        lambda: _read_exact_row(token, spreadsheet_id, row_number)
    )
    if reread.get("status") != status or reread.get("completed_at") != completed_at:
        raise RuntimeError("pilot_verifier_queue_terminal_readback_failed")


def _requeue_request(
    token: str,
    spreadsheet_id: str,
    row_number: int,
    *,
    error: str,
) -> None:
    _bookkeeping_call(
        lambda: pilot.batch_update_values(
            token,
            spreadsheet_id,
            _mapped_updates(
                row_number,
                {
                    "status": PENDING,
                    "claimed_at": "",
                    "completed_at": "",
                    "result_json": "",
                    "error": str(error)[:MAX_CELL_CHARS],
                },
            ),
        )
    )
    reread = _bookkeeping_call(
        lambda: _read_exact_row(token, spreadsheet_id, row_number)
    )
    if reread.get("status") != PENDING or reread.get("completed_at"):
        raise RuntimeError("pilot_verifier_queue_requeue_readback_failed")


def _validated_payload(row: dict[str, str]) -> dict[str, Any]:
    request_id = row.get("request_id", "").strip()
    if not request_id or len(request_id) > 100:
        raise ValueError("invalid_queue_request_id")
    try:
        payload = json.loads(row.get("payload_json", ""))
    except json.JSONDecodeError as exc:
        raise ValueError("invalid_queue_payload_json") from exc
    if not isinstance(payload, dict):
        raise ValueError("invalid_queue_payload")
    automation_id = row.get("automation_id", "").strip()
    if not automation_id or payload.get("automation_id") != automation_id:
        raise ValueError("queue_automation_mismatch")
    return payload


def _resolved_pilot_row(contract, rows, expected: dict[str, Any]):
    matches = [
        (number, row)
        for number, row in rows
        if contract.identity(row) == contract.identity(expected)
    ]
    return matches[0] if len(matches) == 1 and all(contract.identity(expected)) else None


def _request_effect_state(
    token: str,
    spreadsheet_id: str,
    payload: dict[str, Any],
) -> tuple[str, dict[str, Any] | None]:
    """Classify an interrupted request without repeating a possible mutation."""
    import pilot_verifier_contract as contract

    action = payload.get("action")
    if action in {"preview", "owner_write_preview", "receipt", "reconcile_pilot_duplicate", "relocate_legacy_owner"}:
        return "replay_safe", None
    if action not in {"update", "promote_owner"}:
        return "ambiguous", None

    _, rows, main_rows, _, _ = contract.snapshot(token, spreadsheet_id)
    expected = payload.get("expected", {})
    resolved = _resolved_pilot_row(contract, rows, expected)
    if not resolved:
        return "ambiguous", None
    number, row = resolved
    expected_matches = all(row.get(key, "") == str(value) for key, value in expected.items())

    if action == "update":
        fields = payload.get("fields", {})
        reason = pilot.normalize_space(payload.get("adjudication_reason", ""))
        note = pilot.normalize_key(row.get("promotion_notes", ""))
        reason_written = bool(
            reason
            and pilot.normalize_key(reason) in note
            and f"verifier reviewed by {pilot.normalize_key(payload.get('automation_id', ''))}" in note
        )
        applied = bool(fields) and all(
            row.get(key, "") == str(value) for key, value in fields.items()
        ) and reason_written
        if applied:
            return "applied", {
                "ok": True,
                "contract": contract.VERSION,
                "pilot_row": number,
                "owner_row": row.get("matched_main_row", ""),
                "readback": True,
                "sheet1_writes": 0,
                "sends": 0,
                "queue_recovered": True,
            }
        return ("replay_safe", None) if expected_matches else ("ambiguous", None)

    owner = contract.validate_owner_payload(payload.get("owner"))
    linked = pilot.reconcile_pilot_link(number, row, main_rows)
    if (
        row.get("status") == "qualified"
        and row.get("promotion_status") == "promoted"
        and row.get("import_ready") == "promoted"
        and linked.get("outcome") == "linked"
        and contract.owner_matches(linked.get("main_row", {}), row, owner)
    ):
        return "applied", {
            "ok": True,
            "contract": contract.VERSION,
            "pilot_row": number,
            "owner_row": linked.get("matched_main_row"),
            "readback": True,
            "sheet1_writes": 1,
            "sends": 0,
            "queue_recovered": True,
        }
    if expected_matches and linked.get("outcome") == "missing":
        return "replay_safe", None
    return "ambiguous", None


def _recover_interrupted_request(
    token: str,
    spreadsheet_id: str,
    row_number: int,
    row: dict[str, str],
    *,
    error: str,
) -> str:
    payload = _validated_payload(row)
    state, result = _bookkeeping_call(
        lambda: _request_effect_state(token, spreadsheet_id, payload)
    )
    if state == "applied":
        _finish_request(
            token,
            spreadsheet_id,
            row_number,
            status=COMPLETED,
            completed_at=_timestamp(_utc_now()),
            result=result,
        )
        return COMPLETED
    if state == "replay_safe":
        _requeue_request(
            token,
            spreadsheet_id,
            row_number,
            error=f"transient_retry: {error}",
        )
        return PENDING
    _finish_request(
        token,
        spreadsheet_id,
        row_number,
        status=FAILED,
        completed_at=_timestamp(_utc_now()),
        error=f"interrupted_state_ambiguous: {error}",
    )
    return FAILED


def _stale_processing_rows(
    rows: list[tuple[int, dict[str, str]]],
    observed_at: dt.datetime,
) -> list[tuple[int, dict[str, str]]]:
    cutoff = observed_at.astimezone(dt.timezone.utc) - dt.timedelta(
        seconds=PROCESSING_STALE_SECONDS
    )
    stale = []
    for number, row in rows:
        if row.get("status", "").strip().lower() != PROCESSING:
            continue
        claimed_at = _parse_timestamp(row.get("claimed_at", ""))
        if claimed_at is None or claimed_at <= cutoff:
            stale.append((number, row))
    return stale


def _record_state(*, now: dt.datetime, stats: dict[str, int], error: str = "") -> None:
    with _STATE_LOCK:
        _STATE["last_poll_at"] = _timestamp(now)
        _STATE["last_stats"] = dict(stats)
        _STATE["last_error"] = error[:1000]
        if not error:
            _STATE["last_success_at"] = _timestamp(now)


def status_snapshot() -> dict[str, Any]:
    with _STATE_LOCK:
        return dict(_STATE)


def process_pending(
    token: str,
    spreadsheet_id: str,
    *,
    limit: int = 25,
    now: dt.datetime | None = None,
) -> dict[str, int]:
    """Claim and execute pending requests in Render without any outreach."""
    observed_at = now or _utc_now()
    stats = {"pending": 0, "claimed": 0, "completed": 0, "failed": 0}
    if limit <= 0:
        _record_state(now=observed_at, stats=stats)
        return stats
    if not _PROCESS_LOCK.acquire(blocking=False):
        stats["busy"] = 1
        return stats
    try:
        ensure_queue_tab(token, spreadsheet_id)
        rows = read_queue(token, spreadsheet_id)
        for row_number, row in _stale_processing_rows(rows, observed_at):
            outcome = _recover_interrupted_request(
                token,
                spreadsheet_id,
                row_number,
                row,
                error="stale_processing_recovery",
            )
            stats["recovered"] = stats.get("recovered", 0) + int(outcome == COMPLETED)
            stats["requeued"] = stats.get("requeued", 0) + int(outcome == PENDING)
            stats["failed"] += int(outcome == FAILED)
        if any(key in stats for key in ("recovered", "requeued")):
            rows = read_queue(token, spreadsheet_id)
        pending = [(number, row) for number, row in rows if row.get("status", "").strip().lower() == PENDING]
        stats["pending"] = len(pending)
        from pilot_verifier_contract import handle

        for row_number, row in pending[:limit]:
            claimed_at = _timestamp(_utc_now())
            request_id = row.get("request_id", "")
            if not _claim_request(token, spreadsheet_id, row_number, request_id, claimed_at):
                stats["failed"] += 1
                continue
            stats["claimed"] += 1
            try:
                payload = _validated_payload(row)
                result = handle(token, spreadsheet_id, payload)
                _finish_request(
                    token,
                    spreadsheet_id,
                    row_number,
                    status=COMPLETED,
                    completed_at=_timestamp(_utc_now()),
                    result=result,
                )
                stats["completed"] += 1
            except Exception as exc:  # noqa: BLE001 - persist or safely retry
                error = f"{type(exc).__name__}: {exc}"
                if _is_transient_error(exc):
                    try:
                        outcome = _recover_interrupted_request(
                            token,
                            spreadsheet_id,
                            row_number,
                            row,
                            error=error,
                        )
                    except Exception as recovery_exc:  # leave claimed for stale recovery
                        if not _is_transient_error(recovery_exc):
                            raise
                        stats["deferred"] = stats.get("deferred", 0) + 1
                        continue
                    stats["completed"] += int(outcome == COMPLETED)
                    stats["requeued"] = stats.get("requeued", 0) + int(outcome == PENDING)
                    stats["failed"] += int(outcome == FAILED)
                    continue
                _finish_request(
                    token,
                    spreadsheet_id,
                    row_number,
                    status=FAILED,
                    completed_at=_timestamp(_utc_now()),
                    error=error,
                )
                stats["failed"] += 1
        _record_state(now=observed_at, stats=stats)
        return stats
    except Exception as exc:
        _record_state(now=observed_at, stats=stats, error=f"{type(exc).__name__}: {exc}")
        raise
    finally:
        _PROCESS_LOCK.release()
