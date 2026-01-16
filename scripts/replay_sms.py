#!/usr/bin/env python3
"""Replay initial/follow-up SMS messages for recent rows."""

from __future__ import annotations

import argparse
import logging
from datetime import datetime, timedelta
from typing import Iterable, List, Optional, Tuple

from bot_min import (
    COL_FIRST,
    COL_FU_TS,
    COL_INIT_TS,
    COL_MANUAL_NOTE,
    COL_PHONE,
    COL_REPLY_FLAG,
    COL_STREET,
    GSHEET_ID,
    GSHEET_TAB,
    MIN_COLS,
    SMS_ENABLE,
    SMS_FU_TEMPLATE,
    SMS_SENDER,
    SMS_TEMPLATE,
    SMS_TEST_MODE,
    SMS_TEST_NUMBER,
    SCHEDULER_TZ,
    _digits_only,
    sheets_service,
)

LOG = logging.getLogger("replay_sms")


def _parse_iso(ts_value: str) -> Optional[datetime]:
    if not ts_value:
        return None
    try:
        parsed = datetime.fromisoformat(ts_value)
    except Exception:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=SCHEDULER_TZ)
    return parsed.astimezone(SCHEDULER_TZ)


def _load_rows() -> List[List[str]]:
    resp = sheets_service.spreadsheets().values().get(
        spreadsheetId=GSHEET_ID,
        range=f"{GSHEET_TAB}!A:AB",
        majorDimension="ROWS",
        valueRenderOption="FORMATTED_VALUE",
    ).execute()
    rows = resp.get("values", [])
    if len(rows) <= 1:
        return []
    return rows[1:]


def _eligible_rows(
    rows: Iterable[List[str]],
    cutoff: datetime,
    include_followups: bool,
) -> List[Tuple[int, str, str, str, str]]:
    matches: List[Tuple[int, str, str, str, str]] = []
    for idx, row in enumerate(rows, start=2):
        row += [""] * (MIN_COLS - len(row))
        if row[COL_REPLY_FLAG].strip() or row[COL_MANUAL_NOTE].strip():
            continue
        phone = row[COL_PHONE].strip()
        if not phone:
            continue
        first = row[COL_FIRST].strip()
        address = row[COL_STREET].strip()
        init_ts = _parse_iso(row[COL_INIT_TS].strip())
        if init_ts and init_ts >= cutoff:
            matches.append((idx, phone, first, address, "initial"))
        if include_followups:
            fu_ts = _parse_iso(row[COL_FU_TS].strip())
            if fu_ts and fu_ts >= cutoff:
                matches.append((idx, phone, first, address, "follow_up"))
    return matches


def _resolve_destination(phone: str, *, force_live: bool) -> str:
    if SMS_TEST_MODE:
        if SMS_TEST_NUMBER:
            return SMS_TEST_NUMBER
        if not force_live:
            raise ValueError(
                "SMS_TEST_MODE is enabled without SMS_TEST_NUMBER; refusing live send.",
            )
    return phone


def _send_sms(phone: str, message: str, *, dry_run: bool) -> None:
    digits = _digits_only(phone)
    if not digits:
        LOG.warning("Skipping invalid phone=%s", phone)
        return
    if dry_run:
        LOG.info("DRY_RUN send to %s", digits)
        return
    SMS_SENDER.send(digits, message)


def _format_message(kind: str, first: str, address: str) -> str:
    if kind == "follow_up":
        return SMS_FU_TEMPLATE
    return SMS_TEMPLATE.format(first=first, address=address)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Replay initial/follow-up SMS sends for recent rows.",
    )
    parser.add_argument(
        "--hours",
        type=float,
        default=22.0,
        help="How many hours back to replay (default: 22).",
    )
    parser.add_argument(
        "--initial-only",
        action="store_true",
        help="Replay only initial messages (skip follow-ups).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Log what would be sent without sending SMS.",
    )
    parser.add_argument(
        "--force-live",
        action="store_true",
        help="Allow live sends even when SMS_TEST_MODE is enabled without a test number.",
    )
    args = parser.parse_args()

    if not SMS_ENABLE:
        LOG.error("SMS_ENABLE is false; refusing to send replay messages.")
        return 1

    now = datetime.now(tz=SCHEDULER_TZ)
    cutoff = now - timedelta(hours=args.hours)
    rows = _load_rows()
    candidates = _eligible_rows(
        rows,
        cutoff,
        include_followups=not args.initial_only,
    )
    if not candidates:
        LOG.info("No rows eligible for replay in the last %.2f hours.", args.hours)
        return 0

    LOG.info("Replaying %s messages (since %s).", len(candidates), cutoff.isoformat())
    if SMS_TEST_MODE and not SMS_TEST_NUMBER and not args.force_live:
        LOG.error(
            "SMS_TEST_MODE is enabled without SMS_TEST_NUMBER; "
            "use --force-live to override.",
        )
        return 1

    sent = 0
    for row_idx, phone, first, address, kind in candidates:
        destination = _resolve_destination(phone, force_live=args.force_live)
        message = _format_message(kind, first, address)
        try:
            _send_sms(destination, message, dry_run=args.dry_run)
            sent += 1
            LOG.info("Replayed %s SMS for row %s to %s", kind, row_idx, destination)
        except Exception as exc:
            LOG.error(
                "Failed to replay %s SMS for row %s to %s: %s",
                kind,
                row_idx,
                destination,
                exc,
            )

    LOG.info("Replay complete: %s/%s messages attempted.", sent, len(candidates))
    return 0


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s",
    )
    raise SystemExit(main())
