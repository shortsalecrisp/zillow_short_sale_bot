"""Owner-approved exception for one listing, not a general active-file bypass.

Yoni approved this exact exception, release, initial SMS, and normal CRM path on
2026-11-06 in the dedicated verifier chat. The existing Buyer/Agent contact stays
on its real Crisp file. Any identity or match change fails closed.
"""

import re
from datetime import datetime, timezone
from zoneinfo import ZoneInfo


EXCEPTION_ID = "ashleigh-pacific-49354863-owner-20261106"


def _text(value):
    return re.sub(r"[^a-z0-9]+", " ", str(value or "").casefold()).strip()


def approved_active_file_outreach_exception(row, lookup):
    """Return the sole approved exception ID; never hide the actual file match."""
    if (lookup.get("configured") is not True or lookup.get("active_file") is not True
            or lookup.get("unavailable")):
        return ""
    value = lambda index: str(row[index] or "").strip() if index < len(row) else ""
    phone = re.sub(r"\D", "", value(2))
    if phone.startswith("1") and len(phone) == 11:
        phone = phone[1:]
    if not (
        _text(value(0)) == "ashleigh"
        and _text(value(1)) == "camberg"
        and phone == "3605139034"
        and value(3).casefold() == "acamberg@pcshomegroup.com"
        and _text(value(4)) == "9336 pacific highway se"
        and _text(value(5)) == "olympia"
        and value(6).casefold() == "wa"
        and value(27) == "49354863"
    ):
        return ""
    match = lookup.get("match")
    if not isinstance(match, dict):
        return ""
    # Only the independently verified email match on her *other* transaction.
    # A property match, a different file, a phone-only match, or malformed
    # reasons cannot inherit this authorization.
    if (match.get("fileId") != "cmp1zhrd6026uky04so10navp"
            or match.get("reference") != "V1-2000690"
            or _text(match.get("propertyAddress")) != "14471 100th ave se"
            or match.get("matchReasons") != ["agent_email"]):
        return ""
    return EXCEPTION_ID


def approved_exception_initial_window_open(now=None):
    """An exception never authorizes an early, late, or weekend initial SMS."""
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        return False
    local = now.astimezone(ZoneInfo("America/Los_Angeles"))
    return local.weekday() < 5 and 8 <= local.hour < 20
