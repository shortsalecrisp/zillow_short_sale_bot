"""Proof-only recovery of queue timestamps incorrectly used as initial-send times."""
import json
import re
from zoneinfo import ZoneInfo


def receipt_time_for_queue_marker(row_number, row, entries, parse_time):
    """Return the sole sent receipt only when W equals its enqueue/not-before time.

    Never reinterpret an arbitrary timestamp, corrected phone, competing initial,
    duplicate receipt ID, or existing follow-up as permission for another send.
    """
    if len(row) < 28 or not row[7] or row[8] or row[23]:
        return None
    phone = re.sub(r"\D", "", str(row[2]))[-10:]
    stable_id = str(row[27]).strip()
    marker = parse_time(row[22])
    if len(phone) != 10 or not stable_id or marker is None:
        return None
    initials = [e for e in entries if len(e) >= 18
                and re.sub(r"\D", "", str(e[4]))[-10:] == phone
                and str(e[6]).startswith("__initial_outreach__:")]
    if len(initials) != 1:
        return None
    entry = initials[0]
    if entry[1] != "sent":
        return None
    try:
        metadata = json.loads(entry[6].split(":", 1)[1])
        original_row = int(metadata["crm_row"])
        receipt_row = int(entry[17])
    except (ValueError, TypeError, KeyError):
        return None
    message_match = re.fullmatch(r"initial-(\d+)-[0-9a-f]{16}", str(entry[3]))
    if (not message_match or int(message_match[1]) != original_row
            or receipt_row != original_row or metadata.get("stable_id") != stable_id
            or entry[2] not in {"scheduler-" + entry[3], "render-" + entry[3]}):
        return None
    if sum(e[2] == entry[2] or e[3] == entry[3] for e in entries if len(e) > 3) != 1:
        return None
    created, not_before, started, sent = (parse_time(entry[i]) for i in (0, 8, 13, 14))
    if (None in (created, started, sent) or not created <= started <= sent
            or not marker < started or marker.date() != sent.date()
            or not any(t is not None and abs((marker - t).total_seconds()) < 1
                       for t in (created, not_before))):
        return None
    # Existing reminder attempts, including uncertain ones, must be reconciled first.
    if any(len(e) > 6 and re.sub(r"\D", "", str(e[4]))[-10:] == phone
           and str(e[6]).startswith("__scheduled_followup__:") for e in entries):
        return None
    return sent


def recipient_zone(state):
    """Use the same state mapping as the existing handset send-hour gate."""
    groups = {
        "America/Los_Angeles": "CA NV OR WA",
        "America/Denver": "CO ID MT NM UT WY",
        "America/Chicago": "AL AR IA IL KS LA MN MO MS ND NE OK SD TN TX WI",
        "America/New_York": "CT DC DE FL GA KY MA MD ME NC NH NJ NY OH PA RI SC VA VT WV",
        "America/Detroit": "MI", "America/Indiana/Indianapolis": "IN",
        "America/Anchorage": "AK", "America/Phoenix": "AZ", "Pacific/Honolulu": "HI",
    }
    return next((zone for zone, states in groups.items() if str(state).strip().upper() in states.split()), None)


def within_recipient_hours(state, now):
    zone = recipient_zone(state)
    return bool(zone and now.tzinfo and 8 <= now.astimezone(ZoneInfo(zone)).hour < 20)
