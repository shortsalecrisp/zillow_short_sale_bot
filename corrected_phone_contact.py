"""Explicit, receipt-bound reconciliation of an owner-reviewed phone correction."""
import hashlib
import json
import re
from datetime import timezone

MARKER = "CODEX_CORRECTED_PHONE_CONTACT_V1"
HOLD_NOTE = "Phone corrected; current-number verification required; old SIP recovery stopped"
FOLLOWUP_TEXT = "Hey, just wanted to follow up on my message from earlier. Let me know if I can help with anything\u2014happy to connect whenever works for you!"


def digits(value):
    return re.sub(r"\D", "", str(value))[-10:]


def state_hash(row):
    return hashlib.sha256(json.dumps(list(row), separators=(",", ":"), default=str).encode()).hexdigest()


def _metadata(entry, prefix):
    try:
        return json.loads(str(entry[6]).removeprefix(prefix)) if str(entry[6]).startswith(prefix) else None
    except (ValueError, TypeError, IndexError):
        return None


def sent_receipt(entries, request_id, row_number, stable_id, phone, prefix, parse):
    matches = [e for e in entries if len(e) >= 19 and e[2] == request_id]
    if len(matches) != 1:
        raise ValueError("Receipt request is missing or ambiguous")
    e = matches[0]
    metadata = _metadata(e, prefix)
    if (e[1] != "sent" or digits(e[4]) != phone or not metadata
            or str(metadata.get("stable_id")) != stable_id
            or int(metadata.get("crm_row", 0)) != row_number or int(e[17]) != row_number
            or not e[3] or sum(x[3] == e[3] for x in entries if len(x) > 3) != 1):
        raise ValueError("Receipt is not a unique sent current-owner receipt")
    created, started, sent = [parse(e[i]) for i in (0, 13, 14)]
    if None in (created, started, sent) or not created <= started <= sent:
        raise ValueError("Receipt terminal interval is invalid")
    iso = lambda value: value.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    return e, {"schema": "initial-sms-receipt-v1", "request_id": e[2], "message_id": e[3],
               "phone": phone, "stable_id": stable_id, "created_at": iso(created),
               "send_started_at": iso(started), "sent_at": iso(sent), "status": "sent"}


def prepare(row_number, row, fleet, entries, payload, parse):
    row = list(row) + [""] * max(0, 45 - len(row))
    phone, old = digits(row[2]), digits(payload.get("formerPhone", ""))
    stable = str(row[27]).strip()
    if (len(phone) != 10 or len(old) != 10 or old == phone or not stable
            or row[9] != HOLD_NOTE or row[30] != "phone_identity_review_required"
            or any(row[i] for i in [10, 12, 13, 15, 16, 17, 19, 20, 21, 28, 29, 31, 32, 33, 35, 36, 37, 38, 39, 40, 43, 44])):
        raise ValueError("Identity review or protected/attempt state changed")
    matches = [r for r in fleet if len(r) > 27 and (digits(r[2]) == phone or str(r[27]) == stable)]
    if len(matches) != 1 or str(matches[0][27]) != stable:
        raise ValueError("Phone/listing ownership is not unique")
    current, receipt = sent_receipt(entries, payload.get("initialRequestId"), row_number, stable,
                                    phone, "__initial_outreach__:", parse)
    previous, _ = sent_receipt(entries, payload.get("formerFollowupRequestId"), row_number, stable,
                               old, "__scheduled_followup__:", parse)
    owning = parse(row[22])
    old_followup = parse(row[23])
    if (not row[7] or not row[8] or owning is None or old_followup is None
            or not parse(current[13]) <= owning <= parse(current[14])
            or abs((old_followup - parse(previous[14])).total_seconds()) > 120
            or not old_followup < owning):
        raise ValueError("Owning initial/follow-up anchors do not match their phone receipts")
    if any(len(e) > 6 and digits(e[4]) == phone and e[2] != current[2] for e in entries):
        raise ValueError("Another current-phone request needs reconciliation")
    if MARKER in str(row[25]):
        raise ValueError("Existing corrected episode must be reconciled, not replaced")
    episode = {"rowNumber": row_number, "phone": phone, "formerPhone": old, "stableId": stable,
               "identity": [str(row[i]) for i in [0, 1, 2, 3, 4, 5, 6, 27]],
               "initialReceipt": receipt, "formerFollowupRequestId": previous[2],
               "priorI": row[8], "priorJ": row[9], "priorX": str(row[23]),
               "initialMessageSha256": hashlib.sha256(str(current[5]).encode()).hexdigest(),
               "followupMessageSha256": hashlib.sha256(FOLLOWUP_TEXT.encode()).hexdigest(),
               "textOnly": True}
    return episode


def active_episode(row_number, row, entries, parse):
    lines = [line for line in str(row[25]).splitlines() if line.startswith(MARKER + " ")]
    if not lines:
        return None
    if len(lines) != 1:
        raise ValueError("Ambiguous corrected-phone episode")
    episode = json.loads(lines[0][len(MARKER) + 1:])
    if (episode.get("rowNumber") != row_number or episode.get("textOnly") is not True
            or episode.get("identity") != [str(row[i]) for i in [0, 1, 2, 3, 4, 5, 6, 27]]):
        raise ValueError("Corrected-phone episode identity changed")
    # A later owning follow-up is never ignored. Its current-phone receipt must
    # be reconciled by the ordinary duplicate guard, not another send.
    if row[9] or str(row[23]) != episode["priorX"] or row[8] != episode["priorI"]:
        return None
    _, receipt = sent_receipt(entries, episode["initialReceipt"]["request_id"], row_number,
                              episode["stableId"], episode["phone"], "__initial_outreach__:", parse)
    if receipt != episode["initialReceipt"]:
        raise ValueError("Corrected-phone receipt changed")
    for e in entries:
        if len(e) > 6 and digits(e[4]) == episode["phone"] and e[2] != receipt["request_id"]:
            raise ValueError("Current-phone follow-up/request already exists; reconcile it")
    followup_identity_anchor(row, episode, parse)
    return episode


def followup_identity_anchor(row, episode, parse):
    """Bind queue identity to handset W, not the later receipt-processing time."""
    receipt = episode["initialReceipt"]
    owning, started, sent = [parse(value) for value in
                             (row[22], receipt["send_started_at"], receipt["sent_at"])]
    if (not row[7] or digits(row[2]) != receipt["phone"]
            or str(row[27]) != receipt["stable_id"]
            or None in (owning, started, sent) or not started <= owning <= sent):
        raise ValueError("Owning handset initial timestamp is outside its exact sent receipt")
    return owning.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
