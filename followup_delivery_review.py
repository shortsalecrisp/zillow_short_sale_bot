"""Read-only delivery inventory. This module cannot enqueue or generate media."""
import hashlib
import json
import re
from collections import Counter


def digits(value):
    return re.sub(r"\D", "", str(value or ""))[-10:]


def metadata(entry, prefix):
    try:
        raw = str(entry[6])
        value = json.loads(raw[len(prefix):]) if raw.startswith(prefix) else None
        return value if isinstance(value, dict) else {}
    except (IndexError, TypeError, ValueError):
        return {}


def review(rows, entries, *, parse_time, elapsed, now, due_hours, grace_minutes,
           contact_hold, conversation_hold, within_hours):
    """Survey all bounded rows, independent of AQ and scheduler lookback.

    Results are candidates, never authority to send. The recovery route and
    locked Apps eligibility/receipt guards remain mandatory and authoritative.
    """
    entries = [list(e) + [""] * max(0, 19 - len(e)) for e in entries]
    by_phone = {}
    for entry in entries:
        if len(entry) >= 15:
            by_phone.setdefault(digits(entry[4]), []).append(entry)
    id_counts = [Counter(str(e[i]) for e in entries if len(e) > i and e[i]) for i in (2, 3)]
    items, resolved, counts = [], [], Counter()
    for number, raw in enumerate(rows, 2):
        row = list(raw) + [""] * max(0, 45 - len(raw))
        if not row[7] and not row[22]:
            continue
        phone, stable = digits(row[2]), str(row[27]).strip()
        initial = parse_time(row[22])
        item = {"row": number, "phone": phone, "stable_id": stable,
                "initial_sent_at": initial.isoformat() if initial else None,
                "status": "blocked", "reason": "initial_timestamp_missing_or_invalid"}
        receipt = None
        same = by_phone.get(phone, [])
        initials = [(e, metadata(e, "__initial_outreach__:")) for e in same
                    if str(e[6]).startswith("__initial_outreach__:")]
        matching = []
        if initial and len(phone) == 10 and stable:
            for entry, meta in initials:
                started, sent = (parse_time(entry[i]) for i in (13, 14))
                if (entry[1] == "sent" and meta.get("stable_id") == stable
                        and started and sent and started <= initial <= sent):
                    matching.append(entry)
        if len(matching) == 1:
            receipt = matching[0]
            match = re.fullmatch(r"initial-(\d+)-[0-9a-f]{16}", str(receipt[3]))
            meta = metadata(receipt, "__initial_outreach__:")
            created, started, sent = (parse_time(receipt[i]) for i in (0, 13, 14))
            if (not match or str(meta.get("crm_row")) != match[1]
                    or str(receipt[17]) != match[1] or receipt[2] not in
                    {"scheduler-" + receipt[3], "render-" + receipt[3]}
                    or not created or not created <= started <= sent
                    or any(c[str(receipt[i])] != 1 for c, i in zip(id_counts, (2, 3)))):
                receipt = None
        if receipt:
            item["initial_request_id"], item["initial_message_id"] = receipt[2:4]
            item["episode_key"] = hashlib.sha256(json.dumps(
                [stable, phone, receipt[2], receipt[3]], separators=(",", ":")).encode()).hexdigest()
            followups = []
            for entry in same:
                meta = metadata(entry, "__scheduled_followup__:")
                if meta:
                    followups.append((entry, meta))
                elif str(entry[3]).startswith(("followup-", "followup-video-")):
                    followups.append((entry, {}))
            exact = [(e, m) for e, m in followups
                     if isinstance(m.get("initial_receipt"), dict)
                     and m["initial_receipt"].get("request_id") == receipt[2]
                     and m.get("initial_receipt", {}).get("message_id") == receipt[3]
                     and m.get("initial_receipt", {}).get("phone") == phone
                     and m.get("stable_id") == stable]
            sent = [(e, m) for e, m in exact if e[1] == "sent" and parse_time(e[14])]
            if len(sent) == 1 and len(followups) == 1:
                e = sent[0][0]
                stamp, started, finished = (parse_time(v) for v in (row[23], e[13], e[14]))
                verified = (str(row[8]).lower() == "x" and stamp and started and finished
                            and started <= stamp <= finished and row[11] == e[5])
                item.update(status="sent" if verified else "blocked",
                            reason="sent_receipt_verified" if verified
                            else "sent_receipt_sheet_readback_needed", request_id=e[2], message_id=e[3],
                            sent_at=parse_time(e[14]).isoformat())
            elif followups:
                item.update(status="pending" if len(exact) == 1 and len(followups) == 1
                            and exact[0][0][1] in {"queued", "claimed", "send_started", "receipt_pending"}
                            else "blocked", reason="existing_attempt_reconcile_only")
                if len(exact) == 1:
                    item.update(request_id=exact[0][0][2], message_id=exact[0][0][3],
                                outbox_status=exact[0][0][1])
            elif row[8] or row[23]:
                item["reason"] = "existing_marker_requires_receipt_review"
            elif not row[7]:
                item["reason"] = "initial_marker_missing"
            elif contact_hold(row):
                item["reason"] = "contact_identity_hold"
            elif (str(row[19]).strip().lower() not in {"", "false", "no", "0"}
                  or str(row[16]).strip().lower() not in {"", "false", "no", "0"}
                  or conversation_hold(row) or row[21] or row[43] or row[44]):
                item["reason"] = "reply_or_manual_control"
            elif str(row[10]).strip().upper() in {"R", "Y", "G", "O", "H"}:
                item["reason"] = "crm_status_hold"
            elif elapsed(initial, now) + grace_minutes / 60 < due_hours:
                item.update(status="deferred", reason="six_business_hours_not_due")
            elif not within_hours(row[6], now) or now.weekday() >= 5:
                item.update(status="deferred", reason="recipient_local_hours")
            else:
                item.update(status="due_unsent", reason="fresh_locked_eligibility_required")
        elif initial:
            item["reason"] = "current_phone_initial_receipt_missing_or_ambiguous"
        counts[item["status"]] += 1
        # Successful historical episodes need no open-work ledger item.
        if item["status"] != "sent":
            items.append(item)
        else:
            resolved.append(item)
    return {"schema": "followup-delivery-review-v1", "observed_at": now.isoformat(),
            "sent": False, "counts": dict(counts), "items": items, "verified_sent": resolved,
            "scope": "Bounded non-atomic CRM/outbox survey; fresh reply/opt-out/manual/identity and competing-attempt guards required before enqueue, claim and send_start."}


def merge_ledger(previous, survey, retry_after):
    """Retain unresolved history; close only with positive receipt readback."""
    if survey.get("schema") != "followup-delivery-review-v1" or survey.get("complete") is not True:
        raise ValueError("Incomplete survey cannot advance the delivery ledger")
    ledger = json.loads(json.dumps(previous or {"schema":"followup-delivery-ledger-v1", "episodes":{}}))
    if ledger.get("schema") != "followup-delivery-ledger-v1":
        raise ValueError("Unexpected delivery ledger schema")
    for item in survey["items"] + survey["verified_sent"]:
        key = item.get("episode_key") or "unresolved-" + hashlib.sha256(json.dumps(
            [item["stable_id"], item["phone"], item["initial_sent_at"]], separators=(",", ":")).encode()).hexdigest()
        old = ledger["episodes"].get(key, {})
        ledger["episodes"][key] = {**item, "first_observed_at":old.get("first_observed_at",survey["observed_at"]),
            "last_observed_at":survey["observed_at"], "retry_after":None if item["status"]=="sent" else retry_after}
    ledger["last_complete_survey_at"] = survey["observed_at"]
    return ledger
