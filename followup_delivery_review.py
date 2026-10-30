"""Read-only delivery inventory. This module cannot enqueue or generate media."""
import hashlib
import json
import re
from collections import Counter
from datetime import datetime, timezone


def digits(value):
    return re.sub(r"\D", "", str(value or ""))[-10:]


def metadata(entry, prefix):
    try:
        raw = str(entry[6])
        value = json.loads(raw[len(prefix):]) if raw.startswith(prefix) else None
        return value if isinstance(value, dict) else {}
    except (IndexError, TypeError, ValueError):
        return {}


def _receipt(entry, stable, parse_time):
    def stamp(index):
        return parse_time(entry[index]).astimezone(timezone.utc).isoformat(
            timespec="milliseconds").replace("+00:00", "Z")
    return {"schema": "initial-sms-receipt-v1", "request_id": entry[2], "message_id": entry[3],
            "phone": digits(entry[4]), "stable_id": stable, "status": "sent",
            "created_at": stamp(0), "send_started_at": stamp(13), "sent_at": stamp(14)}


def _prior_phone_initial(entry, stable, anchor, parse_time, id_counts):
    meta = metadata(entry, "__initial_outreach__:")
    match = re.fullmatch(r"initial-(\d+)-[0-9a-f]{16}", str(entry[3]))
    created, started, sent = (parse_time(entry[i]) for i in (0, 13, 14))
    return (entry[1] == "sent" and bool(str(entry[5]).strip()) and bool(match)
            and str(meta.get("stable_id")) == stable and str(meta.get("crm_row")) == match[1]
            and str(entry[17]) == match[1]
            and entry[2] in {"scheduler-" + entry[3], "render-" + entry[3]}
            and all(c[str(entry[i])] == 1 for c, i in zip(id_counts, (2, 3)))
            and created is not None and started is not None and sent is not None
            and created <= started <= anchor <= sent)


def _initial_readback(row, receipt, same, parse_time, id_counts):
    if str(row[7]).lower() != "x" or not str(receipt[5]).strip():
        return False
    if row[11] == receipt[5]:
        return True
    # A real later reply may legitimately replace L before the verifier runs.
    # Require that exact later handset receipt in this row's assistant history.
    try:
        history = json.loads(row[17] or "[]")
    except (TypeError, ValueError):
        return False
    if not isinstance(history, list) or not str(row[11]).strip():
        return False
    for entry in same:
        sent = parse_time(entry[14])
        if (entry[1] == "sent" and entry[5] == row[11] and sent
                and sent >= parse_time(receipt[14])
                and all(c[str(entry[i])] == 1 for c, i in zip(id_counts, (2, 3)))
                and any(isinstance(h, dict) and h.get("role") == "assistant"
                        and h.get("text") == row[11] and h.get("receipt_id") in entry[2:4]
                        for h in history)):
            return True
    return False


def review(rows, entries, *, parse_time, elapsed, now, due_hours, grace_minutes,
           contact_hold, conversation_hold, within_hours):
    """Survey all bounded rows, independent of AQ and scheduler lookback.

    Results are candidates, never authority to send. The recovery route and
    locked Apps eligibility/receipt guards remain mandatory and authoritative.
    """
    entries = [list(e) + [""] * max(0, 19 - len(e)) for e in entries]
    by_phone, initials_by_stable = {}, {}
    for entry in entries:
        if len(entry) >= 15:
            by_phone.setdefault(digits(entry[4]), []).append(entry)
            initial_meta = metadata(entry, "__initial_outreach__:")
            if initial_meta.get("stable_id"):
                initials_by_stable.setdefault(str(initial_meta["stable_id"]), []).append(entry)
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
            item["current_initial_receipt"] = _receipt(receipt, stable, parse_time)
            item["initial_contact_readback_verified"] = _initial_readback(
                row, receipt, same, parse_time, id_counts)
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
                if verified and str(row[7]).lower() == "x":
                    item["initial_contact_readback_verified"] = True
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
            former = [e for e in initials_by_stable.get(stable, [])
                      if len(phone) == 10 and digits(e[4]) != phone
                      and _prior_phone_initial(e, stable, initial, parse_time, id_counts)]
            if len(former) == 1:
                # This is initial-contact work, never authority for an immediate
                # reminder or for retrying an existing current-phone attempt.
                item["reason"] = ("current_phone_attempt_reconcile_only" if same
                                  else "corrected_phone_initial_contact_required")
                item["remediation"] = {
                    "kind": "corrected_phone_initial_contact", "owner": "lead_verifier",
                    "action": "reconcile_current_phone_attempt" if same else "review_corrected_phone_initial",
                    "send_authorized": False, "former_phone": digits(former[0][4]),
                    "former_initial_receipt": _receipt(former[0], stable, parse_time),
                    "current_phone_attempts": [{"request_id": e[2], "message_id": e[3],
                                                "status": e[1]} for e in same],
                }
        counts[item["status"]] += 1
        # Successful historical episodes need no open-work ledger item.
        if item["status"] != "sent":
            items.append(item)
        else:
            resolved.append(item)
    return {"schema": "followup-delivery-review-v1", "observed_at": now.isoformat(),
            "sent": False, "counts": dict(counts), "items": items, "verified_sent": resolved,
            "remediation_queue": [item for item in items if item.get("remediation")],
            "scope": "Bounded non-atomic CRM/outbox survey; fresh reply/opt-out/manual/identity and competing-attempt guards required before enqueue, claim and send_start."}


def _verified_initial_readback(item):
    proof = item.get("current_initial_receipt")
    if item.get("initial_contact_readback_verified") is not True or not isinstance(proof, dict):
        return False
    if (proof.get("schema") != "initial-sms-receipt-v1" or proof.get("status") != "sent"
            or proof.get("phone") != item.get("phone") or proof.get("stable_id") != item.get("stable_id")
            or proof.get("request_id") != item.get("initial_request_id")
            or proof.get("message_id") != item.get("initial_message_id")):
        return False
    try:
        created, started, owning, sent = [datetime.fromisoformat(str(v).replace("Z", "+00:00"))
            for v in [proof["created_at"], proof["send_started_at"], item["initial_sent_at"], proof["sent_at"]]]
        return all(v.tzinfo for v in [created, started, owning, sent]) and created <= started <= owning <= sent
    except (ValueError, TypeError, KeyError):
        return False


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
        history = list(old.get("history", []))
        if old and (old.get("status"), old.get("reason")) != (item["status"], item["reason"]):
            history.append({k: old.get(k) for k in
                            ("status", "reason", "row", "phone", "stable_id", "initial_sent_at",
                             "request_id", "message_id", "last_observed_at")})
        ledger["episodes"][key] = {**item, "history": history,
            "first_observed_at":old.get("first_observed_at",survey["observed_at"]),
            "last_observed_at":survey["observed_at"], "retry_after":None if item["status"]=="sent" else retry_after}
    # A corrected phone starts a new receipt-bound initial episode. Close only
    # its former missing-initial issue, not a reminder or any other safety hold.
    initial_gaps = {"current_phone_initial_receipt_missing_or_ambiguous",
                    "corrected_phone_initial_contact_required", "current_phone_attempt_reconcile_only"}
    for item in survey["items"] + survey["verified_sent"]:
        proof = item.get("current_initial_receipt")
        if not _verified_initial_readback(item):
            continue
        for old in ledger["episodes"].values():
            if (old.get("reason") in initial_gaps and old.get("phone") == item["phone"]
                    and old.get("stable_id") == item["stable_id"]):
                old.setdefault("history", []).append({k: old.get(k) for k in
                    ("status", "reason", "row", "phone", "stable_id", "initial_sent_at", "last_observed_at")})
                old.update(status="resolved_initial_contact", reason="current_phone_initial_receipt_now_verified",
                    resolution={"observed_at": survey["observed_at"], "initial_receipt": proof,
                                "followup_sent": item["status"] == "sent"},
                    last_observed_at=survey["observed_at"], retry_after=None)
    ledger["remediation_queue"] = [dict(item, ledger_key=key)
        for key, item in ledger["episodes"].items()
        if item.get("remediation") and item["status"] not in {"sent", "resolved_initial_contact"}]
    ledger["last_complete_survey_at"] = survey["observed_at"]
    return ledger
