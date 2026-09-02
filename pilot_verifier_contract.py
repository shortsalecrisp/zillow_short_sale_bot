"""Pilot-only handoff writes and deterministic verifier receipts. Never sends outreach.

The authenticated routes use live Sheets reads, not caller-provided row numbers or
caller-computed green flags. Read-only preview is the release verification path.
"""
from __future__ import annotations

import datetime as dt
import re
import threading
import urllib.parse
from zoneinfo import ZoneInfo

from scripts import free_short_sale_source_pilot as pilot

VERSION = "pilot_verifier_contract_v1"
PILOT_TAB = "Lead Source Pilot"
MAIN_TAB = "Sheet1"
NY = ZoneInfo("America/New_York")
AUTOMATIONS = {
    "lead-verifier-8-am": 8, "lead-verifier-11-am": 11,
    "lead-verifier-2-pm": 14, "lead-verifier-5-pm": 17, "lead-verifier-8-pm": 20,
}
WRITE_FIELDS = {
    "status", "failure_reason", "promotion_status", "promotion_notes",
    "import_ready", "duplicate_key", "matched_main_row",
}
_WRITE_LOCK = threading.Lock()
OWNER_ARTIFACT_ROW_FLOOR = 9000
OWNER_WRITE_END_COLUMN = "AQ"
OWNER_REQUIRED_FIELDS = {
    "agent_name", "last_name", "phone", "email", "phone_confidence",
    "contact_verification_note", "email_confidence",
}


def header_index(headers, name):
    """Fail closed rather than writing a neighboring column after schema drift."""
    normalized = [pilot.normalized_header(str(h)) for h in headers]
    key = pilot.normalized_header(name)
    if normalized.count(key) != 1:
        raise ValueError(f"missing_or_ambiguous_header:{name}")
    return normalized.index(key)


def mapped_updates(tab, headers, row, fields):
    if row < 2 or not fields or set(fields) - WRITE_FIELDS:
        raise ValueError("invalid_pilot_write_scope")
    quoted = "'" + tab.replace("'", "''") + "'"
    return [
        {"range": f"{quoted}!{pilot.column_letter(header_index(headers, key) + 1)}{row}",
         "values": [[str(value)]]}
        for key, value in fields.items()
    ]


def read_table(token, spreadsheet_id, tab, fields):
    """Read headers first, then only required columns across the whole owner tab."""
    quoted = "'" + tab.replace("'", "''") + "'"
    header_rows = pilot.get_values(token, spreadsheet_id, f"{quoted}!1:1")
    if not header_rows:
        raise ValueError(f"missing_headers:{tab}")
    headers = header_rows[0]
    columns = sorted((header_index(headers, field), field) for field in fields)
    groups = []
    for index, field in columns:
        if groups and index == groups[-1][-1][0] + 1:
            groups[-1].append((index, field))
        else:
            groups.append([(index, field)])
    ranges = [f"{quoted}!{pilot.column_letter(g[0][0]+1)}2:{pilot.column_letter(g[-1][0]+1)}" for g in groups]
    query = urllib.parse.urlencode([("ranges", r) for r in ranges])
    result = pilot.sheets_request(token, "GET", f"{spreadsheet_id}/values:batchGet?{query}")
    values = result.get("valueRanges", [])
    if len(values) != len(groups):
        raise ValueError(f"incomplete_owner_read:{tab}")
    rows = {}
    for group, block in zip(groups, values):
        for number, row in enumerate(block.get("values", []), 2):
            target = rows.setdefault(number, {})
            for offset, (_, field) in enumerate(group):
                target[field] = str(row[offset]) if offset < len(row) else ""
    return headers, [(n, r) for n, r in sorted(rows.items()) if any(r.values())]


def snapshot(token, spreadsheet_id):
    ph, pr = read_table(token, spreadsheet_id, PILOT_TAB, pilot.PILOT_HEADERS)
    # Sheet1 has legacy duplicate/blank headers. Validate required columns only.
    _, mr = read_table(token, spreadsheet_id, MAIN_TAB,
                       ["agent_name", "last_name", "phone", "email", "listing_address", "city", "state", "created_at"])
    rh, rr = read_table(token, spreadsheet_id, pilot.RUN_RECEIPT_TAB, pilot.RUN_RECEIPT_HEADERS)
    return ph, pr, mr, rh, rr


def identity(row):
    return (row.get("synthetic_zpid", ""),
            pilot.street_state_key(row.get("listing_address", ""), row.get("state", "")))


def normalized_phone(value):
    digits = re.sub(r"\D", "", str(value or ""))
    return digits[-10:] if len(digits) >= 10 else ""


def validate_owner_payload(owner):
    if not isinstance(owner, dict) or set(owner) != OWNER_REQUIRED_FIELDS:
        raise ValueError("exact_owner_fields_required")
    cleaned = {key: pilot.normalize_space(str(value)) for key, value in owner.items()}
    if not pilot.looks_like_person_name(f"{cleaned['agent_name']} {cleaned['last_name']}"):
        raise ValueError("individual_listing_agent_required")
    if not normalized_phone(cleaned["phone"]):
        raise ValueError("attributable_phone_required")
    if not re.fullmatch(r"[^\s@]+@[^\s@]+\.[^\s@]+", cleaned["email"]):
        raise ValueError("agent_specific_email_required")
    weak_phone = {"office", "team", "main", "generic"}
    weak_email = {"team_or_listing", "routing", "generic", "low_pattern"}
    if not cleaned["phone_confidence"] or any(token in cleaned["phone_confidence"].lower() for token in weak_phone):
        raise ValueError("individual_phone_class_required")
    if not cleaned["email_confidence"] or any(token in cleaned["email_confidence"].lower() for token in weak_email):
        raise ValueError("agent_specific_email_class_required")
    if not cleaned["contact_verification_note"]:
        raise ValueError("contact_verification_note_required")
    return cleaned


def owner_row_number(main_rows):
    real_rows = [
        number for number, row in main_rows
        if number < OWNER_ARTIFACT_ROW_FLOOR and any(
            pilot.normalize_space(row.get(field, ""))
            for field in ("agent_name", "last_name", "phone", "email", "listing_address", "city", "state", "created_at")
        )
    ]
    target = (max(real_rows) + 1) if real_rows else 2
    if target >= OWNER_ARTIFACT_ROW_FLOOR:
        raise ValueError("no_safe_operational_owner_row")
    return target


def owner_header_positions(headers):
    positions = {field: header_index(headers, field) for field in (
        "agent_name", "last_name", "phone", "email", "listing_address",
        "city", "state", "contact_verification_note", "created_at",
    )}
    note = positions["contact_verification_note"]
    created = positions["created_at"]
    if created != note + 2:
        raise ValueError("owner_confidence_schema_drift")
    normalized = [pilot.normalized_header(str(value)) for value in headers]

    def confidence_position(name, fallback):
        key = pilot.normalized_header(name)
        if normalized.count(key) == 1:
            return normalized.index(key)
        if normalized.count(key) > 1 or fallback < 0 or fallback >= len(headers):
            raise ValueError("owner_confidence_schema_drift")
        if pilot.normalize_space(str(headers[fallback])):
            raise ValueError("owner_confidence_schema_drift")
        return fallback

    phone_confidence = confidence_position("phone_confidence", note - 1)
    email_confidence = confidence_position("email_confidence", note + 1)
    if phone_confidence < 0 or email_confidence >= len(headers):
        raise ValueError("owner_confidence_schema_drift")
    positions.update(phone_confidence=phone_confidence, email_confidence=email_confidence)
    return positions


def owner_write_updates(headers, row_number, pilot_row, owner, automation):
    positions = owner_header_positions(headers)
    values = {
        "agent_name": owner["agent_name"], "last_name": owner["last_name"],
        "phone": owner["phone"], "email": owner["email"],
        "listing_address": pilot_row["listing_address"], "city": pilot_row["city"],
        "state": pilot_row["state"], "phone_confidence": owner["phone_confidence"],
        "contact_verification_note": owner["contact_verification_note"] +
            f"; verifier_contract={VERSION}; verifier={automation}",
        "email_confidence": owner["email_confidence"],
        "created_at": pilot_row["synthetic_zpid"],
    }
    updates = []
    for fields in (("agent_name", "last_name", "phone", "email", "listing_address", "city", "state"),
                   ("phone_confidence", "contact_verification_note", "email_confidence", "created_at")):
        indexes = [positions[field] for field in fields]
        if indexes != list(range(indexes[0], indexes[0] + len(indexes))):
            raise ValueError("owner_write_schema_drift")
        start = pilot.column_letter(indexes[0] + 1)
        end = pilot.column_letter(indexes[-1] + 1)
        updates.append({"range": f"'{MAIN_TAB}'!{start}{row_number}:{end}{row_number}",
                        "values": [[values[field] for field in fields]]})
    return updates


def owner_row_is_empty(token, spreadsheet_id, row_number):
    values = pilot.get_values(token, spreadsheet_id,
        f"'{MAIN_TAB}'!A{row_number}:{OWNER_WRITE_END_COLUMN}{row_number}")
    return not values or not any(pilot.normalize_space(str(value)) for value in values[0])


def owner_matches(row, pilot_row, owner):
    return (
        pilot.stable_id_from_main_row(row) == pilot_row.get("synthetic_zpid")
        and pilot.street_state_key(row.get("listing_address", ""), row.get("state", ""))
            == pilot.street_state_key(pilot_row.get("listing_address", ""), pilot_row.get("state", ""))
        and pilot.normalize_key(row.get("city", "")) == pilot.normalize_key(pilot_row.get("city", ""))
        and pilot.normalize_key(f"{row.get('agent_name', '')} {row.get('last_name', '')}")
            == pilot.normalize_key(f"{owner['agent_name']} {owner['last_name']}")
        and normalized_phone(row.get("phone", "")) == normalized_phone(owner["phone"])
        and row.get("email", "").strip().lower() == owner["email"].lower()
    )


def preappend_checks(pilot_number, pilot_row, main_rows, owner):
    if (pilot_row.get("status") != "qualified" or pilot_row.get("promotion_status") != "verifier_held"
            or pilot_row.get("import_ready") != "verify" or pilot_row.get("matched_main_row")):
        raise ValueError("pilot_not_verifier_held")
    link = pilot.reconcile_pilot_link(pilot_number, pilot_row, main_rows)
    if link["outcome"] != "missing":
        raise ValueError(f"existing_owner_conflict:{link['outcome']}")
    phone = normalized_phone(owner["phone"])
    matches = [number for number, row in main_rows if normalized_phone(row.get("phone", "")) == phone]
    if matches:
        raise ValueError(f"existing_phone_owner:{matches[0]}")
    return owner_row_number(main_rows)


def sheet_id(token, spreadsheet_id, tab):
    result = pilot.sheets_request(token, "GET",
        f"{spreadsheet_id}?fields=sheets(properties(sheetId,title))")
    matches = [item.get("properties", {}).get("sheetId") for item in result.get("sheets", [])
               if item.get("properties", {}).get("title") == tab]
    if len(matches) != 1:
        raise ValueError(f"missing_or_ambiguous_sheet:{tab}")
    return matches[0]


def delete_owner_row(token, spreadsheet_id, row_number):
    pilot.sheets_request(token, "POST", f"{spreadsheet_id}:batchUpdate", {
        "requests": [{"deleteDimension": {"range": {
            "sheetId": sheet_id(token, spreadsheet_id, MAIN_TAB), "dimension": "ROWS",
            "startIndex": row_number - 1, "endIndex": row_number,
        }}}]
    })


def existing_listing_owner(row, main_rows):
    """Address-only duplicates also require city and exact agent/contact attribution."""
    def address_identity(record):
        address = re.sub(r"\bpl\b", "place", record.get("listing_address", ""), flags=re.IGNORECASE)
        parsed = pilot.canonical_address_identity(address, record.get("state", ""))
        # Use the full normalized street, not relaxed_street: Place and Road
        # at the same number must not collapse merely because their agent agrees.
        return parsed["street"], parsed["state"], parsed["unit"]

    matches = []
    for number, main in main_rows:
        if address_identity(row) != address_identity(main):
            continue
        if pilot.normalize_key(row.get("city", "")) != pilot.normalize_key(main.get("city", "")):
            continue
        pilot_name = pilot.normalize_key(row.get("first_name", "") + " " + row.get("last_name", ""))
        main_name = pilot.normalize_key(main.get("agent_name", "") + " " + main.get("last_name", ""))
        phone = re.sub(r"\D", "", row.get("phone", ""))[-10:]
        main_phone = re.sub(r"\D", "", main.get("phone", ""))[-10:]
        note = row.get("promotion_notes", "")
        note_phones = [re.sub(r"\D", "", p)[-10:] for p in re.findall(r"\+?\d[\d ()+.-]{8,}\d", note)]
        # Historical Pilot rows intentionally have blank A:D. Accept the explicit
        # verifier's named-agent/phone attribution, never a similar-name match.
        contact_matches = bool(pilot_name and pilot_name == main_name and len(phone) == 10 and phone == main_phone)
        named_owner = re.search(r"(?:\bhas|\bto|\bowner_agent=)\s*" + re.escape(main_name) + r"(?:\b|$)",
                                pilot.normalize_key(note).replace("owner agent ", "owner_agent="))
        note_matches = bool(main_name and named_owner and len(main_phone) == 10 and main_phone in note_phones)
        if contact_matches or note_matches:
            matches.append(number)
    return matches[0] if len(matches) == 1 else None


def source_completed(receipts, date, now):
    for _, row in receipts:
        if (row.get("schedule_slot_id") == f"source:{date}" and row.get("run_mode") == "scheduled_source"
                and row.get("status") == "completed" and row.get("pipeline_complete", "").lower() == "true"):
            try:
                timestamp = dt.datetime.fromisoformat(row["observed_at"].replace("Z", "+00:00"))
                if timestamp.tzinfo and timestamp <= now and timestamp.astimezone(NY).date().isoformat() == date:
                    return True
            except (KeyError, ValueError):
                pass
    return False


def build_receipt(*, pilot_rows, main_rows, receipts, date, now, automation_id,
                  run_receipt_id, global_sms_blockers, evidence_ok, source_cutoff=None):
    if automation_id not in AUTOMATIONS:
        raise ValueError("unknown_routine_verifier")
    if not isinstance(global_sms_blockers, list) or any(type(r) is not int or r < 2 for r in global_sms_blockers):
        raise ValueError("explicit_global_sms_blockers_required")
    cohort = [(n, r) for n, r in pilot_rows if pilot.first_seen_date(r.get("first_seen_at", "")) == dt.date.fromisoformat(date)]
    counts = dict(reviewed=0, promoted=0, verifier_held=0, rejected=0, duplicates=0, rolled_back=0)
    gaps = {}
    for number, row in cohort:
        status, state, ready = (row.get(k, "").lower() for k in ("status", "promotion_status", "import_ready"))
        note = row.get("promotion_notes", "").lower()
        adjudicated = bool(re.search(r"lead[ -]verifier|verifier_reviewed_by=", note))
        reason = "verifier_adjudication_missing"
        category = None
        if adjudicated:
            if status == "qualified" and state == ready == "promoted":
                link = pilot.reconcile_pilot_link(number, row, main_rows)
                contract = pilot.promoted_acceptance_contract(row, link)
                owner = link.get("main_row", {})
                if contract["accepted"] and evidence_ok(number, row) and all(owner.get(k) for k in ("agent_name", "last_name", "phone", "email")):
                    category = "promoted"
                else:
                    reason = "promoted_owner_contact_or_source_gap"
            elif status == "rejected" and ready == "skip" and row.get("failure_reason"):
                category = "rejected"
            elif status == "duplicate" and ready == "skip":
                if state == "skipped_duplicate_listing":
                    owner = existing_listing_owner(row, main_rows)
                    if owner and row.get("matched_main_row") == str(owner):
                        category = "duplicates"
                    else:
                        reason = "duplicate_listing_owner_gap"
                elif state == "duplicate_existing_agent" and not row.get("matched_main_row") and row.get("failure_reason") == "existing_agent_owner_contacted":
                    category = "duplicates"
                else:
                    reason = "duplicate_adjudication_gap"
            elif status == "qualified" and state == "verifier_held" and ready == "verify":
                if (row.get("failure_reason") or re.search(r"missing|conflict|no.agent.specific|contact.gap|evidence.gap", note)):
                    category = "verifier_held"
                    counts["rolled_back"] += int("rolled_back" in note)
                else:
                    reason = "hold_reason_missing"
        if category:
            counts[category] += 1
            counts["reviewed"] += 1
        else:
            gaps[number] = reason
    source_green = source_completed(receipts, date, source_cutoff or now)
    green = source_green and not gaps
    fields = [f"contract={VERSION}", f"pilot_pipeline_complete={str(green).lower()}",
              "global_sms_blockers=" + (",".join(map(str, sorted(set(global_sms_blockers)))) or "none"),
              f"source_complete={str(source_green).lower()}",
              "same_day_source_rows=" + (",".join(str(n) for n, _ in cohort) or "none")]
    fields += [f"{key}={value}" for key, value in counts.items()]
    fields += [f"unresolved={len(gaps)}", "unresolved_rows=" + (",".join(map(str, gaps)) or "none")]
    detail = "; ".join(fields)
    if len(detail) > 45_000:
        raise ValueError("receipt_detail_too_large")
    return {
        "schedule_slot_id": f"post_source_verifier:{date}:{automation_id}",
        "run_receipt_id": run_receipt_id, "run_date": date, "run_mode": "pilot_verifier",
        "status": "completed" if green else "completed_degraded", "observed_at": now.isoformat(),
        "pipeline_complete": str(green).lower(), "detail": detail,
    }, gaps


def handle(token, spreadsheet_id, payload, *, now=None):
    """Only Pilot bookkeeping or a new terminal receipt; preview performs no writes."""
    now = now or dt.datetime.now(dt.timezone.utc)
    action = payload.get("action")
    automation = payload.get("automation_id")
    if automation not in AUTOMATIONS or action not in {
        "owner_write_preview", "promote_owner", "update", "receipt", "preview"
    }:
        raise ValueError("invalid_action_or_automation")
    with _WRITE_LOCK:
        ph, rows, main_rows, rh, receipts = snapshot(token, spreadsheet_id)
        evidence_cache = {}

        def evidence_ok(number, row):
            parsed, failure = pilot.parse_pilot_payload(row)
            if failure or not pilot.has_durable_source_evidence(parsed):
                return False
            receipt = parsed["sourceEvidenceReceipt"]
            if receipt not in evidence_cache:
                try:
                    url = pilot.resolve_source_evidence_receipt(token, spreadsheet_id, receipt)
                    evidence_cache[receipt] = pilot.safe_source_reference(url) == row.get("source_url")
                except (ValueError, RuntimeError):
                    evidence_cache[receipt] = False
            return evidence_cache[receipt]

        if action == "owner_write_preview":
            target = owner_row_number(main_rows)
            owner_headers = pilot.get_values(token, spreadsheet_id, f"'{MAIN_TAB}'!1:1")
            if not owner_headers:
                raise ValueError("missing_headers:Sheet1")
            positions = owner_header_positions(owner_headers[0])
            return {
                "ok": True, "preview": True, "contract": VERSION,
                "owner_row": target, "artifact_row_floor": OWNER_ARTIFACT_ROW_FLOOR,
                "owner_row_empty": owner_row_is_empty(token, spreadsheet_id, target),
                "write_ranges": [
                    f"'{MAIN_TAB}'!A{target}:G{target}",
                    f"'{MAIN_TAB}'!{pilot.column_letter(positions['phone_confidence'] + 1)}{target}:"
                    f"{pilot.column_letter(positions['created_at'] + 1)}{target}",
                ],
                "writes": 0, "sends": 0,
            }

        if action == "promote_owner":
            expected = payload.get("expected", {})
            required = {"synthetic_zpid", "listing_address", "city", "state", "status", "promotion_status", "import_ready"}
            if not isinstance(expected, dict) or not required.issubset(expected):
                raise ValueError("expected_identity_and_state_required")
            matches = [(n, r) for n, r in rows if identity(r) == identity(expected)]
            if len(matches) != 1 or not all(identity(expected)):
                raise ValueError("pilot_identity_missing_or_ambiguous")
            number, row = matches[0]
            for key, value in expected.items():
                if row.get(key, "") != str(value):
                    raise ValueError(f"pilot_changed:{key}")
            if not evidence_ok(number, row):
                raise ValueError("durable_source_evidence_required")
            owner = validate_owner_payload(payload.get("owner"))
            reason = pilot.normalize_space(payload.get("adjudication_reason", ""))
            if not reason:
                raise ValueError("adjudication_reason_required")
            target = preappend_checks(number, row, main_rows, owner)

            # Reread both owner surfaces immediately before allocating the exact row.
            ph2, rows2, main2, _, _ = snapshot(token, spreadsheet_id)
            current = dict(rows2).get(number)
            if ph2 != ph or current != row:
                raise ValueError("pilot_changed_before_owner_write")
            if preappend_checks(number, current, main2, owner) != target:
                raise ValueError("active_owner_tail_changed")
            if not owner_row_is_empty(token, spreadsheet_id, target):
                raise ValueError("active_owner_row_not_empty")
            owner_headers = pilot.get_values(token, spreadsheet_id, f"'{MAIN_TAB}'!1:1")
            if not owner_headers:
                raise ValueError("missing_headers:Sheet1")
            pilot.batch_update_values(token, spreadsheet_id,
                owner_write_updates(owner_headers[0], target, current, owner, automation))

            try:
                _, rows3, owners3, _, _ = snapshot(token, spreadsheet_id)
                linked = pilot.reconcile_pilot_link(number, current, owners3)
                if (linked.get("outcome") != "linked" or linked.get("matched_main_row") != target
                        or not owner_matches(linked.get("main_row", {}), current, owner)):
                    raise ValueError("owner_write_readback_failed_do_not_send")
                changes = {
                    "promotion_status": "promoted", "import_ready": "promoted",
                    "matched_main_row": str(target),
                    "promotion_notes": (current.get("promotion_notes", "") + "; " +
                        f"verifier_reviewed_by={automation}; {reason}; owner Sheet1 row {target} has " +
                        f"{owner['agent_name']} {owner['last_name']}; owner_phone={owner['phone']}; " +
                        "owner append and identity readback passed").strip("; "),
                }
                proposed = {**current, **changes}
                latest = dict(rows3).get(number)
                if latest != current:
                    raise ValueError("pilot_changed_before_promotion_write")
                pilot.batch_update_values(token, spreadsheet_id,
                    mapped_updates(PILOT_TAB, ph, number, changes))
                _, rows4, owners4, _, _ = snapshot(token, spreadsheet_id)
                final = dict(rows4).get(number, {})
                final_link = pilot.reconcile_pilot_link(number, final, owners4)
                if (final != proposed or final_link.get("outcome") != "linked"
                        or final_link.get("matched_main_row") != target
                        or not owner_matches(final_link.get("main_row", {}), final, owner)):
                    raise ValueError("promotion_owner_readback_failed_do_not_send")
            except Exception:
                try:
                    _, _, rollback_owners, _, _ = snapshot(token, spreadsheet_id)
                    owned_rows = [n for n, candidate in rollback_owners if owner_matches(candidate, current, owner)]
                    if owned_rows == [target]:
                        delete_owner_row(token, spreadsheet_id, target)
                finally:
                    raise
            return {"ok": True, "contract": VERSION, "pilot_row": number,
                    "owner_row": target, "readback": True, "sheet1_writes": 1, "sends": 0}

        if action == "update":
            expected = payload.get("expected", {})
            matches = [(n, r) for n, r in rows if identity(r) == identity(expected)]
            if len(matches) != 1 or not all(identity(expected)):
                raise ValueError("pilot_identity_missing_or_ambiguous")
            number, row = matches[0]
            changes = dict(payload.get("fields", {}))
            mapped_updates(PILOT_TAB, ph, number, changes)
            for key, value in expected.items():
                if row.get(key, "") != str(value):
                    raise ValueError(f"pilot_changed:{key}")
            if not {"synthetic_zpid", "listing_address", "state", "status", "promotion_status", "import_ready"}.issubset(expected):
                raise ValueError("expected_identity_and_state_required")
            reason = pilot.normalize_space(payload.get("adjudication_reason", ""))
            if not reason:
                raise ValueError("adjudication_reason_required")
            changes["promotion_notes"] = (row.get("promotion_notes", "") + "; " +
                f"verifier_reviewed_by={automation}; {reason}").strip("; ")
            proposed = {**row, **{k: str(v) for k, v in changes.items()}}
            if "duplicate_key" in changes and changes["duplicate_key"] != pilot.street_state_key(row["listing_address"], row["state"]):
                raise ValueError("invalid_duplicate_key")
            if proposed.get("promotion_status") == "promoted":
                if proposed.get("status") != "qualified" or proposed.get("import_ready") != "promoted":
                    raise ValueError("invalid_promoted_state")
                link = pilot.reconcile_pilot_link(number, proposed, main_rows)
                if not pilot.promoted_acceptance_contract(proposed, link)["accepted"] or not evidence_ok(number, proposed):
                    raise ValueError("promoted_acceptance_gap")
            elif proposed.get("matched_main_row"):
                if proposed.get("promotion_status") != "skipped_duplicate_listing" or str(existing_listing_owner(proposed, main_rows) or "") != proposed["matched_main_row"]:
                    raise ValueError("nonpromoted_owner_gap")
            # Re-resolve immediately before the write. Never use a caller's row index.
            ph2, check = read_table(token, spreadsheet_id, PILOT_TAB, pilot.PILOT_HEADERS)
            if ph2 != ph or dict(check).get(number) != row:
                raise ValueError("pilot_changed_before_write")
            pilot.batch_update_values(token, spreadsheet_id, mapped_updates(PILOT_TAB, ph, number, changes))
            _, after, owners, _, _ = snapshot(token, spreadsheet_id)
            reread = dict(after).get(number, {})
            if reread != proposed:
                raise ValueError("pilot_write_readback_failed_do_not_send")
            if proposed.get("promotion_status") == "promoted" and not pilot.promoted_acceptance_contract(reread, pilot.reconcile_pilot_link(number, reread, owners))["accepted"]:
                raise ValueError("owner_shifted_after_write_do_not_send")
            if proposed.get("promotion_status") == "skipped_duplicate_listing" and str(existing_listing_owner(reread, owners) or "") != reread.get("matched_main_row"):
                raise ValueError("duplicate_owner_shifted_after_write_do_not_send")
            return {"ok": True, "contract": VERSION, "pilot_row": number, "owner_row": reread.get("matched_main_row", ""), "readback": True, "sheet1_writes": 0, "sends": 0}

        date = payload.get("run_date", now.astimezone(NY).date().isoformat())
        if date != now.astimezone(NY).date().isoformat():
            raise ValueError("historical_receipt_forbidden")
        receipt_id = str(payload.get("run_receipt_id", ""))
        if action == "receipt":
            if not re.fullmatch(r"[A-Za-z0-9_-]{12,100}", receipt_id):
                raise ValueError("unique_run_receipt_id_required")
            # Idempotent retries return the existing owner receipt, never overwrite it.
            same_id = [(n, r) for n, r in receipts if r.get("run_receipt_id") == receipt_id]
            if same_id:
                if len(same_id) != 1 or not pilot.verifier_schedule_slot_matches(same_id[0][1].get("schedule_slot_id", ""), date, automation):
                    raise ValueError("receipt_id_conflict")
                return {"ok": True, "duplicate": True, "receipt_row": same_id[0][0], "receipt": same_id[0][1], "sends": 0}
            if any(pilot.verifier_schedule_slot_matches(r.get("schedule_slot_id", ""), date, automation) for _, r in receipts):
                raise ValueError("terminal_slot_already_exists_do_not_rewrite")
            started = dt.datetime.fromisoformat(str(payload.get("started_at", "")).replace("Z", "+00:00"))
            if not started.tzinfo or started > now or started.astimezone(NY).date().isoformat() != date:
                raise ValueError("invalid_run_start")
            scheduled = started.astimezone(NY).replace(hour=AUTOMATIONS[automation], minute=0, second=0, microsecond=0)
            if payload.get("run_kind") != "organic_scheduled" or abs((started - scheduled).total_seconds()) > 1800:
                raise ValueError("organic_scheduled_run_required")
        receipt, gaps = build_receipt(pilot_rows=rows, main_rows=main_rows, receipts=receipts,
            date=date, now=now, automation_id=automation, run_receipt_id=receipt_id,
            global_sms_blockers=payload.get("global_sms_blockers"), evidence_ok=evidence_ok,
            source_cutoff=started if action == "receipt" else now)
        if action == "preview":
            return {"ok": True, "preview": True, "contract": VERSION, "receipt": receipt, "gaps": gaps, "writes": 0, "sends": 0}
        receipt["detail"] += "; run_kind=organic_scheduled; started_at=" + started.isoformat()
        # Preserve all fields without the source helper's 500-character truncation.
        values = [""] * len(rh)
        for key, value in receipt.items():
            values[header_index(rh, key)] = value
        pilot.append_values(token, spreadsheet_id, f"'{pilot.RUN_RECEIPT_TAB}'!A:{pilot.column_letter(len(rh))}", [values])
        _, final = read_table(token, spreadsheet_id, pilot.RUN_RECEIPT_TAB, pilot.RUN_RECEIPT_HEADERS)
        matches = [(n, r) for n, r in final if r.get("run_receipt_id") == receipt_id]
        slot_matches = [r for _, r in final if pilot.verifier_schedule_slot_matches(r.get("schedule_slot_id", ""), date, automation)]
        if len(matches) != 1 or matches[0][1] != receipt or len(slot_matches) != 1:
            raise ValueError("terminal_receipt_readback_failed")
        return {"ok": True, "contract": VERSION, "receipt_row": matches[0][0], "receipt": receipt, "gaps": gaps, "sends": 0}
