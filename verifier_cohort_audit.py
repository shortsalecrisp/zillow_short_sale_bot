"""Read-only current-cohort triage. Never authorizes or sends a message."""
import argparse
import datetime as dt
import json
import re
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

VERSION = 'current_cohort_contact_sms_v3_contact_closeout'
# Only unambiguous state zones. Split-zone states require fresh explicit evidence.
STATE_ZONES = {}
for zone, states in {
    'America/New_York': 'CT DE DC GA ME MD MA NH NJ NY NC OH PA RI SC VT VA WV',
    'America/Chicago': 'AL AR IL IA LA MN MS MO OK WI',
    'America/Denver': 'CO MT NM UT WY',
    'America/Los_Angeles': 'CA WA',
    'Pacific/Honolulu': 'HI',
}.items():
    STATE_ZONES.update(dict.fromkeys(states.split(), zone))
ACTIVE = {'queued', 'pending', 'claimed', 'processing', 'send_started',
          'started', 'receipt_pending', 'uncertain'}


def digits(value):
    value = re.sub(r'\D', '', str(value or ''))
    return value[1:] if len(value) == 11 and value.startswith('1') else value


def valid_phone(value):
    value = digits(value)
    # NANP: NXX-NXX-XXXX; N9X area codes are reserved for expansion.
    return bool(re.fullmatch(r'[2-9][0-8]\d[2-9]\d{6}', value)
                and value[1:3] != '11' and value[4:6] != '11')


def contact_issues(row):
    issues = []
    if not row[0].strip() or not row[1].strip() or not row[4].strip():
        issues.append('identity_or_address_review')
    if not valid_phone(row[2]):
        issues.append('missing_or_invalid_phone')
    email = row[3].strip()
    if not re.fullmatch(r'[^\s@]+@[^\s@]+\.[A-Za-z]{2,}', email):
        issues.append('missing_or_invalid_email')
    # Flag a possible escaped-newline extraction artifact; NEVER strip it here.
    local = email.split('@')[0].lower()
    first = re.sub(r'[^a-z]', '', row[0].lower())
    if first and local.startswith('n' + first):
        issues.append('possible_literal_newline_email_prefix')
    provenance = row[25]
    confidence = row[26].lower()
    if (not re.search(r'https?://', provenance)
            or not re.match(r'(verified_|team_or_listing_email|brokerage_routing_email)', confidence)
            or re.search(r'unsafe|unverified|conflict|no_personal_email|not_found', confidence)):
        issues.append('published_contact_attribution_review')
    if not row[24] or row[24].lower() in {'low', 'unknown', 'unsafe'}:
        issues.append('phone_attribution_review')
    return issues


def table(snapshot, name):
    t = snapshot['tables'][name]
    if (t['start_row'] != 1 or t['end_row'] != t['grid_rows']
            or t['truncated'] is not False or len(t['values']) != t['grid_rows']):
        raise ValueError('incomplete_scan:' + name)
    return t['values']


def recipient_window(row, number, snapshot, now):
    """Hours diagnostic only; never grants sending authority."""
    supplied = snapshot.get('recipient_time_zones', {}).get(str(number), {})
    zone = STATE_ZONES.get(row[6].strip().upper())
    source = 'unambiguous_listing_state' if zone else None
    if supplied:
        if (digits(supplied.get('phone')) != digits(row[2])
                or not re.match(r'https?://', str(supplied.get('source_url', '')))):
            return {'status': 'timezone_review_required', 'reason': 'unattributed_timezone'}
        zone, source = supplied.get('time_zone'), supplied['source_url']
    if not zone:
        return {'status': 'timezone_review_required', 'reason': 'missing_or_split_zone_location'}
    try:
        local = now.astimezone(ZoneInfo(zone))
    except (ZoneInfoNotFoundError, ValueError, TypeError):
        return {'status': 'timezone_review_required', 'reason': 'invalid_timezone'}
    opened = 8 <= local.hour < 20
    next_open = local.replace(hour=8, minute=0, second=0, microsecond=0)
    if local.hour >= 20:
        next_open += dt.timedelta(days=1)
    return {'status': 'open' if opened else 'closed', 'time_zone': zone,
            'local_time': local.isoformat(), 'source': source,
            'next_open_at': None if opened else next_open.isoformat(),
            'send_authorized': False}


def execution_closeout(audit, execution, initial=None):
    """Reject omissions and invented completion without sending or writing."""
    errors, open_rows = [], []
    try:
        observed = dt.datetime.fromisoformat(audit['observed_at'])
        executed = dt.datetime.fromisoformat(execution['observed_at'])
        if executed.tzinfo is None or abs((observed - executed).total_seconds()) > 300:
            raise ValueError('stale_execution')
    except (KeyError, ValueError, TypeError):
        errors.append({'row': None, 'error': 'stale_or_missing_execution_timestamp'})
    current = {r['row']: r for r in audit['ledger']}
    frozen = {r['row']: r for r in (initial or audit)['ledger']}
    expected = set(current) | set(frozen)
    items = {}
    for item in execution.get('items', []):
        number = item.get('row')
        if number in items:
            errors.append({'row': number, 'error': 'duplicate_execution_row'})
        items[number] = item
    for number in sorted(expected):
        row, item = current.get(number), items.get(number)
        if row is None or item is None:
            errors.append({'row': number, 'error': 'omitted_frozen_or_final_row'})
            continue
        if (digits(item.get('phone')) != digits(row.get('phone'))
                or str(item.get('stable_id') or '') != str(row.get('stable_id') or '')):
            errors.append({'row': number, 'error': 'execution_identity_mismatch'})
            continue
        outcome = item.get('outcome')
        if outcome == 'sent':
            if row['sms_disposition'] != 'sent_proven':
                errors.append({'row': number, 'error': 'sent_proof_or_sheet_readback_missing'})
        elif outcome == 'no_send':
            if row['sms_disposition'] not in {'reply_or_takeover_or_closed', 'prior_phone_contact',
                                              'excluded_owner_review_no_send'} or not item.get('evidence'):
                errors.append({'row': number, 'error': 'no_send_disposition_unproved'})
        elif outcome in {'deferred_local_hours', 'pending_request', 'blocked'}:
            open_rows.append(number)
            if outcome == 'deferred_local_hours' and row.get('recipient_window', {}).get('status') != 'closed':
                errors.append({'row': number, 'error': 'local_hours_deferral_invalid'})
            if outcome == 'pending_request' and not any(
                    r.get('request_id') == item.get('request_id') and r.get('status') in ACTIVE
                    for r in row.get('requests', [])):
                errors.append({'row': number, 'error': 'pending_request_not_proven'})
            if outcome == 'blocked' and item.get('blocker_type') not in {'safety', 'external', 'tool'}:
                errors.append({'row': number, 'error': 'concrete_blocker_required'})
            if not all(item.get(k) for k in ('reason', 'evidence', 'next_action', 'retry_after')):
                errors.append({'row': number, 'error': 'open_item_missing_evidence_or_next_action'})
            try:
                retry = dt.datetime.fromisoformat(item.get('retry_after', ''))
                observed = dt.datetime.fromisoformat(audit['observed_at'])
                if retry.tzinfo is None or retry <= observed:
                    raise ValueError('retry_not_future')
            except (ValueError, TypeError):
                errors.append({'row': number, 'error': 'retry_after_invalid'})
        else:
            errors.append({'row': number, 'error': 'unprocessed_or_invalid_outcome'})
    for number in set(items) - expected:
        errors.append({'row': number, 'error': 'unexpected_execution_row'})
    contact_open_rows = [n for n, r in current.items() if any(
        issue in r.get('contact_issues', []) for issue in (
            'missing_or_invalid_phone', 'missing_or_invalid_email',
            'possible_literal_newline_email_prefix'))]
    for number in contact_open_rows:
        contact = items.get(number, {}).get('contact_resolution', {})
        if (contact.get('outcome') != 'blocked' or not all(contact.get(k) for k in
                ('sources_tried', 'reason', 'next_action', 'retry_after', 'handoff_id', 'handoff_status'))):
            errors.append({'row': number, 'error': 'contact_research_or_sol_handoff_missing'})
        if contact.get('handoff_status') not in {'confirmed_exact_row', 'blocked_delivery'}:
            errors.append({'row': number, 'error': 'contact_handoff_readback_missing'})
    return {'balanced': not errors, 'complete': not errors and not open_rows and not contact_open_rows,
            'contact_open_rows': sorted(contact_open_rows),
            'expected_count': len(expected), 'execution_count': len(items),
            'open_rows': open_rows, 'errors': errors}


def evaluate(snapshot, now=None, carried_rows=()):
    now = now or dt.datetime.now(dt.timezone.utc)
    observed = dt.datetime.fromisoformat(snapshot['observed_at'])
    if observed.tzinfo is None or not 0 <= (now - observed).total_seconds() <= 300:
        raise ValueError('stale_or_invalid_snapshot')
    main = table(snapshot, 'Sheet1')
    outbox = table(snapshot, 'sms_pending_sends')
    if any(len(main[0]) <= i or main[0][i] != label for i, label in {
            0: 'agent_name', 1: 'last_name', 2: 'phone', 3: 'email',
            4: 'listing_address', 7: 'initial_text_sent',
            11: 'last_outbound_text', 25: 'contact_verification_note',
            42: 'codex-verified'}.items()):
        raise ValueError('sheet1_schema_drift')
    required = ['status', 'phone', 'request_id', 'message_id', 'crm_row', 'reply_text', 'sent_at']
    if any(outbox[0].count(k) != 1 for k in required):
        raise ValueError('outbox_schema_drift')
    receipts = [dict(zip(outbox[0], r)) for r in outbox[1:]]
    queue = table(snapshot, 'Initial SMS Queue')
    if any(queue[0].count(k) != 1 for k in ['request_id', 'payload_json', 'status']):
        raise ValueError('initial_queue_schema_drift')
    queued_by_phone = {}
    for raw in queue[1:]:
        q = dict(zip(queue[0], raw))
        if not q.get('request_id'):
            continue
        try:
            payload = json.loads(q.get('payload_json') or '{}')
            if not isinstance(payload, dict):
                raise ValueError('non_object_payload')
        except (TypeError, ValueError) as exc:
            raise ValueError('unreadable_initial_queue_payload:' + str(q['request_id'])) from exc
        queued_by_phone.setdefault(digits(payload.get('phone')), []).append(q)
    by_phone = {}
    for r in receipts:
        by_phone.setdefault(digits(r.get('phone')), []).append(r)
    owners, meaningful, artifacts = {}, {}, []
    for number, raw in enumerate(main[1:], 2):
        row = [str(v or '') for v in raw] + [''] * max(0, 43 - len(raw))
        owners.setdefault(digits(row[2]), []).append(number)
        if not any(row[i].strip() for i in (0, 1, 4)):
            continue
        if number >= 9000 and not any(row[4:7]) and all(
                not v or re.fullmatch(r'\+?\d+|[^\s@]+@[^\s@]+\.[^\s@]+', v)
                for v in row[:4]):
            artifacts.append(number)
            continue
        meaningful[number] = row
    recent = list(meaningful)[-200:]
    selected = set(recent) | set(carried_rows)
    if snapshot.get('capture_mode') == 'cohort_sparse_v1':
        # This faster first-work capture is NEVER a full-sheet closeout snapshot.
        # All A:G identities/phones are present, but A:AQ is required for every
        # selected meaningful row; a partial recent-cohort read fails closed.
        details = set(snapshot['tables']['Sheet1'].get('detail_rows', []))
        if not (selected & set(meaningful)).issubset(details):
            raise ValueError('incomplete_current_cohort_details')
    ledger = []
    for number in sorted(selected):
        if number not in meaningful:
            ledger.append({'row': number, 'contact_issues': ['carried_identity_requires_reresolution'],
                           'sms_disposition': 'open_identity', 'next_action': 'resolve_stable_identity_no_send'})
            continue
        row = meaningful[number]
        phone = digits(row[2])
        all_attempts = by_phone.get(phone, []) if phone else []
        queue_attempts = queued_by_phone.get(phone, []) if phone else []
        initials = [r for r in all_attempts if str(r.get('crm_row')) == str(number)
                    and re.fullmatch(r'(?:scheduler-|render-)?initial-' + str(number) + r'-.+',
                                     str(r.get('message_id', '')))]
        sent = [r for r in initials if r.get('status') == 'sent' and r.get('sent_at')
                and r.get('request_id') and r.get('reply_text')]
        issues = contact_issues(row)
        reply = bool(row[9].strip() or row[16].upper() == 'TRUE'
                     or row[19].upper() == 'TRUE'
                     or row[13].lower() in {'handoff', 'done', 'do_not_contact', 'opt_out'}
                     or row[15].lower() in {'closed_no_interest', 'opt_out', 'do_not_contact'})
        excluded = row[27] == '338004481' or ' '.join(row[:2]).strip().lower() == 'chad neumann'
        if excluded:
            disposition, action = 'excluded_owner_review_no_send', 'preserve_owner_exclusion_never_contact'
        elif not valid_phone(row[2]):
            disposition, action = 'open_phone', 'research_phone_then_resurvey_no_send'
        elif sent:
            if row[7].lower() == 'x' and row[22] and row[11]:
                disposition, action = 'sent_proven', 'no_resend'
            else:
                disposition, action = 'sent_needs_sheet_reconciliation', 'reconcile_H_W_L_from_exact_receipt_no_send'
        elif any(r.get('status') in ACTIVE or not r.get('status')
                 for r in all_attempts + queue_attempts):
            disposition, action = 'existing_uncertain_request', 'reconcile_existing_request_no_new_request'
        elif reply:
            disposition, action = 'reply_or_takeover_or_closed', 'preserve_controls_no_send'
        elif any(r.get('status') == 'sent' for r in all_attempts):
            disposition, action = 'prior_phone_contact', 'reconcile_identity_and_owner_no_send'
        elif any(n != number for n in owners.get(phone, [])):
            disposition, action = 'duplicate_phone_review', 'resolve_all_phone_owners_before_any_write_or_send'
        elif initials or queue_attempts:
            disposition, action = 'existing_failed_request', 'diagnose_exact_attempt_and_uncertainty_before_retry'
        else:
            old_phone_initial = [r for r in receipts if str(r.get('crm_row')) == str(number)
                                 and digits(r.get('phone')) != phone
                                 and re.fullmatch(r'(?:scheduler-|render-)?initial-' + str(number) + r'-.+',
                                                  str(r.get('message_id', '')))]
            disposition = 'corrected_phone_initial_review' if old_phone_initial else 'never_attempted_initial_review'
            action = 'verify_qualification_identity_contact_all_native_controls_queues_hours_then_one_approved_initial'
        urgent_sms = disposition in {'never_attempted_initial_review', 'corrected_phone_initial_review',
                                     'existing_uncertain_request', 'existing_failed_request'}
        urgent_contact = any(i in issues for i in {'missing_or_invalid_phone',
                             'missing_or_invalid_email', 'possible_literal_newline_email_prefix'})
        priority = 0 if urgent_sms or urgent_contact else (1 if issues else 2)
        ledger.append({'row': number, 'priority': priority, 'name': ' '.join(row[:2]), 'address': row[4],
                       'stable_id': row[27], 'phone': row[2], 'email': row[3],
                       'contact_issues': issues, 'sms_disposition': disposition,
                       'requests': [{'request_id': r.get('request_id'), 'status': r.get('status')}
                                    for r in all_attempts + queue_attempts], 'next_action': action,
                       'recipient_window': recipient_window(row, number, snapshot, now),
                       'send_authorized': False})
    ledger.sort(key=lambda r: (r.get('priority', 0), r['row']))
    return {'version': VERSION, 'observed_at': snapshot['observed_at'],
            'full_sheet_closeout': False,
            'actual_tail': max(meaningful, default=1), 'recent_count': len(recent),
            'ledger_count': len(ledger), 'classified_artifacts': artifacts, 'ledger': ledger,
            'contact_review_rows': [r['row'] for r in ledger if r['contact_issues']],
            'sms_review_rows': [r['row'] for r in ledger if r['sms_disposition'] not in {
                'sent_proven', 'reply_or_takeover_or_closed', 'prior_phone_contact',
                'excluded_owner_review_no_send'}],
            'outreach_calls': 0}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--snapshot', type=Path, required=True)
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--carried-row', type=int, action='append', default=[])
    p.add_argument('--initial-audit', type=Path)
    p.add_argument('--execution-ledger', type=Path)
    args = p.parse_args()
    result = evaluate(json.loads(args.snapshot.read_text()), carried_rows=args.carried_row)
    if args.initial_audit and not args.execution_ledger:
        p.error('--initial-audit requires --execution-ledger')
    if args.execution_ledger:
        result['execution_closeout'] = execution_closeout(
            result, json.loads(args.execution_ledger.read_text()),
            json.loads(args.initial_audit.read_text()) if args.initial_audit else None)
    with args.output.open('x') as f:
        json.dump(result, f, indent=2)
    print(json.dumps({k: v for k, v in result.items() if k not in {'ledger', 'classified_artifacts'}}))
    if args.execution_ledger and not result['execution_closeout']['complete']:
        raise SystemExit(2)


if __name__ == '__main__':
    main()
