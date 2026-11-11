"""Native, read-only cohort capture and unskippable verifier receipt gate.

Only the private proof journal is written. Never sends or modifies lead controls.
"""
import base64
import datetime as dt
import gzip
import hashlib
import json
import re
import threading
import time
import urllib.parse
import urllib.error
from zoneinfo import ZoneInfo

from scripts import free_short_sale_source_pilot as sheets
from verifier_cohort_audit import evaluate, execution_closeout, digits, ACTIVE

TAB = 'Verifier Closeout Proof'
HEADERS = ['run_id', 'automation_id', 'phase', 'observed_at', 'sha256',
           'chunk_index', 'chunk_count', 'artifact_gzip_base64']
AUTOMATIONS = {'lead-verifier-8-am', 'lead-verifier-11-am', 'lead-verifier-2-pm',
               'lead-verifier-5-pm', 'lead-verifier-8-pm'}
LOCK = threading.RLock()
VERSION = 'native_verifier_closeout_v1'


def native_get(token, path):
    for attempt, delay in enumerate((5, 20, 45, 0)):
        try:
            return sheets.sheets_request(token, 'GET', path)
        except urllib.error.HTTPError as exc:
            if exc.code not in (429, 500, 502, 503, 504) or not delay:
                raise
            time.sleep(delay)  # Read-only retry; never replay a send or lease.


def batch_read(token, sid, specs):
    """Each spec is a bounded range. Empty successful blocks may be padded."""
    result = []
    for start in range(0, len(specs), 12):
        group = specs[start:start + 12]
        query = urllib.parse.urlencode([('valueRenderOption', 'UNFORMATTED_VALUE')] +
                                      [('ranges', spec) for spec in group])
        response = native_get(token, f'{sid}/values:batchGet?{query}')
        blocks = response.get('valueRanges')
        if not isinstance(blocks, list) or len(blocks) != len(group):
            raise ValueError('incomplete_native_capture')
        for block in blocks:
            if not block.get('range') or ('values' in block and not isinstance(block['values'], list)):
                raise ValueError('invalid_native_capture_block')
            result.append(block.get('values', []))
    return result


def grid_read(token, sid, tab, rows, columns):
    values = []
    specs = [f"'{tab}'!A{n}:{columns}{min(n + 499, rows)}" for n in range(1, rows + 1, 500)]
    for number, block in zip(range(1, rows + 1, 500), batch_read(token, sid, specs)):
        count = min(500, rows - number + 1)
        if len(block) > count:
            raise ValueError('capture_range_overflow')
        values.extend(block + [[] for _ in range(count - len(block))])
    return values


def meaningful(values):
    result = {}
    for number, raw in enumerate(values[1:], 2):
        row = [str(v or '') for v in raw] + [''] * max(0, 7 - len(raw))
        if not any(row[i].strip() for i in (0, 1, 4)):
            continue
        if number >= 9000 and not any(row[4:7]) and all(
                not v or re.fullmatch(r'\+?\d+|[^\s@]+@[^\s@]+\.[^\s@]+', v) for v in row[:4]):
            continue
        result[number] = row
    return result


def identity(row):
    return (row[0].strip().lower(), row[1].strip().lower(), digits(row[2]), row[4].strip().lower())


def capture(token, sid, frozen=(), carried_rows=()):
    meta = native_get(token, f'{sid}?fields=sheets.properties')
    sizes = {s['properties']['title']: s['properties']['gridProperties']['rowCount'] for s in meta['sheets']}
    main = grid_read(token, sid, 'Sheet1', sizes['Sheet1'], 'G')
    meaningful_rows = meaningful(main)
    selected = set(list(meaningful_rows)[-200:])
    for n in carried_rows:
        if n not in meaningful_rows:
            raise ValueError('carried_identity_requires_reresolution:' + str(n))
        selected.add(n)
    resolved = []
    for old in frozen:
        matches = [n for n, row in meaningful_rows.items() if identity(row) == tuple(old['identity'])]
        if len(matches) != 1:
            raise ValueError('frozen_identity_missing_or_ambiguous:' + str(old['row']))
        selected.add(matches[0])
        resolved.append({**old, 'row': matches[0]})
    header = batch_read(token, sid, ["'Sheet1'!A1:AQ1"])[0]
    if not header:
        raise ValueError('missing_sheet_header')
    main[0] = header[0]
    # Read whole A:AB details plus AQ for every selected identity, including late additions.
    selected = sorted(selected)
    specs = []
    for n in selected:
        specs.extend([f"'Sheet1'!A{n}:AB{n}", f"'Sheet1'!AQ{n}:AQ{n}"])
    # Coalesce neighboring detail rows; 50-row blocks avoid large voice/history responses.
    groups = []
    for n in selected:
        if groups and n == groups[-1][-1] + 1 and len(groups[-1]) < 50:
            groups[-1].append(n)
        else:
            groups.append([n])
    specs = [r for g in groups for r in (f"'Sheet1'!A{g[0]}:AB{g[-1]}", f"'Sheet1'!AQ{g[0]}:AQ{g[-1]}")]
    blocks = batch_read(token, sid, specs)
    for i, group in enumerate(groups):
        details, aq = blocks[2*i:2*i+2]
        for offset, n in enumerate(group):
            row = (details[offset] if offset < len(details) else [])
            if len(row) < 5 or identity([str(v or '') for v in row]) != identity(meaningful_rows[n]):
                raise ValueError('identity_changed_during_capture')
            main[n-1] = row + [''] * (42 - len(row)) + [(aq[offset][0] if offset < len(aq) and aq[offset] else '')]
    # Project all outbox rows to the seven audited columns; preserve every row offset.
    native_header = batch_read(token, sid, ["'sms_pending_sends'!A1:Z1"])[0][0]
    required = ['status', 'request_id', 'message_id', 'phone', 'reply_text', 'sent_at', 'crm_row']
    indices = []
    for key in required:
        if native_header.count(key) != 1:
            raise ValueError('outbox_schema_drift')
        indices.append(native_header.index(key))
    segments = []
    for index in sorted(indices):
        if segments and index == segments[-1][-1] + 1:
            segments[-1].append(index)
        else:
            segments.append([index])
    count = sizes['sms_pending_sends']
    outbox = [[] for _ in range(count)]
    outbox[0] = required
    specs, bindings = [], []
    for begin in range(2, count + 1, 500):
        end = min(begin + 499, count)
        for segment in segments:
            specs.append(f"'sms_pending_sends'!{sheets.column_letter(segment[0]+1)}{begin}:{sheets.column_letter(segment[-1]+1)}{end}")
            bindings.append((begin, end, segment))
    for (begin, end, segment), block in zip(bindings, batch_read(token, sid, specs)):
        for n in range(begin, end + 1):
            if not outbox[n-1]:
                outbox[n-1] = [''] * len(required)
            row = block[n-begin] if n-begin < len(block) else []
            for j, native in enumerate(segment):
                outbox[n-1][indices.index(native)] = row[j] if j < len(row) else ''
    queue = grid_read(token, sid, 'Initial SMS Queue', sizes['Initial SMS Queue'], 'I')
    carried = set()
    historical = []
    for entry in outbox[1:]:
        if entry and entry[0] in ACTIVE and re.fullmatch(r'(?:scheduler-|render-)?initial-\d+-.+', str(entry[2])):
            matches = [n for n, row in meaningful_rows.items() if digits(row[2]) == digits(entry[3])]
            if len(matches) == 1:
                if matches[0] in selected:
                    carried.add(matches[0])
                else:
                    # Inventory old uncertain history separately. It is not
                    # standing authorization to sweep historical outreach.
                    historical.append({'row': matches[0], 'request_id': entry[1],
                                       'phone': entry[3], 'status': entry[0]})
    for entry in queue[1:]:
        if len(entry) > 4 and entry[4] in ACTIVE:
            payload = json.loads(entry[3])
            matches = [n for n, row in meaningful_rows.items() if digits(row[2]) == digits(payload.get('phone'))]
            if len(matches) == 1:
                carried.add(matches[0])
    extra = sorted(carried - set(selected))
    extra_blocks = batch_read(token, sid, [r for n in extra for r in
                             (f"'Sheet1'!A{n}:AB{n}", f"'Sheet1'!AQ{n}:AQ{n}")])
    for i, n in enumerate(extra):
        detail, aq = extra_blocks[2*i:2*i+2]
        row = detail[0] if detail else []
        if len(row) < 5 or identity([str(v or '') for v in row]) != identity(meaningful_rows[n]):
            raise ValueError('carried_identity_changed_during_capture')
        main[n-1] = row + [''] * (42-len(row)) + [aq[0][0] if aq and aq[0] else '']
    selected = sorted(set(selected) | carried)
    # Seal against a lead arriving or changing while the capture was in progress.
    final_ids = grid_read(token, sid, 'Sheet1', sizes['Sheet1'], 'G')
    if main[0][:7] != final_ids[0][:7] or any(main[n][:7] != final_ids[n][:7] for n in range(1, len(main))):
        raise ValueError('identities_changed_retry_final_capture')
    snapshot = {'observed_at': dt.datetime.now(dt.timezone.utc).isoformat(),
                'capture_mode': 'cohort_sparse_v1', 'carried_rows': sorted(carried | set(carried_rows)),
                'historical_open_initials': historical, 'tables': {}}
    for tab, values in [('Sheet1', main), ('sms_pending_sends', outbox), ('Initial SMS Queue', queue)]:
        snapshot['tables'][tab] = dict(start_row=1, end_row=sizes[tab], grid_rows=sizes[tab], truncated=False, values=values)
    snapshot['tables']['Sheet1']['detail_rows'] = selected
    return snapshot, resolved


def journal_read(token, sid, run_id, phase):
    # Read the small index first, then only this run's compressed artifact.
    # Never re-download all historical snapshots to validate one receipt.
    rows = batch_read(token, sid, [f"'{TAB}'!A:G"])[0]
    matches = [(number, r) for number, r in enumerate(rows[1:], 2)
               if len(r) >= 7 and r[0] == run_id and r[2] == phase]
    if not matches:
        return None
    digest = matches[-1][1][4]
    parts = [(number, r) for number, r in matches if r[4] == digest]
    if len(parts) != int(parts[0][1][6]) or len({r[5] for _, r in parts}) != len(parts):
        raise ValueError('incomplete_immutable_gate_artifact')
    parts.sort(key=lambda entry: int(entry[1][5]))
    payloads = batch_read(token, sid, [f"'{TAB}'!H{number}:H{number}" for number, _ in parts])
    if any(not block or not block[0] or not isinstance(block[0][0], str) for block in payloads):
        raise ValueError('incomplete_immutable_gate_artifact')
    data = gzip.decompress(base64.b64decode(''.join(block[0][0] for block in payloads)))
    if hashlib.sha256(data).hexdigest() != digest:
        raise ValueError('gate_artifact_digest_mismatch')
    return json.loads(data)


def journal_write(token, sid, run_id, automation, phase, artifact):
    encoded = json.dumps(artifact, separators=(',', ':')).encode()
    digest = hashlib.sha256(encoded).hexdigest()
    compressed = base64.b64encode(gzip.compress(encoded)).decode()
    parts = [compressed[n:n+30000] for n in range(0, len(compressed), 30000)]
    values = [[run_id, automation, phase, artifact['observed_at'], digest, str(i), str(len(parts)), part]
              for i, part in enumerate(parts)]
    sheets.append_values(token, sid, f"'{TAB}'!A:H", values)
    if journal_read(token, sid, run_id, phase) != artifact:
        raise ValueError('gate_artifact_readback_failed')
    return digest


def terminal(initial, snapshot, resolutions=None):
    """The server derives every outcome from fresh evidence, never caller green flags."""
    frozen = initial['frozen']
    live = meaningful(snapshot['tables']['Sheet1']['values'])
    mapped = []
    for entry in frozen:
        matches = [n for n, row in live.items() if identity(row) == tuple(entry['identity'])]
        if len(matches) != 1:
            raise ValueError('frozen_identity_missing_or_ambiguous')
        mapped.append(matches[0])
    audit = evaluate(snapshot, carried_rows=set(mapped) | set(snapshot.get('carried_rows', [])))
    first = {'ledger': [{**r, 'row': n} for r, n in zip(initial['audit']['ledger'], mapped)]}
    # Freeze order is audit ledger order, not Sheet row order.
    old_keys = {tuple(r['identity']) for r in frozen}
    additions = [n for n, row in live.items() if n in snapshot['tables']['Sheet1']['detail_rows'] and identity(row) not in old_keys]
    items = []
    for row in audit['ledger']:
        disposition = row['sms_disposition']
        item = {k: row[k] for k in ('row', 'phone', 'stable_id')}
        item.update(reason=disposition, evidence='Native current Sheet/outbox/queue capture',
                    next_action=row['next_action'], retry_after=(dt.datetime.now(dt.timezone.utc)+dt.timedelta(minutes=15)).isoformat())
        if disposition == 'sent_proven':
            item['outcome'] = 'sent'
        elif disposition in {'reply_or_takeover_or_closed', 'prior_phone_contact', 'excluded_owner_review_no_send'}:
            item['outcome'] = 'no_send'
        else:
            active = next((r for r in row['requests'] if r['status'] in ACTIVE), None)
            if active:
                item.update(outcome='pending_request', request_id=active['request_id'])
            else:
                item.update(outcome='blocked', blocker_type='safety',
                            reason='Native terminal gate requires verifier disposition: '+disposition)
        if (resolutions or {}).get(str(row['row'])):
            item['contact_resolution'] = resolutions[str(row['row'])]
        items.append(item)
    execution = {'observed_at': snapshot['observed_at'], 'items': items}
    closed = execution_closeout(audit, execution, first)
    audit['execution_closeout'] = closed
    return {'contract': VERSION, 'observed_at': snapshot['observed_at'], 'audit': audit,
            'execution_ledger': execution, 'late_rows': additions,
            'historical_open_initials': snapshot.get('historical_open_initials', []),
            'complete': closed['complete'], 'sms_open_rows': audit['sms_review_rows'],
            'terminal_gate_executed': True, 'outreach_calls': 0}


def handle(token, sid, payload):
    automation, run_id, action = (payload.get(k, '') for k in ('automation_id', 'run_receipt_id', 'action'))
    if automation not in AUTOMATIONS or not re.fullmatch(r'[A-Za-z0-9_-]{12,100}', run_id):
        raise ValueError('invalid_closeout_run_identity')
    if action == 'begin' and payload.get('run_kind', 'organic_scheduled') == 'organic_scheduled':
        local = dt.datetime.now(ZoneInfo('America/New_York'))
        hour = {'lead-verifier-8-am':8, 'lead-verifier-11-am':11, 'lead-verifier-2-pm':14,
                'lead-verifier-5-pm':17, 'lead-verifier-8-pm':20}[automation]
        slot = local.replace(hour=hour, minute=0, second=0, microsecond=0)
        if not slot-dt.timedelta(minutes=5) <= local < slot+dt.timedelta(hours=1):
            raise ValueError('outside_named_verifier_slot')
    with LOCK:
        sheets.ensure_headers_tab(token, sid, TAB, HEADERS, hidden=True)
        initial = journal_read(token, sid, run_id, 'initial')
        if initial and initial['automation_id'] != automation:
            raise ValueError('closeout_run_automation_mismatch')
        if action == 'begin':
            if payload.get('run_kind', 'organic_scheduled') not in {'organic_scheduled', 'manual_verification'}:
                raise ValueError('invalid_run_kind')
            if initial:
                return {'ok': True, 'duplicate': True, 'initial': initial}
            snap, _ = capture(token, sid, carried_rows=payload.get('carried_rows', []))
            audit = evaluate(snap, carried_rows=snap.get('carried_rows', []))
            live = meaningful(snap['tables']['Sheet1']['values'])
            initial = {'observed_at': snap['observed_at'], 'automation_id': automation,
                       'run_kind': payload.get('run_kind', 'organic_scheduled'),
                       'run_receipt_id': run_id, 'audit': audit,
                       'snapshot': snap,
                       'frozen': [{'row': r['row'], 'identity': list(identity(live[r['row']]))} for r in audit['ledger']]}
            journal_write(token, sid, run_id, automation, 'initial', initial)
            return {'ok': True, 'initial': initial}
        if action not in {'final', 'status'}:
            raise ValueError('invalid_closeout_action')
        if not initial or initial['automation_id'] != automation:
            raise ValueError('missing_initial_run_capture_terminal_forbidden')
        if action == 'status':
            return {'ok': True, 'initial': initial, 'terminal': journal_read(token, sid, run_id, 'terminal')}
        age = (dt.datetime.now(dt.timezone.utc) - dt.datetime.fromisoformat(initial['observed_at'])).total_seconds()
        if not 0 <= age <= 5400:
            raise ValueError('initial_run_capture_stale')
        snap, _ = capture(token, sid, initial['frozen'])
        result = terminal(initial, snap, payload.get('contact_resolutions'))
        result['snapshot'] = snap
        result['run_receipt_id'] = run_id
        result['automation_id'] = automation
        result['run_kind'] = initial['run_kind']
        journal_write(token, sid, run_id, automation, 'terminal', result)
        return {'ok': True, 'terminal': result}
