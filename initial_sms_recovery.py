"""Durable initial enqueue intents. Delivery remains owned by handset receipts."""
import hashlib
import json
from datetime import datetime, timezone

HEADERS = ['request_id', 'submitted_at', 'automation_id', 'payload_json', 'status',
           'claimed_at', 'completed_at', 'result_json', 'error']


def persist(queue, payload, automation_id='initial-sms-recovery', retry_after=''):
    # Pin the entire validated request, including corrected-phone flags. Never
    # include transport credentials or turn a different phone into the same job.
    encoded = json.dumps(payload, sort_keys=True, separators=(',', ':'))
    key = 'initial-intent-' + hashlib.sha256(encoded.encode()).hexdigest()[:24]
    rows = queue.get_all_values()
    if not rows or rows[0][:9] != HEADERS:
        raise ValueError('initial_queue_schema_drift')
    if any(row and row[0] == key for row in rows[1:]):
        return key
    queue.append_row([key, datetime.now(timezone.utc).isoformat(), automation_id,
                      encoded, 'pending', retry_after, '', '', ''], value_input_option='RAW')
    # Persistence must precede any network enqueue or process-local timer.
    if not any(row and row[0] == key and row[3] == encoded
               for row in queue.get_all_values()[1:]):
        raise RuntimeError('initial_intent_readback_failed')
    return key


def retryable_error(status, detail):
    # Same deterministic outbox identity is reconciled by the guarded enqueue.
    # Never reset a handset lease, retry a terminal failed outbox, or weaken a
    # recipient/consent guard. Transient enqueue failures retain their intent.
    return status in (429, 500, 502, 503, 504)
