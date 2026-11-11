import json
from datetime import datetime, timedelta, timezone
from test_internal_sms_endpoint import _import_webhook_server, FakeSendResult


def item(module, queue, retry_at=''):
    queue.rows[2] = ['intent-test', datetime.now(timezone.utc).isoformat(), 'test',
                     json.dumps(dict(row=12, phone='555-111-2212', first='Alex',
                                     address='123 Main', mark_codex_verified=True)),
                     'pending', retry_at, '', '', '']
    return {**dict(zip(module.INITIAL_SMS_QUEUE_HEADERS, queue.rows[2])), '_row_num': 2}


def test_transient_502_keeps_same_durable_intent_with_future_retry(monkeypatch):
    module, _, _ = _import_webhook_server(monkeypatch, sender_result=FakeSendResult())
    queue = module._get_initial_sms_queue_ws()
    rec = item(module, queue)
    def fail(_):
        raise module.HTTPException(status_code=502, detail='tasker_outbox_enqueue_failed:ReadTimeout')
    monkeypatch.setattr(module, '_send_initial_sms_from_payload', fail)
    module._process_initial_sms_queue_item(queue, rec)
    assert queue.rows[2][0] == 'intent-test'
    assert queue.rows[2][4] == 'pending'
    assert datetime.fromisoformat(queue.rows[2][5]) > datetime.now(timezone.utc)
    assert module._claim_next_initial_sms_queue_item(queue) is None


def test_eastern_cutoff_does_not_skip_queue_processing(monkeypatch):
    module, _, _ = _import_webhook_server(monkeypatch, sender_result=FakeSendResult())
    queue = module._get_initial_sms_queue_ws()
    item(module, queue)
    monkeypatch.setattr(module, '_within_initial_hours', lambda _: False)
    assert module._process_initial_sms_queue(max_items=1) == 1


def test_guard_rejection_stays_failed_without_automatic_retry(monkeypatch):
    module, _, _ = _import_webhook_server(monkeypatch, sender_result=FakeSendResult())
    queue = module._get_initial_sms_queue_ws()
    rec = item(module, queue)
    def reject(_):
        raise module.HTTPException(status_code=409, detail='row_phone_mismatch')
    monkeypatch.setattr(module, '_send_initial_sms_from_payload', reject)
    module._process_initial_sms_queue_item(queue, rec)
    assert queue.rows[2][4] == 'failed'


def test_http_timeout_persists_pinned_intent_before_error_response(monkeypatch):
    from fastapi.testclient import TestClient
    module, sheet, _ = _import_webhook_server(monkeypatch, sender_result=FakeSendResult())
    def fail(_):
        raise module.HTTPException(status_code=502, detail='tasker_outbox_enqueue_failed:ReadTimeout')
    monkeypatch.setattr(module, '_send_initial_sms_from_payload', fail)
    response = TestClient(module.app).post('/internal/send-initial-sms',
        headers={'Authorization': 'Bearer secret-token'},
        json=dict(row=12, phone='555-111-2212', first='Alex', address='123 Main'))
    assert response.status_code == 502
    queue = module._get_initial_sms_queue_ws()
    saved = queue.get_all_values()[-1]
    assert saved[4] == 'pending'
    assert datetime.fromisoformat(saved[5]) > datetime.now(timezone.utc)
    assert json.loads(saved[3])['stable_id'] == module._row_value(sheet.rows[12], 27)
