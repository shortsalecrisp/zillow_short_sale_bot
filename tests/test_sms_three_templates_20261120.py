"""Owner-approved copy parity without network, generation or outreach."""
import ast
import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from test_internal_sms_endpoint import FakeSendResult, _import_webhook_server, _source_sms_template

ROOT = Path(__file__).resolve().parents[1]
APPROVED = json.loads((ROOT / 'tests/fixtures/sms_approved_opener.json').read_text())


def test_initial_is_exact_and_only_first_name_is_interpolated():
    template = _source_sms_template('SMS_TEMPLATE')
    assert template == APPROVED['template']
    for first in ('Alex', 'Anne-Marie', "O'Neil"):
        assert template.format(first=first, address='5000 Example Lane') == APPROVED['template'].replace('{first}', first)
    assert set(template) <= {chr(n) for n in range(128)}


def test_fee_constant_is_exact():
    for statement in ast.parse((ROOT / 'webhook_server.py').read_text()).body:
        if isinstance(statement, ast.Assign) and any(isinstance(t, ast.Name) and t.id == 'SMS_SPECIFIC_FEE_REPLY' for t in statement.targets):
            assert ast.literal_eval(statement.value) == APPROVED['first_fee_reply']
            break
    else:
        raise AssertionError('fee constant missing')


def test_initial_property_binding_uses_fresh_sheet_not_payload(monkeypatch):
    module, sheet, sender = _import_webhook_server(monkeypatch, sender_result=FakeSendResult())
    original = module._enqueue_initial_sms_via_tasker_outbox
    captured = []

    def capture(**kwargs):
        captured.append(kwargs)
        return original(**kwargs)

    monkeypatch.setattr(module, '_enqueue_initial_sms_via_tasker_outbox', capture)
    result = TestClient(module.app).post('/internal/send-initial-sms',
        headers={'authorization': 'Bearer secret-token'},
        json={'row': 12, 'phone': '555-111-2212', 'address': 'Wrong injected address'})
    assert result.status_code == 200
    assert captured[0]['listing_address'] == sheet.rows[12][4]
    assert captured[0]['listing_address'] != 'Wrong injected address'
    assert captured[0]['message'] == APPROVED['template'].format(first='Alex')


@pytest.mark.parametrize('question', ['What is your fee?', 'How much does it cost?', 'Who pays you?', 'How do you get paid?', 'Does the seller pay anything?'])
def test_first_fee_copy_and_receipt_recognition_match_apps(monkeypatch, question):
    module, _, sender = _import_webhook_server(monkeypatch, sender_result=FakeSendResult())
    decision = module._sms_fee_decision({}, question)
    assert decision['reply_text'] == APPROVED['first_fee_reply']
    assert decision['response_id'] == 'fee_specific'
    assert module._sms_is_specific_fee_reply(decision['reply_text'])
    assert module._sms_is_specific_fee_reply('I work nationwide. ' + decision['reply_text'] + ' Can we talk?')
    assert sender.calls == []


def test_legacy_fee_receipt_and_new_fee_repeat_still_block(monkeypatch):
    module, _, sender = _import_webhook_server(monkeypatch, sender_result=FakeSendResult())
    legacy = "My fee is $5,000, paid by the buyer at closing only if the deal closes. There's no service fee to you or the seller, and I don't take anything from your commission."
    assert module._sms_is_specific_fee_reply(legacy)
    for text in (legacy, APPROVED['first_fee_reply']):
        row = {'history_json': json.dumps([{'role': 'assistant', 'text': text, 'receipt_id': 'offline', 'response_id': 'fee_specific'}])}
        decision = module._sms_fee_decision(row, 'How much is your fee?')
        assert decision['block_reply'] is True
        assert decision['handoff_needed'] is True
    assert sender.calls == []
