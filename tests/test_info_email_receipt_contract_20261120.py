"""No-network receipt gating for the approved automatic information-email path."""
from types import SimpleNamespace

import pytest

from test_internal_sms_endpoint import FakeSendResult, _import_webhook_server
from test_sms_approved_reply_revision import chatbot


@pytest.mark.parametrize('receipt', [
    {'ok': False, 'sent_at': '2026-11-20T18:00:00Z'},
    {'ok': True},
    {'ok': True, 'sent_at': ''},
])
def test_missing_or_failed_email_receipt_never_counts_as_success(monkeypatch, receipt):
    module, _, _ = _import_webhook_server(monkeypatch, sender_result=FakeSendResult())
    calls = []
    def post(url, **kwargs):
        calls.append((url, kwargs))
        return SimpleNamespace(raise_for_status=lambda: None, json=lambda: receipt)
    monkeypatch.setattr(module.requests, 'post', post)
    with pytest.raises(RuntimeError, match='receipt unavailable'):
        module._sms_send_requested_info_email(2, '5550101001', 'agent@example.test', 'offline-1')
    assert len(calls) == 1


def test_exact_email_receipt_is_required_before_the_sms_ack(monkeypatch):
    module, _, _ = _import_webhook_server(monkeypatch, sender_result=FakeSendResult())
    calls = []
    def post(url, **kwargs):
        calls.append((url, kwargs))
        return SimpleNamespace(raise_for_status=lambda: None,
            json=lambda: {'ok': True, 'sent_at': '2026-11-20T18:00:00Z'})
    monkeypatch.setattr(module.requests, 'post', post)
    module._sms_send_requested_info_email(2, '5550101001', 'agent@example.test', 'offline-1')
    assert len(calls) == 1
    assert calls[0][1]['json']['email'] == 'agent@example.test'
    assert calls[0][1]['json']['request_id'] == 'offline-1'
    assert calls[0][1]['timeout'] == 45


@pytest.mark.parametrize('fields,message', [
    ({'human_override': 'TRUE'}, 'Please email me information at agent@example.test'),
    ({'handoff_flag': 'TRUE', 'ai_state': 'handoff'}, 'Please email me information at agent@example.test'),
    ({'auto_reply_count': '3'}, 'Please email me information at agent@example.test'),
    ({}, 'STOP. Do not email me at agent@example.test'),
])
def test_info_request_cannot_bypass_takeover_optout_or_reply_cap(chatbot, monkeypatch, fields, message):
    calls = []
    chatbot.set(**fields)
    monkeypatch.setattr(chatbot.module, '_sms_send_requested_info_email', lambda *args: calls.append(args))
    result = chatbot.receive(message)
    assert result['should_reply'] is False
    assert result['reply_text'] == ''
    assert calls == []
