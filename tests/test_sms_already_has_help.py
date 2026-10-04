import pytest
from test_internal_sms_endpoint import _import_webhook_server, FakeSendResult


@pytest.mark.parametrize('text', [
    'I already have a negotiator.',
    'We already have a processor handling it.',
    'I have an attorney handling the short sale.',
    'Our title company is handling the short sale.',
])
def test_existing_provider_receives_backup_acknowledgment(monkeypatch, text):
    module, _sheet, sender = _import_webhook_server(monkeypatch, sender_result=FakeSendResult())
    decision = module._sms_fast_decision({}, text)
    assert decision['reply_text'] == module.SMS_ALREADY_HAS_HELP_REPLY
    assert decision['lead_status'] == 'R'
    assert decision['conversation_done'] is True
    assert decision['handoff_needed'] is False
    assert not sender.calls


@pytest.mark.parametrize('text', [
    'No thanks, I already have an attorney.',
    'Not interested, we have a negotiator.',
])
def test_explicit_refusal_keeps_standard_closeout(monkeypatch, text):
    module, _sheet, _sender = _import_webhook_server(monkeypatch, sender_result=FakeSendResult())
    decision = module._sms_fast_decision({}, text)
    assert decision['reply_text'] == module.SMS_STANDARD_CLOSEOUT_REPLY
    assert decision['lead_status'] == 'R'


def test_existing_provider_does_not_replace_fee_answer(monkeypatch):
    module, _sheet, _sender = _import_webhook_server(monkeypatch, sender_result=FakeSendResult())
    decision = module._sms_fast_decision({}, 'I already have an attorney, but what is your fee?')
    assert '$5,000' in decision['reply_text']
    assert decision['reply_text'] != module.SMS_ALREADY_HAS_HELP_REPLY


def test_human_override_prevents_backup_response(monkeypatch):
    module, _sheet, _sender = _import_webhook_server(monkeypatch, sender_result=FakeSendResult())
    decision = module._sms_fast_decision({'human_override':'TRUE'}, 'I already have a negotiator.')
    assert decision['block_reply'] is True
    assert decision['preserve_existing_state'] is True


@pytest.mark.parametrize('text', [
    'Who gets the credit?',
    'Can I receive the $1,000?',
    'Can the referral fee go to my broker?',
    'How is the first file credit applied?',
])
def test_credit_allocation_needs_personal_review(monkeypatch, text):
    module, _sheet, sender = _import_webhook_server(monkeypatch, sender_result=FakeSendResult())
    decision = module._sms_fast_decision({}, text)
    assert decision['block_reply'] is True
    assert decision['handoff_needed'] is True
    assert decision['handoff_type'] == 'FIRST-FILE CREDIT REVIEW'
    assert not sender.calls
