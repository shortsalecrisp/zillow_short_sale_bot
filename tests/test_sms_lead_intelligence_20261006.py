from test_internal_sms_endpoint import _import_webhook_server, FakeSendResult


def test_python_referral_company_info_matches_primary_path(monkeypatch):
    module, _sheet, sender = _import_webhook_server(monkeypatch, sender_result=FakeSendResult())
    decision = module._sms_fast_decision(
        {},
        'Can you send me information about your company? I have a colleague looking for short sale help.',
    )
    assert 'Crisp Short Sales' in decision['reply_text']
    assert 'https://www.crispshortsales.com' in decision['reply_text']
    assert '$5,000' not in decision['reply_text']
    assert decision['handoff_type'] == 'REFERRAL / COMPANY INFO'
    assert decision['handoff_needed'] is True
    assert not sender.calls


def test_python_fee_wording_matches_primary_path(monkeypatch):
    module, _sheet, sender = _import_webhook_server(monkeypatch, sender_result=FakeSendResult())
    decision = module._sms_fast_decision(
        {},
        'What is your fee and what verbiage should I put in the listing and purchase contract?',
    )
    assert '$5,000' in decision['reply_text']
    assert 'Yoni provide the exact contract or listing wording' in decision['reply_text']
    assert decision['handoff_type'] == 'FEE DISCLOSURE WORDING REVIEW'
    assert decision['handoff_needed'] is True
    assert not sender.calls


def test_python_durable_replay_requires_delivery_receipt(monkeypatch):
    module, _sheet, _sender = _import_webhook_server(monkeypatch, sender_result=FakeSendResult())
    text = 'What is your fee?'
    row = {
        'last_inbound_text': text,
        'response_status': text,
        'history_json': (
            '[{"role":"agent","text":"What is your fee?"},'
            '{"role":"assistant","text":"Draft answer with no receipt"}]'
        ),
    }
    assert module._sms_is_durable_handled_duplicate(row, text) is False
    row['history_json'] = (
        '[{"role":"agent","text":"What is your fee?"},'
        '{"role":"assistant","text":"Delivered answer","receipt_id":"receipt-1"}]'
    )
    assert module._sms_is_durable_handled_duplicate(row, text) is True


def test_python_historical_sms_time_and_refusal_do_not_create_callback(monkeypatch):
    module, _sheet, _sender = _import_webhook_server(monkeypatch, sender_result=FakeSendResult())
    inbound = "Hello! Oh I see you did text me at 4am this morning. No thank you!"
    assert module._sms_is_scheduled_callback(inbound) is False
    decision = module._sms_fast_decision({}, inbound)
    assert decision["lead_status"] == "R"
    assert decision["conversation_done"] is True
    assert decision.get("handoff_needed") is False
    assert not decision.get("callback_time")
    assert decision.get("call_booking_status") == "closed_no_interest"


def test_python_scheduled_callback_requires_spoken_call_intent(monkeypatch):
    module, _sheet, _sender = _import_webhook_server(monkeypatch, sender_result=FakeSendResult())
    assert module._sms_is_scheduled_callback("Please call me at 4 pm") is True
    assert module._sms_is_scheduled_callback("Can we talk tomorrow afternoon?") is True
    assert module._sms_is_scheduled_callback("Please text me at 4 pm") is False
    assert module._sms_is_scheduled_callback("Tomorrow at 4 pm works") is False
    assert module._sms_is_scheduled_callback("Feel free to reach out to me Monday") is False
    assert module._sms_is_scheduled_callback("Can you touch base in three weeks time?") is False
