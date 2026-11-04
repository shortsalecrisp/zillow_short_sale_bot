from test_internal_sms_endpoint import _import_webhook_server, FakeSendResult


def test_under_contract_status_update_is_not_existing_help(monkeypatch):
    module, _sheet, _sender = _import_webhook_server(monkeypatch, sender_result=FakeSendResult())
    decision = module._sms_fast_decision(
        {}, "This property is going under contract. Need to change status on MLS my apologies"
    )
    assert decision["reply_text"] == module.SMS_LISTING_STATUS_UPDATE_REPLY
    assert decision["lead_status"] == "R"
    assert decision["handoff_needed"] is False
    assert "without inferring existing short-sale help" in decision["reason"]


def test_explicit_voice_message_request_is_silent_human_review(monkeypatch):
    module, _sheet, _sender = _import_webhook_server(monkeypatch, sender_result=FakeSendResult())
    decision = module._sms_fast_decision(
        {},
        "I already have help. Can you send me a voice text and let me know how you get paid?",
    )
    assert decision["reply_text"] == ""
    assert decision["block_reply"] is True
    assert decision["handoff_needed"] is True
    assert decision["alert_needed"] is True
    assert decision["handoff_type"] == "VOICE MESSAGE REQUEST"
    assert decision["call_booking_status"] == "interested_no_call"
    assert decision["callback_time"] == ""


def test_existing_help_and_ordinary_text_requests_keep_their_prior_routes(monkeypatch):
    module, _sheet, _sender = _import_webhook_server(monkeypatch, sender_result=FakeSendResult())
    covered = module._sms_fast_decision({}, "I already have a negotiator handling this file.")
    assert covered["reply_text"] == module.SMS_ALREADY_HAS_HELP_REPLY
    assert covered["lead_status"] == "H"
    ordinary = module._sms_fast_decision({}, "Can you text me your website?")
    assert ordinary["handoff_type"] != "VOICE MESSAGE REQUEST"
