import pytest
from test_internal_sms_endpoint import _import_webhook_server, FakeSendResult


@pytest.mark.parametrize("message,closed", [
    ("I already have help However, do you service Virginia too?", True),
    ("I already have help. Can you cover VA?", True),
    ("I handle it myself. Do you work in North Carolina?", False),
    ("No thanks. Do you serve Texas?", True),
    ("Do you service all states?", False),
    ("Do you cover nationwide?", False),
])
def test_service_area_question_precedes_coverage(monkeypatch, message, closed):
    module, _, _ = _import_webhook_server(monkeypatch, sender_result=FakeSendResult())
    decision = module._sms_fast_decision({}, message)
    assert "based in Atlanta and work nationwide" in decision["reply_text"]
    assert decision["handoff_needed"] is False
    assert decision["conversation_done"] is closed
    assert decision["lead_status"] == ("O" if closed else "Y")


@pytest.mark.parametrize("message", [
    "I already have help. Your service sounds useful.",
    "I handle it myself and cover Virginia.",
    "Do you work in Equator?",
    "Do you cover the buyer fee?",
    "Do you service mortgages?",
])
def test_service_or_capability_words_do_not_imply_location(monkeypatch, message):
    module, _, _ = _import_webhook_server(monkeypatch, sender_result=FakeSendResult())
    decision = module._sms_fast_decision({}, message)
    assert "work nationwide" not in str((decision or {}).get("reply_text", ""))
