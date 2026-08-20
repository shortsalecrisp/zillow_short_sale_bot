from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CHATBOT = (ROOT / "apps_script" / "sms_chatbot.js").read_text()


def test_apps_script_preserves_explicit_weekday_callback_through_reply_cap():
    assert "function isExplicitDayOrDateCallbackSignal_" in CHATBOT
    assert "function extractScheduledCallbackReference_" in CHATBOT
    assert '[HEADERS.call_booking_status]: "scheduled_callback"' in CHATBOT
    assert '[HEADERS.callback_requested]: "yes"' in CHATBOT
    assert '[HEADERS.callback_time]: callbackTime' in CHATBOT
    assert "const ruleResult = applyFastRules_(inboundText, currentRowObj);" in CHATBOT
    assert "ruleResult.handoff_needed || ruleResult.needs_review || ruleResult.alert_needed" in CHATBOT


def test_apps_script_embeds_weekday_callback_positive_and_negative_controls():
    assert "Feel free to reach out to me Monday. Today isn't a good day" in CHATBOT
    assert "Not tomorrow, please call me Monday" in CHATBOT
    assert "Please don't call me Monday" in CHATBOT
    assert "I have an open house Monday" in CHATBOT
    assert "Call the lender Monday" in CHATBOT
    assert "Let's set up a time to talk tomorrow. Let me know what time works best for you." in CHATBOT
    assert "Let's talk to the lender tomorrow" in CHATBOT
