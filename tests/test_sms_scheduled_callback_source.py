from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CHATBOT = (ROOT / "apps_script" / "sms_chatbot.js").read_text()


def test_apps_script_routes_explicit_weekday_callbacks_before_generic_handoff():
    assert "function isExplicitDayOrDateCallbackSignal_" in CHATBOT
    assert "function extractScheduledCallbackReference_" in CHATBOT
    assert '[HEADERS.call_booking_status]: "scheduled_callback"' in CHATBOT
    assert '[HEADERS.callback_requested]: "yes"' in CHATBOT
    assert '[HEADERS.callback_time]: callbackTime' in CHATBOT
    assert CHATBOT.index("if (isSchedulingSignal_(inboundText))") < CHATBOT.index("if (capReached)")


def test_apps_script_embeds_weekday_callback_positive_and_negative_controls():
    assert "Feel free to reach out to me Monday. Today isn't a good day" in CHATBOT
    assert "Not tomorrow, please call me Monday" in CHATBOT
    assert "Please don't call me Monday" in CHATBOT
    assert "I have an open house Monday" in CHATBOT
