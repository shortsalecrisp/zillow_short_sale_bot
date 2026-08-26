from pathlib import Path


CHATBOT = (Path(__file__).resolve().parents[1] / "apps_script" / "sms_chatbot.js").read_text()


def test_deferred_hot_lead_alert_says_no_action_now():
    assert '"DEFERRED HOT LEAD - NO ACTION NOW"' in CHATBOT
    assert "No reply or callback is requested now" in CHATBOT
    assert "wait for re-engagement" in CHATBOT
