from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CHATBOT = (ROOT / "apps_script" / "sms_chatbot.js").read_text()


def test_apps_script_suppresses_same_topic_not_short_sale_continuations_before_generic_closeout():
    assert "function isPostCloseoutNotShortSaleContinuation_" in CHATBOT
    assert "Same-topic continuation after not-short-sale closeout; no additional reply needed" in CHATBOT
    assert CHATBOT.index("isPostCloseoutNotShortSaleContinuation_(inboundText, currentRowObj)") < CHATBOT.index(
        "if (!hasFeeQuestion && isNotShortSaleSignal_(inboundText))"
    )
    assert "Can you help with probate?" not in CHATBOT


def test_apps_script_relationship_only_rule_closes_as_o_without_handoff():
    assert "function isRelationshipOnlyAfterExistingCoverageSignal_" in CHATBOT
    assert '[HEADERS.mailshake_status]: "O"' in CHATBOT
    assert '[HEADERS.ai_state]: "done"' in CHATBOT
    assert '[HEADERS.call_booking_status]: "warm_future_interest"' in CHATBOT
    assert 'reason: "Current file already covered; relationship left open without sales follow-up"' in CHATBOT
    assert CHATBOT.index("isRelationshipOnlyAfterExistingCoverageSignal_(inboundText, currentRowObj)") < CHATBOT.index(
        "if (!hasFeeQuestion && isNotShortSaleSignal_(inboundText))"
    )
