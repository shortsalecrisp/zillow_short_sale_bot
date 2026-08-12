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


def test_apps_script_answers_short_sale_source_follow_up_without_inventing_data_details():
    assert "function isShortSaleSourceQuestion_" in CHATBOT
    assert '"what would make you think it was"' in CHATBOT
    assert '"what made you think it was"' in CHATBOT
    assert (
        'reply_text: "I thought i saw in the listing that it said it was a short sale. '
        'My mistake if i misread that. Thanks"'
    ) in CHATBOT
    assert "notice of default or lis pendens" not in CHATBOT


def test_apps_script_relationship_only_rule_closes_as_o_without_handoff():
    assert "function isRelationshipOnlyAfterExistingCoverageSignal_" in CHATBOT
    assert '[HEADERS.mailshake_status]: "O"' in CHATBOT
    assert '[HEADERS.ai_state]: "done"' in CHATBOT
    assert '[HEADERS.call_booking_status]: "warm_future_interest"' in CHATBOT
    assert 'reason: "Current file already covered; relationship left open without sales follow-up"' in CHATBOT
    assert CHATBOT.index("isRelationshipOnlyAfterExistingCoverageSignal_(inboundText, currentRowObj)") < CHATBOT.index(
        "if (!hasFeeQuestion && isNotShortSaleSignal_(inboundText))"
    )
    assert "Ill definitely keep your information for future short sale opportunities" in CHATBOT


def test_apps_script_future_buyer_recontact_closes_warm_without_takeover():
    assert "function isFutureBuyerRecontactSignal_" in CHATBOT
    assert "function buildFutureBuyerRecontactReply_" in CHATBOT
    assert "So let you know when I eventually get a buyer?" in CHATBOT
    assert "Agent will reconnect after securing a buyer; warm future interest closed without takeover" in CHATBOT


def test_apps_script_substantive_repeat_routes_to_handoff():
    assert '"SUBSTANTIVE QUESTION FOLLOW-UP"' in CHATBOT
    assert "Agent asked a new substantive question after a similar prior answer" in CHATBOT
    assert "How is the buyer going to pay if they are losing money?" in CHATBOT
