from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CHATBOT = (ROOT / "apps_script" / "sms_chatbot.js").read_text()


def test_apps_script_suppresses_same_topic_not_short_sale_continuations_before_generic_closeout():
    assert "function isPostCloseoutNotShortSaleContinuation_" in CHATBOT
    assert "Same-topic continuation after not-short-sale closeout; no additional reply needed" in CHATBOT
    assert CHATBOT.index("isPostCloseoutNotShortSaleContinuation_(inboundText, currentRowObj)") < CHATBOT.index(
        "if (!ruleResult.matched && !hasFeeQuestion && isNotShortSaleSignal_(inboundText))"
    )
    assert "Can you help with probate?" not in CHATBOT


def test_apps_script_closes_short_sale_source_challenges_with_only_the_approved_apology():
    assert "function isShortSaleSourceQuestion_" in CHATBOT
    assert '"what would make you think it was"' in CHATBOT
    assert '"what made you think it was"' in CHATBOT
    assert '"what gave you the impression"' in CHATBOT
    assert 'return "I\'m sorry, I thought I saw it in the listing but I may have misread it.";' in CHATBOT
    assert 'reason: "Agent challenged the short-sale premise; apologized and closed out"' in CHATBOT
    assert "notice of default or lis pendens" not in CHATBOT
    assert "initial message identified" not in CHATBOT
    assert "verified lender, payoff, or lien information" not in CHATBOT


def test_apps_script_relationship_only_rule_closes_as_o_without_handoff():
    assert "function isRelationshipOnlyAfterExistingCoverageSignal_" in CHATBOT
    assert '[HEADERS.mailshake_status]: "O"' in CHATBOT
    assert '[HEADERS.ai_state]: "done"' in CHATBOT
    assert '[HEADERS.call_booking_status]: "warm_future_interest"' in CHATBOT
    assert 'reason: "Current file already covered; relationship left open without sales follow-up"' in CHATBOT
    assert CHATBOT.index("isRelationshipOnlyAfterExistingCoverageSignal_(inboundText, currentRowObj)") < CHATBOT.index(
        "if (!ruleResult.matched && !hasFeeQuestion && isNotShortSaleSignal_(inboundText))"
    )
    assert "Ill definitely keep your information for future short sale opportunities" in CHATBOT


def test_apps_script_future_buyer_recontact_closes_warm_without_takeover():
    assert "function isFutureBuyerRecontactSignal_" in CHATBOT
    assert "function buildFutureBuyerRecontactReply_" in CHATBOT
    assert "So let you know when I eventually get a buyer?" in CHATBOT
    assert "Agent will reconnect after securing a buyer; warm future interest closed without takeover" in CHATBOT


def test_apps_script_answers_buyer_provision_question_without_rejection_closeout():
    assert "function isBuyerProvisionQuestionSignal_" in CHATBOT
    assert "function buildBuyerProvisionClarificationReply_" in CHATBOT
    assert 'const buyerProvision = applyFastRules_("So you bring the buyer?!", baseRow);' in CHATBOT
    assert '"buyer_provision_question_gets_scope_clarification"' in CHATBOT
    assert 'sanitizeReplyBuyerOffer_(buyerProvision.reply_text) === buyerProvision.reply_text' in CHATBOT
    assert "No, I don't bring the buyer. I just handle the processing with the bank." in CHATBOT


def test_apps_script_substantive_repeat_routes_to_handoff():
    assert '"SUBSTANTIVE QUESTION FOLLOW-UP"' in CHATBOT
    assert "Agent asked a new substantive question after a similar prior answer" in CHATBOT
    assert "How is the buyer going to pay if they are losing money?" in CHATBOT


def test_apps_script_call_interest_and_company_questions_outrank_closeout():
    assert "const hasPhoneCallInterest = isPhoneCallInterestSignal_(inboundText);" in CHATBOT
    assert "const hasPresentServiceInterest = isPresentServiceInterestSignal_(inboundText);" in CHATBOT
    assert "const hasCompanyIdentityQuestion = isCompanyIdentityQuestionSignal_(inboundText);" in CHATBOT
    assert "Agent expressed phone-call interest after a prior closeout" in CHATBOT
    assert "Answered company identity directly while preserving prior closeout" in CHATBOT
    assert "I would be interested to have a call to see how your services differ from theirs." in CHATBOT
    assert "I already have an attorney. What company are you so I can let my attorney know?" in CHATBOT
    assert CHATBOT.index('String(currentRowObj[HEADERS.human_override] || "").toUpperCase() === "TRUE"') < CHATBOT.index(
        "(hasPhoneCallInterest || hasPresentServiceInterest) && isClosedMarketingConversation_(currentRowObj)"
    )
    assert CHATBOT.index('String(currentRowObj[HEADERS.human_override] || "").toUpperCase() === "TRUE"') < CHATBOT.index(
        "hasCompanyIdentityQuestion &&"
    )


def test_apps_script_title_company_service_info_and_compliance_rules_are_deterministic():
    assert "function isTitleCompanyRoleConfusionSignal_" in CHATBOT
    assert "function buildTitleCompanyRoleClarificationReply_" in CHATBOT
    assert "Crisp isn't a title company" in CHATBOT
    assert "function hasServiceInfoRequestContext_" in CHATBOT
    assert "function buildServiceInfoEmailAcknowledgement_" in CHATBOT
    assert "I have your email for the additional information" in CHATBOT
    assert "function isComplianceOrLicensingQuestionSignal_" in CHATBOT
    assert '"COMPLIANCE / LICENSING QUESTION"' in CHATBOT
    assert CHATBOT.index("isComplianceOrLicensingQuestionSignal_(t)") < CHATBOT.index(
        "const priorityQuestion = buildPriorityQuestionDecisionV3_"
    )
