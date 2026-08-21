from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CHATBOT = (ROOT / "apps_script" / "sms_chatbot.js").read_text()


def test_information_request_uses_approval_gated_acknowledgement():
    assert "reply_text: acknowledgementReply" in CHATBOT
    assert "sendInfoEmailApprovalRequest_(infoEmailData)" in CHATBOT
    assert "INFO_EMAIL_APPROVAL_REQUIRED" in CHATBOT
    assert "An agent requested the short-sale info email. Approval is required before sending." in CHATBOT
    assert "Absolutely, I'll email you more information shortly. Thanks for sending your email." in CHATBOT


def test_info_email_workflow_is_flag_driven_not_reply_copy_driven():
    start = CHATBOT.index("function shouldSendInfoEmail_")
    end = CHATBOT.index("function getInfoEmailAcknowledgementReply_", start)
    source = CHATBOT[start:end]

    assert "ruleResult.send_info_email" in source
    assert "ruleResult.info_email_to" in source
    assert "decision.reply_text" not in source
    assert "getInfoEmailAcknowledgementReply_" not in source


def test_info_email_approval_recovery_route_and_diagnostics_exist():
    unified = (ROOT / "apps_script" / "zz_unified_post.js").read_text()
    assert "request_info_email_approval: true" in unified
    assert 'if (action === "request_info_email_approval")' in unified
    assert "requestInfoEmailApprovalForRow_(body)" in unified
    assert "function requestInfoEmailApprovalForRow_(body)" in CHATBOT
    assert 'appendSmsDebugLog_("info_email_approval_requested"' in CHATBOT
    assert 'appendSmsDebugLog_("info_email_approval_failed"' in CHATBOT


def test_spaced_email_local_part_is_compacted_before_validation():
    assert "const spacedMatch = raw.match(" in CHATBOT
    assert 'spacedMatch[1].replace(/\\s+/g, "") + "@" + spacedMatch[2]' in CHATBOT


def test_broad_information_request_phrases_are_recognized():
    assert "/\\bsend (?:me|us) (?:some |more )?(?:info|information)\\b/" in CHATBOT
    assert "/\\b(?:info|information) (?:on|about) (?:your )?(?:services|company)\\b/" in CHATBOT


def test_email_draft_does_not_claim_a_prior_conversation():
    assert "Im glad we got a chance to speak this morning" not in CHATBOT
    assert 'return ("Crisp Short Sales - " + property).slice(0, 160);' in CHATBOT
    assert "I have been handling short sales for over 15 years" in CHATBOT


def test_info_email_uses_mobile_safe_approval_gateway():
    assert '"https://crisp-voice-bot.onrender.com/info-email/approve"' in CHATBOT
    assert '"?target=" + encodeURIComponent(baseUrl)' in CHATBOT
    assert '"&id=" + encodeURIComponent(approvalId)' in CHATBOT


def test_info_email_rehydrates_personalization_from_crm():
    assert "function hydrateInfoEmailDataFromSheet_(data)" in CHATBOT
    assert "const hydratedData = hydrateInfoEmailDataFromSheet_(data);" in CHATBOT
    assert "const data = savedData ? hydrateInfoEmailDataFromSheet_(savedData) : null;" in CHATBOT
    assert 'fill("first_name", getCanonicalFirstName_(best));' in CHATBOT
    assert 'fill("listing_address", best[HEADERS.listing_address]);' in CHATBOT


def test_info_email_greeting_does_not_fall_back_to_hi_there():
    assert 'const greeting = firstName ? "Hi " + firstName + "," : "Hi,";' in CHATBOT
    assert 'return "there";' not in CHATBOT[CHATBOT.index("function getAgentInfoEmailFirstName_") :]


def test_info_email_acknowledgement_bypasses_phone_only_sanitizer():
    assert (
        "normalizeWhitespace_(text) === normalizeWhitespace_(getInfoEmailAcknowledgementReply_())"
        in CHATBOT
    )
    assert "the downstream approval workflow can recognize and queue the email" in CHATBOT


def test_explicitly_provided_email_replaces_stale_crm_email():
    assert (
        'if (ruleResult.info_email_to && isValidEmailAddress_(ruleResult.info_email_to)) {'
        in CHATBOT
    )
    assert (
        'isValidEmailAddress_(ruleResult.info_email_to) && !String(currentRowObj[HEADERS.email] || "").trim()'
        not in CHATBOT
    )


def test_decline_plus_information_request_is_a_warm_o_opportunity():
    assert "function isDeclineWithInfoRequestSignal_(text, rowObj)" in CHATBOT
    assert 'lead_status: isWarmInfoOpportunity ? "O" : "Y"' in CHATBOT
    assert 'conversation_done: isWarmInfoOpportunity' in CHATBOT
    assert 'updates[HEADERS.call_booking_status] = "warm_future_interest"' in CHATBOT
    assert 'candidate === "O"' in CHATBOT


def test_information_request_bypasses_early_decline_closeout_and_syncs_exact_email_rows():
    assert "const hasInformationRequest = isEmailRequestSignal_(inboundText)" in CHATBOT
    assert any(
        "!hasInformationRequest" in line and "isAlreadyHandledSignal_(inboundText)" in line
        for line in CHATBOT.splitlines()
    )
    assert any(
        "!hasInformationRequest" in line and "isClearNoSignal_(inboundText)" in line
        for line in CHATBOT.splitlines()
    )
    assert "function syncWarmInfoOpportunityRows_(sheet, email, rowObj, latestInbound)" in CHATBOT
    assert "if (rowEmail === normalizedEmail) updateRowFields_(sheet, item.row, updates);" in CHATBOT
