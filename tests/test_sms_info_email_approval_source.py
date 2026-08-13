from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CHATBOT = (ROOT / "apps_script" / "sms_chatbot.js").read_text()


def test_information_request_uses_approval_gated_acknowledgement():
    assert "reply_text: getInfoEmailAcknowledgementReply_()" in CHATBOT
    assert "sendInfoEmailApprovalRequest_(infoEmailData)" in CHATBOT
    assert "INFO_EMAIL_APPROVAL_REQUIRED" in CHATBOT
    assert "An agent requested the short-sale info email. Approval is required before sending." in CHATBOT


def test_spaced_email_local_part_is_compacted_before_validation():
    assert "const spacedMatch = raw.match(" in CHATBOT
    assert 'spacedMatch[1].replace(/\\s+/g, "") + "@" + spacedMatch[2]' in CHATBOT


def test_broad_information_request_phrases_are_recognized():
    assert "/\\bsend (?:me|us) (?:some |more )?(?:info|information)\\b/" in CHATBOT
    assert "/\\b(?:info|information) (?:on|about) (?:your )?(?:services|company)\\b/" in CHATBOT


def test_email_draft_does_not_claim_a_prior_conversation():
    assert "Im glad we got a chance to speak this morning" not in CHATBOT
    assert 'return "Crisp Short Sales - How I Can Help";' in CHATBOT
    assert "I have been handling short sales for over 15 years" in CHATBOT


def test_info_email_acknowledgement_bypasses_phone_only_sanitizer():
    assert (
        "normalizeWhitespace_(text) === normalizeWhitespace_(getInfoEmailAcknowledgementReply_())"
        in CHATBOT
    )
    assert "the downstream approval workflow can recognize and queue the email" in CHATBOT
