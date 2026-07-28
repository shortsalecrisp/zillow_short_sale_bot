from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
UNIFIED = (ROOT / "apps_script" / "zz_unified_post.js").read_text()


def test_final_receipt_accepts_only_the_known_form_decoded_plus_change():
    assert "function pendingSmsReplyMatchesFormDecoded_" in UNIFIED
    assert 'String(pendingValue || "").indexOf("+") === -1' in UNIFIED
    assert '.replace(/\\+/g, " ")' in UNIFIED
    assert "pendingSmsReplyMatchesFormDecoded_(rows[i][5], replyText)" in UNIFIED


def test_form_decoded_fallback_requires_all_immutable_identifiers():
    fallback = UNIFIED.index("pendingSmsReplyMatchesFormDecoded_(rows[i][5], replyText)")
    guard = UNIFIED.rfind("if (requestId &&", 0, fallback)
    assert guard >= 0
    guarded_source = UNIFIED[guard:fallback]
    assert "messageId &&" in guarded_source
    assert "phone &&" in guarded_source
    assert "replyText &&" in guarded_source
    assert 'String(rows[i][2] || "") === requestId' in guarded_source
    assert 'String(rows[i][3] || "") === messageId' in guarded_source
    assert "normalizePhone_(rows[i][4]) === phone" in guarded_source
