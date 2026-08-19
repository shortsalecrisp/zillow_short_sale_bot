from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
OUTBOX = (ROOT / "apps_script" / "sms_outbox.js").read_text(encoding="utf-8")
UNIFIED = (ROOT / "apps_script" / "zz_unified_post.js").read_text(encoding="utf-8")


def test_v10_routes_are_registered():
    for action in (
        "enqueue_incoming_sms",
        "claim_pending_send",
        "send_started",
        "install_outbox_triggers",
        "outbox_status",
        "reply_sent",
        "sms_send_failed",
    ):
        assert f"{action}: true" in UNIFIED


def test_inbound_queue_and_outbox_have_leases_and_durable_states():
    for marker in (
        '"sms_inbound_queue"',
        '"sms_pending_sends"',
        '"queued"',
        '"processing"',
        '"claimed"',
        '"send_started"',
        '"sent"',
        '"superseded"',
        '"uncertain"',
        '"lease_token"',
        '"lease_until"',
        '"worker_id"',
    ):
        assert marker in OUTBOX


def test_claim_revalidates_latest_crm_message_before_send():
    assert "getPendingSmsStaleReason_" in OUTBOX
    assert "A newer inbound message exists" in OUTBOX
    assert "Human takeover is active" in OUTBOX
    assert "Latest inbound text changed" in OUTBOX
    assert "Older ShortSaleLeads layouts do not have last_inbound_text" in OUTBOX
    assert "inboundText && currentInboundText" in OUTBOX


def test_stale_text_guard_uses_same_case_insensitive_normalization_on_both_sides():
    assert 'function normalizePendingSmsInboundText_(value)' in OUTBOX
    assert 'normalizeWhitespace_(String(value || "")).toLowerCase()' in OUTBOX
    assert 'var inboundText = normalizePendingSmsInboundText_(outboxRow[6]);' in OUTBOX
    assert 'normalizePendingSmsInboundText_(rowObj[HEADERS.last_inbound_text])' in OUTBOX
    assert 'function testPendingSmsStaleTextNormalization_()' in OUTBOX


def test_watchdog_recovers_claims_but_does_not_blindly_resend_uncertain_sms():
    assert 'status === "claimed"' in OUTBOX
    assert 'setValue("queued")' in OUTBOX
    assert 'status === "send_started"' in OUTBOX
    assert 'setValue("uncertain")' in OUTBOX
    assert "SMS SEND RESULT UNCERTAIN" in OUTBOX


def test_worker_and_watchdog_triggers_are_self_installed():
    assert "processSmsInboundQueue_: 1" in OUTBOX
    assert "smsOutboxWatchdog_: 5" in OUTBOX


def test_transport_retries_are_idempotent():
    assert "buildPendingSmsClaimResponse_" in OUTBOX
    assert "A lost HTTP response must return the same active lease" in OUTBOX
    assert 'activeStatus !== "claimed"' in OUTBOX
    assert 'String(match.values[1] || "") === "send_started"' in OUTBOX
    assert "receiptCorrelation.already_sent" in UNIFIED


def test_pending_send_claim_treats_lock_contention_as_retryable():
    claim_start = OUTBOX.index("function claimPendingSmsSendV10_(body)")
    claim_end = OUTBOX.index("function buildPendingSmsClaimResponse_", claim_start)
    claim_source = OUTBOX[claim_start:claim_end]

    assert "lock.tryLock(3000)" in claim_source
    assert "lock.waitLock(10000)" not in claim_source
    assert "busyPendingSmsClaim_()" in claim_source
    assert 'retryable: true' in OUTBOX
    assert 'retry_after_seconds: 5' in OUTBOX
    assert 'reason: "SMS outbox temporarily busy; retry"' in OUTBOX


def test_receipts_recover_only_with_complete_exact_lease_identity():
    for marker in (
        "findPendingSmsRowByExactLeaseIdentityV10_",
        "findPendingSmsRowIndexByExactLeaseIdentityV10_",
        "Exact lease correlation requires request, message, phone, and lease",
        'correlation_mode: "exact_lease_identity"',
        "testSmsReceiptLeaseIdentity_",
    ):
        assert marker in OUTBOX
    assert "!requestId || !messageId || !phone || !leaseToken" in OUTBOX
    assert 'String(rows[i][10] || "") !== leaseToken' in OUTBOX


def test_reply_history_uses_canonical_pending_text_after_transport_damage():
    assert "canonicalReceiptBody" in UNIFIED
    assert "canonical_reply_text" in UNIFIED
    assert "handleReplySent_(canonicalReceiptBody)" in UNIFIED
    assert "markPendingSmsSendComplete_(canonicalReceiptBody)" in UNIFIED


def test_safe_deployment_probe_exercises_receipt_lease_identity():
    assert "probe.receipt_lease_identity" in UNIFIED
    assert "testSmsReceiptLeaseIdentity_()" in UNIFIED
    assert "probe.stale_text_normalization" in UNIFIED
    assert "testPendingSmsStaleTextNormalization_()" in UNIFIED
