from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
OUTBOX = (ROOT / "apps_script" / "sms_outbox.js").read_text(encoding="utf-8")
UNIFIED = (ROOT / "apps_script" / "zz_unified_post.js").read_text(encoding="utf-8")


def test_v10_routes_are_registered():
    for action in (
        "enqueue_incoming_sms",
        "enqueue_initial_sms",
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
    assert "A newer substantive inbound message exists" in OUTBOX


def test_inbound_queue_coalesces_rapid_same_phone_bubbles_before_classification():
    assert "fragmentIndexes" in OUTBOX
    assert 'fragmentCreatedAt - previousFragmentAt > 20000' in OUTBOX
    assert 'now - newestCreatedAt < 20000' in OUTBOX
    assert 'blockValues[fragmentOffset][1] = "coalesced"' in OUTBOX
    assert 'filter(Boolean).join(" ")' in OUTBOX
    assert "SMS_INBOUND_QUEUE_HEADERS_.length" in OUTBOX
    assert ").setValues(blockValues)" in OUTBOX


def test_inbound_retry_can_recover_after_crm_commit_before_decision_snapshot():
    chatbot = (ROOT / "apps_script" / "sms_chatbot.js").read_text(encoding="utf-8")
    assert "recovery_retry: claim.attempts > 1" in OUTBOX
    assert "const recoveryRetry = body && body.recovery_retry === true" in chatbot
    assert "!recoveringSameMessage" in chatbot


def test_handoff_email_uses_durable_idempotent_outbox_and_retry_trigger():
    assert "SMS_HANDOFF_EMAIL_HEADERS_" in OUTBOX
    assert "function queueHandoffEmailV11_" in OUTBOX
    assert "function drainHandoffEmailOutboxV11_" in OUTBOX
    assert "function reconcileUncertainHandoffEmailV11_" in OUTBOX
    assert "function wasHandoffEmailSentV11_" in OUTBOX
    assert 'getSheetByName("sms_handoff_email_outbox")' in OUTBOX
    assert "Utilities.DigestAlgorithm.SHA_256" in OUTBOX
    assert "drainHandoffEmailOutboxV11_: 1" in OUTBOX
    queue_source = OUTBOX.split("function queueHandoffEmailV11_", 1)[1].split(
        "function drainHandoffEmailOutboxV11_", 1
    )[0]
    drain_source = OUTBOX.split("function drainHandoffEmailOutboxV11_", 1)[1].split(
        "function getPendingSmsHeaders_", 1
    )[0]
    assert "LockService.getScriptLock()" in queue_source
    assert "lock_fallback: true" in queue_source
    assert '"uncertain"' in queue_source
    assert "activeIds" in drain_source
    assert "Claim expired; delivery outcome unknown" in drain_source
    assert "GmailApp.search" in drain_source
    assert 'status === "reconciling"' in drain_source
    assert 'status === "claimed" && (!leaseUntil || leaseUntil < Date.now())' in drain_source
    assert 'claimedEmailRow[1] = "claimed"' in drain_source
    assert ".setValues([claimedEmailRow])" in drain_source
    assert 'reconcilingRow[1] = "reconciling"' in drain_source
    assert ".setValues([reconcilingRow])" in drain_source
    assert "Email sent; receipt write failed" in drain_source
    chatbot = (ROOT / "apps_script" / "sms_chatbot.js").read_text()
    handoff_sender = chatbot.split("function sendHandoffEmail_", 1)[1].split(
        "function shouldSendInfoEmail_", 1
    )[0]
    assert "queueHandoffEmailV11_" in handoff_sender
    assert "MailApp.sendEmail" not in handoff_sender
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
    assert "drainPendingSmsControlEventsV11_: 1" in OUTBOX


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


def test_claim_transitions_are_atomic_and_reclaim_missing_leases():
    inbound_start = OUTBOX.index("function claimQueuedSmsInbound_()")
    inbound_end = OUTBOX.index("function completeQueuedSmsInbound_", inbound_start)
    inbound_source = OUTBOX[inbound_start:inbound_end]
    pending_start = OUTBOX.index("function claimPendingSmsSendV10_(body)")
    pending_end = OUTBOX.index("function buildPendingSmsClaimResponse_", pending_start)
    pending_source = OUTBOX[pending_start:pending_end]

    assert 'status === "processing" && (!leaseUntil || leaseUntil < now)' in inbound_source
    assert 'claimedInboundRow[1] = "processing"' in inbound_source
    assert ".setValues([claimedInboundRow])" in inbound_source
    assert 'sheet.getRange(sheetRow, 2).setValue("processing")' not in inbound_source

    assert 'status === "claimed" && (!leaseUntil || leaseUntil < now)' in pending_source
    assert 'claimedPendingRow[1] = "claimed"' in pending_source
    assert ".setValues([claimedPendingRow])" in pending_source
    assert 'sheet.getRange(sheetRow, 2).setValue("claimed")' not in pending_source


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


def test_initial_outreach_uses_durable_outbox_and_marks_crm_only_on_tasker_receipt():
    server = (ROOT / "webhook_server.py").read_text(encoding="utf-8")
    scheduler = (ROOT / "bot_min.py").read_text(encoding="utf-8")
    assert '"action": "enqueue_initial_sms"' in server
    assert '"status": "queued"' in server
    send_source = server.split("def _send_initial_sms_from_payload", 1)[1].split("def ", 1)[0]
    assert "send_with_diagnostics" not in send_source
    assert "_mark_initial_sms_sent" not in send_source
    assert "function enqueueInitialSmsV13_" in OUTBOX
    assert "function applyInitialSmsReceiptV13_" in OUTBOX
    assert 'send_kind === "initial_outreach"' in UNIFIED
    assert 'send_kind: String(rows[matchIndex][6]' in UNIFIED
    send_sms_source = scheduler.split("def send_sms(", 1)[1].split("def _within_initial_hours", 1)[0]
    assert '"action": "enqueue_initial_sms"' in send_sms_source
    assert "if not follow_up:" in send_sms_source
    assert "mark_sent(" not in send_sms_source


def test_reply_sent_crm_writeback_is_idempotent_by_receipt_identity():
    chatbot = (ROOT / "apps_script" / "sms_chatbot.js").read_text(encoding="utf-8")
    start = chatbot.index("function handleReplySent_(body)")
    end = chatbot.index("function handleManualReplySent_", start)
    source = chatbot[start:end]

    assert "buildReplyReceiptId_" in source
    assert "entry.receipt_id" in source
    assert 'receipt_id: receiptId' in source
    assert "SMS_REPLY_RECEIPT_BUSY" in source
    assert "SMS_REPLY_RECEIPT_LOCK_BUSY" in source
    assert ".setValues([rowValues])" in source
    assert "appendHistory_(" not in source


def test_human_control_paths_share_the_conversation_lease():
    chatbot = (ROOT / "apps_script" / "sms_chatbot.js").read_text(encoding="utf-8")
    manual_start = chatbot.index("function handleManualReplySent_(body)")
    override_start = chatbot.index("function markOverride_(body)", manual_start)
    language_start = chatbot.index("function normalizeLanguageSignalText_", override_start)
    manual_source = chatbot[manual_start:override_start]
    override_source = chatbot[override_start:language_start]

    for source in (manual_source, override_source):
        assert "acquireSmsConversationLease_" in source
        assert "releaseSmsConversationLease_" in source
        assert "queuePendingSmsControlEventV11_" in source
        assert "queued: true" in source
    assert "Manual reply takeover queued" in manual_source
    assert "Override queued" in override_source


def test_pending_human_control_is_durable_and_suppresses_outbox_work():
    chatbot = (ROOT / "apps_script" / "sms_chatbot.js").read_text(encoding="utf-8")
    for marker in (
        "SMS_PENDING_CONTROL_V11_",
        "function drainPendingSmsControlEventsV11_()",
        "control_event_id",
        "hasPendingSmsTakeoverV11_(phone)",
    ):
        assert marker in chatbot
    assert 'reason: "Manual takeover is pending"' in OUTBOX
    assert "hasPendingSmsTakeoverV11_(phone)" in OUTBOX


def test_safe_deployment_probe_exercises_receipt_lease_identity():
    assert "probe.receipt_lease_identity" in UNIFIED
    assert "testSmsReceiptLeaseIdentity_()" in UNIFIED
    assert "probe.stale_text_normalization" in UNIFIED
    assert "testPendingSmsStaleTextNormalization_()" in UNIFIED
