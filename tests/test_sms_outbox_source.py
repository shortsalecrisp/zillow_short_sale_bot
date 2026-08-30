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


def test_unauthorized_webhook_noise_is_logged_without_per_request_email_alerts():
    assert "function isUnauthorizedError_(err)" in UNIFIED
    assert '/^\\s*(?:Error:\\s*)?Unauthorized\\s*$/i.test(message)' in UNIFIED

    catch_source = UNIFIED.split("} catch (err) {", 1)[1].split(
        "function getUnifiedIgnoredInboundReason_", 1
    )[0]
    unauthorized_pos = catch_source.index("isUnauthorizedError_(err)")
    alert_pos = catch_source.index('sendSystemAlertEmail_("SMS BOT ERROR"')
    assert unauthorized_pos < alert_pos
    assert 'error: "Unauthorized"' in catch_source


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


def test_offer_scope_clarification_can_send_before_approved_handoff_lock():
    assert "approvedOfferScopeReply" in OUTBOX
    assert "isOfferSubmissionConfusionSignal_(inboundText)" in OUTBOX
    assert "!approvedOfferScopeReply && typeof hasPendingSmsTakeoverV11_" in OUTBOX
    assert '!approvedOfferScopeReply && String(rowObj[HEADERS.human_override]' in OUTBOX


def test_inbound_queue_coalesces_rapid_same_phone_bubbles_before_classification():
    assert "fragmentIndexes" in OUTBOX
    assert 'fragmentCreatedAt - previousFragmentAt > 20000' in OUTBOX
    assert 'now - newestCreatedAt < 20000' in OUTBOX
    assert 'blockValues[fragmentOffset][1] = "coalesced"' in OUTBOX
    assert "seenFragmentTexts" in OUTBOX
    assert "if (seenFragmentTexts[fragmentKey]) return false" in OUTBOX
    assert '}).join(" ")' in OUTBOX
    assert "SMS_INBOUND_QUEUE_HEADERS_.length" in OUTBOX
    assert ").setValues(blockValues)" in OUTBOX


def test_inbound_queue_dedupes_same_transport_message_across_different_ids():
    assert "buildSmsInboundTransportFingerprint_" in OUTBOX
    assert 'var transportCacheKey = "sms_inbound_transport_" + transportFingerprint' in OUTBOX
    assert "rowTransportFingerprint === transportFingerprint" in OUTBOX
    assert "cache.put(transportCacheKey, queueId, 600)" in OUTBOX


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


def test_watchdog_recovers_claims_and_retries_one_missing_receipt_before_alerting():
    assert 'status === "claimed"' in OUTBOX
    assert 'setValue("queued")' in OUTBOX
    assert 'status === "send_started"' in OUTBOX
    assert 'setValue("uncertain")' in OUTBOX
    assert "SMS SEND RESULT UNCERTAIN" in OUTBOX
    assert "function recoverMissingSmsReceiptOnce_" in OUTBOX
    assert "function buildAutomaticSmsRetryRowV15_" in OUTBOX
    assert 'Number(source[9] || 0) >= 2' in OUTBOX
    assert "Automatic one-time retry queued after missing Tasker send receipt" in OUTBOX
    assert "function testAutomaticSmsRetryPolicyV15_" in OUTBOX


def test_targeted_uncertain_recovery_requires_exact_identity_and_transport_lock():
    assert "function recoverUncertainSmsSendV15_" in OUTBOX
    assert 'String(row[2] || "") !== requestId' in OUTBOX
    assert 'String(row[1] || "") !== "uncertain"' in OUTBOX
    assert "function clearTransportGeneratedSmsHandoffV15_" in OUTBOX
    assert 'conversation_summary] || "") !== reason' in OUTBOX
    assert "CRM state no longer supports an initial SMS retry" in OUTBOX
    assert "recover_uncertain_send: true" in UNIFIED
    assert 'if (action === "recover_uncertain_send")' in UNIFIED


def test_watchdog_recovers_confirmed_crm_receipt_before_transport_takeover():
    assert "function findConfirmedSmsReplyReceiptInHistory_" in OUTBOX
    assert "function findConfirmedSmsReplyReceiptForPendingRow_" in OUTBOX
    assert "function recordRecoveredSmsReplyReceipt_" in OUTBOX
    assert 'String(later.receipt_id || "").trim()' in OUTBOX
    assert 'normalizePendingSmsInboundText_(later.text) === reply' in OUTBOX
    assert 'setValue("Recovered from confirmed CRM reply receipt")' in OUTBOX
    assert 'if (alertSmsOutboxProblem_(sheet, sheetRow, row, "SMS SEND RESULT UNCERTAIN"))' in OUTBOX
    assert "function testConfirmedSmsReplyReceiptRecovery_" in OUTBOX


def test_worker_and_watchdog_triggers_are_self_installed():
    assert "processSmsInboundQueue_: 1" in OUTBOX
    assert "auditSmsInboundCompletenessV14_: 5" in OUTBOX
    assert "smsOutboxWatchdog_: 5" in OUTBOX
    assert "drainPendingSmsControlEventsV11_: 1" in OUTBOX


def test_inbound_is_persisted_before_best_effort_trigger_maintenance():
    start = OUTBOX.index("function enqueueIncomingSmsV10_")
    end = OUTBOX.index("function ensureSmsOutboxTriggersBestEffortV14_", start)
    source = OUTBOX[start:end]

    assert "installSmsOutboxTriggers_();" not in source
    assert "ensureSmsOutboxTriggersBestEffortV14_();" in source
    fallback_start = source.index("if (!hasLock)")
    fallback_end = source.index("return buildQueuedSmsInboundResponse_", fallback_start)
    fallback_source = source[fallback_start:fallback_end]
    assert fallback_source.index("sheet.appendRow") < fallback_source.index(
        "ensureSmsOutboxTriggersBestEffortV14_();"
    )
    assert source.rindex("sheet.appendRow") < source.rindex("ensureSmsOutboxTriggersBestEffortV14_();")
    assert "Trigger maintenance is intentionally after the durable append" in source
    cached_duplicate = source.index("if (cachedQueueId)")
    assert source.index("ensureSmsOutboxTriggersBestEffortV14_();", cached_duplicate) > cached_duplicate


def test_inbound_route_does_no_debug_or_transport_write_before_durable_enqueue():
    handler_start = UNIFIED.index("function handleUnifiedSmsPost_")
    enqueue_start = UNIFIED.index('if (action === "enqueue_incoming_sms")', handler_start)
    enqueue_end = UNIFIED.index('if (action === "enqueue_initial_sms")', enqueue_start)
    handler_preamble = UNIFIED[handler_start:enqueue_start]
    enqueue_source = UNIFIED[enqueue_start:enqueue_end]

    assert "installSmsPendingSendWatchdogTrigger_" not in handler_preamble
    assert 'action !== "incoming_sms" && action !== "enqueue_incoming_sms"' in handler_preamble
    queue_call = enqueue_source.index("enqueueIncomingSmsV10_(body, requestId)")
    activity_call = enqueue_source.index('recordTaskerTransportActivityV12_("inbound", body)')
    assert queue_call < activity_call


def test_every_processed_inbound_gets_an_explicit_terminal_disposition():
    assert '"disposition"' in OUTBOX
    assert '"disposition_reason"' in OUTBOX
    assert '"disposition_at"' in OUTBOX
    assert "classifySmsInboundDispositionV14_" in OUTBOX
    for disposition in (
        'handoff ? "reply_queued_and_manual_review" : "reply_queued"',
        'type: "manual_review"',
        'type: "manual_takeover"',
        'type: "intentional_skip"',
        '"processing_failed"',
    ):
        assert disposition in OUTBOX

    completion_start = OUTBOX.index("function completeQueuedSmsInbound_")
    completion_end = OUTBOX.index("function auditSmsInboundCompletenessV14_", completion_start)
    completion_source = OUTBOX[completion_start:completion_end]
    assert "currentRow[14] = disposition" in completion_source
    assert "currentRow[15] = dispositionReason" in completion_source
    assert "currentRow[16] = disposition ? new Date()" in completion_source
    assert ".setValues([currentRow])" in completion_source


def test_inbound_completeness_auditor_retries_stale_work_and_alerts_once():
    start = OUTBOX.index("function auditSmsInboundCompletenessV14_()")
    end = OUTBOX.index("function hasSmsOutboxRecordForInboundV14_", start)
    source = OUTBOX[start:end]

    assert 'status === "queued"' in source
    assert 'status === "processing"' in source
    assert 'status === "failed"' in source
    assert 'status === "processed" && !disposition' in source
    assert "hasSmsOutboxRecordForInboundV14_" in source
    assert "sendSmsInboundCompletenessHandoffV14_" in source
    assert "if (auditAlertedAt) return" in source
    assert "if (needsProcessor) processSmsInboundQueue_();" in source


def test_enqueue_ignored_messages_are_durably_logged_with_reason():
    enqueue_start = UNIFIED.index('if (action === "enqueue_incoming_sms")')
    enqueue_end = UNIFIED.index('if (action === "enqueue_initial_sms")', enqueue_start)
    source = UNIFIED[enqueue_start:enqueue_end]
    assert 'appendSmsDebugLog_("incoming_sms_ignored"' in source
    assert "reason: queueIgnored" in source


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
    assert "A late Tasker receipt proves the initial send recovered" in OUTBOX
    assert "SMS OUTBOX NOT CLAIMED|SMS SEND RESULT UNCERTAIN|SMS SEND NOT CONFIRMED" in OUTBOX
    assert 'range: "M" + crmRow, value: ""' in OUTBOX
    assert 'range: "N" + crmRow, value: ""' in OUTBOX
    assert 'send_kind === "initial_outreach"' in UNIFIED
    assert 'send_kind: String(rows[matchIndex][6]' in UNIFIED
    send_sms_source = scheduler.split("def send_sms(", 1)[1].split("def _within_initial_hours", 1)[0]
    assert '"action": "enqueue_initial_sms"' in send_sms_source
    assert "if not follow_up:" in send_sms_source
    assert '"stable_id": stable_id' in send_sms_source
    assert "mark_sent(" not in send_sms_source


def test_initial_receipt_for_replaced_crm_phone_is_terminal_without_mutating_new_contact():
    receipt_start = OUTBOX.index("function applyInitialSmsReceiptV13_")
    receipt_end = OUTBOX.index("function enqueueIncomingSmsV10_", receipt_start)
    receipt_source = OUTBOX[receipt_start:receipt_end]

    assert "resolveInitialSmsReceiptRowV14_" in receipt_source
    assert "!resolvedRow.ok" in receipt_source
    assert 'stale_receipt: true' in receipt_source
    assert 'crm_write_skipped: true' in receipt_source
    assert 'throw new Error("Initial SMS receipt CRM phone no longer matches")' not in receipt_source
    assert '"initial_sms_stale_receipt"' in UNIFIED


def test_initial_receipt_recovers_shifted_rows_by_stable_id_or_unique_phone():
    assert 'stable_id: String(body && (body.stable_id || body.zpid || body.listing_id)' in OUTBOX
    assert "function resolveInitialSmsReceiptRowV14_" in OUTBOX
    assert 'mode: "stable_id"' in OUTBOX
    assert 'mode: "unique_phone"' in OUTBOX
    assert "testInitialSmsReceiptRowShiftRecoveryV14_" in OUTBOX
    assert "probe.initial_receipt_row_shift_recovery" in UNIFIED
    assert "Stable listing ID matched a different or ambiguous phone" in OUTBOX
    server = (ROOT / "webhook_server.py").read_text(encoding="utf-8")
    assert 'or _row_value(row, 27)' in server
    assert '"stable_id": stable_id' in server


def test_duplicate_listing_cleanup_uses_whole_row_deletes_not_blank_rows():
    server = (ROOT / "webhook_server.py").read_text(encoding="utf-8")
    scheduler = (ROOT / "bot_min.py").read_text(encoding="utf-8")
    assert "ws.delete_rows(row_idx)" in server
    assert "ws.delete_rows(delete_idx)" in server
    assert '"deleteDimension"' in scheduler
    assert "clearContents" not in server
    assert "batch_clear" not in server
    assert ".values().clear" not in scheduler


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
    assert "probe.receipt_aware_watchdog" in UNIFIED
    assert "testConfirmedSmsReplyReceiptRecovery_()" in UNIFIED
