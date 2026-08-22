var SMS_INBOUND_QUEUE_HEADERS_ = [
  "created_at", "status", "queue_id", "dedupe_key", "message_id", "phone",
  "message", "received_at", "attempts", "lease_token", "lease_until",
  "last_error", "processed_at", "outbox_request_id"
];

var SMS_PENDING_SEND_HEADERS_ = [
  "created_at", "status", "request_id", "message_id", "phone", "reply_text",
  "inbound_text", "last_alert_at", "not_before", "attempts", "lease_token",
  "lease_until", "claimed_at", "send_started_at", "sent_at", "last_error",
  "inbound_queue_id", "crm_row", "worker_id"
];

var SMS_HANDOFF_EMAIL_HEADERS_ = [
  "created_at", "status", "email_id", "payload_json", "attempts",
  "last_error", "sent_at", "lease_token", "lease_until"
];

function queueHandoffEmailV11_(payload) {
  var message = payload || {};
  var to = String(message.to || "").trim();
  var subject = String(message.subject || "").trim();
  var body = String(message.body || "");
  if (!to || !subject || !body) throw new Error("Handoff email requires to, subject, and body");

  installSmsOutboxTriggers_();
  var digest = Utilities.computeDigest(
    Utilities.DigestAlgorithm.SHA_256,
    [to.toLowerCase(), subject, body].join("\n---\n")
  );
  var emailId = digest.map(function(value) {
    return (value + 256).toString(16).slice(-2);
  }).join("").slice(0, 32);
  var ss = getSmsSpreadsheet_();
  var sheet = ss.getSheetByName("sms_handoff_email_outbox") || ss.insertSheet("sms_handoff_email_outbox");
  ensureSmsSheetHeaders_(sheet, SMS_HANDOFF_EMAIL_HEADERS_);

  var lock = LockService.getScriptLock();
  if (!lock.tryLock(5000)) {
    // appendRow is atomic at the Sheet boundary. A duplicate row is safer
    // than losing a terminal handoff; the drain dedupes by deterministic ID.
    sheet.appendRow([
      new Date(), "queued", emailId, JSON.stringify({ to: to, subject: subject, body: body }),
      0, "Queued without dedupe lock after contention", "", "", ""
    ]);
    return { ok: true, queued: true, email_id: emailId, lock_fallback: true };
  }
  try {
    var rows = sheet.getLastRow() > 1
      ? sheet.getRange(2, 1, sheet.getLastRow() - 1, SMS_HANDOFF_EMAIL_HEADERS_.length).getValues()
      : [];
    for (var i = rows.length - 1; i >= 0; i--) {
      if (String(rows[i][2] || "") === emailId &&
          ["queued", "claimed", "reconciling", "sent", "uncertain"].indexOf(String(rows[i][1] || "")) !== -1) {
        return { ok: true, queued: false, duplicate: true, email_id: emailId, status: String(rows[i][1] || "") };
      }
    }

    sheet.appendRow([
      new Date(), "queued", emailId, JSON.stringify({ to: to, subject: subject, body: body }),
      0, "", "", "", ""
    ]);
  } finally {
    lock.releaseLock();
  }
  try {
    drainHandoffEmailOutboxV11_();
  } catch (_) {}
  return { ok: true, queued: true, email_id: emailId };
}

function drainHandoffEmailOutboxV11_() {
  reconcileUncertainHandoffEmailV11_();
  var ss = getSmsSpreadsheet_();
  var sheet = ss.getSheetByName("sms_handoff_email_outbox");
  if (!sheet || sheet.getLastRow() < 2) return { ok: true, processed: 0 };
  ensureSmsSheetHeaders_(sheet, SMS_HANDOFF_EMAIL_HEADERS_);

  var lock = LockService.getScriptLock();
  if (!lock.tryLock(5000)) return { ok: false, retryable: true, reason: "Handoff email outbox is busy" };
  var rowNumber = 0;
  var leaseToken = Utilities.getUuid();
  var payload = null;
  try {
    var rows = sheet.getRange(2, 1, sheet.getLastRow() - 1, SMS_HANDOFF_EMAIL_HEADERS_.length).getValues();
    var activeIds = {};
    rows.forEach(function(row) {
      var rowStatus = String(row[1] || "");
      if (["claimed", "reconciling", "sent", "uncertain"].indexOf(rowStatus) !== -1) {
        activeIds[String(row[2] || "")] = true;
      }
    });
    for (var i = 0; i < rows.length; i++) {
      var status = String(rows[i][1] || "");
      var leaseUntil = new Date(rows[i][8]).getTime();
      if (status === "claimed" && (!leaseUntil || leaseUntil < Date.now())) {
        // The execution may have ended after MailApp accepted the email but
        // before Sheets recorded the receipt. Never auto-resend that row.
        var expiredClaimRow = i + 2;
        var uncertainRow = rows[i].slice();
        uncertainRow[1] = "uncertain";
        uncertainRow[5] = "Claim expired; delivery outcome unknown";
        uncertainRow[7] = "";
        uncertainRow[8] = new Date(Date.now() + 5 * 60 * 1000);
        sheet.getRange(expiredClaimRow, 1, 1, SMS_HANDOFF_EMAIL_HEADERS_.length)
          .setValues([uncertainRow]);
        activeIds[String(rows[i][2] || "")] = true;
        continue;
      }
      if (status !== "queued") continue;
      rowNumber = i + 2;
      var emailId = String(rows[i][2] || "");
      if (activeIds[emailId]) {
        sheet.getRange(rowNumber, 2).setValue("duplicate");
        rowNumber = 0;
        continue;
      }
      try {
        payload = JSON.parse(String(rows[i][3] || "{}"));
      } catch (err) {
        sheet.getRange(rowNumber, 2).setValue("failed");
        sheet.getRange(rowNumber, 6).setValue("Invalid payload: " + err);
        rowNumber = 0;
        continue;
      }
      var claimedEmailRow = rows[i].slice();
      claimedEmailRow[1] = "claimed";
      claimedEmailRow[4] = Number(rows[i][4] || 0) + 1;
      claimedEmailRow[7] = leaseToken;
      claimedEmailRow[8] = new Date(Date.now() + 2 * 60 * 1000);
      // Status and lease ownership are one transition. A failed Sheet write
      // leaves the prior queued row intact and eligible for another worker.
      sheet.getRange(rowNumber, 1, 1, SMS_HANDOFF_EMAIL_HEADERS_.length)
        .setValues([claimedEmailRow]);
      activeIds[emailId] = true;
      break;
    }
  } finally {
    lock.releaseLock();
  }
  if (!rowNumber || !payload) return { ok: true, processed: 0 };

  try {
    MailApp.sendEmail({ to: payload.to, subject: payload.subject, body: payload.body });
  } catch (err) {
    sheet.getRange(rowNumber, 2).setValue("queued");
    sheet.getRange(rowNumber, 6).setValue(String(err));
    sheet.getRange(rowNumber, 8, 1, 2).setValues([["", ""]]);
    throw err;
  }

  // MailApp returning successfully is the delivery boundary. Never retry that
  // email merely because recording the receipt in Sheets has a transient error.
  try {
    sheet.getRange(rowNumber, 2).setValue("sent");
    sheet.getRange(rowNumber, 6, 1, 4).setValues([["", new Date(), "", ""]]);
    return { ok: true, processed: 1 };
  } catch (err) {
    try {
      sheet.getRange(rowNumber, 2).setValue("uncertain");
      sheet.getRange(rowNumber, 6).setValue("Email sent; receipt write failed: " + String(err));
      sheet.getRange(rowNumber, 8, 1, 2).setValues([["", new Date(Date.now() + 5 * 60 * 1000)]]);
    } catch (_) {}
    return { ok: false, processed: 1, uncertain: true, reason: "Email sent but receipt write was uncertain" };
  }
}

function reconcileUncertainHandoffEmailV11_() {
  var ss = getSmsSpreadsheet_();
  var sheet = ss.getSheetByName("sms_handoff_email_outbox");
  if (!sheet || sheet.getLastRow() < 2) return { ok: true, processed: 0 };
  ensureSmsSheetHeaders_(sheet, SMS_HANDOFF_EMAIL_HEADERS_);

  var lock = LockService.getScriptLock();
  if (!lock.tryLock(3000)) return { ok: false, retryable: true, reason: "Handoff reconciliation is busy" };
  var rowNumber = 0;
  var emailId = "";
  var payload = null;
  var leaseToken = Utilities.getUuid();
  try {
    var rows = sheet.getRange(2, 1, sheet.getLastRow() - 1, SMS_HANDOFF_EMAIL_HEADERS_.length).getValues();
    for (var i = 0; i < rows.length; i++) {
      var status = String(rows[i][1] || "");
      var retryAt = new Date(rows[i][8]).getTime();
      if (status === "reconciling" && (!retryAt || retryAt < Date.now())) {
        var retryReconciliationRow = rows[i].slice();
        retryReconciliationRow[1] = "uncertain";
        retryReconciliationRow[5] = "Reconciliation lease expired; retrying owner-surface check";
        retryReconciliationRow[7] = "";
        retryReconciliationRow[8] = new Date();
        sheet.getRange(i + 2, 1, 1, SMS_HANDOFF_EMAIL_HEADERS_.length)
          .setValues([retryReconciliationRow]);
        rows[i] = retryReconciliationRow;
        status = "uncertain";
        retryAt = Date.now();
      }
      if (status !== "uncertain" || (retryAt && retryAt > Date.now())) continue;
      try {
        payload = JSON.parse(String(rows[i][3] || "{}"));
      } catch (_) {
        sheet.getRange(i + 2, 2).setValue("failed");
        continue;
      }
      rowNumber = i + 2;
      emailId = String(rows[i][2] || "");
      var reconcilingRow = rows[i].slice();
      reconcilingRow[1] = "reconciling";
      reconcilingRow[7] = leaseToken;
      reconcilingRow[8] = new Date(Date.now() + 2 * 60 * 1000);
      sheet.getRange(rowNumber, 1, 1, SMS_HANDOFF_EMAIL_HEADERS_.length)
        .setValues([reconcilingRow]);
      break;
    }
  } finally {
    lock.releaseLock();
  }
  if (!rowNumber || !payload) return { ok: true, processed: 0 };

  var foundInSent = wasHandoffEmailSentV11_(payload, emailId);
  if (!lock.tryLock(3000)) return { ok: false, retryable: true, reason: "Handoff reconciliation receipt is busy" };
  try {
    var currentStatus = String(sheet.getRange(rowNumber, 2).getValue() || "");
    var currentLease = String(sheet.getRange(rowNumber, 8).getValue() || "");
    if (currentStatus !== "reconciling" || currentLease !== leaseToken) {
      return { ok: false, processed: 0, reason: "Reconciliation lease changed" };
    }
    sheet.getRange(rowNumber, 2).setValue(foundInSent ? "sent" : "queued");
    sheet.getRange(rowNumber, 6).setValue(foundInSent
      ? "Recovered sent receipt from Gmail"
      : "No matching Gmail Sent message; safe to retry");
    sheet.getRange(rowNumber, 7).setValue(foundInSent ? new Date() : "");
    sheet.getRange(rowNumber, 8, 1, 2).setValues([["", ""]]);
    return { ok: true, processed: 1, found_in_sent: foundInSent };
  } finally {
    lock.releaseLock();
  }
}

function wasHandoffEmailSentV11_(payload, emailId) {
  if (!payload || !payload.to || !payload.subject || !emailId) return false;
  var safeTo = String(payload.to).replace(/["\\]/g, " ");
  var safeSubject = String(payload.subject).replace(/["\\]/g, " ");
  var threads = GmailApp.search('in:sent to:"' + safeTo + '" subject:"' + safeSubject + '" newer_than:14d', 0, 30);
  for (var i = 0; i < threads.length; i++) {
    var messages = threads[i].getMessages();
    for (var j = 0; j < messages.length; j++) {
      var message = messages[j];
      var digest = Utilities.computeDigest(
        Utilities.DigestAlgorithm.SHA_256,
        [
          String(payload.to).toLowerCase(),
          String(message.getSubject() || "").trim(),
          String(message.getPlainBody() || "").replace(/\r\n/g, "\n")
        ].join("\n---\n")
      );
      var candidateId = digest.map(function(value) {
        return (value + 256).toString(16).slice(-2);
      }).join("").slice(0, 32);
      if (candidateId === emailId) return true;
    }
  }
  return false;
}

function getPendingSmsHeaders_() {
  return SMS_PENDING_SEND_HEADERS_.slice();
}

function enqueueInitialSmsV13_(body, webhookRequestId) {
  var phone = normalizePhone_(body && body.phone || "");
  var replyText = normalizeWhitespace_(String(body && (body.reply_text || body.message) || ""));
  var crmRow = Number(body && body.crm_row || body && body.row || 0);
  var messageId = String(body && body.message_id || "").trim();
  var requestId = String(body && body.request_id || webhookRequestId || Utilities.getUuid()).trim();
  if (phone.length !== 10 || !replyText || !Number.isInteger(crmRow) || crmRow < 2 || !messageId) {
    return { ok: false, queued: false, error: "Initial SMS requires phone, message, CRM row, and message ID" };
  }

  installSmsOutboxTriggers_();
  var ss = getSmsSpreadsheet_();
  var sheet = ss.getSheetByName("sms_pending_sends") || ss.insertSheet("sms_pending_sends");
  ensureSmsSheetHeaders_(sheet, SMS_PENDING_SEND_HEADERS_);
  var metadata = "__initial_outreach__:" + JSON.stringify({
    crm_row: crmRow,
    mark_codex_verified: body.mark_codex_verified !== false
  });
  var lock = LockService.getScriptLock();
  if (!lock.tryLock(5000)) {
    return { ok: false, queued: false, retryable: true, error: "Initial SMS outbox is temporarily busy" };
  }
  try {
    var rows = sheet.getLastRow() > 1
      ? sheet.getRange(2, 1, sheet.getLastRow() - 1, SMS_PENDING_SEND_HEADERS_.length).getValues()
      : [];
    for (var i = rows.length - 1; i >= 0; i--) {
      if (String(rows[i][3] || "") !== messageId) continue;
      var existingStatus = String(rows[i][1] || "");
      if (["queued", "claimed", "send_started", "sent"].indexOf(existingStatus) !== -1) {
        return {
          ok: true,
          queued: existingStatus !== "sent",
          duplicate: true,
          status: existingStatus,
          request_id: String(rows[i][2] || ""),
          message_id: messageId,
          pending_row: i + 2
        };
      }
    }
    sheet.appendRow([
      new Date(), "queued", requestId, messageId, phone, replyText, metadata,
      "", new Date(), 0, "", "", "", "", "", "", "", crmRow, ""
    ]);
    return {
      ok: true,
      queued: true,
      status: "queued",
      request_id: requestId,
      message_id: messageId,
      pending_row: sheet.getLastRow()
    };
  } finally {
    lock.releaseLock();
  }
}

function applyInitialSmsReceiptV13_(body, correlation) {
  var crmRow = Number(correlation && correlation.crm_row || 0);
  var phone = normalizePhone_(correlation && correlation.canonical_phone || body && body.phone || "");
  var message = String(correlation && correlation.canonical_reply_text || body && body.reply_text || "").trim();
  if (!Number.isInteger(crmRow) || crmRow < 2 || phone.length !== 10 || !message) {
    throw new Error("Initial SMS receipt is missing its CRM correlation");
  }
  var metadata = {};
  try {
    metadata = JSON.parse(String(correlation.send_metadata || "").replace(/^__initial_outreach__:/, ""));
  } catch (_) {}
  var sheet = getSheet_();
  var currentPhone = normalizePhone_(sheet.getRange(crmRow, 3).getValue());
  if (currentPhone !== phone) throw new Error("Initial SMS receipt CRM phone no longer matches");
  var sentAtRaw = body && body.sent_at;
  var sentAt = /^\d{10,}$/.test(String(sentAtRaw || ""))
    ? new Date(Number(sentAtRaw))
    : new Date(sentAtRaw || new Date());
  if (isNaN(sentAt.getTime())) sentAt = new Date();
  var updates = [
    { range: "H" + crmRow, value: "x" },
    { range: "W" + crmRow, value: sentAt },
    { range: "L" + crmRow, value: message },
    { range: "O" + crmRow, value: sentAt }
  ];
  // A late Tasker receipt proves the initial send recovered. Remove only the
  // transport-generated takeover state so a future agent reply is not muted.
  var transportAlert = String(sheet.getRange(crmRow, 13).getValue() || "").trim();
  if (/^(SMS OUTBOX NOT CLAIMED|SMS SEND RESULT UNCERTAIN|SMS SEND NOT CONFIRMED)$/i.test(transportAlert)) {
    updates.push(
      { range: "M" + crmRow, value: "" },
      { range: "N" + crmRow, value: "" },
      { range: "Q" + crmRow, value: "FALSE" },
      { range: "T" + crmRow, value: "FALSE" }
    );
  }
  if (metadata.mark_codex_verified !== false) updates.push({ range: "AQ" + crmRow, value: "x" });
  updates.forEach(function(update) { sheet.getRange(update.range).setValue(update.value); });
  return { ok: true, initial_sms: true, row: crmRow, sent_at: sentAt.toISOString() };
}

function enqueueIncomingSmsV10_(body, webhookRequestId) {
  var phone = normalizePhone_(body && body.phone || "");
  var message = normalizeWhitespace_(String(body && body.message || ""));
  if (phone.length !== 10 || !message) {
    return { ok: false, queued: false, error: "A valid phone and message are required" };
  }

  installSmsOutboxTriggers_();
  var ss = getSmsSpreadsheet_();
  var sheet = ss.getSheetByName("sms_inbound_queue") || ss.insertSheet("sms_inbound_queue");
  ensureSmsSheetHeaders_(sheet, SMS_INBOUND_QUEUE_HEADERS_);
  var now = new Date();
  var suppliedMessageId = String(body && body.message_id || "").trim();
  var receivedAt = String(body && body.received_at || now.toISOString()).trim();
  var dedupeKey = buildSmsInboundDedupeKey_(phone, message, suppliedMessageId);
  var transportFingerprint = buildSmsInboundTransportFingerprint_(phone, message, receivedAt);
  var cache = CacheService.getScriptCache();
  var cacheKey = "sms_inbound_" + dedupeKey;
  var transportCacheKey = "sms_inbound_transport_" + transportFingerprint;
  var cachedQueueId = cache.get(cacheKey) || cache.get(transportCacheKey);
  if (cachedQueueId) {
    return {
      ok: true,
      queued: false,
      duplicate: true,
      queue_id: cachedQueueId,
      reason: "Duplicate inbound transport suppressed"
    };
  }

  var lock = LockService.getScriptLock();
  var hasLock = lock.tryLock(3000);
  var queueId;
  var messageId = suppliedMessageId || (phone + "-" + now.getTime());

  // A duplicate queue row is safer than dropping a unique inbound. The
  // downstream CRM dedupe still prevents a duplicate bot response.
  if (!hasLock) {
    queueId = Utilities.getUuid();
    sheet.appendRow([
      now, "queued", queueId, dedupeKey, messageId, phone, message,
      receivedAt, 0, "", "", "", "", ""
    ]);
    cache.put(cacheKey, queueId, 600);
    cache.put(transportCacheKey, queueId, 600);
    try {
      appendSmsDebugLog_("incoming_sms_enqueued_lock_fallback", {
        request_id: webhookRequestId || "",
        phone: phone,
        message: message,
        reason: "Inbound appended without the global lock after brief contention",
        queue_id: queueId,
        message_id: messageId
      });
    } catch (_) {}
    return buildQueuedSmsInboundResponse_(queueId, messageId);
  }

  try {
    var lastRow = sheet.getLastRow();
    var firstDataRow = Math.max(2, lastRow - 249);
    var rows = lastRow >= firstDataRow
      ? sheet.getRange(firstDataRow, 1, lastRow - firstDataRow + 1, SMS_INBOUND_QUEUE_HEADERS_.length).getValues()
      : [];
    for (var i = rows.length - 1; i >= 0; i--) {
      var created = new Date(rows[i][0]).getTime();
      var rowTransportFingerprint = buildSmsInboundTransportFingerprint_(
        rows[i][5], rows[i][6], rows[i][7]
      );
      if ((String(rows[i][3] || "") === dedupeKey || rowTransportFingerprint === transportFingerprint) &&
          created && now.getTime() - created < 10 * 60 * 1000) {
        cache.put(cacheKey, String(rows[i][2] || ""), 600);
        cache.put(transportCacheKey, String(rows[i][2] || ""), 600);
        return {
          ok: true,
          queued: false,
          duplicate: true,
          queue_id: String(rows[i][2] || ""),
          reason: "Duplicate inbound transport suppressed"
        };
      }
    }

    queueId = Utilities.getUuid();
    sheet.appendRow([
      now, "queued", queueId, dedupeKey, messageId, phone, message,
      receivedAt, 0, "", "", "", "", ""
    ]);
    cache.put(cacheKey, queueId, 600);
    cache.put(transportCacheKey, queueId, 600);
  } finally {
    lock.releaseLock();
  }

  try {
    appendSmsDebugLog_("incoming_sms_enqueued", {
      request_id: webhookRequestId || "",
      phone: phone,
      message: message,
      reason: "V10 durable inbound queue",
      queue_id: queueId,
      message_id: messageId
    });
  } catch (_) {}
  return buildQueuedSmsInboundResponse_(queueId, messageId);
}

function buildQueuedSmsInboundResponse_(queueId, messageId) {
  return {
    ok: true,
    queued: true,
    queue_id: queueId,
    message_id: messageId,
    should_reply: false,
    should_reply_text: "false",
    handoff_needed: false,
    handoff_needed_text: "false",
    reason: "Inbound safely queued for processing"
  };
}

function buildSmsInboundDedupeKey_(phone, message, messageId) {
  var digest = Utilities.computeDigest(
    Utilities.DigestAlgorithm.SHA_256,
    normalizePhone_(phone) + "|" + (String(messageId || "").trim()
      ? "id:" + String(messageId).trim()
      : "text:" + normalizeWhitespace_(String(message || "")).toLowerCase())
  );
  return Utilities.base64EncodeWebSafe(digest).replace(/=+$/, "");
}

function buildSmsInboundTransportFingerprint_(phone, message, receivedAt) {
  var normalizedReceivedAt = normalizeWhitespace_(String(receivedAt || "")).toLowerCase();
  var digest = Utilities.computeDigest(
    Utilities.DigestAlgorithm.SHA_256,
    normalizePhone_(phone) + "|" +
      normalizeWhitespace_(String(message || "")).toLowerCase() + "|" +
      normalizedReceivedAt
  );
  return Utilities.base64EncodeWebSafe(digest).replace(/=+$/, "");
}

function processSmsInboundQueue_() {
  var started = Date.now();
  var processed = 0;
  while (processed < 4 && Date.now() - started < 4.5 * 60 * 1000) {
    var claim = claimQueuedSmsInbound_();
    if (!claim) break;
    try {
      var normalized = loadSmsInboundDecisionSnapshotV10_(claim.queue_id);
      var outboxRequestId = normalized && normalized.request_id || Utilities.getUuid();
      if (!normalized) {
        var smsResult = handleIncomingSms_({
          action: "incoming_sms",
          phone: claim.phone,
          message: claim.message,
          received_at: claim.received_at,
          message_id: claim.message_id,
          recovery_retry: claim.attempts > 1
        });
        normalized = normalizeTaskerPayload_(smsResult);
        normalized.request_id = outboxRequestId;
        normalized.message_id = claim.message_id;
        normalized.reply_to_phone = claim.phone;
        saveSmsInboundDecisionSnapshotV10_(claim.queue_id, normalized);
      }
      if (normalized.should_reply === true) {
        registerPendingSmsSendV10_({
          phone: claim.phone,
          message: claim.message,
          message_id: claim.message_id
        }, normalized, outboxRequestId, claim.queue_id);
      }
      if (!completeQueuedSmsInbound_(claim, "processed", "", outboxRequestId)) {
        var completionBusy = new Error("Inbound completion is temporarily busy");
        completionBusy.retryable = true;
        completionBusy.code = "SMS_INBOUND_COMPLETION_BUSY";
        throw completionBusy;
      }
      clearSmsInboundDecisionSnapshotV10_(claim.queue_id);
      appendSmsDebugLog_("incoming_sms_queue_processed", {
        request_id: outboxRequestId,
        phone: claim.phone,
        message: claim.message,
        should_reply: normalized.should_reply_text || "",
        reply_text: normalized.reply_text || "",
        reason: normalized.reason || "",
        lead_status: normalized.lead_status || "",
        queue_id: claim.queue_id
      });
    } catch (err) {
      var retryable = !!(err && err.retryable);
      var exhausted = claim.attempts >= (retryable ? 8 : 3);
      completeQueuedSmsInbound_(claim, exhausted ? "failed" : "queued", String(err), "");
      if (exhausted) clearSmsInboundDecisionSnapshotV10_(claim.queue_id);
      appendSmsDebugLog_("incoming_sms_queue_error", {
        request_id: claim.queue_id,
        phone: claim.phone,
        message: claim.message,
        reason: String(err),
        attempts: claim.attempts
      });
      if (exhausted) {
        try {
          sendSystemAlertEmail_("SMS BOT INBOUND PROCESSING FAILED", String(err));
        } catch (_) {}
      }
      if (retryable) break;
    }
    processed++;
  }
  return { ok: true, processed: processed };
}

function smsInboundDecisionSnapshotKeyV10_(queueId) {
  return "sms_inbound_decision_v10_" + String(queueId || "").replace(/[^A-Za-z0-9_-]/g, "");
}

function saveSmsInboundDecisionSnapshotV10_(queueId, normalized) {
  if (!queueId || !normalized) return;
  PropertiesService.getScriptProperties().setProperty(
    smsInboundDecisionSnapshotKeyV10_(queueId),
    JSON.stringify({ saved_at: Date.now(), decision: normalized })
  );
}

function loadSmsInboundDecisionSnapshotV10_(queueId) {
  if (!queueId) return null;
  var props = PropertiesService.getScriptProperties();
  var key = smsInboundDecisionSnapshotKeyV10_(queueId);
  var raw = props.getProperty(key);
  if (!raw) return null;
  try {
    var parsed = JSON.parse(raw);
    if (!parsed || !parsed.decision || Date.now() - Number(parsed.saved_at || 0) > 24 * 60 * 60 * 1000) {
      props.deleteProperty(key);
      return null;
    }
    return parsed.decision;
  } catch (_) {
    props.deleteProperty(key);
    return null;
  }
}

function clearSmsInboundDecisionSnapshotV10_(queueId) {
  if (!queueId) return;
  PropertiesService.getScriptProperties().deleteProperty(smsInboundDecisionSnapshotKeyV10_(queueId));
}

function claimQueuedSmsInbound_() {
  var ss = getSmsSpreadsheet_();
  var sheet = ss.getSheetByName("sms_inbound_queue");
  if (!sheet || sheet.getLastRow() < 2) return null;
  ensureSmsSheetHeaders_(sheet, SMS_INBOUND_QUEUE_HEADERS_);
  var lock = LockService.getScriptLock();
  if (!lock.tryLock(3000)) return null;
  try {
    var rows = sheet.getRange(2, 1, sheet.getLastRow() - 1, SMS_INBOUND_QUEUE_HEADERS_.length).getValues();
    var now = Date.now();
    for (var i = 0; i < rows.length; i++) {
      var status = String(rows[i][1] || "");
      var leaseUntil = new Date(rows[i][10]).getTime();
      if (status !== "queued" && !(status === "processing" && (!leaseUntil || leaseUntil < now))) continue;
      var createdAt = new Date(rows[i][0]).getTime();
      var phone = normalizePhone_(rows[i][5]);
      var fragmentIndexes = [i];
      var newestCreatedAt = createdAt;
      var previousFragmentAt = createdAt;
      for (var j = i + 1; j < rows.length; j++) {
        if (String(rows[j][1] || "") !== "queued" || normalizePhone_(rows[j][5]) !== phone) continue;
        var fragmentCreatedAt = new Date(rows[j][0]).getTime();
        if (!fragmentCreatedAt || !previousFragmentAt || fragmentCreatedAt - previousFragmentAt > 20000) break;
        fragmentIndexes.push(j);
        newestCreatedAt = Math.max(newestCreatedAt || 0, fragmentCreatedAt);
        previousFragmentAt = fragmentCreatedAt;
      }
      // Wait through the entire burst window. Each new bubble refreshes the
      // debounce, so one conversation turn is classified exactly once.
      if (status === "queued" && newestCreatedAt && now - newestCreatedAt < 20000) continue;

      var selectedIndex = fragmentIndexes[fragmentIndexes.length - 1];
      var seenFragmentTexts = {};
      var combinedMessage = fragmentIndexes.map(function(index) {
        return normalizeWhitespace_(String(rows[index][6] || ""));
      }).filter(function(fragmentText) {
        if (!fragmentText) return false;
        var fragmentKey = fragmentText.toLowerCase();
        if (seenFragmentTexts[fragmentKey]) return false;
        seenFragmentTexts[fragmentKey] = true;
        return true;
      }).join(" ");
      // Commit the combined message and every source disposition in one Sheet
      // write. Retrying after a transient error can never concatenate an
      // already-combined row with a surviving source fragment.
      if (fragmentIndexes.length > 1) {
        var blockStart = fragmentIndexes[0];
        var blockEnd = selectedIndex;
        var blockValues = rows.slice(blockStart, blockEnd + 1).map(function(row) {
          return row.slice();
        });
        blockValues[selectedIndex - blockStart][6] = combinedMessage;
        for (var k = 0; k < fragmentIndexes.length - 1; k++) {
          var fragmentOffset = fragmentIndexes[k] - blockStart;
          blockValues[fragmentOffset][1] = "coalesced";
          blockValues[fragmentOffset][11] = "Combined with queue " + String(rows[selectedIndex][2] || "");
          blockValues[fragmentOffset][12] = new Date();
        }
        sheet.getRange(
          blockStart + 2,
          1,
          blockValues.length,
          SMS_INBOUND_QUEUE_HEADERS_.length
        ).setValues(blockValues);
        for (var b = 0; b < blockValues.length; b++) {
          rows[blockStart + b] = blockValues[b];
        }
        rows[selectedIndex][6] = combinedMessage;
      }

      var attempts = Number(rows[selectedIndex][8] || 0) + 1;
      var leaseToken = Utilities.getUuid();
      var sheetRow = selectedIndex + 2;
      var claimedInboundRow = rows[selectedIndex].slice();
      claimedInboundRow[1] = "processing";
      claimedInboundRow[8] = attempts;
      claimedInboundRow[9] = leaseToken;
      claimedInboundRow[10] = new Date(now + 5 * 60 * 1000);
      // Status and lease metadata are one transition. A partial Sheets write
      // can no longer leave a processing row without a reclaimable lease.
      sheet.getRange(sheetRow, 1, 1, SMS_INBOUND_QUEUE_HEADERS_.length)
        .setValues([claimedInboundRow]);
      return {
        row: sheetRow,
        queue_id: String(rows[selectedIndex][2] || ""),
        message_id: String(rows[selectedIndex][4] || ""),
        phone: normalizePhone_(rows[selectedIndex][5]),
        message: String(rows[selectedIndex][6] || ""),
        received_at: String(rows[selectedIndex][7] || ""),
        attempts: attempts,
        lease_token: leaseToken
      };
    }
    return null;
  } finally {
    lock.releaseLock();
  }
}

function completeQueuedSmsInbound_(claim, status, error, outboxRequestId) {
  var ss = getSmsSpreadsheet_();
  var sheet = ss.getSheetByName("sms_inbound_queue");
  if (!sheet || !claim || !claim.row) return false;
  ensureSmsSheetHeaders_(sheet, SMS_INBOUND_QUEUE_HEADERS_);
  var lock = LockService.getScriptLock();
  if (!lock.tryLock(5000)) return false;
  try {
    var currentQueueId = String(sheet.getRange(claim.row, 3).getValue() || "");
    var currentLease = String(sheet.getRange(claim.row, 10).getValue() || "");
    if (currentQueueId !== claim.queue_id || currentLease !== claim.lease_token) return false;
    sheet.getRange(claim.row, 2).setValue(status);
    sheet.getRange(claim.row, 10, 1, 2).setValues([["", ""]]);
    sheet.getRange(claim.row, 12).setValue(error || "");
    if (status === "processed") sheet.getRange(claim.row, 13).setValue(new Date());
    if (outboxRequestId) sheet.getRange(claim.row, 14).setValue(outboxRequestId);
    return true;
  } finally {
    lock.releaseLock();
  }
}

function registerPendingSmsSendV10_(body, normalized, requestId, inboundQueueId) {
  var ss = getSmsSpreadsheet_();
  var sheet = ss.getSheetByName("sms_pending_sends") || ss.insertSheet("sms_pending_sends");
  ensureSmsSheetHeaders_(sheet, SMS_PENDING_SEND_HEADERS_);
  var phone = normalizePhone_(body && body.phone || "");
  var replyText = String(normalized && normalized.reply_text || "").trim();
  var messageId = String(body && body.message_id || "").trim();
  var delaySeconds = Math.max(0, Number(normalized && normalized.delay_seconds || 15));
  var now = new Date();
  var pendingRow = [
    now, "queued", requestId, messageId, phone, replyText,
    String(body && body.message || ""), "", new Date(now.getTime() + delaySeconds * 1000),
    0, "", "", "", "", "", "", inboundQueueId || "", findSmsCrmRowByPhone_(phone), ""
  ];
  if (typeof hasPendingSmsTakeoverV11_ === "function" && hasPendingSmsTakeoverV11_(phone)) {
    return { ok: true, queued: false, suppressed: true, reason: "Manual takeover is pending" };
  }
  var lock = LockService.getScriptLock();
  var hasLock = lock.tryLock(3000);
  if (!hasLock) {
    // The durable inbound decision snapshot lets the queue retry this exact
    // registration. Never append without the lock because two workers could
    // otherwise create two sendable rows for the same inbound message.
    var busyError = new Error("Pending SMS registration is temporarily busy");
    busyError.retryable = true;
    busyError.code = "SMS_PENDING_REGISTRATION_BUSY";
    throw busyError;
  }
  try {
    var rows = sheet.getLastRow() > 1
      ? sheet.getRange(2, 1, sheet.getLastRow() - 1, SMS_PENDING_SEND_HEADERS_.length).getValues()
      : [];
    for (var i = rows.length - 1; i >= 0; i--) {
      var status = String(rows[i][1] || "");
      if (messageId && String(rows[i][3] || "") === messageId &&
          ["queued", "claimed", "send_started", "sent"].indexOf(status) !== -1) {
        return { ok: true, duplicate: true, pending_row: i + 2 };
      }
    }
    sheet.appendRow(pendingRow);
    return { ok: true, queued: true, pending_row: sheet.getLastRow() };
  } finally {
    lock.releaseLock();
  }
}

function claimPendingSmsSendV10_(body) {
  var ss = getSmsSpreadsheet_();
  var sheet = ss.getSheetByName("sms_pending_sends");
  if (!sheet || sheet.getLastRow() < 2) return noPendingSmsClaim_();
  ensureSmsSheetHeaders_(sheet, SMS_PENDING_SEND_HEADERS_);
  var lock = LockService.getScriptLock();
  if (!lock.tryLock(3000)) return busyPendingSmsClaim_();
  try {
    var rows = sheet.getRange(2, 1, sheet.getLastRow() - 1, SMS_PENDING_SEND_HEADERS_.length).getValues();
    var now = Date.now();
    var workerId = String(body && body.worker_id || "pixel-v10").trim();

    // A lost HTTP response must return the same active lease to this worker,
    // never claim a second conversation.
    for (var activeIndex = 0; activeIndex < rows.length; activeIndex++) {
      var activeStatus = String(rows[activeIndex][1] || "");
      var activeLeaseUntil = new Date(rows[activeIndex][11]).getTime();
      if (activeStatus !== "claimed" || String(rows[activeIndex][18] || "") !== workerId ||
          !activeLeaseUntil || activeLeaseUntil < now) continue;
      return buildPendingSmsClaimResponse_(rows[activeIndex], activeIndex + 2);
    }

    for (var i = 0; i < rows.length; i++) {
      var status = String(rows[i][1] || "");
      var notBefore = new Date(rows[i][8]).getTime();
      var leaseUntil = new Date(rows[i][11]).getTime();
      var eligible = status === "queued" && (!notBefore || notBefore <= now);
      if (status === "claimed" && (!leaseUntil || leaseUntil < now) && !rows[i][13]) eligible = true;
      if (!eligible) continue;

      var sheetRow = i + 2;
      var staleReason = getPendingSmsStaleReason_(rows[i]);
      if (staleReason) {
        sheet.getRange(sheetRow, 2).setValue("superseded");
        sheet.getRange(sheetRow, 16).setValue(staleReason);
        continue;
      }

      var phone = normalizePhone_(rows[i][4]);
      if (phone.length !== 10 || !String(rows[i][5] || "").trim()) {
        sheet.getRange(sheetRow, 2).setValue("failed");
        sheet.getRange(sheetRow, 16).setValue("Invalid destination or empty reply");
        continue;
      }

      var leaseToken = Utilities.getUuid();
      var attempts = Number(rows[i][9] || 0) + 1;
      var claimedPendingRow = rows[i].slice();
      claimedPendingRow[1] = "claimed";
      claimedPendingRow[9] = attempts;
      claimedPendingRow[10] = leaseToken;
      claimedPendingRow[11] = new Date(now + 5 * 60 * 1000);
      claimedPendingRow[12] = new Date();
      claimedPendingRow[18] = workerId;
      // Commit ownership, token, lease, and worker together. If this write
      // fails, the prior queued/expired state remains eligible for retry.
      sheet.getRange(sheetRow, 1, 1, SMS_PENDING_SEND_HEADERS_.length)
        .setValues([claimedPendingRow]);
      rows[i] = claimedPendingRow;
      return buildPendingSmsClaimResponse_(claimedPendingRow, sheetRow);
    }
    return noPendingSmsClaim_();
  } finally {
    lock.releaseLock();
  }
}

function buildPendingSmsClaimResponse_(row, sheetRow) {
  return {
    ok: true,
    should_send: true,
    should_send_text: "true",
    pending_row: sheetRow,
    request_id: String(row[2] || ""),
    message_id: String(row[3] || ""),
    phone: normalizePhone_(row[4]),
    reply_text: String(row[5] || ""),
    inbound_text: String(row[6] || ""),
    lease_token: String(row[10] || ""),
    attempts: Number(row[9] || 0)
  };
}

function noPendingSmsClaim_() {
  return { ok: true, should_send: false, should_send_text: "false", reason: "No due SMS reply" };
}

function busyPendingSmsClaim_() {
  return {
    ok: true,
    should_send: false,
    should_send_text: "false",
    retryable: true,
    retry_after_seconds: 5,
    reason: "SMS outbox temporarily busy; retry"
  };
}

function markPendingSmsSendStartedV10_(body) {
  var match = findLeasedPendingSmsRow_(body, ["claimed", "send_started"]);
  var correlationMode = "exact_text";
  if (!match.ok) {
    match = findPendingSmsRowByExactLeaseIdentityV10_(body, ["claimed", "send_started"]);
    correlationMode = "exact_lease_identity";
  }
  if (!match.ok) return match;
  var staleReason = getPendingSmsStaleReason_(match.values);
  if (staleReason) {
    match.sheet.getRange(match.row, 2).setValue("superseded");
    match.sheet.getRange(match.row, 16).setValue(staleReason);
    match.sheet.getRange(match.row, 11, 1, 2).setValues([["", ""]]);
    match.sheet.getRange(match.row, 19).setValue("");
    return { ok: false, should_send: false, status: "superseded", reason: staleReason };
  }
  if (String(match.values[1] || "") === "send_started") {
    return {
      ok: true,
      duplicate: true,
      status: "send_started",
      pending_row: match.row,
      correlation_mode: correlationMode
    };
  }
  match.sheet.getRange(match.row, 2).setValue("send_started");
  match.sheet.getRange(match.row, 14).setValue(new Date());
  return {
    ok: true,
    status: "send_started",
    pending_row: match.row,
    correlation_mode: correlationMode
  };
}

function requeuePendingSmsSendAfterFailureV10_(body) {
  var match = findLeasedPendingSmsRow_(body, ["claimed", "send_started"]);
  if (!match.ok) return match;
  var attempts = Number(match.values[9] || 0);
  var nextStatus = attempts >= 3 ? "failed" : "queued";
  match.sheet.getRange(match.row, 2).setValue(nextStatus);
  match.sheet.getRange(match.row, 9).setValue(new Date(Date.now() + 2 * 60 * 1000));
  match.sheet.getRange(match.row, 11, 1, 4).setValues([["", "", "", ""]]);
  match.sheet.getRange(match.row, 19).setValue("");
  match.sheet.getRange(match.row, 16).setValue(String(body && (body.error || body.reason) || "SMS Failure event"));
  return { ok: true, status: nextStatus, pending_row: match.row, attempts: attempts };
}

function findLeasedPendingSmsRow_(body, allowedStatuses) {
  var ss = getSmsSpreadsheet_();
  var sheet = ss.getSheetByName("sms_pending_sends");
  if (!sheet || sheet.getLastRow() < 2) return { ok: false, reason: "No pending-send ledger is available" };
  ensureSmsSheetHeaders_(sheet, SMS_PENDING_SEND_HEADERS_);
  var rows = sheet.getRange(2, 1, sheet.getLastRow() - 1, SMS_PENDING_SEND_HEADERS_.length).getValues();
  var requestId = String(body && (body.request_id || body.sms_request_id) || "").trim();
  var messageId = String(body && body.message_id || "").trim();
  var leaseToken = String(body && body.lease_token || "").trim();
  var phone = normalizePhone_(body && body.phone || "");
  var replyText = normalizePendingSmsReply_(body && body.reply_text || "");
  for (var i = rows.length - 1; i >= 0; i--) {
    if (allowedStatuses.indexOf(String(rows[i][1] || "")) === -1) continue;
    if (requestId && String(rows[i][2] || "") !== requestId) continue;
    if (messageId && String(rows[i][3] || "") !== messageId) continue;
    if (!leaseToken || String(rows[i][10] || "") !== leaseToken) continue;
    if (phone && normalizePhone_(rows[i][4]) !== phone) continue;
    if (replyText && normalizePendingSmsReply_(rows[i][5]) !== replyText) continue;
    return { ok: true, sheet: sheet, row: i + 2, values: rows[i] };
  }
  return { ok: false, reason: "No exact leased send matched the callback" };
}

// Tasker can alter an echoed form value without altering the SMS Android was
// asked to send. The request, message, phone, and one-time lease together bind
// the callback to one exact pending row, so this fallback never matches on
// phone or text alone.
function findPendingSmsRowByExactLeaseIdentityV10_(body, allowedStatuses) {
  var ss = getSmsSpreadsheet_();
  var sheet = ss.getSheetByName("sms_pending_sends");
  if (!sheet || sheet.getLastRow() < 2) {
    return { ok: false, reason: "No pending-send ledger is available" };
  }
  ensureSmsSheetHeaders_(sheet, SMS_PENDING_SEND_HEADERS_);
  var rows = sheet.getRange(
    2,
    1,
    sheet.getLastRow() - 1,
    SMS_PENDING_SEND_HEADERS_.length
  ).getValues();
  var rowIndex = findPendingSmsRowIndexByExactLeaseIdentityV10_(rows, body, allowedStatuses);
  if (rowIndex < 0) {
    return {
      ok: false,
      reason: rowIndex === -2
        ? "Exact lease correlation requires request, message, phone, and lease"
        : "No pending send matched the exact lease identity"
    };
  }
  return {
    ok: true,
    sheet: sheet,
    row: rowIndex + 2,
    values: rows[rowIndex],
    correlation_mode: "exact_lease_identity"
  };
}

function findPendingSmsRowIndexByExactLeaseIdentityV10_(rows, body, allowedStatuses) {
  var requestId = String(body && (body.request_id || body.sms_request_id) || "").trim();
  var messageId = String(body && body.message_id || "").trim();
  var phone = normalizePhone_(body && body.phone || "");
  var leaseToken = String(body && body.lease_token || "").trim();
  if (!requestId || !messageId || !phone || !leaseToken) return -2;

  for (var i = rows.length - 1; i >= 0; i--) {
    if (allowedStatuses.indexOf(String(rows[i][1] || "")) === -1) continue;
    if (String(rows[i][2] || "") !== requestId) continue;
    if (String(rows[i][3] || "") !== messageId) continue;
    if (normalizePhone_(rows[i][4]) !== phone) continue;
    if (String(rows[i][10] || "") !== leaseToken) continue;
    return i;
  }
  return -1;
}

function testSmsReceiptLeaseIdentity_() {
  var row = [
    new Date(), "send_started", "request-1", "message-1", "4075528213",
    "I’ve handled short sales for 15+ years.", "", "", "", 1,
    "lease-1", new Date(), new Date(), new Date(), "", "", "", 4900, "pixel-v16"
  ];
  var alteredEcho = {
    request_id: "request-1",
    message_id: "message-1",
    phone: "407-552-8213",
    reply_text: "I’ve handled short sales for 15  years.",
    lease_token: "lease-1"
  };
  var matched = findPendingSmsRowIndexByExactLeaseIdentityV10_(
    [row], alteredEcho, ["claimed", "send_started", "sent"]
  );
  if (matched !== 0) throw new Error("Exact lease identity did not recover altered receipt text");

  alteredEcho.lease_token = "wrong-lease";
  if (findPendingSmsRowIndexByExactLeaseIdentityV10_(
    [row], alteredEcho, ["claimed", "send_started", "sent"]
  ) !== -1) {
    throw new Error("Exact lease identity accepted a mismatched lease");
  }
  delete alteredEcho.message_id;
  if (findPendingSmsRowIndexByExactLeaseIdentityV10_(
    [row], alteredEcho, ["claimed", "send_started", "sent"]
  ) !== -2) {
    throw new Error("Exact lease identity accepted incomplete identifiers");
  }
  return { ok: true };
}

function normalizePendingSmsInboundText_(value) {
  return normalizeWhitespace_(String(value || "")).toLowerCase();
}

function testPendingSmsStaleTextNormalization_() {
  var original = normalizePendingSmsInboundText_("Handling it Myself.  I'm a pro with it.");
  var stored = normalizePendingSmsInboundText_("handling it myself. i'm a pro with it.");
  if (original !== stored) throw new Error("Equivalent inbound text did not normalize identically");

  var newer = normalizePendingSmsInboundText_("Handling it myself, but I have another question.");
  if (original === newer) throw new Error("Genuinely newer inbound text was treated as equivalent");
  return { ok: true };
}

function getPendingSmsStaleReason_(outboxRow) {
  var phone = normalizePhone_(outboxRow[4]);
  var inboundText = normalizePendingSmsInboundText_(outboxRow[6]);
  var replyText = normalizePendingSmsInboundText_(outboxRow[5]);
  var approvedOfferScopeReply =
    replyText === normalizePendingSmsInboundText_("I don't represent a buyer or submit offers. I handle the lender-side short-sale work for the listing agent.") &&
    typeof isOfferSubmissionConfusionSignal_ === "function" &&
    isOfferSubmissionConfusionSignal_(inboundText);
  if (!approvedOfferScopeReply && typeof hasPendingSmsTakeoverV11_ === "function" && hasPendingSmsTakeoverV11_(phone)) {
    return "Manual takeover is pending";
  }
  var messageId = String(outboxRow[3] || "");
  var isInitialOutreach = String(outboxRow[6] || "").indexOf("__initial_outreach__:") === 0;
  var sheet = getSheet_();
  var data = getSheetData_(sheet);
  for (var i = 0; i < data.length; i++) {
    var rowObj = data[i].obj;
    if (normalizePhone_(rowObj[HEADERS.phone]) !== phone) continue;
    if (!approvedOfferScopeReply && String(rowObj[HEADERS.human_override] || "").toUpperCase() === "TRUE") return "Human takeover is active";
    if (isInitialOutreach) return "";
    // Older ShortSaleLeads layouts do not have last_inbound_text. In that
    // layout, last_message_id remains the authoritative stale-reply guard.
    var hasInboundColumn = Object.prototype.hasOwnProperty.call(rowObj, HEADERS.last_inbound_text);
    var currentInboundText = hasInboundColumn
      ? normalizePendingSmsInboundText_(rowObj[HEADERS.last_inbound_text])
      : "";
    var newerInboundIsCourtesy = currentInboundText &&
      (isFinalCourtesyReply_(currentInboundText) || isPunctuationCorrectionFragment_(currentInboundText));
    if (messageId && String(rowObj[HEADERS.last_message_id] || "") !== messageId && !newerInboundIsCourtesy) {
      return "A newer substantive inbound message exists";
    }
    if (inboundText && currentInboundText && currentInboundText !== inboundText && !newerInboundIsCourtesy) {
      return "Latest inbound text changed";
    }
    return "";
  }
  return "CRM row no longer matches destination";
}

function findSmsCrmRowByPhone_(phone) {
  try {
    var data = getSheetData_(getSheet_());
    for (var i = 0; i < data.length; i++) {
      if (normalizePhone_(data[i].obj[HEADERS.phone]) === normalizePhone_(phone)) return data[i].row;
    }
  } catch (_) {}
  return "";
}

function getSmsOutboxStatus_() {
  var ss = getSmsSpreadsheet_();
  var result = { ok: true, inbound: {}, outbound: {}, transport: getTaskerTransportHealthV12_() };
  var inbound = ss.getSheetByName("sms_inbound_queue");
  var outbound = ss.getSheetByName("sms_pending_sends");
  if (inbound && inbound.getLastRow() > 1) {
    inbound.getRange(2, 2, inbound.getLastRow() - 1, 1).getValues().forEach(function(row) {
      var key = String(row[0] || "unknown");
      result.inbound[key] = (result.inbound[key] || 0) + 1;
    });
  }
  if (outbound && outbound.getLastRow() > 1) {
    outbound.getRange(2, 2, outbound.getLastRow() - 1, 1).getValues().forEach(function(row) {
      var key = String(row[0] || "unknown");
      result.outbound[key] = (result.outbound[key] || 0) + 1;
    });
  }
  return result;
}

function recordTaskerTransportActivityV12_(kind, body) {
  var now = Date.now();
  var normalizedKind = String(kind || "activity").toLowerCase();
  var props = PropertiesService.getScriptProperties();
  var values = {
    SMS_TASKER_LAST_ACTIVITY_MS: String(now),
    SMS_TASKER_LAST_ACTIVITY_KIND: normalizedKind
  };
  if (normalizedKind === "heartbeat") values.SMS_TASKER_LAST_HEARTBEAT_MS = String(now);
  if (normalizedKind === "claim") values.SMS_TASKER_LAST_CLAIM_MS = String(now);
  if (body && body.transport_version) values.SMS_TASKER_TRANSPORT_VERSION = String(body.transport_version);
  if (body && body.worker_id) values.SMS_TASKER_WORKER_ID = String(body.worker_id);
  props.setProperties(values, false);
  return getTaskerTransportHealthV12_(now);
}

function getTaskerTransportHealthV12_(nowOverride) {
  var now = Number(nowOverride || Date.now());
  var props = PropertiesService.getScriptProperties();
  var lastActivityMs = Number(props.getProperty("SMS_TASKER_LAST_ACTIVITY_MS") || 0);
  var lastHeartbeatMs = Number(props.getProperty("SMS_TASKER_LAST_HEARTBEAT_MS") || 0);
  var lastClaimMs = Number(props.getProperty("SMS_TASKER_LAST_CLAIM_MS") || 0);
  var maxAgeSeconds = Number(props.getProperty("SMS_TASKER_HEALTH_MAX_AGE_SECONDS") || 600);
  if (!isFinite(maxAgeSeconds) || maxAgeSeconds < 60) maxAgeSeconds = 600;
  var ageSeconds = lastActivityMs ? Math.max(0, Math.floor((now - lastActivityMs) / 1000)) : null;
  var healthy = ageSeconds !== null && ageSeconds <= maxAgeSeconds;
  var reason = healthy
    ? "Tasker transport is active"
    : (lastActivityMs ? "Tasker transport activity is stale" : "No Tasker transport activity has been recorded");
  return {
    ok: true,
    healthy: healthy,
    reason: reason,
    max_age_seconds: maxAgeSeconds,
    age_seconds: ageSeconds,
    last_activity_at: lastActivityMs ? new Date(lastActivityMs).toISOString() : "",
    last_activity_kind: props.getProperty("SMS_TASKER_LAST_ACTIVITY_KIND") || "",
    last_heartbeat_at: lastHeartbeatMs ? new Date(lastHeartbeatMs).toISOString() : "",
    last_claim_at: lastClaimMs ? new Date(lastClaimMs).toISOString() : "",
    transport_version: props.getProperty("SMS_TASKER_TRANSPORT_VERSION") || "",
    worker_id: props.getProperty("SMS_TASKER_WORKER_ID") || "",
    server_time: new Date(now).toISOString()
  };
}

function monitorTaskerTransportHealthV12_() {
  var health = getTaskerTransportHealthV12_();
  var props = PropertiesService.getScriptProperties();
  var priorState = props.getProperty("SMS_TASKER_TRANSPORT_ALERT_STATE") || "unknown";
  if (health.healthy) {
    if (priorState !== "healthy") {
      props.setProperty("SMS_TASKER_TRANSPORT_RECOVERED_AT", new Date().toISOString());
    }
    props.setProperty("SMS_TASKER_TRANSPORT_ALERT_STATE", "healthy");
    return health;
  }
  if (priorState === "stale") return health;
  var body = [
    "The Android Tasker transport is not contacting Apps Script, so outbound SMS is being held instead of marked sent.",
    "",
    "Reason: " + health.reason,
    "Last activity: " + (health.last_activity_at || "none recorded"),
    "Last heartbeat: " + (health.last_heartbeat_at || "none recorded"),
    "Last claim: " + (health.last_claim_at || "none recorded"),
    "Transport version: " + (health.transport_version || "unknown"),
    "",
    "No Tasker settings were changed. Open Tasker and AutoRemote or restart the phone to restore the existing transport."
  ].join("\n");
  sendSystemAlertEmail_("SMS TASKER TRANSPORT OFFLINE", body);
  props.setProperties({
    SMS_TASKER_TRANSPORT_ALERT_STATE: "stale",
    SMS_TASKER_TRANSPORT_ALERTED_AT: new Date().toISOString()
  }, false);
  return health;
}

function smsOutboxWatchdog_() {
  var ss = getSmsSpreadsheet_();
  var sheet = ss.getSheetByName("sms_pending_sends");
  if (!sheet || sheet.getLastRow() < 2) return;
  ensureSmsSheetHeaders_(sheet, SMS_PENDING_SEND_HEADERS_);
  var rows = sheet.getRange(2, 1, sheet.getLastRow() - 1, SMS_PENDING_SEND_HEADERS_.length).getValues();
  var now = Date.now();
  rows.forEach(function(row, index) {
    var sheetRow = index + 2;
    var status = String(row[1] || "");
    var created = new Date(row[0]).getTime();
    var leaseUntil = new Date(row[11]).getTime();
    var sendStarted = new Date(row[13]).getTime();
    if (status === "claimed" && leaseUntil && leaseUntil < now && !sendStarted) {
      sheet.getRange(sheetRow, 2).setValue("queued");
      sheet.getRange(sheetRow, 11, 1, 3).setValues([["", "", ""]]);
      sheet.getRange(sheetRow, 19).setValue("");
      return;
    }
    if (status === "send_started" && sendStarted && now - sendStarted >= 10 * 60 * 1000 && !row[7]) {
      if (alertSmsOutboxProblem_(sheet, sheetRow, row, "SMS SEND RESULT UNCERTAIN")) {
        sheet.getRange(sheetRow, 2).setValue("uncertain");
      }
      return;
    }
    if (status === "queued" && created && now - created >= 15 * 60 * 1000 && !row[7]) {
      alertSmsOutboxProblem_(sheet, sheetRow, row, "SMS OUTBOX NOT CLAIMED");
    }
  });
}

function findConfirmedSmsReplyReceiptInHistory_(history, inboundText, replyText) {
  var inbound = normalizePendingSmsInboundText_(inboundText);
  var reply = normalizePendingSmsInboundText_(replyText);
  if (!inbound || !reply) return { confirmed: false, sent_at: "" };
  var entries = Array.isArray(history) ? history : [];
  for (var i = entries.length - 1; i >= 0; i--) {
    var entry = entries[i] || {};
    if (String(entry.role || "").toLowerCase() !== "agent" ||
        normalizePendingSmsInboundText_(entry.text) !== inbound) continue;
    for (var j = i + 1; j < entries.length; j++) {
      var later = entries[j] || {};
      if (String(later.role || "").toLowerCase() === "assistant" &&
          normalizePendingSmsInboundText_(later.text) === reply &&
          String(later.receipt_id || "").trim()) {
        return { confirmed: true, sent_at: String(later.ts || "") };
      }
    }
    return { confirmed: false, sent_at: "" };
  }
  return { confirmed: false, sent_at: "" };
}

function testConfirmedSmsReplyReceiptRecovery_() {
  var inbound = "How does your service work?";
  var reply = "I handle the lender paperwork, calls, follow-up, and negotiations through approval.";
  var history = [
    { role: "agent", text: inbound, ts: "2026-08-21T16:07:00-04:00" },
    { role: "assistant", text: reply, ts: "2026-08-21T16:09:00-04:00", receipt_id: "receipt-1" }
  ];
  var confirmed = findConfirmedSmsReplyReceiptInHistory_(history, inbound, reply);
  if (!confirmed.confirmed || confirmed.sent_at !== "2026-08-21T16:09:00-04:00") {
    throw new Error("Confirmed CRM reply receipt did not recover uncertain send");
  }
  if (findConfirmedSmsReplyReceiptInHistory_(history, "What is your fee?", reply).confirmed ||
      findConfirmedSmsReplyReceiptInHistory_(history, inbound, "Different reply").confirmed ||
      findConfirmedSmsReplyReceiptInHistory_([
        { role: "agent", text: inbound },
        { role: "assistant", text: reply }
      ], inbound, reply).confirmed) {
    throw new Error("Receipt recovery accepted an uncorrelated or unconfirmed history entry");
  }
  return { ok: true };
}

function findConfirmedSmsReplyReceiptForPendingRow_(row) {
  var inboundText = String(row && row[6] || "");
  if (!inboundText || inboundText.indexOf("__initial_outreach__:") === 0) {
    return { confirmed: false, sent_at: "" };
  }
  try {
    var data = getSheetData_(getSheet_());
    for (var i = 0; i < data.length; i++) {
      var obj = data[i].obj;
      if (normalizePhone_(obj[HEADERS.phone]) !== normalizePhone_(row[4])) continue;
      return findConfirmedSmsReplyReceiptInHistory_(
        getHistoryArray_(obj[HEADERS.history_json]),
        inboundText,
        row[5]
      );
    }
  } catch (_) {}
  return { confirmed: false, sent_at: "" };
}

function recordRecoveredSmsReplyReceipt_(sheet, sheetRow, receipt) {
  var sentAt = new Date(receipt && receipt.sent_at || new Date());
  if (isNaN(sentAt.getTime())) sentAt = new Date();
  sheet.getRange(sheetRow, 2).setValue("sent");
  sheet.getRange(sheetRow, 8).setValue("");
  sheet.getRange(sheetRow, 15).setValue(sentAt);
  sheet.getRange(sheetRow, 16).setValue("Recovered from confirmed CRM reply receipt");
}

function alertSmsOutboxProblem_(sheet, sheetRow, row, reason) {
  // The Tasker receipt can reach the CRM while the outbox status write is
  // still uncertain. A same-phone, same-inbound, same-reply history receipt
  // is terminal proof; recover the ledger instead of locking the conversation.
  var receipt = findConfirmedSmsReplyReceiptForPendingRow_(row);
  if (receipt.confirmed) {
    recordRecoveredSmsReplyReceipt_(sheet, sheetRow, receipt);
    return false;
  }
  var context = getSmsLeadContextByPhone_(row[4]);
  lockSmsConversationAfterTransportAlertV10_(row[4], reason);
  sendHandoffEmail_({
    handoff_type: reason,
    agent_name: context.agent_name,
    last_name: context.last_name,
    phone: row[4],
    email: context.email,
    listing_address: context.listing_address,
    city: context.city,
    state: context.state,
    last_message: row[5] || "",
    history: context.history
  });
  sheet.getRange(sheetRow, 8).setValue(new Date());
  return true;
}

function lockSmsConversationAfterTransportAlertV10_(phone, reason) {
  try {
    var sheet = getSheet_();
    var data = getSheetData_(sheet);
    for (var i = 0; i < data.length; i++) {
      if (normalizePhone_(data[i].obj[HEADERS.phone]) !== normalizePhone_(phone)) continue;
      updateRowFields_(sheet, data[i].row, {
        [HEADERS.conversation_summary]: reason || "SMS transport needs manual follow-up",
        [HEADERS.ai_state]: "handoff",
        [HEADERS.handoff_flag]: "TRUE",
        [HEADERS.human_override]: "TRUE"
      });
      break;
    }
  } catch (_) {}
}

function cancelPendingSmsForPhoneV10_(phone, status) {
  var normalizedPhone = normalizePhone_(phone);
  if (!normalizedPhone) return { ok: false, reason: "A phone number is required" };
  var ss = getSmsSpreadsheet_();
  var sheet = ss.getSheetByName("sms_pending_sends");
  if (!sheet || sheet.getLastRow() < 2) return { ok: true, cancelled: 0 };
  ensureSmsSheetHeaders_(sheet, SMS_PENDING_SEND_HEADERS_);
  var lock = LockService.getScriptLock();
  if (!lock.tryLock(5000)) return { ok: false, retryable: true, reason: "SMS outbox is busy" };
  try {
    var rows = sheet.getRange(2, 1, sheet.getLastRow() - 1, SMS_PENDING_SEND_HEADERS_.length).getValues();
    var cancelled = 0;
    for (var i = 0; i < rows.length; i++) {
      if (normalizePhone_(rows[i][4]) !== normalizedPhone) continue;
      if (["pending", "alerted", "queued", "claimed", "send_started"].indexOf(String(rows[i][1] || "")) === -1) continue;
      var rowNumber = i + 2;
      sheet.getRange(rowNumber, 2).setValue(status || "manual_sent");
      sheet.getRange(rowNumber, 11, 1, 2).setValues([["", ""]]);
      sheet.getRange(rowNumber, 16).setValue("Cancelled because human takeover is active");
      sheet.getRange(rowNumber, 19).setValue("");
      cancelled++;
    }
    return { ok: true, cancelled: cancelled };
  } finally {
    lock.releaseLock();
  }
}

function getSmsLeadContextByPhone_(phone) {
  var empty = { agent_name: "", last_name: "", email: "", listing_address: "", city: "", state: "", history: [] };
  try {
    var data = getSheetData_(getSheet_());
    for (var i = 0; i < data.length; i++) {
      var obj = data[i].obj;
      if (normalizePhone_(obj[HEADERS.phone]) !== normalizePhone_(phone)) continue;
      return {
        agent_name: obj[HEADERS.agent_name] || "",
        last_name: obj[HEADERS.last_name] || "",
        email: obj[HEADERS.email] || "",
        listing_address: obj[HEADERS.listing_address] || "",
        city: obj[HEADERS.city] || "",
        state: obj[HEADERS.state] || "",
        history: getHistoryArray_(obj[HEADERS.history_json])
      };
    }
  } catch (_) {}
  return empty;
}

function installSmsOutboxTriggers_() {
  var required = {
    processSmsInboundQueue_: 1,
    smsOutboxWatchdog_: 5,
    monitorTaskerTransportHealthV12_: 5,
    drainHandoffEmailOutboxV11_: 1,
    drainPendingSmsControlEventsV11_: 1
  };
  var triggers = ScriptApp.getProjectTriggers();
  Object.keys(required).forEach(function(handler) {
    var exists = triggers.some(function(trigger) { return trigger.getHandlerFunction() === handler; });
    if (!exists) ScriptApp.newTrigger(handler).timeBased().everyMinutes(required[handler]).create();
  });
  return { ok: true, installed: true };
}

function ensureSmsSheetHeaders_(sheet, headers) {
  if (sheet.getLastRow() === 0) {
    sheet.getRange(1, 1, 1, headers.length).setValues([headers]);
    return;
  }
  var existing = sheet.getRange(1, 1, 1, headers.length).getValues()[0];
  if (existing.join("|") !== headers.join("|")) {
    sheet.getRange(1, 1, 1, headers.length).setValues([headers]);
  }
}
