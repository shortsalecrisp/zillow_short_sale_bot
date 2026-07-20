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

function getPendingSmsHeaders_() {
  return SMS_PENDING_SEND_HEADERS_.slice();
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
  var dedupeKey = buildSmsInboundDedupeKey_(phone, message);
  var now = new Date();
  var lock = LockService.getScriptLock();
  lock.waitLock(10000);
  try {
    var lastRow = sheet.getLastRow();
    var firstDataRow = Math.max(2, lastRow - 249);
    var rows = lastRow >= firstDataRow
      ? sheet.getRange(firstDataRow, 1, lastRow - firstDataRow + 1, SMS_INBOUND_QUEUE_HEADERS_.length).getValues()
      : [];
    for (var i = rows.length - 1; i >= 0; i--) {
      var created = new Date(rows[i][0]).getTime();
      if (String(rows[i][3] || "") === dedupeKey && created && now.getTime() - created < 10 * 60 * 1000) {
        return {
          ok: true,
          queued: false,
          duplicate: true,
          queue_id: String(rows[i][2] || ""),
          reason: "Duplicate inbound transport suppressed"
        };
      }
    }

    var queueId = Utilities.getUuid();
    var messageId = String(body && body.message_id || "").trim() || (phone + "-" + now.getTime());
    sheet.appendRow([
      now, "queued", queueId, dedupeKey, messageId, phone, message,
      String(body && body.received_at || now.toISOString()), 0, "", "", "", "", ""
    ]);
    appendSmsDebugLog_("incoming_sms_enqueued", {
      request_id: webhookRequestId || "",
      phone: phone,
      message: message,
      reason: "V10 durable inbound queue",
      queue_id: queueId,
      message_id: messageId
    });
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
  } finally {
    lock.releaseLock();
  }
}

function buildSmsInboundDedupeKey_(phone, message) {
  var digest = Utilities.computeDigest(
    Utilities.DigestAlgorithm.SHA_256,
    normalizePhone_(phone) + "|" + normalizeWhitespace_(String(message || "")).toLowerCase()
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
      var smsResult = handleIncomingSms_({
        action: "incoming_sms",
        phone: claim.phone,
        message: claim.message,
        received_at: claim.received_at,
        message_id: claim.message_id
      });
      var normalized = normalizeTaskerPayload_(smsResult);
      var outboxRequestId = Utilities.getUuid();
      normalized.request_id = outboxRequestId;
      normalized.message_id = claim.message_id;
      normalized.reply_to_phone = claim.phone;
      if (normalized.should_reply === true) {
        registerPendingSmsSendV10_({
          phone: claim.phone,
          message: claim.message,
          message_id: claim.message_id
        }, normalized, outboxRequestId, claim.queue_id);
      }
      completeQueuedSmsInbound_(claim, "processed", "", outboxRequestId);
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
      completeQueuedSmsInbound_(claim, claim.attempts >= 3 ? "failed" : "queued", String(err), "");
      appendSmsDebugLog_("incoming_sms_queue_error", {
        request_id: claim.queue_id,
        phone: claim.phone,
        message: claim.message,
        reason: String(err),
        attempts: claim.attempts
      });
      if (claim.attempts >= 3) {
        try {
          sendSystemAlertEmail_("SMS BOT INBOUND PROCESSING FAILED", String(err));
        } catch (_) {}
      }
    }
    processed++;
  }
  return { ok: true, processed: processed };
}

function claimQueuedSmsInbound_() {
  var ss = getSmsSpreadsheet_();
  var sheet = ss.getSheetByName("sms_inbound_queue");
  if (!sheet || sheet.getLastRow() < 2) return null;
  ensureSmsSheetHeaders_(sheet, SMS_INBOUND_QUEUE_HEADERS_);
  var lock = LockService.getScriptLock();
  lock.waitLock(10000);
  try {
    var rows = sheet.getRange(2, 1, sheet.getLastRow() - 1, SMS_INBOUND_QUEUE_HEADERS_.length).getValues();
    var now = Date.now();
    for (var i = 0; i < rows.length; i++) {
      var status = String(rows[i][1] || "");
      var leaseUntil = new Date(rows[i][10]).getTime();
      if (status !== "queued" && !(status === "processing" && leaseUntil && leaseUntil < now)) continue;
      var attempts = Number(rows[i][8] || 0) + 1;
      var leaseToken = Utilities.getUuid();
      var sheetRow = i + 2;
      sheet.getRange(sheetRow, 2).setValue("processing");
      sheet.getRange(sheetRow, 9, 1, 3).setValues([[attempts, leaseToken, new Date(now + 5 * 60 * 1000)]]);
      return {
        row: sheetRow,
        queue_id: String(rows[i][2] || ""),
        message_id: String(rows[i][4] || ""),
        phone: normalizePhone_(rows[i][5]),
        message: String(rows[i][6] || ""),
        received_at: String(rows[i][7] || ""),
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
  if (!sheet || !claim || !claim.row) return;
  ensureSmsSheetHeaders_(sheet, SMS_INBOUND_QUEUE_HEADERS_);
  var currentQueueId = String(sheet.getRange(claim.row, 3).getValue() || "");
  var currentLease = String(sheet.getRange(claim.row, 10).getValue() || "");
  if (currentQueueId !== claim.queue_id || currentLease !== claim.lease_token) return;
  sheet.getRange(claim.row, 2).setValue(status);
  sheet.getRange(claim.row, 10, 1, 2).setValues([["", ""]]);
  sheet.getRange(claim.row, 12).setValue(error || "");
  if (status === "processed") sheet.getRange(claim.row, 13).setValue(new Date());
  if (outboxRequestId) sheet.getRange(claim.row, 14).setValue(outboxRequestId);
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
  var lock = LockService.getScriptLock();
  lock.waitLock(10000);
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
    sheet.appendRow([
      now, "queued", requestId, messageId, phone, replyText,
      String(body && body.message || ""), "", new Date(now.getTime() + delaySeconds * 1000),
      0, "", "", "", "", "", "", inboundQueueId || "", findSmsCrmRowByPhone_(phone), ""
    ]);
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
  lock.waitLock(10000);
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
      if (status === "claimed" && leaseUntil && leaseUntil < now && !rows[i][13]) eligible = true;
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
      sheet.getRange(sheetRow, 2).setValue("claimed");
      sheet.getRange(sheetRow, 10, 1, 4).setValues([[
        attempts, leaseToken, new Date(now + 5 * 60 * 1000), new Date()
      ]]);
      sheet.getRange(sheetRow, 19).setValue(workerId);
      rows[i][1] = "claimed";
      rows[i][9] = attempts;
      rows[i][10] = leaseToken;
      rows[i][11] = new Date(now + 5 * 60 * 1000);
      rows[i][18] = workerId;
      return buildPendingSmsClaimResponse_(rows[i], sheetRow);
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

function markPendingSmsSendStartedV10_(body) {
  var match = findLeasedPendingSmsRow_(body, ["claimed", "send_started"]);
  if (!match.ok) return match;
  if (String(match.values[1] || "") === "send_started") {
    return { ok: true, duplicate: true, status: "send_started", pending_row: match.row };
  }
  match.sheet.getRange(match.row, 2).setValue("send_started");
  match.sheet.getRange(match.row, 14).setValue(new Date());
  return { ok: true, status: "send_started", pending_row: match.row };
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

function getPendingSmsStaleReason_(outboxRow) {
  var phone = normalizePhone_(outboxRow[4]);
  var messageId = String(outboxRow[3] || "");
  var inboundText = normalizeWhitespace_(String(outboxRow[6] || ""));
  var sheet = getSheet_();
  var data = getSheetData_(sheet);
  for (var i = 0; i < data.length; i++) {
    var rowObj = data[i].obj;
    if (normalizePhone_(rowObj[HEADERS.phone]) !== phone) continue;
    if (String(rowObj[HEADERS.human_override] || "").toUpperCase() === "TRUE") return "Human takeover is active";
    if (messageId && String(rowObj[HEADERS.last_message_id] || "") !== messageId) return "A newer inbound message exists";
    // Older ShortSaleLeads layouts do not have last_inbound_text. In that
    // layout, last_message_id remains the authoritative stale-reply guard.
    var hasInboundColumn = Object.prototype.hasOwnProperty.call(rowObj, HEADERS.last_inbound_text);
    var currentInboundText = hasInboundColumn
      ? normalizeWhitespace_(String(rowObj[HEADERS.last_inbound_text] || ""))
      : "";
    if (inboundText && currentInboundText && currentInboundText !== inboundText) return "Latest inbound text changed";
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
  var result = { ok: true, inbound: {}, outbound: {} };
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
      alertSmsOutboxProblem_(sheet, sheetRow, row, "SMS SEND RESULT UNCERTAIN");
      sheet.getRange(sheetRow, 2).setValue("uncertain");
      return;
    }
    if (status === "queued" && created && now - created >= 15 * 60 * 1000 && !row[7]) {
      alertSmsOutboxProblem_(sheet, sheetRow, row, "SMS OUTBOX NOT CLAIMED");
    }
  });
}

function alertSmsOutboxProblem_(sheet, sheetRow, row, reason) {
  var context = getSmsLeadContextByPhone_(row[4]);
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
    smsOutboxWatchdog_: 5
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
