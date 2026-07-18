function doPost(e) {
  var payload = parseUnifiedJsonPost_(e);
  var action = payload && payload.action ? String(payload.action).toLowerCase() : "";

  if (payload && payload.token && !isUnifiedSmsAction_(action)) {
    return handleUnifiedVoicePost_(payload);
  }

  return handleUnifiedSmsPost_(e);
}

function isUnifiedSmsAction_(action) {
  var smsActions = {
    incoming_sms: true,
    reply_sent: true,
    manual_reply_sent: true,
    sms_send_failed: true,
    mark_override: true,
    takeover: true
  };
  return !!smsActions[String(action || "").toLowerCase()];
}

function parseUnifiedJsonPost_(e) {
  var raw = e && e.postData && typeof e.postData.contents === "string"
    ? e.postData.contents
    : "";
  if (!raw) return null;

  try {
    return JSON.parse(raw);
  } catch (_) {
    return null;
  }
}

function handleUnifiedVoicePost_(payload) {
  try {
    if (typeof isVoiceBotQueueDryRunPayload_ === "function" && isVoiceBotQueueDryRunPayload_(payload)) {
      validateVoiceBotQueueRefillPayload_(payload);
      return jsonOutput_(processVoiceBotCallQueueDryRun());
    }

    if (typeof isVoiceBotQueueRefillPayload_ === "function" && isVoiceBotQueueRefillPayload_(payload)) {
      validateVoiceBotQueueRefillPayload_(payload);
      return jsonOutput_(processVoiceBotCallQueue());
    }

    return jsonOutput_(handleVoiceBotCallback_(payload));
  } catch (err) {
    var code = err && err.code ? err.code : "voice_callback_error";
    var message = err && err.message ? err.message : String(err);

    try {
      if (typeof voiceBotLog_ === "function") {
        voiceBotLog_({
          event: "voice_callback_rejected",
          code: code,
          message: message
        });
      }
    } catch (_) {}

    return jsonOutput_({
      ok: false,
      code: code,
      error: message
    });
  }
}

function handleUnifiedSmsPost_(e) {
  var requestId = Utilities.getUuid();

  // Self-install the five-minute watchdog on the first live webhook call so
  // a separate one-time manual setup step cannot be forgotten.
  try {
    if (typeof installSmsPendingSendWatchdogTrigger_ === "function") {
      installSmsPendingSendWatchdogTrigger_();
    }
  } catch (_) {}

  try {
    if (typeof appendSmsDebugLog_ === "function") {
      appendSmsDebugLog_("doPost_start", {
        request_id: requestId,
        raw_body: maskSensitiveDebugText_(getRawPostBody_(e)),
        parameters: e && e.parameter ? maskSensitiveDebugText_(safeJsonStringify_(e.parameter)) : ""
      });
    }
  } catch (_) {}

  try {
    var body = parseIncomingRequest_(e);
    validateToken_(body.token);

    var action = body.action || "incoming_sms";
    if (action === "codex_probe") {
      var probe = { ok: true, action: action, has_append: typeof appendSmsDebugLog_ === "function", has_sheet_helper: typeof getSmsSpreadsheet_ === "function" };
      try {
        var ss = getSmsSpreadsheet_();
        probe.spreadsheet_name = ss.getName();
        var debugSheet = ss.getSheetByName("sms_debug_log");
        probe.debug_rows_before = debugSheet ? debugSheet.getLastRow() : -1;
        appendSmsDebugLog_("codex_probe", { request_id: requestId, phone: body.phone || "", message: body.message || "", reason: "Codex deployment probe" });
        debugSheet = ss.getSheetByName("sms_debug_log");
        probe.debug_rows_after = debugSheet ? debugSheet.getLastRow() : -1;
        probe.append_ok = true;
      } catch (probeErr) {
        probe.append_ok = false;
        probe.error = String(probeErr);
        probe.stack = probeErr && probeErr.stack ? probeErr.stack : "";
      }
      return jsonOutput_(probe);
    }


    if (action === "incoming_sms") {
      var ignoredInbound = getUnifiedIgnoredInboundReason_(body);
      if (ignoredInbound) {
        try {
          if (typeof appendSmsDebugLog_ === "function") {
            appendSmsDebugLog_("incoming_sms_ignored", {
              request_id: requestId,
              phone: body.phone || "",
              message: body.message || "",
              reason: ignoredInbound
            });
          }
        } catch (_) {}
        return jsonOutput_(normalizeTaskerPayload_({
          ok: true,
          should_reply: false,
          handoff_needed: false,
          needs_review: false,
          reason: ignoredInbound
        }));
      }

      if (shouldSuppressUnifiedDuplicateInbound_(body)) {
        try {
          if (typeof appendSmsDebugLog_ === "function") {
            appendSmsDebugLog_("incoming_sms_duplicate_suppressed", {
              request_id: requestId,
              phone: body.phone || "",
              message: body.message || "",
              reason: "Duplicate inbound notification suppressed"
            });
          }
        } catch (_) {}
        return jsonOutput_(normalizeTaskerPayload_({
          ok: true,
          should_reply: false,
          handoff_needed: false,
          needs_review: false,
          reason: "Duplicate inbound notification suppressed"
        }));
      }

      var smsResult = handleIncomingSms_(body);
      var normalized = normalizeTaskerPayload_(smsResult);
      normalized.request_id = requestId;
      normalized.message_id = String(body.message_id || "");
      normalized.reply_to_phone = normalizePhone_(body.phone || "");
      if (normalized.should_reply === true && typeof registerPendingSmsSend_ === "function") {
        registerPendingSmsSend_(body, normalized, requestId);
      }
      try {
        if (typeof appendSmsDebugLog_ === "function") {
          appendSmsDebugLog_("incoming_sms_result", {
            request_id: requestId,
            phone: body.phone || "",
            message: body.message || "",
            should_reply: normalized.should_reply_text || "",
            reply_text: normalized.reply_text || "",
            reason: normalized.reason || "",
            lead_status: normalized.lead_status || ""
          });
        }
      } catch (_) {}
      return jsonOutput_(normalized);
    }

    if (action === "reply_sent") {
      var receiptCorrelation = validatePendingSmsSendReceipt_(body);
      if (!receiptCorrelation.ok) {
        try {
          appendSmsDebugLog_("reply_sent_correlation_rejected", {
            callback_request_id: requestId,
            original_request_id: body.request_id || body.sms_request_id || "",
            message_id: body.message_id || "",
            phone: body.phone || "",
            reply_text: body.reply_text || "",
            reason: receiptCorrelation.reason || "No exact pending-send match"
          });
          sendSystemAlertEmail_("SMS BOT RECEIPT REJECTED", safeJsonStringify_(receiptCorrelation));
        } catch (_) {}
        return jsonOutput_({
          ok: false,
          correlation_rejected: true,
          reason: receiptCorrelation.reason || "No exact pending-send match"
        });
      }
      var replySentResult = handleReplySent_(body);
      if (typeof markPendingSmsSendComplete_ === "function") {
        markPendingSmsSendComplete_(body);
      }
      try {
        if (typeof appendSmsDebugLog_ === "function") {
          appendSmsDebugLog_("reply_sent_result", {
            request_id: requestId,
            phone: body.phone || "",
            reply_text: body.reply_text || "",
            result: safeJsonStringify_(replySentResult)
          });
        }
      } catch (_) {}
      return jsonOutput_(replySentResult);
    }

    if (action === "sms_send_failed") {
      if (typeof markPendingSmsSendFailed_ === "function") {
        markPendingSmsSendFailed_(body);
      }
      try {
        sendSystemAlertEmail_("SMS BOT SEND FAILED", safeJsonStringify_(body));
      } catch (_) {}
      return jsonOutput_({ ok: true, action: action, alerted: true });
    }

    if (action === "manual_reply_sent" && typeof handleManualReplySent_ === "function") {
      var manualResult = handleManualReplySent_(body);
      if (typeof markPendingSmsSendManuallyResolved_ === "function") {
        manualResult.pending_send = markPendingSmsSendManuallyResolved_(body);
      }
      try {
        if (typeof appendSmsDebugLog_ === "function") {
          appendSmsDebugLog_("manual_reply_sent_result", {
            request_id: requestId,
            phone: body.phone || "",
            reply_text: body.reply_text || body.message || "",
            result: safeJsonStringify_(manualResult)
          });
        }
      } catch (_) {}
      return jsonOutput_(manualResult);
    }

    if (action === "mark_override" || action === "takeover") {
      var overrideResult = markOverride_(body);
      try {
        if (typeof appendSmsDebugLog_ === "function") {
          appendSmsDebugLog_(action + "_result", {
            request_id: requestId,
            phone: body.phone || "",
            result: safeJsonStringify_(overrideResult)
          });
        }
      } catch (_) {}
      return jsonOutput_(overrideResult);
    }

    try {
      if (typeof appendSmsDebugLog_ === "function") {
        appendSmsDebugLog_("unknown_action", {
          request_id: requestId,
          action: action,
          body: maskSensitiveDebugText_(safeJsonStringify_(body))
        });
      }
    } catch (_) {}
    return jsonOutput_({ ok: false, error: "Unknown action" });
  } catch (err) {
    try {
      if (typeof appendSmsDebugLog_ === "function") {
        appendSmsDebugLog_("doPost_error", {
          request_id: requestId,
          error: String(err),
          stack: err && err.stack ? err.stack : ""
        });
      }
    } catch (_) {}

    if (typeof isUnauthorizedError_ === "function" && isUnauthorizedError_(err)) {
      return jsonOutput_({
        ok: false,
        error: "Unauthorized"
      });
    }

    try {
      sendSystemAlertEmail_("SMS BOT ERROR", String(err) + "\n\n" + (err && err.stack ? err.stack : ""));
    } catch (_) {}

    return jsonOutput_({
      ok: false,
      error: String(err),
      stack: err && err.stack ? err.stack : ""
    });
  }
}

function getUnifiedIgnoredInboundReason_(body) {
  var phone = String(body && body.phone || "").trim();
  var message = String(body && body.message || "").trim();
  var phoneLower = phone.toLowerCase();
  var messageLower = message.toLowerCase();
  var phoneDigits = phone.replace(/\D/g, "");

  if (!phone || !message) {
    return "Missing phone or message ignored";
  }

  if (
    phoneLower === "device pairing" ||
    phoneLower === "messages is doing work in the background" ||
    messageLower === "messages is doing work in the background" ||
    messageLower === "your messages are available on the device you've paired"
  ) {
    return "Google Messages system notification ignored";
  }

  if (phoneDigits.length < 10) {
    return "Non-phone notification ignored";
  }

  return "";
}

function shouldSuppressUnifiedDuplicateInbound_(body) {
  var action = String(body && body.action || "incoming_sms").toLowerCase();
  if (action !== "incoming_sms") return false;

  var phone = String(body && body.phone || "");
  var message = normalizeWhitespace_(String(body && body.message || ""));
  var phoneDigits = phone.replace(/\D/g, "").replace(/^1(?=\d{10}$)/, "");
  if (!phoneDigits || !message) return false;

  var keySource = phoneDigits + "|" + message.toLowerCase();
  var digest = Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_256, keySource);
  var key = "sms_inbound_" + Utilities.base64EncodeWebSafe(digest).slice(0, 64);
  var cache = CacheService.getScriptCache();

  if (cache.get(key)) {
    return true;
  }

  cache.put(key, "1", 600);
  return false;
}


function getRawPostBody_(e) {
  return e && e.postData && typeof e.postData.contents === "string" ? e.postData.contents : "";
}

function safeJsonStringify_(value) {
  try {
    return JSON.stringify(value);
  } catch (err) {
    return String(value);
  }
}

function maskSensitiveDebugText_(value) {
  return String(value || "")
    .replace(/(token=)[^&\s]+/gi, "$1[redacted]")
    .replace(/(\"token\"\s*:\s*\")[^\"]+(\")/gi, "$1[redacted]$2")
    .replace(/Bearer\s+[A-Za-z0-9._-]+/g, "Bearer [redacted]");
}

function appendSmsDebugLog_(stage, data) {
  data = data || {};
  var ss = getSmsSpreadsheet_();
  var sheet = ss.getSheetByName("sms_debug_log") || ss.insertSheet("sms_debug_log");
  var headers = ["logged_at", "stage", "request_id", "phone", "message", "should_reply", "reply_text", "reason", "lead_status", "details_json"];
  if (sheet.getLastRow() === 0) {
    sheet.appendRow(headers);
  } else {
    var existing = sheet.getRange(1, 1, 1, headers.length).getValues()[0];
    if (existing.join("|") !== headers.join("|")) {
      sheet.getRange(1, 1, 1, headers.length).setValues([headers]);
    }
  }
  sheet.appendRow([
    new Date().toISOString(),
    stage || "",
    data.request_id || "",
    data.phone || "",
    data.message || "",
    data.should_reply || "",
    data.reply_text || "",
    data.reason || "",
    data.lead_status || "",
    maskSensitiveDebugText_(safeJsonStringify_(data))
  ]);
}

// Records a reply before Tasker attempts to send it. This makes a missing
// reply_sent callback visible instead of silently dropping the send.
function registerPendingSmsSend_(body, normalized, requestId) {
  var ss = getSmsSpreadsheet_();
  var sheet = ss.getSheetByName("sms_pending_sends") || ss.insertSheet("sms_pending_sends");
  var headers = ["created_at", "status", "request_id", "message_id", "phone", "reply_text", "inbound_text", "last_alert_at"];
  ensurePendingSmsHeaders_(sheet, headers);

  var phone = String(body && body.phone || "").trim();
  var replyText = String(normalized && normalized.reply_text || "").trim();
  var messageId = String(body && body.message_id || "").trim();
  var rows = sheet.getLastRow() > 1 ? sheet.getRange(2, 1, sheet.getLastRow() - 1, headers.length).getValues() : [];

  // Retries of the same webhook must not create multiple pending records.
  for (var i = rows.length - 1; i >= 0; i--) {
    if (String(rows[i][2] || "") === requestId ||
        (messageId && String(rows[i][3] || "") === messageId && String(rows[i][1] || "") === "pending")) {
      return;
    }
  }

  sheet.appendRow([new Date(), "pending", requestId, messageId, phone, replyText,
    String(body && body.message || ""), ""]);
}

function markPendingSmsSendComplete_(body) {
  return updatePendingSmsSendStatus_(body, "sent", "");
}

function markPendingSmsSendFailed_(body) {
  return updatePendingSmsSendStatus_(body, "failed", String(body && body.error || body && body.reason || ""));
}

function markPendingSmsSendManuallyResolved_(body) {
  var ss = getSmsSpreadsheet_();
  var sheet = ss.getSheetByName("sms_pending_sends");
  if (!sheet || sheet.getLastRow() < 2) return { ok: false, reason: "No pending-send ledger is available" };

  var headers = ["created_at", "status", "request_id", "message_id", "phone", "reply_text", "inbound_text", "last_alert_at"];
  ensurePendingSmsHeaders_(sheet, headers);
  var rows = sheet.getRange(2, 1, sheet.getLastRow() - 1, headers.length).getValues();
  var phone = normalizePhone_(body && body.phone || "");
  var replyText = normalizePendingSmsReply_(body && (body.pending_reply_text || body.reply_text || body.message) || "");

  for (var i = rows.length - 1; i >= 0; i--) {
    var status = String(rows[i][1] || "");
    if (status !== "pending" && status !== "alerted") continue;
    if (normalizePhone_(rows[i][4]) !== phone || normalizePendingSmsReply_(rows[i][5]) !== replyText) continue;
    sheet.getRange(i + 2, 2).setValue("manual_sent");
    sheet.getRange(i + 2, 8).setValue(new Date());
    return { ok: true, pending_row: i + 2, status: "manual_sent" };
  }
  return { ok: false, reason: "No pending or alerted send matched the manual reply" };
}

function normalizePendingSmsReply_(value) {
  return normalizeWhitespace_(String(value || ""))
    .replace(/[\\u200B-\\u200D\\uFEFF]/g, "")
    .toLowerCase();
}

function findPendingSmsRow_(rows, body) {
  var requestId = String(body && (body.request_id || body.sms_request_id) || "").trim();
  var messageId = String(body && body.message_id || "").trim();
  var phone = normalizePhone_(body && body.phone || "");
  var replyText = normalizePendingSmsReply_(body && body.reply_text || "");

  function identifiersMatch_(row) {
    if (requestId && String(row[2] || "") !== requestId) return false;
    if (messageId && String(row[3] || "") !== messageId) return false;
    if (phone && normalizePhone_(row[4]) !== phone) return false;
    if (replyText && normalizePendingSmsReply_(row[5]) !== replyText) return false;
    return true;
  }

  if (requestId || messageId) {
    for (var i = rows.length - 1; i >= 0; i--) {
      if (String(rows[i][1] || "") !== "pending") continue;
      var hasRequestedId = (requestId && String(rows[i][2] || "") === requestId) ||
        (messageId && String(rows[i][3] || "") === messageId);
      if (hasRequestedId) return identifiersMatch_(rows[i]) ? i : -2;
    }
    return -1;
  }

  // Backward-compatible transition path for the currently installed Tasker
  // restore. It is exact on both destination and reply, never phone alone.
  if (phone && replyText) {
    for (var j = rows.length - 1; j >= 0; j--) {
      if (String(rows[j][1] || "") !== "pending") continue;
      if (normalizePhone_(rows[j][4]) === phone &&
          normalizePendingSmsReply_(rows[j][5]) === replyText) {
        return j;
      }
    }
  }

  return -1;
}

function validatePendingSmsSendReceipt_(body) {
  var ss = getSmsSpreadsheet_();
  var sheet = ss.getSheetByName("sms_pending_sends");
  if (!sheet || sheet.getLastRow() < 2) {
    return { ok: false, reason: "No pending-send ledger is available" };
  }

  var headers = ["created_at", "status", "request_id", "message_id", "phone", "reply_text", "inbound_text", "last_alert_at"];
  ensurePendingSmsHeaders_(sheet, headers);
  var rows = sheet.getRange(2, 1, sheet.getLastRow() - 1, headers.length).getValues();
  var matchIndex = findPendingSmsRow_(rows, body);
  if (matchIndex === -2) {
    return { ok: false, reason: "Receipt identifiers matched a pending send but phone or reply text did not" };
  }
  if (matchIndex < 0) {
    return { ok: false, reason: "Receipt did not match an exact pending phone/reply or correlated request/message ID" };
  }
  return { ok: true, pending_row: matchIndex + 2 };
}

function updatePendingSmsSendStatus_(body, status, note) {
  var ss = getSmsSpreadsheet_();
  var sheet = ss.getSheetByName("sms_pending_sends");
  if (!sheet || sheet.getLastRow() < 2) return { ok: false, reason: "No pending-send ledger is available" };

  var headers = ["created_at", "status", "request_id", "message_id", "phone", "reply_text", "inbound_text", "last_alert_at"];
  ensurePendingSmsHeaders_(sheet, headers);
  var rows = sheet.getRange(2, 1, sheet.getLastRow() - 1, headers.length).getValues();
  var matchIndex = findPendingSmsRow_(rows, body);

  if (matchIndex >= 0) {
    var matchRow = matchIndex + 2;
    sheet.getRange(matchRow, 2).setValue(status);
    sheet.getRange(matchRow, 8).setValue(note || new Date());
    return { ok: true, pending_row: matchRow, status: status };
  }
  return { ok: false, reason: matchIndex === -2 ? "Pending-send identifiers conflict with phone or reply text" : "No exact pending-send match" };
}
function ensurePendingSmsHeaders_(sheet, headers) {
  if (sheet.getLastRow() === 0) {
    sheet.getRange(1, 1, 1, headers.length).setValues([headers]);
    return;
  }

  var existing = sheet.getRange(1, 1, 1, headers.length).getValues()[0];
  if (existing.join("|") !== headers.join("|")) {
    sheet.getRange(1, 1, 1, headers.length).setValues([headers]);
  }
}

// Install this as a time-driven trigger once. It alerts only once per missed
// callback, so a transient Tasker delay cannot create an email storm.
function smsPendingSendWatchdog_() {
  var ss = getSmsSpreadsheet_();
  var sheet = ss.getSheetByName("sms_pending_sends");
  if (!sheet || sheet.getLastRow() < 2) return;

  var headers = ["created_at", "status", "request_id", "message_id", "phone", "reply_text", "inbound_text", "last_alert_at"];
  ensurePendingSmsHeaders_(sheet, headers);
  var rows = sheet.getRange(2, 1, sheet.getLastRow() - 1, headers.length).getValues();
  var now = Date.now();

  rows.forEach(function(row, index) {
    if (String(row[1] || "") !== "pending") return;
    var created = new Date(row[0]).getTime();
    if (!created || now - created < 10 * 60 * 1000) return;

    var phone = String(row[4] || "");
    var history = [];
    try {
      var data = getSheetData_(getSheet_());
      var match = data.find(function(item) { return normalizePhone_(item.obj[HEADERS.phone]) === normalizePhone_(phone); });
      if (match) history = getHistoryArray_(match.obj[HEADERS.history_json]);
    } catch (_) {}

    try {
      sendHandoffEmail_({
        handoff_type: "SMS SEND NOT CONFIRMED",
        phone: phone,
        last_message: row[5] || "",
        history: history
      });
      sheet.getRange(index + 2, 2).setValue("alerted");
      sheet.getRange(index + 2, 8).setValue(new Date());
    } catch (err) {
      sheet.getRange(index + 2, 8).setValue(String(err));
    }
  });
}

function installSmsPendingSendWatchdogTrigger_() {
  var triggers = ScriptApp.getProjectTriggers();
  var exists = triggers.some(function(trigger) {
    return trigger.getHandlerFunction() === "smsPendingSendWatchdog_";
  });
  if (!exists) {
    ScriptApp.newTrigger("smsPendingSendWatchdog_").timeBased().everyMinutes(5).create();
  }
  return { ok: true, installed: true };
}
