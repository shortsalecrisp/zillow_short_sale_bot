// Diagnostics must not grow the workbook that authorizes and records SMS sends.
function smsDiagnosticTextV37_(value, limit) {
  var text = maskSensitiveDebugText_(String(value == null ? "" : value));
  return text.length > limit ? text.slice(0, limit) + " [truncated]" : text;
}

function smsDiagnosticValueV37_(value, depth) {
  if (depth > 5) return "[depth limit]";
  if (value == null || typeof value === "boolean" || typeof value === "number") return value;
  if (typeof value === "string") {
    var trimmed = value.trim();
    if (/^[\[{]/.test(trimmed)) {
      try { return smsDiagnosticValueV37_(JSON.parse(trimmed), depth + 1); } catch (_) {}
    }
    return smsDiagnosticTextV37_(value, 2000);
  }
  if (Array.isArray(value)) return value.slice(0, 25).map(function(item) {
    return smsDiagnosticValueV37_(item, depth + 1);
  });
  if (typeof value === "object") {
    var safe = {};
    Object.keys(value).slice(0, 40).forEach(function(key) {
      safe[key] = /(?:token|secret|password|authorization|api[_-]?key|signature|cookie)/i.test(key)
        ? "[redacted]" : smsDiagnosticValueV37_(value[key], depth + 1);
    });
    return safe;
  }
  return smsDiagnosticTextV37_(value, 2000);
}

function writeSmsDiagnosticV37_(stage, data) {
  var record = {
    schema: "sms-diagnostic-v37", logged_at: new Date().toISOString(),
    stage: smsDiagnosticTextV37_(stage, 120), data: smsDiagnosticValueV37_(data || {}, 0)
  };
  var encoded = JSON.stringify(record);
  if (encoded.length > 16000) {
    record.data = { request_id: smsDiagnosticTextV37_(data && data.request_id, 200),
      message_id: smsDiagnosticTextV37_(data && data.message_id, 200),
      reason: smsDiagnosticTextV37_(data && data.reason, 500), truncated: true };
    encoded = JSON.stringify(record);
  }
  if (typeof console !== "undefined" && typeof console.log === "function") console.log(encoded);
  else Logger.log(encoded);
  return true;
}

function isSmsStorageErrorV37_(subject, body) {
  return String(subject || "") === "SMS BOT ERROR" &&
    /The document cannot be modified|Service Spreadsheets failed|Spreadsheet service.*(?:failed|unavailable)/i.test(String(body || ""));
}

function sendCoalescedSmsStorageAlertV37_(body, toEmail) {
  var lock = LockService.getScriptLock();
  if (!lock.tryLock(100)) {
    try { writeSmsDiagnosticV37_("storage_incident_lock_busy", { reason: body }); } catch (_) {}
    return false;
  }
  var notice = null;
  var noticeAt = 0;
  try {
    var props = PropertiesService.getScriptProperties();
    var now = Date.now();
    var state = JSON.parse(props.getProperty("SMS_STORAGE_INCIDENT_V37") || "null");
    if (!state || state.status !== "open") state = { status: "open", first_at: now, count: 0 };
    state.count = Math.min(Number(state.count || 0) + 1, 1000000000);
    state.last_at = now;
    if (!state.notice_attempt_at || now - state.notice_attempt_at >= 30 * 60 * 1000) {
      state.notice_attempt_at = now;
      state.notice_state = "attempted";
      noticeAt = now;
      notice = "SMS storage outage: new queue entries may be blocked. This is not agent interest or confirmed delivery.\n\n" +
        "Failures observed in this incident: " + state.count + "\n" +
        "First observed: " + new Date(state.first_at).toISOString() + "\n" +
        "Do not resend blindly. Reconcile the original request and safety checks.\n\n" + smsDiagnosticTextV37_(body, 4000);
    }
    props.setProperty("SMS_STORAGE_INCIDENT_V37", JSON.stringify(state));
    try { writeSmsDiagnosticV37_("storage_incident", { count: state.count, reason: body }); } catch (_) {}
  } finally { lock.releaseLock(); }
  if (!notice) return false;
  MailApp.sendEmail({ to: toEmail, subject: "SMS BOT ERROR - storage outage", body: notice });
  // The attempted marker already prevents blind notification retries on errors.
  if (lock.tryLock(100)) {
    try {
      var latestProps = PropertiesService.getScriptProperties();
      var latest = JSON.parse(latestProps.getProperty("SMS_STORAGE_INCIDENT_V37") || "null");
      if (latest && latest.notice_attempt_at === noticeAt) {
        latest.notice_state = "accepted";
        latestProps.setProperty("SMS_STORAGE_INCIDENT_V37", JSON.stringify(latest));
      }
    } finally { lock.releaseLock(); }
  }
  return true;
}

function noteSmsStorageWriteV37_(requestId) {
  try {
    var props = PropertiesService.getScriptProperties();
    var incident = JSON.parse(props.getProperty("SMS_STORAGE_INCIDENT_V37") || "null");
    if (incident && incident.status === "open") props.setProperty("SMS_STORAGE_RECOVERY_PENDING_V37",
      JSON.stringify({ request_id: String(requestId || ""), written_at: Date.now() }));
  } catch (_) {}
}

function flushSmsStorageRecoveryV37_() {
  var lock = LockService.getScriptLock();
  if (!lock.tryLock(100)) return false;
  var notice = null;
  try {
    var props = PropertiesService.getScriptProperties();
    var incident = JSON.parse(props.getProperty("SMS_STORAGE_INCIDENT_V37") || "null");
    var write = JSON.parse(props.getProperty("SMS_STORAGE_RECOVERY_PENDING_V37") || "null");
    if (!incident || incident.status !== "open" || !write || write.written_at < incident.last_at) return false;
    incident.status = "recovered";
    incident.recovery_notice_state = "attempted";
    incident.recovered_at = write.written_at;
    props.setProperty("SMS_STORAGE_INCIDENT_V37", JSON.stringify(incident));
    props.deleteProperty("SMS_STORAGE_RECOVERY_PENDING_V37");
    notice = { to: props.getProperty("HANDOFF_EMAIL") || "yoni.kutler@ygkutler.com",
      subject: "SMS storage queue writes recovered",
      body: "A normally scheduled follow-up was stored successfully after the storage outage. " +
        "This confirms queue-write recovery, not handset delivery. No blind resend was performed.\n\n" +
        "Coalesced failures: " + incident.count + "\nRequest: " + String(write.request_id || "") };
  } finally { lock.releaseLock(); }
  if (!notice) return false;
  MailApp.sendEmail(notice);
  return true;
}

