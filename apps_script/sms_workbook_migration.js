var SMS_MIGRATION_STATE_V1_ = "sms_workbook_migration_v1";
var SMS_MIGRATION_ROUTE_V1_ = "sms_operational_workbook_v1";
var SMS_MIGRATION_EVENT_V1_ = "sms_migration_event_v1_";
var SMS_MIGRATION_SOURCE_V1_ = "1rbffUky_CpaC0O9HxtChBlIet4BYB1Tu1ul1j8R_pSE";
var SMS_MIGRATION_TARGET_V1_ = "12UzsoQCo4W0WB_lNl3BjKpQ_wXNhEH7xegkFRVu2M70";
var SMS_MIGRATION_ADMIN_SHA256_V1_ = "8c61e27d9f774004af47c094ff98bc4842588ed535ce141f7d58eb82a4686712";
var SMS_MIGRATION_RESPONSE_V1_ = null;
var SMS_MIGRATION_REPLAY_V1_ = false;

function smsMigrationStateV1_() {
  var raw = PropertiesService.getScriptProperties().getProperty(SMS_MIGRATION_STATE_V1_);
  if (!raw) return { active: false };
  var state = JSON.parse(raw);
  if (typeof state.active !== "boolean") throw new Error("Invalid migration state; no processing authorized");
  return state;
}

function smsMigrationActiveV1_() {
  return smsMigrationStateV1_().active === true;
}

function smsMigrationWorkbookIdV1_() {
  var id = PropertiesService.getScriptProperties().getProperty(SMS_MIGRATION_ROUTE_V1_) || SMS_MIGRATION_SOURCE_V1_;
  if (id !== SMS_MIGRATION_SOURCE_V1_ && id !== SMS_MIGRATION_TARGET_V1_) throw new Error("Invalid operational workbook selector");
  return id;
}

function smsMigrationCanonicalV1_(value) {
  if (Array.isArray(value)) return value.map(smsMigrationCanonicalV1_);
  if (!value || typeof value !== "object") return value;
  var out = Object.create(null);
  Object.keys(value).sort().forEach(function(key) {
    if (!/^(token|authorization|access_token|api_key|password|secret)$/i.test(key)) out[key] = smsMigrationCanonicalV1_(value[key]);
  });
  return out;
}

function smsMigrationHashV1_(value) {
  return Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_256, value, Utilities.Charset.UTF_8)
    .map(function(b) { return ("0" + ((b + 256) % 256).toString(16)).slice(-2); }).join("");
}

function smsMigrationBytesV1_(value) {
  return Utilities.newBlob(value).getBytes().length;
}

function smsMigrationEventsV1_() {
  var props = PropertiesService.getScriptProperties().getProperties();
  return Object.keys(props).filter(function(key) {
    return key.indexOf(SMS_MIGRATION_EVENT_V1_) === 0 && /_manifest$/.test(key);
  }).map(function(key) { return { key: key, value: JSON.parse(props[key]) }; })
    .sort(function(a, b) { return a.value.received_at.localeCompare(b.value.received_at) || a.key.localeCompare(b.key); });
}

function smsMigrationStatusV1_() {
  var state = smsMigrationStateV1_(), counts = {}, all = PropertiesService.getScriptProperties().getProperties(), bytes = 0;
  smsMigrationEventsV1_().forEach(function(item) { counts[item.value.status] = (counts[item.value.status] || 0) + 1; });
  Object.keys(all).forEach(function(key) { bytes += smsMigrationBytesV1_(key) + smsMigrationBytesV1_(all[key]); });
  return { ok: true, migration: state, workbook_id: smsMigrationWorkbookIdV1_(), buffered: counts, property_bytes: bytes,
    sender_calls: 0, tasker_changes: 0 };
}

function smsMigrationBufferV1_(body, kind) {
  var clean = smsMigrationCanonicalV1_(body), raw = JSON.stringify(clean), digest = smsMigrationHashV1_(raw);
  var stable = clean.message_id || clean.request_id || clean.sms_request_id || (clean.received_at && clean.phone);
  var id = stable ? digest : digest + "_" + Utilities.getUuid().replace(/-/g, "");
  var prefix = SMS_MIGRATION_EVENT_V1_ + id, key = prefix + "_manifest";
  var encoded = Utilities.base64Encode(raw, Utilities.Charset.UTF_8), chunks = [], writes = {};
  if (encoded.length > 96000) throw new Error("Migration event too large; event NOT acknowledged");
  for (var offset = 0; offset < encoded.length; offset += 7000) chunks.push(encoded.slice(offset, offset + 7000));
  var lock = LockService.getScriptLock();
  if (!lock.tryLock(5000)) throw new Error("Migration buffer busy; event NOT acknowledged");
  try {
    if (!smsMigrationActiveV1_()) return null;
    var props = PropertiesService.getScriptProperties(), existing = props.getProperty(key);
    if (existing) {
      smsMigrationReadEventV1_(key, JSON.parse(existing));
      return { ok: true, queued: true, migration_buffered: true, duplicate: true, should_reply: false, should_send: false,
        should_send_text: "false", delivery_reconciled: false };
    }
    var record = { id: id, kind: kind || "sms", received_at: new Date().toISOString(), sha256: digest,
      chunks: chunks.length, prefix: prefix, status: "queued" };
    chunks.forEach(function(part, index) { writes[prefix + "_" + index] = part; });
    writes[key] = JSON.stringify(record);
    var all = props.getProperties(), used = 0;
    Object.keys(writes).forEach(function(k) { all[k] = writes[k]; });
    Object.keys(all).forEach(function(k) { used += smsMigrationBytesV1_(k) + smsMigrationBytesV1_(all[k]); });
    if (used > 430000) throw new Error("Migration buffer capacity reached; event NOT acknowledged");
    delete writes[key];
    props.setProperties(writes, false);
    // The manifest is the commit marker. Never acknowledge an unverified prefix.
    smsMigrationReadEventV1_(key, record);
    props.setProperty(key, JSON.stringify(record));
    smsMigrationReadEventV1_(key, JSON.parse(props.getProperty(key)));
    return { ok: true, queued: true, migration_buffered: true, should_reply: false, should_send: false,
      should_send_text: "false", delivery_reconciled: false };
  } finally { lock.releaseLock(); }
}

function smsMigrationReadEventV1_(key, record) {
  if (key !== record.prefix + "_manifest" || record.prefix.indexOf(SMS_MIGRATION_EVENT_V1_) !== 0 ||
      !/^[a-f0-9]{64}$/.test(record.sha256) || record.chunks < 1 || record.chunks > 14) throw new Error("Invalid migration event reference");
  var props = PropertiesService.getScriptProperties(), parts = [];
  for (var i = 0; i < record.chunks; i++) {
    var part = props.getProperty(record.prefix + "_" + i);
    if (part === null) throw new Error("Incomplete migration event; no replay authorized");
    parts.push(part);
  }
  var raw = Utilities.newBlob(Utilities.base64Decode(parts.join(""))).getDataAsString("UTF-8");
  if (smsMigrationHashV1_(raw) !== record.sha256) throw new Error("Migration event hash mismatch");
  return JSON.parse(raw);
}

function smsMigrationControlV1_(body) {
  var props = PropertiesService.getScriptProperties(), command = String(body.migration_control || "");
  if (!body.api_key || body.api_key === props.getProperty("ALLOWED_TOKEN") || smsMigrationHashV1_(String(body.api_key)) !== SMS_MIGRATION_ADMIN_SHA256_V1_) throw new Error("Migration administrator authorization required");
  if (command === "status") return smsMigrationStatusV1_();
  var lock = LockService.getScriptLock();
  if (!lock.tryLock(5000)) throw new Error("Migration control busy");
  try {
    var state = smsMigrationStateV1_();
    if (command === "begin") {
      var day = Utilities.formatDate(new Date(), "America/New_York", "yyyy-MM-dd"), hour = Number(Utilities.formatDate(new Date(), "America/New_York", "HH"));
      if (day !== "2026-11-18" || hour < 8 || hour >= 20 || body.approval !== "owner-approved-original-rebuild-20261118") throw new Error("Migration begin not authorized for this window");
      if (state.active || smsMigrationEventsV1_().length || smsMigrationWorkbookIdV1_() !== SMS_MIGRATION_SOURCE_V1_) throw new Error("Existing migration/ingress requires reconciliation");
      state = { active: true, epoch: Utilities.getUuid(), started_at: new Date().toISOString(), target_ready: false,
        source: SMS_MIGRATION_SOURCE_V1_, target: SMS_MIGRATION_TARGET_V1_ };
      props.setProperty(SMS_MIGRATION_STATE_V1_, JSON.stringify(state));
    } else {
      if (!state.active || body.epoch !== state.epoch) throw new Error("Migration epoch mismatch");
      if (command === "target_ready") {
        if (!/^[a-f0-9]{64}$/.test(String(body.reconciliation_sha256 || ""))) throw new Error("Missing reconciliation receipt");
        if (state.target_ready && state.reconciliation_sha256 !== body.reconciliation_sha256) throw new Error("Target receipt already pinned");
        var target = SpreadsheetApp.openById(SMS_MIGRATION_TARGET_V1_);
        if (!target || target.getId() !== SMS_MIGRATION_TARGET_V1_ || !target.getSheetByName("Sheet1")) throw new Error("Restored target unavailable");
        props.setProperty(SMS_MIGRATION_ROUTE_V1_, SMS_MIGRATION_TARGET_V1_);
        state.target_ready = true; state.reconciliation_sha256 = body.reconciliation_sha256;
        props.setProperty(SMS_MIGRATION_STATE_V1_, JSON.stringify(state));
      } else if (command === "finish" || command === "abort") {
        if (Object.keys(props.getProperties()).some(function(key) { return key.indexOf(SMS_MIGRATION_EVENT_V1_) === 0; })) throw new Error("Buffered, incomplete or uncertain events remain; cannot resume");
        if ((command === "finish") !== !!state.target_ready) throw new Error("Wrong migration close path");
        state.active = false; state.closed_at = new Date().toISOString(); state.close_reason = command;
        props.setProperty(SMS_MIGRATION_STATE_V1_, JSON.stringify(state));
      } else if (command !== "drain") throw new Error("Unknown migration command");
    }
  } finally { lock.releaseLock(); }
  if (command === "drain") return smsMigrationDrainV1_(body);
  return smsMigrationStatusV1_();
}

function smsMigrationDrainV1_(body) {
  var state = smsMigrationStateV1_();
  if (!state.active || body.epoch !== state.epoch || (!state.target_ready && body.rollback_source !== true)) throw new Error("Migration replay not authorized");
  var props = PropertiesService.getScriptProperties(), entries = smsMigrationEventsV1_(), completed = 0;
  for (var i = 0; i < entries.length && i < 5; i++) {
    var item = entries[i], lock = LockService.getScriptLock();
    if (!lock.tryLock(5000)) throw new Error("Migration event busy");
    var record;
    try {
      record = JSON.parse(props.getProperty(item.key));
      if (record.status !== "queued") throw new Error("Uncertain migration event requires native reconciliation; no retry authorized");
      record.status = "applying"; props.setProperty(item.key, JSON.stringify(record));
    } finally { lock.releaseLock(); }
    try {
      var payload = smsMigrationReadEventV1_(item.key, record), result;
      SMS_MIGRATION_REPLAY_V1_ = true; SMS_MIGRATION_RESPONSE_V1_ = null;
      if (record.kind === "voice") {
        payload.token = VOICE_BOT_SHARED_TOKEN;
        result = handleVoiceBotCallback_(payload);
      } else {
        payload.token = props.getProperty("ALLOWED_TOKEN");
        // Restoring ingress only appends to the existing durable queue. It
        // never runs recognition, reply generation or an outbound worker.
        if (payload.action === "incoming_sms") payload.action = "enqueue_incoming_sms";
        if (!payload.received_at && payload.action === "enqueue_incoming_sms") payload.received_at = record.received_at;
        result = JSON.parse(handleUnifiedSmsPost_({ postData: { contents: JSON.stringify(payload) } }).getContent());
      }
      if (!result || result.ok === false) throw new Error("Migration event application not confirmed");
      for (var p = 0; p < record.chunks; p++) props.deleteProperty(record.prefix + "_" + p);
      props.deleteProperty(item.key); completed++;
    } catch (err) {
      record.status = "uncertain"; props.setProperty(item.key, JSON.stringify(record));
      throw new Error("Migration event outcome uncertain; inspect native state before any further action");
    } finally { SMS_MIGRATION_REPLAY_V1_ = false; SMS_MIGRATION_RESPONSE_V1_ = null; }
  }
  var status = smsMigrationStatusV1_(); status.applied = completed; return status;
}

function smsMigrationGateRequestV1_(body) {
  try { return smsMigrationGateRequestCoreV1_(body); }
  catch (err) {
    SMS_MIGRATION_RESPONSE_V1_ = { ok: false, retryable: true, should_send: false, should_send_text: "false", should_reply: false,
      code: "SMS_MIGRATION_NOT_ACKNOWLEDGED", reason: String(err && err.message || "Migration request not completed") };
    return Object.assign({}, body, { action: "transport_health" });
  }
}

function smsMigrationGateRequestCoreV1_(body) {
  if (SMS_MIGRATION_REPLAY_V1_) return body;
  SMS_MIGRATION_RESPONSE_V1_ = null;
  var expected = PropertiesService.getScriptProperties().getProperty("ALLOWED_TOKEN");
  if (body && body.migration_control && body.action === "outbox_status" && expected) {
    SMS_MIGRATION_RESPONSE_V1_ = smsMigrationControlV1_(body);
    return Object.assign({}, body, { token: expected, action: "transport_health" });
  }
  if (!body || !expected || body.token !== expected) return body;
  if (!smsMigrationActiveV1_()) return body;
  var action = String(body.action || "incoming_sms").toLowerCase(), response;
  if (action === "tasker_heartbeat" || action === "claim_pending_send" || action === "send_started") {
    if (typeof recordTaskerTransportActivityV12_ === "function") recordTaskerTransportActivityV12_(action === "tasker_heartbeat" ? "heartbeat" : action === "claim_pending_send" ? "claim" : "send_started", body);
    var snapshot = { action: "enqueue_incoming_sms", phone: String(body.snapshot_phone || ""), message: String(body.snapshot_message || ""),
      received_at: String(body.snapshot_received_at || ""), message_id: String(body.snapshot_message_id || ""), transport_version: String(body.transport_version || "") };
    if (!getUnifiedIgnoredInboundReason_(snapshot)) smsMigrationBufferV1_(snapshot, "sms");
  }
  if (["incoming_sms", "enqueue_incoming_sms", "reply_sent", "sms_send_failed", "manual_reply_sent", "mark_override", "takeover"].indexOf(action) !== -1) {
    response = smsMigrationBufferV1_(Object.assign({}, body, { action: action }), "sms");
    if (response === null) return body;
  } else {
    response = { ok: action === "tasker_heartbeat" || action === "claim_pending_send" || action === "transport_health" || action === "outbox_status",
      retryable: true, retry_after_seconds: 30, eligible: false, queued: false, should_send: false, should_send_text: "false", should_reply: false,
      status: "workbook_maintenance", code: "SMS_WORKBOOK_MAINTENANCE", reason: "Workbook maintenance; no SMS authorized", server_time: new Date().toISOString() };
  }
  SMS_MIGRATION_RESPONSE_V1_ = response;
  return Object.assign({}, body, { action: "transport_health" });
}

function smsMigrationOutputV1_(obj) {
  if (SMS_MIGRATION_RESPONSE_V1_ === null) return obj;
  var response = SMS_MIGRATION_RESPONSE_V1_; SMS_MIGRATION_RESPONSE_V1_ = null; return response;
}

function smsMigrationVoiceGateV1_(payload) {
  if (SMS_MIGRATION_REPLAY_V1_ || !smsMigrationActiveV1_()) return null;
  if ((typeof isVoiceBotQueueDryRunPayload_ === "function" && isVoiceBotQueueDryRunPayload_(payload)) ||
      (typeof isVoiceBotQueueRefillPayload_ === "function" && isVoiceBotQueueRefillPayload_(payload))) {
    validateVoiceBotQueueRefillPayload_(payload);
    return { ok: false, retryable: true, queued: false, reason: "Workbook maintenance; no calls authorized" };
  }
  validateVoiceBotPayload_(payload);
  return smsMigrationBufferV1_(payload, "voice");
}
