const HEADERS = {
  agent_name: "agent_name",
  last_name: "last_name",
  phone: "phone",
  email: "email",
  listing_address: "listing_address",
  city: "city",
  state: "st",
  zip: "zip",
  initial_text_sent: "initial_text",
  followup_text_sent: "follow_up",
  response_status: "response_status",
  mailshake_status: "mailshake_status",
  last_inbound_text: "last_inbound_text",
  last_inbound_at: "last_inbound_at",
  last_outbound_text: "last_outbound_text",
  conversation_summary: "conversation_summary",
  ai_state: "ai_state",
  last_contact_time: "last_contact_time",
  call_booking_status: "call_booking_status",
  callback_requested: "callback_requested",
  callback_time: "callback_time",
  handoff_flag: "handoff_flag",
  history_json: "history_json",
  auto_reply_count: "auto_reply_count",
  human_override: "human_override",
  last_message_id: "last_message_id",
};

function doPostLegacy_(e) {
  try {
    const payload = JSON.parse(e.postData.contents);
    if (payload && payload.token) {
      return jsonOutput_(handleVoiceBotCallback_(payload));
    }
  } catch (err) {
    // ignore parse errors and continue into normal SMS bot handling
  }

  try {
    const body = parseIncomingRequest_(e);
    validateToken_(body.token);

    const action = body.action || "incoming_sms";

    if (action === "incoming_sms") {
      const result = handleIncomingSms_(body);
      return jsonOutput_(normalizeTaskerPayload_(result));
    }
    if (action === "reply_sent") {
      return jsonOutput_(handleReplySent_(body));
    }

    if (action === "mark_override") {
      return jsonOutput_(markOverride_(body));
    }

    return jsonOutput_({ ok: false, error: "Unknown action" });
  } catch (err) {
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

function doGet(e) {
  try {
    const params = e && e.parameter ? e.parameter : {};
    const action = String(params.action || "").toLowerCase();
    if (action === "approve_info_email") {
      return htmlOutput_(approvePendingInfoEmail_(params.id));
    }

    return htmlOutput_({
      ok: false,
      message: "Unknown action"
    });
  } catch (err) {
    return htmlOutput_({
      ok: false,
      message: String(err)
    });
  }
}

function normalizeHandledInboundText_(value) {
  return normalizeWhitespace_(String(value || "")).toLowerCase();
}

function canonicalizeRepeatedCompleteInboundForDedupe_(value) {
  const normalized = normalizeHandledInboundText_(value);
  const tokens = normalized ? normalized.split(" ") : [];
  if (tokens.length < 6) return normalized;

  // Tasker can occasionally coalesce the same complete SMS bubble two or
  // more times. Collapse only exact consecutive repetitions with a
  // substantive (3+ token) base message, and use this value for dedupe only.
  for (let unitLength = 3; unitLength <= Math.floor(tokens.length / 2); unitLength += 1) {
    if (tokens.length % unitLength !== 0) continue;
    const base = tokens.slice(0, unitLength);
    let repeated = true;
    for (let index = unitLength; index < tokens.length; index += 1) {
      if (tokens[index] !== base[index % unitLength]) {
        repeated = false;
        break;
      }
    }
    if (repeated) return base.join(" ");
  }
  return normalized;
}

function historyHasConfirmedReplyAfterInbound_(rowObj, inboundText) {
  const currentText = canonicalizeRepeatedCompleteInboundForDedupe_(inboundText);
  if (!currentText) return false;

  const history = getHistoryArray_(rowObj && rowObj[HEADERS.history_json]);
  for (let i = history.length - 1; i >= 0; i -= 1) {
    const entry = history[i] || {};
    if (String(entry.role || "").toLowerCase() !== "agent" ||
        canonicalizeRepeatedCompleteInboundForDedupe_(entry.text) !== currentText) {
      continue;
    }

    for (let j = i + 1; j < history.length; j += 1) {
      const later = history[j] || {};
      if (String(later.role || "").toLowerCase() === "assistant" &&
          normalizeWhitespace_(String(later.text || ""))) {
        return true;
      }
    }
    return false;
  }
  return false;
}

function isIntentionalNoReplyDisposition_(rowObj, inboundText) {
  const aiState = String(rowObj && rowObj[HEADERS.ai_state] || "").toLowerCase();
  const handoffFlag = String(rowObj && rowObj[HEADERS.handoff_flag] || "").toUpperCase() === "TRUE";
  const humanOverride = String(rowObj && rowObj[HEADERS.human_override] || "").toUpperCase() === "TRUE";
  return aiState === "handoff" || handoffFlag ||
    (humanOverride && aiState !== "done") ||
    isOptOutSignal_(inboundText) ||
    isFinalCourtesyReply_(inboundText);
}

function isDurableHandledDuplicateInbound_(rowObj, inboundText) {
  const priorText = canonicalizeRepeatedCompleteInboundForDedupe_(rowObj && rowObj[HEADERS.last_inbound_text]);
  const currentText = canonicalizeRepeatedCompleteInboundForDedupe_(inboundText);
  if (!priorText || !currentText || priorText !== currentText) return false;
  const postHandoffCallbackUpdate = typeof isPostHandoffCallbackUpdate_ === "function" &&
    isPostHandoffCallbackUpdate_(rowObj, inboundText);
  if (isSchedulingSignal_(inboundText) || postHandoffCallbackUpdate) return false;
  // A repeated substantive question can be intentional (for example, asking
  // the exact fee again after a generic payment explanation). Never let
  // text-only replay protection hide that follow-up.
  if (isSubstantiveFollowupSignal_(inboundText) || isPaymentOrFeeQuestionSignal_(inboundText)) {
    return false;
  }

  const responseText = canonicalizeRepeatedCompleteInboundForDedupe_(rowObj && rowObj[HEADERS.response_status]);
  if (responseText !== currentText) return false;

  return historyHasConfirmedReplyAfterInbound_(rowObj, inboundText) ||
    isIntentionalNoReplyDisposition_(rowObj, inboundText);
}

function isRecentDuplicateInboundText_(rowObj, inboundText, receivedAt) {
  const priorText = canonicalizeRepeatedCompleteInboundForDedupe_(rowObj && rowObj[HEADERS.last_inbound_text]);
  const currentText = canonicalizeRepeatedCompleteInboundForDedupe_(inboundText);
  if (!priorText || !currentText || priorText !== currentText) return false;
  if (isSubstantiveFollowupSignal_(inboundText) || isPaymentOrFeeQuestionSignal_(inboundText)) {
    return false;
  }

  const lastContactRaw = rowObj && (
    rowObj[HEADERS.last_inbound_at] || rowObj[HEADERS.last_contact_time]
  );
  const lastContactTs = lastContactRaw instanceof Date
    ? lastContactRaw.getTime()
    : Date.parse(String(lastContactRaw || ""));
  const receivedTs = receivedAt instanceof Date
    ? receivedAt.getTime()
    : Date.parse(String(receivedAt || ""));

  if (Number.isNaN(lastContactTs) || Number.isNaN(receivedTs)) return false;
  return Math.abs(receivedTs - lastContactTs) <= 5 * 60 * 1000;
}
function parseIncomingRequest_(e) {
  const raw = e && e.postData && typeof e.postData.contents === "string"
    ? e.postData.contents
    : "";

  if (raw) {
    try {
      return JSON.parse(raw);
    } catch (_) {}

    const canonicalTaskerForm = parseCanonicalTaskerFormBody_(raw);
    if (canonicalTaskerForm) return canonicalTaskerForm;
  }

  if (e && e.parameter && Object.keys(e.parameter).length > 0) {
    return e.parameter;
  }

  if (raw && raw.indexOf("=") !== -1) {
    return parseFormEncodedBody_(raw);
  }

  throw new Error("Unable to parse incoming request body");
}

function parseFormEncodedBody_(raw) {
  const obj = {};
  raw.split("&").forEach(pair => {
    const parts = pair.split("=");
    const key = decodeURIComponent((parts[0] || "").replace(/\+/g, " "));
    const value = decodeURIComponent((parts.slice(1).join("=") || "").replace(/\+/g, " "));
    if (key) obj[key] = value;
  });
  return obj;
}

function parseCanonicalTaskerFormBody_(raw) {
  const actionMatch = String(raw || "").match(/(?:^|&)action=([^&]*)/);
  const action = actionMatch ? decodeFormComponent_(actionMatch[1]) : "";
  const fieldsByAction = {
    incoming_sms: ["token", "action", "phone", "message", "received_at", "message_id"],
    reply_sent: ["token", "action", "request_id", "message_id", "phone", "reply_text", "lease_token", "sent_at"]
  };
  const fields = fieldsByAction[action];
  if (!fields) return null;

  const result = {};
  let cursor = 0;
  for (let index = 0; index < fields.length; index += 1) {
    const marker = (index === 0 ? "" : "&") + fields[index] + "=";
    if (String(raw).slice(cursor, cursor + marker.length) !== marker) return null;
    const valueStart = cursor + marker.length;
    const nextMarker = index + 1 < fields.length ? "&" + fields[index + 1] + "=" : "";
    const valueEnd = nextMarker ? String(raw).indexOf(nextMarker, valueStart) : String(raw).length;
    if (valueEnd < 0) return null;
    result[fields[index]] = decodeFormComponent_(String(raw).slice(valueStart, valueEnd));
    cursor = valueEnd;
  }
  return result;
}

function decodeFormComponent_(value) {
  const normalized = String(value || "").replace(/\+/g, " ");
  try {
    return decodeURIComponent(normalized);
  } catch (_) {
    return normalized;
  }
}

function testSmsTransportParsing_() {
  const fullInbound = "I am the owner & broker. There is no issue getting bank release.";
  const legacyRaw = "token=test&action=incoming_sms&phone=+15732803889&message=" + fullInbound
    + "&received_at=7-18-26 10.19&message_id=+15732803889-1784384356624";
  const recoveredLegacy = parseIncomingRequest_({
    postData: { contents: legacyRaw },
    parameter: { token: "test", action: "incoming_sms", phone: " 15732803889", message: "I am the owner " }
  });
  if (recoveredLegacy.message !== fullInbound) {
    throw new Error("Legacy ampersand recovery regression: " + JSON.stringify(recoveredLegacy));
  }

  const encodedRaw = "token=test&action=incoming_sms&phone=%2B15732803889&message="
    + encodeURIComponent(fullInbound)
    + "&received_at=7-18-26%2010.19&message_id=%2B15732803889-1784384356624";
  const recoveredEncoded = parseIncomingRequest_({ postData: { contents: encodedRaw }, parameter: {} });
  if (recoveredEncoded.message !== fullInbound || recoveredEncoded.phone !== "+15732803889") {
    throw new Error("URL-encoded inbound regression: " + JSON.stringify(recoveredEncoded));
  }

  const receiptText = "Free to you & the seller; buyer pays only if/when it closes.";
  const receiptRaw = "token=test&action=reply_sent&request_id=req-1&message_id=msg-1"
    + "&phone=%2B18328984452&reply_text=" + encodeURIComponent(receiptText)
    + "&lease_token=lease-1&sent_at=1784412169582";
  const recoveredReceipt = parseIncomingRequest_({ postData: { contents: receiptRaw }, parameter: {} });
  if (recoveredReceipt.reply_text !== receiptText || recoveredReceipt.request_id !== "req-1" || recoveredReceipt.lease_token !== "lease-1") {
    throw new Error("URL-encoded receipt regression: " + JSON.stringify(recoveredReceipt));
  }

  const jsonInbound = parseIncomingRequest_({ postData: { contents: JSON.stringify({
    token: "test",
    action: "incoming_sms",
    phone: "+15732803889",
    message: fullInbound,
    received_at: "7-18-26 10.19",
    message_id: "json-1"
  }) } });
  if (jsonInbound.message !== fullInbound || jsonInbound.message_id !== "json-1") {
    throw new Error("JSON transport regression: " + JSON.stringify(jsonInbound));
  }

  return { ok: true };
}

function validateToken_(token) {
  const expected = PropertiesService.getScriptProperties().getProperty("ALLOWED_TOKEN");
  if (!expected || token !== expected) {
    throw new Error("Unauthorized");
  }
}

function handleIncomingSms_(body) {
  const phone = normalizePhone_(body && body.phone || "");
  const lease = acquireSmsConversationLease_(phone);
  if (!lease.ok) {
    const err = new Error(lease.reason || "Conversation is already being processed");
    err.retryable = true;
    err.code = "SMS_CONVERSATION_BUSY";
    throw err;
  }

  try {
    return handleIncomingSmsCore_(body);
  } finally {
    releaseSmsConversationLease_(lease);
  }
}

function acquireSmsConversationLease_(phone) {
  if (!phone || phone.length !== 10) {
    return { ok: false, reason: "A valid phone is required for conversation processing" };
  }

  const lock = LockService.getScriptLock();
  if (!lock.tryLock(3000)) {
    return { ok: false, reason: "Conversation lease registry is temporarily busy" };
  }

  try {
    const props = PropertiesService.getScriptProperties();
    const key = "SMS_CONVERSATION_LEASE_" + phone;
    const now = Date.now();
    const existingRaw = props.getProperty(key);
    if (existingRaw) {
      try {
        const existing = JSON.parse(existingRaw);
        if (Number(existing.expires_at || 0) > now) {
          return { ok: false, reason: "This conversation is already being processed" };
        }
      } catch (_) {}
    }

    const token = Utilities.getUuid();
    props.setProperty(key, JSON.stringify({
      token: token,
      // Apps Script executions can approach six minutes when OpenAI or Sheets
      // are slow. Keep the lease alive for the full execution window.
      expires_at: now + 6 * 60 * 1000
    }));
    return { ok: true, key: key, token: token };
  } finally {
    lock.releaseLock();
  }
}

function releaseSmsConversationLease_(lease) {
  if (!lease || !lease.ok || !lease.key || !lease.token) return;
  const lock = LockService.getScriptLock();
  if (!lock.tryLock(3000)) return;

  try {
    const props = PropertiesService.getScriptProperties();
    const currentRaw = props.getProperty(lease.key);
    if (!currentRaw) return;
    try {
      const current = JSON.parse(currentRaw);
      if (String(current.token || "") === String(lease.token)) {
        props.deleteProperty(lease.key);
      }
    } catch (_) {
      props.deleteProperty(lease.key);
    }
  } finally {
    lock.releaseLock();
  }
}

function handleIncomingSmsCore_(body) {
  const phoneRaw = String(body.phone || "").trim();
  const inboundText = normalizeWhitespace_(String(body.message || ""));
  const messageId = String(body.message_id || "");
  const receivedAt = body.received_at || new Date().toISOString();
  const recoveryRetry = body && body.recovery_retry === true;

  if (!phoneRaw || !inboundText) {
    throw new Error("Missing phone or message");
  }

  let sheet;
  let rowInfo;
  let row;
  let rowObj;
  const dedupeLock = LockService.getScriptLock();

  if (!dedupeLock.tryLock(15000)) {
    appendSmsDebugLog_("incoming_sms_lock_busy", {
      phone: phoneRaw,
      message: inboundText,
      reason: "Inbound dedupe lock busy; suppressed to prevent duplicate reply"
    });

    const err = new Error("Inbound dedupe lock is temporarily busy");
    err.retryable = true;
    err.code = "SMS_INBOUND_DEDUPE_BUSY";
    throw err;
  }

  try {
    sheet = getSheet_();
    const data = getSheetData_(sheet);
    rowInfo = findOrCreateRowByPhone_(sheet, data, phoneRaw);
    row = rowInfo.row;
    rowObj = rowInfo.rowObj;

    const recoveringSameMessage = !!(
      recoveryRetry && messageId &&
      rowObj[HEADERS.last_message_id] && rowObj[HEADERS.last_message_id] === messageId
    );
    if (rowObj[HEADERS.last_message_id] && rowObj[HEADERS.last_message_id] === messageId && !recoveringSameMessage) {
      return {
        ok: true,
        duplicate: true,
        should_reply: false,
        reason: "Duplicate message_id ignored"
      };
    }

    if (!recoveringSameMessage && isRecentDuplicateInboundText_(rowObj, inboundText, receivedAt)) {
      appendSmsDebugLog_("incoming_sms_duplicate_suppressed", {
        phone: phoneRaw,
        message: inboundText,
        reason: "Recent duplicate inbound text suppressed"
      });

      return {
        ok: true,
        duplicate: true,
        should_reply: false,
        reason: "Recent duplicate inbound text ignored"
      };
    }

    if (!recoveringSameMessage && isDurableHandledDuplicateInbound_(rowObj, inboundText)) {
      appendSmsDebugLog_("incoming_sms_duplicate_suppressed", {
        phone: phoneRaw,
        message: inboundText,
        reason: "Durably handled inbound replay suppressed"
      });
      return {
        ok: true,
        duplicate: true,
        should_reply: false,
        reason: "Durably handled inbound replay ignored"
      };
    }

    if (isSmsReactionToLastOutbound_(inboundText, rowObj)) {
      updateRowFields_(sheet, row, {
        [HEADERS.last_inbound_text]: normalizeHandledInboundText_(inboundText),
        [HEADERS.last_inbound_at]: receivedAt,
        [HEADERS.last_contact_time]: receivedAt,
        [HEADERS.last_message_id]: messageId
      });
      appendHistory_(sheet, row, { role: "agent", text: inboundText, ts: receivedAt });
      appendSmsDebugLog_("incoming_sms_reaction_suppressed", {
        phone: phoneRaw,
        message: inboundText,
        reason: "Reaction to the last outbound message suppressed before response selection",
        message_id: messageId
      });

      return {
        ok: true,
        reaction: true,
        should_reply: false,
        reply_text: "",
        handoff_needed: false,
        needs_review: false,
        reason: "Reaction to the last outbound message; no reply needed"
      };
    }

    if (
      String(rowObj[HEADERS.ai_state] || "").toLowerCase() === "done" &&
      isPunctuationCorrectionFragment_(inboundText)
    ) {
      appendHistory_(sheet, row, { role: "agent", text: inboundText, ts: receivedAt });
      updateRowFields_(sheet, row, {
        [HEADERS.last_inbound_text]: normalizeHandledInboundText_(inboundText),
        [HEADERS.last_inbound_at]: receivedAt,
        [HEADERS.last_contact_time]: receivedAt,
        [HEADERS.last_message_id]: messageId
      });
      appendSmsDebugLog_("incoming_sms_correction_suppressed", {
        phone: phoneRaw,
        message: inboundText,
        reason: "Punctuation-only correction after closeout suppressed",
        message_id: messageId
      });

      return {
        ok: true,
        correction: true,
        should_reply: false,
        reply_text: "",
        handoff_needed: false,
        needs_review: false,
        reason: "Punctuation-only correction after closeout; no additional reply needed"
      };
    }

    if (!recoveringSameMessage) {
      updateRowFields_(sheet, row, {
        [HEADERS.last_inbound_text]: normalizeHandledInboundText_(inboundText),
        [HEADERS.last_inbound_at]: receivedAt,
        [HEADERS.last_contact_time]: receivedAt,
        [HEADERS.last_message_id]: messageId
      });
    }
  } finally {
    dedupeLock.releaseLock();
  }

  if (!(recoveryRetry && messageId && String(rowObj[HEADERS.last_message_id] || "") === messageId)) {
    appendHistory_(sheet, row, { role: "agent", text: inboundText, ts: receivedAt });
  }

  const refreshedData = getSheetData_(sheet);
  const refreshedRowInfo = refreshedData.find(r => r.row === row);
  const currentRowObj = refreshedRowInfo ? refreshedRowInfo.obj : rowObj;
  const currentCount = Number(currentRowObj[HEADERS.auto_reply_count] || 0);
  const capReached = currentCount >= 3;
  const hasFeeQuestion = isPaymentOrFeeQuestionSignal_(inboundText);
  const hasExperienceQuestion = isExperienceTrackRecordQuestionSignal_(inboundText);
  const hasClientConsultationInterest = isClientConsultationInterestSignal_(inboundText);
  const hasExistingCrispRelationship = isExistingCrispRelationshipSignal_(inboundText);
  const hasInformationRequest = isEmailRequestSignal_(inboundText) || !!extractEmailAddress_(inboundText);
  const hasPhoneCallInterest = isPhoneCallInterestSignal_(inboundText);
  const hasPresentServiceInterest = isPresentServiceInterestSignal_(inboundText);
  const hasCompanyIdentityQuestion = isCompanyIdentityQuestionSignal_(inboundText);

  if (isOptOutSignal_(inboundText)) {
    updateRowFields_(sheet, row, {
      [HEADERS.response_status]: inboundText,
      [HEADERS.mailshake_status]: "R",
      [HEADERS.conversation_summary]: "Opt-out / stop request",
      [HEADERS.ai_state]: "done",
      [HEADERS.call_booking_status]: "closed_no_interest",
      [HEADERS.handoff_flag]: "FALSE",
      [HEADERS.human_override]: "TRUE"
    });

    return {
      ok: true,
      should_reply: false,
      reply_text: "",
      lead_status: "R",
      conversation_done: true,
      handoff_needed: false,
      needs_review: false,
      reason: "Opt-out / stop request"
    };
  }

  if (/\breply\s+stop\s+to\s+(?:end|unsubscribe|cancel|opt out)\b/i.test(inboundText)) {
    updateRowFields_(sheet, row, {
      [HEADERS.response_status]: inboundText,
      [HEADERS.mailshake_status]: "R",
      [HEADERS.conversation_summary]: "Automated STOP instruction / non-agent responder",
      [HEADERS.ai_state]: "done",
      [HEADERS.call_booking_status]: "closed_no_interest",
      [HEADERS.handoff_flag]: "FALSE"
    });
    return {
      ok: true,
      should_reply: true,
      reply_text: "STOP",
      lead_status: "R",
      conversation_done: true,
      handoff_needed: false,
      needs_review: false,
      reason: "Automated STOP instruction / non-agent responder"
    };
  }

  // Classify once before the cap so terminal high-value intents (scheduled
  // callbacks, direct help, stats, fee negotiation) keep their specific
  // handoff reason instead of being flattened into MAX REPLIES.
  const ruleResult = applyFastRules_(inboundText, currentRowObj);

  // The reply cap is a terminal human handoff, even for questions that have
  // an early deterministic answer. Preserve opt-outs, routing noise, and an
  // already-active handoff without generating another alert.
  if (
    capReached &&
    String(currentRowObj[HEADERS.human_override] || "").toUpperCase() !== "TRUE" &&
    !isManualFollowupLocked_(currentRowObj) &&
    !isAutomatedRoutingNoticeSignal_(inboundText) &&
    !isPostHandoffCallbackUpdate_(currentRowObj, inboundText) &&
    !ruleResult.bypass_reply_cap &&
    !isTerminalCloseoutDecision_(ruleResult) &&
    !(ruleResult.matched && (ruleResult.handoff_needed || ruleResult.needs_review || ruleResult.alert_needed))
  ) {
    if (isFinalCourtesyReply_(inboundText)) {
      return {
        ok: true,
        should_reply: false,
        handoff_needed: false,
        needs_review: false,
        reason: "Courtesy acknowledgment at reply cap; no further reply needed"
      };
    }
    return handleMaxRepliesHandoff_(sheet, row, currentRowObj, phoneRaw, inboundText);
  }

  if (
    isSpanishLanguageSignal_(inboundText) &&
    !isSpanishFeeQuestionSignal_(inboundText) &&
    String(currentRowObj[HEADERS.human_override] || "").toUpperCase() !== "TRUE" &&
    !isManualFollowupLocked_(currentRowObj)
  ) {
    const replyText = buildSpanishCapabilityReply_();

    updateRowFields_(sheet, row, {
      [HEADERS.response_status]: inboundText,
      [HEADERS.mailshake_status]: "Y",
      [HEADERS.conversation_summary]: "Agent requested Spanish communication; English response provided",
      [HEADERS.ai_state]: "active",
      [HEADERS.call_booking_status]: "interested_no_call",
      [HEADERS.handoff_flag]: "FALSE",
      [HEADERS.human_override]: "FALSE"
    });

    return {
      ok: true,
      should_reply: !capReached,
      reply_text: capReached ? "" : replyText,
      lead_status: "Y",
      conversation_done: false,
      handoff_needed: false,
      needs_review: false,
      reason: "Agent requested Spanish communication; English response provided"
    };
  }
  if (isPostHandoffCallbackUpdate_(currentRowObj, inboundText)) {
    const callbackTime = extractScheduledCallbackReference_(inboundText);
    const priorCallbackTime = normalizeCallbackTime_(currentRowObj[HEADERS.callback_time]);
    const changed = normalizeCallbackTime_(callbackTime) !== priorCallbackTime;
    const preservedLeadStatus = String(currentRowObj[HEADERS.mailshake_status] || "Y");

    if (changed) {
      const history = getHistoryArray_(currentRowObj[HEADERS.history_json]);
      sendHandoffEmail_({
        handoff_type: "CALLBACK UPDATE",
        agent_name: currentRowObj[HEADERS.agent_name] || "",
        last_name: currentRowObj[HEADERS.last_name] || "",
        initial_text: currentRowObj[HEADERS.initial_text_sent] || "",
        phone: phoneRaw,
        email: currentRowObj[HEADERS.email] || "",
        listing_address: currentRowObj[HEADERS.listing_address] || "",
        city: currentRowObj[HEADERS.city] || "",
        state: currentRowObj[HEADERS.state] || "",
        zip: currentRowObj[HEADERS.zip] || "",
        last_message: inboundText,
        history: history
      });
    }

    updateRowFields_(sheet, row, {
      [HEADERS.response_status]: inboundText,
      [HEADERS.conversation_summary]: changed ? "Callback updated after human handoff" : "Callback timing repeated after human handoff",
      [HEADERS.ai_state]: "handoff",
      [HEADERS.call_booking_status]: "scheduled_callback",
      [HEADERS.callback_requested]: "yes",
      [HEADERS.callback_time]: callbackTime,
      [HEADERS.handoff_flag]: "TRUE",
      [HEADERS.human_override]: "TRUE"
    });

    return {
      ok: true,
      should_reply: false,
      reply_text: "",
      lead_status: preservedLeadStatus,
      conversation_done: false,
      handoff_needed: true,
      needs_review: false,
      callback_updated: changed,
      alert_needed: changed,
      handoff_type: changed ? "CALLBACK UPDATE" : "",
      reason: changed ? "Callback updated after human handoff" : "Callback timing repeated after human handoff"
    };
  }

  if (
    String(currentRowObj[HEADERS.human_override] || "").toUpperCase() === "TRUE" &&
    (
      String(currentRowObj[HEADERS.ai_state] || "").toLowerCase() === "handoff" ||
      String(currentRowObj[HEADERS.handoff_flag] || "").toUpperCase() === "TRUE"
    ) &&
    isUnmistakableTerminalRejectionSignal_(inboundText)
  ) {
    updateRowFields_(sheet, row, {
      [HEADERS.response_status]: inboundText,
      [HEADERS.mailshake_status]: "R",
      [HEADERS.conversation_summary]: "Explicit rejection after human handoff; closed with no automated reply",
      [HEADERS.ai_state]: "done",
      [HEADERS.call_booking_status]: "closed_no_interest",
      [HEADERS.callback_requested]: "no",
      [HEADERS.callback_time]: "",
      [HEADERS.handoff_flag]: "FALSE",
      [HEADERS.human_override]: "FALSE"
    });

    return {
      ok: true,
      should_reply: false,
      reply_text: "",
      lead_status: "R",
      conversation_done: true,
      handoff_needed: false,
      needs_review: false,
      reason: "Explicit rejection after human handoff; closed with no automated reply"
    };
  }

  if (String(currentRowObj[HEADERS.human_override] || "").toUpperCase() === "TRUE") {
    return {
      ok: true,
      should_reply: false,
      handoff_needed: true,
      needs_review: false,
      reason: "Human override enabled"
    };
  }

  if (isManualFollowupLocked_(currentRowObj)) {
    return {
      ok: true,
      should_reply: false,
      handoff_needed: true,
      needs_review: false,
      reason: "Manual follow-up already active"
    };
  }

  if (!ruleResult.matched && (hasPhoneCallInterest || hasPresentServiceInterest) && isClosedMarketingConversation_(currentRowObj)) {
    const history = getHistoryArray_(currentRowObj[HEADERS.history_json]);
    const handoffType = hasPhoneCallInterest ? "CALL REQUESTED" : "RENEWED INTEREST";
    const reason = hasPhoneCallInterest
      ? "Agent expressed phone-call interest after a prior closeout"
      : "Agent expressed present service interest after a prior closeout";

    sendHandoffEmail_({
      handoff_type: handoffType,
      agent_name: currentRowObj[HEADERS.agent_name] || "",
      last_name: currentRowObj[HEADERS.last_name] || "",
      initial_text: currentRowObj[HEADERS.initial_text_sent] || "",
      phone: phoneRaw,
      email: currentRowObj[HEADERS.email] || "",
      listing_address: currentRowObj[HEADERS.listing_address] || "",
      city: currentRowObj[HEADERS.city] || "",
      state: currentRowObj[HEADERS.state] || "",
      zip: currentRowObj[HEADERS.zip] || "",
      last_message: inboundText,
      history: history
    });

    updateRowFields_(sheet, row, {
      [HEADERS.response_status]: inboundText,
      [HEADERS.mailshake_status]: "Y",
      [HEADERS.conversation_summary]: reason,
      [HEADERS.ai_state]: "handoff",
      [HEADERS.call_booking_status]: "interested_no_call",
      [HEADERS.handoff_flag]: "TRUE",
      [HEADERS.human_override]: "TRUE"
    });

    return {
      ok: true,
      should_reply: false,
      reply_text: "",
      lead_status: "Y",
      conversation_done: false,
      handoff_needed: true,
      needs_review: false,
      handoff_type: handoffType,
      reason: reason
    };
  }

  if (
    !ruleResult.matched &&
    hasCompanyIdentityQuestion &&
    (isAlreadyHandledSignal_(inboundText) || isClosedMarketingConversation_(currentRowObj))
  ) {
    const replyText = buildCoveredCompanyIdentityReply_();
    const reason = "Answered company identity directly while preserving prior closeout";

    updateRowFields_(sheet, row, {
      [HEADERS.response_status]: inboundText,
      [HEADERS.mailshake_status]: "R",
      [HEADERS.conversation_summary]: reason,
      [HEADERS.ai_state]: "done",
      [HEADERS.call_booking_status]: "closed_no_interest",
      [HEADERS.handoff_flag]: "FALSE",
      [HEADERS.human_override]: "FALSE"
    });

    return {
      ok: true,
      should_reply: !capReached,
      reply_text: capReached ? "" : replyText,
      lead_status: "R",
      conversation_done: true,
      handoff_needed: false,
      needs_review: false,
      reason: reason
    };
  }

  if (isAutomatedRoutingNoticeSignal_(inboundText)) {
    const reason = "Automated routing or alternate-number notice ignored";

    updateRowFields_(sheet, row, {
      [HEADERS.response_status]: inboundText,
      [HEADERS.conversation_summary]: reason
    });

    return {
      ok: true,
      should_reply: false,
      reply_text: "",
      lead_status: String(currentRowObj[HEADERS.mailshake_status] || "Y"),
      conversation_done: false,
      handoff_needed: false,
      needs_review: false,
      block_reply: true,
      reason: reason
    };
  }

  if (
    capReached &&
    !ruleResult.bypass_reply_cap &&
    !isTerminalCloseoutDecision_(ruleResult) &&
    !(ruleResult.matched && (ruleResult.handoff_needed || ruleResult.needs_review || ruleResult.alert_needed))
  ) {
    if (isFinalCourtesyReply_(inboundText)) {
      return {
        ok: true,
        should_reply: false,
        handoff_needed: false,
        needs_review: false,
        reason: "Courtesy acknowledgment at reply cap; no further reply needed"
      };
    }
    return handleMaxRepliesHandoff_(sheet, row, currentRowObj, phoneRaw, inboundText);
  }

  // Resolve direct questions and explicit requests before generic decline or
  // already-covered language. This prevents a leading "I handle it" clause
  // from hiding the agent's actual question later in the same message.
  if (isIdentityResendSignal_(inboundText)) {
    const replyText = buildIdentityResendReply_(currentRowObj);

    updateRowFields_(sheet, row, {
      [HEADERS.response_status]: inboundText,
      [HEADERS.mailshake_status]: "Y",
      [HEADERS.conversation_summary]: "Agent asked who this is; original introduction resent",
      [HEADERS.ai_state]: "active",
      [HEADERS.call_booking_status]: "interested_no_call",
      [HEADERS.handoff_flag]: "FALSE",
      [HEADERS.human_override]: "FALSE"
    });

    return {
      ok: true,
      should_reply: true,
      reply_text: replyText,
      lead_status: "Y",
      conversation_done: false,
      handoff_needed: false,
      needs_review: false,
      reason: "Agent asked who this is; original introduction resent"
    };
  }

  if (isPostCloseoutNotShortSaleContinuation_(inboundText, currentRowObj)) {
    return {
      ok: true,
      should_reply: false,
      reply_text: "",
      lead_status: String(currentRowObj[HEADERS.mailshake_status] || "R"),
      conversation_done: true,
      handoff_needed: false,
      needs_review: false,
      reason: "Same-topic continuation after not-short-sale closeout; no additional reply needed"
    };
  }

  if (
    (
      String(currentRowObj[HEADERS.ai_state] || "").toLowerCase() === "done" ||
      lastOutboundWasYoniNameAndNumberReply_(currentRowObj)
    ) &&
    isFinalCourtesyReply_(inboundText) &&
    !isSubstantiveFollowupSignal_(inboundText)
  ) {
    return {
      ok: true,
      should_reply: false,
      handoff_needed: false,
      needs_review: false,
      reason: "Conversation already closed"
    };
  }

  if (isAiOrAutomationQuestionSignal_(inboundText)) {
    const history = getHistoryArray_(currentRowObj[HEADERS.history_json]);
    const reason = "Agent asked whether the conversation is AI/automated or actually Yoni";

    sendHandoffEmail_({
      handoff_type: "AI / HUMAN CHECK",
      agent_name: currentRowObj[HEADERS.agent_name] || "",
      last_name: currentRowObj[HEADERS.last_name] || "",
      initial_text: currentRowObj[HEADERS.initial_text_sent] || "",
      phone: phoneRaw,
      email: currentRowObj[HEADERS.email] || "",
      listing_address: currentRowObj[HEADERS.listing_address] || "",
      city: currentRowObj[HEADERS.city] || "",
      state: currentRowObj[HEADERS.state] || "",
      zip: currentRowObj[HEADERS.zip] || "",
      last_message: inboundText,
      history: history
    });

    updateRowFields_(sheet, row, {
      [HEADERS.response_status]: inboundText,
      [HEADERS.mailshake_status]: "Y",
      [HEADERS.conversation_summary]: reason,
      [HEADERS.ai_state]: "handoff",
      [HEADERS.call_booking_status]: "interested_no_call",
      [HEADERS.handoff_flag]: "TRUE",
      [HEADERS.human_override]: "TRUE"
    });

    return {
      ok: true,
      should_reply: false,
      reply_text: "",
      lead_status: "Y",
      conversation_done: false,
      handoff_needed: true,
      needs_review: false,
      reason: reason
    };
  }

  if (
    !ruleResult.matched &&
    !hasFeeQuestion &&
    !hasExperienceQuestion &&
    !hasClientConsultationInterest &&
    !hasExistingCrispRelationship &&
    isFutureBuyerRecontactSignal_(inboundText)
  ) {
    const replyText = buildFutureBuyerRecontactReply_();
    const reason = "Agent will reconnect after securing a buyer; warm future interest closed without takeover";

    updateRowFields_(sheet, row, {
      [HEADERS.response_status]: inboundText,
      [HEADERS.mailshake_status]: "O",
      [HEADERS.conversation_summary]: reason,
      [HEADERS.ai_state]: "done",
      [HEADERS.call_booking_status]: "warm_future_interest",
      [HEADERS.handoff_flag]: "FALSE",
      [HEADERS.human_override]: "FALSE"
    });

    return {
      ok: true,
      should_reply: !capReached,
      reply_text: capReached ? "" : replyText,
      lead_status: "O",
      conversation_done: true,
      handoff_needed: false,
      needs_review: false,
      reason: reason
    };
  }

  if (!ruleResult.matched && isFutureNegotiationInterestSignal_(inboundText)) {
    const reason = "Agent expressed interest in future short-sale negotiation support";
    const replyText = buildFutureInterestReply_(inboundText);

    updateRowFields_(sheet, row, {
      [HEADERS.response_status]: inboundText,
      [HEADERS.mailshake_status]: "O",
      [HEADERS.conversation_summary]: reason,
      [HEADERS.ai_state]: "done",
      [HEADERS.call_booking_status]: "warm_future_interest",
      [HEADERS.handoff_flag]: "FALSE",
      [HEADERS.human_override]: "FALSE"
    });

    return {
      ok: true,
      should_reply: !capReached,
      reply_text: capReached ? "" : replyText,
      lead_status: "O",
      conversation_done: true,
      handoff_needed: false,
      needs_review: false,
      reason: reason
    };
  }

  if (
    !ruleResult.matched &&
    !hasFeeQuestion &&
    !hasExperienceQuestion &&
    !hasClientConsultationInterest &&
    !hasExistingCrispRelationship &&
    isRelationshipOnlyAfterExistingCoverageSignal_(inboundText, currentRowObj)
  ) {
    const replyText = buildRelationshipOnlyCloseoutReply_(inboundText);

    updateRowFields_(sheet, row, {
      [HEADERS.response_status]: inboundText,
      [HEADERS.mailshake_status]: "O",
      [HEADERS.conversation_summary]: "Current file already covered; relationship left open without sales follow-up",
      [HEADERS.ai_state]: "done",
      [HEADERS.call_booking_status]: "warm_future_interest",
      [HEADERS.handoff_flag]: "FALSE",
      [HEADERS.human_override]: "FALSE"
    });

    return {
      ok: true,
      should_reply: !capReached,
      reply_text: capReached ? "" : replyText,
      lead_status: "O",
      conversation_done: true,
      handoff_needed: false,
      needs_review: false,
      reason: "Current file already covered; relationship left open without sales follow-up"
    };
  }

  if (!ruleResult.matched && !hasFeeQuestion && isNotShortSaleSignal_(inboundText)) {
    const replyText = "Ahh, ok... thanks for letting me know. Good luck with your listing!";

    updateRowFields_(sheet, row, {
      [HEADERS.response_status]: inboundText,
      [HEADERS.mailshake_status]: "R",
      [HEADERS.conversation_summary]: "Not actually a short sale / changed listing",
      [HEADERS.ai_state]: "done",
      [HEADERS.call_booking_status]: "closed_no_interest",
      [HEADERS.handoff_flag]: "FALSE"
    });

    return {
      ok: true,
      should_reply: !capReached,
      reply_text: capReached ? "" : replyText,
      lead_status: "R",
      conversation_done: true,
      handoff_needed: false,
      needs_review: false,
      reason: "Not actually a short sale"
    };
  }

  if (!ruleResult.matched && !hasFeeQuestion && isSelfHandlingFutureHelpSignal_(inboundText)) {
    const replyText = buildFutureKeepInMindServiceReply_();

    updateRowFields_(sheet, row, {
      [HEADERS.response_status]: inboundText,
      [HEADERS.mailshake_status]: "O",
      [HEADERS.conversation_summary]: "Agent handles short sales personally; future help offered",
      [HEADERS.ai_state]: "done",
      [HEADERS.call_booking_status]: "warm_future_interest",
      [HEADERS.handoff_flag]: "FALSE"
    });

    return {
      ok: true,
      should_reply: !capReached,
      reply_text: capReached ? "" : replyText,
      lead_status: "O",
      conversation_done: true,
      handoff_needed: false,
      needs_review: false,
      reason: "Agent handles short sales personally; future help offered"
    };
  }

  if (!ruleResult.matched && isCredentialQuestionSignal_(inboundText)) {
    const replyText = buildCredentialQuestionReply_();

    updateRowFields_(sheet, row, {
      [HEADERS.response_status]: inboundText,
      [HEADERS.mailshake_status]: "Y",
      [HEADERS.conversation_summary]: "Agent asked whether Crisp is an attorney or provides legal advice",
      [HEADERS.ai_state]: "active",
      [HEADERS.call_booking_status]: "interested_no_call",
      [HEADERS.handoff_flag]: "FALSE",
      [HEADERS.human_override]: "FALSE"
    });

    return {
      ok: true,
      should_reply: !capReached,
      reply_text: capReached ? "" : replyText,
      lead_status: "Y",
      conversation_done: false,
      handoff_needed: false,
      needs_review: false,
      reason: "Answered attorney / legal-advice credential question directly"
    };
  }

  if (!ruleResult.matched && isNegotiatorRoleQuestionSignal_(inboundText)) {
    const replyText = buildNegotiatorRoleQuestionReply_();

    updateRowFields_(sheet, row, {
      [HEADERS.response_status]: inboundText,
      [HEADERS.mailshake_status]: "Y",
      [HEADERS.conversation_summary]: "Agent asked whether Crisp acts as the short sale negotiator",
      [HEADERS.ai_state]: "active",
      [HEADERS.call_booking_status]: "interested_no_call",
      [HEADERS.handoff_flag]: "FALSE",
      [HEADERS.human_override]: "FALSE"
    });

    return {
      ok: true,
      should_reply: !capReached,
      reply_text: capReached ? "" : replyText,
      lead_status: "Y",
      conversation_done: false,
      handoff_needed: false,
      needs_review: false,
      reason: "Clarified short sale negotiator role and invited a call"
    };
  }

  if (!ruleResult.matched && !hasFeeQuestion && !hasExperienceQuestion && !hasClientConsultationInterest && !hasExistingCrispRelationship && !hasInformationRequest && !hasPhoneCallInterest && !hasPresentServiceInterest && !hasCompanyIdentityQuestion && isAlreadyHandledSignal_(inboundText)) {
    const replyText = getStandardNoCloseoutReply_();

    updateRowFields_(sheet, row, {
      [HEADERS.response_status]: inboundText,
      [HEADERS.mailshake_status]: "R",
      [HEADERS.conversation_summary]: "Already represented / handled",
      [HEADERS.ai_state]: "done",
      [HEADERS.call_booking_status]: "closed_no_interest",
      [HEADERS.handoff_flag]: "FALSE"
    });

    return {
      ok: true,
      should_reply: !capReached,
      reply_text: capReached ? "" : replyText,
      lead_status: "R",
      conversation_done: true,
      handoff_needed: false,
      needs_review: false,
      reason: "Already represented / handled"
    };
  }

  if (!ruleResult.matched && !hasFeeQuestion && !hasExperienceQuestion && !hasClientConsultationInterest && !hasExistingCrispRelationship && !hasInformationRequest && isClearNoSignal_(inboundText)) {
    const closeoutReply = getStandardNoCloseoutReply_();

    updateRowFields_(sheet, row, {
      [HEADERS.response_status]: inboundText,
      [HEADERS.mailshake_status]: "R",
      [HEADERS.conversation_summary]: "Clear no / closed out",
      [HEADERS.ai_state]: "done",
      [HEADERS.call_booking_status]: "closed_no_interest",
      [HEADERS.handoff_flag]: "FALSE"
    });

    return {
      ok: true,
      should_reply: !capReached,
      reply_text: capReached ? "" : closeoutReply,
      lead_status: "R",
      conversation_done: true,
      handoff_needed: false,
      needs_review: false,
      reason: "Clear no / closed out"
    };
  }

  if (ruleResult.matched) {
    let decision = normalizeAiDecision_(ruleResult, currentRowObj[HEADERS.mailshake_status]);
    decision = applyReplySanitizers_(decision, currentRowObj);
    decision = enforceDurableFollowupPromiseRule_(decision, inboundText);
    decision = ensureQuestionDisposition_(decision, inboundText);
    decision = applyRepeatGuard_(decision, currentRowObj, inboundText);
    const updates = {
      [HEADERS.response_status]: inboundText,
      [HEADERS.conversation_summary]: decision.reason || ""
    };

    if (!decision.preserve_existing_state) {
      updates[HEADERS.mailshake_status] = decision.lead_status;
      const terminalHandoff = decision.handoff_needed || decision.needs_review || decision.alert_needed;
      updates[HEADERS.ai_state] = terminalHandoff ? "handoff" : (decision.conversation_done ? "done" : "active");
      updates[HEADERS.handoff_flag] = terminalHandoff ? "TRUE" : "FALSE";
      updates[HEADERS.human_override] = terminalHandoff ? "TRUE" : "FALSE";

      if (ruleResult.info_email_to && isValidEmailAddress_(ruleResult.info_email_to)) {
        updates[HEADERS.email] = ruleResult.info_email_to;
      }

      if (decision.call_booking_status) {
        updates[HEADERS.call_booking_status] = decision.call_booking_status;
      } else if (decision.lead_status === "G") {
        updates[HEADERS.call_booking_status] = "call_set_or_hot";
      } else if (decision.lead_status === "Y") {
        updates[HEADERS.call_booking_status] = "interested_no_call";
      } else if (decision.lead_status === "O") {
        updates[HEADERS.call_booking_status] = "warm_future_interest";
      } else if (decision.lead_status === "R") {
        updates[HEADERS.call_booking_status] = "closed_no_interest";
      }
      if (decision.callback_time) {
        updates[HEADERS.callback_requested] = decision.callback_requested || "yes";
        updates[HEADERS.callback_time] = decision.callback_time;
      }
    }

    // A terminal alert must be durably queued before the CRM is locked. If
    // queuing fails, the inbound worker retries without being stopped by its
    // own partially committed human-override state.
    if (decision.handoff_needed || decision.needs_review || decision.alert_needed) {
      const history = getHistoryArray_(currentRowObj[HEADERS.history_json]);
      sendHandoffEmail_({
        handoff_type: decision.handoff_type || ruleResult.handoff_type || (decision.needs_review ? "NEEDS REVIEW" : "MANUAL FOLLOW-UP"),
        agent_name: currentRowObj[HEADERS.agent_name] || "",
        last_name: currentRowObj[HEADERS.last_name] || "",
        initial_text: currentRowObj[HEADERS.initial_text_sent] || "",
        phone: phoneRaw,
        email: currentRowObj[HEADERS.email] || "",
        listing_address: currentRowObj[HEADERS.listing_address] || "",
        city: currentRowObj[HEADERS.city] || "",
        state: currentRowObj[HEADERS.state] || "",
        zip: currentRowObj[HEADERS.zip] || "",
        last_message: inboundText,
        history: history
      });
    }

    updateRowFields_(sheet, row, updates);

    if (decision.lead_status === "O" && ruleResult.info_email_to) {
      syncWarmInfoOpportunityRows_(
        sheet,
        ruleResult.info_email_to,
        currentRowObj,
        inboundText
      );
    }

    if (shouldSendInfoEmail_(ruleResult, decision)) {
      const infoEmailData = {
        to: ruleResult.info_email_to,
        first_name: getCanonicalFirstName_(currentRowObj),
        agent_name: currentRowObj[HEADERS.agent_name] || "",
        last_name: currentRowObj[HEADERS.last_name] || "",
        listing_address: currentRowObj[HEADERS.listing_address] || "",
        city: currentRowObj[HEADERS.city] || "",
        state: currentRowObj[HEADERS.state] || "",
        zip: currentRowObj[HEADERS.zip] || "",
        phone: phoneRaw,
        last_message: inboundText
      };

      if (isInfoEmailApprovalRequired_()) {
        sendInfoEmailApprovalRequest_(infoEmailData);
      } else {
        sendAgentInfoEmail_(infoEmailData);
      }
    }

    return {
      ok: true,
      should_reply: shouldSendBotReply_(decision, capReached && !decision.bypass_reply_cap),
      reply_text: shouldSendBotReply_(decision, capReached && !decision.bypass_reply_cap) ? (decision.reply_text || "") : "",
      lead_status: decision.lead_status,
      conversation_done: !!decision.conversation_done,
      handoff_needed: !!decision.handoff_needed,
      needs_review: !!decision.needs_review,
      alert_needed: !!decision.alert_needed,
      reason: decision.reason || ""
    };
  }

  if (isImmediateCallSignal_(inboundText)) {
    const history = getHistoryArray_(currentRowObj[HEADERS.history_json]);

    sendHandoffEmail_({
      handoff_type: "CALL NOW",
      agent_name: currentRowObj[HEADERS.agent_name] || "",
      last_name: currentRowObj[HEADERS.last_name] || "",
      initial_text: currentRowObj[HEADERS.initial_text_sent] || "",
      phone: phoneRaw,
      email: currentRowObj[HEADERS.email] || "",
      listing_address: currentRowObj[HEADERS.listing_address] || "",
      city: currentRowObj[HEADERS.city] || "",
      state: currentRowObj[HEADERS.state] || "",
      zip: currentRowObj[HEADERS.zip] || "",
      last_message: inboundText,
      history: history
    });

    updateRowFields_(sheet, row, {
      [HEADERS.response_status]: inboundText,
      [HEADERS.mailshake_status]: "Y",
      [HEADERS.conversation_summary]: "Immediate callback requested",
      [HEADERS.ai_state]: "handoff",
      [HEADERS.call_booking_status]: "call_now",
      [HEADERS.handoff_flag]: "TRUE",
      [HEADERS.human_override]: "TRUE"
    });

    return {
      ok: true,
      should_reply: false,
      reply_text: "",
      lead_status: "Y",
      conversation_done: false,
      handoff_needed: true,
      needs_review: false,
      reason: "Immediate callback requested"
    };
  }

  if (isSchedulingSignal_(inboundText)) {
    const decision = buildSchedulingReply_(inboundText);
    const callbackTime = extractScheduledCallbackReference_(inboundText)
      || extractSchedulingTimePhrase_(inboundText)
      || normalizeWhitespace_(String(inboundText || ""));
    const history = getHistoryArray_(currentRowObj[HEADERS.history_json]);

    sendHandoffEmail_({
      handoff_type: "SCHEDULED CALLBACK",
      agent_name: currentRowObj[HEADERS.agent_name] || "",
      last_name: currentRowObj[HEADERS.last_name] || "",
      initial_text: currentRowObj[HEADERS.initial_text_sent] || "",
      phone: phoneRaw,
      email: currentRowObj[HEADERS.email] || "",
      listing_address: currentRowObj[HEADERS.listing_address] || "",
      city: currentRowObj[HEADERS.city] || "",
      state: currentRowObj[HEADERS.state] || "",
      zip: currentRowObj[HEADERS.zip] || "",
      last_message: inboundText,
      history: history
    });

    updateRowFields_(sheet, row, {
      [HEADERS.response_status]: inboundText,
      [HEADERS.mailshake_status]: "Y",
      [HEADERS.conversation_summary]: "Scheduling / callback timing discussed",
      [HEADERS.ai_state]: "handoff",
      [HEADERS.call_booking_status]: "scheduled_callback",
      [HEADERS.callback_requested]: "yes",
      [HEADERS.callback_time]: callbackTime,
      [HEADERS.handoff_flag]: "TRUE",
      [HEADERS.human_override]: "TRUE"
    });

    return {
      ok: true,
      should_reply: false,
      reply_text: "",
      lead_status: "Y",
      conversation_done: false,
      handoff_needed: true,
      needs_review: false,
      reason: "Scheduled callback timing"
    };
  }

  let decision = getAiDecision_({ row: row, rowObj: currentRowObj }, inboundText);
  decision = normalizeAiDecision_(decision, currentRowObj[HEADERS.mailshake_status]);
  decision = applyReplySanitizers_(decision, currentRowObj);
  decision = enforceDurableFollowupPromiseRule_(decision, inboundText);
  if (decision.reply_text !== buildShortSaleTimelineReply_() && containsUnsupportedStatsClaim_(decision.reply_text)) {
    decision = buildManualHandoffDecision_(
      "AI attempted to answer with unsupported stats or numeric performance claims",
      "STATS QUESTION"
    );
  }
  decision = ensureQuestionDisposition_(decision, inboundText);
  decision = applyRepeatGuard_(decision, currentRowObj, inboundText);

  const updates = {
    [HEADERS.response_status]: inboundText,
    [HEADERS.conversation_summary]: decision.reason || ""
  };

  if (!decision.preserve_existing_state) {
    updates[HEADERS.mailshake_status] = decision.lead_status;
    const terminalHandoff = decision.handoff_needed || decision.needs_review || decision.alert_needed;
    updates[HEADERS.ai_state] = terminalHandoff ? "handoff" : (decision.conversation_done ? "done" : "active");
    updates[HEADERS.handoff_flag] = terminalHandoff ? "TRUE" : "FALSE";
    updates[HEADERS.human_override] = terminalHandoff ? "TRUE" : "FALSE";

    if (decision.call_booking_status) {
      updates[HEADERS.call_booking_status] = decision.call_booking_status;
    } else if (decision.lead_status === "G") {
      updates[HEADERS.call_booking_status] = "call_set_or_hot";
    } else if (decision.lead_status === "Y") {
      updates[HEADERS.call_booking_status] = "interested_no_call";
    } else if (decision.lead_status === "R") {
      updates[HEADERS.call_booking_status] = "closed_no_interest";
    }
    if (decision.callback_time) {
      updates[HEADERS.callback_requested] = decision.callback_requested || "yes";
      updates[HEADERS.callback_time] = decision.callback_time;
    }
  }

  // Queue the terminal alert before committing the terminal CRM lock. The
  // deterministic email ID makes retries safe if a later Sheet write fails.
  if (decision.handoff_needed || decision.needs_review || decision.alert_needed) {
    const history = getHistoryArray_(currentRowObj[HEADERS.history_json]);
    sendHandoffEmail_({
      handoff_type: decision.handoff_type || (decision.needs_review ? "NEEDS REVIEW" : "MANUAL FOLLOW-UP"),
      agent_name: currentRowObj[HEADERS.agent_name] || "",
      last_name: currentRowObj[HEADERS.last_name] || "",
      initial_text: currentRowObj[HEADERS.initial_text_sent] || "",
      phone: phoneRaw,
      email: currentRowObj[HEADERS.email] || "",
      listing_address: currentRowObj[HEADERS.listing_address] || "",
      city: currentRowObj[HEADERS.city] || "",
      state: currentRowObj[HEADERS.state] || "",
      zip: currentRowObj[HEADERS.zip] || "",
      last_message: inboundText,
      history: history
    });
  }

  updateRowFields_(sheet, row, updates);

  return {
    ok: true,
    should_reply: shouldSendBotReply_(decision, false),
    reply_text: shouldSendBotReply_(decision, false) ? (decision.reply_text || "") : "",
    lead_status: decision.lead_status,
    conversation_done: !!decision.conversation_done,
    handoff_needed: !!decision.handoff_needed,
    needs_review: !!decision.needs_review,
    alert_needed: !!decision.alert_needed,
    reason: decision.reason || ""
  };
}

function normalizeTaskerPayload_(obj) {
  const out = Object.assign({}, obj || {});

  out.reply_text = typeof out.reply_text === "string" ? out.reply_text : "";
  out.reason = typeof out.reason === "string" ? out.reason : "";
  out.delay_seconds = String(out.delay_seconds || 15);
  out.should_reply_text = out.should_reply === true ? "true" : "false";
  out.handoff_needed_text = out.handoff_needed === true ? "true" : "false";

  return out;
}

function isManualFollowupLocked_(rowObj) {
  const handoffFlag = String(rowObj && rowObj[HEADERS.handoff_flag] || "").toUpperCase() === "TRUE";
  const aiState = String(rowObj && rowObj[HEADERS.ai_state] || "").toLowerCase();
  return handoffFlag || aiState === "handoff";
}

function isTerminalCloseoutDecision_(decision) {
  const d = decision || {};
  return !!d.matched &&
    !!d.conversation_done &&
    (d.lead_status === "R" || d.lead_status === "O") &&
    !d.handoff_needed &&
    !d.needs_review &&
    !d.alert_needed;
}

function shouldSendBotReply_(decision, capReached) {
  if (capReached) {
    return false;
  }

  const d = decision || {};
  if ((d.handoff_needed || d.needs_review || d.alert_needed) && !d.send_reply_before_handoff) {
    return false;
  }
  if (d.block_reply) {
    return false;
  }

  return !!d.reply_text;
}

function handleReplySent_(body) {
  const phoneRaw = String(body.phone || "").trim();
  const replyText = normalizeWhitespace_(String(body.reply_text || ""));
  const sentAt = body.sent_at || new Date().toISOString();
  const phone = normalizePhone_(phoneRaw);
  if (!phone || !replyText) throw new Error("Reply receipt requires phone and reply text");

  const conversationLease = acquireSmsConversationLease_(phone);
  if (!conversationLease.ok) {
    const leaseError = new Error(conversationLease.reason || "Conversation is already being processed");
    leaseError.retryable = true;
    leaseError.code = "SMS_REPLY_RECEIPT_BUSY";
    throw leaseError;
  }

  try {
    const sheet = getSheet_();
    const data = getSheetData_(sheet);
    const rowInfo = findOrCreateRowByPhone_(sheet, data, phoneRaw);
    const receiptId = buildReplyReceiptId_(body, phone, replyText, sentAt);
    const lock = LockService.getScriptLock();
    if (!lock.tryLock(5000)) {
      const lockError = new Error("Reply receipt ledger is temporarily busy");
      lockError.retryable = true;
      lockError.code = "SMS_REPLY_RECEIPT_LOCK_BUSY";
      throw lockError;
    }

    try {
      const headers = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];
      const rowValues = sheet.getRange(rowInfo.row, 1, 1, headers.length).getValues()[0];
      const historyIndex = headers.indexOf(HEADERS.history_json);
      const outboundIndex = headers.indexOf(HEADERS.last_outbound_text);
      const contactIndex = headers.indexOf(HEADERS.last_contact_time);
      const countIndex = headers.indexOf(HEADERS.auto_reply_count);
      if (historyIndex < 0 || outboundIndex < 0 || contactIndex < 0 || countIndex < 0) {
        throw new Error("Reply receipt columns are missing from the CRM sheet");
      }

      const history = getHistoryArray_(rowValues[historyIndex]);
      if (history.some(entry => String(entry && entry.receipt_id || "") === receiptId)) {
        return { ok: true, duplicate: true, reason: "Reply receipt was already applied to CRM" };
      }

      history.push({ role: "assistant", text: replyText, ts: sentAt, receipt_id: receiptId });
      rowValues[historyIndex] = JSON.stringify(history.slice(-20));
      rowValues[outboundIndex] = replyText;
      rowValues[contactIndex] = sentAt;
      rowValues[countIndex] = Number(rowValues[countIndex] || 0) + 1;
      if (hasPendingSmsTakeoverV11_(phone)) {
        const aiStateIndex = headers.indexOf(HEADERS.ai_state);
        const handoffIndex = headers.indexOf(HEADERS.handoff_flag);
        const overrideIndex = headers.indexOf(HEADERS.human_override);
        if (aiStateIndex >= 0) rowValues[aiStateIndex] = "handoff";
        if (handoffIndex >= 0) rowValues[handoffIndex] = "TRUE";
        if (overrideIndex >= 0) rowValues[overrideIndex] = "TRUE";
      }
      // The history idempotency marker and reply count share one row write.
      // A Tasker retry therefore either reapplies nothing or completes once.
      sheet.getRange(rowInfo.row, 1, 1, headers.length).setValues([rowValues]);
      return { ok: true, receipt_id: receiptId };
    } finally {
      lock.releaseLock();
    }
  } finally {
    releaseSmsConversationLease_(conversationLease);
  }
}

function buildReplyReceiptId_(body, phone, replyText, sentAt) {
  const requestId = String(body && (body.request_id || body.sms_request_id) || "");
  const messageId = String(body && body.message_id || "");
  const leaseToken = String(body && body.lease_token || "");
  const hasTransportIdentity = !!(requestId && messageId && leaseToken);
  const material = hasTransportIdentity
    ? [requestId, messageId, leaseToken, phone, replyText].join("|")
    : [phone, replyText, String(sentAt || "")].join("|");
  const digest = Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_256, material);
  return digest.map(value => (value + 256).toString(16).slice(-2)).join("").slice(0, 32);
}

function handleManualReplySent_(body) {
  const phoneRaw = String(body.phone || "").trim();
  const replyText = normalizeWhitespace_(String(body.reply_text || body.message || ""));
  const sentAt = body.sent_at || new Date().toISOString();
  const phone = normalizePhone_(phoneRaw);
  if (!phone || !replyText) {
    throw new Error("Manual reply receipt requires phone and reply text");
  }

  const pendingControl = queuePendingSmsControlEventV11_(phone, {
    action: "manual_reply_sent",
    phone: phoneRaw,
    reply_text: replyText,
    sent_at: sentAt
  });
  const conversationLease = acquireSmsConversationLease_(phone);
  if (!conversationLease.ok) {
    return {
      ok: true,
      queued: true,
      manual: true,
      human_override: "TRUE",
      reason: "Manual reply takeover queued behind active conversation"
    };
  }

  try {
    const result = applyManualReplySentCoreV11_(phoneRaw, replyText, sentAt, pendingControl.event_id);
    clearPendingSmsControlEventV11_(phone, pendingControl.event_id);
    return result;
  } finally {
    releaseSmsConversationLease_(conversationLease);
  }
}

function applyManualReplySentCoreV11_(phoneRaw, replyText, sentAt, eventId) {
  const sheet = getSheet_();
  const data = getSheetData_(sheet);
  const rowInfo = findOrCreateRowByPhone_(sheet, data, phoneRaw);
  const history = getHistoryArray_(rowInfo.rowObj[HEADERS.history_json]);
  const alreadyRecorded = !!eventId && history.some(entry => String(entry && entry.control_event_id || "") === String(eventId));
  if (!alreadyRecorded) {
    appendHistory_(sheet, rowInfo.row, {
      role: "assistant",
      text: replyText,
      ts: sentAt,
      control_event_id: eventId || ""
    });
  }
  updateRowFields_(sheet, rowInfo.row, {
    [HEADERS.last_outbound_text]: replyText,
    [HEADERS.last_contact_time]: sentAt,
    [HEADERS.ai_state]: "handoff",
    [HEADERS.handoff_flag]: "TRUE",
    [HEADERS.human_override]: "TRUE"
  });
  return { ok: true, row: rowInfo.row, manual: true, duplicate: alreadyRecorded };
}

function markOverride_(body) {
  const phoneRaw = String(body.phone || "").trim();
  const value = String(body.value || "TRUE").toUpperCase() === "TRUE" ? "TRUE" : "FALSE";
  const phone = normalizePhone_(phoneRaw);
  if (!phone) throw new Error("Override requires a valid phone");

  const pendingControl = queuePendingSmsControlEventV11_(phone, {
    action: "mark_override",
    phone: phoneRaw,
    value: value
  });
  const conversationLease = acquireSmsConversationLease_(phone);
  if (!conversationLease.ok) {
    return {
      ok: true,
      queued: true,
      phone: phoneRaw,
      human_override: value,
      reason: "Override queued behind active conversation"
    };
  }

  try {
    const result = applyOverrideCoreV11_(phoneRaw, value);
    clearPendingSmsControlEventV11_(phone, pendingControl.event_id);
    return result;
  } finally {
    releaseSmsConversationLease_(conversationLease);
  }
}

function applyOverrideCoreV11_(phoneRaw, value) {
  const sheet = getSheet_();
  const data = getSheetData_(sheet);
  const rowInfo = findOrCreateRowByPhone_(sheet, data, phoneRaw);
  updateRowFields_(sheet, rowInfo.row, {
    [HEADERS.human_override]: value
  });
  return { ok: true, phone: phoneRaw, human_override: value };
}

function smsPendingControlKeyV11_(phone) {
  return "SMS_PENDING_CONTROL_V11_" + normalizePhone_(phone);
}

function queuePendingSmsControlEventV11_(phone, payload) {
  const event = Object.assign({}, payload || {}, {
    event_id: Utilities.getUuid(),
    queued_at: new Date().toISOString()
  });
  PropertiesService.getScriptProperties().setProperty(
    smsPendingControlKeyV11_(phone),
    JSON.stringify(event)
  );
  if (typeof installSmsOutboxTriggers_ === "function") installSmsOutboxTriggers_();
  return event;
}

function getPendingSmsControlEventV11_(phone) {
  const raw = PropertiesService.getScriptProperties().getProperty(smsPendingControlKeyV11_(phone));
  if (!raw) return null;
  try {
    return JSON.parse(raw);
  } catch (_) {
    return { action: "mark_override", value: "TRUE", event_id: "malformed" };
  }
}

function hasPendingSmsTakeoverV11_(phone) {
  const event = getPendingSmsControlEventV11_(phone);
  if (!event) return false;
  return event.action === "manual_reply_sent" || String(event.value || "TRUE").toUpperCase() === "TRUE";
}

function clearPendingSmsControlEventV11_(phone, eventId) {
  const props = PropertiesService.getScriptProperties();
  const key = smsPendingControlKeyV11_(phone);
  const lock = LockService.getScriptLock();
  if (!lock.tryLock(3000)) return false;
  try {
    const current = getPendingSmsControlEventV11_(phone);
    if (!current || String(current.event_id || "") !== String(eventId || "")) return false;
    props.deleteProperty(key);
    return true;
  } finally {
    lock.releaseLock();
  }
}

function drainPendingSmsControlEventsV11_() {
  const props = PropertiesService.getScriptProperties();
  const all = props.getProperties();
  const prefix = "SMS_PENDING_CONTROL_V11_";
  const keys = Object.keys(all).filter(key => key.indexOf(prefix) === 0).slice(0, 20);
  let processed = 0;
  keys.forEach(key => {
    const phone = key.slice(prefix.length);
    const event = getPendingSmsControlEventV11_(phone);
    if (!event) return;
    const lease = acquireSmsConversationLease_(phone);
    if (!lease.ok) return;
    try {
      if (event.action === "manual_reply_sent") {
        applyManualReplySentCoreV11_(
          event.phone || phone,
          normalizeWhitespace_(String(event.reply_text || "")),
          event.sent_at || new Date().toISOString(),
          event.event_id || ""
        );
      } else {
        applyOverrideCoreV11_(event.phone || phone, String(event.value || "TRUE").toUpperCase() === "TRUE" ? "TRUE" : "FALSE");
      }
      if (clearPendingSmsControlEventV11_(phone, event.event_id)) processed++;
    } finally {
      releaseSmsConversationLease_(lease);
    }
  });
  return { ok: true, processed: processed };
}

function normalizeLanguageSignalText_(text) {
  let t = normalizeWhitespace_(String(text || "").toLowerCase());
  if (!t) return "";
  if (typeof t.normalize === "function") {
    t = t.normalize("NFD").replace(/[\u0300-\u036f]/g, "");
  }
  return t;
}

function isSpanishFeeQuestionSignal_(text) {
  const t = normalizeLanguageSignalText_(text);
  if (!t) return false;

  if (/\btarifa para el comprador\b/.test(t) ||
      /\b(?:cual|cul) (?:seria|sera) la tarifa\b/.test(t)) {
    return true;
  }

  const asksAmount = /\b(?:cual|cul|cuanto|que)\b/.test(t);
  const mentionsFee = /\b(?:tarifa|costo|costaria|cobra|cobran|precio)\b/.test(t);
  return asksAmount && mentionsFee;
}

function isSpanishLanguageSignal_(text) {
  const t = normalizeLanguageSignalText_(text);
  if (!t) return false;

  return /\b(?:espanol|espaol|spanish)\b/.test(t) ||
    /\b(?:hablas|habla|hablo)\s+(?:espanol|espaol|spanish)\b/.test(t) ||
    /\b(?:no tengo|cual seria|cul sera|tarifa para el comprador|ambas partes)\b/.test(t);
}

function buildSpanishCapabilityReply_() {
  return "No, I'm sorry, I don't speak Spanish, but I'd still love to help if you think communicating in English would be possible.";
}

function isOptOutSignal_(text) {
  const t = normalizeWhitespace_(String(text || "").toLowerCase());

  const patterns = [
    /^stop[.!?]*$/,
    /^stop(?:\s*[.!?,;:\u2014-]+\s*|\s+(?!by\b|in\b|at\b|over\b|to\b)).+/,
    /^unsubscribe[.!?]*$/,
    /\b(?:stop|quit|end)\s+(?:texting|messaging|contacting|sms)\b/,
    /\b(?:don't|dont|do not)\s+(?:text|message|contact|sms)\b/,
    /\bremove (?:me|my (?:info|information)|this (?:info|information))\b/,
    /\btake me off\b/,
    /\bopt\s*out\b/,
    /\bwrong number\b/
  ];

  return patterns.some(pattern => pattern.test(t));
}

function isPaymentOrFeeQuestionSignal_(text) {
  const t = normalizeWhitespace_(String(text || "").toLowerCase());
  if (!t) return false;
  if (isSpanishFeeQuestionSignal_(t)) return true;

  const directPhrases = [
    "what is the cost",
    "what's the cost",
    "what does it cost",
    "what are the expenses",
    "what are the costs",
    "how much does it cost",
    "what do you charge",
    "what does your company charge",
    "what's the charge",
    "what is the charge",
    "what's your fee",
    "what is your fee",
    "what's your rate",
    "what is your rate",
    "what's the rate",
    "what is the rate",
    "what's your pricing",
    "what is your pricing",
    "how much is your rate",
    "how much do you charge",
    "how much is the fee",
    "how much is your fee",
    "how much would you charge",
    "how do you get paid",
    "how are you paid",
    "who pays you",
    "how do you make money",
    "how does crisp get paid",
    "what percentage",
    "what is the percentage",
    "percentage you get",
    "how much of a percentage"
  ];

  if (directPhrases.some(function(phrase) { return t.indexOf(phrase) !== -1; })) {
    return true;
  }

  const asksAmount = t.indexOf("how much") !== -1 ||
    t.indexOf("what") !== -1 ||
    t.indexOf("how") !== -1 ||
    t.indexOf("who") !== -1;
  const mentionsFee = t.indexOf("fee") !== -1 ||
    t.indexOf("cost") !== -1 ||
    t.indexOf("charge") !== -1 ||
    t.indexOf("expense") !== -1 ||
    t.indexOf("paid") !== -1 ||
    t.indexOf("payment") !== -1 ||
    t.indexOf("percentage") !== -1 ||
    t.indexOf("pricing") !== -1 ||
    t.indexOf("price") !== -1 ||
    /\b(?:your|the)\s+rate\b/.test(t);

  return asksAmount && mentionsFee;
}

function isInitialFeeReplyText_(text) {
  const t = normalizeLanguageSignalText_(text);
  const englishDisclosure = t.indexOf("flat fee") !== -1 &&
    t.indexOf("buyer") !== -1 &&
    t.indexOf("closing") !== -1 &&
    (t.indexOf("free") !== -1 || t.indexOf("no cost") !== -1);
  const spanishDisclosure = t.indexOf("tarifa fija") !== -1 &&
    t.indexOf("comprador") !== -1 &&
    t.indexOf("cierre") !== -1 &&
    (t.indexOf("no hay costo") !== -1 || t.indexOf("sin costo") !== -1);
  return (englishDisclosure || spanishDisclosure) && !isSpecificFeeReplyText_(t);
}

function isSpecificFeeReplyText_(text) {
  const t = normalizeLanguageSignalText_(text);
  const compact = t.replace(/[^a-z0-9]/g, "");
  const mentionsBuyer = t.indexOf("buyer") !== -1 || t.indexOf("comprador") !== -1;
  const mentionsClosing = t.indexOf("closing") !== -1 || t.indexOf("cierre") !== -1;
  return compact.indexOf("5000") !== -1 && mentionsBuyer && mentionsClosing;
}

function buildFeeQuestionDecision_(rowObj, lastOutbound) {
  const history = getHistoryArray_(rowObj && rowObj[HEADERS.history_json]);
  const priorAssistantTexts = history
    .filter(function(entry) { return entry && entry.role === "assistant"; })
    .map(function(entry) { return normalizeWhitespace_(String(entry.text || "")); });

  const normalizedLastOutbound = normalizeWhitespace_(String(lastOutbound || ""));
  if (normalizedLastOutbound) {
    priorAssistantTexts.push(normalizedLastOutbound);
  }

  const pendingFeeStage = getPendingFeeReplyStageV3_(rowObj);
  const hasPriorSpecificFeeReply = priorAssistantTexts.some(isSpecificFeeReplyText_) || pendingFeeStage === "specific";
  const hasPriorInitialFeeReply = priorAssistantTexts.some(isInitialFeeReplyText_) || pendingFeeStage === "initial";

  if (hasPriorSpecificFeeReply) {
    return buildManualHandoffDecision_(
      "Agent is still asking about fee/payment after the specific $5,000 answer",
      "FEE QUESTION FOLLOW-UP"
    );
  }

  if (hasPriorInitialFeeReply) {
    return {
      matched: true,
      reply_text: "The fee is $5,000, paid by the buyer at closing. As long as it's disclosed up front in the listing, the buyer can factor it into their offer price.",
      lead_status: "Y",
      conversation_done: false,
      handoff_needed: false,
      needs_review: false,
      block_reply: false,
      reason: "Repeated fee/payment question - gave specific $5,000 buyer-paid answer"
    };
  }

  return {
    matched: true,
    reply_text: "There's no cost to you or the seller, and I don't take anything from your commission. I charge a flat fee to the buyer at closing, only if the deal closes.",
    lead_status: "Y",
    conversation_done: false,
    handoff_needed: false,
    needs_review: false,
    block_reply: false,
    reason: "Asked about charge, fee, percentage, or how Crisp gets paid"
  };
}

function getPendingFeeReplyStageV3_(rowObj) {
  const phone = normalizePhone_(String(rowObj && rowObj[HEADERS.phone] || ""));
  if (!phone) return "";
  try {
    const ss = getSmsSpreadsheet_();
    const sheet = ss.getSheetByName("sms_pending_sends");
    if (!sheet || sheet.getLastRow() < 2) return "";
    const firstRow = Math.max(2, sheet.getLastRow() - 99);
    const rows = sheet.getRange(firstRow, 1, sheet.getLastRow() - firstRow + 1, SMS_PENDING_SEND_HEADERS_.length).getValues();
    const cutoff = Date.now() - 7 * 24 * 60 * 60 * 1000;
    for (let i = rows.length - 1; i >= 0; i -= 1) {
      const status = String(rows[i][1] || "");
      const createdAt = new Date(rows[i][0]).getTime();
      if (["queued", "claimed", "send_started", "sent"].indexOf(status) === -1 ||
          normalizePhone_(rows[i][4]) !== phone || !createdAt || createdAt < cutoff) continue;
      if (isSpecificFeeReplyText_(rows[i][5])) return "specific";
      if (isInitialFeeReplyText_(rows[i][5])) return "initial";
    }
  } catch (_) {}
  return "";
}

function isFeeNegotiationSignal_(text) {
  const t = normalizeWhitespace_(String(text || "").toLowerCase());
  if (!t) return false;
  const mentionsPricing = /\b(?:fee|price|pricing|rate|charge|cost|\$\s*\d|\d{3,5})\b/.test(t);
  const asksConcession = /\b(?:match|beat|lower|reduce|discount|negotiate|counter|concession)\b/.test(t) ||
    /\b(?:would|will|can|could)\s+you\s+(?:do|charge|take|accept)\b/.test(t) ||
    /\bi\s+(?:made|am making)\s+you\s+an?\s+offer\b/.test(t);
  return mentionsPricing && asksConcession;
}

function buildPriorityQuestionDecisionV3_(text, rowObj, lastOutbound) {
  const t = normalizeWhitespace_(String(text || "").toLowerCase());
  if (!t) return null;

  // A request to send information is a delivery workflow, not a general
  // "how do you help" question. Let the dedicated email branch collect or
  // use the address and queue the approved info email.
  if (isEmailRequestSignal_(t) || extractEmailAddress_(t)) return null;

  if (isUnsupportedPerformanceStatsQuestionSignal_(t)) {
    return buildManualHandoffDecision_(
      "Agent asked for unsupported performance stats; manual follow-up needed",
      "STATS QUESTION"
    );
  }

  if (isFeeNegotiationSignal_(t)) {
    return buildManualHandoffDecision_(
      "Agent proposed or requested a pricing concession",
      "FEE NEGOTIATION"
    );
  }

  if (isExistingCrispRelationshipSignal_(t)) {
    const existingClient = buildManualHandoffDecision_(
      "Agent identified an active or existing Crisp/Yoni relationship",
      "EXISTING CRISP CLIENT"
    );
    existingClient.lead_status = "R";
    return existingClient;
  }

  const flags = {
    fee: isPaymentOrFeeQuestionSignal_(t),
    help: /\b(?:how do you help|how can you help|what do you do|what exactly do you do|what do you handle|how does this work|how does that work|what does (?:this|that|the service|your service) look like|what are you offering|what kind of help|what (?:are|is) your services?|explain (?:some )?more details?|more information about your services?|willing to (?:review|hear) what you (?:have to offer|do))\b/.test(t),
    local: isLocalQuestionSignal_(t),
    company: isCompanyIdentityQuestionSignal_(t),
    website: isWebsiteReviewsRequestSignal_(t),
    contact_card: isContactCardRequestSignal_(t),
    contact_info: isPlainContactInfoRequestSignal_(t),
    experience: isExperienceTrackRecordQuestionSignal_(t),
    timeline: isShortSaleTimelineQuestionSignal_(t),
    number: isCurrentTextingNumberQuestionSignal_(t),
    credential: isCredentialQuestionSignal_(t),
    negotiator: isNegotiatorRoleQuestionSignal_(t),
    language: isSpanishLanguageSignal_(t),
    source: isShortSaleSourceQuestion_(t),
    differentiation: isDifferentiationQuestionSignal_(t)
  };

  const matchedKeys = Object.keys(flags).filter(function(key) { return flags[key]; });
  if (!matchedKeys.length) return null;

  if (flags.source && matchedKeys.length === 1) {
    return {
      matched: true,
      reply_text: "I thought I saw it marked online as a short sale. My mistake if I misread it. Thanks.",
      lead_status: "R",
      conversation_done: true,
      handoff_needed: false,
      needs_review: false,
      block_reply: false,
      reason: "Agent asked why the listing was considered a short sale"
    };
  }

  if (flags.differentiation && matchedKeys.length === 1) {
    return {
      matched: true,
      reply_text: buildDifferentiationQuestionReply_(),
      lead_status: "Y",
      conversation_done: false,
      handoff_needed: false,
      needs_review: false,
      block_reply: false,
      alert_needed: true,
      handoff_type: "HOT LEAD - DIFFERENTIATION QUESTION",
      reason: "Answered differentiation question before generic coverage language"
    };
  }

  const covered = isAlreadyHandledSignal_(t) || isClearNoSignal_(t) ||
    isNotShortSaleSignal_(t) || isClosedMarketingConversation_(rowObj);
  const presentInterest = isPresentServiceInterestSignal_(t) || isOpenToReviewOfferSignal_(t);
  const leadStatus = covered && !presentInterest ? "O" : "Y";
  const done = leadStatus === "O";

  if (flags.fee && (
    isPhoneCallInterestSignal_(t) ||
    isImmediateCallSignal_(t) ||
    isOpenCallWindowSignal_(t) ||
    isSchedulingSignal_(t) ||
    isDirectHelpRequestSignal_(t) ||
    /\b(?:call\s+me(?:\s+now)?|i\s+need\s+help|would\s+love\s+(?:some\s+)?help|can\s+use\s+(?:some\s+)?help)\b/.test(t) ||
    /\b(?:can we|could we|would love to|i(?:'|’)d love to)\s+(?:talk|chat|speak)\b/.test(t) ||
    presentInterest
  )) {
    const feeWithInterest = buildFeeQuestionDecision_(rowObj, lastOutbound);
    if (feeWithInterest.handoff_needed) return feeWithInterest;
    feeWithInterest.lead_status = "Y";
    feeWithInterest.conversation_done = false;
    feeWithInterest.handoff_needed = true;
    feeWithInterest.block_reply = false;
    feeWithInterest.handoff_type = "HOT LEAD - FEE AND CALL INTEREST";
    feeWithInterest.reason = "Answered fee question and handed off present call or service interest";
    return feeWithInterest;
  }

  if (flags.help && flags.local && flags.fee && matchedKeys.length === 3) {
    const compositeFeeDecision = buildFeeQuestionDecision_(rowObj, lastOutbound);
    if (compositeFeeDecision.handoff_needed) return compositeFeeDecision;
    const compositeFeeClause = compositeFeeDecision.reply_text.indexOf("$5,000") !== -1
      ? "The buyer-paid fee is a flat $5,000 at closing."
      : "There's no fee to you or the seller; the buyer pays a flat fee at closing only if the deal closes.";
    return {
      matched: true,
      reply_text: "I'm based in Atlanta and work nationwide, and I handle the lender-side paperwork, calls, follow-up, and negotiations through approval. " + compositeFeeClause,
      lead_status: leadStatus,
      conversation_done: done,
      handoff_needed: false,
      needs_review: false,
      block_reply: false,
      reason: "Answered a bounded service, location, and fee question"
    };
  }

  if (matchedKeys.length > 2 || (matchedKeys.length > 1 && (flags.source || flags.differentiation))) {
    return buildManualHandoffDecision_(
      "Agent asked multiple questions that need one careful human answer",
      "COMPLEX MULTI-QUESTION"
    );
  }

  if (matchedKeys.length === 1) {
    if (flags.fee) {
      const feeDecision = buildFeeQuestionDecision_(rowObj, lastOutbound);
      if (!feeDecision.handoff_needed) {
        feeDecision.lead_status = leadStatus;
        feeDecision.conversation_done = done;
      }
      return feeDecision;
    }
    if (flags.help) return {
      matched: true, reply_text: buildHowWeHelpReply_(), lead_status: leadStatus,
      conversation_done: done, handoff_needed: false, needs_review: false, block_reply: false,
      reason: "Answered how Crisp helps before generic coverage language"
    };
    if (flags.local) return {
      matched: true, reply_text: buildLocalQuestionReply_(rowObj), lead_status: leadStatus,
      conversation_done: done, handoff_needed: false, needs_review: false, block_reply: false,
      reason: "Answered where Yoni is located"
    };
    if (flags.company) return {
      matched: true, reply_text: buildCompanyIdentityReply_(), lead_status: leadStatus,
      conversation_done: done, handoff_needed: false, needs_review: false, block_reply: false,
      reason: "Answered company identity question"
    };
    if (flags.website) return {
      matched: true, reply_text: buildWebsiteReviewsReply_(), lead_status: leadStatus,
      conversation_done: done, handoff_needed: false, needs_review: false, block_reply: false,
      reason: "Answered website or reviews request"
    };
    if (flags.contact_card) return {
      matched: true, reply_text: "Sure, what's the best email for you?", lead_status: leadStatus,
      conversation_done: false, handoff_needed: false, needs_review: false, block_reply: false,
      reason: "Agent asked for a business card"
    };
    if (flags.contact_info) return {
      matched: true, reply_text: "Yoni Kutler, 404-300-9526, yoni@crispshortsales.com.", lead_status: leadStatus,
      conversation_done: done, handoff_needed: false, needs_review: false, block_reply: false,
      reason: "Agent asked for Yoni's contact information"
    };
    if (flags.experience) return {
      matched: true, reply_text: buildExperienceReplyForQuestion_(t), lead_status: leadStatus,
      conversation_done: done, handoff_needed: false, needs_review: false, block_reply: false,
      reason: "Answered approved experience question"
    };
    if (flags.timeline) return {
      matched: true, reply_text: buildShortSaleTimelineReply_(), lead_status: leadStatus,
      conversation_done: done, handoff_needed: false, needs_review: false, block_reply: false,
      reason: "Answered approved short-sale timeline question"
    };
    if (flags.number) return {
      matched: true, reply_text: "Yes, this number is great - call or text anytime. Thanks!", lead_status: leadStatus,
      conversation_done: done, handoff_needed: false, needs_review: false, block_reply: false,
      reason: "Confirmed the current texting number"
    };
    if (flags.credential) return {
      matched: true, reply_text: buildCredentialQuestionReply_(), lead_status: leadStatus,
      conversation_done: done, handoff_needed: false, needs_review: false, block_reply: false,
      reason: "Answered attorney or legal-advice question"
    };
    if (flags.negotiator) return {
      matched: true, reply_text: buildNegotiatorRoleQuestionReply_(), lead_status: leadStatus,
      conversation_done: done, handoff_needed: false, needs_review: false, block_reply: false,
      reason: "Clarified the short-sale negotiator role"
    };
    if (flags.language) return {
      matched: true, reply_text: buildSpanishCapabilityReply_(), lead_status: leadStatus,
      conversation_done: done, handoff_needed: false, needs_review: false, block_reply: false,
      reason: "Agent asked whether Yoni speaks Spanish"
    };
  }

  const answers = [];
  if (flags.company) answers.push("I'm with Crisp Short Sales.");
  if (flags.help) answers.push("I handle the lender-side paperwork, calls, follow-up, and negotiations through approval.");
  if (flags.local) answers.push("I'm based in Atlanta and work nationwide; the lender-side work is handled remotely.");
  if (flags.fee) {
    const feeDecision = buildFeeQuestionDecision_(rowObj, lastOutbound);
    if (feeDecision.handoff_needed) return feeDecision;
    answers.push(feeDecision.reply_text.indexOf("$5,000") !== -1
      ? "The buyer-paid fee is a flat $5,000 at closing."
      : "There's no fee to you or the seller; the buyer pays a flat fee at closing only if the deal closes.");
  }
  if (flags.experience) answers.push(buildExperienceReplyForQuestion_(t));
  if (flags.timeline) answers.push("A complete package and offer often takes about 60-90 days for a lender decision, though timing varies.");
  if (flags.website) answers.push("My website is https://www.crispshortsales.com.");
  if (flags.contact_card) answers.push("What's the best email for you?");
  if (flags.contact_info) answers.push("Yoni Kutler, 404-300-9526, yoni@crispshortsales.com.");
  if (flags.number) answers.push("Yes, this number is great - call or text anytime.");
  if (flags.credential) answers.push("I'm not an attorney; I handle the lender-side short-sale process and negotiations.");
  if (flags.negotiator) answers.push("Yes, essentially; I handle the short-sale process and lender negotiations through approval.");
  if (flags.language) answers.push("I'm sorry, I don't speak Spanish, but I'd still be happy to help in English.");

  return {
    matched: true,
    reply_text: answers.join(" "),
    lead_status: leadStatus,
    conversation_done: done,
    handoff_needed: false,
    needs_review: false,
    block_reply: false,
    reason: "Answered a bounded two-question inbound message"
  };
}

function applyFastRules_(text, rowObj) {
  const t = normalizeWhitespace_(String(text || "").toLowerCase());
  const lastOutbound = normalizeWhitespace_(String(rowObj && rowObj[HEADERS.last_outbound_text] || ""));

  if (isCourtesyInformationAcknowledgmentSignal_(t)) {
    return {
      matched: true,
      reply_text: "",
      lead_status: String(rowObj && rowObj[HEADERS.mailshake_status] || "Y"),
      conversation_done: false,
      handoff_needed: false,
      needs_review: false,
      block_reply: true,
      preserve_existing_state: true,
      reason: "Courtesy acknowledgment of information; no response needed"
    };
  }

  if (isComplianceOrLicensingQuestionSignal_(t)) {
    return buildManualHandoffDecision_(
      "Agent asked a licensing, statutory, or regulatory compliance question; manual review required",
      "COMPLIANCE / LICENSING QUESTION"
    );
  }

  if (isTitleCompanyRoleConfusionSignal_(t)) {
    return {
      matched: true,
      reply_text: buildTitleCompanyRoleClarificationReply_(),
      lead_status: "Y",
      conversation_done: false,
      handoff_needed: false,
      needs_review: false,
      block_reply: false,
      reason: "Clarified that Crisp's lender-side role is separate from the title company"
    };
  }

  if (isEquatorPortalSignal_(t)) {
    return {
      matched: true,
      reply_text: buildEquatorPortalReply_(),
      lead_status: "Y",
      conversation_done: false,
      handoff_needed: false,
      needs_review: false,
      block_reply: false,
      call_booking_status: "interested_no_call",
      bypass_reply_cap: true,
      reason: "Explained Yoni's Equator expertise and ability to handle portal tasks and communication"
    };
  }

  const priorityQuestion = buildPriorityQuestionDecisionV3_(t, rowObj, lastOutbound);
  if (priorityQuestion) return priorityQuestion;

  if (isExperienceTrackRecordQuestionSignal_(t)) {
    return {
      matched: true,
      reply_text: buildExperienceTrackRecordReply_(),
      lead_status: "Y",
      conversation_done: false,
      handoff_needed: false,
      needs_review: false,
      block_reply: false,
      reason: "Answered approved experience and track-record question"
    };
  }

  if (isShortSaleTimelineQuestionSignal_(t) && !isUnsupportedPerformanceStatsQuestionSignal_(t)) {
    return {
      matched: true,
      reply_text: buildShortSaleTimelineReply_(),
      lead_status: "Y",
      conversation_done: false,
      handoff_needed: false,
      needs_review: false,
      block_reply: false,
      reason: "Answered approved short-sale timeline question"
    };
  }

  if (isCurrentTextingNumberQuestionSignal_(t)) {
    const existingStatus = String(rowObj && rowObj[HEADERS.mailshake_status] || "").toUpperCase();
    const remainsClosed = existingStatus === "R";
    return {
      matched: true,
      reply_text: "Yes, this number is great - call or text anytime. Thanks!",
      lead_status: remainsClosed ? "R" : "Y",
      conversation_done: remainsClosed,
      handoff_needed: false,
      needs_review: false,
      block_reply: false,
      reason: "Confirmed the current texting number without repeating the agent's phone"
    };
  }

  if (isYoniNameAndNumberRequestSignal_(t)) {
    return {
      matched: true,
      reply_text: buildYoniNameAndNumberReply_(),
      lead_status: "Y",
      conversation_done: false,
      handoff_needed: false,
      needs_review: false,
      block_reply: false,
      reason: "Agent asked for Yoni's name and phone number"
    };
  }

  if (isListingPromotionRequestSignal_(t)) {
    return buildManualHandoffDecision_(
      "Agent asked Yoni to post, share, advertise, or circulate the listing in his area or network",
      "LISTING PROMOTION REQUEST"
    );
  }

  if (isExistingCrispRelationshipSignal_(t)) {
    const existingClient = buildManualHandoffDecision_(
      "Agent identified an active or existing Crisp/Yoni relationship; exit marketing and route to Yoni",
      "EXISTING CRISP CLIENT"
    );
    existingClient.lead_status = "R";
    return existingClient;
  }

  if (isClientConsultationInterestSignal_(t)) {
    return {
      matched: true,
      reply_text: buildClientConsultationInterestReply_(),
      lead_status: "Y",
      conversation_done: false,
      handoff_needed: false,
      needs_review: false,
      block_reply: false,
      reason: "Agent will discuss short-sale help with their client and get back to Yoni"
    };
  }

  if (isFutureNegotiationInterestSignal_(t)) {
    return {
      matched: true,
      reply_text: buildFutureInterestReply_(t),
      lead_status: "O",
      conversation_done: true,
      handoff_needed: false,
      needs_review: false,
      block_reply: false,
      reason: "Agent expressed interest in future short-sale negotiation support"
    };
  }

  if (isFutureBuyerRecontactSignal_(t)) {
    return {
      matched: true,
      reply_text: buildFutureBuyerRecontactReply_(),
      lead_status: "O",
      conversation_done: true,
      handoff_needed: false,
      needs_review: false,
      block_reply: false,
      reason: "Agent will reconnect after securing a buyer; warm future interest closed without takeover"
    };
  }

  if (isRelationshipOnlyAfterExistingCoverageSignal_(t, rowObj)) {
    return {
      matched: true,
      reply_text: buildRelationshipOnlyCloseoutReply_(t),
      lead_status: "O",
      conversation_done: true,
      handoff_needed: false,
      needs_review: false,
      block_reply: false,
      call_booking_status: "warm_future_interest",
      reason: "Current file already covered; relationship left open without sales follow-up"
    };
  }

  if (isNotShortSaleVagueFutureSignal_(t)) {
    return {
      matched: true,
      reply_text: "Thanks for letting me know. If a short sale comes up in the future, feel free to reach out. Good luck with the listing!",
      lead_status: "R",
      conversation_done: true,
      handoff_needed: false,
      needs_review: false,
      block_reply: false,
      reason: "Current listing is not a short sale; vague future interest only"
    };
  }

  if (isUnderControlFutureHelpCloseoutSignal_(t)) {
    return {
      matched: true,
      reply_text: "Understood, thanks for letting me know. If anything changes, feel free to reach out.",
      lead_status: "R",
      conversation_done: true,
      handoff_needed: false,
      needs_review: false,
      block_reply: false,
      reason: "Agent has the current matter under control and will reach out only if help is later needed"
    };
  }

  if (isSelfHandlingOpportunitySignal_(t)) {
    return {
      matched: true,
      reply_text: "I understand, and I help a lot of agents in the same situation. I can take the lender paperwork, calls, follow-up, and negotiations off your plate if you ever want help with that part.",
      lead_status: "Y",
      conversation_done: false,
      handoff_needed: false,
      needs_review: false,
      block_reply: false,
      reason: "Agent is handling the short sale themselves; gave one value response"
    };
  }

  if (isLocalQuestionSignal_(t)) {
    return {
      matched: true,
      reply_text: buildLocalQuestionReply_(rowObj),
      lead_status: "Y",
      conversation_done: false,
      handoff_needed: false,
      needs_review: false,
      block_reply: false,
      reason: "Agent asked where Yoni is located"
    };
  }

  if (isContactCardRequestSignal_(t)) {
    return {
      matched: true,
      reply_text: "Sure, what's the best email for you?",
      lead_status: "Y",
      conversation_done: false,
      handoff_needed: false,
      needs_review: false,
      block_reply: false,
      reason: "Agent asked for a business card or contact information"
    };
  }

  if (isShortSaleSourceQuestion_(t)) {
    return {
      matched: true,
      reply_text: "I thought i saw in the listing that it said it was a short sale. My mistake if i misread that. Thanks",
      lead_status: "R",
      conversation_done: true,
      handoff_needed: false,
      needs_review: false,
      block_reply: false,
      reason: "Agent asked why the listing was considered a short sale"
    };
  }

  if (isInPersonMeetingRequestSignal_(t)) {
    return {
      matched: true,
      reply_text: "",
      lead_status: "Y",
      conversation_done: false,
      handoff_needed: true,
      needs_review: false,
      block_reply: true,
      handoff_type: "IN-PERSON MEETING REQUEST",
      reason: "Agent requested an in-person meeting or office visit"
    };
  }

  if (isMissedCallOrAlternateNumberSignal_(t) && lastOutboundWasCallPromise_(lastOutbound)) {
    return buildManualHandoffDecision_(
      "Agent mentioned a missed call or gave an alternate callback number after a prior call promise",
      "MISSED CALL FOLLOW-UP"
    );
  }

  if (isNoCurrentShortSaleHelpSignal_(t)) {
    return {
      matched: true,
      reply_text: "Absolutely, I'd be happy to help. I can handle the lender paperwork, calls, follow-up, and negotiations through approval, so you can focus on your client and the listing. Would you like to go over everything briefly by phone?",
      lead_status: "Y",
      conversation_done: false,
      handoff_needed: false,
      needs_review: false,
      block_reply: false,
      call_booking_status: "interested_no_call",
      reason: "Agent said no one is currently helping with the short-sale process"
    };
  }

  if (isDirectHelpRequestSignal_(t)) {
    return {
      matched: true,
      reply_text: "Absolutely, I'd love to help. Are you free for a quick call now, or would later today be better?",
      lead_status: "Y",
      conversation_done: false,
      handoff_needed: true,
      needs_review: false,
      block_reply: false,
      call_booking_status: "interested_no_call",
      handoff_type: "HOT LEAD - DIRECT HELP REQUEST",
      reason: "Agent directly asked for help"
    };
  }

  if (isSelfInitiatedDeferredContactSignal_(t)) {
    return {
      matched: true,
      reply_text: "No problem - message me when you're free.",
      lead_status: "Y",
      conversation_done: false,
      handoff_needed: false,
      needs_review: false,
      alert_needed: true,
      send_reply_before_handoff: true,
      block_reply: false,
      call_booking_status: "interested_no_call",
      handoff_type: "DEFERRED HOT LEAD",
      reason: "Agent is busy and will initiate contact later; preserved as a hot lead without immediate call permission"
    };
  }

  if (isImmediateCallSignal_(t) || isOpenCallWindowSignal_(t)) {
    return {
      matched: true,
      reply_text: "Perfect, thanks.",
      lead_status: "Y",
      conversation_done: false,
      handoff_needed: true,
      needs_review: false,
      block_reply: false,
      call_booking_status: "call_now",
      handoff_type: "CALL WINDOW OPEN",
      reason: "Agent is available for a call; acknowledged and handed off"
    };
  }

  if (isUnavailableUntilCallbackReferenceSignal_(t)) {
    const callbackReference = extractScheduledCallbackReference_(t);
    return {
      matched: true,
      reply_text: "No problem. What time " + callbackReference + " works best for a quick call?",
      lead_status: "Y",
      conversation_done: false,
      handoff_needed: false,
      needs_review: false,
      block_reply: false,
      call_booking_status: "interested_no_call",
      reason: "Agent is unavailable until a future day; asked for a time instead of promising an unscheduled follow-up"
    };
  }

  if (isSchedulingSignal_(t)) {
    const hasSpecificTime = !!extractSchedulingTimePhrase_(t);
    const callbackTime = extractScheduledCallbackReference_(t) ||
      extractSchedulingTimePhrase_(t) || t;
    return {
      matched: true,
      reply_text: hasSpecificTime ? "Perfect, thanks." : "Sounds good. What time works best for you?",
      lead_status: "Y",
      conversation_done: false,
      handoff_needed: true,
      needs_review: false,
      block_reply: false,
      call_booking_status: "scheduled_callback",
      callback_requested: "yes",
      callback_time: callbackTime,
      handoff_type: "SCHEDULED CALLBACK",
      reason: "Agent proposed callback timing; acknowledged and handed off"
    };
  }

  if (isOfferSubmissionConfusionSignal_(t)) {
    if (lastOutboundWasOfferScopeClarification_(rowObj)) {
      return buildManualHandoffDecision_(
        "Agent repeated a buyer or offer-submission request after Crisp's role was clarified",
        "BUYER OR OFFER CONFUSION"
      );
    }
    return {
      matched: true,
      reply_text: "I don't represent a buyer or submit offers. I handle the lender-side short-sale work for the listing agent.",
      lead_status: "Y",
      conversation_done: false,
      handoff_needed: true,
      needs_review: false,
      block_reply: false,
      send_reply_before_handoff: true,
      handoff_type: "OFFER SUBMISSION REQUEST",
      reason: "Clarified Crisp's role and routed the actionable offer-submission request to Yoni"
    };
  }

  if (isPresentServiceInterestSignal_(t) && !isPhoneCallInterestSignal_(t)) {
    return buildManualHandoffDecision_(
      "Agent expressed present interest in Crisp's services",
      "RENEWED INTEREST"
    );
  }

  if (isPhoneCallInterestSignal_(t)) {
    return {
      matched: true,
      reply_text: "Sure, I'd love to. When's a good time for me to call, now or later today?",
      lead_status: "Y",
      conversation_done: false,
      handoff_needed: true,
      needs_review: false,
      block_reply: false,
      handoff_type: "CALL REQUESTED",
      reason: "Agent wants to speak by phone"
    };
  }

  if (isOpenCallWindowSignal_(t)) {
    return {
      matched: true,
      reply_text: "Perfect, thanks.",
      lead_status: "Y",
      conversation_done: false,
      handoff_needed: true,
      needs_review: false,
      block_reply: false,
      handoff_type: "CALL WINDOW OPEN",
      reason: "Agent shared an immediate callback window"
    };
  }

  if (isGatekeeperForwardingSignal_(t)) {
    return {
      matched: true,
      reply_text: buildGatekeeperForwardingReply_(rowObj),
      lead_status: "Y",
      conversation_done: false,
      handoff_needed: false,
      needs_review: false,
      block_reply: false,
      reason: "Gatekeeper or front desk is passing the message along"
    };
  }

  if (isOpenToReviewOfferSignal_(t)) {
    return {
      matched: true,
      reply_text: buildOfferReviewReply_(rowObj),
      lead_status: "Y",
      conversation_done: false,
      handoff_needed: false,
      needs_review: false,
      block_reply: false,
      reason: "Agent is handling it personally but is open to hearing what Crisp offers"
    };
  }

  if (isWebsiteReviewsRequestSignal_(t)) {
    return {
      matched: true,
      reply_text: buildWebsiteReviewsReply_(),
      lead_status: "Y",
      conversation_done: false,
      handoff_needed: false,
      needs_review: false,
      block_reply: false,
      reason: "Agent asked for website, brochure, flyer, reviews, or testimonials"
    };
  }

  if (isDifferentiationQuestionSignal_(t)) {
    return {
      matched: true,
      reply_text: buildDifferentiationQuestionReply_(),
      lead_status: "Y",
      conversation_done: false,
      handoff_needed: false,
      needs_review: false,
      block_reply: false,
      alert_needed: true,
      handoff_type: "HOT LEAD - DIFFERENTIATION QUESTION",
      reason: "Answered differentiation and communication question; hot-lead alert requested"
    };
  }

  const providedEmail = extractEmailAddress_(t);
  const targetEmail = normalizeEmailAddress_(providedEmail || String(rowObj && rowObj[HEADERS.email] || ""));
  const isWarmInfoOpportunity = isDeclineWithInfoRequestSignal_(t, rowObj);

  if (providedEmail || isEmailRequestSignal_(t)) {
    if (targetEmail) {
      const acknowledgementReply = hasServiceInfoRequestContext_(t, rowObj)
        ? buildServiceInfoEmailAcknowledgement_()
        : getInfoEmailAcknowledgementReply_();
      return {
        matched: true,
        reply_text: acknowledgementReply,
        lead_status: isWarmInfoOpportunity ? "O" : "Y",
        conversation_done: isWarmInfoOpportunity,
        handoff_needed: false,
        needs_review: false,
        block_reply: false,
        send_info_email: true,
        info_email_to: targetEmail,
        reason: isWarmInfoOpportunity
          ? "Current opportunity declined or covered; agent requested information for future opportunities"
          : (providedEmail
            ? "Agent sent an email address; info email approval requested"
            : "Agent asked for info by email; info email approval requested")
      };
    }

    const serviceInfoRequested = hasServiceInfoRequestContext_(t, rowObj);
    return {
      matched: true,
      reply_text: serviceInfoRequested
        ? buildServiceInfoEmailAcknowledgement_(false)
        : "sure, no problem. What is your email?",
      lead_status: isWarmInfoOpportunity ? "O" : "Y",
      conversation_done: false,
      handoff_needed: false,
      needs_review: false,
      block_reply: false,
      reason: isWarmInfoOpportunity
        ? "Current opportunity declined or covered; requested future information and asked for email address"
        : "Agent asked for info by email but no email address is available yet"
    };
  }

  if (isStatsOrNumericClaimQuestion_(t)) {
    return buildManualHandoffDecision_(
      "Agent asked for stats, success rates, timelines, volume, or other numbers that need manual response",
      "STATS QUESTION"
    );
  }

  if (isDeferredInterestSignal_(t)) {
    return buildManualHandoffDecision_(
      "Agent said they may reach out later or when they have more time after showing interest",
      "DEFERRED INTEREST"
    );
  }

  const buyerRequestPatterns = [
    /\bjust need (?:a )?buyers?\b/,
    /\b(?:i|we) need (?:a )?buyers?\b/,
    /\bneed (?:a )?buyers?\b/,
    /\bdo you have (?:a )?buyers?\b/,
    /\bhave (?:any )?buyers?\b/,
    /\bgot (?:any )?buyers?\b/,
    /\bbuyer list\b/,
    /\blist of buyers?\b/,
    /\bsend (?:me |us )?(?:any )?buyers?\b/,
    /\bbring (?:me |us )?(?:a )?buyers?\b/
  ];

  for (const pattern of buyerRequestPatterns) {
    if (pattern.test(t)) {
      return {
        matched: true,
        reply_text: "I don't necessarily have a buyer I can bring you in the deal, but I can help you find a buyer by letting them know you have a short sale specialist helping to expedite the process with the lender.",
        lead_status: "Y",
        conversation_done: false,
        handoff_needed: false,
        needs_review: false,
        block_reply: false,
        reason: "Agent asked about buyers or said they only need a buyer"
      };
    }
  }

  const helpQuestionPatterns = [
    /\bhow do you help\b/,
    /\bhow can you help\b/,
    /\bwhat do you do\b/,
    /\bwhat exactly do you do\b/,
    /\bwhat do you handle\b/,
    /\bhow does this work\b/,
    /\bhow does that work\b/,
    /\bwhat does that look like\b/,
    /\bwhat are you offering\b/,
    /\bwhat kind of help\b/
  ];

  for (const pattern of helpQuestionPatterns) {
    if (pattern.test(t)) {
      return {
        matched: true,
        reply_text: buildHowWeHelpReply_(),
        lead_status: "Y",
        conversation_done: false,
        handoff_needed: false,
        needs_review: false,
        block_reply: false,
        reason: "Asked how Crisp helps or what Yoni does"
      };
    }
  }

  if (isPaymentOrFeeQuestionSignal_(t)) {
    return buildFeeQuestionDecision_(rowObj, lastOutbound);
  }

  if (isCompanyIdentityQuestionSignal_(t)) {
    return {
      matched: true,
      reply_text: buildCompanyIdentityReply_(),
      lead_status: "Y",
      conversation_done: false,
      handoff_needed: false,
      needs_review: false,
      block_reply: false,
      reason: "Asked who Yoni is with / whether he is a mortgage broker"
    };
  }

  if (isClearNoSignal_(t)) {
    return {
      matched: true,
      reply_text: getStandardNoCloseoutReply_(),
      lead_status: "R",
      conversation_done: true,
      handoff_needed: false,
      needs_review: false,
      block_reply: false,
      reason: "Clear no / closed out"
    };
  }

  const hardNoPatterns = [
    /\bwrong number\b/,
    /\bremove me\b/,
    /\bdon't text\b/,
    /\bdo not text\b/
  ];

  for (const pattern of hardNoPatterns) {
    if (pattern.test(t)) {
      return {
        matched: true,
        reply_text: "",
        lead_status: "R",
        conversation_done: true,
        handoff_needed: false,
        needs_review: false,
        block_reply: true,
        reason: "Negative / opt-out style response"
      };
    }
  }

  return { matched: false };
}

function isPhoneCallInterestSignal_(text) {
  const t = normalizeWhitespace_(String(text || "").toLowerCase());

  if (isSchedulingSignal_(t) || isImmediateCallSignal_(t)) {
    return false;
  }

  const patterns = [
    /\binterested\b.*\bcall\b/,
    /\bhop\w*\s+on\s+a\s+call\b/,
    /\bwould like to\b.*\bcall\b/,
    /\bwant to\b.*\bcall\b/,
    /\bopen to\b.*\bcall\b/,
    /\bwilling to\b.*\bcall\b/,
    /\blearn more\b.*\bcall\b/,
    /\btalk\b.*\bphone\b/,
    /\bchat\b.*\bphone\b/,
    /\blet['’]?s\s+(?:talk|chat|speak)\b/,
    /\bquick\s+call\b/,
    /\b(?:please|can you|could you|would you)\s+call\s+me\b/
  ];

  return patterns.some(pattern => pattern.test(t));
}

function isPresentServiceInterestSignal_(text) {
  const t = normalizeWhitespace_(String(text || "").toLowerCase());
  if (!t || /\b(?:not interested|no thanks?|do not|don['\u2019]?t|dont)\b/.test(t)) {
    return false;
  }
  if (/\b(?:in the future|someday|if i need|if we need|keep (?:you|your|the) (?:in mind|info|information))\b/.test(t)) {
    return false;
  }
  if (/\b(?:would love|want|need|could use|can use)\s+(?:some\s+)?help\b/.test(t) ||
      /\b(?:interested|open)\s+(?:in|to)\s+(?:hearing|learning)\s+more\b/.test(t)) {
    return true;
  }
  const interest = /\b(?:i(?:['\u2019]?m| am)?|we(?:['\u2019]?re| are)?)\s+(?:am\s+|are\s+)?(?:interested|open to|ready to|would like|want|need|could use)\b/.test(t);
  const service = /\b(?:your services?|crisp(?: short sales?)?|short sale help|help with (?:this|the|my|our) (?:file|listing|short sale)|work with you|learn more|more details|getting more details|see how you (?:can|could) help)\b/.test(t);
  return interest && service;
}

function isCompanyIdentityQuestionSignal_(text) {
  const t = normalizeWhitespace_(String(text || "").toLowerCase());
  const patterns = [
    /\bwho are you with\b/,
    /\bwhat company\b/,
    /\bwho do you work with\b/,
    /\bwho do you work for\b/,
    /\bare you a mtg broker\b/,
    /\bare you a mortgage broker\b/,
    /\bwith what company\b/
  ];
  return patterns.some(function(pattern) { return pattern.test(t); });
}

function buildCompanyIdentityReply_() {
  return "I'm with Crisp Short Sales. I handle lender-side short-sale processing and negotiations for agents and homeowners.";
}

function buildCoveredCompanyIdentityReply_() {
  return "My company is Crisp Short Sales. Thanks for letting me know you already have help, and good luck with the file.";
}

function isAutomatedRoutingNoticeSignal_(text) {
  const t = normalizeWhitespace_(String(text || "").toLowerCase());
  if (!t) return false;

  const patterns = [
    /\byour message was sent to\b.*\b(?:agent|broker|realtor|representative)\b/,
    /\b(?:we|i)\s+(?:sent|passed|forwarded|routed)\s+(?:your|this|the)\s+message\s+(?:to|along)\b/,
    /\bmessage\s+(?:sent|forwarded|routed)\s+to\s+(?:a\s+)?(?:redfin\s+)?(?:premier\s+)?agent\b/,
    /\b(?:please\s+)?(?:call|text|contact)\s+\d{3}[-.\s]?\d{3}[-.\s]?\d{4}\s+instead\b/,
    /\bautomated\s+(?:message|notice|notification)\b.*\b(?:agent|broker|routing|forward)\b/,
    /\bredfin\b.*\b(?:premier\s+agent|passed\s+this\s+message|message\s+was\s+sent)\b/,
    /\byou(?:'|’)ve reached\b.*\b(?:different|another|alternate) number (?:for|to) text(?:ing)?\b.*\bwe(?:'|’)ll send you (?:a )?message from that number\b/
  ];
  return patterns.some(function(pattern) { return pattern.test(t); });
}

function buildWebsiteReviewsReply_() {
  return "https://www.crispshortsales.com\nYou can also find reviews from agents and homeowners on Google.";
}

function isWebsiteReviewsRequestSignal_(text) {
  const t = normalizeWhitespace_(String(text || "").toLowerCase());
  if (!t) return false;
  return /\b(?:website|web site|brochure|flyer|flier|one[- ]?pager|reviews|testimonials?)\b/.test(t);
}

function isDifferentiationQuestionSignal_(text) {
  const t = normalizeWhitespace_(String(text || "").toLowerCase());
  if (!t || !/\b(?:how|what|why|different|differs?|compare|compares?|communication|documentation|updates?)\b/.test(t)) {
    return false;
  }
  const difference = /\b(?:how|what|why)\b.{0,80}\b(?:different|differs?|better|compare|compares?)\b/.test(t) ||
    /\b(?:different|differs?|better)\b.{0,80}\b(?:than|from|with)\b/.test(t);
  const communication = /\b(?:communication|communicat(?:e|ion|ing)|documentation|documents?|paperwork|updates?)\b/.test(t);
  const existingHelp = isAlreadyHandledSignal_(t) || /\b(?:theirs?|them|their|my current|our current)\b/.test(t);
  return (difference && existingHelp) || (difference && communication) || (communication && existingHelp);
}

function buildDifferentiationQuestionReply_() {
  return "I focus exclusively on the lender-side short-sale work and keep you updated throughout the process. If that sounds useful, I'm happy to talk through your listing.";
}

function isClosedMarketingConversation_(rowObj) {
  const aiState = String(rowObj && rowObj[HEADERS.ai_state] || "").toLowerCase();
  const bookingStatus = String(rowObj && rowObj[HEADERS.call_booking_status] || "").toLowerCase();
  const leadStatus = String(rowObj && rowObj[HEADERS.mailshake_status] || "").toUpperCase();
  return aiState === "done" || bookingStatus === "closed_no_interest" || leadStatus === "R";
}

function isOpenCallWindowSignal_(text) {
  const t = normalizeWhitespace_(String(text || "").toLowerCase());

  const immediateAvailabilityPatterns = [
    /\b(?:i'?m|im)\s+free\s+now\b/,
    /\b(?:i'?m|im)\s+available\s+now\b/,
    /\bavailable now\b/,
    /\bfree now\b/,
    /\bnow works\b/,
    /\banytime now\b/,
    /\bright now\b/
  ];

  const callbackWindowPatterns = [
    /\bnow until\b/,
    /\bnow till\b/,
    /\buntil\s+\d{1,2}(?::\d{2})?\s*(?:a|am|p|pm)?\b/,
    /\btill\s+\d{1,2}(?::\d{2})?\s*(?:a|am|p|pm)?\b/,
    /\bthrough\s+\d{1,2}(?::\d{2})?\s*(?:a|am|p|pm)?\b/
  ];

  const mentionsImmediateAvailability = immediateAvailabilityPatterns.some(pattern => pattern.test(t));
  const mentionsWindow = callbackWindowPatterns.some(pattern => pattern.test(t));

  return mentionsImmediateAvailability || (mentionsWindow && /\bcall\b/.test(t));
}

function isAiOrAutomationQuestionSignal_(text) {
  const t = normalizeWhitespace_(String(text || "").toLowerCase());

  const patterns = [
    /\bis\s+this\s+(?:actually\s+)?(?:your\s+phone|you|yoni)\b/,
    /\bis\s+this\s+(?:ai|a\s+bot|bot|automated|auto[-\s]?generated|chatbot)\b/,
    /\bis\s+this\s+(?:ai\s+)?prompted\b/,
    /\bai\s+prompted\b/,
    /\bai[-\s]?generated\b/,
    /\bare\s+you\s+(?:ai|a\s+bot|bot|automated|real|a\s+real\s+person|human)\b/,
    /\bam\s+i\s+texting\s+(?:ai|a\s+bot|bot|a\s+real\s+person|a\s+human)\b/,
    /\bis\s+there\s+(?:a\s+)?(?:real\s+person|human)\b/,
    /\bautomated\s+(?:text|message|sms|response)\b/,
    /\bauto\s+(?:text|message|sms|response)\b/
  ];

  return patterns.some(pattern => pattern.test(t));
}

function isFutureNegotiationInterestSignal_(text) {
  const t = normalizeWhitespace_(String(text || "").toLowerCase());
  if (!t) {
    return false;
  }

  const opennessPatterns = [
    /\bopen[-\s]?minded\b/,
    /\bopen\s+to\b/,
    /\binterested\b/,
    /\binterested\s+about\b/,
    /\bi\s+would\s+like\b/,
    /\bi'?d\s+like\b/,
    /\bwould\s+like\b/,
    /\bwould\s+love\b/,
    /\bi(?:'|\u2019)?d\s+love\b/,
    /\bi\s+want\b/,
    /\bwant\s+to\s+know\b/,
    /\bwould\s+consider\b/,
    /\bconsidering\b/,
    /\bwhat\s+you\s+have\s+to\s+say\b/
  ];

  const futurePatterns = [
    /\bfuture\b/,
    /\bgoing\s+forward\b/,
    /\bdown\s+the\s+road\b/,
    /\blater\b/,
    /\bnext\s+one\b/,
    /\bthe\s+next\s+one\b/,
    /\bstacking\s+up\b/
  ];

  const shortSaleProcessPatterns = [
    /\bnegotiation\b/,
    /\bnegotiations\b/,
    /\bshort\s+sales?\b/,
    /\bdistressed\s+propert(?:y|ies)\b/,
    /\bforeclosures?\b/,
    /\bbank\s+side\b/,
    /\blender\b/,
    /\bfees?\b/,
    /\bcharge\b/,
    /\bpricing\b/,
    /\binformation\b/,
    /\binfo\b/,
    /\bprocess(?:es)?\b/
  ];

  return opennessPatterns.some(pattern => pattern.test(t)) &&
    futurePatterns.some(pattern => pattern.test(t)) &&
    shortSaleProcessPatterns.some(pattern => pattern.test(t));
}

function buildFutureInterestReply_(inboundText) {
  const t = normalizeWhitespace_(String(inboundText || "").toLowerCase());
  const asksAboutFee = /\bfees?\b/.test(t) ||
    /\bcharge\b/.test(t) ||
    /\bpricing\b/.test(t) ||
    /\bwhat\s+do\s+you\s+charge\b/.test(t) ||
    /\bhow\s+much\b/.test(t);

  if (asksAboutFee) {
    return "Absolutely. There's no fee to you or the seller; the buyer pays a flat fee at closing only if the deal closes.";
  }

  return "Absolutely. I handle the lender paperwork, calls, follow-up, and negotiations through approval. If you want to compare notes on a future file, I'd be happy to talk.";
}

function isInPersonMeetingRequestSignal_(text) {
  const t = normalizeWhitespace_(String(text || "").toLowerCase());

  const directInPersonPatterns = [
    /\bdrop by\b/,
    /\bcome by\b/,
    /\bstop by\b/,
    /\bmeet in person\b/,
    /\bin person\b/,
    /\bmeet at the office\b/,
    /\bmeet at our office\b/,
    /\bcome to the office\b/,
    /\bdrop by the office\b/,
    /\bstop by the office\b/,
    /\bvisit the office\b/
  ];

  if (directInPersonPatterns.some(pattern => pattern.test(t))) {
    return true;
  }

  const mentionsOffice = /\boffice\b/.test(t);
  const mentionsScheduling = [
    /\bavailability\b/,
    /\bwhat time works\b/,
    /\bwhat time\b/,
    /\btuesday\b/,
    /\bwednesday\b/,
    /\bmonday\b/,
    /\bthursday\b/,
    /\bfriday\b/,
    /\bset up a call\b/,
    /\bset up a conversation\b/,
    /\bhave a conversation\b/,
    /\bchat\b/
  ].some(pattern => pattern.test(t));

  return mentionsOffice && mentionsScheduling;
}

function isGatekeeperForwardingSignal_(text) {
  const t = normalizeWhitespace_(String(text || "").toLowerCase());

  const gatekeeperRole = [
    /\bfront desk\b/,
    /\breception\b/,
    /\breceptionist\b/,
    /\bassistant\b/,
    /\badmin\b/,
    /\boffice\b/
  ].some(pattern => pattern.test(t));

  const relayIntent = [
    /\bbring this forward\b/,
    /\bpass this along\b/,
    /\bforward this\b/,
    /\bforward it\b/,
    /\bget back with you\b/,
    /\bget back to you\b/,
    /\bshare this with\b/,
    /\blet .* know\b/,
    /\bi'?ll let .* know\b/,
    /\bwe'?ll let .* know\b/
  ].some(pattern => pattern.test(t));

  return gatekeeperRole && relayIntent;
}

function buildGatekeeperForwardingReply_(rowObj) {
  const firstName = getCanonicalFirstName_(rowObj);
  if (firstName) {
    return "Thanks, I appreciate it. Please have " + firstName + " text me here if they'd like to talk.";
  }

  return "Thanks, I appreciate it. Please have the agent text me here if they'd like to talk.";
}

function isOpenToReviewOfferSignal_(text) {
  const t = normalizeWhitespace_(String(text || "").toLowerCase());

  const isHandlingPersonally = [
    /\bhandling that part myself\b/,
    /\bhandle that part myself\b/,
    /\bdoing that part myself\b/,
    /\bdoing it myself\b/,
    /\bhandling it myself\b/,
    /\bi am handling\b.*\bmyself\b/,
    /\bi'm handling\b.*\bmyself\b/
  ].some(pattern => pattern.test(t));

  const isOpenToReview = [
    /\bwilling to review\b/,
    /\breview what you have to offer\b/,
    /\bwhat you have to offer\b/,
    /\bopen to hearing\b/,
    /\binterested in hearing\b/,
    /\binterested to learn more\b/,
    /\bopen to learn more\b/,
    /\bwilling to hear more\b/
  ].some(pattern => pattern.test(t));

  return isHandlingPersonally && isOpenToReview;
}

function buildOfferReviewReply_(rowObj) {
  const firstName = getCanonicalFirstName_(rowObj);
  const thanksLine = firstName ? "Thanks " + firstName + ", I'd love to explain." : "Thanks, I'd love to explain.";

  return thanksLine + " I handle the lender paperwork, calls, follow-up, and negotiations through approval, with no cost to you or the seller. Would a quick call now or later today be easier?";
}

function buildHowWeHelpReply_() {
  return "I handle the lender side of the short sale, including the paperwork, calls, follow-up, and negotiations through approval. It takes that work off your plate so you can focus on the listing and your client.";
}

function isDirectHelpRequestSignal_(text) {
  const t = normalizeWhitespace_(String(text || "").toLowerCase());
  return /^(?:actually[, ]+)?(?:i|we)\s+(?:do\s+)?need\s+(?:some\s+)?help[.!?]*$/.test(t) ||
    /\b(?:i|we)\s+(?:(?:do\s+)?need|would\s+love|could\s+use|can\s+use)\s+(?:some\s+)?help\b/.test(t);
}

function isNoCurrentShortSaleHelpSignal_(text) {
  const t = normalizeWhitespace_(String(text || "").toLowerCase());
  if (!t || /\b(?:do not|don['’]?t|dont)\s+need\s+help\b/.test(t)) return false;
  if (/\b(?:already|currently)\s+(?:have|using|working with)\s+(?:help|someone|anyone|a company|a negotiator)\b/.test(t)) return false;

  return /\b(?:i|we)\s+(?:do not|don['’]?t|dont)\s+have\s+(?:any\s+)?(?:help|anyone|anybody|someone)\s+(?:helping|handling|working)?\b/.test(t) ||
    /\b(?:no one|nobody)\s+(?:is\s+)?(?:helping|handling|working)(?:\s+with\s+me)?\b/.test(t) ||
    /\b(?:i|we)\s+have\s+no\s+(?:help|one|one helping|one handling)\b/.test(t);
}

function isOfferSubmissionConfusionSignal_(text) {
  const t = normalizeWhitespace_(String(text || "").toLowerCase());
  return /\b(?:please\s+)?submit\s+(?:an?|the|my|our|your)\s+offer\b/.test(t) ||
    /\b(?:can|could|will|would)\s+you\s+submit\s+(?:an?|the|my|our|your)\s+offer\b/.test(t) ||
    /\bsubmit\s+(?:it|this)\s+to\s+(?:the\s+)?(?:seller|listing agent|agent)\b/.test(t);
}

function lastOutboundWasOfferScopeClarification_(rowObj) {
  const t = normalizeWhitespace_(String(rowObj && rowObj[HEADERS.last_outbound_text] || "").toLowerCase());
  return t.indexOf("i don't represent a buyer or submit offers") !== -1;
}

function isMissedCallOrAlternateNumberSignal_(text) {
  const t = normalizeWhitespace_(String(text || "").toLowerCase());

  const patterns = [
    /\bdidn'?t see the call\b/,
    /\bdid not see the call\b/,
    /\bdidn'?t get the call\b/,
    /\bdid not get the call\b/,
    /\bmissed the call\b/,
    /\bstraight to vm\b/,
    /\bstraight to voicemail\b/,
    /\bgoing straight to vm\b/,
    /\bgoing straight to voicemail\b/,
    /\bhaving trouble\b/,
    /\bcall my cell\b/,
    /\bcall this number\b/,
    /\bcall me on my cell\b/,
    /\buse this number\b/
  ];

  return patterns.some(pattern => pattern.test(t));
}

function isDeferredInterestSignal_(text) {
  const t = normalizeWhitespace_(String(text || "").toLowerCase());
  if (!t) {
    return false;
  }

  const patterns = [
    /\bi['’]?ll\s+reach\s+out\b.*\b(when|once|later|time|can)\b/,
    /\bi\s+will\s+reach\s+out\b.*\b(when|once|later|time|can)\b/,
    /\bwe['’]?ll\s+reach\s+out\b.*\b(when|once|later|time|can)\b/,
    /\bwe\s+will\s+reach\s+out\b.*\b(when|once|later|time|can)\b/,
    /\bi['’]?ll\s+circle\s+back\b/,
    /\bi\s+will\s+circle\s+back\b/,
    /\bi['’]?ll\s+get\s+back\s+to\s+you\b/,
    /\bi\s+will\s+get\s+back\s+to\s+you\b/,
    /\bi['’]?ll\s+be\s+in\s+touch\b/,
    /\bi\s+will\s+be\s+in\s+touch\b/,
    /\blet\s+me\s+circle\s+back\b/,
    /\blet\s+me\s+get\s+back\s+to\s+you\b/
  ];

  return patterns.some(pattern => pattern.test(t));
}

function normalizeSmsReactionText_(text) {
  return normalizeWhitespace_(
    String(text || "")
      .replace(/[\u2009\u200a\u200b\u200c\u200d\u2060\ufeff]/g, " ")
      .replace(/^[\s"'“”‘’]+|[\s"'“”‘’]+$/g, "")
  ).toLowerCase();
}

function normalizeSmsReactionComparisonText_(text) {
  return normalizeSmsReactionText_(text).replace(/['\u2018\u2019]/g, "");
}

function extractSmsReactionTarget_(text) {
  const raw = normalizeWhitespace_(
    String(text || "").replace(/[\u2009\u200a\u200b\u200c\u200d\u2060\ufeff]/g, " ")
  );
  if (!raw) return null;

  let match = raw.match(/^(liked|loved|emphasized|disliked|laughed at|questioned)\s+["“]?(.+?)["”]?$/i);
  if (match) {
    return {
      explicit: true,
      target: normalizeSmsReactionText_(match[2])
    };
  }

  match = raw.match(/^to\s+["“]?(.+?)["”]?$/i);
  if (match) {
    return {
      explicit: false,
      target: normalizeSmsReactionText_(match[1])
    };
  }

  return null;
}

function canonicalizeSmsInboundDedupeMessage_(text) {
  const reaction = extractSmsReactionTarget_(text);
  if (reaction && reaction.target) {
    return "__reaction__|" + reaction.target;
  }
  return normalizeSmsReactionText_(text);
}

function isSmsReactionToLastOutbound_(text, rowObj) {
  const reaction = extractSmsReactionTarget_(text);
  if (!reaction || !reaction.target) return false;

  const reactionTarget = normalizeSmsReactionComparisonText_(reaction.target);
  const lastOutbound = normalizeSmsReactionComparisonText_(
    rowObj && rowObj[HEADERS.last_outbound_text]
  );
  return Boolean(lastOutbound && reactionTarget === lastOutbound);
}

function isFinalCourtesyReply_(text) {
  const t = normalizeWhitespace_(String(text || "").toLowerCase())
    .replace(/(?:[\s.!?]+|[\u2600-\u27bf]|\ud83c[\udc00-\udfff]|\ud83d[\udc00-\udfff]|\ud83e[\udd00-\udfff])+$/g, "")
    .replace(/[^a-z0-9\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();

  const patterns = [
    /^thanks$/,
    /^thank you$/,
    /^thankyou$/,
    /^ok$/,
    /^okay$/,
    /^got it$/,
    /^will do$/,
    /^will do thank you$/,
    /^will do thanks$/,
    /^sounds good$/,
    /^sounds good thank you$/,
    /^sounds good thanks$/,
    /^appreciate it$/,
    /^thank you so much$/,
    /^thanks so much$/,
    /^ok thank you$/,
    /^okay thank you$/,
    /^thank you i appreciate it$/,
    /^thanks i appreciate it$/,
    /^thank you appreciate it$/,
    /^thank you!$/,
    /^thanks!$/,
    /^👍$/,
    /^thumbs up$/,
    /^great to know! thank you$/,
    /^great to know thank you$/,
    /^thank you$/,
    /^thanks$/
  ];

  return patterns.some(pattern => pattern.test(t));
}

function isSubstantiveFollowupSignal_(text) {
  const raw = normalizeWhitespace_(String(text || ""));
  if (!raw || isFinalCourtesyReply_(raw)) return false;
  if (raw.indexOf("?") !== -1) return true;
  const t = normalizeWhitespace_(raw.toLowerCase());
  const patterns = [
    /^(?:who|what|when|where|why|how|can|could|would|do|does|did|is|are|am|will|should|may)\b/,
    /\b(?:send|share|email|text)\b.*\b(?:business\s+card|contact\s+card|contact\s+info|information|website|link)\b/,
    /\b(?:business\s+card|contact\s+card)\b/
  ];
  return patterns.some(pattern => pattern.test(t));
}

function isClosedNotShortSaleConversation_(rowObj) {
  if (!rowObj || String(rowObj[HEADERS.ai_state] || "").toLowerCase() !== "done") {
    return false;
  }
  const summary = normalizeWhitespace_(String(rowObj[HEADERS.conversation_summary] || "").toLowerCase());
  return /\bnot (?:actually )?a short sale\b|\bchanged listing\b/.test(summary);
}

function isPostCloseoutNotShortSaleContinuation_(text, rowObj) {
  if (!isClosedNotShortSaleConversation_(rowObj) || isSubstantiveFollowupSignal_(text)) {
    return false;
  }
  const t = normalizeWhitespace_(String(text || "").toLowerCase());
  if (!t) return false;

  const requestsAction = /\b(?:help|call|talk|meet|send|share|email|text|website|link|fee|cost|service|work with)\b/.test(t);
  if (requestsAction) return false;

  const sameTopicPatterns = [
    /\b(?:clone|cloned|copy|copied|duplicate|duplicated|carry|carried)\b.{0,60}\b(?:listing|over|forward|data|field|fields)\b/,
    /\b(?:listing|data|field|fields)\b.{0,60}\b(?:clone|cloned|copy|copied|duplicate|duplicated|carry|carried)\b/,
    /\b(?:typo|mistake|error|incorrect|wrong|syndicat|imported|carried over)\b/,
    /\b(?:it(?:'s| is)|this is|the listing is)\s+(?:a\s+)?(?:probate|estate sale|foreclosure)\b/,
    /\b(?:thanks|thank you)\b.{0,60}\b(?:attention|heads up|letting me know|bringing)\b/
  ];
  return isFinalCourtesyReply_(t) || isNotShortSaleSignal_(t) || sameTopicPatterns.some(function(pattern) { return pattern.test(t); });
}

function hasPreviouslyCoveredContext_(rowObj) {
  const parts = [
    rowObj && rowObj[HEADERS.response_status],
    rowObj && rowObj[HEADERS.conversation_summary]
  ];
  getHistoryArray_(rowObj && rowObj[HEADERS.history_json]).forEach(function(entry) {
    if (entry && entry.role === "agent") parts.push(entry.text || "");
  });
  const combined = normalizeWhitespace_(String(parts.filter(Boolean).join(" ")).toLowerCase());
  return /\b(?:already (?:have|has|working with|represented)|have (?:a |my |our )?(?:negotiator|processor|attorney|lawyer|team|someone|help)|handled|handling (?:it|this|the file)|covered)\b/.test(combined) ||
    /\balready represented\b|\balready handled\b/.test(combined);
}

function isRelationshipOnlyAfterExistingCoverageSignal_(text, rowObj) {
  const t = normalizeWhitespace_(String(text || "").toLowerCase());
  const currentCoverage = /\b(?:already (?:have|has|working with|represented)|have (?:a |my |our )?(?:negotiator|processor|attorney|lawyer|team|someone|help)|handled|handling (?:it|this|the file)|covered)\b/.test(t) ||
    /\b(?:currently|already)\s+have\s+(?:someone|somebody|a\s+person|a\s+company|a\s+team)\s+(?:helping|assisting|handling|working\s+on)\b/.test(t) ||
    /\b(?:i|we)(?:['\u2019]?m|\s+am|['\u2019]?re|\s+are)\s+(?:currently\s+)?working\s+with\s+(?:someone|somebody|a\s+person|a\s+company|a\s+team)\b/.test(t);
  if (!t || isSubstantiveFollowupSignal_(t) || (!hasPreviouslyCoveredContext_(rowObj) && !currentCoverage)) {
    return false;
  }
  const passiveRelationshipPatterns = [
    /\b(?:i|we)(?:['\u2019]?ll| will)\s+(?:keep|save|hold onto)\s+(?:your|ur|you)\s+(?:info|information|contact|number|details)\b/,
    /\b(?:keep|save|hold onto)\s+(?:your|ur|you)\s+(?:info|information|contact|number|details)\b/,
    /\bkeep\s+(?:me|us)\s+in\s+mind\b/,
    /\bfeel free to\s+(?:keep|save)\s+(?:my|our)\s+(?:info|information|contact|number|details)\b/
  ];
  return passiveRelationshipPatterns.some(function(pattern) { return pattern.test(t); });
}

function isFutureBuyerRecontactSignal_(text) {
  const t = normalizeWhitespace_(String(text || "").toLowerCase());
  if (!t || isImmediateCallSignal_(t) || isSchedulingSignal_(t)) return false;

  const recontact = /\b(?:let\s+(?:you|u)\s+know|reach\s+out(?:\s+to\s+(?:you|u))?|contact\s+(?:you|u)|get\s+back\s+to\s+(?:you|u)|circle\s+back(?:\s+with\s+(?:you|u))?)\b/.test(t);
  const futureBuyer = /\b(?:when|once|after|if)\b.{0,60}\b(?:get|got|have|find|found|secure|secured|receive|received)\b.{0,30}\b(?:a\s+)?buyer\b/.test(t);
  const presentRequest = /\b(?:call|talk|speak|chat|meet|email|send|text)\b.{0,40}\b(?:now|today|tomorrow|this\s+week|next\s+week|at\s+\d|after\s+\d|before\s+\d)\b/.test(t);
  const rejection = /\b(?:no\s+thanks?|not\s+interested|do\s+not|don['\u2019]?t|dont)\b/.test(t);

  return recontact && futureBuyer && !presentRequest && !rejection;
}

function buildFutureBuyerRecontactReply_() {
  return "Yes, absolutely. Once you have a buyer, reach out and I can help with the lender side and paperwork.";
}

function buildRelationshipOnlyCloseoutReply_(text) {
  const t = normalizeWhitespace_(String(text || "").toLowerCase());
  if (/\bkeep\s+(?:me|us)\s+in\s+mind\b/.test(t)) {
    return "Absolutely - thanks. I'll keep you in mind, too.";
  }
  return "Thanks, I appreciate it. Feel free to reach out if a short sale comes up.";
}

function isNotShortSaleVagueFutureSignal_(text) {
  const t = normalizeWhitespace_(String(text || "").toLowerCase());
  const notShortSale = [
    /\bnot (?:actually )?a short sale\b/,
    /\bisn[\u2019'’]?t (?:actually )?a short sale\b/,
    /\bwasn[\u2019'’]?t meant to be (?:a )?short sale\b/,
    /\bwas not meant to be (?:a )?short sale\b/
  ].some(function(pattern) { return pattern.test(t); });
  const vagueFuture = [
    /\bkeep (?:you|u) in mind\b/,
    /\bif i ever (?:get|have) (?:one|a short sale)\b/,
    /\bif (?:one|a short sale) comes up\b/,
    /\bmaybe in the future\b/
  ].some(function(pattern) { return pattern.test(t); });
  const substantiveNextStep = /\?|\b(?:call|talk|meet|send|share|email|website|link|business card|another short sale|other short sale)\b/.test(t);
  return notShortSale && vagueFuture && !substantiveNextStep;
}

function isUnderControlFutureHelpCloseoutSignal_(text) {
  const t = normalizeWhitespace_(String(text || "").toLowerCase());
  const currentMatterControlled = [
    /\b(?:i|we)\s+(?:have|got)\s+(?:everything|it|this|the\s+file|the\s+listing)\s+under\s+control\b/,
    /\b(?:everything|it|this|the\s+file|the\s+listing)\s+(?:is|['’]s)\s+under\s+control\b/
  ].some(function(pattern) { return pattern.test(t); });
  const futureOnly = [
    /\b(?:(?:i|we)\s+will|(?:i|we)['’]ll|will)\s+(?:reach\s+out|contact\s+you|get\s+back\s+to\s+you|let\s+you\s+know)\b.*\b(?:if|when)\b.*\b(?:need|want|could\s+use)\b/,
    /\b(?:(?:i|we)\s+will|(?:i|we)['’]ll|will)\s+(?:reach\s+out|contact\s+you|get\s+back\s+to\s+you|let\s+you\s+know)\b.*\b(?:later|in\s+the\s+future)\b/
  ].some(function(pattern) { return pattern.test(t); });
  const presentRequest = isSubstantiveFollowupSignal_(t) ||
    isImmediateCallSignal_(t) ||
    isSchedulingSignal_(t) ||
    /\b(?:call|talk|meet|send|share|email|text)\s+(?:me|us|you)\b.*\b(?:now|today|tomorrow|this\s+week|next\s+week|at|after|before)\b/.test(t);

  return currentMatterControlled && futureOnly && !presentRequest;
}

function isClientConsultationInterestSignal_(text) {
  const t = normalizeWhitespace_(String(text || "").toLowerCase());
  if (!t) return false;

  const clientDiscussion = [
    /\blet me (?:chat|talk|speak|check|discuss) with (?:my|our|the) client\b/,
    /\b(?:i|we)(?:['\u2019]ll| will) (?:chat|talk|speak|check|discuss) with (?:my|our|the) client\b/,
    /\b(?:run|bring) (?:this|it) (?:past|by) (?:my|our|the) client\b/
  ].some(function(pattern) { return pattern.test(t); });

  const presentInterest = [
    /\b(?:think|believe|feel)\b.{0,80}\b(?:best|better|helpful)\b.{0,80}\b(?:someone|somebody|you|yoni)\b.{0,80}\b(?:handle|help|assist)\b/,
    /\b(?:think|believe|feel)\b.{0,80}\b(?:someone|somebody|you|yoni)\b.{0,80}\b(?:should|could|can|needs? to|would)\b.{0,40}\b(?:handle|help|assist)\b/,
    /\b(?:need|want|would like|could use)\b.{0,40}\bhelp\b/
  ].some(function(pattern) { return pattern.test(t); });

  const followupIntent = [
    /\b(?:i|we)(?:['\u2019]ll| will) get back to you\b/,
    /\b(?:i|we)(?:['\u2019]ll| will) let you know\b/,
    /\b(?:i|we)(?:['\u2019]ll| will) follow up with you\b/,
    /\b(?:circle back|follow up) with you\b/
  ].some(function(pattern) { return pattern.test(t); });

  const explicitRejection = [
    /\bno thanks?\b/,
    /\bnot interested\b/,
    /\b(?:do not|don't|dont) (?:think (?:i|we) )?need (?:any )?(?:help|assistance)\b/,
    /\balready have\b.{0,40}\b(?:someone|somebody|help|negotiator|processor|attorney|title company)\b/
  ].some(function(pattern) { return pattern.test(t); });

  return clientDiscussion && (presentInterest || followupIntent) && !explicitRejection;
}

function buildClientConsultationInterestReply_() {
  return "That makes sense. I handle the lender paperwork, calls, follow-up, and negotiations through approval, with no fee to you or the seller. I'm happy to speak with either of you whenever you're ready.";
}

function isExistingCrispRelationshipSignal_(text) {
  const t = normalizeWhitespace_(String(text || "").toLowerCase());
  if (!t) return false;

  const directRelationshipPatterns = [
    /\b(?:i|we)(?:['\u2019]m| am|['\u2019]re| are)?\s+(?:already|currently)\s+(?:work|working|set\s*up|signed\s*up|registered)\s+with\s+(?:you|yoni|crisp(?: short sales?)?|kristina|lexi)\b/,
    /\b(?:i|we)(?:['\u2019]m| am|['\u2019]re| are)?\s+(?:already|currently)?\s*(?:work|working)\s+with\s+(?:kristina|lexi)(?:\s+and\s+(?:kristina|lexi))?\b/,
    /\b(?:i|we)\s+(?:already\s+)?(?:have|use)\s+(?:an?\s+)?(?:active|existing)?\s*crisp(?: short sales?)?\s+(?:portal|account)\b/,
    /\b(?:already|currently)\s+(?:an?\s+)?crisp(?: short sales?)?\s+(?:client|customer)\b/,
    /\b(?:already|currently)\s+(?:an?\s+)?(?:client|customer)\s+(?:of|with)\s+crisp(?: short sales?)?\b/,
    /\b(?:i|we)\s+(?:already|currently)?\s*(?:have|use)\s+(?:yoni|crisp(?: short sales?)?)\s+(?:handling|helping|assisting|working\s+on)\b/,
    /\b(?:included|copied|looped)\s+(?:you|yoni)\s+(?:in|on)\b/
  ];

  return directRelationshipPatterns.some(function(pattern) { return pattern.test(t); });
}

function isSelfHandlingOpportunitySignal_(text) {
  const t = normalizeWhitespace_(String(text || "").toLowerCase()).replace(/\bmy\s+self\b/g, "myself");
  const selfHandling = [
    /\bhandling (?:that part |it )?myself\b/,
    /\bhandle (?:that part |it )?myself\b/,
    /\bdoing it myself\b/,
    /\bdo it myself\b/,
    /\b(?:trying|attempting) to handle it\b/,
    /\b(?:i am|i['’]?m|we are|we['’]?re)?\s*(?:communicating|working|dealing|talking) (?:directly )?with (?:the )?(?:bank|lender)\b/
  ].some(function(pattern) { return pattern.test(t); });
  const clearRejection = [
    /\bno thanks?\b/,
    /\bnot interested\b/,
    /\b(?:don['’]?t|do not|dont) need\b/,
    /\b(?:thanks?|thank you) for (?:the|your) offer\b/,
    /\b(?:i am|i['’]?m|we are|we['’]?re) good\b/
  ].some(function(pattern) { return pattern.test(t); });
  const substantiveNextStep = isSubstantiveFollowupSignal_(t) ||
    isPaymentOrFeeQuestionSignal_(t) ||
    /\b(?:interested|open to|might need|may need|could use|want help|call me|talk tomorrow|set up a time)\b/.test(t);
  return selfHandling && !clearRejection && !substantiveNextStep && !isNotShortSaleSignal_(t);
}

function isListingPromotionRequestSignal_(text) {
  const t = normalizeWhitespace_(String(text || "").toLowerCase());
  if (!t) return false;

  const promotionAction = /\b(?:post|share|advertise|market|promote|circulate|send)\b/.test(t);
  const listingReference = /\b(?:listing|property|home)\b/.test(t) ||
    /\b(?:post|share|advertise|market|promote|circulate)\s+(?:it|this)\b/.test(t);
  const targetAudience = /\b(?:your|local)\s+(?:area|market|network|agents?|buyers?)\b/.test(t) ||
    /\b(?:in|to|with|around)\s+(?:your|the)\s+(?:area|market|network|agents?|buyers?)\b/.test(t);

  return promotionAction && listingReference && targetAudience;
}

function isLocalQuestionSignal_(text) {
  const t = normalizeWhitespace_(String(text || "").toLowerCase());
  const patterns = [
    /\bare\s+(?:you|u)\s+local\b/,
    /\bare\s+(?:you|u)\s+(?:located|based)\s+(?:in|near|around|out of)\b/,
    /\bwhere\s+are\s+(?:you|u)\s+(?:located|based)\b/,
    /\bwhere(?:'s| is)\s+(?:your|ur)\s+office\b/,
    /\bwhere\s+do\s+(?:you|u)\s+work\s+out\s+of\b/,
    /\blocal\s+to\b/,
    /\b(?:are|r)\s+you\s+in\s+(?:dfw|dallas(?:\s*[-/]\s*fort\s+worth)?|fort\s+worth)\b/
  ];
  return patterns.some(pattern => pattern.test(t));
}

function isExperienceTrackRecordQuestionSignal_(text) {
  const t = normalizeWhitespace_(String(text || "").toLowerCase());
  if (!t) return false;

  const unsupportedPerformanceQuestion = [
    /\b(?:success|approval|approved|close|closing|conversion)\s+rate\b/,
    /\bwhat\s+(?:percent|percentage)\b.{0,60}\b(?:success|approval|approved|close|closing|conversion|deals?|files?)\b/,
    /\bhow\s+often\b.*\b(?:approve|approved|approval|close|closing|success|successful)\b/,
    /\bhow\s+(?:long|fast)\b.*\b(?:approval|approved|approve|close|closing|process|take|takes|timeline)\b/,
    /\b(?:average|typical)\b.*\b(?:time|timeline|approval|close|closing|days|weeks|months)\b/
  ].some(function(pattern) { return pattern.test(t); });
  if (unsupportedPerformanceQuestion) return false;

  const patterns = [
    /\bhow\s+long\b.*\b(?:handled|handling|doing|done|worked|working|been)\b.*\bshort sales?\b/,
    /\bhow\s+long\b.*\bshort sales?\b/,
    /\bhow\s+many\s+years\b.*\bshort sales?\b/,
    /\bhow\s+much\s+experience\b.*\bshort sales?\b/,
    /\bwhat(?:'s| is)\s+(?:your|ur)\s+track record\b/,
    /\b(?:your|ur)\s+track record\b/,
    /\bhow\s+many\b.*\b(?:short sales?|deals?|files?|transactions?)\b.*\b(?:handled|done|closed|completed)\b/
  ];
  return patterns.some(function(pattern) { return pattern.test(t); });
}

function buildExperienceTrackRecordReply_() {
  return "I have been doing this over 15 years and this is all that I do - help agents and homeowners with the short sale process. So I have a lot of experience and am confident I can get your deal closed.";
}

function buildExperienceReplyForQuestion_(text) {
  return buildExperienceTrackRecordReply_();
}

function buildShortSaleTimelineReply_() {
  return "A complete short-sale package and offer often takes about 60-90 days for a lender decision, though timing varies by lender and lien complexity.";
}

function isEquatorPortalSignal_(text) {
  return /\bequator\b/.test(normalizeWhitespace_(String(text || "").toLowerCase()));
}

function buildEquatorPortalReply_() {
  return "I'm very familiar with Equator and can handle all of the tasks and communication in the system to take that work off your hands.";
}

function isShortSaleTimelineQuestionSignal_(text) {
  const t = normalizeWhitespace_(String(text || "").toLowerCase());
  if (!t) return false;
  const patterns = [
    /\b(?:timeline|timeframe|turnaround time)\b/,
    /\bhow\s+(?:long|fast|soon|much\s+time)\b.{0,90}\b(?:short sale|process|bank|lender|foreclosure|approval|approved|approve|decision|review|close|closing|take|takes)\b/,
    /\b(?:average|typical|minimum|maximum)\b.{0,90}\b(?:time|timeline|days|weeks|months|approval|decision|review|close|closing|process)\b/,
    /\bhow\s+(?:long|soon)\b.{0,90}\b(?:stop|postpone|delay)\b.{0,40}\bforeclosure\b/,
    /\b(?:time|days|weeks|months)\b.{0,50}\b(?:until|before|to|get)\b.{0,50}\b(?:lender|approval|approved|decision|review|close|closing)\b/,
    /\bhow\s+many\s+(?:days|weeks|months)\b.{0,90}\b(?:short sale|process|bank|lender|approval|decision|review|close|closing|take|takes)\b/
  ];
  return patterns.some(function(pattern) { return pattern.test(t); });
}

function isUnsupportedPerformanceStatsQuestionSignal_(text) {
  const t = normalizeWhitespace_(String(text || "").toLowerCase());
  if (!t) return false;
  const patterns = [
    /\b(?:success|approval|approved|close|closing|conversion)\s+rate\b/,
    /\bwhat\s+(?:percent|percentage)\b.{0,60}\b(?:success|approval|approved|close|closing|conversion|deals?|files?)\b/,
    /\b(?:your|the)\s+(?:stats?|statistics|numbers)\b/,
    /\bhow\s+often\b.{0,80}\b(?:approve|approved|approval|close|closing|success|successful)\b/
  ];
  return patterns.some(function(pattern) { return pattern.test(t); });
}

function isCurrentTextingNumberQuestionSignal_(text) {
  const t = normalizeWhitespace_(String(text || "").toLowerCase());
  const patterns = [
    /\bis\s+this\s+(?:the\s+)?(?:best|good|right)\s+number\s+to\s+(?:reach|call|text)\s+(?:you|u)\b/,
    /\bis\s+this\s+(?:your|ur)\s+(?:(?:best|direct|cell|mobile)\s+)?number\b/,
    /\bcan\s+i\s+(?:reach|call|text)\s+(?:you|u)\s+(?:at|on)\s+this\s+number\b/,
    /\bcan\s+i\s+(?:(?:call|text)|call\s+or\s+text)\s+(?:you|u)\s+here\b/,
    /\b(?:reach|call|text)\s+(?:you|u)\s+(?:at|on)\s+this\s+number\b/
  ];
  return patterns.some(function(pattern) { return pattern.test(t); });
}

function isYoniNameAndNumberRequestSignal_(text) {
  const t = normalizeWhitespace_(String(text || "").toLowerCase());
  const patterns = [
    /\b(?:can|could|would|will)\s+(?:you|u)\s+(?:please\s+)?(?:send|give|text|share)\s+(?:me|us)\s+(?:your|ur)\s+name\b.*\b(?:phone|cell|mobile|number)\b/,
    /\b(?:send|give|text|share)\s+(?:me|us)\s+(?:your|ur)\s+name\b.*\b(?:phone|cell|mobile|number)\b/,
    /\b(?:can|could|may)\s+i\s+(?:get|have)\s+(?:your|ur)\s+name\b.*\b(?:phone|cell|mobile|number)\b/,
    /\bwhat(?:'s| is)\s+(?:your|ur)\s+name\b.*\b(?:phone|cell|mobile|number)\b/,
    /\b(?:your|ur)\s+name\s+and\s+(?:phone\s+)?number\s*\??$/
  ];
  return patterns.some(function(pattern) { return pattern.test(t); });
}

function buildYoniNameAndNumberReply_() {
  return "Yoni Kutler - 404-300-9526. You can call or text anytime.";
}

function lastOutboundWasYoniNameAndNumberReply_(rowObj) {
  return normalizeWhitespace_(String(rowObj && rowObj[HEADERS.last_outbound_text] || "")) ===
    buildYoniNameAndNumberReply_();
}

function buildLocalQuestionReply_(rowObj) {
  return "I'm based in Atlanta and work nationwide. The lender-side short sale work is handled remotely.";
}

function isContactCardRequestSignal_(text) {
  const t = normalizeWhitespace_(String(text || "").toLowerCase());
  const patterns = [
    /\b(?:send|share|text|email)\b.*\b(?:business\s+card|contact\s+card|vcard)\b/,
    /\b(?:business\s+card|contact\s+card|vcard)\b/,
  ];
  return patterns.some(pattern => pattern.test(t));
}

function isPlainContactInfoRequestSignal_(text) {
  const t = normalizeWhitespace_(String(text || "").toLowerCase());
  if (!t || isContactCardRequestSignal_(t)) return false;
  if (isCourtesyInformationAcknowledgmentSignal_(t)) {
    return false;
  }
  return /\b(?:send|share|give|text)\b.{0,40}\b(?:me|us)\b.{0,20}\b(?:your|ur)\s+(?:contact\s+)?(?:info|information|details|phone|number|email)\b/.test(t) ||
    /\b(?:send|share|give|text)\b.{0,20}\b(?:your|ur)\s+(?:contact\s+)?(?:info|information|details|phone|number|email)\b/.test(t) ||
    /\b(?:can|could|may)\s+i\s+(?:get|have)\b.{0,20}\b(?:your|ur)\s+(?:contact\s+)?(?:info|information|details|phone|number|email)\b/.test(t) ||
    /\bhow\s+(?:can|do)\s+i\s+(?:reach|contact)\s+(?:you|u)\b/.test(t) ||
    /\b(?:your|ur)\s+(?:contact\s+)?(?:info|information|details)\s*\?\s*$/.test(t);
}

function isCourtesyInformationAcknowledgmentSignal_(text) {
  const t = normalizeWhitespace_(String(text || "").toLowerCase());
  return /^(?:thank(?:s|\s+you)?|i\s+appreciate\s+you)\s+(?:for\s+)?(?:your|the)\s+(?:contact\s+)?(?:info|information|details)[.!]*$/.test(t);
}
function isSelfHandlingFutureHelpSignal_(text) {
  const t = normalizeWhitespace_(String(text || "").toLowerCase()).replace(/\bmy\s+self\b/g, "myself");
  return /\b(?:i|we)\s+handle(?:s)?\s+(?:it|this|that|the\s+file|the\s+listing)\s+myself(?:\s+usually)?\b/.test(t) ||
    /\bhandle(?:s)?\s+(?:it|this|that)\s+myself(?:\s+usually)?\b/.test(t);
}

function isCredentialQuestionSignal_(text) {
  const t = normalizeWhitespace_(String(text || "").toLowerCase());
  const patterns = [
    /\b(?:are|r)\s+(?:you|u)\s+(?:an?\s+)?(?:attorney|lawyer)\b/,
    /\b(?:are|r)\s+(?:you|u)\s+licensed\s+(?:as\s+)?(?:an?\s+)?(?:attorney|lawyer)\b/,
    /\bdo\s+(?:you|u)\s+(?:give|provide|offer)\s+legal\s+advice\b/,
    /\b(?:is|are)\s+crisp\s+(?:an?\s+)?(?:law\s+firm|attorney|lawyer)\b/
  ];
  return patterns.some(function(pattern) { return pattern.test(t); });
}

function isComplianceOrLicensingQuestionSignal_(text) {
  const t = normalizeWhitespace_(String(text || "").toLowerCase());
  if (!t || isCompanyIdentityQuestionSignal_(t) || /\b(?:attorney|lawyer|legal advice|law firm)\b/.test(t)) {
    return false;
  }
  const patterns = [
    /\bdebt adjust(?:er|or|ment|ing)\b/,
    /\bbanking commission\b/,
    /\b(?:statute|statutory|regulat(?:ion|ory)|compliance)\b/,
    /\b(?:are|r) (?:you|u) licensed\b/,
    /\bdo (?:you|u) (?:have|hold|need) (?:an? )?licen[cs]e\b/,
    /\blicensed (?:in|to operate in|to work in)\b/,
    /\bwhat (?:licen[cs]e|permit|authorization)\b/
  ];
  return patterns.some(function(pattern) { return pattern.test(t); });
}

function isTitleCompanyRoleConfusionSignal_(text) {
  const t = normalizeWhitespace_(String(text || "").toLowerCase());
  if (t.indexOf("title company") === -1 || /\b(?:no thanks?|not interested|do not|don't|dont)\b/.test(t)) {
    return false;
  }
  return /\b(?:already have|have|use|using|work with|working with|working for)\b.{0,50}\btitle company\b/.test(t) ||
    /\btitle company\b.{0,50}\b(?:if that(?:'s| is) what you mean|is that what you mean|do you mean)\b/.test(t);
}

function buildTitleCompanyRoleClarificationReply_() {
  return "Thanks for clarifying. Crisp isn't a title company. I handle the lender-side short-sale paperwork, calls, and negotiations through approval, while your title company handles title and closing. Would that kind of help be useful on this file?";
}

function buildCredentialQuestionReply_() {
  return "No, I'm not an attorney and I don't provide legal advice. I handle the lender-side short-sale process and negotiations needed for approval.";
}

function isNegotiatorRoleQuestionSignal_(text) {
  const t = normalizeWhitespace_(String(text || "").toLowerCase());
  const patterns = [
    /^(?:(?:so|basically|essentially|like)\s+)?(?:(?:you(?:'re| are))\s+)?(?:a\s+)?(?:short\s+sale\s+)?negotiator\s*\??$/,
    /\b(?:are|r)\s+(?:you|u)\s+(?:a\s+)?(?:short\s+sale\s+)?negotiator\b/,
    /\b(?:does\s+that\s+mean|so|then)\s+(?:you(?:'re| are)|you\s+are)\s+(?:a\s+)?(?:short\s+sale\s+)?negotiator\b/
  ];
  return patterns.some(function(pattern) { return pattern.test(t); });
}

function buildNegotiatorRoleQuestionReply_() {
  return "Yes, essentially. I handle the short-sale process and lender negotiations through approval; if you want, I can answer any questions on a quick call.";
}

function isAlreadyHandledSignal_(text) {
  if (isNegotiatorRoleQuestionSignal_(text)) return false;
  const t = normalizeWhitespace_(String(text || "").toLowerCase());
  const patterns = [
    /\b(?:my|our|the)\s+(?:lawyer|attorney|negotiator|short sale negotiator)\b/,
    /\b(?:i|we)\s+(?:have|use|retained|hired|work with|are working with|already have)\s+(?:an?\s+)?(?:lawyer|attorney|negotiator|short sale negotiator)\b/,
    /\bwe have a negotiator\b/,
    /\bi have a negotiator\b/,
    /\bsigned agreement\b/,
    /\balready signed\b/,
    /\balready under contract\b/,
    /\balready have representation\b/,
    /\balready working with\b/,
    /\balready have someone\b/,
    /\bi already have help\b/,
    /\bwe already have help\b/,
    /\bi have help\b/,
    /\bwe have help\b/,
    /\bi have someone already\b/,
    /\bwe have someone already\b/,
    /\b(?:i|we)\s+(?:currently|already)\s+have\s+(?:someone|somebody|a\s+person|a\s+company|a\s+team)\s+(?:helping|assisting|handling|working\s+on)\b/,
    /\balready have a processor\b/,
    /\b(?:i|we)\s+have\s+(?:a|my|our)?\s*team\s+(?:handling|working on|taking care of)\b/,
    /\b(?:my|our|the)\s+team\s+(?:is\s+)?(?:handling|working on|taking care of)\b/,
    /\bteam\s+(?:is\s+)?(?:already\s+)?(?:handling|working on|taking care of)\s+it\b/,
    /\b(?:my|our)\s+team\s+(?:has|have)\s+it\s+handled\b/,
    /\bwe are dealing directly with the bank\b/,
    /\bwe're dealing directly with the bank\b/,
    /\bworking with the bank already\b/,
    /\b(?:i|we)\s+(?:work|deal)\s+with\s+(?!you\b|yoni\b|crisp\b).+\s+already\b/,
    /\b(?:i|we)\s+(?:am|are|\x27m|\x27re)?\s*(?:already\s+)?(?:working|dealing)\s+with\s+(?!you\b|yoni\b|crisp\b)(?:someone|somebody|a\s+person|a\s+company|a\s+team|[a-z][a-z0-9&.\x27-]*(?:\s+[a-z][a-z0-9&.\x27-]*){0,3})\b/,
    /\b(?:i|we)\s+(?:already\s+)?(?:use|have)\s+(?:someone|somebody|a\s+person|a\s+company|a\s+team|help|a\s+processor|a\s+negotiator|a\s+specialist)\b/,
    /^no[,.]?\s+.+\b(?:with|through|using)\b.+$/
  ];

  return patterns.some(pattern => pattern.test(t));
}

function isClearNoSignal_(text) {
  const t = normalizeWhitespace_(String(text || "").toLowerCase());

  if (isSelfHandlingOpportunitySignal_(t)) {
    return false;
  }

  const patterns = [
    /\bwe are good\b/,
    /\bwe're good\b/,
    /\bwe are good for now\b/,
    /\bwe're good for now\b/,
    /\bno\s*,?\s*thank(?:s|\s+you)?\b/,
    /\bnot interested\b/,
    /\b(?:i|we)\s+(?:do not|don'?t)\s+need\s+(?:any\s+)?(?:help|assistance)\b/,
    /\bno need for (?:any )?(?:help|assistance)\b/,
    /\bwe have it covered\b/,
    /\bwe got it covered\b/,
    /\bi'm good\b/,
    /\bim good\b/,
    /\bi'm fine\b/,
    /\bim fine\b/,
    /\bwe are fine\b/,
    /\bwe're fine\b/,
    /\bthank you for reaching out\b/,
    /\bthanks for reaching out\b/,
    /\bappreciate you reaching out\b/,
    /\bwe are all set\b/,
    /\bwe're all set\b/,
    /\ball set\b/,
    /\b(?:i|we)\s+(?:think\s+)?(?:(?:i|we)\s+)?(?:have|got)\s+(?:(?:it|this|everything|the\s+file|the\s+listing|an?)\s+)?under\s+control\b/,
    /\bthank you.*we.*good\b/,
    /\bthank you.*we're good\b/,
    /\bthank you.*i'm fine\b/,
    /\bthank you.*im fine\b/
  ];

  return patterns.some(pattern => pattern.test(t));
}

function isUnmistakableTerminalRejectionSignal_(text) {
  const t = normalizeWhitespace_(String(text || "").toLowerCase());
  if (!t || /\?/.test(t) || isSchedulingSignal_(t) || isPhoneCallInterestSignal_(t) ||
      isPresentServiceInterestSignal_(t) || isDirectHelpRequestSignal_(t)) {
    return false;
  }

  const patterns = [
    /\bno\s*,?\s*thank(?:s|\s+you)?\b/,
    /\bnot interested\b/,
    /\b(?:i|we)\s+(?:do not|don['’]?t|dont)\s+need\s+(?:any\s+)?(?:help|assistance)\b/,
    /\b(?:we(?:'|’)re|we are)\s+all set\b/,
    /\bwe\s+(?:have|got)\s+it\s+covered\b/,
    /\b(?:i\s+just\s+)?(?:do not|don['’]?t|dont)\s+think\s+(?:a|the|any)?\s*buyer\s+(?:will|would)\s+(?:go for|accept|agree to|pay)\b/,
    /\b(?:they|a buyer|the buyer|buyers?)\s+(?:aren['’]?t|are not|won['’]?t|will not|isn['’]?t|is not)\s+(?:going to\s+)?(?:pay|go for|accept|agree to)\b/,
    /\b(?:this|that)\s+(?:won['’]?t|will not|isn['’]?t|is not)\s+(?:going to\s+)?work\b/
  ];
  return patterns.some(function(pattern) { return pattern.test(t); });
}

function isPunctuationCorrectionFragment_(text) {
  const t = normalizeWhitespace_(String(text || "").toLowerCase());
  return /^[!.,;:\s]*(?:not|no)[!?.;:\s]*$/.test(t);
}

function getStandardNoCloseoutReply_() {
  return "Ok, no problem. If anything ever changes in the future and you're looking for some additional help with these files, please just keep me in mind. Thanks!";
}

function isNotShortSaleSignal_(text) {
  const t = normalizeWhitespace_(String(text || "").toLowerCase());

  const patterns = [
    /\baccidentally put as a short sale\b/,
    /\baccidentally listed as a short sale\b/,
    /\bhas been changed\b/,
    /\bit was changed\b/,
    /\bnot a short sale\b/,
    /\bno longer a short sale\b/,
    /\bwasn'?t meant to be (?:a )?short sale\b/,
    /\bwas not meant to be (?:a )?short sale\b/,
    /\bnot meant to be (?:a )?short sale\b/
  ];

  return patterns.some(pattern => pattern.test(t));
}

function isShortSaleSourceQuestion_(text) {
  const t = normalizeWhitespace_(String(text || "").toLowerCase());

  const anaphoricPhrases = [
    "what would make you think it was",
    "what made you think it was",
    "why would you think it was",
    "why did you think it was",
    "why do you think it was"
  ];

  if (anaphoricPhrases.some(phrase => t.indexOf(phrase) !== -1)) return true;
  if (t.indexOf("short sale") === -1) return false;

  const directPhrases = [
    "why did you think",
    "why do you think",
    "why would you think",
    "why did you say",
    "why do you say",
    "why was it",
    "why is it",
    "where did you see",
    "where do you see",
    "where was it",
    "what made you think",
    "what makes you think"
  ];

  if (directPhrases.some(phrase => t.indexOf(phrase) !== -1)) return true;

  return false;
}

function isImmediateCallSignal_(text) {
  const t = normalizeWhitespace_(String(text || "").toLowerCase());

  const patterns = [
    /^yes$/,
    /^now$/,
    /^available now$/,
    /^i'?m available now$/,
    /^im available now$/,
    /^call me$/,
    /^call me now$/,
    /^give me a call$/,
    /^give me a call now$/,
    /^you can call me$/,
    /^yes call me$/,
    /^ok call me$/,
    /^okay call me$/,
    /^now works$/,
    /^i'?m free$/,
    /^im free$/,
    /^i'?m free now$/,
    /^im free now$/,
    /^yes i'?m available$/,
    /^yes im available$/,
    /^yes i'?m available now$/,
    /^yes im available now$/
  ];

  return patterns.some(pattern => pattern.test(t));
}

function extractScheduledCallbackReference_(text) {
  const raw = normalizeWhitespace_(String(text || ""));
  if (!raw) return "";
  const searchable = raw.replace(/\bnot\s+tomorrow\b/ig, " ");

  const match = searchable.match(
    /\bafter\s+(?:the\s+)?weekend\b|\btomorrow\b|\bnext\s+week\b|\b(?:first|second|third|fourth|last)\s+week\s+(?:of|in)\s+(?:january|february|march|april|may|june|july|august|september|october|november|december)\b|\b(?:(?:this|next|coming)\s+)?(?:monday|tuesday|wednesday|thursday|friday|saturday|sunday)\b|\b(?:january|february|march|april|may|june|july|august|september|october|november|december)\s+\d{1,2}(?:st|nd|rd|th)?(?:,\s*\d{4})?\b|\b\d{1,2}[\/-]\d{1,2}(?:[\/-]\d{2,4})?\b/i
  );
  if (!match) return "";
  let reference = match[0];
  const escaped = reference.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const qualified = searchable.match(new RegExp("(?:\\b(?:morning|afternoon|evening)\\b\\s+(?:on\\s+)?)?" + escaped + "(?:\\s+\\b(?:morning|afternoon|evening)\\b)?", "i"));
  if (qualified) {
    reference = qualified[0];
    const before = reference.match(/\b(morning|afternoon|evening)\b\s+(?:on\s+)?(.+)$/i);
    if (before) {
      reference = before[2] + " " + before[1];
    }
  }

  return reference.replace(/\b[a-z]/g, function(letter) {
    return letter.toUpperCase();
  });
}

function isUnavailableUntilCallbackReferenceSignal_(text) {
  const t = normalizeWhitespace_(String(text || "").toLowerCase());
  if (!t || !extractScheduledCallbackReference_(t) || isExplicitDayOrDateCallbackSignal_(t)) return false;
  return /\b(?:i(?:['’]?m|\s+am)|we(?:['’]?re|\s+are)|i(?:['’]?ll|\s+will)\s+be|we(?:['’]?ll|\s+will)\s+be)\s+(?:out(?:\s+of\s+(?:the\s+)?office)?|away|unavailable)\s+until\b/.test(t) ||
    /\b(?:i|we)\s+(?:won['’]?t|will\s+not)\s+be\s+(?:back|available)\s+until\b/.test(t);
}

function isSelfInitiatedDeferredContactSignal_(text) {
  const t = normalizeWhitespace_(String(text || "").toLowerCase());
  if (!t) return false;
  const currentlyUnavailable = /\b(?:i(?:['’]?m|\s+am)\s+)?(?:working|busy)(?:\s+right\s+now)?\b/.test(t) ||
    /\b(?:at|in)\s+(?:my|the)\s+(?:other\s+)?job\b/.test(t) ||
    /\b(?:on\s+the\s+clock|at\s+work)\b/.test(t);
  const selfInitiatedFollowup = /\b(?:i|we)(?:['’]?ll|\s+will)?\s+(?:let\s+you\s+know|message\s+you|text\s+you|reach\s+out(?:\s+to\s+you)?|contact\s+you|get\s+back\s+to\s+you)\b/.test(t);
  const explicitInboundCallback = /\b(?:call|text|contact)\s+me\b/.test(t) ||
    /\b(?:can|could|would|will)\s+you\s+(?:call|text|contact)\b/.test(t);
  return currentlyUnavailable && selfInitiatedFollowup && !explicitInboundCallback;
}

function normalizeCallbackTime_(value) {
  return normalizeWhitespace_(String(value || "")).toLowerCase();
}

function isCallbackUpdateTiming_(text) {
  const t = normalizeWhitespace_(String(text || "").toLowerCase());
  if (!t || !extractScheduledCallbackReference_(t)) return false;
  return /\b(?:would|will|works?|work)\s+(?:be\s+)?(?:better|best|good|fine|ok|okay)\b/.test(t) ||
    /\b(?:push|move|reschedule|switch|change)\b.{0,40}\b(?:to|into|for|on)\b/.test(t) ||
    /\b(?:focus|aim|target)\b.{0,40}\b(?:for|on)\b/.test(t) ||
    /\b(?:better|best|good|fine|ok|okay)\b.{0,20}\b(?:on|for)\b/.test(t);
}

function isPostHandoffCallbackUpdate_(rowObj, inboundText) {
  if (String(rowObj && rowObj[HEADERS.human_override] || "").toUpperCase() !== "TRUE") return false;
  if (!isSchedulingSignal_(inboundText) && !isCallbackUpdateTiming_(inboundText)) return false;
  const aiState = String(rowObj && rowObj[HEADERS.ai_state] || "").toLowerCase();
  const handoffFlag = String(rowObj && rowObj[HEADERS.handoff_flag] || "").toUpperCase() === "TRUE";
  const bookingStatus = String(rowObj && rowObj[HEADERS.call_booking_status] || "").toLowerCase();
  return aiState === "handoff" || handoffFlag || bookingStatus === "scheduled_callback" || bookingStatus === "interested_no_call";
}

function isExplicitDayOrDateCallbackSignal_(text) {
  const t = normalizeWhitespace_(String(text || "").toLowerCase());
  if (!t || !extractScheduledCallbackReference_(t)) return false;

  if (/\b(?:do not|don['’]?t|dont)\s+(?:call|text|contact|reach out|follow up|get in touch|connect)\b/.test(t) ||
      /\bnot\s+(?:(?:this|next|coming)\s+)?(?:monday|tuesday|wednesday|thursday|friday|saturday|sunday)\b/.test(t) ||
      /\b(?:talk|speak|chat)\s+(?:with|to)\s+(?:(?:the|a|my|our)\s+)?(?:lender|bank|client|seller|buyer)\b/.test(t)) {
    return false;
  }

  const patterns = [
    /\b(?:call|text|contact)\s+me\b/,
    /\b(?:reach out|follow up|get in touch|connect)\s+(?:with\s+|to\s+)?me\b/,
    /\b(?:feel free to|please|can you|could you|would you|you can)\s+(?:call|text|contact|reach out|follow up|get in touch|connect)\b/,
    /\b(?:i['’]?ll|i will|we['’]?ll|we will)\s+(?:call|text|contact)\s+you\b/,
    /\b(?:i['’]?ll|i will|we['’]?ll|we will)\s+(?:reach out|follow up|get in touch|connect)\s+(?:with\s+|to\s+)?you\b/,
    /\b(?:let['’]?s|lets|can\s+we|could\s+we|would\s+you|can\s+you)\s+(?:set\s+up\s+(?:a\s+)?time\s+to\s+)?(?:talk|speak|chat)\b/,
    /\b(?:i|we)\s+can\s+(?:talk|speak|chat)\b/,
    /\bset\s+up\s+(?:a\s+)?time\s+(?:for\s+us\s+)?to\s+(?:talk|speak|chat)\b/
  ];

  return patterns.some(function(pattern) { return pattern.test(t); });
}

function isSchedulingSignal_(text) {
  const t = normalizeWhitespace_(String(text || "").toLowerCase());

  if (isExplicitDayOrDateCallbackSignal_(t)) {
    return true;
  }

  if (/\bnot tomorrow\b/.test(t)) {
    return false;
  }

  const patterns = [
    /\baround\s+\d{1,2}(?::\d{2})?\s*(?:a|am|p|pm)?\b/,
    /\bafter\s+\d{1,2}(?::\d{2})?\s*(?:a|am|p|pm)?\b/,
    /\btill\s+\d{1,2}(?::\d{2})?\s*(?:a|am|p|pm)?\b/,
    /\buntil\s+\d{1,2}(?::\d{2})?\s*(?:a|am|p|pm)?\b/,
    /\blater this afternoon\b/,
    /\bthis afternoon\b/,
    /\btomorrow morning\b/,
    /\btomorrow afternoon\b/,
    /\btomorrow at\b/,
    /\btomorrow around\b/,
    /\bavailable around\b/,
    /\bavailable at\b/,
    /\bcall me after\b/,
    /\byou can reach out around\b/,
    /\byou can reach out after\b/,
    /\bopen house today till\b/,
    /\b\d{1,2}:\d{2}\b/,
    /\b\d{1,2}\s?(?:a|am|p|pm)\b/
  ];

  return patterns.some(pattern => pattern.test(t));
}

function buildSchedulingReply_(inboundText) {
  const t = normalizeWhitespace_(String(inboundText || ""));
  const lower = t.toLowerCase();
  const timePhrase = extractSchedulingTimePhrase_(t);

  if (/open house today till 2/i.test(t)) {
    return { reply_text: "No problem. I can give you a call around 2:30 if that works." };
  }

  if (/later this afternoon/i.test(t) || /this afternoon/i.test(t)) {
    return { reply_text: "No problem. I can give you a call this afternoon. What time is best for you?" };
  }

  if (/after\s+\d/i.test(lower)) {
    const afterPhrase = extractRelativeTimeWindow_(t, "after");
    return {
      reply_text: "Ok, I can call " + (afterPhrase || "after that") + ". Is there a specific time that's best?"
    };
  }

  if (/around\s+\d/i.test(lower) && timePhrase) {
    return { reply_text: "Perfect. I can call around " + timePhrase + "." };
  }

  if (/tomorrow/i.test(lower) && timePhrase) {
    return { reply_text: "Sounds good. I can call tomorrow around " + timePhrase + "." };
  }

  if (timePhrase) {
    return { reply_text: "Sounds good. I can call around " + timePhrase + "." };
  }

  return { reply_text: "Sounds good. What time works best for you?" };
}

function normalizeTimePhrase_(value) {
  return String(value || "")
    .toLowerCase()
    .replace(/\s+/g, " ")
    .replace(/(\d)(am|pm)\b/i, "$1 $2")
    .replace(/(\d)(a)\b/i, "$1 am")
    .replace(/(\d)(p)\b/i, "$1 pm")
    .trim();
}

function extractSchedulingTimePhrase_(text) {
  const match = String(text || "").match(/\b\d{1,2}(?::\d{2})?\s?(?:a|am|p|pm)?\b/i);
  return match ? normalizeTimePhrase_(match[0]) : "";
}

function extractRelativeTimeWindow_(text, keyword) {
  const escapedKeyword = escapeRegex_(keyword);
  const pattern = new RegExp("\\b" + escapedKeyword + "\\s+\\d{1,2}(?::\\d{2})?\\s*(?:a|am|p|pm)?\\b", "i");
  const match = String(text || "").match(pattern);
  return match ? normalizeTimePhrase_(match[0]) : "";
}

function getAiDecision_(rowInfo, inboundText) {
  const props = PropertiesService.getScriptProperties();
  const apiKey = props.getProperty("OPENAI_API_KEY");
  if (!apiKey) throw new Error("Missing OPENAI_API_KEY");

  const rowObj = rowInfo.rowObj;
  const agentFirstName = getCanonicalFirstName_(rowObj);
  const history = getHistoryArray_(rowObj[HEADERS.history_json]).slice(-8);

  const systemPrompt = buildSystemPrompt_(rowObj);
  const userPayload = {
    agent_context: {
      agent_first_name: agentFirstName,
      agent_name: rowObj[HEADERS.agent_name] || "",
      last_name: rowObj[HEADERS.last_name] || "",
      phone: rowObj[HEADERS.phone] || "",
      listing_address: rowObj[HEADERS.listing_address] || "",
      city: rowObj[HEADERS.city] || "",
      state: rowObj[HEADERS.state] || "",
      response_status: rowObj[HEADERS.response_status] || "",
      mailshake_status: rowObj[HEADERS.mailshake_status] || "N"
    },
    conversation_history: history,
    latest_inbound_message: inboundText
  };

  const requestBody = {
    model: "gpt-5-mini",
    input: [
      { role: "system", content: [{ type: "input_text", text: systemPrompt }] },
      { role: "user", content: [{ type: "input_text", text: JSON.stringify(userPayload) }] }
    ],
    text: {
      format: {
        type: "json_schema",
        name: "sms_agent_response",
        strict: true,
        schema: {
          type: "object",
          additionalProperties: false,
          properties: {
            reply_text: { type: "string" },
            lead_status: { type: "string", enum: ["R", "Y", "G", "N", "O"] },
            conversation_done: { type: "boolean" },
            handoff_needed: { type: "boolean" },
            needs_review: { type: "boolean" },
            block_reply: { type: "boolean" },
            reason: { type: "string" }
          },
          required: ["reply_text", "lead_status", "conversation_done", "handoff_needed", "needs_review", "block_reply", "reason"]
        }
      }
    }
  };

  const resp = UrlFetchApp.fetch("https://api.openai.com/v1/responses", {
    method: "post",
    contentType: "application/json",
    headers: {
      Authorization: "Bearer " + apiKey
    },
    payload: JSON.stringify(requestBody),
    muteHttpExceptions: true
  });

  const code = resp.getResponseCode();
  const raw = resp.getContentText();

  if (code < 200 || code >= 300) {
    throw new Error("OpenAI API error " + code + ": " + raw);
  }

  const parsed = JSON.parse(raw);
  const text = extractOutputText_(parsed);
  const decision = JSON.parse(text);

  if (typeof decision.reply_text !== "string") {
    throw new Error("AI returned invalid reply_text");
  }

  return applyReplySanitizers_(decision, rowObj);
}

function normalizeAiDecision_(decision, existingStatus) {
  const normalized = Object.assign({
    reply_text: "",
    lead_status: "Y",
    conversation_done: false,
    handoff_needed: false,
    needs_review: false,
    block_reply: false,
    reason: ""
  }, decision || {});

  normalized.lead_status = coerceRespondedLeadStatus_(normalized.lead_status, existingStatus);
  normalized.lead_status = coerceSmsTextLeadStatus_(normalized.lead_status);
  normalized.conversation_done = !!normalized.conversation_done;
  normalized.alert_needed = !!normalized.alert_needed;
  normalized.handoff_needed = !!normalized.handoff_needed || normalized.lead_status === "G" ||
    !!normalized.needs_review || normalized.alert_needed;
  normalized.needs_review = !!normalized.needs_review;
  normalized.block_reply = !!normalized.block_reply;
  normalized.reply_text = typeof normalized.reply_text === "string" ? normalized.reply_text : "";
  normalized.reason = typeof normalized.reason === "string" ? normalized.reason : "";

  return normalized;
}

function coerceSmsTextLeadStatus_(candidateStatus) {
  const candidate = String(candidateStatus || "").toUpperCase();

  if (candidate === "G") {
    return "Y";
  }

  return candidate || "Y";
}

function isSelfHandlingValueReplyText_(text) {
  const t = normalizeWhitespace_(String(text || "").toLowerCase());
  return (t.indexOf("i can take the lender side off your plate") !== -1 ||
      t.indexOf("i can take lender side off your plate") !== -1) &&
    t.indexOf("there is no cost to you or the seller") !== -1 &&
    (t.indexOf("would a quick call about this file be helpful") !== -1 ||
      t.indexOf("would you be open to a quick call about this file") !== -1);
}

function buildSelfHandlingRepeatCloseDecision_() {
  return {
    matched: true,
    reply_text: getStandardNoCloseoutReply_(),
    lead_status: "R",
    conversation_done: true,
    handoff_needed: false,
    needs_review: false,
    block_reply: false,
    reason: "Agent repeated self-handling stance after one value response; closed without takeover"
  };
}

function buildStatePreservingRepeatSuppressionDecision_(rowObj) {
  const existingStatus = String(rowObj && rowObj[HEADERS.mailshake_status] || "").toUpperCase() || "Y";
  const existingAiState = String(rowObj && rowObj[HEADERS.ai_state] || "").toLowerCase();
  return {
    matched: true,
    reply_text: "",
    lead_status: existingStatus,
    conversation_done: existingAiState === "done",
    handoff_needed: false,
    needs_review: false,
    block_reply: true,
    preserve_existing_state: true,
    reason: "Potential repetitive bot loop detected; prior conversation state preserved"
  };
}

function applyRepeatGuard_(decision, rowObj, inboundText) {
  const guarded = Object.assign({}, decision || {});
  const lastOutbound = normalizeWhitespace_(String(rowObj && rowObj[HEADERS.last_outbound_text] || ""));
  const inbound = normalizeWhitespace_(String(inboundText || ""));

  if (!guarded.reply_text || guarded.block_reply) {
    return guarded;
  }

  if (isDeliveryFollowupSignal_(inbound) && lastOutboundWasMaterialPromise_(lastOutbound)) {
    return buildManualHandoffDecision_("Agent is asking for resend or alternate delivery after a prior send promise");
  }

  if (isMissedCallOrAlternateNumberSignal_(inbound) && lastOutboundWasCallPromise_(lastOutbound)) {
    return buildManualHandoffDecision_(
      "Agent mentioned a missed call or alternate callback number after a prior call promise",
      "MISSED CALL FOLLOW-UP"
    );
  }

  if (isSelfHandlingOpportunitySignal_(inbound) && isSelfHandlingValueReplyText_(lastOutbound)) {
    return buildSelfHandlingRepeatCloseDecision_();
  }

  if (isPotentialRepeatReply_(guarded.reply_text, lastOutbound)) {
    if (isSubstantiveFollowupSignal_(inbound)) {
      return buildManualHandoffDecision_(
        "Agent asked a new substantive question after a similar prior answer",
        "SUBSTANTIVE QUESTION FOLLOW-UP"
      );
    }
    return buildStatePreservingRepeatSuppressionDecision_(rowObj);
  }

  return guarded;
}

function buildManualHandoffDecision_(reason, handoffType) {
  return {
    matched: true,
    reply_text: "",
    lead_status: "Y",
    conversation_done: false,
    handoff_needed: true,
    needs_review: false,
    block_reply: true,
    handoff_type: handoffType || "POTENTIAL BOT LOOP",
    reason: reason || "Manual follow-up needed"
  };
}

function handleMaxRepliesHandoff_(sheet, row, rowObj, phoneRaw, inboundText) {
  sendHandoffEmail_({
    handoff_type: "MAX REPLIES REACHED",
    agent_name: rowObj[HEADERS.agent_name] || "",
    last_name: rowObj[HEADERS.last_name] || "",
    initial_text: rowObj[HEADERS.initial_text_sent] || "",
    phone: phoneRaw,
    email: rowObj[HEADERS.email] || "",
    listing_address: rowObj[HEADERS.listing_address] || "",
    city: rowObj[HEADERS.city] || "",
    state: rowObj[HEADERS.state] || "",
    zip: rowObj[HEADERS.zip] || "",
    last_message: inboundText,
    history: getHistoryArray_(rowObj[HEADERS.history_json])
  });

  updateRowFields_(sheet, row, {
    [HEADERS.response_status]: inboundText,
    [HEADERS.mailshake_status]: "Y",
    [HEADERS.conversation_summary]: "Max Replies Reached",
    [HEADERS.ai_state]: "handoff",
    [HEADERS.call_booking_status]: "interested_no_call",
    [HEADERS.handoff_flag]: "TRUE",
    [HEADERS.human_override]: "TRUE"
  });

  return {
    ok: true,
    should_reply: false,
    reply_text: "",
    lead_status: "Y",
    conversation_done: false,
    handoff_needed: true,
    needs_review: false,
    reason: "Max auto replies reached - handoff to Yoni"
  };
}

function applyReplySanitizers_(decision, rowObj) {
  const sanitized = Object.assign({}, decision || {});
  sanitized.reply_text = sanitizeReplySelfIntro_(sanitized.reply_text);
  sanitized.reply_text = sanitizeReplyNameUsage_(sanitized.reply_text, rowObj);
  sanitized.reply_text = sanitizeReplySignoff_(sanitized.reply_text);
  sanitized.reply_text = sanitizeReplyCallPromise_(sanitized.reply_text);
  sanitized.reply_text = sanitizeReplyPropertyReference_(sanitized.reply_text, rowObj);
  sanitized.reply_text = sanitizeReplyBuyerOffer_(sanitized.reply_text);
  sanitized.reply_text = sanitizeReplyPhoneOnlyCta_(sanitized.reply_text);
  sanitized.reply_text = sanitizeReplyFileCta_(sanitized.reply_text);
  return sanitized;
}

function isDeliveryFollowupSignal_(text) {
  const t = normalizeWhitespace_(String(text || "").toLowerCase());
  const patterns = [
    /\bdidn'?t get it\b/,
    /\bdid not get it\b/,
    /\bdidn'?t receive it\b/,
    /\bdid not receive it\b/,
    /\bdidn'?t get it by email\b/,
    /\bdidn'?t get the email\b/,
    /\bplease text it instead\b/,
    /\btext it instead\b/,
    /\bemail it instead\b/,
    /\bsend it instead\b/,
    /\bresend\b/
  ];

  return patterns.some(pattern => pattern.test(t));
}

function lastOutboundWasMaterialPromise_(text) {
  const t = normalizeWhitespace_(String(text || "").toLowerCase());
  const patterns = [
    /\bone[- ]page summary\b/,
    /\bi'?ll send\b/,
    /\bi will send\b/,
    /\bi'?ll text\b/,
    /\bi will text\b/,
    /\bi'?ll email\b/,
    /\bi will email\b/,
    /\bexpect it in a minute\b/,
    /\bsummary\b/
  ];

  return patterns.some(pattern => pattern.test(t));
}

function lastOutboundWasCallPromise_(text) {
  const t = normalizeWhitespace_(String(text || "").toLowerCase());
  const patterns = [
    /\bcalling you now\b/,
    /\bi'?ll call you now\b/,
    /\bi will call you now\b/,
    /\bi'?ll call now\b/,
    /\bi will call now\b/,
    /\bi'?ll call your cell now\b/,
    /\bi will call your cell now\b/,
    /\btalk in a sec\b/,
    /\bgive you a call shortly\b/,
    /\bcall shortly\b/,
    /\bok just a second\b/
  ];

  return patterns.some(pattern => pattern.test(t));
}

function isPotentialRepeatReply_(replyText, lastOutboundText) {
  const current = normalizeLoopGuardText_(replyText);
  const previous = normalizeLoopGuardText_(lastOutboundText);

  if (!current || !previous) {
    return false;
  }

  if (current === previous) {
    return true;
  }

  if (current.length >= 40 && (current.indexOf(previous) !== -1 || previous.indexOf(current) !== -1)) {
    return true;
  }

  const currentTokens = current.split(" ").filter(Boolean);
  const previousTokens = previous.split(" ").filter(Boolean);
  if (currentTokens.length < 6 || previousTokens.length < 6) {
    return false;
  }

  const previousSet = {};
  previousTokens.forEach(token => previousSet[token] = true);

  let overlap = 0;
  currentTokens.forEach(token => {
    if (previousSet[token]) overlap += 1;
  });

  const denominator = Math.max(currentTokens.length, previousTokens.length);
  return denominator > 0 && (overlap / denominator) >= 0.72;
}

function normalizeLoopGuardText_(text) {
  return String(text || "")
    .toLowerCase()
    .replace(/\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b/gi, " email ")
    .replace(/\b\d{10,}\b/g, " phone ")
    .replace(/\b\d{3}[-.)\s]*\d{3}[-.\s]*\d{4}\b/g, " phone ")
    .replace(/[^a-z0-9\s]/g, " ")
    .replace(/\b(?:the|a|an|to|for|of|and|or|it|this|that|now|just|really|very)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function getRespondedLeadStatusFallback_(existingStatus) {
  const existing = String(existingStatus || "").toUpperCase();
  if (existing === "R" || existing === "Y" || existing === "G" || existing === "O") {
    return existing;
  }
  return "Y";
}

function coerceRespondedLeadStatus_(candidateStatus, existingStatus) {
  const candidate = String(candidateStatus || "").toUpperCase();
  if (candidate === "R" || candidate === "Y" || candidate === "G" || candidate === "O") {
    return candidate;
  }
  return getRespondedLeadStatusFallback_(existingStatus);
}

function buildSystemPrompt_(rowObj) {
  const yourName = PropertiesService.getScriptProperties().getProperty("YOUR_NAME") || "Yoni";
  const agentFirstName = getCanonicalFirstName_(rowObj);

  return `
You are texting as ${yourName}, who helps agents with short sale processing, lender negotiations, approvals, and getting deals to closing.

Write in Yoni's voice, but never claim that a physical action, call, email, location, language ability, or completed task happened unless the system context proves it.

STYLE:
- Match the tone of the agent
- Keep responses short by default
- Only go long if they ask real questions about process, fees, or structure
- Never sound salesy or pushy
- Never over-explain unless necessary
- Always write in first person as ${yourName} using "I", "me", and "my"
- Never refer to ${yourName} in the third person
- Never use emojis
- Never use bullet points
- Never use em dashes
- Never mention Calendly or any scheduling link
- Default to no more than two short sentences and one call to action
- Answer only what the agent asked; do not introduce fees, buyers, email, statistics, or scheduling unless relevant
- Read the entire inbound message. A later direct question or expression of interest outranks an earlier self-handling or decline clause
- Never claim to speak Spanish or reply in Spanish. If an agent asks whether I speak Spanish or writes in Spanish, reply in English: \"No, I'm sorry, I don't speak Spanish, but I'd still love to help if you think communicating in English would be possible.\"

HOW TO RESPOND:
1. Always respond directly to what they said
2. Keep it simple and natural
3. Do not force a call unless it feels natural
4. If they are not interested, exit cleanly
5. If they already have help, acknowledge it and leave the door open lightly
6. If they say "keep me in mind" or something similar, thank them and leave the door open
7. If they ask questions, answer clearly and simply
8. In follow-up replies, do not keep restating the full property address unless it is truly needed
9. Prefer phrases like "your listing", "the file", or "this one" instead of repeating the full address

IMPORTANT BEHAVIOR:
- For a clear no, your final closeout should be something like:
  "Ok, no problem. If anything changes in the future and you're looking for additional help with these files, please just keep me in mind. Thanks"
- Treat polite declines like "I'm fine", "we're all set", and "thank you for reaching out" the same as "no thanks"
- If they say they already have a negotiator, processor, lawyer, or someone handling it, treat that as a no and use the normal closeout
- If they ask whether I am a negotiator, confirm that I handle the short sale process and lender negotiations, then invite a phone call. That is a clarification question, not a statement that they already have help.
- If they ask whether this is the best or correct number to reach me, reply exactly: "Yes, this number is great - call or text anytime. Thanks!" Never include or repeat any numeric phone number because the phone in agent_context belongs to the agent, not me.
- After a clear no closeout, if they later only say "thank you", "ok", "sounds good", thumbs up, or something similar, do not respond
- The correct first name for this agent is "${agentFirstName || "unknown"}"
- If you use their name, use only that exact first name
- Never switch to a middle name, last name, nickname, or any other inferred name from context or history
- If you are not completely sure about using the name, do not use their name at all
- Never begin a reply with "Yoni here", "I'm Yoni", or "This is Yoni"
- Do not sign normal text replies with your name
- Never end a reply with "${yourName}", "- ${yourName}", "— ${yourName}", "Yoni", "- Yoni", or any similar signature
- If a message ends with thanks, just end it with "Thanks" or "Thanks!" and not "Thanks, ${yourName}"
- If they say the listing is not actually a short sale or was changed, just acknowledge it and wish them luck
- If they give a callback time, keep your reply casual and short
- If they give a time window, it is okay to suggest a time inside that window
- If they ask for a website, link, agency info, or where to learn more, give the clean URL exactly as "https://www.crispshortsales.com" and add one short credibility line about Google reviews. Do not add spaces inside the URL.
- If they ask if you are local, say you are based in Atlanta and work nationwide. Do not claim state-specific experience unless it is explicitly provided in system context.
- If they say they are handling it themselves but are willing to review what you have to offer, explain the service directly in the text and ask for a quick call
- If they say the offer was accepted and ask what you do or how you help, congratulate them briefly, ask whether the short sale still needs lender approval, explain that I can handle the approval and closing work, and end with: \"Let me know if you want to find a time to talk it over.\"
- Never say \"Want me to take the file?\" or ask whether I should take the file; that is not how \${yourName} talks
- If they ask "How do you help?" or anything similar, explain only that you handle the lender-side paperwork, calls, follow-up, and negotiations through approval. Do not discuss payment unless they also ask about it.
- The goal of the conversation is always to move toward a phone conversation with ${yourName} when appropriate
- If a conversation needs manual follow-up from ${yourName}, do not send a text reply to the agent
- In any manual handoff situation, leave reply_text empty, set block_reply = true, and let ${yourName} take over
- If they ask whether this is AI, a bot, automated, actually your phone, or whether they are texting a real person, do not reply
- In that situation, set handoff_needed = true, block_reply = true, leave reply_text empty, and let ${yourName} respond personally
- If they sound open to future short-sale negotiation help, distressed-property support, or future work together, treat it as an interested lead, answer naturally, and ask for a time to talk
- If they already have the current file handled but ask for information, a fee, or what I offer for future short sales, answer the specific question they asked
- Explain payment only when they explicitly ask about fee, cost, price, percentage, or how I get paid
- If they say they want to hop on a call or are interested in learning more by phone, set handoff_needed = true and do not reply so ${yourName} can respond personally
- If they give an immediate live window like "I'm free now", "available now", or "anytime now until 2", set handoff_needed = true and do not reply so ${yourName} can take over
- If they say they will reconnect only after they get, find, or secure a buyer and make no present request, briefly confirm that they can reach out then and close as warm future interest without a handoff
- For other deferred interest that asks for a current next step or needs personal follow-up, set handoff_needed = true, block_reply = true, leave reply_text empty, and do not close them out as not interested
- Never say "Calling you now", "I'll call you now", "Talk in a sec", or anything that implies the call is already happening this second
- If they say they missed the call, the call did not come through, or they share an alternate callback number, do not keep texting promises about the call - set handoff_needed = true, block_reply = true, and let ${yourName} take over
- Do not offer to send a short-sale packet, packet, docs, documents, materials, overview, deck, PDF, summary, email summary, text summary, or written explanation unless the agent specifically asks for your info by email
- Never offer to send buyers, buyer leads, potential buyers, or anyone interested in the property
- If they mention buyers but they already have help in place, ignore the buyer comment and just close out politely
- Do not ask for their email address and do not offer to email or text materials unless they specifically ask for your info by email and no email address is available yet
- If a front desk person or gatekeeper replies, say: "Thanks, I appreciate it. Please have the agent text me here if they'd like to talk."
- If anyone asks ${yourName} to meet in person, drop by the office, or come by the office, do not respond with availability and do not set the meeting yourself
- In that situation, set handoff_needed = true, block_reply = true, and let ${yourName} respond manually
- If they send an email address or ask you to email them info and the email address is available in the message or row, reply exactly: "Sure, no problem."
- If they ask you to email them info and no email address is available yet, reply exactly: "sure, no problem. What is your email?"
- For the first fee/payment question, explain that there is no fee to the agent or seller and Crisp charges the buyer a flat fee at closing only if the deal closes.
- If they ask the amount again after that answer, state that the buyer-paid fee is $5,000 at closing.
- If they negotiate the fee, request a discount, or keep pressing after the $5,000 answer, hand off without another bot reply.
- Never mention 1%, fee ranges, commission split percentages, or any made-up pricing details
- If they ask how long I have handled short sales, how much experience I have, how many short sales I have handled, or what my track record is, answer with the approved 15-plus-year experience response and invite a quick call. Do not treat that as a rejection or a stats handoff.
- For any short-sale timeline question, reply exactly: "A complete short-sale package and offer often takes about 60-90 days for a lender decision, though timing varies by lender and lien complexity."
- Never provide or invent success rates, approval rates, close rates, closing rates, percentages, averages, or other unapproved performance stats beyond that approved timeline
- If they ask for a success rate, approval rate, close rate, percentage, or another unapproved performance statistic, set handoff_needed = true, block_reply = true, leave reply_text empty, and let ${yourName} answer personally. If a message mixes a timeline question with one of those unsupported performance questions, hand it off rather than answering only part of it.
- Do not estimate, approximate, say "roughly", or include unsupported numeric claims
- If you find yourself about to repeat the same or a very similar reply, do not repeat it - instead set handoff_needed = true, block_reply = true, and let ${yourName} take over

BUSINESS RULES:
- Company name is Crisp Short Sales
- You are not a mortgage broker
- You specialize in helping agents and homeowners through the short sale process and getting lender approvals as quickly as possible
- You have been doing this for over 15 years
- No cost to agent or seller
- Paid by the buyer at closing
- Charge a flat fee for the service
- No commission split
- Only paid if deal closes

LEAD STATUS:
- R = not interested / stop / already handled / closed out
- O = not interested in help on the current file, but asked for information or left the door open for future opportunities
- G = only use this when a human has actually connected live on the phone, not for text-based call interest or scheduling
- Y = default for any inbound response that is not clearly R, including "let's talk", "call me", "available now", future availability, or callback timing by text
- N = only for leads with no response at all and should never be returned here because this function only runs after an inbound response

REVIEW RULE:
- If you are unsure whether something is R or Y, choose Y and set needs_review = true

HANDOFF:
- Set handoff_needed = true if the conversation needs ${yourName} to step in
- Set handoff_needed = true for strong call intent or scheduling
- Set handoff_needed = true if needs_review = true
- Set handoff_needed = false for normal clear closeouts and normal simple replies

OUTPUT:
Return valid JSON only.
Keep responses natural and human.
`.trim();
}

function getCanonicalFirstName_(rowObj) {
  const rawName = normalizeWhitespace_(String(rowObj && rowObj[HEADERS.agent_name] || ""));
  if (!rawName) return "";

  const withoutTitle = rawName.replace(/^(mr|mrs|ms|miss|dr)\.?\s+/i, "");
  const firstToken = withoutTitle.split(/\s+/)[0] || "";
  return firstToken.replace(/[^A-Za-z'-]/g, "");
}

function isIdentityResendSignal_(text) {
  const t = normalizeWhitespace_(String(text || "").toLowerCase())
    .replace(/[\u2018\u2019]/g, "'")
    .replace(/[.!?]+$/g, "")
    .trim();

  const patterns = [
    /^(?:i'?m\s+)?sorry[, ]*(?:but\s+)?who is this$/,
    /^who is this$/,
    /^who'?s this$/,
    /^who am i (?:speaking|talking|texting) (?:with|to)$/,
    /^may i ask who this is$/,
    /^can i ask who this is$/
  ];

  return patterns.some(pattern => pattern.test(t));
}

function buildIdentityResendReply_(rowObj) {
  const firstName = getCanonicalFirstName_(rowObj) || "there";
  const listingAddress = normalizeWhitespace_(String(rowObj && rowObj[HEADERS.listing_address] || ""));
  const listingReference = listingAddress
    ? " I saw your short sale at " + listingAddress + "."
    : " I saw your short sale listing.";

  return "Sorry, I had messaged you earlier: Hey " + firstName
    + ", this is Yoni Kutler with Crisp Short Sales."
    + listingReference
    + " I help agents by handling the bank side of the short sale process so files get approved faster and are less likely to fall apart."
    + " There's no cost to you or your seller. Are you handling that part yourself or do you already have help?";
}

function sanitizeReplySelfIntro_(replyText) {
  let text = String(replyText || "").trim();
  if (!text) {
    return text;
  }

  text = text.replace(/^(?:yoni here|this is yoni|i'?m yoni|i am yoni)\s*(?:[-,:.!]\s*)*/i, "");

  return text.trim();
}

function sanitizeReplyNameUsage_(replyText, rowObj) {
  const text = String(replyText || "");
  const firstName = getCanonicalFirstName_(rowObj);
  if (!text || !firstName) {
    return text;
  }

  const commonGreetingWords = {
    absolutely: true,
    cool: true,
    good: true,
    great: true,
    no: true,
    perfect: true,
    sounds: true,
    sure: true,
    thanks: true,
    thank: true,
    will: true,
    yes: true
  };

  const namePatterns = [
    { pattern: /^(thanks)\s+[A-Za-z'-]+(\b|[,.!?:;])/i, prefix: "Thanks " },
    { pattern: /^(thank you)\s+[A-Za-z'-]+(\b|[,.!?:;])/i, prefix: "Thank you " },
    { pattern: /^(hi)\s+[A-Za-z'-]+(\b|[,.!?:;])/i, prefix: "Hi " },
    { pattern: /^(hey)\s+[A-Za-z'-]+(\b|[,.!?:;])/i, prefix: "Hey " },
    { pattern: /^(ok)\s+[A-Za-z'-]+(\b|[,.!?:;])/i, prefix: "Ok " },
    { pattern: /^(okay)\s+[A-Za-z'-]+(\b|[,.!?:;])/i, prefix: "Okay " }
  ];

  for (const item of namePatterns) {
    if (item.pattern.test(text)) {
      return text.replace(item.pattern, function(match, leadingWord, trailingChar) {
        const possibleName = normalizeWhitespace_(match.replace(new RegExp("^" + leadingWord + "\\s+", "i"), "").replace(/[,.!?:;]/g, ""));
        if (commonGreetingWords[possibleName.toLowerCase()]) {
          return match;
        }
        return item.prefix + firstName + (trailingChar || "");
      });
    }
  }

  return text;
}

function sanitizeReplySignoff_(replyText) {
  let text = String(replyText || "");
  if (!text) {
    return text;
  }

  text = text.replace(/,\s*Yoni\s+Kutler\s*$/i, "");
  text = text.replace(/,\s*Yoni\s*$/i, "");
  text = text.replace(/\s*[-–—]+\s*Yoni\s+Kutler\s*$/i, "");
  text = text.replace(/\s*[-–—]+\s*Yoni\s*$/i, "");
  text = text.replace(/\n\s*Yoni\s+Kutler\s*$/i, "");
  text = text.replace(/\n\s*Yoni\s*$/i, "");
  text = text.replace(/([.!?])\s*Yoni\s+Kutler\s*$/i, "$1");
  text = text.replace(/([.!?])\s*Yoni\s*$/i, "$1");
  text = text.replace(/\bThanks,\s*Yoni\s+Kutler\b/i, "Thanks");
  text = text.replace(/\bThanks,\s*Yoni\b/i, "Thanks");
  text = text.replace(/\bThank you,\s*Yoni\s+Kutler\b/i, "Thank you");
  text = text.replace(/\bThank you,\s*Yoni\b/i, "Thank you");

  return text.trim();
}

function sanitizeReplyCallPromise_(replyText) {
  const text = String(replyText || "").trim();
  if (!text) {
    return text;
  }

  if (/(?:^|\b)(calling you now|i'?ll call you now|i will call you now|i'?ll call your cell now|i will call your cell now|talk in a sec)(?:\b|[.!?])/i.test(text)) {
    return "Perfect, thanks.";
  }

  return text;
}

function enforceDurableFollowupPromiseRule_(decision, inboundText) {
  const guarded = Object.assign({}, decision || {});
  const reply = normalizeWhitespace_(String(guarded.reply_text || ""));
  if (!/\b(?:i|we)(?:['’]?ll|\s+will)\s+(?:check\s+back|follow\s+up|reach\s+out|call|text|contact)\b/i.test(reply)) {
    return guarded;
  }
  if (String(guarded.call_booking_status || "").toLowerCase() === "scheduled_callback" && guarded.callback_time) {
    return guarded;
  }

  const reference = extractScheduledCallbackReference_(inboundText) || extractScheduledCallbackReference_(reply);
  guarded.reply_text = reference
    ? "No problem. What time " + reference + " works best for a quick call?"
    : "No problem. What day and time works best for a quick call?";
  guarded.call_booking_status = "interested_no_call";
  guarded.callback_time = "";
  guarded.callback_requested = "";
  guarded.reason = "Replaced an unscheduled bot follow-up promise with a request for durable callback timing";
  return guarded;
}

function ensureQuestionDisposition_(decision, inboundText) {
  const guarded = Object.assign({}, decision || {});
  if (!isSubstantiveFollowupSignal_(inboundText)) return guarded;
  if (normalizeWhitespace_(String(guarded.reply_text || ""))) return guarded;
  if (guarded.handoff_needed || guarded.needs_review || guarded.alert_needed) return guarded;
  return buildManualHandoffDecision_(
    "Substantive agent question had no safe automated answer; manual follow-up needed",
    "UNANSWERED QUESTION REVIEW"
  );
}

function sanitizeReplyPropertyReference_(replyText, rowObj) {
  let text = String(replyText || "");
  const listingAddress = normalizeWhitespace_(String(rowObj && rowObj[HEADERS.listing_address] || ""));

  if (!text || !listingAddress) {
    return text;
  }

  const escapedAddress = escapeRegex_(listingAddress);

  text = text.replace(new RegExp("\\bwith\\s+" + escapedAddress + "\\b", "gi"), "with your listing");
  text = text.replace(new RegExp("\\bfor\\s+" + escapedAddress + "\\b", "gi"), "for your listing");
  text = text.replace(new RegExp("\\bon\\s+" + escapedAddress + "\\b", "gi"), "on your listing");
  text = text.replace(new RegExp("\\bat\\s+" + escapedAddress + "\\b", "gi"), "at your listing");
  text = text.replace(new RegExp("\\b" + escapedAddress + "\\b", "gi"), "your listing");

  return text
    .replace(/\byour listing\s+or\s+your listing\b/gi, "your listing")
    .replace(/\s+/g, " ")
    .trim();
}

function sanitizeReplyBuyerOffer_(replyText) {
  const text = String(replyText || "").trim();
  if (!text) {
    return text;
  }

  const normalized = normalizeWhitespace_(text.toLowerCase());

  // Allow the approved clarification that we do not bring buyers, while still
  // blocking any reply that promises to send or bring buyer leads.
  if (normalized.indexOf("i don't necessarily have a buyer") !== -1 || normalized.indexOf("i dont necessarily have a buyer") !== -1 || normalized.indexOf("i do not necessarily have a buyer") !== -1) {
    return text;
  }

  const offersBuyers = [
    /\bsend\b.*\bbuyers?\b/,
    /\bhave any buyers?\b/,
    /\bhave buyers?\b/,
    /\bbuyer leads?\b/,
    /\bbring\b.*\bbuyers?\b/,
    /\bpotential buyers?\b/,
    /\binterested buyers?\b/
  ].some(pattern => pattern.test(normalized));

  if (offersBuyers) {
    return getStandardNoCloseoutReply_();
  }

  return text;
}

function sanitizeReplyPhoneOnlyCta_(replyText) {
  const text = String(replyText || "").trim();
  if (!text) {
    return text;
  }

  // Approved info-email requests must keep the canonical acknowledgement so
  // the downstream approval workflow can recognize and queue the email.
  if (normalizeWhitespace_(text) === normalizeWhitespace_(getInfoEmailAcknowledgementReply_())) {
    return text;
  }

  if (/^thanks for sending your email[.!]?$/i.test(text)) {
    return text;
  }

  if (/\b(?:what(?:'s| is)|which is)\s+(?:the\s+)?best email\b|\bwhat(?:'s| is) your email\b/i.test(text)) {
    return text;
  }

  const normalized = normalizeWhitespace_(text.toLowerCase());
  const mentionsWrittenMaterials = [
    /\bpacket\b/,
    /\bpackets\b/,
    /\bdocs\b/,
    /\bdocuments\b/,
    /\bmaterials\b/,
    /\bone[- ]page\b/,
    /\bsummary\b/,
    /\bpdf\b/,
    /\bdeck\b/,
    /\boverview\b/,
    /\bbrochure\b/,
    /\binformation\b/,
    /\binfo\b/
  ].some(pattern => pattern.test(normalized));

  const offersDelivery = [
    /\bi can send\b/,
    /\bi could send\b/,
    /\bi'?ll send\b/,
    /\bi will send\b/,
    /\bsend them\b/,
    /\bsend it\b/,
    /\bemail it\b/,
    /\bemail them\b/,
    /\btext it\b/,
    /\btext them\b/,
    /\bbest email\b/,
    /\bemail for sending\b/,
    /\bshould i text\b/,
    /\bshould i email\b/
  ].some(pattern => pattern.test(normalized));

  if (mentionsWrittenMaterials || offersDelivery) {
    return "I appreciate it. If there's a good time for us to chat about your listing, just let me know and I can give you a call.";
  }

  return text;
}

function sanitizeReplyFileCta_(replyText) {
  const text = String(replyText || "").trim();
  if (!text) {
    return text;
  }

  return text.replace(/\bwant me to take the file\??/gi, "Let me know if you want to find a time to talk it over.");
}
function containsEmailAddress_(text) {
  return !!extractEmailAddress_(text);
}

function extractEmailAddress_(text) {
  const raw = String(text || "");
  const spacedMatch = raw.match(
    /\b(?:my\s+)?email(?:\s+address)?\s*(?:is|:)\s*([A-Z0-9._%+-]+(?:\s+[A-Z0-9._%+-]+){1,4})\s*@\s*([A-Z0-9.-]+\.[A-Z]{2,})\b/i
  );
  if (spacedMatch) {
    return normalizeEmailAddress_(spacedMatch[1].replace(/\s+/g, "") + "@" + spacedMatch[2]);
  }

  const match = raw.match(/\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b/i);
  return match ? normalizeEmailAddress_(match[0]) : "";
}

function normalizeEmailAddress_(email) {
  const cleaned = String(email || "").trim().replace(/[.,;:!?]+$/g, "").toLowerCase();
  return isValidEmailAddress_(cleaned) ? cleaned : "";
}

function isValidEmailAddress_(email) {
  return /^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$/i.test(String(email || "").trim());
}

function isDeclineWithInfoRequestSignal_(text, rowObj) {
  const parts = [
    text,
    rowObj && rowObj[HEADERS.response_status]
  ];

  getHistoryArray_(rowObj && rowObj[HEADERS.history_json]).forEach(function(entry) {
    if (entry && entry.role === "agent") parts.push(entry.text || "");
  });

  const combined = normalizeWhitespace_(String(parts.filter(Boolean).join(" ")).toLowerCase());
  if (!combined || !isEmailRequestSignal_(combined)) return false;

  const selfHandled = /\b(?:i|we)\s+(?:am|are|will be|usually)?\s*(?:handling|handle|do)\s+(?:it|this|the file|the short sale|that part)?\s*(?:myself|ourselves|in[- ]house)\b/.test(combined) ||
    /\b(?:i|we)\s+do\s+(?:my|our)\s+own\b/.test(combined);

  return isAlreadyHandledSignal_(combined) || isClearNoSignal_(combined) || selfHandled;
}

function hasServiceInfoRequestContext_(text, rowObj) {
  const parts = [text, rowObj && rowObj[HEADERS.response_status]];
  getHistoryArray_(rowObj && rowObj[HEADERS.history_json]).forEach(function(entry) {
    if (entry && entry.role === "agent") parts.push(entry.text || "");
  });
  const combined = normalizeWhitespace_(String(parts.filter(Boolean).join(" ")).toLowerCase());
  return /\b(?:more\s+)?info(?:rmation)?\s+(?:on|about)\s+(?:your\s+)?services?\b/.test(combined) ||
    /\b(?:what|how)\b.{0,35}\b(?:services?|help|handle|offer)\b/.test(combined);
}

function buildServiceInfoEmailAcknowledgement_(hasEmail) {
  if (hasEmail === false) {
    return "Absolutely, I'd be happy to email you more information. What's the best email?";
  }
  return getInfoEmailAcknowledgementReply_();
}

function isEmailRequestSignal_(text) {
  const t = normalizeWhitespace_(String(text || "").toLowerCase());
  const patterns = [
    /\bemail me\b/,
    /\bplease email me\b/,
    /\bsend me your info\b/,
    /\bsend your info\b/,
    /\bsend me info\b/,
    /\bsend me your information\b/,
    /\bsend your information\b/,
    /\bemail your info\b/,
    /\bemail your information\b/,
    /\bshoot me an email\b/,
    /\bemail me your info\b/,
    /\bemail me your information\b/,
    /\bsend (?:me|us) (?:some |more )?(?:info|information)\b/,
    /\b(?:get|receive) (?:some |more )?(?:info|information)\b/,
    /\b(?:info|information) (?:on|about) (?:your )?(?:services|company)\b/
  ];

  return patterns.some(pattern => pattern.test(t));
}

function isStatsOrNumericClaimQuestion_(text) {
  const t = normalizeWhitespace_(String(text || "").toLowerCase());
  if (!t) {
    return false;
  }
  if (isExperienceTrackRecordQuestionSignal_(t)) {
    return false;
  }

  const patterns = [
    /\bsuccess rate\b/,
    /\bapproval rate\b/,
    /\bapproved rate\b/,
    /\bclose rate\b/,
    /\bclosing rate\b/,
    /\bconversion rate\b/,
    /\btrack record\b/,
    /\bstats?\b/,
    /\bstatistics\b/,
    /\byour numbers\b/,
    /\bwhat are your numbers\b/,
    /\bhow often\b.*\b(approve|approved|approval|close|closing|success|successful)\b/,
    /\bhow many\b.*\b(short sales?|deals?|files?|approvals?|closings?|transactions?)\b/,
    /\bhow long\b.*\b(approval|approved|approve|close|closing|process|take|takes|timeline)\b/,
    /\bhow fast\b.*\b(approval|approved|approve|close|closing|get approved)\b/,
    /\baverage\b.*\b(time|timeline|approval|close|closing|days|weeks|months)\b/,
    /\btypical\b.*\b(time|timeline|approval|close|closing|days|weeks|months)\b/,
    /\btimeline\b/,
    /\btimeframe\b/,
    /\bturnaround time\b/
  ];

  return patterns.some(pattern => pattern.test(t));
}

function containsUnsupportedStatsClaim_(text) {
  const t = normalizeWhitespace_(String(text || "").toLowerCase());
  if (!t) {
    return false;
  }

  const patterns = [
    /\b(success|approval|close|closing|conversion)\s+rate\b/,
    /\b\d+\s*(?:-|to)\s*\d+\s*%/,
    /\b\d+\s*%\b/,
    /\b\d+\s*(?:percent|percentage)\b/,
    /\b(?:roughly|around|about|approximately)\s+\d+/,
    /\b(?:hundreds|thousands|dozens)\s+of\s+(short sales?|deals?|files?|approvals?|closings?|transactions?)\b/,
    /\b\d+\s+(short sales?|deals?|files?|approvals?|closings?|transactions?)\b/,
    /\b\d+\s+(days|weeks|months)\b/,
    /\baverage\s+(time|timeline|approval|close|closing)\b/,
    /\bapproval\s+(time|timeline)\b/,
    /\bclosing\s+(time|timeline)\b/
  ];

  return patterns.some(pattern => pattern.test(t));
}

function escapeRegex_(value) {
  return String(value || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function getHandoffDisplayName_(data) {
  const sheetName = cleanNameForEmail_(data && data.agent_name);
  const sheetLastName = cleanNameForEmail_(data && data.last_name);
  const openerFirstName = extractFirstNameFromInitialText_(
    data && (data.initial_text || data.initial_text_sent || "") || findInitialAssistantText_(data && data.history)
  );

  if (openerFirstName && sheetLastName) {
    return joinUniqueNameTokens_([openerFirstName, sheetLastName]);
  }

  if (openerFirstName && sheetName && !nameContainsToken_(sheetName, openerFirstName) && isLikelyLastNameOnly_(sheetName)) {
    return joinUniqueNameTokens_([openerFirstName, sheetName]);
  }

  const fromSheet = joinUniqueNameTokens_([sheetName, sheetLastName]);
  return fromSheet || data && data.phone || "Unknown Agent";
}

function cleanNameForEmail_(value) {
  return normalizeWhitespace_(String(value || ""))
    .replace(/^(mr|mrs|ms|miss|dr)\.?\s+/i, "")
    .replace(/^[,.\s]+|[,.\s]+$/g, "");
}

function extractFirstNameFromInitialText_(text) {
  const match = normalizeWhitespace_(String(text || "")).match(/^hey\s+([A-Za-z][A-Za-z'-]*)\b/i);
  return match ? cleanNameForEmail_(match[1]) : "";
}

function findInitialAssistantText_(history) {
  if (!Array.isArray(history)) {
    return "";
  }

  for (const entry of history) {
    const text = normalizeWhitespace_(String(entry && entry.text || ""));
    if (entry && entry.role === "assistant" && /^hey\s+[A-Za-z][A-Za-z'-]*\b/i.test(text)) {
      return text;
    }
  }

  return "";
}

function isLikelyLastNameOnly_(name) {
  const tokens = cleanNameForEmail_(name).split(/\s+/).filter(Boolean);
  if (tokens.length === 1) {
    return true;
  }

  return tokens.length === 2 && /^(jr|sr|ii|iii|iv)\.?$/i.test(tokens[1]);
}

function nameContainsToken_(name, token) {
  const cleanedToken = cleanNameForEmail_(token).toLowerCase();
  if (!cleanedToken) {
    return false;
  }

  return cleanNameForEmail_(name)
    .split(/\s+/)
    .some(part => cleanNameForEmail_(part).toLowerCase() === cleanedToken);
}

function joinUniqueNameTokens_(parts) {
  const tokens = [];
  const seen = {};

  parts.forEach(part => {
    cleanNameForEmail_(part).split(/\s+/).forEach(token => {
      const cleaned = cleanNameForEmail_(token);
      const key = cleaned.toLowerCase();
      if (cleaned && !seen[key]) {
        seen[key] = true;
        tokens.push(cleaned);
      }
    });
  });

  return tokens.join(" ");
}

function sendHandoffEmail_(data) {
  const props = PropertiesService.getScriptProperties();
  const toEmail = props.getProperty("HANDOFF_EMAIL") || "yoni.kutler@ygkutler.com";
  const fullName = getHandoffDisplayName_(data);
  const handoffType = data.handoff_type || "MANUAL FOLLOW-UP";
  const formattedPhone = formatPhoneForEmail_(data.phone);
  const formattedAddress = formatPropertyAddressForEmail_(data);
  const historyText = formatConversationHistory_(data.history || []);

  const subject = `NEW LEAD 🔥 - ${handoffType} - ${fullName}`;

  const body = `
We have a new lead interested in your services, and a manual follow-up is now needed.

Handoff Reason: ${handoffType}
Agent Name: ${fullName}
Phone: ${formattedPhone}
Email: ${data.email || ""}
Address: ${formattedAddress}

Last message received:
${data.last_message || ""}

Full Convo:
${historyText}
`.trim();

  return queueHandoffEmailV11_({
    to: toEmail,
    subject: subject,
    body: body
  });
}

function shouldSendInfoEmail_(ruleResult, decision) {
  return !!(
    ruleResult &&
    ruleResult.send_info_email &&
    isValidEmailAddress_(ruleResult.info_email_to) &&
    decision &&
    !decision.block_reply &&
    !decision.handoff_needed &&
    !decision.needs_review
  );
}

function getInfoEmailAcknowledgementReply_() {
  return "Absolutely, I'll email you more information shortly. Thanks for sending your email.";
}

function isInfoEmailApprovalRequired_() {
  const props = PropertiesService.getScriptProperties();
  const raw = props.getProperty("INFO_EMAIL_APPROVAL_REQUIRED");
  if (raw === null || raw === undefined || String(raw).trim() === "") {
    return true;
  }

  return !/^(false|0|no|off)$/i.test(String(raw).trim());
}

function sendInfoEmailApprovalRequest_(data) {
  const hydratedData = hydrateInfoEmailDataFromSheet_(data);
  const props = PropertiesService.getScriptProperties();
  const toEmail = props.getProperty("INFO_EMAIL_APPROVAL_TO") || props.getProperty("HANDOFF_EMAIL") || "yoni.kutler@ygkutler.com";
  const agentName = getHandoffDisplayName_(hydratedData);
  const approvalId = createInfoEmailApproval_(hydratedData);
  const approvalUrl = buildInfoEmailApprovalUrl_(approvalId);
  const subject = "APPROVE INFO EMAIL - " + agentName + " - " + getStreetNameForInfoEmail_(hydratedData.listing_address);
  const body = `
An agent requested the short-sale info email. Approval is required before sending.

Approve and send:
${approvalUrl}

Agent: ${agentName}
Email: ${hydratedData.to || ""}
Phone: ${formatPhoneForEmail_(hydratedData.phone)}
Property: ${formatPropertyAddressForEmail_(hydratedData)}

Last message:
${hydratedData.last_message || ""}

Draft subject:
${buildAgentInfoEmailSubject_(hydratedData)}

Draft body:
${buildAgentInfoEmailBody_(hydratedData)}
`.trim();

  try {
    MailApp.sendEmail({
      to: toEmail,
      subject: subject,
      body: body
    });
    try {
      appendSmsDebugLog_("info_email_approval_requested", {
        phone: hydratedData.phone || "",
        message: hydratedData.to || "",
        reason: subject,
        result: approvalId
      });
    } catch (_) {}
    return { ok: true, approval_id: approvalId, to: toEmail, subject: subject };
  } catch (err) {
    try {
      appendSmsDebugLog_("info_email_approval_failed", {
        phone: hydratedData.phone || "",
        message: hydratedData.to || "",
        reason: String(err),
        stack: err && err.stack ? err.stack : ""
      });
    } catch (_) {}
    throw err;
  }
}

function requestInfoEmailApprovalForRow_(body) {
  const sheet = getSheet_();
  const rows = getSheetData_(sheet);
  const requestedRow = Number(body && body.row || 0);
  const requestedPhone = normalizePhone_(body && body.phone);
  let item = null;

  if (requestedRow >= 2) {
    item = rows.find(candidate => candidate.row === requestedRow) || null;
  }
  if (!item && requestedPhone) {
    item = rows.find(candidate =>
      normalizePhone_(candidate && candidate.obj && candidate.obj[HEADERS.phone]) === requestedPhone
    ) || null;
  }
  if (!item) throw new Error("SMS row not found for info-email approval recovery");

  const rowObj = item.obj || {};
  const targetEmail = normalizeEmailAddress_(body && body.email || rowObj[HEADERS.email]);
  if (!isValidEmailAddress_(targetEmail)) {
    throw new Error("Missing valid agent email for info-email approval recovery");
  }

  const result = sendInfoEmailApprovalRequest_({
    to: targetEmail,
    first_name: getCanonicalFirstName_(rowObj),
    agent_name: rowObj[HEADERS.agent_name] || "",
    last_name: rowObj[HEADERS.last_name] || "",
    listing_address: rowObj[HEADERS.listing_address] || "",
    city: rowObj[HEADERS.city] || "",
    state: rowObj[HEADERS.state] || "",
    zip: rowObj[HEADERS.zip] || "",
    phone: rowObj[HEADERS.phone] || body.phone || "",
    last_message: rowObj[HEADERS.last_inbound_text] || rowObj[HEADERS.response_status] || ""
  });

  return Object.assign({ ok: true, row: item.row, agent_email: targetEmail }, result || {});
}

function sendAgentInfoEmail_(data) {
  const hydratedData = hydrateInfoEmailDataFromSheet_(data);
  const toEmail = normalizeEmailAddress_(hydratedData.to);
  if (!toEmail) {
    throw new Error("Missing valid agent info email recipient");
  }

  return sendAgentInfoEmailViaBackend_({
    to: toEmail,
    subject: buildAgentInfoEmailSubject_(hydratedData),
    body: buildAgentInfoEmailBody_(hydratedData)
  });
}

function hydrateInfoEmailDataFromSheet_(data) {
  const hydrated = Object.assign({}, data || {});
  const targetPhone = normalizePhone_(hydrated.phone);
  const targetEmail = normalizeEmailAddress_(hydrated.to);
  if (!targetPhone && !targetEmail) return hydrated;

  try {
    const sheet = getSheet_();
    const rows = getSheetData_(sheet);
    let best = null;
    let bestScore = -1;

    rows.forEach(item => {
      const rowObj = item && item.obj ? item.obj : {};
      const rowPhone = normalizePhone_(rowObj[HEADERS.phone]);
      const rowEmail = normalizeEmailAddress_(rowObj[HEADERS.email]);
      const phoneMatch = !!targetPhone && rowPhone === targetPhone;
      const emailMatch = !!targetEmail && rowEmail === targetEmail;
      if (!phoneMatch && !emailMatch) return;

      let score = phoneMatch ? 20 : 0;
      score += emailMatch ? 10 : 0;
      score += String(rowObj[HEADERS.agent_name] || "").trim() ? 4 : 0;
      score += String(rowObj[HEADERS.last_name] || "").trim() ? 2 : 0;
      score += String(rowObj[HEADERS.listing_address] || "").trim() ? 4 : 0;
      if (score > bestScore) {
        best = rowObj;
        bestScore = score;
      }
    });

    if (!best) return hydrated;
    const fill = (key, value) => {
      if (!String(hydrated[key] || "").trim() && String(value || "").trim()) {
        hydrated[key] = value;
      }
    };

    fill("first_name", getCanonicalFirstName_(best));
    fill("agent_name", best[HEADERS.agent_name]);
    fill("last_name", best[HEADERS.last_name]);
    fill("listing_address", best[HEADERS.listing_address]);
    fill("city", best[HEADERS.city]);
    fill("state", best[HEADERS.state]);
    fill("zip", best[HEADERS.zip]);
    fill("phone", best[HEADERS.phone]);
    fill("to", best[HEADERS.email]);
  } catch (_) {
    // Approval should remain usable even if the live CRM lookup is temporarily unavailable.
  }

  return hydrated;
}

function createInfoEmailApproval_(data) {
  const approvalId = Utilities.getUuid();
  const payload = {
    created_at: new Date().toISOString(),
    data: data
  };

  PropertiesService.getScriptProperties().setProperty(
    "INFO_EMAIL_PENDING_" + approvalId,
    JSON.stringify(payload)
  );

  return approvalId;
}

function buildInfoEmailApprovalUrl_(approvalId) {
  const props = PropertiesService.getScriptProperties();
  const configuredBaseUrl = String(props.getProperty("INFO_EMAIL_APPROVAL_BASE_URL") || "").trim();
  const baseUrl = configuredBaseUrl || ScriptApp.getService().getUrl();
  if (!baseUrl) {
    throw new Error("Missing INFO_EMAIL_APPROVAL_BASE_URL for approval link");
  }

  const gatewayUrl = String(
    props.getProperty("INFO_EMAIL_APPROVAL_GATEWAY_URL") ||
    "https://crisp-voice-bot.onrender.com/info-email/approve"
  ).trim();
  return gatewayUrl
    + "?target=" + encodeURIComponent(baseUrl)
    + "&id=" + encodeURIComponent(approvalId);
}

function approvePendingInfoEmail_(approvalId) {
  const id = String(approvalId || "").trim();
  if (!/^[0-9a-f-]{36}$/i.test(id)) {
    return {
      ok: false,
      message: "Invalid approval link."
    };
  }

  const props = PropertiesService.getScriptProperties();
  const key = "INFO_EMAIL_PENDING_" + id;
  const raw = props.getProperty(key);
  if (!raw) {
    return {
      ok: false,
      message: "This approval link is no longer available or was already used."
    };
  }

  const parsed = JSON.parse(raw);
  const savedData = parsed && parsed.data ? parsed.data : null;
  const data = savedData ? hydrateInfoEmailDataFromSheet_(savedData) : null;
  if (!data) {
    props.deleteProperty(key);
    return {
      ok: false,
      message: "The saved approval payload was invalid."
    };
  }

  const sendResult = sendAgentInfoEmail_(data);
  props.deleteProperty(key);
  return {
    ok: true,
    message: "Info email sent.",
    to: data.to || "",
    subject: buildAgentInfoEmailSubject_(data),
    backend: sendResult
  };
}

function sendAgentInfoEmailViaBackend_(payload) {
  const props = PropertiesService.getScriptProperties();
  const endpoint = String(props.getProperty("INFO_EMAIL_BACKEND_URL") || "").trim();
  const secret = String(props.getProperty("INFO_EMAIL_BACKEND_SECRET") || "").trim();
  if (!endpoint || !secret) {
    throw new Error("Private info email backend is not configured");
  }

  const response = UrlFetchApp.fetch(endpoint, {
    method: "post",
    contentType: "application/json",
    muteHttpExceptions: true,
    headers: {
      "x-crisp-info-email-secret": secret
    },
    payload: JSON.stringify(payload)
  });

  const status = response.getResponseCode();
  const text = response.getContentText();
  let parsed = {};
  try {
    parsed = text ? JSON.parse(text) : {};
  } catch (_) {
    parsed = { raw: text };
  }

  if (status < 200 || status >= 300 || !parsed.ok) {
    throw new Error("Private info email backend failed: HTTP " + status + " " + text);
  }

  return parsed;
}

function htmlOutput_(result) {
  const safeTitle = result && result.ok ? "Info email sent" : "Info email not sent";
  const safeMessage = escapeHtmlForApprovalPage_(String(result && result.message || ""));
  const details = result && result.ok
    ? "<p><strong>To:</strong> " + escapeHtmlForApprovalPage_(String(result.to || "")) + "</p>"
      + "<p><strong>Subject:</strong> " + escapeHtmlForApprovalPage_(String(result.subject || "")) + "</p>"
    : "";

  return HtmlService.createHtmlOutput(
    "<!doctype html><html><head><base target=\"_top\"><meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">"
    + "<style>body{font-family:Arial,sans-serif;margin:32px;line-height:1.45;color:#17202a}main{max-width:680px}</style></head>"
    + "<body><main><h2>" + escapeHtmlForApprovalPage_(safeTitle) + "</h2><p>" + safeMessage + "</p>" + details + "</main></body></html>"
  );
}

function escapeHtmlForApprovalPage_(value) {
  return String(value || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function buildAgentInfoEmailSubject_(data) {
  const property = formatPropertyAddressForEmail_(data) || "Your Listing";
  return ("Crisp Short Sales - " + property).slice(0, 160);
}

function getStreetNameForInfoEmail_(listingAddress) {
  const firstPart = String(listingAddress || "").split(",")[0].trim();
  return firstPart || "Your Listing";
}

function buildAgentInfoEmailBody_(data) {
  const firstName = getAgentInfoEmailFirstName_(data);
  const greeting = firstName ? "Hi " + firstName + "," : "Hi,";

  return `
${greeting}

Thanks for reaching out. I help agents by handling the entire lender side of the short sale process, including collecting documents, submitting the package, lender calls and follow-up, valuations, negotiations, and getting the file through approval and closing.

There is no cost to the agent or seller and no commission split. I charge a flat fee to the buyer, paid only if the deal closes. As long as the fee is disclosed in the listing, buyers can factor it into their offer.

I have been handling short sales for over 15 years, and this is all I do. I would be happy to talk about your upcoming listings and answer any questions.

Thanks!

Yoni Kutler
404-300-9526
www.crispshortsales.com
www.facebook.com/CrispShortSales
`.trim();
}

function getAgentInfoEmailFirstName_(data) {
  const explicit = String(data && data.first_name || "").trim();
  if (explicit) {
    return explicit;
  }

  const rawName = normalizeWhitespace_(String(data && data.agent_name || ""));
  if (!rawName) {
    return "";
  }

  const withoutTitle = rawName.replace(/^(mr|mrs|ms|miss|dr)\.?\s+/i, "");
  const firstToken = withoutTitle.split(/\s+/)[0] || "";
  return firstToken.replace(/[^A-Za-z'-]/g, "");
}

function formatPhoneForEmail_(phone) {
  const digits = normalizePhone_(phone);
  if (digits.length === 10) {
    return digits.slice(0, 3) + "-" + digits.slice(3, 6) + "-" + digits.slice(6);
  }

  return String(phone || "");
}

function formatPropertyAddressForEmail_(data) {
  const parts = [
    String(data && data.listing_address || "").trim(),
    String(data && data.city || "").trim(),
    String(data && data.state || "").trim(),
    String(data && data.zip || "").trim()
  ].filter(Boolean);

  return parts.join(", ");
}

function sendSystemAlertEmail_(subject, body) {
  const props = PropertiesService.getScriptProperties();
  const toEmail = props.getProperty("HANDOFF_EMAIL") || "yoni.kutler@ygkutler.com";

  MailApp.sendEmail({
    to: toEmail,
    subject: subject,
    body: body
  });
}

function formatConversationHistory_(history) {
  if (!Array.isArray(history) || history.length === 0) {
    return "No conversation history available.";
  }

  return history.map(entry => {
    const role = entry.role === "assistant" ? "Bot" : "Agent";
    const ts = entry.ts || "";
    const text = entry.text || "";
    return `[${ts}] ${role}: ${text}`;
  }).join("\n");
}

function extractOutputText_(parsed) {
  if (parsed.output_text) return parsed.output_text;

  if (Array.isArray(parsed.output)) {
    for (const item of parsed.output) {
      if (Array.isArray(item.content)) {
        for (const part of item.content) {
          if (typeof part.text === "string") return part.text;
        }
      }
    }
  }

  throw new Error("Could not extract output text from OpenAI response");
}


function getSmsSpreadsheet_() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  if (!ss) {
    throw new Error("Spreadsheet not available in bound Apps Script context");
  }
  return ss;
}

function getSheet_() {
  const ss = getSmsSpreadsheet_();
  const sheetName = PropertiesService.getScriptProperties().getProperty("SHEET_NAME");
  const sheet = ss.getSheetByName(sheetName);
  if (!sheet) throw new Error("Sheet not found: " + sheetName);
  return sheet;
}

function getSheetData_(sheet) {
  const values = sheet.getDataRange().getValues();
  const headers = values[0];
  return values.slice(1).map((row, idx) => {
    const obj = {};
    headers.forEach((h, i) => obj[h] = row[i]);
    return { row: idx + 2, obj };
  });
}

function findOrCreateRowByPhone_(sheet, data, phoneRaw) {
  const normalized = normalizePhone_(phoneRaw);

  for (const item of data) {
    const rowPhone = normalizePhone_(String(item.obj[HEADERS.phone] || ""));
    if (rowPhone && rowPhone === normalized) {
      return { row: item.row, rowObj: item.obj };
    }
  }

  const newRow = sheet.getLastRow() + 1;
  const totalColumns = sheet.getLastColumn();
  const blank = new Array(totalColumns).fill("");
  sheet.getRange(newRow, 1, 1, blank.length).setValues([blank]);

  updateRowFields_(sheet, newRow, {
    [HEADERS.phone]: phoneRaw,
    [HEADERS.response_status]: "",
    [HEADERS.mailshake_status]: "N",
    [HEADERS.auto_reply_count]: 0,
    [HEADERS.human_override]: "FALSE",
    [HEADERS.history_json]: "[]"
  });

  const refreshedData = getSheetData_(sheet);
  for (const item of refreshedData) {
    const rowPhone = normalizePhone_(String(item.obj[HEADERS.phone] || ""));
    if (rowPhone && rowPhone === normalized && item.row === newRow) {
      return { row: item.row, rowObj: item.obj };
    }
  }

  throw new Error("Failed to create or find row for phone: " + phoneRaw);
}

function updateRowFields_(sheet, row, updates) {
  const headers = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];

  headers.forEach((header, idx) => {
    if (Object.prototype.hasOwnProperty.call(updates, header)) {
      sheet.getRange(row, idx + 1).setValue(updates[header]);
    }
  });
}

function syncWarmInfoOpportunityRows_(sheet, email, rowObj, latestInbound) {
  const normalizedEmail = normalizeEmailAddress_(email);
  if (!normalizedEmail) return;

  const agentMessages = [];
  getHistoryArray_(rowObj && rowObj[HEADERS.history_json]).forEach(function(entry) {
    if (!entry || entry.role !== "agent") return;
    const value = normalizeWhitespace_(String(entry.text || ""));
    if (value && agentMessages.indexOf(value) === -1) agentMessages.push(value);
  });

  const latest = normalizeWhitespace_(String(latestInbound || ""));
  if (latest && agentMessages.indexOf(latest) === -1) agentMessages.push(latest);
  const responseSummary = agentMessages.slice(-6).join(" ").slice(0, 1000) || latest;
  const updates = {
    [HEADERS.response_status]: responseSummary,
    [HEADERS.mailshake_status]: "O",
    [HEADERS.conversation_summary]: "Current opportunity declined or covered; agent requested information for future opportunities",
    [HEADERS.ai_state]: "done",
    [HEADERS.call_booking_status]: "warm_future_interest",
    [HEADERS.handoff_flag]: "FALSE"
  };

  getSheetData_(sheet).forEach(function(item) {
    const rowEmail = normalizeEmailAddress_(item.obj && item.obj[HEADERS.email]);
    if (rowEmail === normalizedEmail) updateRowFields_(sheet, item.row, updates);
  });
}

function appendHistory_(sheet, row, entry) {
  const headers = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];
  const historyCol = headers.indexOf(HEADERS.history_json) + 1;
  if (historyCol < 1) throw new Error("history_json column not found");

  const current = sheet.getRange(row, historyCol).getValue();
  const arr = getHistoryArray_(current);
  arr.push(entry);
  sheet.getRange(row, historyCol).setValue(JSON.stringify(arr.slice(-20)));
}

function getHistoryArray_(value) {
  if (!value) return [];
  try {
    const arr = JSON.parse(value);
    return Array.isArray(arr) ? arr : [];
  } catch (e) {
    return [];
  }
}

function normalizePhone_(phone) {
  return String(phone || "").replace(/\D/g, "").replace(/^1(?=\d{10}$)/, "");
}

function normalizeWhitespace_(s) {
  return s.replace(/\s+/g, " ").trim();
}

function jsonOutput_(obj) {
  return ContentService
    .createTextOutput(JSON.stringify(obj))
    .setMimeType(ContentService.MimeType.JSON);
}

function testSmsIntentContractV3_() {
  const baseRow = {};
  baseRow[HEADERS.mailshake_status] = "N";
  baseRow[HEADERS.history_json] = "[]";
  baseRow[HEADERS.last_outbound_text] = "";
  baseRow[HEADERS.ai_state] = "active";
  baseRow[HEADERS.call_booking_status] = "";

  const cases = [];
  function record(name, passed, details) {
    cases.push({ name: name, passed: !!passed, details: details || "" });
  }

  const compound = applyFastRules_(
    "I already have someone. What do you do, where are you located, and what is your fee?",
    baseRow
  );
  record(
    "safe_compound_gets_bounded_answer",
    compound.matched && !compound.handoff_needed &&
      compound.reply_text.indexOf("Atlanta") !== -1 &&
      compound.reply_text.indexOf("lender-side") !== -1 &&
      compound.reply_text.indexOf("flat fee") !== -1,
    compound.reason
  );

  const firstFee = applyFastRules_("What do you charge?", baseRow);
  record(
    "fee_tier_one",
    firstFee.matched && !firstFee.handoff_needed &&
      firstFee.reply_text.indexOf("flat fee") !== -1 &&
      firstFee.reply_text.indexOf("$5,000") === -1,
    firstFee.reason
  );

  const feeRow = Object.assign({}, baseRow);
  feeRow[HEADERS.history_json] = JSON.stringify([{ role: "assistant", text: firstFee.reply_text }]);
  const secondFee = applyFastRules_("Right, but how much is the fee exactly?", feeRow);
  record(
    "fee_tier_two",
    secondFee.matched && !secondFee.handoff_needed && secondFee.reply_text.indexOf("$5,000") !== -1,
    secondFee.reason
  );

  const negotiation = applyFastRules_("Can you match the $3,995 fee I pay now?", baseRow);
  record(
    "fee_negotiation_handoff",
    negotiation.matched && negotiation.handoff_needed && negotiation.block_reply && !negotiation.reply_text,
    negotiation.reason
  );

  const selfHandler = applyFastRules_(
    "I am handling it myself, but could you explain some more details?",
    baseRow
  );
  record(
    "self_handler_question",
    selfHandler.matched && selfHandler.lead_status === "Y" &&
      selfHandler.reply_text.indexOf("lender side of the short sale") !== -1,
    selfHandler.reason
  );

  const noCurrentHelp = applyFastRules_(
    "Hi there, actually I do not have anyone helping starting process",
    baseRow
  );
  record(
    "no_current_help_gets_conversational_reply",
    noCurrentHelp.matched && noCurrentHelp.lead_status === "Y" &&
      !noCurrentHelp.handoff_needed &&
      noCurrentHelp.reply_text.indexOf("lender paperwork, calls, follow-up") !== -1 &&
      noCurrentHelp.reply_text.indexOf("briefly by phone") !== -1,
    noCurrentHelp.reason
  );

  const emailRequestRow = Object.assign({}, baseRow);
  emailRequestRow[HEADERS.email] = "ashley@example.com";
  const emailRequest = applyFastRules_(
    "Are you able to email me ashley@example.com",
    emailRequestRow
  );
  record(
    "provided_email_sets_durable_workflow_flag",
    emailRequest.matched && emailRequest.send_info_email === true &&
      emailRequest.info_email_to === "ashley@example.com" &&
      shouldSendInfoEmail_(emailRequest, emailRequest),
    emailRequest.reason
  );

  const source = applyFastRules_("Why did you think it was a short sale?", baseRow);
  record(
    "short_sale_source",
    source.matched && source.lead_status === "R" &&
      source.reply_text.indexOf("marked online") !== -1,
    source.reason
  );

  const futureInterest = applyFastRules_(
    "I have these handled, but future short sales are stacking up and I am interested in what you have to say.",
    baseRow
  );
  record(
    "future_interest_not_source_question",
    futureInterest.matched && futureInterest.lead_status !== "R" &&
      futureInterest.reply_text.indexOf("marked online") === -1,
    futureInterest.reason
  );

  const reviewOffer = applyFastRules_(
    "I am handling it myself, but I am willing to review what you offer.",
    baseRow
  );
  record(
    "review_offer_not_website_reviews",
    reviewOffer.matched && reviewOffer.reply_text.indexOf("crispshortsales.com") === -1 &&
      reviewOffer.reply_text.indexOf("lender") !== -1,
    reviewOffer.reason
  );

  const feePercent = applyFastRules_("What percentage do you collect? My current company takes 1%.", baseRow);
  record(
    "fee_percentage_not_performance_stats",
    feePercent.matched && !feePercent.handoff_needed && feePercent.reply_text.indexOf("flat fee") !== -1,
    feePercent.reason
  );

  const coveredExperience = applyFastRules_(
    "I already have help, but how long have you been doing short sales?",
    baseRow
  );
  record(
    "covered_experience_preserves_o",
    coveredExperience.matched && coveredExperience.lead_status === "O" &&
      coveredExperience.conversation_done === true,
    coveredExperience.reason
  );

  const coveredTimeline = applyFastRules_(
    "I already have help, but how long does the process take?",
    baseRow
  );
  record(
    "covered_timeline_preserves_o",
    coveredTimeline.matched && coveredTimeline.lead_status === "O" &&
      coveredTimeline.conversation_done === true,
    coveredTimeline.reason
  );

  const directHelp = applyFastRules_("I need help", baseRow);
  record(
    "direct_help_acknowledges_and_hands_off",
    directHelp.matched && directHelp.handoff_needed && !directHelp.block_reply &&
      directHelp.reply_text.indexOf("love to help") !== -1,
    directHelp.reason
  );

  const submitOffer = applyFastRules_("Submit your offer subject to inspection", baseRow);
  record(
    "offer_submission_scope_guard_and_handoff",
    submitOffer.matched && submitOffer.handoff_needed && !submitOffer.block_reply &&
      submitOffer.send_reply_before_handoff && shouldSendBotReply_(submitOffer, false) &&
      submitOffer.handoff_type === "OFFER SUBMISSION REQUEST" &&
      submitOffer.reply_text.indexOf("don't represent a buyer") !== -1,
    submitOffer.reason
  );

  const weekendCallback = applyFastRules_("Lets talk after the weekend", baseRow);
  record(
    "relative_weekend_callback",
    weekendCallback.matched && weekendCallback.handoff_needed &&
      weekendCallback.call_booking_status === "scheduled_callback" &&
      weekendCallback.callback_time === "After The Weekend",
    weekendCallback.reason
  );

  const unavailableUntilMonday = applyFastRules_("I will be out until Monday", baseRow);
  record(
    "unavailable_until_asks_for_time",
    unavailableUntilMonday.matched && !unavailableUntilMonday.handoff_needed &&
      unavailableUntilMonday.call_booking_status === "interested_no_call" &&
      unavailableUntilMonday.reply_text === "No problem. What time Monday works best for a quick call?",
    unavailableUntilMonday.reason
  );

  return {
    ok: cases.every(function(item) { return item.passed; }),
    cases: cases
  };
}

function testIncomingSmsManual() {
  const testEvent = {
    postData: {
      contents: JSON.stringify({
        token: PropertiesService.getScriptProperties().getProperty("ALLOWED_TOKEN"),
        action: "incoming_sms",
        phone: "4043169725",
        message: "What do you charge?",
        received_at: "2026-03-29 1:30 PM",
        message_id: "manual-test-001"
      })
    }
  };

  const result = doPost(testEvent);
  Logger.log(result.getContent());
}

function resetTestConversation() {
  const phoneRaw = "4043169725";
  const sheet = getSheet_();
  const data = getSheetData_(sheet);
  const normalizedTarget = normalizePhone_(phoneRaw);

  for (const item of data) {
    const rowPhone = normalizePhone_(String(item.obj[HEADERS.phone] || ""));
    if (rowPhone === normalizedTarget) {
      updateRowFields_(sheet, item.row, {
        [HEADERS.response_status]: "",
        [HEADERS.mailshake_status]: "N",
        [HEADERS.last_inbound_text]: "",
        [HEADERS.last_inbound_at]: "",
        [HEADERS.last_outbound_text]: "",
        [HEADERS.conversation_summary]: "",
        [HEADERS.ai_state]: "",
        [HEADERS.last_contact_time]: "",
        [HEADERS.call_booking_status]: "",
        [HEADERS.handoff_flag]: "FALSE",
        [HEADERS.history_json]: "[]",
        [HEADERS.auto_reply_count]: 0,
        [HEADERS.human_override]: "FALSE",
        [HEADERS.last_message_id]: ""
      });
    }
  }
}

function inspectTestNumber() {
  const phoneRaw = "4043169725";
  const sheet = getSheet_();
  const data = getSheetData_(sheet);
  const normalizedTarget = normalizePhone_(phoneRaw);
  const matches = [];

  for (const item of data) {
    const rowPhone = normalizePhone_(String(item.obj[HEADERS.phone] || ""));
    if (rowPhone === normalizedTarget) {
      matches.push({
        row: item.row,
        phone: item.obj[HEADERS.phone],
        response_status: item.obj[HEADERS.response_status],
        mailshake_status: item.obj[HEADERS.mailshake_status],
        auto_reply_count: item.obj[HEADERS.auto_reply_count],
        handoff_flag: item.obj[HEADERS.handoff_flag],
        last_message_id: item.obj[HEADERS.last_message_id],
        last_inbound_text: item.obj[HEADERS.last_inbound_text],
        last_outbound_text: item.obj[HEADERS.last_outbound_text]
      });
    }
  }

  Logger.log(JSON.stringify(matches, null, 2));
}

function testApprovedLeadIntelligenceRules_() {
  if (!isOptOutSignal_("Stop. Already have a company.") ||
      !isOptOutSignal_("Please remove my info") ||
      isOptOutSignal_("Please stop by the office tomorrow")) {
    throw new Error("Compound opt-out classification regression");
  }

  const transportParsing = testSmsTransportParsing_();
  const receiptLeaseIdentity = testSmsReceiptLeaseIdentity_();
  const selfDecision = applyFastRules_("I have been handling that part myself", {});
  if (!selfDecision.matched || selfDecision.lead_status !== "Y" || selfDecision.conversation_done || selfDecision.handoff_needed) {
    throw new Error("Self-handling opportunity regression: " + JSON.stringify(selfDecision));
  }
  if (selfDecision.reply_text.indexOf("I understand") !== 0 || selfDecision.reply_text.indexOf("I help a lot of agents in the same situation") === -1 || selfDecision.reply_text.indexOf("Would you be open to a quick call about this file?") === -1) {
    throw new Error("Self-handling acknowledgement reply regression: " + JSON.stringify(selfDecision));
  }

  const politeSelfHandlingText = "Thank you for reaching out, I'm handling it myself";
  const politeSelfHandlingDecision = applyFastRules_(politeSelfHandlingText, {});
  if (isClearNoSignal_(politeSelfHandlingText) || !politeSelfHandlingDecision.matched || politeSelfHandlingDecision.lead_status !== "Y" || politeSelfHandlingDecision.conversation_done || politeSelfHandlingDecision.handoff_needed) {
    throw new Error("Polite-preamble self-handling regression: " + JSON.stringify(politeSelfHandlingDecision));
  }
  if (isSelfHandlingOpportunitySignal_("Hello, I am doing it myself and I am pretty good at it. But thank you for the offer.")) {
    throw new Error("Explicit polite rejection must not use the value-response rule");
  }

  const spacedSelfHandlingDecision = applyFastRules_("Sorry, I am handling that part my self", {});
  if (!spacedSelfHandlingDecision.matched || spacedSelfHandlingDecision.lead_status !== "Y" || spacedSelfHandlingDecision.conversation_done || spacedSelfHandlingDecision.handoff_needed) {
    throw new Error("Spaced my-self normalization regression: " + JSON.stringify(spacedSelfHandlingDecision));
  }

  if (!isCredentialQuestionSignal_("Are you an attorney?") || isCredentialQuestionSignal_("I already have an attorney")) {
    throw new Error("Attorney credential-question classification regression");
  }
  if (buildCredentialQuestionReply_().indexOf("not an attorney") === -1 || buildCredentialQuestionReply_().indexOf("don't provide legal advice") === -1) {
    throw new Error("Attorney credential reply regression");
  }
  if (!isNegotiatorRoleQuestionSignal_("So a negotiator?") ||
      !isNegotiatorRoleQuestionSignal_("Are you a short sale negotiator?") ||
      isNegotiatorRoleQuestionSignal_("I already have a negotiator")) {
    throw new Error("Negotiator role-question classification regression");
  }
  if (isAlreadyHandledSignal_("So a negotiator?") ||
      !isAlreadyHandledSignal_("I already have a negotiator")) {
    throw new Error("Negotiator question versus already-handled regression");
  }
  if (buildNegotiatorRoleQuestionReply_().indexOf("essentially that's what I do") === -1 ||
      buildNegotiatorRoleQuestionReply_().indexOf("Is there a good time for me to call?") === -1) {
    throw new Error("Negotiator role reply regression");
  }
  if (!isCurrentTextingNumberQuestionSignal_("Will do. Is this the best number to reach you?") ||
      !isCurrentTextingNumberQuestionSignal_("Is this your direct number?") ||
      !isCurrentTextingNumberQuestionSignal_("Can I call or text you here?") ||
      isCurrentTextingNumberQuestionSignal_("Is 305-555-0100 the best number to reach me?")) {
    throw new Error("Current texting-number question classification regression");
  }
  const closedNumberDecision = applyFastRules_(
    "Will do. Is this the best number to reach you?",
    { [HEADERS.mailshake_status]: "R" }
  );
  if (!closedNumberDecision.matched ||
      closedNumberDecision.reply_text !== "Yes, this number is great - call or text anytime. Thanks!" ||
      closedNumberDecision.lead_status !== "R" ||
      !closedNumberDecision.conversation_done ||
      /\d{3}[-.)\s]*\d{3}[-.\s]*\d{4}/.test(closedNumberDecision.reply_text)) {
    throw new Error("Current texting-number reply regression: " + JSON.stringify(closedNumberDecision));
  }
  const nameAndNumberDecision = applyFastRules_("Can you send me your name and number?", {});
  if (!isYoniNameAndNumberRequestSignal_("Can you send me your name and number?") ||
      !nameAndNumberDecision.matched ||
      nameAndNumberDecision.reply_text !== "Yoni Kutler - 404-300-9526. You can call or text anytime." ||
      nameAndNumberDecision.lead_status !== "Y" ||
      nameAndNumberDecision.conversation_done ||
      nameAndNumberDecision.handoff_needed ||
      nameAndNumberDecision.needs_review ||
      nameAndNumberDecision.block_reply) {
    throw new Error("Yoni name-and-number reply regression: " + JSON.stringify(nameAndNumberDecision));
  }
  if (isYoniNameAndNumberRequestSignal_("I can send you the buyer's name and number tomorrow")) {
    throw new Error("Buyer contact sharing must not match Yoni's name-and-number request rule");
  }
  if (!lastOutboundWasYoniNameAndNumberReply_({ [HEADERS.last_outbound_text]: buildYoniNameAndNumberReply_() })) {
    throw new Error("Yoni name-and-number courtesy closeout regression");
  }
  const informationCourtesyDecision = applyFastRules_("Thank you for your information", {
    [HEADERS.mailshake_status]: "N"
  });
  if (!isCourtesyInformationAcknowledgmentSignal_("Thank you for your information") ||
      isPlainContactInfoRequestSignal_("Thank you for your information") ||
      !isPlainContactInfoRequestSignal_("Please send me your contact information") ||
      !informationCourtesyDecision.matched ||
      !informationCourtesyDecision.block_reply ||
      !informationCourtesyDecision.preserve_existing_state ||
      informationCourtesyDecision.reply_text) {
    throw new Error("Courtesy information acknowledgment regression: " + JSON.stringify(informationCourtesyDecision));
  }
  const experienceQuestion = "Hi there, thanks for reaching out. What is your fee. I've closed them before too. How long have you handled short sales, what is your track record?";
  const experienceDecision = applyFastRules_(experienceQuestion, {});
  if (!isExperienceTrackRecordQuestionSignal_(experienceQuestion) ||
      isStatsOrNumericClaimQuestion_(experienceQuestion) ||
      !experienceDecision.matched ||
      experienceDecision.reply_text !== buildExperienceTrackRecordReply_() ||
      experienceDecision.lead_status !== "Y" ||
      experienceDecision.conversation_done ||
      experienceDecision.handoff_needed) {
    throw new Error("Experience and track-record reply regression: " + JSON.stringify(experienceDecision));
  }
  if (isExperienceTrackRecordQuestionSignal_("What is your success rate and average closing timeline?") ||
      !isStatsOrNumericClaimQuestion_("What is your success rate and average closing timeline?")) {
    throw new Error("Unsupported performance-stat question must still hand off");
  }
  const timelineQuestion = "What is the minimum time to stop foreclosure?";
  const timelineDecision = applyFastRules_(timelineQuestion, {});
  if (!isShortSaleTimelineQuestionSignal_(timelineQuestion) ||
      !timelineDecision.matched ||
      timelineDecision.reply_text !== buildShortSaleTimelineReply_() ||
      timelineDecision.handoff_needed ||
      timelineDecision.block_reply) {
    throw new Error("Approved short-sale timeline reply regression: " + JSON.stringify(timelineDecision));
  }
  const mixedStatsTimelineDecision = applyFastRules_("What is your success rate and average closing timeline?", {});
  if (!mixedStatsTimelineDecision.matched ||
      !mixedStatsTimelineDecision.handoff_needed ||
      !mixedStatsTimelineDecision.block_reply ||
      mixedStatsTimelineDecision.reply_text) {
    throw new Error("Mixed timeline and unsupported stats must still hand off: " + JSON.stringify(mixedStatsTimelineDecision));
  }
  if (!isClearNoSignal_("Thank you for reaching out, I'm handling it myself, but no thank you")) {
    throw new Error("Explicit self-handling rejection must still close out");
  }
  if (!isClearNoSignal_("I don't need help but thanks for reaching out?")) {
    throw new Error("Explicit no-help rejection must use the standard closeout");
  }
  if (!isPunctuationCorrectionFragment_("! Not ?") || isPunctuationCorrectionFragment_("No, what is your fee?")) {
    throw new Error("Punctuation-only correction suppression regression");
  }

  const directBankDecision = applyFastRules_("Hi, I am communicating with the bank directly", {});
  if (!directBankDecision.matched || directBankDecision.lead_status !== "Y" || directBankDecision.conversation_done || directBankDecision.handoff_needed) {
    throw new Error("Direct-bank self-handling regression: " + JSON.stringify(directBankDecision));
  }
  if (isSelfHandlingOpportunitySignal_("I am communicating with the bank directly, but how much is your fee?")) {
    throw new Error("A substantive fee question must outrank the self-handling value-response rule");
  }

  const repeatDecision = applyRepeatGuard_(selfDecision, { [HEADERS.last_outbound_text]: selfDecision.reply_text }, "I handle it myself usually");
  if (repeatDecision.lead_status !== "R" || !repeatDecision.conversation_done || repeatDecision.handoff_needed || repeatDecision.block_reply) {
    throw new Error("Repeated self-handling closeout regression: " + JSON.stringify(repeatDecision));
  }

  const repeatedCloseoutReply = getStandardNoCloseoutReply_();
  const preservedRepeatDecision = applyRepeatGuard_({
    reply_text: repeatedCloseoutReply,
    lead_status: "Y",
    conversation_done: false,
    handoff_needed: false,
    needs_review: false,
    block_reply: false
  }, {
    [HEADERS.last_outbound_text]: repeatedCloseoutReply,
    [HEADERS.mailshake_status]: "R",
    [HEADERS.ai_state]: "done",
    [HEADERS.call_booking_status]: "closed_no_interest",
    [HEADERS.handoff_flag]: "FALSE",
    [HEADERS.human_override]: "FALSE"
  }, "It's not a true short sale. But thank you.");
  if (!preservedRepeatDecision.preserve_existing_state || preservedRepeatDecision.lead_status !== "R" || !preservedRepeatDecision.conversation_done || preservedRepeatDecision.handoff_needed || !preservedRepeatDecision.block_reply) {
    throw new Error("State-preserving repeat suppression regression: " + JSON.stringify(preservedRepeatDecision));
  }

  const substantiveRepeatDecision = applyRepeatGuard_({
    reply_text: "There is no cost to you or the seller. We get paid by the buyer at closing, and charge a flat fee for our service.",
    lead_status: "Y",
    conversation_done: false,
    handoff_needed: false,
    needs_review: false,
    block_reply: false
  }, {
    [HEADERS.last_outbound_text]: "There is no cost to you or the seller. We get paid by the buyer at closing, and charge a flat fee for our service."
  }, "How is the buyer going to pay if they are losing money?");
  if (!substantiveRepeatDecision.handoff_needed || !substantiveRepeatDecision.block_reply || substantiveRepeatDecision.reply_text !== "" || substantiveRepeatDecision.handoff_type !== "SUBSTANTIVE QUESTION FOLLOW-UP") {
    throw new Error("Substantive repeated-answer handoff regression: " + JSON.stringify(substantiveRepeatDecision));
  }

  const underControlText = "Thank you right now I have everything under control but will reach out to you if I need further assistance";
  const underControlDecision = applyFastRules_(underControlText, {});
  if (!underControlDecision.matched || underControlDecision.lead_status !== "R" || !underControlDecision.conversation_done || underControlDecision.handoff_needed || underControlDecision.block_reply) {
    throw new Error("Under-control closeout regression: " + JSON.stringify(underControlDecision));
  }
  const underControlCallbackText = "I have everything under control, but can you call me tomorrow at 3?";
  if (isUnderControlFutureHelpCloseoutSignal_(underControlCallbackText) || !isSchedulingSignal_(underControlCallbackText)) {
    throw new Error("Real callback request must outrank the under-control closeout rule");
  }
  const deferredContactText = "Im working right now in my other job I let you know in the afternoon";
  const deferredContactDecision = applyFastRules_(deferredContactText, {});
  if (!isSelfInitiatedDeferredContactSignal_(deferredContactText) ||
      !deferredContactDecision.matched ||
      deferredContactDecision.handoff_type !== "DEFERRED HOT LEAD" ||
      deferredContactDecision.call_booking_status !== "interested_no_call" ||
      !deferredContactDecision.alert_needed ||
      !deferredContactDecision.send_reply_before_handoff ||
      deferredContactDecision.callback_time ||
      deferredContactDecision.reply_text !== "No problem - message me when you're free.") {
    throw new Error("Busy self-initiated follow-up must stay deferred without callback permission: " + JSON.stringify(deferredContactDecision));
  }
  if (isSelfInitiatedDeferredContactSignal_("I'm busy; please call me tomorrow afternoon")) {
    throw new Error("Explicit callback request must not match self-initiated deferred contact");
  }
  const weekdayCallbackText = "Feel free to reach out to me Monday. Today isn't a good day";
  if (!isExplicitDayOrDateCallbackSignal_(weekdayCallbackText) ||
      !isSchedulingSignal_(weekdayCallbackText) ||
      extractScheduledCallbackReference_(weekdayCallbackText) !== "Monday") {
    throw new Error("Explicit weekday callback classification regression");
  }
  if (!isSchedulingSignal_("Not tomorrow, please call me Monday") ||
      extractScheduledCallbackReference_("Not tomorrow, please call me Monday") !== "Monday") {
    throw new Error("A positive weekday callback must outrank a rejected earlier day");
  }
  if (isExplicitDayOrDateCallbackSignal_("Please don't call me Monday") ||
      isExplicitDayOrDateCallbackSignal_("I have an open house Monday") ||
      isExplicitDayOrDateCallbackSignal_("Call the lender Monday") ||
      isExplicitDayOrDateCallbackSignal_("Let's talk to the lender tomorrow")) {
    throw new Error("Weekday mention without a callback request must not schedule a callback");
  }
  const tomorrowCallbackText = "Let's set up a time to talk tomorrow. Let me know what time works best for you.";
  if (!isExplicitDayOrDateCallbackSignal_(tomorrowCallbackText) ||
      !isSchedulingSignal_(tomorrowCallbackText) ||
      extractScheduledCallbackReference_(tomorrowCallbackText) !== "Tomorrow") {
    throw new Error("Natural-language tomorrow callback classification regression");
  }
  const weekendCallbackText = "Lets talk after the weekend";
  if (!isExplicitDayOrDateCallbackSignal_(weekendCallbackText) ||
      !isSchedulingSignal_(weekendCallbackText) ||
      extractScheduledCallbackReference_(weekendCallbackText) !== "After The Weekend") {
    throw new Error("Relative post-weekend callback classification regression");
  }
  const declarativeCallbackText = "We can talk sometime next week";
  if (!isExplicitDayOrDateCallbackSignal_(declarativeCallbackText) ||
      !isSchedulingSignal_(declarativeCallbackText) ||
      extractScheduledCallbackReference_(declarativeCallbackText) !== "Next Week") {
    throw new Error("Declarative callback classification regression");
  }
  const unavailableUntilMondayDecision = applyFastRules_("I will be out until Monday", {});
  if (!isUnavailableUntilCallbackReferenceSignal_("I will be out until Monday") ||
      unavailableUntilMondayDecision.reply_text !== "No problem. What time Monday works best for a quick call?" ||
      unavailableUntilMondayDecision.call_booking_status !== "interested_no_call") {
    throw new Error("Unavailable-until callback-time question regression");
  }
  const guardedPromise = enforceDurableFollowupPromiseRule_({
    reply_text: "No problem, I'll check back Monday.",
    lead_status: "Y",
    conversation_done: false,
    handoff_needed: false,
    needs_review: false,
    block_reply: false
  }, "I will be out until Monday");
  if (guardedPromise.reply_text !== "No problem. What time Monday works best for a quick call?" ||
      guardedPromise.call_booking_status !== "interested_no_call" || guardedPromise.callback_time) {
    throw new Error("Unscheduled bot follow-up promise guard regression");
  }
  const postHandoffCallbackRow = {
    [HEADERS.ai_state]: "handoff",
    [HEADERS.call_booking_status]: "interested_no_call",
    [HEADERS.handoff_flag]: "TRUE",
    [HEADERS.human_override]: "TRUE"
  };
  if (!isPostHandoffCallbackUpdate_(postHandoffCallbackRow, "Afternoon on Monday would work better") ||
      extractScheduledCallbackReference_("Afternoon on Monday would work better") !== "Monday Afternoon" ||
      !isPostHandoffCallbackUpdate_(postHandoffCallbackRow, "Can we push it into next week?") ||
      extractScheduledCallbackReference_("Can we push it into next week?") !== "Next Week" ||
      isPostHandoffCallbackUpdate_(postHandoffCallbackRow, "I have an open house Monday") ||
      isPostHandoffCallbackUpdate_({ [HEADERS.human_override]: "FALSE" }, "Monday afternoon works better")) {
    throw new Error("Post-handoff callback update regression");
  }
  const monthWindowUpdateText = "Let's focus for the first week of September";
  if (!isPostHandoffCallbackUpdate_(postHandoffCallbackRow, monthWindowUpdateText) ||
      extractScheduledCallbackReference_(monthWindowUpdateText) !== "First Week Of September") {
    throw new Error("Month-window callback refinement regression");
  }
  const terminalRejectionText = "I just don't think a buyer will go for this. They aren't going to pay extra for a short sale.";
  if (!isUnmistakableTerminalRejectionSignal_(terminalRejectionText) ||
      isUnmistakableTerminalRejectionSignal_("I don't think a buyer will pay extra. Can you explain the fee?")) {
    throw new Error("Post-handoff terminal rejection regression");
  }
  const handledDuplicateText = "I have someone thank you";
  const handledDuplicateRow = {
    [HEADERS.last_inbound_text]: "i have someone thank you",
    [HEADERS.response_status]: handledDuplicateText,
    [HEADERS.ai_state]: "done",
    [HEADERS.history_json]: JSON.stringify([
      { role: "agent", text: handledDuplicateText, ts: "2026-08-12T10:05:00-04:00" },
      { role: "assistant", text: getStandardNoCloseoutReply_(), ts: "2026-08-12T10:07:00-04:00" }
    ])
  };
  if (!isDurableHandledDuplicateInbound_(handledDuplicateRow, "  I HAVE someone thank you  ") ||
      isDurableHandledDuplicateInbound_(handledDuplicateRow, "I have someone, but what do you charge?")) {
    throw new Error("Durable handled-inbound duplicate regression");
  }
  const coalescedDuplicateText = "I have someone already I have someone already I have someone already";
  const coalescedDuplicateRow = {
    [HEADERS.last_inbound_text]: coalescedDuplicateText,
    [HEADERS.response_status]: coalescedDuplicateText,
    [HEADERS.ai_state]: "done",
    [HEADERS.history_json]: JSON.stringify([
      { role: "agent", text: coalescedDuplicateText, ts: "2026-08-21T11:51:00-04:00" },
      { role: "assistant", text: getStandardNoCloseoutReply_(), ts: "2026-08-21T11:53:00-04:00" }
    ])
  };
  if (canonicalizeRepeatedCompleteInboundForDedupe_(coalescedDuplicateText) !== "i have someone already" ||
      !isDurableHandledDuplicateInbound_(coalescedDuplicateRow, "I have someone already") ||
      isDurableHandledDuplicateInbound_(coalescedDuplicateRow, "I have someone already, what is your fee?")) {
    throw new Error("Coalesced complete-message replay suppression regression");
  }
  const offerSubmissionDecision = applyFastRules_("Submit your offer subject to inspection", {});
  if (!isOfferSubmissionConfusionSignal_("Submit your offer subject to inspection") ||
      !offerSubmissionDecision.handoff_needed || offerSubmissionDecision.block_reply ||
      !offerSubmissionDecision.send_reply_before_handoff || !shouldSendBotReply_(offerSubmissionDecision, false) ||
      offerSubmissionDecision.handoff_type !== "OFFER SUBMISSION REQUEST") {
    throw new Error("Offer-submission role boundary and handoff regression");
  }
  const randyDuplicateText = "That was an input error and has been corrected - this is not a short sale - but appreciate your text.";
  const randyDuplicateRow = {
    [HEADERS.last_inbound_text]: randyDuplicateText,
    [HEADERS.response_status]: randyDuplicateText,
    [HEADERS.ai_state]: "done",
    [HEADERS.history_json]: JSON.stringify([
      { role: "agent", text: randyDuplicateText, ts: "2026-08-14T17:14:00-04:00" },
      { role: "assistant", text: "Ahh, ok... thanks for letting me know. Good luck with your listing!", ts: "2026-08-14T17:16:00-04:00" }
    ])
  };
  if (!isDurableHandledDuplicateInbound_(randyDuplicateRow, randyDuplicateText)) {
    throw new Error("Answered not-short-sale replay must be durably suppressed");
  }
  const pendingDuplicateRow = Object.assign({}, handledDuplicateRow, {
    [HEADERS.ai_state]: "active",
    [HEADERS.history_json]: JSON.stringify([
      { role: "agent", text: handledDuplicateText, ts: "2026-08-12T10:05:00-04:00" }
    ])
  });
  if (isDurableHandledDuplicateInbound_(pendingDuplicateRow, handledDuplicateText)) {
    throw new Error("Unconfirmed reply must not be durably suppressed");
  }
  const callbackDuplicateText = "Afternoon on Monday would work better";
  const callbackDuplicateRow = Object.assign({}, postHandoffCallbackRow, {
    [HEADERS.last_inbound_text]: callbackDuplicateText,
    [HEADERS.response_status]: callbackDuplicateText,
    [HEADERS.history_json]: JSON.stringify([
      { role: "agent", text: callbackDuplicateText, ts: "2026-08-12T10:05:00-04:00" },
      { role: "assistant", text: "Monday afternoon works.", ts: "2026-08-12T10:06:00-04:00" }
    ])
  });
  if (isDurableHandledDuplicateInbound_(callbackDuplicateRow, callbackDuplicateText)) {
    throw new Error("Callback updates must bypass durable duplicate suppression");
  }
  if (!isClearNoSignal_("Thank you I think I have an under control")) {
    throw new Error("Under-control voice typo must be recognized as a clear closeout");
  }
  if (!isFinalCourtesyReply_("Will do 👍") || !isFinalCourtesyReply_("Sounds good! 👍") || !isFinalCourtesyReply_("Thanks 👍")) {
    throw new Error("Closed-conversation courtesy acknowledgment regression");
  }
  if (isFinalCourtesyReply_("Will do, can you send the website?")) {
    throw new Error("Substantive follow-up must not be treated as final courtesy");
  }
  const reactionTarget = getStandardNoCloseoutReply_();
  const reactionRow = { [HEADERS.last_outbound_text]: reactionTarget };
  const explicitReaction = "Liked \u201c" + reactionTarget + "\u201d";
  const strippedReaction = "to \u201c" + reactionTarget + "\u201d";
  if (!isSmsReactionToLastOutbound_(explicitReaction, reactionRow) || !isSmsReactionToLastOutbound_(strippedReaction, reactionRow)) {
    throw new Error("Reaction-to-last-outbound suppression regression");
  }
  if (canonicalizeSmsInboundDedupeMessage_(explicitReaction) !== canonicalizeSmsInboundDedupeMessage_(strippedReaction)) {
    throw new Error("Explicit and stripped reaction artifacts must share one dedupe key");
  }
  if (isSmsReactionToLastOutbound_("to schedule a call tomorrow", reactionRow)) {
    throw new Error("Ordinary substantive text must not be suppressed as a reaction");
  }
  const apostropheOutbound = "Ok, no problem. If anything changes, I'll be glad to help.";
  const apostropheLossReaction = "to \u201cOk, no problem. If anything changes, Ill be glad to help.\u201d";
  if (!isSmsReactionToLastOutbound_(apostropheLossReaction, { [HEADERS.last_outbound_text]: apostropheOutbound })) {
    throw new Error("Reaction apostrophe-loss suppression regression");
  }

  const clientConsultationText = "Let me chat with my client because I think it's best that somebody handled that on her behalf I will get back to you.";
  const clientConsultationDecision = applyFastRules_(clientConsultationText, {});
  if (!isClientConsultationInterestSignal_(clientConsultationText) ||
      !clientConsultationDecision.matched ||
      clientConsultationDecision.lead_status !== "Y" ||
      clientConsultationDecision.conversation_done ||
      clientConsultationDecision.handoff_needed ||
      clientConsultationDecision.block_reply ||
      clientConsultationDecision.reply_text !== buildClientConsultationInterestReply_()) {
    throw new Error("Client-consultation positive-intent regression: " + JSON.stringify(clientConsultationDecision));
  }
  if (isClientConsultationInterestSignal_("Let me ask my client, but no thanks, we already have someone")) {
    throw new Error("Explicit client-consultation rejection must stay a decline");
  }

  const existingCrispText = "I already have an active Crisp portal and am set up with Yoni.";
  const existingCrispDecision = applyFastRules_(existingCrispText, {});
  if (!isExistingCrispRelationshipSignal_(existingCrispText) ||
      !existingCrispDecision.matched ||
      existingCrispDecision.lead_status !== "Y" ||
      !existingCrispDecision.handoff_needed ||
      !existingCrispDecision.block_reply ||
      existingCrispDecision.reply_text !== "" ||
      existingCrispDecision.handoff_type !== "EXISTING CRISP CLIENT") {
    throw new Error("Existing Crisp client handoff regression: " + JSON.stringify(existingCrispDecision));
  }
  if (isExistingCrispRelationshipSignal_("I already have someone handling it") ||
      isExistingCrispRelationshipSignal_("What company are you with?")) {
    throw new Error("Generic handled/company text must not match an existing Crisp relationship");
  }
  const approvedCallInterestText = "I would be interested to have a call to see how your services differ from theirs.";
  const approvedCallInterestDecision = applyFastRules_(approvedCallInterestText, {
    [HEADERS.ai_state]: "done",
    [HEADERS.mailshake_status]: "R",
    [HEADERS.call_booking_status]: "closed_no_interest"
  });
  if (!isPhoneCallInterestSignal_(approvedCallInterestText) ||
      !approvedCallInterestDecision.matched ||
      !approvedCallInterestDecision.handoff_needed ||
      approvedCallInterestDecision.handoff_type !== "CALL REQUESTED") {
    throw new Error("Call interest must outrank prior closeout context: " + JSON.stringify(approvedCallInterestDecision));
  }
  const approvedServiceInterestText = "I am interested in your services and would like to learn more.";
  const approvedServiceInterestDecision = applyFastRules_(approvedServiceInterestText, {});
  if (!isPresentServiceInterestSignal_(approvedServiceInterestText) ||
      !approvedServiceInterestDecision.matched ||
      !approvedServiceInterestDecision.handoff_needed ||
      approvedServiceInterestDecision.handoff_type !== "RENEWED INTEREST" ||
      isPresentServiceInterestSignal_("No thanks, but I will keep your information in mind for the future.")) {
    throw new Error("Present service interest must reopen for handoff: " + JSON.stringify(approvedServiceInterestDecision));
  }
  const approvedCompanyIdentityText = "I already have an attorney. What company are you so I can let my attorney know?";
  const approvedCompanyIdentityDecision = applyFastRules_(approvedCompanyIdentityText, {});
  if (!isCompanyIdentityQuestionSignal_(approvedCompanyIdentityText) ||
      !isAlreadyHandledSignal_(approvedCompanyIdentityText) ||
      !approvedCompanyIdentityDecision.matched ||
      approvedCompanyIdentityDecision.reply_text.indexOf("My company is Crisp Short Sales.") !== 0) {
    throw new Error("Company identity question must outrank existing-coverage wording: " + JSON.stringify(approvedCompanyIdentityDecision));
  }
  if (buildCoveredCompanyIdentityReply_().indexOf("My company is Crisp Short Sales.") !== 0 ||
      !isClosedMarketingConversation_({ [HEADERS.ai_state]: "done" })) {
    throw new Error("Covered company-identity closeout regression");
  }
  const genericCurrentHelpText = "Hi Yoni, thank you for following up! I currently have someone assisting me with the short sale process for this property, but I appreciate you reaching out. I'll definitely keep your information for future short sale opportunities.";
  if (isExistingCrispRelationshipSignal_(genericCurrentHelpText) ||
      !isRelationshipOnlyAfterExistingCoverageSignal_(genericCurrentHelpText, {})) {
    throw new Error("Generic current-help plus future-only relationship must route to the warm closeout");
  }
  const apostropheLossRelationshipRow = {
    [HEADERS.conversation_summary]: "Already represented / handled",
    [HEADERS.history_json]: JSON.stringify([{ role: "agent", text: "I currently have someone assisting me" }])
  };
  if (!isRelationshipOnlyAfterExistingCoverageSignal_("Ill definitely keep your information for future short sale opportunities", apostropheLossRelationshipRow)) {
    throw new Error("Apostrophe-stripped relationship-only closeout regression");
  }
  const relationshipTypoText = "We are working with someone right now but I will keep you contact in case anything changes";
  const relationshipTypoDecision = applyFastRules_(relationshipTypoText, {});
  if (!isRelationshipOnlyAfterExistingCoverageSignal_(relationshipTypoText, {}) ||
      !relationshipTypoDecision.matched ||
      relationshipTypoDecision.lead_status !== "O" ||
      !relationshipTypoDecision.conversation_done ||
      relationshipTypoDecision.handoff_needed) {
    throw new Error("Common keep-you-contact typo must stay a warm closeout: " + JSON.stringify(relationshipTypoDecision));
  }
  const equatorDecision = applyFastRules_("I'm using Agent Equator. Bank never responds.", {});
  if (!isEquatorPortalSignal_("Can you help on Equator?") ||
      !equatorDecision.matched ||
      equatorDecision.reply_text !== buildEquatorPortalReply_() ||
      equatorDecision.handoff_needed ||
      !equatorDecision.bypass_reply_cap) {
    throw new Error("Equator portal expertise reply regression: " + JSON.stringify(equatorDecision));
  }
  const explicitNegativeDecision = applyFastRules_("No, thank you. Five thousand dollars is insane.", {});
  if (!isTerminalCloseoutDecision_(explicitNegativeDecision) || explicitNegativeDecision.lead_status !== "R") {
    throw new Error("Explicit rejection must remain a terminal closeout before reply-cap handling: " + JSON.stringify(explicitNegativeDecision));
  }
  const futureBuyerText = "So let you know when I eventually get a buyer?";
  const futureBuyerDecision = applyFastRules_(futureBuyerText, {});
  if (!isFutureBuyerRecontactSignal_(futureBuyerText) || !futureBuyerDecision.matched || futureBuyerDecision.lead_status !== "O" || !futureBuyerDecision.conversation_done || futureBuyerDecision.handoff_needed || futureBuyerDecision.block_reply || futureBuyerDecision.reply_text !== buildFutureBuyerRecontactReply_()) {
    throw new Error("Future-buyer recontact closeout regression: " + JSON.stringify(futureBuyerDecision));
  }
  if (!isExistingCrispRelationshipSignal_("Hi Yoni, I am currently working with you on this short sale.")) {
    throw new Error("Direct existing Yoni relationship must still trigger handoff");
  }

  if (!isSpanishLanguageSignal_("No no tengo ayuda aun hablas espaol ??")) {
    throw new Error("Spanish-language signal regression");
  }
  if (buildSpanishCapabilityReply_().indexOf("I don't speak Spanish") === -1 || /I speak Spanish/i.test(buildSpanishCapabilityReply_())) {
    throw new Error("Spanish capability truthfulness regression");
  }
  if (!isSpanishFeeQuestionSignal_("Cul sera la tarifa para el comprador")) {
    throw new Error("Spanish fee question regression");
  }

  const priorSpanishFeeRow = { [HEADERS.history_json]: JSON.stringify([
    { role: "assistant", text: "No hay costo para ti ni para el vendedor; cobramos una tarifa fija al comprador en el cierre." }
  ]) };
  const feeDecision = buildFeeQuestionDecision_(priorSpanishFeeRow, "");
  if (feeDecision.reply_text.indexOf("$5,000") === -1 || feeDecision.handoff_needed) {
    throw new Error("Prior flat-fee disclosure must trigger the specific $5,000 answer: " + JSON.stringify(feeDecision));
  }
  const mixedHandledFeeText = "I already have help, but what is the fee for using your service?";
  if (!isPaymentOrFeeQuestionSignal_(mixedHandledFeeText)) {
    throw new Error("Fee question must be recognized inside a soft-decline message");
  }
  const mixedHandledFeeDecision = applyFastRules_(mixedHandledFeeText, {});
  if (!mixedHandledFeeDecision.matched || mixedHandledFeeDecision.lead_status !== "Y" ||
      mixedHandledFeeDecision.reply_text.indexOf("flat fee to the buyer") === -1) {
    throw new Error("Fee question must outrank soft-decline wording: " + JSON.stringify(mixedHandledFeeDecision));
  }
  const rateQuestionDecision = applyFastRules_("We already have a negotiator. What's your rate?", {});
  if (!isPaymentOrFeeQuestionSignal_("What's your rate?") ||
      !rateQuestionDecision.matched ||
      rateQuestionDecision.lead_status !== "Y" ||
      rateQuestionDecision.handoff_needed ||
      rateQuestionDecision.reply_text.indexOf("flat fee to the buyer") === -1) {
    throw new Error("Rate question must use the fee reply and outrank existing-help wording: " + JSON.stringify(rateQuestionDecision));
  }

  const testimonialsDecision = applyFastRules_("Can you send testimonials?", { [HEADERS.email]: "agent@example.com" });
  if (!testimonialsDecision.matched ||
      testimonialsDecision.handoff_needed ||
      testimonialsDecision.reply_text.indexOf("crispshortsales.com") === -1 ||
      testimonialsDecision.reply_text.toLowerCase().indexOf("what is your email") !== -1) {
    throw new Error("Testimonials request must use website/reviews reply without an email prompt: " + JSON.stringify(testimonialsDecision));
  }

  const differentiationText = "How are you different from them with communication and documentation?";
  const differentiationDecision = applyFastRules_(differentiationText, {});
  if (!isDifferentiationQuestionSignal_(differentiationText) ||
      !differentiationDecision.matched ||
      differentiationDecision.handoff_needed ||
      !differentiationDecision.alert_needed ||
      differentiationDecision.handoff_type !== "HOT LEAD - DIFFERENTIATION QUESTION" ||
      differentiationDecision.reply_text !== buildDifferentiationQuestionReply_()) {
    throw new Error("Differentiation communication question regression: " + JSON.stringify(differentiationDecision));
  }

  const automatedNoticeText = "You've reached Redfin, but we actually use a different number for texting - (214) 427-8372. We'll send you a message from that number!";
  if (!isAutomatedRoutingNoticeSignal_(automatedNoticeText)) {
    throw new Error("Automated routing notice guard regression");
  }

  const notShortDecision = applyFastRules_("Sorry it was not meant to be a short sale. If I ever get one I will keep you in mind!", {});
  if (!notShortDecision.matched || notShortDecision.lead_status !== "R" || !notShortDecision.conversation_done || notShortDecision.handoff_needed) {
    throw new Error("Not-short-sale closeout regression: " + JSON.stringify(notShortDecision));
  }
  if (isNotShortSaleVagueFutureSignal_("This is not a short sale, but can you send your business card?")) {
    throw new Error("Substantive contact request must not use the vague-future closeout rule");
  }

  const listingPromotionDecision = applyFastRules_("Can you post my listing in your area?", {});
  if (!listingPromotionDecision.matched ||
      listingPromotionDecision.handoff_type !== "LISTING PROMOTION REQUEST" ||
      !listingPromotionDecision.handoff_needed ||
      !listingPromotionDecision.block_reply ||
      listingPromotionDecision.reply_text !== "") {
    throw new Error("Listing-promotion manual handoff regression: " + JSON.stringify(listingPromotionDecision));
  }
  if (!isListingPromotionRequestSignal_("Would you share it with agents in your market?") ||
      isListingPromotionRequestSignal_("Are you local to my market?") ||
      isListingPromotionRequestSignal_("Do you have a buyer?")) {
    throw new Error("Listing-promotion signal scope regression");
  }

  const result = {
    selfHandling: selfDecision,
    politeSelfHandling: politeSelfHandlingDecision,
    spacedSelfHandling: spacedSelfHandlingDecision,
    credentialQuestionReply: buildCredentialQuestionReply_(),
    directBank: directBankDecision,
    selfHandlingRepeat: repeatDecision,
    preservedRepeat: preservedRepeatDecision,
    underControl: underControlDecision,
    spanishCapability: buildSpanishCapabilityReply_(),
    spanishFee: feeDecision,
    notShortSale: notShortDecision,
    listingPromotion: listingPromotionDecision,
    approvedCallInterest: approvedCallInterestDecision,
    approvedServiceInterest: approvedServiceInterestDecision,
    approvedCompanyIdentity: approvedCompanyIdentityDecision,
    transportParsing: transportParsing,
    receiptLeaseIdentity: receiptLeaseIdentity,
    ok: true
  };
  Logger.log(JSON.stringify(result));
  return result;
}
