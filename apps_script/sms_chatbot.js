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

function historyHasConfirmedReplyAfterInbound_(rowObj, inboundText) {
  const currentText = normalizeHandledInboundText_(inboundText);
  if (!currentText) return false;

  const history = getHistoryArray_(rowObj && rowObj[HEADERS.history_json]);
  for (let i = history.length - 1; i >= 0; i -= 1) {
    const entry = history[i] || {};
    if (String(entry.role || "").toLowerCase() !== "agent" ||
        normalizeHandledInboundText_(entry.text) !== currentText) {
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
  const priorText = normalizeHandledInboundText_(rowObj && rowObj[HEADERS.last_inbound_text]);
  const currentText = normalizeHandledInboundText_(inboundText);
  if (!priorText || !currentText || priorText !== currentText) return false;
  if (isSchedulingSignal_(inboundText) || isPostHandoffCallbackUpdate_(rowObj, inboundText)) return false;

  const responseText = normalizeHandledInboundText_(rowObj && rowObj[HEADERS.response_status]);
  if (responseText !== currentText) return false;

  return historyHasConfirmedReplyAfterInbound_(rowObj, inboundText) ||
    isIntentionalNoReplyDisposition_(rowObj, inboundText);
}

function isRecentDuplicateInboundText_(rowObj, inboundText, receivedAt) {
  const priorText = normalizeHandledInboundText_(rowObj && rowObj[HEADERS.last_inbound_text]);
  const currentText = normalizeHandledInboundText_(inboundText);
  if (!priorText || !currentText || priorText !== currentText) return false;

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
  const phoneRaw = String(body.phone || "").trim();
  const inboundText = normalizeWhitespace_(String(body.message || ""));
  const messageId = String(body.message_id || "");
  const receivedAt = body.received_at || new Date().toISOString();

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

    return {
      ok: true,
      duplicate: true,
      should_reply: false,
      handoff_needed: false,
      needs_review: false,
      reason: "Inbound dedupe lock busy; duplicate reply suppressed"
    };
  }

  try {
    sheet = getSheet_();
    const data = getSheetData_(sheet);
    rowInfo = findOrCreateRowByPhone_(sheet, data, phoneRaw);
    row = rowInfo.row;
    rowObj = rowInfo.rowObj;

    if (rowObj[HEADERS.last_message_id] && rowObj[HEADERS.last_message_id] === messageId) {
      return {
        ok: true,
        duplicate: true,
        should_reply: false,
        reason: "Duplicate message_id ignored"
      };
    }

    if (isRecentDuplicateInboundText_(rowObj, inboundText, receivedAt)) {
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

    if (isDurableHandledDuplicateInbound_(rowObj, inboundText)) {
      appendSmsDebugLog_("incoming_sms_duplicate_suppressed", {
        phone: phoneRaw,
        message: inboundText,
        reason: "Durable handled inbound duplicate suppressed"
      });

      return {
        ok: true,
        duplicate: true,
        should_reply: false,
        reason: "Durable handled inbound duplicate ignored"
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

    updateRowFields_(sheet, row, {
      [HEADERS.last_inbound_text]: normalizeHandledInboundText_(inboundText),
      [HEADERS.last_inbound_at]: receivedAt,
      [HEADERS.last_contact_time]: receivedAt,
      [HEADERS.last_message_id]: messageId
    });
  } finally {
    dedupeLock.releaseLock();
  }

  appendHistory_(sheet, row, { role: "agent", text: inboundText, ts: receivedAt });

  const refreshedData = getSheetData_(sheet);
  const refreshedRowInfo = refreshedData.find(r => r.row === row);
  const currentRowObj = refreshedRowInfo ? refreshedRowInfo.obj : rowObj;
  const currentCount = Number(currentRowObj[HEADERS.auto_reply_count] || 0);
  const capReached = currentCount >= 4;
  const hasFeeQuestion = isPaymentOrFeeQuestionSignal_(inboundText);
  const hasExperienceQuestion = isExperienceTrackRecordQuestionSignal_(inboundText);
  const hasClientConsultationInterest = isClientConsultationInterestSignal_(inboundText);
  const hasExistingCrispRelationship = isExistingCrispRelationshipSignal_(inboundText);

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

  if (isSpanishLanguageSignal_(inboundText) && !isSpanishFeeQuestionSignal_(inboundText)) {
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
    const history = getHistoryArray_(currentRowObj[HEADERS.history_json]);

    if (changed) {
      sendHandoffEmail_({
        handoff_type: "CALLBACK UPDATED",
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
      handoff_type: changed ? "CALLBACK UPDATED" : "",
      reason: changed ? "Callback updated after human handoff" : "Callback timing repeated after human handoff"
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

  if (isFutureNegotiationInterestSignal_(inboundText)) {
    const history = getHistoryArray_(currentRowObj[HEADERS.history_json]);
    const reason = "Agent expressed interest in future short-sale negotiation support";
    const replyText = buildFutureInterestReply_(inboundText);

    sendHandoffEmail_({
      handoff_type: "FUTURE INTEREST",
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
      reason: reason
    };
  }

  if (
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

  if (!hasFeeQuestion && isNotShortSaleSignal_(inboundText)) {
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

  if (!hasFeeQuestion && isSelfHandlingFutureHelpSignal_(inboundText)) {
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

  if (isCredentialQuestionSignal_(inboundText)) {
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

  if (isNegotiatorRoleQuestionSignal_(inboundText)) {
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

  if (!hasFeeQuestion && !hasExperienceQuestion && !hasClientConsultationInterest && !hasExistingCrispRelationship && isAlreadyHandledSignal_(inboundText)) {
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

  if (!hasFeeQuestion && !hasExperienceQuestion && !hasClientConsultationInterest && !hasExistingCrispRelationship && isClearNoSignal_(inboundText)) {
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

  const ruleResult = applyFastRules_(inboundText, currentRowObj);
  if (ruleResult.matched) {
    let decision = normalizeAiDecision_(ruleResult, currentRowObj[HEADERS.mailshake_status]);
    decision = applyReplySanitizers_(decision, currentRowObj);
    decision = applyRepeatGuard_(decision, currentRowObj, inboundText);
    const updates = {
      [HEADERS.response_status]: inboundText,
      [HEADERS.conversation_summary]: decision.reason || ""
    };

    if (!decision.preserve_existing_state) {
      updates[HEADERS.mailshake_status] = decision.lead_status;
      updates[HEADERS.ai_state] = decision.conversation_done ? "done" : (decision.handoff_needed ? "handoff" : "active");
      updates[HEADERS.handoff_flag] = decision.handoff_needed ? "TRUE" : "FALSE";
      updates[HEADERS.human_override] = decision.handoff_needed || decision.needs_review ? "TRUE" : "FALSE";

      if (ruleResult.info_email_to && isValidEmailAddress_(ruleResult.info_email_to)) {
        updates[HEADERS.email] = ruleResult.info_email_to;
      }

      if (decision.lead_status === "G") {
        updates[HEADERS.call_booking_status] = "call_set_or_hot";
      } else if (decision.lead_status === "Y") {
        updates[HEADERS.call_booking_status] = "interested_no_call";
      } else if (decision.lead_status === "R") {
        updates[HEADERS.call_booking_status] = "closed_no_interest";
      }
    }

    updateRowFields_(sheet, row, updates);

    if (shouldSendInfoEmail_(ruleResult, decision)) {
      const infoEmailData = {
        to: ruleResult.info_email_to,
        first_name: getCanonicalFirstName_(currentRowObj),
        agent_name: currentRowObj[HEADERS.agent_name] || "",
        listing_address: currentRowObj[HEADERS.listing_address] || "",
        city: currentRowObj[HEADERS.city] || "",
        state: currentRowObj[HEADERS.state] || "",
        phone: phoneRaw,
        last_message: inboundText
      };

      if (isInfoEmailApprovalRequired_()) {
        sendInfoEmailApprovalRequest_(infoEmailData);
      } else {
        sendAgentInfoEmail_(infoEmailData);
      }
    }

    if (decision.handoff_needed || decision.needs_review) {
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

    return {
      ok: true,
      should_reply: shouldSendBotReply_(decision, capReached),
      reply_text: shouldSendBotReply_(decision, capReached) ? (decision.reply_text || "") : "",
      lead_status: decision.lead_status,
      conversation_done: !!decision.conversation_done,
      handoff_needed: !!decision.handoff_needed,
      needs_review: !!decision.needs_review,
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

  if (capReached) {
    const history = getHistoryArray_(currentRowObj[HEADERS.history_json]);

    sendHandoffEmail_({
      handoff_type: "Max Replies Reached",
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
      [HEADERS.conversation_summary]: "Max Replies Reached",
      [HEADERS.ai_state]: "handoff",
      [HEADERS.call_booking_status]: "interested_no_call",
      [HEADERS.handoff_flag]: "TRUE",
      [HEADERS.human_override]: "TRUE"
    });

    return {
      ok: true,
      should_reply: false,
      handoff_needed: true,
      needs_review: false,
      reason: "Max auto replies reached - handoff to Yoni"
    };
  }

  let decision = getAiDecision_({ row: row, rowObj: currentRowObj }, inboundText);
  decision = normalizeAiDecision_(decision, currentRowObj[HEADERS.mailshake_status]);
  decision = applyReplySanitizers_(decision, currentRowObj);
  if (containsUnsupportedStatsClaim_(decision.reply_text)) {
    decision = buildManualHandoffDecision_(
      "AI attempted to answer with unsupported stats or numeric performance claims",
      "STATS QUESTION"
    );
  }
  decision = applyRepeatGuard_(decision, currentRowObj, inboundText);

  const updates = {
    [HEADERS.response_status]: inboundText,
    [HEADERS.conversation_summary]: decision.reason || ""
  };

  if (!decision.preserve_existing_state) {
    updates[HEADERS.mailshake_status] = decision.lead_status;
    updates[HEADERS.ai_state] = decision.conversation_done ? "done" : (decision.handoff_needed ? "handoff" : "active");
    updates[HEADERS.handoff_flag] = decision.handoff_needed ? "TRUE" : "FALSE";
    updates[HEADERS.human_override] = decision.handoff_needed || decision.needs_review ? "TRUE" : "FALSE";

    if (decision.lead_status === "G") {
      updates[HEADERS.call_booking_status] = "call_set_or_hot";
    } else if (decision.lead_status === "Y") {
      updates[HEADERS.call_booking_status] = "interested_no_call";
    } else if (decision.lead_status === "R") {
      updates[HEADERS.call_booking_status] = "closed_no_interest";
    }
  }

  updateRowFields_(sheet, row, updates);

  if (decision.handoff_needed || decision.needs_review) {
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

  return {
    ok: true,
    should_reply: shouldSendBotReply_(decision, false),
    reply_text: shouldSendBotReply_(decision, false) ? (decision.reply_text || "") : "",
    lead_status: decision.lead_status,
    conversation_done: !!decision.conversation_done,
    handoff_needed: !!decision.handoff_needed,
    needs_review: !!decision.needs_review,
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

function shouldSendBotReply_(decision, capReached) {
  if (capReached) {
    return false;
  }

  const d = decision || {};
  if (d.handoff_needed || d.needs_review || d.block_reply) {
    return false;
  }

  return !!d.reply_text;
}

function handleReplySent_(body) {
  const phoneRaw = String(body.phone || "").trim();
  const replyText = normalizeWhitespace_(String(body.reply_text || ""));
  const sentAt = body.sent_at || new Date().toISOString();

  const sheet = getSheet_();
  const data = getSheetData_(sheet);
  const rowInfo = findOrCreateRowByPhone_(sheet, data, phoneRaw);

  appendHistory_(sheet, rowInfo.row, { role: "assistant", text: replyText, ts: sentAt });

  const refreshedData = getSheetData_(sheet);
  const refreshedRowInfo = refreshedData.find(r => r.row === rowInfo.row);
  const currentCount = Number((refreshedRowInfo ? refreshedRowInfo.obj[HEADERS.auto_reply_count] : rowInfo.rowObj[HEADERS.auto_reply_count]) || 0);

  updateRowFields_(sheet, rowInfo.row, {
    [HEADERS.last_outbound_text]: replyText,
    [HEADERS.last_contact_time]: sentAt,
    [HEADERS.auto_reply_count]: currentCount + 1
  });

  return { ok: true };
}

function handleManualReplySent_(body) {
  const phoneRaw = String(body.phone || "").trim();
  const replyText = normalizeWhitespace_(String(body.reply_text || body.message || ""));
  const sentAt = body.sent_at || new Date().toISOString();
  if (!normalizePhone_(phoneRaw) || !replyText) {
    throw new Error("Manual reply receipt requires phone and reply text");
  }

  const sheet = getSheet_();
  const data = getSheetData_(sheet);
  const rowInfo = findOrCreateRowByPhone_(sheet, data, phoneRaw);
  appendHistory_(sheet, rowInfo.row, { role: "assistant", text: replyText, ts: sentAt });
  updateRowFields_(sheet, rowInfo.row, {
    [HEADERS.last_outbound_text]: replyText,
    [HEADERS.last_contact_time]: sentAt
  });

  return { ok: true, row: rowInfo.row, manual: true };
}

function markOverride_(body) {
  const phoneRaw = String(body.phone || "").trim();
  const value = String(body.value || "TRUE").toUpperCase() === "TRUE" ? "TRUE" : "FALSE";

  const sheet = getSheet_();
  const data = getSheetData_(sheet);
  const rowInfo = findOrCreateRowByPhone_(sheet, data, phoneRaw);

  updateRowFields_(sheet, rowInfo.row, {
    [HEADERS.human_override]: value
  });

  return { ok: true, phone: phoneRaw, human_override: value };
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
    t.indexOf("percentage") !== -1;

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

  const hasPriorSpecificFeeReply = priorAssistantTexts.some(isSpecificFeeReplyText_);
  const hasPriorInitialFeeReply = priorAssistantTexts.some(isInitialFeeReplyText_);

  if (hasPriorSpecificFeeReply) {
    return buildManualHandoffDecision_(
      "Agent is still asking about fee/payment after the specific $5,000 answer",
      "FEE QUESTION FOLLOW-UP"
    );
  }

  if (hasPriorInitialFeeReply) {
    return {
      matched: true,
      reply_text: "The fee is $5,000, paid by the buyer at closing. As long as it's disclosed up front in the listing, the buyer can factor it into their offer price, and it's typically not an issue. Happy to walk you through exactly how that looks on this listing.",
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
    reply_text: "I don't take a percentage or anything from your commission. It's completely free to the agent and seller. I charge a flat fee to the buyer at closing, only if/when the deal closes - happy to explain the details on a quick call.",
    lead_status: "Y",
    conversation_done: false,
    handoff_needed: false,
    needs_review: false,
    block_reply: false,
    reason: "Asked about charge, fee, percentage, or how Crisp gets paid"
  };
}

function applyFastRules_(text, rowObj) {
  const t = normalizeWhitespace_(String(text || "").toLowerCase());
  const lastOutbound = normalizeWhitespace_(String(rowObj && rowObj[HEADERS.last_outbound_text] || ""));

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
    return buildManualHandoffDecision_(
      "Agent identified an active or existing Crisp/Yoni relationship; exit marketing and route to Yoni",
      "EXISTING CRISP CLIENT"
    );
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
      reply_text: "I understand, and I help a lot of agents in the same situation. I can take the lender side off your plate - the paperwork, calls, follow-up, and negotiations - so you can focus on the listing and your client. There is no cost to you or the seller. Would you be open to a quick call about this file?",
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
      reply_text: "Ok great, will give you a call shortly.",
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

  const providedEmail = extractEmailAddress_(t);
  const targetEmail = normalizeEmailAddress_(providedEmail || String(rowObj && rowObj[HEADERS.email] || ""));

  if (providedEmail || isEmailRequestSignal_(t)) {
    if (targetEmail) {
      return {
        matched: true,
        reply_text: getInfoEmailAcknowledgementReply_(),
        lead_status: "Y",
        conversation_done: false,
        handoff_needed: false,
        needs_review: false,
        block_reply: false,
        send_info_email: true,
        info_email_to: targetEmail,
        reason: providedEmail
          ? "Agent sent an email address; info email approval requested"
          : "Agent asked for info by email; info email approval requested"
      };
    }

    return {
      matched: true,
      reply_text: "sure, no problem. What is your email?",
      lead_status: "Y",
      conversation_done: false,
      handoff_needed: false,
      needs_review: false,
      block_reply: false,
      reason: "Agent asked for info by email but no email address is available yet"
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

  const companyIdentityPatterns = [
    /\bwho are you with\b/,
    /\bwhat company\b/,
    /\bwho do you work with\b/,
    /\bwho do you work for\b/,
    /\bare you a mtg broker\b/,
    /\bare you a mortgage broker\b/,
    /\bwith what company\b/
  ];

  for (const pattern of companyIdentityPatterns) {
    if (pattern.test(t)) {
      return {
        matched: true,
        reply_text: "My company is called Crisp Short Sales - and we specialize in helping agents and homeowners with the short sale process, ensuring lender approvals as quickly as possible. I have been in business over 15 years and this is all I do, help people with the short sale process. I am confident I could help you and your client too if interested. Want to chat for a few minutes about your situation?",
        lead_status: "Y",
        conversation_done: false,
        handoff_needed: false,
        needs_review: false,
        block_reply: false,
        reason: "Asked who Yoni is with / whether he is a mortgage broker"
      };
    }
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
    /\bchat\b.*\bphone\b/
  ];

  return patterns.some(pattern => pattern.test(t));
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
    return "Absolutely. There’s no cost to you or the seller. We get paid by the buyer at closing and charge a flat fee for the service. As long as that’s disclosed up front in the listing, the buyer can factor it into their offer and there’s typically no issue. If you have a few future short sales stacking up, happy to talk through how it works in detail. Want to find a time later this week to chat?";
  }

  return "Absolutely, happy to explain. I handle the bank side of the short sale process - the paperwork, calls, follow-up, and everything needed to get the file approved. There’s no cost to you or the seller, and no commission split. Want to find a time later this week to talk through how it could work for your future short sales?";
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
    return "Thanks, I appreciate it. If " + firstName + " has a few minutes to chat, just let me know a good time for me to call.";
  }

  return "Thanks, I appreciate it. If there's a good time for us to chat, just let me know and I can give you a call.";
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

  return thanksLine + " I can handle the entire short sale process for you including all the paperwork, the phone calls, and everything that goes into getting the deal approved with the lender. I will handle 100% of the short sale process for you, no matter how many liens or mortgages are on title, and the best part is there's no commission split with you or cost to your client at any point. Do you have some time for a quick chat to go over your listing specifics?";
}

function buildHowWeHelpReply_() {
  return "I can handle the entire short sale process for you - all the paperwork, the phone calls... everything that goes into getting the deal approved. It's also free to the agent and seller to work with me. There is no commission split with you and no cost to your client at any point. I get paid by charging a flat fee to the buyer at closing, and as long as you disclose this cost to them up front in the listing - they should be able to take that cost into account with their offer, and theres typically never any issue. Happy to go into more detail with you about your listing and the specifics of my fee and anything else whenever you have some time. Let me know when a good time to call is and I'll reach out. Thanks!";
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
    .replace(/(?:[\s.!?]+|[\u2600-\u27bf]|\ud83c[\udc00-\udfff]|\ud83d[\udc00-\udfff]|\ud83e[\udd00-\udfff])+$/g, "");

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
    /\b(?:currently|already)\s+have\s+(?:someone|somebody|a\s+person|a\s+company|a\s+team)\s+(?:helping|assisting|handling|working\s+on)\b/.test(t);
  if (!t || isSubstantiveFollowupSignal_(t) || (!hasPreviouslyCoveredContext_(rowObj) && !currentCoverage)) {
    return false;
  }
  const passiveRelationshipPatterns = [
    /\b(?:i|we)(?:['\u2019]?ll| will)\s+(?:keep|save|hold onto)\s+(?:your|ur)\s+(?:info|information|contact|number|details)\b/,
    /\b(?:keep|save|hold onto)\s+(?:your|ur)\s+(?:info|information|contact|number|details)\b/,
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
  return "Absolutely. I can handle the entire short sale process for you and your client - all the paperwork, lender calls, follow-up, and negotiations needed to get the file approved. There's no cost to you or the seller. I'm happy to speak with either of you whenever it works for you.";
}

function isExistingCrispRelationshipSignal_(text) {
  const t = normalizeWhitespace_(String(text || "").toLowerCase());
  if (!t) return false;

  const directRelationshipPatterns = [
    /\b(?:i|we)(?:['\u2019]m| am|['\u2019]re| are)?\s+(?:already|currently)\s+(?:working|set\s*up|signed\s*up|registered)\s+with\s+(?:you|yoni|crisp(?: short sales?)?)\b/,
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
    /\bwhat\s+(?:percent|percentage)\b/,
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
  return "I've been doing this for over 15 years, and this is really all I do - help agents and homeowners with the short sale process. I'm confident I can help you and your clients with these deals and get them to closing as quickly as possible. Do you have some time today for a quick call so I can answer any questions you have?";
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
  const stateRaw = normalizeWhitespace_(String(rowObj && rowObj[HEADERS.state] || "")).toUpperCase();
  const stateNames = { TX: "Texas", FL: "Florida", GA: "Georgia", CA: "California", CO: "Colorado", AZ: "Arizona", NC: "North Carolina", SC: "South Carolina", TN: "Tennessee", VA: "Virginia", WA: "Washington", HI: "Hawaii", MI: "Michigan", AK: "Alaska" };
  const stateName = stateNames[stateRaw] || "";
  const stateSentence = stateName ? " I have a lot of experience working in " + stateName + "." : "";
  return "I'm actually located in Atlanta, GA but work all over the country. The short sale process is the same everywhere and the banks are located throughout the country as well." + stateSentence;
}

function isContactCardRequestSignal_(text) {
  const t = normalizeWhitespace_(String(text || "").toLowerCase());
  const patterns = [
    /\b(?:send|share|text|email)\b.*\b(?:business\s+card|contact\s+card|contact\s+info|vcard)\b/,
    /\b(?:business\s+card|contact\s+card|vcard)\b/,
    /\bcontact\s+information\b/
  ];
  return patterns.some(pattern => pattern.test(t));
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

function buildCredentialQuestionReply_() {
  return "No, I'm not an attorney. I specialize in helping agents and homeowners with the short sale process. I don't provide legal advice; title and the closing attorney handle the closing. I handle the process of obtaining the bank's approval of the deal.";
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
  return "Yes, essentially that's what I do. I handle the short sale process and negotiate with the lender to get the deal approved. Happy to explain it and answer any questions over the phone. Is there a good time for me to call?";
}

function isAlreadyHandledSignal_(text) {
  if (isNegotiatorRoleQuestionSignal_(text)) return false;
  const t = normalizeWhitespace_(String(text || "").toLowerCase());
  const patterns = [
    /\blawyer\b/,
    /\battorney\b/,
    /\bnegotiator\b/,
    /\bshort sale negotiator\b/,
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
    /\bno thanks\b/,
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

  const asksSource = t.indexOf("why") !== -1 || t.indexOf("where") !== -1 || t.indexOf("what") !== -1;
  const sourceWords = ["think", "thought", "believe", "assume", "say", "said", "marked", "listed", "showing", "showed", "see", "saw", "find", "found", "get", "got"];

  return asksSource && sourceWords.some(word => t.indexOf(word) !== -1);
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
    /\btomorrow\b|\bnext\s+week\b|\b(?:(?:this|next|coming)\s+)?(?:monday|tuesday|wednesday|thursday|friday|saturday|sunday)\b|\b(?:january|february|march|april|may|june|july|august|september|october|november|december)\s+\d{1,2}(?:st|nd|rd|th)?(?:,\s*\d{4})?\b|\b\d{1,2}[\/-]\d{1,2}(?:[\/-]\d{2,4})?\b/i
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

function normalizeCallbackTime_(value) {
  return normalizeWhitespace_(String(value || "")).toLowerCase();
}

function isCallbackUpdateTiming_(text) {
  const t = normalizeWhitespace_(String(text || "").toLowerCase());
  if (!t || !extractScheduledCallbackReference_(t)) return false;
  return /\b(?:would|will|works?|work)\s+(?:be\s+)?(?:better|best|good|fine|ok|okay)\b/.test(t) ||
    /\b(?:push|move|reschedule|switch|change)\b.{0,40}\b(?:to|into|for|on)\b/.test(t) ||
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
            lead_status: { type: "string", enum: ["R", "Y", "G", "N"] },
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
  normalized.handoff_needed = !!normalized.handoff_needed || normalized.lead_status === "G" || !!normalized.needs_review;
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
  if (existing === "R" || existing === "Y" || existing === "G") {
    return existing;
  }
  return "Y";
}

function coerceRespondedLeadStatus_(candidateStatus, existingStatus) {
  const candidate = String(candidateStatus || "").toUpperCase();
  if (candidate === "R" || candidate === "Y" || candidate === "G") {
    return candidate;
  }
  return getRespondedLeadStatusFallback_(existingStatus);
}

function buildSystemPrompt_(rowObj) {
  const yourName = PropertiesService.getScriptProperties().getProperty("YOUR_NAME") || "Yoni";
  const agentFirstName = getCanonicalFirstName_(rowObj);

  return `
You are texting as ${yourName}, who helps agents with short sale processing, lender negotiations, approvals, and getting deals to closing.

You are not a chatbot. You are a real person texting.

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
- If they ask if you are local, say you are based in Atlanta, work nationwide, and mention relevant state experience if it is obvious from the listing or conversation
- If they say they are handling it themselves but are willing to review what you have to offer, explain the service directly in the text and ask for a quick call
- If they say the offer was accepted and ask what you do or how you help, congratulate them briefly, ask whether the short sale still needs lender approval, explain that I can handle the approval and closing work, and end with: \"Let me know if you want to find a time to talk it over.\"
- Never say \"Want me to take the file?\" or ask whether I should take the file; that is not how \${yourName} talks
- In that situation, explain that you can handle the entire short sale process including paperwork, phone calls, lender approval work, difficult title situations, and that there is no commission split or cost to their client
- If they ask "How do you help?" or anything similar about what you actually do, explain that you handle the entire short sale process including the paperwork, the phone calls, and everything needed to get the deal approved
- In that situation, explain that it is free to the agent and seller, there is no commission split, you charge a flat fee to the buyer at closing, and as long as that cost is disclosed up front in the listing there is typically never any issue
- End that kind of reply by inviting them to pick a time for a call so you can go into the listing specifics and the fee in more detail
- The goal of the conversation is always to move toward a phone conversation with ${yourName} when appropriate
- If a conversation needs manual follow-up from ${yourName}, do not send a text reply to the agent
- In any manual handoff situation, leave reply_text empty, set block_reply = true, and let ${yourName} take over
- If they ask whether this is AI, a bot, automated, actually your phone, or whether they are texting a real person, do not reply
- In that situation, set handoff_needed = true, block_reply = true, leave reply_text empty, and let ${yourName} respond personally
- If they sound open to future short-sale negotiation help, distressed-property support, or future work together, treat it as an interested lead, answer naturally, and ask for a time to talk
- If they say they already have the current file handled but ask for your information, fee, or what you offer for future short sales, do not close them out and do not go silent
- In that situation, explain the flat-fee buyer-paid structure briefly and ask if they want to find a time later that week to talk
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
- Even if a front desk person or gatekeeper replies, thank them briefly and ask for a good time for ${yourName} to call the agent directly
- If anyone asks ${yourName} to meet in person, drop by the office, or come by the office, do not respond with availability and do not set the meeting yourself
- In that situation, set handoff_needed = true, block_reply = true, and let ${yourName} respond manually
- If they send an email address or ask you to email them info and the email address is available in the message or row, reply exactly: "Sure, no problem."
- If they ask you to email them info and no email address is available yet, reply exactly: "sure, no problem. What is your email?"
- If they ask what you charge, what percentage you get, or how the fee works, say this and do not improvise numbers:
  "There is no cost to you or the seller in this deal. We get paid by the buyer at closing, and charge a flat fee for our service. As long as you disclose this cost up front in the listing - the buyer should be able to take that into account with their offer price and then theres usually never any issue. If you want, we can hop on a quick call and ill explain all the specifics to you."
- Never mention 1%, fee ranges, commission split percentages, or any made-up pricing details
- If they ask how long I have handled short sales, how much experience I have, how many short sales I have handled, or what my track record is, answer with the approved 15-plus-year experience response and invite a quick call. Do not treat that as a rejection or a stats handoff.
- Never provide or invent success rates, approval rates, close rates, closing rates, timelines, percentages, averages, or other unapproved performance stats
- If they ask for a success rate, approval rate, close rate, percentage, average timeline, or another unapproved performance statistic, set handoff_needed = true, block_reply = true, leave reply_text empty, and let ${yourName} answer personally
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
    return "Ok great, will give you a call shortly.";
  }

  return text;
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

  MailApp.sendEmail({
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
    !decision.needs_review &&
    normalizeWhitespace_(decision.reply_text) === normalizeWhitespace_(getInfoEmailAcknowledgementReply_())
  );
}

function getInfoEmailAcknowledgementReply_() {
  return "Absolutely, I'll put some information together and email it over. I'd also be happy to talk whenever you have time.";
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
  const props = PropertiesService.getScriptProperties();
  const toEmail = props.getProperty("INFO_EMAIL_APPROVAL_TO") || props.getProperty("HANDOFF_EMAIL") || "yoni.kutler@ygkutler.com";
  const agentName = getHandoffDisplayName_(data);
  const approvalId = createInfoEmailApproval_(data);
  const approvalUrl = buildInfoEmailApprovalUrl_(approvalId);
  const subject = "APPROVE INFO EMAIL - " + agentName + " - " + getStreetNameForInfoEmail_(data && data.listing_address);
  const body = `
An agent requested the short-sale info email. Approval is required before sending.

Approve and send:
${approvalUrl}

Agent: ${agentName}
Email: ${data.to || ""}
Phone: ${formatPhoneForEmail_(data.phone)}
Property: ${formatPropertyAddressForEmail_(data)}

Last message:
${data.last_message || ""}

Draft subject:
${buildAgentInfoEmailSubject_(data)}

Draft body:
${buildAgentInfoEmailBody_(data)}
`.trim();

  MailApp.sendEmail({
    to: toEmail,
    subject: subject,
    body: body
  });
}

function sendAgentInfoEmail_(data) {
  const toEmail = normalizeEmailAddress_(data && data.to);
  if (!toEmail) {
    throw new Error("Missing valid agent info email recipient");
  }

  return sendAgentInfoEmailViaBackend_({
    to: toEmail,
    subject: buildAgentInfoEmailSubject_(data),
    body: buildAgentInfoEmailBody_(data)
  });
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

  return baseUrl + "?action=approve_info_email&id=" + encodeURIComponent(approvalId);
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
  const data = parsed && parsed.data ? parsed.data : null;
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
  return "Crisp Short Sales - How I Can Help";
}

function getStreetNameForInfoEmail_(listingAddress) {
  const firstPart = String(listingAddress || "").split(",")[0].trim();
  return firstPart || "Your Listing";
}

function buildAgentInfoEmailBody_(data) {
  const firstName = getAgentInfoEmailFirstName_(data);

  return `
Hi ${firstName},

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
    return "there";
  }

  const withoutTitle = rawName.replace(/^(mr|mrs|ms|miss|dr)\.?\s+/i, "");
  const firstToken = withoutTitle.split(/\s+/)[0] || "";
  return firstToken.replace(/[^A-Za-z'-]/g, "") || "there";
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
    transportParsing: transportParsing,
    receiptLeaseIdentity: receiptLeaseIdentity,
    ok: true
  };
  Logger.log(JSON.stringify(result));
  return result;
}
