export const VOICE_CONVERSATION_POLICY_VERSION = "permission-screener-20260918";

type AgentPolicy = {
  conversation_config: {
    agent: {
      first_message?: string;
      disable_first_message_interruptions?: boolean;
      prompt?: { built_in_tools?: Record<string, { description?: string; [key: string]: unknown } | null>; [key: string]: unknown };
      [key: string]: unknown;
    };
    turn: { transcribe_on_disabled_interruptions?: boolean; [key: string]: unknown };
    conversation: { client_events?: string[]; [key: string]: unknown };
    [key: string]: unknown;
  };
  workflow?: {
    nodes?: Record<string, {
      conversation_config?: { conversation?: { client_events?: string[]; [key: string]: unknown }; [key: string]: unknown };
      additional_prompt?: string;
      edge_order?: string[];
      [key: string]: unknown;
    }>;
    edges?: Record<string, Record<string, unknown>>;
    [key: string]: unknown;
  };
  [key: string]: unknown;
};

export function applyConversationListeningPolicy<T extends AgentPolicy>(agent: T, options: { listenFirst: boolean }): T {
  const updated = structuredClone(agent);
  const main = updated.workflow?.nodes?.main_conversation;
  if (!main) throw new Error("Main conversation workflow node is required for the listening policy");

  if (options.listenFirst) updated.conversation_config.agent.first_message = "";
  updated.conversation_config.agent.disable_first_message_interruptions = false;
  updated.conversation_config.turn.transcribe_on_disabled_interruptions = true;
  const events = updated.conversation_config.conversation.client_events ?? [];
  updated.conversation_config.conversation.client_events = [...new Set([...events, "interruption"])];
  main.conversation_config ??= {};
  main.conversation_config.conversation ??= {};
  const mainEvents = main.conversation_config.conversation.client_events ?? events;
  main.conversation_config.conversation.client_events = [...new Set([...mainEvents, "interruption"])];
  return updated;
}

export function applyConversationToolPolicy<T extends AgentPolicy>(agent: T, options: { guardedEnding?: boolean } = {}): T {
  const updated = structuredClone(agent);
  const builtins = updated.conversation_config.agent.prompt?.built_in_tools;
  if (!builtins?.skip_turn || (!builtins.end_call && !options.guardedEnding)) throw new Error("Verified end-call and skip-turn tools are required");
  if (builtins.end_call) builtins.end_call.description = [
    "End the current call after a NEW clear live-caller goodbye, rejection, or request to stop or end, following the base contact-preference policy; also use the existing completed-voicemail ending.",
    "A callback_requested or information_requested tool success is not permission to end. After its brief receipt acknowledgment, wait for a NEW caller turn.",
    "Thanks, okay, sounds good, a repeated callback time, an email-only preference, a correction, a question, or silence alone is not a farewell. Never end over an unanswered question or changed preference.",
    "Honor explicit opt-outs and current-call stop requests promptly. Do not prolong the pitch. Use a brief neutral goodbye without promising future contact or claiming suppression was saved unless confirmed.",
  ].join(" ");
  if (options.guardedEnding) {
    builtins.end_call = null;
    builtins.transfer_to_number = null;
  }
  builtins.skip_turn.description = [
    "Wait silently instead of speaking for placeholder silence, background noise, static, breathing, a recording still playing, or an explicit instruction to hold or stay on the line while the phone reaches a person.",
    "Spoken automated hold or connecting words are still a reason to wait. Do not treat them as a live greeting or pitch over them. Continue waiting until a new live person answers or a clear voicemail greeting begins.",
    "Do not use this instead of answering an automated screener's name-and-reason request: speak the exact base-prompt screener sentence first, then wait.",
    "Do not skip a NEW or still-unanswered live person's question, correction, request to stop, or completed greeting. Follow the base prompt for that turn.",
    ...(options.guardedEnding ? [
      "Once the required brief answer or receipt acknowledgment has already been given, wait silently when no NEW or still-unanswered caller question, correction, or request remains.",
      "After a denied or failed ending check, do not replay the already-handled ending request: handle any genuinely pending caller turn, otherwise wait silently. A tool result or placeholder is not a new caller turn.",
      "Do not repeat an answer or acknowledgment, announce an ending-check failure, or invent a generic response while waiting. Handle any genuinely new unanswered caller turn before waiting again.",
    ] : []),
  ].join(" ");
  return updated;
}

export function applyContactToolDescriptions<T extends {
  name: string;
  type: string;
  description?: string;
  parameters?: any;
  api_schema?: { request_body_schema?: any; [key: string]: unknown };
}>(tool: T): T {
  const updated = structuredClone(tool);
  const schema = updated.type === "client" ? updated.parameters : updated.type === "webhook" ? updated.api_schema?.request_body_schema : null;
  if (!schema?.properties || !["callback_requested", "information_requested", "not_interested"].includes(updated.name)) {
    throw new Error("Verified callback, information, or contact-outcome tool schema is required");
  }
  if (updated.name === "callback_requested") {
    if (!schema.properties.callbackTime) throw new Error("Callback time schema is required");
    if (!schema.properties.conversationSummary) throw new Error("Callback request summary schema is required");
    updated.description = "Record a callback request only when a live caller explicitly asks Yoni to call them, or clearly accepts a single callback offer. Busy, unavailable, a question, email-only, silence, or the caller planning to call us is not callback consent. Capture requested timing without inventing it. This records a request, not a booked appointment.";
    schema.description = "Record the live caller's explicitly requested callback using dynamic call metadata; do not schedule or guarantee an appointment.";
    schema.properties.callbackTime.description = "Copy the caller's requested timing words verbatim. Do not convert words to digits, add an unspoken AM or PM, resolve a relative day into a date, or expand or substitute a time zone. Leave missing details missing and note uncertainty separately in conversationSummary for human review. For corrections, copy the corrective words and retain earlier supplied context in conversationSummary. Use ASAP only when the caller actually requested or agreed to that timing. Yoni being unavailable is never permission to infer ASAP or create a callback.";
    schema.properties.conversationSummary.description = "Preserve the caller's actual callback request, supplied timing and latest corrective words. Retain earlier supplied day, time zone, person or number context when a correction is partial. Note unresolved timing for human review without adding an unspoken AM or PM, date or time zone. Do not leave out supplied context or uncertainty to save time, and do not claim an appointment or delivery is confirmed.";
  } else if (updated.name === "information_requested") {
    if (!schema.properties.conversationSummary) throw new Error("Information request summary schema is required");
    updated.description = "Record a live caller's request for information by email. Once their address is caller-confirmed or clearly supplied, execute this tool before acknowledging receipt. Confirm a stored address with the caller once; do not repeat an address they just clearly supplied. If it is missing or unclear, ask only for the address or missing part first. A verbal reassurance is not execution. Do not claim receipt without a successful requestCaptured result or promise sending or delivery. Information-only is not callback or live-transfer consent.";
    schema.description = "Submit the caller's information request with the caller-confirmed or clearly supplied email address; a stored address alone is not confirmation. This captures a request, not a sent or delivered email. Do not substitute a spoken promise for the tool call.";
    schema.properties.conversationSummary.description = "Preserve the caller's actual request, email-only preference, supplied address and any corrections or unanswered questions. Do not invent callback consent, a scheduled action, sending, delivery, or a receipt from a tool that did not return confirmation.";
  } else {
    if (!schema.properties.conversationSummary) throw new Error("Contact summary schema is required");
    updated.description = "Record a live caller's clear rejection, future-contact opt-out, request to end only the current call, choice to initiate future contact themselves, or correction that this is not a short sale. Use the base prompt's distinct outcome markers. A question alone is not a contact outcome; preserve any question accompanying a genuine contact preference or listing correction so it can still be answered. Do not use for a recording, temporary busyness, self-handling alone, email-only or unclear speech. Deferred self-contact and listing corrections are not automatically rejection or permission to end. The backend determines the outcome from attributable caller evidence; a tool call is not proof of a saved opt-out or CRM correction.";
    schema.description = "Submit the caller's actual contact preference or not-short-sale correction for classification, preserving accompanying questions. Ending only this call, deferred self-contact and listing corrections do not automatically establish rejection or future-contact suppression. Recording alone never authorizes ending.";
    schema.properties.conversationSummary.description = "Preserve the live caller's actual words, unanswered questions, restrictions and earlier genuine interest. Distinguish current-call-only ending, clear rejection, future-contact opt-out, deferred self-contact and a not-short-sale correction. Use the base prompt CALL ENDED BY REQUEST, DO NOT CALL or DEFERRED CONTACT marker only for the matching actual intent; retain not a short sale for that actual listing correction. Do not invent intent, promise a CRM correction, erase prior genuine interest or convert caller-initiated future contact into callback consent.";
  }
  return updated;
}

export function applyConversationConsentPolicy<T extends AgentPolicy>(agent: T): T {
  const updated = structuredClone(agent);
  const workflow = updated.workflow;
  const patch = workflow?.nodes?.patch_transfer;
  const fallback = workflow?.nodes?.callback_after_unavailable;
  const edges = workflow?.edges;
  if (!patch || !fallback || !edges?.patch_to_phone || !edges.main_to_patch_after_accepted_result) {
    throw new Error("Verified warm-transfer workflow is required for the consent policy");
  }

  patch.additional_prompt = [
    "You are in the live-transfer patching step. Check the latest caller turn before proceeding.",
    "If the caller asks a new question, says wait, corrects a preference, wants a callback instead, or withdraws consent, do not patch the phone. Return to the main conversation to handle that turn first.",
    "Only when the latest live_transfer_requested result explicitly has transferApproved true AND approvalStatus accepted, the caller still clearly wants Yoni on this call now, and no question is pending, say exactly: \"Ok good news, I've got him. Patching him in now.\"",
    "HTTP success, in_progress, missing approval fields, declined, timeout and call_failed do not mean Yoni is available. Return to main without the patch line for those results.",
    "Do not restart the availability check or duplicate a tool call. A provider approval alone is not continuing caller consent.",
  ].join("\n");
  const events = updated.conversation_config.conversation.client_events ?? [];
  for (const node of [patch, fallback]) {
    node.conversation_config ??= {};
    node.conversation_config.conversation ??= {};
    node.conversation_config.conversation.client_events = [...new Set([
      ...(node.conversation_config.conversation.client_events ?? events), "interruption",
    ])];
  }
  // The provider uses a single edge for both directions between a node pair.
  edges.main_to_patch_after_accepted_result.backward_condition = {
      type: "llm", label: null,
      condition: "Return to main_conversation before phone transfer if the latest tool result lacks BOTH transferApproved true and approvalStatus accepted, or the latest caller turn contains a new unanswered question, says wait, changes preference, or withdraws live-transfer consent. This has priority over patch_to_phone.",
  };
  patch.edge_order = ["main_to_patch_after_accepted_result", "patch_to_phone"];
  edges.patch_to_phone = {
    ...edges.patch_to_phone,
    forward_condition: {
      ...(edges.patch_to_phone.forward_condition as Record<string, unknown> ?? {}),
      type: "llm",
      condition: "Route to phone_transfer only after a live_transfer_requested result has BOTH transferApproved true and approvalStatus accepted, the assistant said it is patching him in, the caller still clearly wants a live transfer now, and no newer question, wait request, correction, callback preference, or withdrawal is pending. Never use a stale approval after consent changes.",
    },
  };
  edges.main_to_patch_after_accepted_result = {
    ...edges.main_to_patch_after_accepted_result,
    forward_condition: {
      ...(edges.main_to_patch_after_accepted_result.forward_condition as Record<string, unknown> ?? {}),
      type: "llm",
      condition: "Route to patch_transfer only when the latest live_transfer_requested result has BOTH approvalStatus accepted and transferApproved true AND the caller still clearly wants Yoni now. Stay in main for any unanswered question, wait request, changed preference or revoked consent. A past approval does not override the latest caller turn.",
    },
  };
  edges.main_to_callback_after_unavailable_result = {
    ...edges.main_to_callback_after_unavailable_result,
    forward_condition: {
      ...(edges.main_to_callback_after_unavailable_result.forward_condition as Record<string, unknown> ?? {}),
      type: "llm",
      condition: "Route to callback_after_unavailable only once for a fresh live_transfer_requested result with transferApproved false and an explicit terminal unavailable, declined, timeout, call_failed, or error status. Do not route on in_progress or missing fields. Stay in main if a caller question, changed preference, or request to stop is pending. Do not re-enter from an old result after handling the caller's choice.",
    },
  };
  // Tool success must pass through business-result validation, not phone patching.
  workflow.nodes!.transfer_result_review = {
    type: "override_agent", label: "Review Transfer Result",
    position: { x: 560, y: -240 }, parent_subgraph_id: null,
    conversation_config: {}, additional_prompt: "Do not speak or call a tool in this routing step. Return to the main conversation to review the availability result and the latest caller turn before any phone patch.",
    additional_knowledge_base: [], additional_tool_ids: [], forced_tool_name: null,
    entry_behavior: "auto", edge_order: ["transfer_result_to_main"],
  };
  edges.transfer_result_to_main = {
    source: "transfer_result_review", target: "main_conversation",
    forward_condition: { type: "unconditional", label: null }, backward_condition: null,
  };
  for (const [name, target] of Object.entries({
    transfer_check_to_patch: "transfer_result_review",
    transfer_check_to_callback: "callback_after_unavailable",
  })) {
    if (!edges[name]) throw new Error("Verified transfer result edges are required");
    edges[name] = { ...edges[name], target };
  }
  const destinations = Object.values(edges).map(edge => [edge.source, edge.target].sort().join(":"));
  if (new Set(destinations).size !== destinations.length) throw new Error("Duplicate workflow edge destinations are not supported");
  fallback.additional_prompt = [
    "You are in the live-transfer unavailable fallback step. Latest questions, corrections and requests to stop take priority.",
    "If the caller has not chosen another next step, say once: \"He isn't available right now. Would you like me to request a callback?\" Then wait.",
    "Only a clear yes to that callback offer allows asking their preferred time. Thanks, a question, silence, or a correction is not consent. Use callback_requested only after a clear callback request; do not assume asap.",
    "If they ask a question, answer it briefly and wait. Do not create a callback merely because you answered a question.",
    "Respect an email-only request, self-initiated future contact, rejection, opt-out, or current-call-only ending using the base prompt rules. Do not offer another live transfer.",
    "After callback_requested returns requestCaptured true for this request, say only: \"Thanks. I've received your callback request.\" A generic success or queued result is not a booked appointment or delivery. On an error or unconfirmed record, say you could not confirm the request.",
    "Then wait for a NEW caller turn without an anything-else question. A repeated time, correction, callback-only preference, thanks or tool completion alone is not permission to end. Use the same guarded-ending rule as the main conversation.",
  ].join("\n");
  return updated;
}
