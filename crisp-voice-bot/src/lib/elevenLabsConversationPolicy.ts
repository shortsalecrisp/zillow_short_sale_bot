export const VOICE_CONVERSATION_POLICY_VERSION = "listening-clarification-20260913";

type AgentPolicy = {
  conversation_config: {
    agent: { first_message?: string; disable_first_message_interruptions?: boolean; [key: string]: unknown };
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
    "After callback_requested confirms the request was recorded, say: \"I've noted your callback request. Thanks.\" Do not claim an appointment, delivery, or a guaranteed time. On an error or unconfirmed record, say you could not confirm the request.",
    "Listen for corrections before ending. Never ask a question and call end_call in the same turn.",
  ].join("\n");
  return updated;
}
