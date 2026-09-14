import { isDeepStrictEqual } from "node:util";

export const TERMINAL_GUARD_NODES = Object.freeze({
  recordContact: "terminal_guard_record_contact",
  reset: "terminal_guard_reset", validate: "terminal_guard_validate",
  failure: "terminal_guard_failure", awaitCaller: "terminal_guard_await_caller",
  wait: "terminal_guard_wait", end: "terminal_guard_end",
});
export const TERMINAL_PERMISSION_VARIABLE = "terminal_permission";
export const TERMINAL_DECISION_VARIABLE = "terminal_decision";

type WorkflowBody = {
  conversation_config: { agent: { prompt: any; dynamic_variables?: any; [key: string]: any }; [key: string]: any };
  workflow: { nodes: Record<string, any>; edges: Record<string, any>; [key: string]: any };
  [key: string]: any;
};
type Options = { resetToolId: string; validateToolId: string; contactToolId: string; mainNodeId: string };

function removeModelEndCall(prompt: any): void {
  prompt.built_in_tools ??= {};
  prompt.built_in_tools.end_call = null;
  if (Array.isArray(prompt.tools)) prompt.tools = prompt.tools.filter((tool: any) =>
    tool.name !== "end_call" && tool.params?.system_tool_type !== "end_call");
}

export function applyGuardedTerminalWorkflow<T extends WorkflowBody>(body: T, options: Options): T {
  const { resetToolId, validateToolId, contactToolId, mainNodeId } = options;
  const nativeOnlyIds = new Set([resetToolId, validateToolId, contactToolId]);
  if (![...nativeOnlyIds].every(id => /^tool_[a-z0-9]+$/.test(id)) || nativeOnlyIds.size !== 3) {
    throw new Error("Three distinct verified contact/reset/validation tool IDs are required");
  }
  const next = structuredClone(body), workflow = next.workflow, n = TERMINAL_GUARD_NODES;
  if (!next.conversation_config?.agent?.prompt || !workflow?.edges || workflow.nodes?.[mainNodeId]?.type !== "override_agent"
    || Object.values(n).some(id => id === mainNodeId)) throw new Error("An existing main conversation override node is required");
  const nodes = workflow.nodes, edges = workflow.edges;
  const globalIds = next.conversation_config.agent.prompt.tool_ids;
  if (!Array.isArray(globalIds) || globalIds.filter(id => id === contactToolId).length !== 1) {
    throw new Error("Verified contact recording tool must remain registered exactly once globally");
  }
  const ownedIds = new Set<string>(Object.values(n));
  const modelContexts = Object.entries(nodes).filter(([id, node]) => !ownedIds.has(id) && node.type === "override_agent");
  for (const [, node] of modelContexts) if (node.forced_tool_name === "end_call" || node.system_tool?.name === "end_call"
    || node.system_tool?.params?.system_tool_type === "end_call") throw new Error("Existing forced terminal context requires an explicit recording-path review");
  removeModelEndCall(next.conversation_config.agent.prompt);
  for (const [, node] of modelContexts) {
    node.conversation_config ??= {}; node.conversation_config.agent ??= {}; node.conversation_config.agent.prompt ??= {};
    removeModelEndCall(node.conversation_config.agent.prompt);
    const nodePrompt = node.conversation_config.agent.prompt;
    const visibleIds = nodePrompt.tool_ids ?? globalIds;
    if (!Array.isArray(visibleIds) || (node.additional_tool_ids != null && !Array.isArray(node.additional_tool_ids))) {
      throw new Error("Explicit model tool lists must be arrays");
    }
    // The global list is a registry; each reachable model receives an explicit restricted list.
    nodePrompt.tool_ids = visibleIds.filter((id: string) => !nativeOnlyIds.has(id));
    node.additional_tool_ids = (node.additional_tool_ids ?? []).filter((id: string) => !nativeOnlyIds.has(id));
    if (Array.isArray(nodePrompt.tools)) nodePrompt.tools = nodePrompt.tools.filter((tool: any) =>
      !["not_interested", "reset_call_end_permission", "validate_call_ending"].includes(tool.name));
  }
  const currentTurnInstruction = "The guarded-ending decision variable is {{terminal_decision}}. When returning from a just-completed validation with pending_question_or_correction, answer or handle the current unanswered live question or correction now. Do not re-enter the guard for that same caller turn, even when the base prompt requests a diagnostic check. Do not infer this decision from a model-visible tool result: assigned fields may be removed there. An older decision does not replace the actual content of a new caller turn.";
  if (!nodes[mainNodeId].additional_prompt?.includes(currentTurnInstruction)) {
    nodes[mainNodeId].additional_prompt = [nodes[mainNodeId].additional_prompt, currentTurnInstruction].filter(Boolean).join("\n\n");
  }
  const dialogueIds = new Set([mainNodeId, "callback_after_unavailable", "patch_transfer", "opening_listener"]);
  const routeOnly = nodes.transfer_result_review;
  if (routeOnly) {
    const outgoing = Object.values(edges).filter((value: any) => value.source === "transfer_result_review");
    if (routeOnly.entry_behavior !== "auto" || outgoing.length !== 1 || outgoing[0].target !== mainNodeId
      || outgoing[0].forward_condition?.type !== "unconditional" || outgoing[0].backward_condition != null) {
      throw new Error("Transfer result router must return unconditionally to the main conversation");
    }
    routeOnly.conversation_config.agent.prompt.tool_ids = [];
    delete routeOnly.conversation_config.agent.prompt.tools;
    routeOnly.additional_tool_ids = [];
  }
  if (modelContexts.some(([id]) => !dialogueIds.has(id) && id !== "transfer_result_review")) {
    throw new Error("Unreviewed model context requires explicit contact routing");
  }
  const contexts = modelContexts.filter(([id]) => dialogueIds.has(id));
  const contactCondition = { type: "llm", condition:
    "Record a LIVE caller's new explicit future opt-out, request to stop this call, clear refusal of the service, correction that this is not a short sale, or statement that they will initiate future contact. Use this contact-recording path before any closing speech; it automatically records the actual request and checks whether ending is permitted. Preserve questions, restrictions and earlier genuine interest. Do not record the same caller turn twice, including after a failed or denied ending check. A callback-only or email-only preference, a declined transfer, self-handling alone, a service question or a genuine goodbye without another contact preference does not require this path. Never use it for a recording, screening, hold or background speech." };
  const endingCondition = { type: "llm", condition:
    "A NEW live caller turn contains a genuine goodbye and no unanswered question, correction, revocation or request to wait. Use the ending-permission guard before any closing speech. If a new opt-out, current-call stop, service refusal or other contact outcome needs recording, use the contact-recording path instead; it includes these ending checks. Never repeat a failed or denied ending check for the same caller turn. A callback-only confirmation, email-only preference, thanks, okay, silence or tool result is not a goodbye. Do not use this for voicemail, screening, hold, recorded-message completion or phone handoff." };
  const recoveryPrompt = "You are a silent call-recovery router, not the sales assistant. Never produce spoken text, a goodbye, an acknowledgment, a pitch, a tool-failure explanation or a presence check. A still-unanswered live question, correction or changed request belongs in the main conversation, including content in the turn that started the check or arriving during it. The already-processed stop and tool results are not new caller turns. Otherwise wait silently. Silence, noise and placeholder ... require skip_turn, not speech. Do not infer consent, repeat a contact action or end the call.";
  const recoveryNode = (label: string, order: string[], wait = false) => ({
    type: "override_agent", label, position: { x: 1500, y: wait ? 600 : 350 },
    parent_subgraph_id: null, forced_tool_name: null,
    entry_behavior: wait ? "wait_for_user" : "auto",
    additional_prompt: "", additional_knowledge_base: [], additional_tool_ids: [], edge_order: order,
    conversation_config: { agent: { first_message: "", prompt: {
      prompt: recoveryPrompt, tool_ids: [], tools: [], knowledge_base: [],
      built_in_tools: { end_call: null, transfer_to_number: null, transfer_to_agent: null,
        voicemail_detection: null, language_detection: null, play_keypad_touch_tone: null,
        skip_turn: { ...structuredClone(next.conversation_config.agent.prompt.built_in_tools.skip_turn),
          description: "Wait silently. Tool results, an already-processed stop, silence, noise and placeholder ... require no spoken acknowledgment." } },
    } } },
  });
  // The provider derives the expression prompt from the schema description on readback.
  const unansweredPrompt = "Return true only if an actual live caller has a still-unanswered question, correction, revocation or other substantive request. Inspect both the turn that initiated the failed or denied check and any speech arriving during it. An already-processed stop or goodbye, a tool result, silence, noise or placeholder ... alone is false. Do not infer interest or consent. Return only a boolean.";
  const unanswered = { type: "expression", expression: { type: "llm", value_schema: {
    type: "boolean", description: unansweredPrompt, enum: null }, prompt: unansweredPrompt } };

  const edge = (source: string, target: string, condition: any, backward: any = null) => ({ source, target,
    forward_condition: { ...condition, label: null }, backward_condition: backward ? { ...backward, label: null } : null });
  const result = (successful: boolean) => ({ type: "result", successful });
  const expression = (value: boolean) => ({ type: "expression", expression: { type: "eq_operator",
    left: { type: "dynamic_variable", name: TERMINAL_PERMISSION_VARIABLE }, right: { type: "boolean_literal", value } } });
  const pendingQuestion = { type: "expression", expression: { type: "and_operator", children: [expression(false).expression,
    { type: "eq_operator", left: { type: "dynamic_variable", name: TERMINAL_DECISION_VARIABLE },
      right: { type: "string_literal", value: "pending_question_or_correction" } }] } };
  const toolNode = (toolId: string, x: number, y: number, order: string[]) => ({
    type: "tool", position: { x, y }, parent_subgraph_id: null, system_tool: null,
    tools: [{ tool_id: toolId, schema_overrides: null }], edge_order: order,
  });
  const guardNodes: Record<string, any> = {
    [n.recordContact]: toolNode(contactToolId, 750, 0, ["terminal_contact_to_reset"]),
    [n.reset]: toolNode(resetToolId, 1000, 0, ["terminal_reset_failure", "terminal_reset_is_false", "terminal_reset_not_false"]),
    [n.validate]: toolNode(validateToolId, 1500, 0, ["terminal_validate_failure", "terminal_permission_true", "terminal_pending_question", "terminal_denied_wait"]),
    [n.failure]: recoveryNode("Route After Ending Check Failure", ["terminal_failure_pending", "terminal_failure_wait"]),
    [n.awaitCaller]: recoveryNode("Route After Denied Ending", ["terminal_await_pending", "terminal_await_wait"]),
    [n.wait]: recoveryNode("Wait For Caller", ["terminal_wait_new_turn"], true),
    [n.end]: { type: "end", position: { x: 2000, y: 0 }, parent_subgraph_id: null, edge_order: [], return_when_nested: true },
  };
  const guardEdges: Record<string, any> = {
    terminal_contact_to_reset: edge(n.recordContact, n.reset, { type: "unconditional" }),
    terminal_reset_failure: edge(n.reset, n.failure, result(false)),
    terminal_reset_is_false: edge(n.reset, n.validate, expression(false)),
    terminal_reset_not_false: edge(n.reset, n.awaitCaller, { type: "unconditional" }),
    terminal_validate_failure: edge(n.validate, n.failure, result(false)),
    terminal_permission_true: edge(n.validate, n.end, expression(true)),
    terminal_pending_question: edge(n.validate, mainNodeId, pendingQuestion),
    terminal_denied_wait: edge(n.validate, n.awaitCaller, { type: "unconditional" }),
    terminal_failure_pending: edge(n.failure, mainNodeId, unanswered),
    terminal_failure_wait: edge(n.failure, n.wait, { type: "unconditional" }),
    terminal_await_pending: edge(n.awaitCaller, mainNodeId, unanswered),
    terminal_await_wait: edge(n.awaitCaller, n.wait, { type: "unconditional" }),
    terminal_wait_new_turn: edge(n.wait, mainNodeId, { type: "llm", condition:
      "A NEW substantive LIVE caller turn arrived after entering this waiting state. Route it to the main conversation before speaking so it can answer or handle the actual request. Do not route on the old processed stop, tool results, silence, noise or placeholder ... . Do not infer consent." }),
  };
  for (const [id] of contexts) {
    guardEdges["terminal_contact_from_" + id] = edge(id, n.recordContact, contactCondition);
    guardEdges["terminal_guard_from_" + id] = edge(id, n.reset, endingCondition);
  }

  for (const [id, node] of Object.entries(guardNodes)) {
    if (nodes[id] && !isDeepStrictEqual(nodes[id], node)) throw new Error("Guard node collision or drift: " + id);
    nodes[id] = node;
  }
  for (const [id, value] of Object.entries(guardEdges)) {
    if (edges[id] && !isDeepStrictEqual(edges[id], value)) throw new Error("Guard edge collision or drift: " + id);
    edges[id] = value;
  }
  for (const [id, node] of contexts) {
    const contactEntry = "terminal_contact_from_" + id, entry = "terminal_guard_from_" + id;
    node.edge_order = [contactEntry, entry, ...(node.edge_order ?? []).filter((edgeId: string) =>
      edgeId !== entry && edgeId !== contactEntry)];
  }
  const prompt = next.conversation_config.agent.prompt;
  prompt.tool_ids = [...new Set([...(prompt.tool_ids ?? []), resetToolId, validateToolId])];
  next.conversation_config.agent.dynamic_variables ??= { dynamic_variable_placeholders: {} };
  next.conversation_config.agent.dynamic_variables.dynamic_variable_placeholders ??= {};
  next.conversation_config.agent.dynamic_variables.dynamic_variable_placeholders[TERMINAL_PERMISSION_VARIABLE] = false;
  next.conversation_config.agent.dynamic_variables.dynamic_variable_placeholders[TERMINAL_DECISION_VARIABLE] = "reset";

  const pairs = Object.values(edges).map((value: any) => [value.source, value.target].sort().join(":"));
  if (new Set(pairs).size !== pairs.length) throw new Error("Duplicate unordered workflow node pair is not supported");
  for (const value of Object.values(edges)) if (!nodes[value.source] || !nodes[value.target]) throw new Error("Dangling workflow edge");
  // Tool-node expression execution and failure-edge priority still require runtime proof; no atomic history lock is claimed.
  return next;
}
