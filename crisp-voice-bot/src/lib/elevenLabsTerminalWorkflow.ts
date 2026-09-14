import { isDeepStrictEqual } from "node:util";

export const TERMINAL_GUARD_NODES = Object.freeze({
  reset: "terminal_guard_reset", validate: "terminal_guard_validate",
  failure: "terminal_guard_failure", awaitCaller: "terminal_guard_await_caller", end: "terminal_guard_end",
});
export const TERMINAL_PERMISSION_VARIABLE = "terminal_permission";
export const TERMINAL_DECISION_VARIABLE = "terminal_decision";

type WorkflowBody = {
  conversation_config: { agent: { prompt: any; dynamic_variables?: any; [key: string]: any }; [key: string]: any };
  workflow: { nodes: Record<string, any>; edges: Record<string, any>; [key: string]: any };
  [key: string]: any;
};
type Options = { resetToolId: string; validateToolId: string; mainNodeId: string };

function removeModelEndCall(prompt: any): void {
  prompt.built_in_tools ??= {};
  prompt.built_in_tools.end_call = null;
  if (Array.isArray(prompt.tools)) prompt.tools = prompt.tools.filter((tool: any) =>
    tool.name !== "end_call" && tool.params?.system_tool_type !== "end_call");
}

export function applyGuardedTerminalWorkflow<T extends WorkflowBody>(body: T, options: Options): T {
  const { resetToolId, validateToolId, mainNodeId } = options;
  if (![resetToolId, validateToolId].every(id => /^tool_[a-z0-9]+$/.test(id)) || resetToolId === validateToolId) {
    throw new Error("Two distinct verified reset/validation tool IDs are required");
  }
  const next = structuredClone(body), workflow = next.workflow, n = TERMINAL_GUARD_NODES;
  if (!next.conversation_config?.agent?.prompt || !workflow?.edges || workflow.nodes?.[mainNodeId]?.type !== "override_agent"
    || Object.values(n).some(id => id === mainNodeId)) throw new Error("An existing main conversation override node is required");
  const nodes = workflow.nodes, edges = workflow.edges;
  const ownedIds = new Set<string>(Object.values(n));
  const modelContexts = Object.entries(nodes).filter(([id, node]) => !ownedIds.has(id) && node.type === "override_agent");
  for (const [, node] of modelContexts) if (node.forced_tool_name === "end_call" || node.system_tool?.name === "end_call"
    || node.system_tool?.params?.system_tool_type === "end_call") throw new Error("Existing forced terminal context requires an explicit recording-path review");
  removeModelEndCall(next.conversation_config.agent.prompt);
  for (const [, node] of modelContexts) {
    node.conversation_config ??= {}; node.conversation_config.agent ??= {}; node.conversation_config.agent.prompt ??= {};
    removeModelEndCall(node.conversation_config.agent.prompt);
  }
  const currentTurnInstruction = "The guarded-ending decision variable is {{terminal_decision}}. When returning from a just-completed validation with pending_question_or_correction, answer or handle the current unanswered live question or correction now. Do not re-enter the guard for that same caller turn, even when the base prompt requests a diagnostic check. Do not infer this decision from a model-visible tool result: assigned fields may be removed there. An older decision does not replace the actual content of a new caller turn.";
  if (!nodes[mainNodeId].additional_prompt?.includes(currentTurnInstruction)) {
    nodes[mainNodeId].additional_prompt = [nodes[mainNodeId].additional_prompt, currentTurnInstruction].filter(Boolean).join("\n\n");
  }
  const dialogueIds = new Set([mainNodeId, "callback_after_unavailable", "patch_transfer", "opening_listener"]);
  const contexts = modelContexts.filter(([id]) => dialogueIds.has(id));
  const mainOrder = (nodes[mainNodeId].edge_order ?? []).filter((id: string) => !id.startsWith("terminal_guard_from_"));
  const endingCondition = { type: "llm", condition:
    "Evaluate the whole latest caller turn before considering an ending. When that complete turn still asks to end this live call, says goodbye, or refuses the service, route through the ending-permission guard before any hang-up. A later question, correction, revocation, or request to wait in the same turn takes precedence over earlier stop words: handle the current request instead of entering the guard. A callback-only confirmation, email-only preference, or tool success does not itself authorize ending. Follow the base prompt only when it explicitly requests a diagnostic guard check, and never repeat that check for the same caller turn. Do not use this for voicemail, recorded-message completion, or phone handoff; preserve their separate workflows." };
  const recoveryInstruction = "On entry after a denied or failed ending check, inspect the current caller turn before doing anything else. If a live question, correction or request arrived during the check and remains unanswered, answer or handle it now under the main conversation policy. Otherwise call skip_turn without speaking and wait for a new live caller turn. Do not announce tool failure, repeat an acknowledgment, ask if the caller is still there, or repeat the ending check for the same caller turn. A tool result, silence or placeholder ... is not a new live caller turn. After a new live caller turn, follow the normal conversation policy.";
  const recoveryNode = (label: string, entryEdge: string, prefix: string) => {
    const node = { ...structuredClone(nodes[mainNodeId]), label, parent_subgraph_id: null, forced_tool_name: null,
      entry_behavior: "generate_immediately", additional_prompt: [nodes[mainNodeId].additional_prompt, recoveryInstruction].filter(Boolean).join("\n\n"),
      edge_order: [entryEdge, ...mainOrder.map((id: string) => prefix + id)] };
    if (node.system_tool === null) delete node.system_tool;
    return node;
  };

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
    [n.reset]: toolNode(resetToolId, 1000, 0, ["terminal_reset_failure", "terminal_reset_is_false", "terminal_reset_not_false"]),
    [n.validate]: toolNode(validateToolId, 1500, 0, ["terminal_validate_failure", "terminal_permission_true", "terminal_pending_question", "terminal_denied_wait"]),
    [n.failure]: recoveryNode("Recover After Ending Check Failure", "terminal_reset_failure", "terminal_guard_failure_"),
    [n.awaitCaller]: recoveryNode("Recover After Denied Ending", "terminal_reset_not_false", "terminal_guard_await_"),
    [n.end]: { type: "end", position: { x: 2000, y: 0 }, parent_subgraph_id: null, edge_order: [], return_when_nested: true },
  };
  const guardEdges: Record<string, any> = {
    terminal_reset_failure: edge(n.reset, n.failure, result(false), endingCondition),
    terminal_reset_is_false: edge(n.reset, n.validate, expression(false)),
    terminal_reset_not_false: edge(n.reset, n.awaitCaller, { type: "unconditional" }, endingCondition),
    terminal_validate_failure: edge(n.validate, n.failure, result(false)),
    terminal_permission_true: edge(n.validate, n.end, expression(true)),
    terminal_pending_question: edge(n.validate, mainNodeId, pendingQuestion),
    terminal_denied_wait: edge(n.validate, n.awaitCaller, { type: "unconditional" }),
  };
  for (const id of mainOrder) {
    if (edges[id]?.source !== mainNodeId) throw new Error("Main outgoing route requires explicit mirroring review: " + id);
    guardEdges["terminal_guard_await_" + id] = { ...structuredClone(edges[id]), source: n.awaitCaller, backward_condition: null };
    guardEdges["terminal_guard_failure_" + id] = { ...structuredClone(edges[id]), source: n.failure, backward_condition: null };
  }
  for (const [id] of contexts) guardEdges["terminal_guard_from_" + id] = edge(id, n.reset, endingCondition);

  for (const [id, node] of Object.entries(guardNodes)) {
    if (nodes[id] && !isDeepStrictEqual(nodes[id], node)) throw new Error("Guard node collision or drift: " + id);
    nodes[id] = node;
  }
  for (const [id, value] of Object.entries(guardEdges)) {
    if (edges[id] && !isDeepStrictEqual(edges[id], value)) throw new Error("Guard edge collision or drift: " + id);
    edges[id] = value;
  }
  for (const [id, node] of contexts) {
    const entry = "terminal_guard_from_" + id;
    node.edge_order = [entry, ...(node.edge_order ?? []).filter((edgeId: string) => edgeId !== entry)];
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
