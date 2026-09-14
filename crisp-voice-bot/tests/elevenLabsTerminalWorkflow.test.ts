import assert from "node:assert/strict";
import test from "node:test";
import { applyGuardedTerminalWorkflow, TERMINAL_GUARD_NODES as n } from "../src/lib/elevenLabsTerminalWorkflow";

const options = { resetToolId: "tool_reset", validateToolId: "tool_validate", contactToolId: "tool_contact", mainNodeId: "main" };
function baseline(): any {
  return { conversation_config: { agent: { first_message: "", prompt: { prompt: "Existing behavior", tool_ids: ["tool_contact", "tool_callback"],
    built_in_tools: { end_call: { name: "end_call" }, voicemail_detection: { name: "voicemail_detection" }, skip_turn: { name: "skip_turn" } },
    tools: [{ name: "end_call", type: "system" }, { name: "other", type: "client" }] } },
    tts: { voice_id: "unchanged", speed: 0.95 }, turn: { silence_end_call_timeout: 45 } },
  workflow: { nodes: { start: { type: "start", edge_order: ["start_main"] },
    main: { type: "override_agent", entry_behavior: "auto", conversation_config: {}, edge_order: ["main_patch", "main_vm"] },
    patch_transfer: { type: "override_agent", additional_prompt: "Existing transfer policy", edge_order: ["patch_phone"] },
    transfer_result_review: { type: "override_agent", entry_behavior: "auto", additional_prompt: "Route only", edge_order: ["review_main"] },
    phone: { type: "phone_number", transfer_destination: { phone_number: "+12025550123" } },
    vm: { type: "end", label: "Existing recording exit", edge_order: [] } },
  edges: { start_main: { source: "start", target: "main", forward_condition: { type: "unconditional" } },
    main_patch: { source: "main", target: "patch_transfer", forward_condition: { type: "llm", condition: "Existing approved transfer" } },
    patch_phone: { source: "patch_transfer", target: "phone", forward_condition: { type: "llm", condition: "Existing continuing consent" } },
    review_main: { source: "transfer_result_review", target: "main", forward_condition: { type: "unconditional" } },
    main_vm: { source: "main", target: "vm", forward_condition: { type: "llm", condition: "Existing recorded exit" } } } },
  platform_settings: { auth: { enable_auth: true } }, phone_numbers: [{ id: "existing" }], procedures: {} };
}

// This local evaluator checks intended priority, not the provider's native execution semantics.
function expression(ast: any, variables: Record<string, unknown>): unknown {
  if (ast.type === "dynamic_variable") return variables[ast.name];
  if (["boolean_literal", "string_literal"].includes(ast.type)) return ast.value;
  if (ast.type === "eq_operator") return expression(ast.left, variables) === expression(ast.right, variables);
  if (ast.type === "and_operator") return ast.children.every((child: any) => expression(child, variables) === true);
  if (ast.type === "llm") return variables.classifier_result;
  throw new Error("Unsupported test expression");
}
function directTarget(workflow: any, nodeId: string, successful: boolean, permission: unknown, decision = "reset", classifier: unknown = false) {
  for (const edgeId of workflow.nodes[nodeId].edge_order) {
    const edge = workflow.edges[edgeId], forward = edge.source === nodeId;
    assert.ok(forward || edge.target === nodeId);
    const condition = forward ? edge.forward_condition : edge.backward_condition;
    if (condition?.type === "unconditional" || (condition?.type === "result" && condition.successful === successful)
      || (condition?.type === "expression" && expression(condition.expression,
        { terminal_permission: permission, terminal_decision: decision, classifier_result: classifier }) === true)) return forward ? edge.target : edge.source;
  }
  throw new Error("No deterministic route");
}

test("direct guard is pure and idempotent with no procedure dependency", () => {
  const before = baseline(), saved = structuredClone(before), after = applyGuardedTerminalWorkflow(before, options);
  assert.deepEqual(before, saved); assert.deepEqual(applyGuardedTerminalWorkflow(after, options), after);
  for (const key of ["procedures", "platform_settings", "phone_numbers"]) assert.deepEqual(after[key], before[key]);
  assert.deepEqual(after.conversation_config.tts, before.conversation_config.tts);
  assert.equal(after.conversation_config.agent.dynamic_variables.dynamic_variable_placeholders.terminal_permission, false);
  assert.equal(after.conversation_config.agent.dynamic_variables.dynamic_variable_placeholders.terminal_decision, "reset");
});

test("contact recording leads directly through reset and validation without another model choice", () => {
  const { nodes, edges } = applyGuardedTerminalWorkflow(baseline(), options).workflow;
  assert.equal(Object.keys(n).length, 7);
  assert.deepEqual(nodes[n.recordContact].tools, [{ tool_id: "tool_contact", schema_overrides: null }]);
  assert.deepEqual(nodes[n.recordContact].edge_order, ["terminal_contact_to_reset"]);
  assert.deepEqual(edges.terminal_contact_to_reset.forward_condition, { type: "unconditional", label: null });
  assert.equal(directTarget({ nodes, edges }, n.recordContact, true, true), n.reset);
  assert.equal(directTarget({ nodes, edges }, n.recordContact, false, true), n.reset);
  assert.deepEqual(nodes[n.reset].tools, [{ tool_id: "tool_reset", schema_overrides: null }]);
  assert.deepEqual(nodes[n.validate].tools, [{ tool_id: "tool_validate", schema_overrides: null }]);
  assert.equal(edges.terminal_reset_is_false.source, n.reset); assert.equal(edges.terminal_reset_is_false.target, n.validate);
  assert.equal(edges.terminal_permission_true.source, n.validate); assert.equal(edges.terminal_permission_true.target, n.end);
  for (const name of ["reset_result", "decision", "denied", "return_current", "return"]) assert.equal(nodes["terminal_guard_" + name], undefined);
});

test("reset failure precedes typed false; malformed or stale true cannot advance locally", () => {
  const workflow = applyGuardedTerminalWorkflow(baseline(), options).workflow;
  assert.deepEqual(workflow.nodes[n.reset].edge_order, ["terminal_reset_failure", "terminal_reset_is_false", "terminal_reset_not_false"]);
  assert.deepEqual(workflow.edges.terminal_reset_failure.forward_condition, { type: "result", successful: false, label: null });
  assert.equal(directTarget(workflow, n.reset, true, false), n.validate);
  for (const value of [true, false, "true", "false", null, undefined, 0, {}]) {
    assert.equal(directTarget(workflow, n.reset, false, value), n.failure);
    if (value !== false) assert.equal(directTarget(workflow, n.reset, true, value), n.awaitCaller);
  }
});

test("validator error outranks stale true, and strict true alone reaches terminal", () => {
  const workflow = applyGuardedTerminalWorkflow(baseline(), options).workflow;
  assert.deepEqual(workflow.nodes[n.validate].edge_order,
    ["terminal_validate_failure", "terminal_permission_true", "terminal_pending_question", "terminal_denied_wait"]);
  for (const value of [true, false, "true", "false", null, undefined, 0, {}]) {
    assert.equal(directTarget(workflow, n.validate, false, value), n.failure);
    assert.equal(directTarget(workflow, n.validate, true, value), value === true ? n.end : n.awaitCaller);
  }
  assert.deepEqual(Object.values(workflow.edges).filter((edge: any) => edge.target === n.end), [workflow.edges.terminal_permission_true]);
  assert.deepEqual(workflow.edges.terminal_permission_true.forward_condition.expression,
    { type: "eq_operator", left: { type: "dynamic_variable", name: "terminal_permission" }, right: { type: "boolean_literal", value: true } });
});

test("only a valid false pending-question decision returns directly to current main", () => {
  const workflow = applyGuardedTerminalWorkflow(baseline(), options).workflow;
  assert.equal(directTarget(workflow, n.validate, true, false, "pending_question_or_correction"), "main");
  for (const value of ["false", null, undefined]) assert.equal(directTarget(workflow, n.validate, true, value, "pending_question_or_correction"), n.awaitCaller);
  assert.equal(directTarget(workflow, n.validate, false, false, "pending_question_or_correction"), n.failure);
  assert.equal(workflow.edges.terminal_pending_question.source, n.validate); assert.equal(workflow.nodes.main.entry_behavior, "auto");
});

test("recovery routes pending questions or waits without inheriting sales generation", () => {
  const { nodes, edges } = applyGuardedTerminalWorkflow(baseline(), options).workflow;
  for (const id of [n.failure, n.awaitCaller]) {
    assert.equal(nodes[id].entry_behavior, "auto");
    assert.equal(nodes[id].additional_prompt, "");
    assert.match(nodes[id].conversation_config.agent.prompt.prompt, /Never produce spoken text/);
    assert.doesNotMatch(nodes[id].conversation_config.agent.prompt.prompt, /Existing behavior/);
    const first = edges[nodes[id].edge_order[0]], fallback = edges[nodes[id].edge_order[1]];
    assert.equal(first.target, "main");
    assert.equal(first.forward_condition.expression.value_schema.type, "boolean");
    assert.match(first.forward_condition.expression.value_schema.description, /still-unanswered/);
    assert.equal(first.forward_condition.expression.value_schema.description, first.forward_condition.expression.prompt);
    assert.equal(first.forward_condition.expression.value_schema.enum, null);
    assert.match(first.forward_condition.expression.prompt, /turn that initiated.*speech arriving during/);
    assert.equal(fallback.target, n.wait); assert.equal(fallback.forward_condition.type, "unconditional");
    assert.equal(directTarget({ nodes, edges }, id, false, false, "validator_error", true), "main");
    for (const value of [false, "true", null, undefined, {}]) {
      assert.equal(directTarget({ nodes, edges }, id, false, true, "validator_error", value), n.wait);
    }
  }
  assert.equal(nodes[n.wait].entry_behavior, "wait_for_user");
  assert.match(edges.terminal_wait_new_turn.forward_condition.condition, /NEW substantive LIVE caller turn/);
  assert.match(edges.terminal_wait_new_turn.forward_condition.condition, /before speaking/);
  for (const id of [n.failure, n.awaitCaller, n.wait]) {
    const prompt = nodes[id].conversation_config.agent.prompt;
    assert.deepEqual(prompt.tool_ids, []); assert.deepEqual(prompt.tools, []);
    assert.deepEqual(prompt.knowledge_base, []);
    for (const [key, value] of Object.entries(prompt.built_in_tools)) assert.ok(key === "skip_turn" || value === null);
    assert.ok(Object.values(edges).filter((e: any) => e.source === id).every((e: any) => ["main", n.wait].includes(e.target)));
  }
  assert.equal(edges.terminal_reset_failure.backward_condition, null);
  assert.equal(edges.terminal_guard_from_transfer_result_review, undefined);
  assert.deepEqual(nodes.transfer_result_review.edge_order, ["review_main"]);
  assert.match(nodes.main.additional_prompt, /\{\{terminal_decision\}\}/);
  assert.match(nodes.main.additional_prompt, /even when the base prompt requests a diagnostic check/);
});

test("own node fields match observed provider shape without changing inherited contexts", () => {
  const { nodes } = applyGuardedTerminalWorkflow(baseline(), options).workflow;
  for (const id of Object.values(n)) assert.equal(nodes[id].parent_subgraph_id, null);
  for (const id of [n.recordContact, n.reset, n.validate]) {
    assert.equal(nodes[id].system_tool, null); assert.equal(Object.hasOwn(nodes[id], "label"), false);
  }
  assert.equal(nodes[n.end].return_when_nested, true); assert.equal(Object.hasOwn(nodes[n.end], "label"), false);
  for (const id of [n.failure, n.awaitCaller]) {
    assert.equal(nodes[id].forced_tool_name, null); assert.equal(Object.hasOwn(nodes[id], "system_tool"), false);
  }
  assert.equal(nodes.main.entry_behavior, "auto");
});

test("all model contexts lose direct end_call while existing transfer and voicemail routes remain", () => {
  const before = baseline(), after = applyGuardedTerminalWorkflow(before, options);
  assert.equal(after.conversation_config.agent.prompt.built_in_tools.end_call, null);
  assert.deepEqual(after.conversation_config.agent.prompt.tools, [{ name: "other", type: "client" }]);
  for (const node of Object.values(after.workflow.nodes) as any[]) if (node.type === "override_agent") assert.equal(node.conversation_config.agent.prompt.built_in_tools.end_call, null);
  for (const [id, edge] of Object.entries(before.workflow.edges)) assert.deepEqual(after.workflow.edges[id], edge);
  assert.deepEqual(after.workflow.nodes.phone, before.workflow.nodes.phone); assert.deepEqual(after.workflow.nodes.vm, before.workflow.nodes.vm);
  assert.deepEqual(after.conversation_config.agent.prompt.built_in_tools.voicemail_detection, before.conversation_config.agent.prompt.built_in_tools.voicemail_detection);
  assert.deepEqual(after.workflow.nodes.main.edge_order.slice(2), before.workflow.nodes.main.edge_order);
  assert.deepEqual(after.workflow.nodes.patch_transfer.edge_order.slice(2), before.workflow.nodes.patch_transfer.edge_order);
  const pairs = Object.values(after.workflow.edges).map((edge: any) => [edge.source, edge.target].sort().join(":"));
  assert.equal(new Set(pairs).size, pairs.length);
  assert.match(after.workflow.edges.terminal_guard_from_main.forward_condition.condition, /NEW live caller turn/);
  assert.match(after.workflow.edges.terminal_guard_from_main.forward_condition.condition, /no unanswered question, correction, revocation/);
  assert.match(after.workflow.edges.terminal_guard_from_main.forward_condition.condition, /use the contact-recording path instead/);
});

test("all reachable dialogue and recovery models hide native-only tools while the registry remains intact", () => {
  const before = baseline();
  before.conversation_config.agent.prompt.tool_ids.push("tool_reset", "tool_validate");
  before.workflow.nodes.patch_transfer.conversation_config = { agent: { prompt: {
    tool_ids: ["tool_contact", "tool_reset", "tool_validate", "tool_callback"],
    tools: [{ name: "not_interested" }, { name: "reset_call_end_permission" }, { name: "validate_call_ending" }, { name: "callback_requested" }],
  } } };
  before.workflow.nodes.patch_transfer.additional_tool_ids = ["tool_contact", "tool_reset", "tool_validate"];
  const after = applyGuardedTerminalWorkflow(before, options);
  assert.deepEqual(after.conversation_config.agent.prompt.tool_ids, before.conversation_config.agent.prompt.tool_ids);
  for (const [id, node] of Object.entries(after.workflow.nodes) as [string, any][]) if (node.type === "override_agent") {
    assert.deepEqual(node.conversation_config.agent.prompt.tool_ids,
      ["transfer_result_review", n.failure, n.awaitCaller, n.wait].includes(id) ? [] : ["tool_callback"]);
    assert.deepEqual(node.additional_tool_ids, []);
    for (const tool of node.conversation_config.agent.prompt.tools ?? []) assert.equal(tool.name, "callback_requested");
  }
});

test("contact paths take priority and keep nonterminal preferences distinct from goodbye", () => {
  const { nodes, edges } = applyGuardedTerminalWorkflow(baseline(), options).workflow;
  assert.deepEqual(nodes.main.edge_order.slice(0, 2), ["terminal_contact_from_main", "terminal_guard_from_main"]);
  assert.equal(edges.terminal_contact_from_main.target, n.recordContact);
  assert.match(edges.terminal_contact_from_main.forward_condition.condition, /Do not record the same caller turn twice/);
  assert.match(edges.terminal_contact_from_main.forward_condition.condition, /callback-only or email-only preference/);
  assert.match(edges.terminal_contact_from_main.forward_condition.condition, /Preserve questions, restrictions and earlier genuine interest/);
  assert.equal(directTarget({ nodes, edges }, n.validate, true, false, "no_ending_request"), n.awaitCaller);
  assert.equal(directTarget({ nodes, edges }, n.validate, true, false, "pending_question_or_correction"), "main");
});

test("unknown model contexts and altered route-only contexts fail closed", () => {
  const unknown = baseline(); unknown.workflow.nodes.extra = { type: "override_agent", conversation_config: {} };
  assert.throws(() => applyGuardedTerminalWorkflow(unknown, options), /Unreviewed model context/);
  const changed = baseline(); changed.workflow.edges.review_main.forward_condition.type = "llm";
  assert.throws(() => applyGuardedTerminalWorkflow(changed, options), /return unconditionally/);
  const missing = baseline(); missing.conversation_config.agent.prompt.tool_ids = ["tool_callback"];
  assert.throws(() => applyGuardedTerminalWorkflow(missing, options), /registered exactly once/);
});

test("invalid tools, forced endings, guard drift and duplicate node pairs fail closed", () => {
  for (const patch of [{ validateToolId: "tool_reset" }, { contactToolId: "tool_validate" }, { resetToolId: "bad" }, { mainNodeId: "missing" }]) assert.throws(() => applyGuardedTerminalWorkflow(baseline(), { ...options, ...patch }));
  const forced = baseline(); forced.workflow.nodes.patch_transfer.forced_tool_name = "end_call";
  assert.throws(() => applyGuardedTerminalWorkflow(forced, options), /recording-path review/);
  const drift = applyGuardedTerminalWorkflow(baseline(), options); drift.workflow.nodes[n.reset].edge_order.reverse();
  assert.throws(() => applyGuardedTerminalWorkflow(drift, options), /collision or drift/);
  const duplicate = baseline(); duplicate.workflow.edges.reverse = { source: "patch_transfer", target: "main", forward_condition: { type: "unconditional" } };
  assert.throws(() => applyGuardedTerminalWorkflow(duplicate, options), /Duplicate/);
});
