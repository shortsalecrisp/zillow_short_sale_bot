import assert from "node:assert/strict";
import test from "node:test";
import { applyConversationOpeningWorkflow, OPENING_LISTENER_PROMPT } from "../src/lib/elevenLabsOpeningWorkflow";

const base = () => ({ conversation_config: { agent: { first_message: "", prompt: { prompt: "Sales context" } },
  tts: { voice_id: "unchanged", speed: 0.95 } }, workflow: { nodes: {
    start_node: { type: "start", edge_order: ["start_to_main"] }, main_conversation: { type: "override_agent", edge_order: ["main_to_transfer"] },
    phone_transfer: { type: "phone_number", edge_order: [] },
  }, edges: { start_to_main: { source: "start_node", target: "main_conversation", forward_condition: { type: "unconditional" } },
    main_to_transfer: { source: "main_conversation", target: "phone_transfer", forward_condition: { type: "llm", condition: "Existing guarded transfer" } } } } });

test("opening gets a separate wait-first prompt without changing the sales prompt or voice", () => {
  const before = base(), copy = structuredClone(before), after = applyConversationOpeningWorkflow(before);
  assert.deepEqual(before, copy);
  assert.equal(after.workflow.edges.start_to_main.target, "opening_listener");
  assert.equal(after.workflow.nodes.opening_listener.entry_behavior, "wait_for_user");
  assert.equal(after.workflow.nodes.opening_listener.conversation_config.agent.prompt.prompt, OPENING_LISTENER_PROMPT);
  assert.equal(after.conversation_config.agent.prompt.prompt, "Sales context");
  assert.deepEqual(after.conversation_config.tts, before.conversation_config.tts);
  assert.equal(after.workflow.nodes.main_conversation.entry_behavior, "generate_immediately");
  assert.equal(after.workflow.nodes.opening_listener.parent_subgraph_id, null);
  assert.equal(after.workflow.nodes.opening_listener.forced_tool_name, null);
  assert.equal(Object.hasOwn(after.workflow.nodes.opening_listener, "system_tool"), false);
  assert.deepEqual(after.workflow.subgraphs, {});
});

test("a current question routes to main, while an old pickup cannot trigger qualification", () => {
  const result = applyConversationOpeningWorkflow(base());
  const condition = result.workflow.edges.opening_to_main.forward_condition.condition;
  assert.match(condition, /LIVE person/);
  assert.match(condition, /NEW live caller turn arrived AFTER/);
  assert.match(condition, /A greeting before the introduction does not qualify/);
  assert.match(OPENING_LISTENER_PROMPT, /Never qualify the listing/);
  assert.doesNotMatch(OPENING_LISTENER_PROMPT, /\{\{openerScript\}\}/);
});

test("screening, voicemail and unrelated recording exit remain separate", () => {
  const result = applyConversationOpeningWorkflow(base());
  assert.match(OPENING_LISTENER_PROMPT, /For recorded please-stay-on-the-line, ringing or hold announcements use skip_turn/);
  assert.match(OPENING_LISTENER_PROMPT, /attempt two has no second message/);
  assert.equal(result.workflow.nodes.unrelated_recording_exit.type, "end");
  assert.equal(result.workflow.nodes.unrelated_recording_exit.return_when_nested, true);
  assert.equal(result.workflow.nodes.unrelated_recording_exit.parent_subgraph_id, null);
  assert.equal(Object.hasOwn(result.workflow.nodes.unrelated_recording_exit, "label"), false);
  assert.match(result.workflow.edges.opening_to_unrelated_recording_exit.forward_condition.condition, /actual automated VOICEMAIL/);
  assert.match(result.workflow.edges.opening_to_unrelated_recording_exit.forward_condition.condition, /A live assistant\/admin/);
  assert.deepEqual(result.workflow.edges.main_to_transfer, base().workflow.edges.main_to_transfer);
});

test("opening application is idempotent and refuses a fixed greeting or unknown start", () => {
  const once = applyConversationOpeningWorkflow(base());
  assert.deepEqual(applyConversationOpeningWorkflow(once), once);
  const fixed = base(); fixed.conversation_config.agent.first_message = "Premature intro";
  assert.throws(() => applyConversationOpeningWorkflow(fixed), /Listen-first/);
  assert.throws(() => applyConversationOpeningWorkflow({}), /Verified start/);
  assert.throws(() => applyConversationOpeningWorkflow({ ...base(), workflow: { ...base().workflow, subgraphs: { nested: {} } } }), /Nested workflow/);
});
