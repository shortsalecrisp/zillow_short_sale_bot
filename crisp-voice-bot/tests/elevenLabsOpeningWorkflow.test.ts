import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import {
  applyConversationOpeningWorkflow,
  buildOpeningListenerPrompt,
  CALLBACK_RECEIPT_WAIT_PROMPT,
  OPENING_LISTENER_PROMPT,
  SHORT_LISTING_RECOVERY_PROMPT,
  VOICE_STATE_TRANSITION_NODES,
} from "../src/lib/elevenLabsOpeningWorkflow";
import {
  OPENING_HANDOFF_POLICY,
  VOICE_CALLBACK_RECEIPT_ACK,
  VOICE_NEEDS_QUESTION,
  VOICE_NEW_LISTENER_RECOVERY,
  VOICE_OPENING_SCRIPT,
  VOICE_SCREENING_SCRIPT,
  VOICE_SHORT_LISTING_RECOVERY,
} from "../src/lib/elevenLabsConversationPolicy";

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
  assert.equal(after.workflow.nodes.opening_listener.conversation_config.agent.prompt.prompt, buildOpeningListenerPrompt("Sales context"));
  assert.equal(after.conversation_config.agent.prompt.prompt, "Sales context");
  assert.deepEqual(after.conversation_config.tts, before.conversation_config.tts);
  assert.equal(after.workflow.nodes.main_conversation.entry_behavior, "generate_immediately");
  assert.equal(after.workflow.nodes.opening_listener.parent_subgraph_id, null);
  assert.equal(after.workflow.nodes.opening_listener.forced_tool_name, null);
  assert.equal(Object.hasOwn(after.workflow.nodes.opening_listener, "system_tool"), false);
  assert.deepEqual(after.workflow.subgraphs, {});
  assert.equal(after.workflow.nodes[VOICE_STATE_TRANSITION_NODES.shortListingRecovery].entry_behavior, "generate_immediately");
  assert.equal(after.workflow.nodes[VOICE_STATE_TRANSITION_NODES.callbackReceiptWait].entry_behavior, "generate_immediately");
  assert.match(SHORT_LISTING_RECOVERY_PROMPT, new RegExp(VOICE_SHORT_LISTING_RECOVERY.replace(/[?{}]/g, "\\$&")));
  assert.ok(SHORT_LISTING_RECOVERY_PROMPT.includes(VOICE_NEW_LISTENER_RECOVERY));
  assert.match(SHORT_LISTING_RECOVERY_PROMPT, /genuinely NEW live person.*has not heard your identity/);
  assert.match(SHORT_LISTING_RECOVERY_PROMPT, /same listener who already heard your identity/);
  assert.match(SHORT_LISTING_RECOVERY_PROMPT, /Never repeat the identity to the same listener/);
  assert.ok(CALLBACK_RECEIPT_WAIT_PROMPT.includes(VOICE_CALLBACK_RECEIPT_ACK));
});

test("screening, repeated greetings and callback receipts have dedicated workflow states", () => {
  const after = applyConversationOpeningWorkflow(base());
  const n = VOICE_STATE_TRANSITION_NODES;
  assert.equal(after.workflow.edges.opening_to_short_listing_recovery.target, n.shortListingRecovery);
  assert.match(after.workflow.edges.opening_to_short_listing_recovery.forward_condition.condition, /automated screening, connecting audio or a hold state/);
  assert.equal(after.workflow.edges.main_to_short_listing_recovery.target, n.shortListingRecovery);
  assert.match(after.workflow.edges.main_to_short_listing_recovery.forward_condition.condition, /only an intelligible repeated hello or presence check/);
  assert.match(after.workflow.edges.main_to_short_listing_recovery.backward_condition.condition, /NEW complete live-caller answer/);
  assert.equal(after.workflow.edges.main_to_callback_receipt_wait.target, n.callbackReceiptWait);
  assert.match(after.workflow.edges.main_to_callback_receipt_wait.forward_condition.condition, /requestCaptured true/);
  assert.match(after.workflow.edges.main_to_callback_receipt_wait.backward_condition.condition, /incomplete fragment/);
  assert.equal(after.workflow.nodes[n.shortListingRecovery].conversation_config.agent.prompt.tool_ids.length, 0);
  assert.equal(after.workflow.nodes[n.callbackReceiptWait].conversation_config.agent.prompt.tool_ids.length, 0);
});

test("a current question routes to main, while an old pickup cannot trigger qualification", () => {
  const result = applyConversationOpeningWorkflow(base());
  const condition = result.workflow.edges.opening_to_main.forward_condition.condition;
  assert.match(condition, /LIVE person/);
  assert.match(condition, /NEW live caller turn arrived AFTER/);
  assert.match(condition, /A greeting before the introduction does not qualify/);
  assert.match(OPENING_LISTENER_PROMPT, /During the initial greeting, before a NEW live caller reply after the introduction, do not qualify the listing/);
  assert.match(OPENING_LISTENER_PROMPT, /Once that new reply arrives, the shared business policy and opening handoff contract govern the next response even if this node remains active/);
  assert.doesNotMatch(OPENING_LISTENER_PROMPT, /Never qualify[^\n]+in this stage/);
  assert.match(OPENING_LISTENER_PROMPT, /We help with short-sale lender paperwork\. Is \{\{streetAddress\}\} your listing\?/);
  assert.match(OPENING_LISTENER_PROMPT, /If the only new transcript is "\.\.\."/);
  assert.match(OPENING_LISTENER_PROMPT, /never say "Are you there\?"/);
  assert.doesNotMatch(OPENING_LISTENER_PROMPT, /\{\{openerScript\}\}/);
});

test("the live opener is identical in the listener and main prompts", () => {
  const mainPrompt = readFileSync(new URL("../docs/elevenlabs-agent-prompt.md", import.meta.url), "utf8");
  assert.ok(OPENING_LISTENER_PROMPT.includes(`"${VOICE_OPENING_SCRIPT}"`));
  assert.ok(mainPrompt.includes(`"${VOICE_OPENING_SCRIPT}"`));
  assert.ok(mainPrompt.includes(`"${VOICE_NEEDS_QUESTION}"`));
  assert.ok(mainPrompt.includes(`"${VOICE_SCREENING_SCRIPT}"`));
  assert.match(OPENING_LISTENER_PROMPT, /Do not speak before the recipient finishes their pickup/);
});

test("screening, voicemail and unrelated recording exit remain separate", () => {
  const result = applyConversationOpeningWorkflow(base());
  assert.match(OPENING_LISTENER_PROMPT, /\{\{assistantName\}\} with Crisp Short Sales, about the short-sale listing at \{\{streetAddress\}\}/);
  assert.ok(OPENING_LISTENER_PROMPT.includes(VOICE_NEW_LISTENER_RECOVERY));
  assert.match(OPENING_LISTENER_PROMPT, /genuinely new live listener who has not heard your identity/);
  assert.doesNotMatch(OPENING_LISTENER_PROMPT, /We help listing agents with lender paperwork and follow-up/);
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
  assert.throws(() => buildOpeningListenerPrompt(""), /Full main conversation prompt/);
  assert.throws(() => applyConversationOpeningWorkflow({ ...base(), workflow: { ...base().workflow, subgraphs: { nested: {} } } }), /Nested workflow/);
});

test("the executed start node retains the full business policy if its model-selected transition is late", () => {
  const mainPrompt = readFileSync(new URL("../docs/elevenlabs-agent-prompt.md", import.meta.url), "utf8").split("## Prompt\n")[1].trim();
  const input = base(); input.conversation_config.agent.prompt.prompt = mainPrompt;
  const after = applyConversationOpeningWorkflow(input);
  const node = after.workflow.nodes[after.workflow.edges.start_to_main.target];
  const effectivePrompt = node.conversation_config.agent.prompt.prompt;
  assert.ok(effectivePrompt.startsWith(mainPrompt + "\n\n"), "First node must have every current business rule, not a stranded intro-only override");
  assert.ok(effectivePrompt.includes(OPENING_HANDOFF_POLICY));
  assert.ok(effectivePrompt.includes(VOICE_NEEDS_QUESTION));
  assert.match(effectivePrompt, /if routing is delayed, apply that same policy here/);
  assert.match(effectivePrompt, /listing ownership only, not interest in help, a callback, or a live transfer/);
  assert.doesNotMatch(effectivePrompt, /Has the seller already completed|Is it okay if I ask one quick question/);
  for (const heading of ["Listening and repair", "Live admins and wrong contacts", "Information request", "Live transfer request", "Contact preferences and endings"]) {
    assert.ok(effectivePrompt.includes("# " + heading), heading);
  }
  // These are resolved runtime configuration contracts, not simulated LLM turns.
  assert.equal(node.entry_behavior, "wait_for_user");
  assert.equal(after.workflow.nodes.main_conversation.entry_behavior, "generate_immediately");
});

test("updating an existing opening preserves terminal guards and tool bindings", () => {
  const before: any = applyConversationOpeningWorkflow(base());
  before.workflow.nodes.opening_listener.edge_order.unshift("terminal_contact_from_opening_listener", "terminal_guard_from_opening_listener");
  before.workflow.edges.terminal_contact_from_opening_listener = { source: "opening_listener", target: "terminal_contact", forward_condition: { condition: "Explicit opt-out first" } };
  before.workflow.edges.terminal_guard_from_opening_listener = { source: "opening_listener", target: "terminal_guard", forward_condition: { condition: "Validate ending" } };
  const agent = before.workflow.nodes.opening_listener.conversation_config.agent;
  agent.prompt.tool_ids = ["verified-contact-tool"];
  agent.prompt.prompt = "STALE LONG OPENING";
  agent.prompt.built_in_tools.skip_turn = { name: "skip_turn", params: { wait_timeout_secs: -1 } };
  before.workflow.nodes.main_conversation.additional_prompt = "Existing terminal pending-question recovery.";
  const copy = structuredClone(before), after = applyConversationOpeningWorkflow(before);
  assert.deepEqual(before, copy);
  assert.deepEqual(after.workflow.nodes.opening_listener.edge_order.slice(0, 2), ["terminal_contact_from_opening_listener", "terminal_guard_from_opening_listener"]);
  assert.deepEqual(after.workflow.edges.terminal_contact_from_opening_listener, before.workflow.edges.terminal_contact_from_opening_listener);
  assert.deepEqual(after.workflow.edges.terminal_guard_from_opening_listener, before.workflow.edges.terminal_guard_from_opening_listener);
  assert.deepEqual(after.workflow.nodes.opening_listener.conversation_config.agent.prompt.tool_ids, ["verified-contact-tool"]);
  assert.deepEqual(after.workflow.nodes.opening_listener.conversation_config.agent.prompt.built_in_tools.skip_turn, agent.prompt.built_in_tools.skip_turn);
  assert.equal(after.workflow.nodes.opening_listener.conversation_config.agent.prompt.built_in_tools.end_call, null);
  assert.match(after.workflow.nodes.main_conversation.additional_prompt, /^Existing terminal pending-question recovery\./);
  assert.doesNotMatch(after.workflow.nodes.opening_listener.conversation_config.agent.prompt.prompt, /STALE LONG OPENING/);
  assert.deepEqual(applyConversationOpeningWorkflow(after), after);
});
