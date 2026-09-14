const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");
require("tsx/cjs");
const axios = require("axios");
axios.defaults.adapter = async config => { throw new Error("Unexpected network request: " + config.url); };
const { buildConversationRelease, resolveVerifiedContactToolId } = require("../src/scripts/syncElevenLabsAgent.ts");
const source = fs.readFileSync(path.resolve(__dirname, "../src/scripts/syncElevenLabsAgent.ts"), "utf8");

const control = { resetToolId: "tool_reset", validateToolId: "tool_validate", contactToolId: "tool_oldcontact", mainNodeId: "main_conversation" };
const replacements = { tool_oldcallback: "tool_newcallback", tool_oldcontact: "tool_newcontact", tool_oldinformation: "tool_newinformation" };
function contactMap() {
  return { plan: { tools: [
    { name: "information_requested", old_id: "tool_oldinformation" },
    { name: "not_interested", old_id: "tool_oldcontact" },
    { name: "callback_requested", old_id: "tool_oldcallback" },
  ] } };
}
function baseline() {
  const edge = (source, target, type = "llm") => ({ source, target, forward_condition: { type, condition: "Existing route" } });
  return {
    conversation_config: {
      agent: { first_message: "Current introduction", disable_first_message_interruptions: true, prompt: {
        prompt: "Current prompt", llm: "previous-provider-model", temperature: 0.25, max_tokens: 2048,
        llm_backup: "previous-fallback", reasoning_effort: "low",
        tool_ids: ["tool_live", "tool_oldcallback", "tool_oldcontact", "tool_oldinformation"],
        tools: [{ name: "not_interested" }, { name: "live_transfer_requested" }, { name: "callback_requested" }, { name: "information_requested" }],
        built_in_tools: { skip_turn: { name: "skip_turn" }, end_call: { name: "end_call" }, voicemail_detection: { name: "voicemail_detection" } },
      } },
      turn: { initial_wait_time: 1.6, turn_timeout: 1.5, silence_end_call_timeout: 45, transcribe_on_disabled_interruptions: false },
      conversation: { client_events: ["audio", "user_transcript"] },
      tts: { voice_id: "unchanged-voice", model_id: "unchanged-tts-model", speed: 0.93 },
    },
    workflow: { nodes: {
      start_node: { type: "start", edge_order: ["start_to_main"] },
      main_conversation: { type: "override_agent", conversation_config: {}, edge_order: ["main_to_patch_after_accepted_result", "main_to_callback_after_unavailable_result", "main_to_transfer_check"] },
      patch_transfer: { type: "override_agent", additional_prompt: "Current transfer policy", edge_order: ["patch_to_phone"] },
      callback_after_unavailable: { type: "override_agent", additional_prompt: "Current callback policy", edge_order: [] },
      transfer_check: { type: "tool", tools: [{ tool_id: "tool_live" }], edge_order: ["transfer_check_to_patch", "transfer_check_to_callback"] },
      phone_transfer: { type: "phone_number", transfer_destination: { phone_number: "+12025550123" }, edge_order: [] },
    }, edges: {
      start_to_main: edge("start_node", "main_conversation", "unconditional"),
      main_to_patch_after_accepted_result: edge("main_conversation", "patch_transfer"),
      main_to_callback_after_unavailable_result: edge("main_conversation", "callback_after_unavailable"),
      main_to_transfer_check: edge("main_conversation", "transfer_check"),
      transfer_check_to_patch: edge("transfer_check", "patch_transfer", "result"),
      transfer_check_to_callback: edge("transfer_check", "callback_after_unavailable", "result"),
      patch_to_phone: edge("patch_transfer", "phone_transfer"),
    } },
  };
}

test("sync starts from provider-owned voice, tools, and unrelated settings", () => {
  assert.match(source, /conversation_config: agent.conversation_config/);
  assert.match(source, /workflow: agent.workflow/);
  assert.doesNotMatch(source, /llm: config|TTS_SPEED|buildWarmTransferWorkflow|client.get<ToolResponse>/);
});
test("sync defaults to dry run and guards writes with reviewed live identifiers", () => {
  assert.match(source, /process.argv.includes\("--apply"\)/);
  assert.match(source, /Apply requires the reviewed live version and prompt SHA guards/);
  assert.match(source, /Agent changed after candidate creation; nothing applied/);
  assert.match(source, /enable_versioning_if_not_enabled: true/);
  assert.match(source, /if \(controlMapPath && !contactToolMapPath\)/);
  assert.match(source, /A full guarded release requires a verified contact-tool map/);
});
test("sync saves rollback and candidate receipts and verifies readback", () => {
  for (const file of ["before.json", "candidate.json", "after.json", "receipt.json"]) assert.ok(source.includes(file));
  assert.match(source, /Prompt readback mismatch after write/);
  assert.match(source, /Conversation policy readback mismatch after write/);
  assert.match(source, /if \(require.main === module\)/);
});

test("candidate sets only the selected base model and preserves other model and voice settings", () => {
  const current = baseline(), before = structuredClone(current);
  const result = buildConversationRelease(current, "Approved prompt", true);
  assert.deepEqual(current, before);
  assert.equal(result.conversation_config.agent.prompt.llm, "gpt-4.1");
  for (const key of ["temperature", "max_tokens", "llm_backup", "reasoning_effort"]) {
    assert.equal(result.conversation_config.agent.prompt[key], current.conversation_config.agent.prompt[key]);
  }
  assert.deepEqual(result.conversation_config.tts, current.conversation_config.tts);
  assert.deepEqual(result.conversation_config.agent.prompt.tool_ids, current.conversation_config.agent.prompt.tool_ids);
  assert.equal(result.conversation_config.agent.prompt.tools, undefined);
});

test("contact identity comes only from the exact named verified plan entry, independent of array order", () => {
  const current = baseline(), map = contactMap(), before = structuredClone(map);
  assert.equal(resolveVerifiedContactToolId(current, map), "tool_oldcontact");
  assert.deepEqual(map, before);
  map.plan.tools.reverse();
  current.conversation_config.agent.prompt.tool_ids.reverse();
  current.conversation_config.agent.prompt.tools.reverse();
  assert.equal(resolveVerifiedContactToolId(current, map), "tool_oldcontact");
  delete current.conversation_config.agent.prompt.tools;
  assert.equal(resolveVerifiedContactToolId(current, map), "tool_oldcontact");
  assert.deepEqual(map.plan.tools, before.plan.tools.reverse());
});

test("missing, ambiguous, renamed, invalid or clone-only contact identity fails closed", () => {
  for (const map of [undefined, {}, { plan: { tools: {} } }, { plan: { tools: [] } },
    { plan: { tools: [{ name: "NOT_INTERESTED", old_id: "tool_oldcontact" }] } },
    { plan: { tools: [{ name: "not_interested", new_id: "tool_newcontact" }] } },
    { plan: { tools: [{ name: "not_interested", old_id: "not-a-tool-id" }] } },
    { plan: { tools: [{ name: "not_interested", old_id: "tool_oldcontact" }, { name: "not_interested", old_id: "tool_oldcontact" }] } },
    { plan: { tools: [{ name: "not_interested", old_id: "tool_oldcontact" }, { name: "not_interested", old_id: "tool_othercontact" }] } },
  ]) assert.throws(() => resolveVerifiedContactToolId(baseline(), map), /Exactly one verified not_interested old tool ID/);
});

test("contact identity must occur exactly once in the owning global registry", () => {
  for (const ids of [undefined, {}, [], ["tool_newcontact"], ["tool_oldcontact", "tool_oldcontact"]]) {
    const current = baseline(); current.conversation_config.agent.prompt.tool_ids = ids;
    assert.throws(() => resolveVerifiedContactToolId(current, contactMap()), /registered exactly once globally/);
  }
});

test("CLI resolves the old contact identity only after owning map verification", () => {
  const verification = source.indexOf("await verifyContactToolMap(client, contactToolMap, current, expectedContactToolMap)");
  const resolution = source.indexOf("contactToolId: resolveVerifiedContactToolId(current, contactToolMap)");
  assert.ok(verification >= 0 && resolution > verification);
  assert.match(source, /contactToolId: string;/);
  assert.match(source, /control\?: ConversationReleaseControl/);
});

test("full guarded release requires an explicit valid contact identity", () => {
  const { contactToolId: _unused, ...missing } = control;
  for (const value of [missing, { ...control, contactToolId: "tool_missing" }, { ...control, contactToolId: control.resetToolId }]) {
    assert.throws(() => buildConversationRelease(baseline(), "Approved prompt", true, undefined, value),
      /verified contact\/reset\/validation tool IDs|required|registered exactly once globally/i);
  }
});

test("full release keeps the registry but binds only the native contact node to the mapped clone", () => {
  const current = baseline(), before = structuredClone(current);
  const result = buildConversationRelease(current, "Approved prompt", true, replacements, {
    ...control, contactToolId: resolveVerifiedContactToolId(current, contactMap()),
  });
  assert.deepEqual(current, before);
  assert.equal(result.conversation_config.agent.prompt.llm, "gpt-4.1");
  assert.deepEqual(result.workflow.nodes.terminal_guard_record_contact.tools, [{ tool_id: "tool_newcontact", schema_overrides: null }]);
  const globalIds = result.conversation_config.agent.prompt.tool_ids;
  for (const id of [...Object.values(replacements), control.resetToolId, control.validateToolId]) {
    assert.equal(globalIds.filter(value => value === id).length, 1);
  }
  assert.deepEqual(result.workflow.nodes.terminal_guard_reset.tools, [{ tool_id: control.resetToolId, schema_overrides: null }]);
  assert.deepEqual(result.workflow.nodes.terminal_guard_validate.tools, [{ tool_id: control.validateToolId, schema_overrides: null }]);
  for (const node of Object.values(result.workflow.nodes)) {
    if (node.type !== "override_agent") continue;
    const ids = node.conversation_config.agent.prompt.tool_ids;
    assert.ok(Array.isArray(ids));
    for (const id of ["tool_oldcontact", "tool_newcontact", control.resetToolId, control.validateToolId]) {
      assert.equal(ids.includes(id), false);
      assert.equal((node.additional_tool_ids ?? []).includes(id), false);
    }
  }
  const allBindings = JSON.stringify(result);
  for (const oldId of Object.keys(replacements)) assert.equal(allBindings.includes(oldId), false);
});
