import assert from "node:assert/strict";
import test from "node:test";
import { conversationControlTools, normalizeControlToolReadback, verifyConversationControlMap } from "../src/scripts/elevenLabsConversationControlTools";
import { contactToolDigest } from "../src/scripts/prepareElevenLabsContactTools";

const secret = "synthetic-unit-test-secret";
const owner = { agent_id: "agent_test", main_branch_id: "branch_test" };
function prepared() {
  const configs = conversationControlTools(secret), payload = { schema_version: 1, voice_only: true,
    agent_id: owner.agent_id, branch_id: owner.main_branch_id, tools: {
      reset: { id: "tool_reset", config_sha256: contactToolDigest(configs.reset) },
      validate: { id: "tool_validate", config_sha256: contactToolDigest(configs.validate) },
    } };
  return { configs, map: { ...payload, map_sha256: contactToolDigest(payload) } };
}

test("control requests are typed, authenticated, system-bound and destination-limited", () => {
  const tools = conversationControlTools(secret);
  for (const tool of Object.values(tools)) assert.deepEqual(tool.assignments, [
    { source: "response", dynamic_variable: "terminal_permission", value_path: "permission", sanitize: true, preserve_native_type: true },
    { source: "response", dynamic_variable: "terminal_decision", value_path: "decision", sanitize: true, preserve_native_type: true },
  ]);
  assert.equal(tools.validate.api_schema.request_headers["x-crisp-elevenlabs-secret"], secret);
  assert.equal(tools.validate.api_schema.url, "https://crisp-voice-bot.onrender.com/elevenlabs/conversation-control/validate-ending");
  assert.equal(tools.validate.api_schema.request_body_schema.properties.history.dynamic_variable, "system__conversation_history");
  assert.equal(tools.validate.api_schema.request_body_schema.properties.conversation_id.dynamic_variable, "system__conversation_id");
  assert.equal(tools.validate.follow_redirects, false);
  assert.throws(() => conversationControlTools(undefined), /authentication/);
});

test("control readback normalization accepts only enumerated neutral response fields", () => {
  const expected = conversationControlTools(secret).validate, actual = structuredClone(expected);
  actual.api_schema.kind = "webhook";
  actual.disable_interruptions = false;
  actual.api_schema.request_body_schema.properties.history.description = "";
  assert.deepEqual(normalizeControlToolReadback(expected, actual), expected);
  for (const change of [
    (value: any) => { value.api_schema.request_body_schema.properties.history.description = "Let the model reconstruct history"; },
    (value: any) => { value.api_schema.url = "https://untrusted.invalid/history"; },
    (value: any) => { value.assignments[0].preserve_native_type = false; },
    (value: any) => { value.assignments.splice(1); },
    (value: any) => { value.assignments[1].value_path = "permission"; },
    (value: any) => { value.api_schema.request_headers["x-crisp-elevenlabs-secret"] = "wrong"; },
    (value: any) => { value.api_schema.request_body_schema.properties.history.constant_value = "forged"; },
    (value: any) => { value.new_provider_field = true; },
  ]) {
    const changed = structuredClone(actual); change(changed);
    assert.throws(() => normalizeControlToolReadback(expected, changed));
  }
});

test("exact owner/map/tool hashes are required before a full release can use controls", async () => {
  const { configs, map } = prepared(), calls: string[] = [];
  const client = { async get(url: string) { calls.push(url); const kind = url.endsWith("tool_reset") ? "reset" : "validate";
    return { data: { id: map.tools[kind].id, tool_config: configs[kind] } }; } };
  assert.deepEqual(await verifyConversationControlMap(client, map, owner, secret, map.map_sha256), {
    resetToolId: "tool_reset", validateToolId: "tool_validate", mainNodeId: "main_conversation",
  });
  assert.equal(calls.length, 2);
  await assert.rejects(verifyConversationControlMap(client, map, { ...owner, agent_id: "agent_other" }, secret), /map required/);
  assert.equal(calls.length, 2);
  await assert.rejects(verifyConversationControlMap(client, map, owner, "wrong"), /contract/);
  const modified = structuredClone(map); modified.tools.validate.id = "tool_other";
  await assert.rejects(verifyConversationControlMap(client, modified, owner, secret), /map required/);
});
