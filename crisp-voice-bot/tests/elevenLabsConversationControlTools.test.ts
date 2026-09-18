import assert from "node:assert/strict";
import test from "node:test";
import { mkdtemp, readFile, readdir, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { conversationControlTools, normalizeControlToolReadback, verifyConversationControlMap } from "../src/scripts/elevenLabsConversationControlTools";
import { contactToolDigest } from "../src/scripts/prepareElevenLabsContactTools";
import { prepareConversationControlTools } from "../src/scripts/prepareElevenLabsConversationControlTools";

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
  actual.api_schema.mtls_auth_connection = null;
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

async function fixture(t: any) {
  const dir = await mkdtemp(path.join(os.tmpdir(), "conversation-control-tools-"));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const agent = { agent_id: "agent_test", branch_id: "branch_test", main_branch_id: "branch_test" };
  const configs = new Map<string, any>();
  const calls: Array<{ method: string; endpoint: string; body?: any }> = [];
  let postBehavior: ((id: string) => void) | undefined;
  const client = {
    async get(endpoint: string) {
      calls.push({ method: "GET", endpoint });
      if (endpoint === "/v1/convai/agents/agent_test") return { data: structuredClone(agent) };
      assert.match(endpoint, /^\/v1\/convai\/tools\/tool_/);
      const id = endpoint.split("/").at(-1)!;
      assert.ok(configs.has(id));
      return { data: { id, tool_config: structuredClone(configs.get(id)) } };
    },
    async post(endpoint: string, body: any) {
      calls.push({ method: "POST", endpoint, body: structuredClone(body) });
      assert.equal(endpoint, "/v1/convai/tools");
      const marker = JSON.parse(await readFile(path.join(dir, "create-attempt.json"), "utf8"));
      assert.equal(marker.status, "creation_attempt_started_no_automatic_retry");
      const id = `tool_${body.tool_config.name === "reset_call_end_permission" ? "reset" : "validate"}`;
      const config = structuredClone(body.tool_config);
      config.api_schema.kind = "webhook";
      config.api_schema.request_body_schema.properties.conversation_id.description = "";
      if (config.api_schema.request_body_schema.properties.history) {
        config.api_schema.request_body_schema.properties.history.description = "";
      }
      configs.set(id, config);
      postBehavior?.(id);
      return { data: { id } };
    },
  };
  return { dir, agent, client, calls, configs, setPostBehavior: (behavior: typeof postBehavior) => { postBehavior = behavior; } };
}

test("conversation-control preparation creates a verified map with durable receipts", async t => {
  const state = await fixture(t);
  const identity = { agent_id: state.agent.agent_id, branch_id: state.agent.branch_id, main_branch_id: state.agent.main_branch_id };
  const dry = await prepareConversationControlTools({ identity, receiptDir: state.dir }, state.client, secret);
  assert.equal(dry.status, "prepared_not_created");
  assert.deepEqual(await readdir(state.dir), ["plan.json"]);
  const result = await prepareConversationControlTools({ identity, receiptDir: state.dir, create: true,
    expectedPlanHash: dry.plan_sha256 }, state.client, secret);
  assert.equal(result.status, "controls_verified_not_attached");
  const map = JSON.parse(await readFile(result.map_path, "utf8"));
  assert.deepEqual(await verifyConversationControlMap(state.client, map, state.agent, secret, result.map_sha256), {
    resetToolId: "tool_reset", validateToolId: "tool_validate", mainNodeId: "main_conversation",
  });
  assert.equal(state.calls.filter(call => call.method === "POST").length, 2);
  const journal = await readFile(path.join(state.dir, "journal.jsonl"), "utf8");
  assert.match(journal, /candidate_map_verified_not_attached/);
  assert.ok(!journal.includes(secret));
  assert.ok(!JSON.stringify(map).includes(secret));
  await assert.rejects(prepareConversationControlTools({ identity, receiptDir: state.dir, create: true,
    expectedPlanHash: dry.plan_sha256 }, state.client, secret), /already attempted/);
});

test("conversation-control creation stops after uncertain provider readback", async t => {
  const state = await fixture(t);
  const identity = { agent_id: state.agent.agent_id, branch_id: state.agent.branch_id, main_branch_id: state.agent.main_branch_id };
  const dry = await prepareConversationControlTools({ identity, receiptDir: state.dir }, state.client, secret);
  state.setPostBehavior(id => { state.configs.get(id).api_schema.url = "https://wrong.invalid"; });
  await assert.rejects(prepareConversationControlTools({ identity, receiptDir: state.dir, create: true,
    expectedPlanHash: dry.plan_sha256 }, state.client, secret), /uncertain or incomplete/);
  assert.equal(state.calls.filter(call => call.method === "POST").length, 1);
  assert.ok(!(await readdir(state.dir)).includes("candidate-map.json"));
  const journal = await readFile(path.join(state.dir, "journal.jsonl"), "utf8");
  assert.match(journal, /manual_reconciliation_required_do_not_retry/);
});
