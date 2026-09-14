import assert from "node:assert/strict";
import test from "node:test";
import { mkdtemp, readFile, readdir, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { applyContactToolDescriptions } from "../src/lib/elevenLabsConversationPolicy";
import {
  contactToolDigest, prepareContactTools, replaceContactToolBindings, verifyContactDescriptionOnlyChange,
  verifyContactToolMap, type ContactToolPreparationOptions,
} from "../src/scripts/prepareElevenLabsContactTools";
import { buildConversationRelease } from "../src/scripts/syncElevenLabsAgent";

const secret = "synthetic-transport-secret-never-in-receipts";
function tool(name: string): any {
  return { type: "webhook", name, description: "Old description", response_timeout_secs: 20,
    disable_interruptions: true, execution_mode: "immediate", follow_redirects: false,
    dynamic_variables: { dynamic_variable_placeholders: {} },
    api_schema: { url: `https://webhook.invalid/${name}?token=${secret}`, method: "POST",
      request_headers: { Authorization: `Bearer ${secret}` }, auth_connection: { secret_id: secret },
      request_body_schema: { type: "object", description: "Old schema", required: ["rowNumber"], properties: {
        rowNumber: { type: "integer", dynamic_variable: "rowNumber" },
        callbackTime: { type: "string", description: "Old timing" },
        conversationSummary: { type: "string", description: "Old summary" },
        token: { type: "string", constant_value: secret },
      } }, response_body_schema: { type: "object" }, response_filter: { unchanged: true } },
  };
}

async function fixture(t: any) {
  const dir = await mkdtemp(path.join(os.tmpdir(), "contact-tool-clone-"));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const ids = { callback_requested: "tool_callback", not_interested: "tool_contact", information_requested: "tool_information" };
  const configs = new Map<string, any>(Object.entries(ids).map(([name, id]) => [id, tool(name)]));
  const agent: any = {
    agent_id: "agent_test", branch_id: "branch_main", main_branch_id: "branch_main", version_id: "version_reviewed",
    conversation_config: {
      agent: { first_message: "Unchanged opening", prompt: { prompt: "Current prompt", llm: "unchanged-model",
        tool_ids: ["tool_other", ids.callback_requested, ids.not_interested, ids.information_requested],
        tools: [...configs.values()].map(value => structuredClone(value)),
        built_in_tools: { end_call: { description: "old" }, skip_turn: { description: "old" } } } },
      tts: { voice_id: "voice_unchanged", speed: 0.95 }, turn: { turn_timeout: 1.5 },
      conversation: { client_events: ["audio", "user_transcript"] },
    },
    workflow: { nodes: {
      main_conversation: { additional_tool_ids: [ids.callback_requested, ids.information_requested] },
      patch_transfer: { edge_order: ["patch_to_phone"], additional_prompt: "Old" },
      callback_after_unavailable: { additional_tool_ids: [ids.not_interested] },
      transfer_check: { tools: [{ tool_id: "tool_other" }, { tool_id: ids.callback_requested }] },
      phone_transfer: { transfer_destination: { phone_number: "+12025550123" } },
    }, edges: {
      main_to_patch_after_accepted_result: { source: "main_conversation", target: "patch_transfer" },
      main_to_callback_after_unavailable_result: { source: "main_conversation", target: "callback_after_unavailable" },
      main_to_transfer_check: { source: "main_conversation", target: "transfer_check" },
      transfer_check_to_patch: { source: "transfer_check", target: "patch_transfer" },
      transfer_check_to_callback: { source: "transfer_check", target: "callback_after_unavailable" },
      patch_to_phone: { source: "patch_transfer", target: "phone_transfer" },
    } }, platform_settings: { protected: true }, phone_numbers: [], whatsapp_accounts: [], procedures: {},
  };
  const options: ContactToolPreparationOptions = { receiptDir: dir, toolIds: ids,
    identity: { agent_id: agent.agent_id, branch_id: agent.branch_id, main_branch_id: agent.main_branch_id, version_id: agent.version_id } };
  const calls: Array<{ method: string; endpoint: string; body?: any }> = [];
  let postBehavior: ((id: string) => void) | undefined;
  const client = {
    async get(endpoint: string) {
      calls.push({ method: "GET", endpoint });
      if (endpoint === "/v1/convai/agents/agent_test") return { data: structuredClone(agent) };
      assert.match(endpoint, /^\/v1\/convai\/tools\/tool_/);
      const id = endpoint.split("/").at(-1)!;
      assert.ok(configs.has(id), "Only known synthetic tool IDs may be read");
      return { data: { id, tool_config: structuredClone(configs.get(id)), access_info: { do_not_copy: secret }, usage_stats: { calls: 1 } } };
    },
    async post(endpoint: string, body: any) {
      assert.equal(endpoint, "/v1/convai/tools", "Only tool creation is available, never a shared-tool or agent write");
      const marker = JSON.parse(await readFile(path.join(dir, "create-attempt.json"), "utf8"));
      assert.equal(marker.status, "creation_attempt_started_no_automatic_retry");
      const journal = (await readFile(path.join(dir, "journal.jsonl"), "utf8")).trim().split("\n").map(line => JSON.parse(line));
      assert.equal(journal.at(-1).event, "create_intent", "A durable intent must exist before each POST");
      assert.equal(journal.at(-1).name, body.tool_config.name);
      assert.deepEqual(Object.keys(body), ["tool_config"], "GET envelope metadata must not be copied");
      calls.push({ method: "POST", endpoint, body: structuredClone(body) });
      const id = `tool_clone_${calls.filter(call => call.method === "POST").length}`;
      configs.set(id, structuredClone(body.tool_config));
      postBehavior?.(id);
      return { data: { id } };
    },
  };
  return { dir, ids, configs, agent, options, calls, client, setPostBehavior: (behavior: typeof postBehavior) => { postBehavior = behavior; } };
}

async function createFixture(t: any) {
  const state = await fixture(t);
  const dry = await prepareContactTools(state.options, state.client);
  const result = await prepareContactTools({ ...state.options, create: true, expectedPlanHash: dry.plan_sha256 }, state.client);
  const map = JSON.parse(await readFile(result.map_path, "utf8"));
  return { ...state, dry, result, map };
}

test("dry-run defaults to fresh identity and three exact tool reads; receipts contain no transport secrets", async t => {
  const state = await fixture(t);
  const result = await prepareContactTools(state.options, state.client);
  assert.equal(result.status, "prepared_not_created");
  assert.deepEqual(state.calls.map(call => call.method), ["GET", "GET", "GET", "GET", "GET"]);
  assert.deepEqual(state.calls.slice(2).map(call => call.endpoint), ["/v1/convai/tools/tool_callback", "/v1/convai/tools/tool_contact", "/v1/convai/tools/tool_information"]);
  assert.deepEqual(await readdir(state.dir), ["plan.json"]);
  const plan = await readFile(path.join(state.dir, "plan.json"), "utf8");
  assert.ok(!plan.includes(secret));
  assert.ok(!plan.includes("webhook.invalid"));
  assert.equal(JSON.parse(plan).plan_sha256, result.plan_sha256);
});

test("creation requires the exact reviewed plan and fresh expected main/version/binding identity", async t => {
  const state = await fixture(t);
  await assert.rejects(prepareContactTools({ ...state.options, create: true }, state.client), /reviewed SHA256/);
  await assert.rejects(prepareContactTools({ ...state.options, create: true, expectedPlanHash: "0".repeat(64) }, state.client), /changed since review/);
  state.agent.version_id = "version_changed";
  await assert.rejects(prepareContactTools(state.options, state.client), /identity drifted/);
  state.agent.version_id = state.options.identity.version_id;
  state.agent.main_branch_id = "branch_other";
  await assert.rejects(prepareContactTools(state.options, state.client), /identity drifted/);
  state.agent.main_branch_id = state.options.identity.main_branch_id;
  state.agent.conversation_config.agent.prompt.tool_ids.pop();
  await assert.rejects(prepareContactTools(state.options, state.client), /attached exactly once/);
  assert.equal(state.calls.filter(call => call.method === "POST").length, 0);
});

test("description guard rejects transport, auth, schema and behavior changes outside the exact allowed leaves", () => {
  const before = tool("callback_requested");
  const desired = applyContactToolDescriptions(before);
  assert.equal(verifyContactDescriptionOnlyChange(before, desired).length, 4);
  for (const name of ["not_interested", "information_requested"]) {
    const original = tool(name);
    assert.equal(verifyContactDescriptionOnlyChange(original, applyContactToolDescriptions(original)).length, 3);
  }
  for (const mutate of [
    (value: any) => { value.api_schema.url = "https://other.invalid"; },
    (value: any) => { value.api_schema.request_headers.Authorization = "different"; },
    (value: any) => { value.api_schema.request_body_schema.required.push("email"); },
    (value: any) => { value.api_schema.request_body_schema.properties.rowNumber.type = "string"; },
    (value: any) => { value.api_schema.request_body_schema.properties.rowNumber.description = "Unreviewed metadata change"; },
    (value: any) => { value.response_timeout_secs = 90; },
    (value: any) => { value.disable_interruptions = false; },
    (value: any) => { value.name = "information_requested"; },
  ]) {
    const drifted = structuredClone(desired); mutate(drifted);
    assert.throws(() => verifyContactDescriptionOnlyChange(before, drifted), /outside the allowed description paths/);
  }
  assert.throws(() => verifyContactDescriptionOnlyChange({ ...before, type: "client" }, desired), /webhook/);
});

test("exactly three new clones preserve full config in memory and produce a durable secret-free candidate map", async t => {
  const state = await createFixture(t);
  const posts = state.calls.filter(call => call.method === "POST");
  assert.equal(posts.length, 3);
  for (const [index, name] of ["callback_requested", "not_interested", "information_requested"].entries()) {
    assert.deepEqual(posts[index].body.tool_config, applyContactToolDescriptions(tool(name)));
    assert.deepEqual(state.configs.get(Object.values(state.ids)[index]), tool(name), "Shared source tool is unchanged");
  }
  assert.deepEqual(state.result.replacements, { tool_callback: "tool_clone_1", tool_contact: "tool_clone_2", tool_information: "tool_clone_3" });
  assert.deepEqual(await verifyContactToolMap(state.client, state.map, state.agent, state.result.map_sha256), state.result.replacements);
  for (const file of await readdir(state.dir)) assert.ok(!(await readFile(path.join(state.dir, file), "utf8")).includes(secret), file);
  await assert.rejects(prepareContactTools({ ...state.options, create: true, expectedPlanHash: state.dry.plan_sha256 }, state.client), /already attempted/);
  assert.equal(state.calls.filter(call => call.method === "POST").length, 3);
});

test("ambiguous creation is journaled once and cannot be retried automatically", async t => {
  const state = await fixture(t);
  const dry = await prepareContactTools(state.options, state.client);
  state.setPostBehavior(() => { throw new Error(`Synthetic timeout with sensitive data ${secret}`); });
  const options = { ...state.options, create: true, expectedPlanHash: dry.plan_sha256 };
  await assert.rejects(prepareContactTools(options, state.client), /reconcile the journal and do not retry/);
  await assert.rejects(prepareContactTools(options, state.client), /already attempted/);
  assert.equal(state.calls.filter(call => call.method === "POST").length, 1);
  assert.ok(!(await readdir(state.dir)).includes("candidate-map.json"));
  const journal = await readFile(path.join(state.dir, "journal.jsonl"), "utf8");
  assert.match(journal, /manual_reconciliation_required_do_not_retry/);
  assert.ok(!journal.includes(secret));
});

test("clone readback drift retains its created ID and stops before the second clone", async t => {
  const state = await fixture(t);
  const dry = await prepareContactTools(state.options, state.client);
  state.setPostBehavior(id => { state.configs.get(id).api_schema.method = "GET"; });
  await assert.rejects(prepareContactTools({ ...state.options, create: true, expectedPlanHash: dry.plan_sha256 }, state.client), /uncertain or incomplete/);
  assert.equal(state.calls.filter(call => call.method === "POST").length, 1);
  const journal = (await readFile(path.join(state.dir, "journal.jsonl"), "utf8")).trim().split("\n").map(line => JSON.parse(line));
  assert.ok(journal.some(entry => entry.event === "created_id_readback_required" && entry.new_id === "tool_clone_1"));
  assert.ok(!(await readdir(state.dir)).includes("candidate-map.json"));
});

test("map verification rejects stale originals, clones, versions, bindings and review hashes", async t => {
  const state = await createFixture(t);
  await assert.rejects(verifyContactToolMap(state.client, state.map, state.agent, "0".repeat(64)), /map hash mismatch/);
  for (const id of ["tool_callback", "tool_clone_1"]) {
    const before = structuredClone(state.configs.get(id));
    state.configs.get(id).api_schema.request_headers.Authorization = "drifted";
    await assert.rejects(verifyContactToolMap(state.client, state.map, state.agent), /config drifted/);
    state.configs.set(id, before);
  }
  const stale = structuredClone(state.agent); stale.version_id = "other_version";
  await assert.rejects(verifyContactToolMap(state.client, state.map, stale), /identity drifted/);
  const rebound = structuredClone(state.agent); rebound.workflow.nodes.main_conversation.additional_tool_ids = [];
  await assert.rejects(verifyContactToolMap(state.client, state.map, rebound), /configuration changed/);
  const partial = structuredClone(state.map); delete partial.replacements.tool_contact;
  const { map_sha256: _hash, ...payload } = partial; partial.map_sha256 = contactToolDigest(payload);
  await assert.rejects(verifyContactToolMap(state.client, partial, state.agent), /exactly three/);
});

test("binding replacement changes only the three IDs at global and workflow binding paths", async t => {
  const state = await fixture(t);
  const before = structuredClone(state.agent);
  const replacements = { tool_callback: "tool_clone_1", tool_contact: "tool_clone_2", tool_information: "tool_clone_3" };
  const remapped = replaceContactToolBindings(before, replacements);
  assert.deepEqual(remapped.conversation_config.agent.prompt.tool_ids, ["tool_other", "tool_clone_1", "tool_clone_2", "tool_clone_3"]);
  assert.equal(remapped.workflow.nodes.transfer_check.tools[0].tool_id, "tool_other");
  assert.equal(remapped.workflow.nodes.transfer_check.tools[1].tool_id, "tool_clone_1");
  assert.deepEqual(remapped.workflow.nodes.callback_after_unavailable.additional_tool_ids, ["tool_clone_2"]);
  assert.deepEqual(remapped.workflow.nodes.main_conversation.additional_tool_ids, ["tool_clone_1", "tool_clone_3"]);
  assert.deepEqual(replaceContactToolBindings(remapped, { tool_clone_1: "tool_callback", tool_clone_2: "tool_contact", tool_clone_3: "tool_information" }), before);
  assert.deepEqual(state.agent, before);
  const unsupported = structuredClone(before); unsupported.workflow.nodes.main_conversation.unrecognized_reference = "tool_callback";
  assert.throws(() => replaceContactToolBindings(unsupported, replacements), /unsupported|outside a supported binding/);
  const release = buildConversationRelease(before, "Approved prompt", true, replacements);
  assert.deepEqual(release, replaceContactToolBindings(buildConversationRelease(before, "Approved prompt", true), replacements));
  assert.equal(release.conversation_config.agent.prompt.tools, undefined);
  assert.deepEqual(release.conversation_config.tts, before.conversation_config.tts);
});

test("sync retains body/version guards and verifies the reviewed map before and after PATCH", async () => {
  const source = await readFile(path.resolve(__dirname, "../src/scripts/syncElevenLabsAgent.ts"), "utf8");
  assert.match(source, /value\("contact-tool-map"\)/);
  assert.match(source, /value\("expected-contact-tool-map-sha"\)/);
  assert.match(source, /Applying a contact-tool map requires its exact reviewed map SHA256/);
  assert.match(source, /verifyContactToolMap\(client, contactToolMap, check, expectedContactToolMap\)/);
  assert.match(source, /Candidate changed since review; nothing applied/);
  assert.match(source, /enable_versioning_if_not_enabled: true/);
});
