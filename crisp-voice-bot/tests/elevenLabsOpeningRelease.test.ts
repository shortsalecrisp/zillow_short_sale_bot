import assert from "node:assert/strict";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { buildOpeningRelease, openingProtectedHashes, syncOpeningRelease } from "../src/scripts/syncElevenLabsOpeningRelease";
import { bodyDigest, writableBody } from "../src/scripts/syncElevenLabsAgent";

function fixture(): any {
  return { agent_id: "agent_test", branch_id: "agtbrch_test", version_id: "version_before",
    conversation_config: { agent: { first_message: "", prompt: { prompt: "old main", tool_ids: ["tool_protected"] } },
      tts: { model_id: "test_tts", voice_id: "eryn", speed: 0.95 }, turn: { turn_model: "turn_v3", turn_timeout: 1.5 } },
    workflow: { nodes: {
      start_node: { type: "start" },
      opening_listener: { type: "override_agent", entry_behavior: "wait_for_user", edge_order: ["terminal_guard", "opening_to_main"],
        conversation_config: { agent: { first_message: "", prompt: { prompt: "old long opener", tool_ids: ["tool_protected"], built_in_tools: { end_call: null } } } } },
      main_conversation: { type: "override_agent", additional_prompt: "Preserve terminal permission", edge_order: ["terminal_guard"] },
      terminal_guard: { type: "tool", tool_id: "tool_guard" },
    }, edges: { start_to_main: { source: "start_node", target: "opening_listener" },
      opening_to_main: { source: "opening_listener", target: "main_conversation", forward_condition: { type: "llm", condition: "old" } },
      terminal_guard: { source: "opening_listener", target: "terminal_guard", forward_condition: { type: "llm", condition: "opt out" } },
    } }, platform_settings: { protected: true }, phone_numbers: ["owned_number"] };
}

const prompt = "Approved short opener and full conversation rules. ".repeat(6);

test("effective listener and main are released together without touching terminal/tools/voice", () => {
  const current = fixture();
  const body = buildOpeningRelease(current, prompt);
  assert.equal(body.conversation_config.agent.prompt.prompt, prompt);
  assert.notEqual(body.workflow.nodes.opening_listener.conversation_config.agent.prompt.prompt, "old long opener");
  assert.deepEqual(openingProtectedHashes({ ...current, ...body }), openingProtectedHashes(current));
  assert.deepEqual(body.workflow.nodes.opening_listener.edge_order, current.workflow.nodes.opening_listener.edge_order);
  assert.match(body.workflow.nodes.main_conversation.additional_prompt, /Preserve terminal permission/);
});

test("refuses another entry path or protected first message", () => {
  const current = fixture(); current.conversation_config.agent.first_message = "Hello";
  assert.throws(() => buildOpeningRelease(current, prompt), /listen-first/);
});

async function setup(t: any) {
  const dir = await mkdtemp(path.join(os.tmpdir(), "opening-release-test-"));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const current = fixture(); const promptPath = path.join(dir, "prompt.md");
  await writeFile(promptPath, `## Prompt\n${prompt}`);
  return { current, options: { apiKey: "test-only", agentId: current.agent_id, branchId: current.branch_id,
    expectedVersion: current.version_id, expectedBodyHash: bodyDigest(writableBody(current)),
    expectedCandidateHash: bodyDigest(buildOpeningRelease(current, prompt.trim())), promptPath, receiptDir: dir } };
}
const response = (object: any) => ({ ok: true, status: 200, json: async () => structuredClone(object) }) as Response;

test("prepare has no write; apply requires final effective workflow readback", async t => {
  const { current, options } = await setup(t); const methods: string[] = [];
  const fetcher: typeof fetch = async (url, init) => {
    assert.match(String(url), /branch_id=agtbrch_test/); methods.push(String(init?.method));
    if (init?.method === "PATCH") {
      const body = JSON.parse(String(init.body));
      assert.deepEqual(Object.keys(body.conversation_config), ["agent"]);
      current.conversation_config.agent.prompt.prompt = body.conversation_config.agent.prompt.prompt;
      current.workflow = body.workflow; current.version_id = "version_after";
    }
    return response(current);
  };
  assert.equal((await syncOpeningRelease(options, fetcher)).status, "prepared_not_applied");
  assert.deepEqual(methods, ["GET"]); methods.length = 0;
  assert.equal((await syncOpeningRelease({ ...options, apply: true }, fetcher)).status, "live_opening_and_workflow_verified_audio_not_verified");
  assert.deepEqual(methods, ["GET", "GET", "PATCH", "GET"]);
});

test("stale opening readback cannot pass on main prompt alone", async t => {
  const { current, options } = await setup(t);
  await assert.rejects(syncOpeningRelease({ ...options, apply: true }, async (_url, init) => {
    if (init?.method === "PATCH") current.conversation_config.agent.prompt.prompt = prompt.trim();
    return response(current);
  }), /readback differs/);
});

test("version or candidate drift blocks PATCH", async t => {
  const { current, options } = await setup(t);
  await assert.rejects(syncOpeningRelease({ ...options, apply: true, expectedCandidateHash: "0".repeat(64) }, async (_url, init) => {
    assert.equal(init?.method, "GET"); return response(current);
  }), /Candidate does not match/);
  current.version_id = "other";
  await assert.rejects(syncOpeningRelease(options, async () => response(current)), /drifted/);
});

test("ambiguous provider write is never automatically retried", async t => {
  const { current, options } = await setup(t); let writes = 0;
  await assert.rejects(syncOpeningRelease({ ...options, apply: true }, async (_url, init) => {
    if (init?.method === "PATCH") { writes++; throw new Error("timeout"); }
    return response(current);
  }), /timeout/);
  assert.equal(writes, 1);
  assert.equal(JSON.parse(await readFile(path.join(options.receiptDir, "receipt.json"), "utf8")).status, "needs_readback_review_do_not_repeat_patch");
});
