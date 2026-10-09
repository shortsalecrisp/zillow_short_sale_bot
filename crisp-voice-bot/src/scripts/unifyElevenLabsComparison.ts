import axios from "axios";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import path from "node:path";
import { bodyDigest, writableBody, extractPromptSection, verifyReleaseReadback } from "./syncElevenLabsAgent";
import { conversationControlTools, normalizeControlToolReadback } from "./elevenLabsConversationControlTools";
import { applyContactToolDescriptions, applyConversationListeningPolicy, applyConversationToolPolicy, applyConversationConsentPolicy } from "../lib/elevenLabsConversationPolicy";
import { applyConversationOpeningWorkflow, VOICE_STATE_TRANSITION_NODES } from "../lib/elevenLabsOpeningWorkflow";
import { ELEVENLABS_TTS_CONTROL_BRANCH_ID, ELEVENLABS_TTS_TEST_BRANCH_ID, ELEVENLABS_TTS_CONTROL_MODEL, ELEVENLABS_TTS_TEST_MODEL } from "../lib/elevenLabsRuntimeExperiment";

export function comparisonBody(body: any): any {
  const result = structuredClone(body);
  delete result.conversation_config.tts.model_id;
  return result;
}

export function assertUniformComparison(control: any, test: any): void {
  if (control.conversation_config.tts.model_id !== ELEVENLABS_TTS_CONTROL_MODEL
    || test.conversation_config.tts.model_id !== ELEVENLABS_TTS_TEST_MODEL
    || bodyDigest(comparisonBody(control)) !== bodyDigest(comparisonBody(test))) {
    throw new Error("The TTS model must be the only difference between comparison arms");
  }
}

export function buildUniformRelease(owner: any, prompt: string): any {
  let body = applyConversationToolPolicy(applyConversationConsentPolicy(applyConversationListeningPolicy(writableBody(owner), { listenFirst: true })), { guardedEnding: true });
  body.conversation_config.agent.prompt.prompt = prompt;
  body.conversation_config.agent.prompt.llm = "gpt-4.1";
  body = applyConversationOpeningWorkflow(body);
  // Keep the existing provider-verified terminal guards; extend their exact entry conditions to the two new dialogue states.
  for (const id of Object.values(VOICE_STATE_TRANSITION_NODES)) {
    const node = body.workflow.nodes[id];
    node.entry_behavior = "auto";
    node.conversation_config.agent.prompt.built_in_tools = {
      ...Object.fromEntries(Object.keys(owner.workflow.nodes.terminal_guard_wait.conversation_config.agent.prompt.built_in_tools).map(key => [key, null])),
      ...node.conversation_config.agent.prompt.built_in_tools,
    };
    for (const kind of ["contact", "guard"]) {
      const key = `terminal_${kind}_from_${id}`;
      const existing = body.workflow.edges[`terminal_${kind}_from_main_conversation`];
      if (!existing) throw new Error("Existing guarded main entry required");
      body.workflow.edges[key] = { ...structuredClone(existing), source: id };
    }
    node.edge_order = [`terminal_contact_from_${id}`, `terminal_guard_from_${id}`, ...node.edge_order];
  }
  body.workflow.nodes.unrelated_recording_exit.outcome = "success";
  body.workflow.nodes.unrelated_recording_exit.exit_slot = null;
  for (const [id, node] of Object.entries(body.workflow.nodes) as [string, any][]) {
    const guards = [`terminal_contact_from_${id}`, `terminal_guard_from_${id}`].filter(key => body.workflow.edges[key]);
    const outgoing = Object.entries(body.workflow.edges).filter(([, edge]: any) => edge.source === id).map(([key]) => key);
    node.edge_order = [...new Set([...guards, ...(node.edge_order ?? []), ...outgoing])];
  }
  return body;
}

async function main(): Promise<void> {
  const dir = process.argv.find(a => a.startsWith("--receipt-dir="))?.split("=")[1];
  if (!dir) throw new Error("Private receipt directory required");
  const { config } = await import("../lib/config");
  const client = axios.create({ baseURL: "https://api.elevenlabs.io", headers: { "xi-api-key": config.elevenLabs.apiKey }, timeout: 30000 });
  const ids = [ELEVENLABS_TTS_CONTROL_BRANCH_ID, ELEVENLABS_TTS_TEST_BRANCH_ID];
  const endpoint = (id: string) => `/v1/convai/agents/${config.elevenLabs.agentId}?branch_id=${id}`;
  const before = await Promise.all(ids.map(async id => (await client.get(endpoint(id))).data));
  const traffic = async () => {
    const { data } = await client.get(`/v1/convai/agents/${config.elevenLabs.agentId}/branches`);
    for (const id of ids) if (!data.results?.some((b: any) => b.id === id && b.current_live_percentage === 50 && !b.is_archived)) {
      throw new Error("Existing native 50/50 traffic split required");
    }
  };
  await traffic();
  for (let i = 0; i < ids.length; i++) {
    const reviewed = JSON.parse(await readFile(path.join(dir, `${ids[i]}.json`), "utf8"));
    if (before[i].version_id !== reviewed.version_id || bodyDigest(before[i]) !== bodyDigest(reviewed)) throw new Error("Reviewed provider snapshot drifted");
  }
  const owner = before[0];
  const toolId = (node: string) => owner.workflow.nodes[node]?.tools?.[0]?.tool_id;
  const control = { resetToolId: toolId("terminal_guard_reset"), validateToolId: toolId("terminal_guard_validate"),
    contactToolId: toolId("terminal_guard_record_contact"), mainNodeId: "main_conversation" };
  const expected = conversationControlTools(config.elevenLabs.toolSecret);
  for (const [kind, id] of [["reset", control.resetToolId], ["validate", control.validateToolId]] as const) {
    if (!id || !owner.conversation_config.agent.prompt.tool_ids.includes(id)) throw new Error("Unregistered control tool");
    const { data } = await client.get(`/v1/convai/tools/${id}`);
    if (data.id !== id) throw new Error("Control identity mismatch");
    normalizeControlToolReadback(expected[kind], data.tool_config);
  }
  const { data: contact } = await client.get(`/v1/convai/tools/${control.contactToolId}`);
  if (contact.id !== control.contactToolId || contact.tool_config?.name !== "not_interested"
    || bodyDigest(contact.tool_config) !== bodyDigest(applyContactToolDescriptions(contact.tool_config))) throw new Error("Contact policy mismatch");
  if (bodyDigest(owner.conversation_config.agent.prompt.tool_ids) !== bodyDigest(before[1].conversation_config.agent.prompt.tool_ids)) throw new Error("Tool bindings differ; separate review required");
  const prompt = extractPromptSection(await readFile(path.resolve(__dirname, "../../docs/elevenlabs-agent-prompt.md"), "utf8"));
  const shared = buildUniformRelease(owner, prompt);
  const candidates = [ELEVENLABS_TTS_CONTROL_MODEL, ELEVENLABS_TTS_TEST_MODEL].map(model => {
    const body = structuredClone(shared); body.conversation_config.tts.model_id = model; return body;
  });
  assertUniformComparison(...candidates as [any, any]);
  await mkdir(dir, { recursive: true, mode: 0o700 });
  const plan = { schemaVersion: 1, branches: ids, beforeVersions: before.map(b => b.version_id),
    beforeHashes: before.map(bodyDigest), candidateHashes: candidates.map(bodyDigest), sharedHash: bodyDigest(comparisonBody(shared)) };
  const planPath = path.join(dir, "uniform-plan.json");
  if (!process.argv.includes("--apply")) {
    await writeFile(planPath, JSON.stringify(plan, null, 2), { mode: 0o600 });
    console.log(JSON.stringify(plan)); return;
  }
  if (bodyDigest(JSON.parse(await readFile(planPath, "utf8"))) !== bodyDigest(plan)) throw new Error("Dry-run plan changed");
  // Recheck both arms before the first write; never retry an ambiguous provider write.
  for (let i = 0; i < ids.length; i++) if (bodyDigest((await client.get(endpoint(ids[i]))).data) !== plan.beforeHashes[i]) throw new Error("Provider drift before apply");
  const after: any[] = [];
  for (let i = 0; i < ids.length; i++) {
    await client.patch(endpoint(ids[i]) + "&enable_versioning_if_not_enabled=true", candidates[i]);
    const live = (await client.get(endpoint(ids[i]))).data;
    await writeFile(path.join(dir, `${ids[i]}-after.json`), JSON.stringify(live), { mode: 0o600 });
    verifyReleaseReadback(before[i], candidates[i], live);
    after.push(live);
  }
  for (let i = 0; i < ids.length; i++) {
    const fresh = (await client.get(endpoint(ids[i]))).data;
    if (fresh.version_id !== after[i].version_id) throw new Error("Concurrent provider change after apply");
    verifyReleaseReadback(before[i], candidates[i], fresh);
  }
  assertUniformComparison(writableBody(after[0]), writableBody(after[1]));
  await traffic();
  const receipt = { ...plan, verifiedAt: new Date().toISOString(), afterVersions: after.map(b => b.version_id),
    promptHash: bodyDigest(prompt), workflowHash: bodyDigest(after[0].workflow), toolsUnchanged: true, trafficPercent: [50, 50] };
  await writeFile(path.join(dir, "uniform-receipt.json"), JSON.stringify(receipt, null, 2), { mode: 0o600 });
  console.log(JSON.stringify(receipt));
}

if (require.main === module) main().catch(error => { console.error(error instanceof Error ? error.message : "Uniform release failed"); process.exitCode = 1; });
