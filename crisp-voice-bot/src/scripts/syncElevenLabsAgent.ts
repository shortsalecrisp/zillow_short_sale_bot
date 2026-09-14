import axios from "axios";
import { createHash } from "node:crypto";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import path from "node:path";
import {
  applyConversationConsentPolicy,
  applyConversationListeningPolicy,
  applyConversationToolPolicy,
  VOICE_CONVERSATION_POLICY_VERSION,
} from "../lib/elevenLabsConversationPolicy";
import { replaceContactToolBindings, verifyContactToolMap } from "./prepareElevenLabsContactTools";
import { applyConversationOpeningWorkflow } from "../lib/elevenLabsOpeningWorkflow";
import { applyGuardedTerminalWorkflow } from "../lib/elevenLabsTerminalWorkflow";
import { verifyConversationControlMap } from "./elevenLabsConversationControlTools";

const PROMPT_PATH = path.resolve(__dirname, "../../docs/elevenlabs-agent-prompt.md");
const digest = (value: string) => createHash("sha256").update(value).digest("hex");
export function canonical(value: any): any {
  if (Array.isArray(value)) return value.map(canonical);
  if (value && typeof value === "object") return Object.fromEntries(Object.keys(value).sort().map(key => [key, canonical(value[key])]));
  return value;
}
export const bodyDigest = (value: unknown) => digest(JSON.stringify(canonical(value)));

export function writableBody(agent: any) {
  const body = structuredClone({ conversation_config: agent.conversation_config, workflow: agent.workflow });
  // Expanded GET-only tool schemas can contain webhook credentials. Referenced IDs remain unchanged.
  delete body.conversation_config.agent.prompt.tools;
  return body;
}

export function safeReceipt(value: any): any {
  if (Array.isArray(value)) return value.map(safeReceipt);
  if (value && typeof value === "object") return Object.fromEntries(Object.entries(value).map(([key, item]) => [key,
    /^(?:request_headers|authorization|api_key|xi-api-key|password|secret|shareable_token)$/i.test(key) ? "[redacted]" : safeReceipt(item)]));
  return value;
}

export function verifyReleaseReadback(before: any, candidate: any, after: any): void {
  if (bodyDigest(candidate) !== bodyDigest(writableBody(after))) {
    throw new Error("Conversation policy readback mismatch after write");
  }
  for (const key of ["platform_settings", "phone_numbers", "whatsapp_accounts", "procedures"]) {
    if (bodyDigest(before[key] ?? null) !== bodyDigest(after[key] ?? null)) throw new Error(`Unrelated provider field drifted: ${key}`);
  }
}

export function extractPromptSection(markdown: string): string {
  const parts = markdown.split("## Prompt\n");
  if (parts.length !== 2) throw new Error("Exactly one prompt section is required");
  return parts[1].trim();
}

export function buildConversationRelease(current: any, prompt: string, listenFirst: boolean, contactToolReplacements?: Record<string, string>,
  control?: { resetToolId: string; validateToolId: string; mainNodeId: string }) {
  // Start from the owning provider, never a stale reconstructed model/tool config.
  let body = applyConversationToolPolicy(applyConversationConsentPolicy(applyConversationListeningPolicy(writableBody(current), { listenFirst })), { guardedEnding: Boolean(control) });
  body.conversation_config.agent.prompt.prompt = prompt;
  if (control) {
    body = applyGuardedTerminalWorkflow(applyConversationOpeningWorkflow(body), control);
    // GET adds these neutral base slots; override prompts must retain inheritance.
    const builtIns = body.conversation_config.agent.prompt.built_in_tools;
    for (const name of ["transfer_to_agent", "update_state", "memory_entry_search", "run_subagent", "request_help",
      "language_detection", "transfer_to_number", "play_keypad_touch_tone", "voicemail_detection",
      "transfer_to_genesys", "transfer_to_genesys_chat", "transfer_to_genesys_bot"]) {
      if (!Object.hasOwn(builtIns, name)) builtIns[name] = null;
    }
  }
  return contactToolReplacements ? replaceContactToolBindings(body, contactToolReplacements) : body;
}

async function main(): Promise<void> {
  const { config } = await import("../lib/config");
  const { apiKey, agentId, branchId } = config.elevenLabs;
  if (!apiKey || !agentId || !branchId) throw new Error("ElevenLabs key, agent and branch configuration required");
  const value = (name: string) => process.argv.find(arg => arg.startsWith(`--${name}=`))?.slice(name.length + 3);
  const apply = process.argv.includes("--apply");
  const expectedVersion = value("expected-version");
  const expectedPrompt = value("expected-prompt-sha");
  const expectedCandidate = value("expected-candidate-sha");
  const contactToolMapPath = value("contact-tool-map");
  const expectedContactToolMap = value("expected-contact-tool-map-sha");
  const controlMapPath = value("conversation-control-map");
  const expectedControlMap = value("expected-conversation-control-map-sha");
  const listenFirst = process.argv.includes("--listen-first");
  const receiptDir = path.resolve(value("receipt-dir") ?? "tmp/voice-conversation-release");
  if (apply && (!expectedVersion || !expectedPrompt || !expectedCandidate)) {
    throw new Error("Apply requires the reviewed live version and prompt SHA guards");
  }
  if (apply && Number(listenFirst) + Number(process.argv.includes("--keep-first-message")) !== 1) {
    throw new Error("Apply requires exactly one reviewed startup choice");
  }
  if ((expectedContactToolMap && !contactToolMapPath) || (apply && contactToolMapPath && !expectedContactToolMap)) {
    throw new Error("Applying a contact-tool map requires its exact reviewed map SHA256");
  }
  if ((expectedControlMap && !controlMapPath) || (apply && (!controlMapPath || !expectedControlMap || !listenFirst))) {
    throw new Error("Applying the full candidate requires a reviewed conversation-control map and listen-first startup");
  }
  const client = axios.create({
    baseURL: config.elevenLabs.baseUrl, timeout: 45_000,
    headers: { "Content-Type": "application/json", "xi-api-key": apiKey },
  });
  const endpoint = `/v1/convai/agents/${agentId}`;
  const params = { branch_id: branchId };
  const { data: current } = await client.get(endpoint, { params });
  if (current.agent_id !== agentId || current.branch_id !== branchId || current.main_branch_id !== branchId) {
    throw new Error("Expected the configured agent's effective main branch");
  }
  const beforePrompt = current.conversation_config.agent.prompt.prompt;
  if (expectedVersion && current.version_id !== expectedVersion) throw new Error("Live agent version drifted; review again");
  if (expectedPrompt && digest(beforePrompt) !== expectedPrompt) throw new Error("Live prompt drifted; review again");
  const contactToolMap = contactToolMapPath ? JSON.parse(await readFile(path.resolve(contactToolMapPath), "utf8")) : undefined;
  const contactToolReplacements = contactToolMap
    ? await verifyContactToolMap(client, contactToolMap, current, expectedContactToolMap) : undefined;
  const prompt = extractPromptSection(await readFile(PROMPT_PATH, "utf8"));
  const controlMap = controlMapPath ? JSON.parse(await readFile(path.resolve(controlMapPath), "utf8")) : undefined;
  const control = controlMap ? await verifyConversationControlMap(client, controlMap, current, config.elevenLabs.toolSecret, expectedControlMap) : undefined;
  const body = buildConversationRelease(current, prompt, listenFirst, contactToolReplacements, control);
  if (expectedCandidate && bodyDigest(body) !== expectedCandidate) throw new Error("Candidate changed since review; nothing applied");
  await mkdir(receiptDir, { recursive: true });
  const receipt = {
    checked_at: new Date().toISOString(), agent_id: agentId, branch_id: branchId,
    before_version: current.version_id, before_prompt_sha256: digest(beforePrompt),
    candidate_prompt_sha256: digest(prompt), policy: VOICE_CONVERSATION_POLICY_VERSION,
    candidate_body_sha256: bodyDigest(body),
    contact_tool_map_sha256: contactToolMap?.map_sha256 ?? null,
    contact_tool_replacements: contactToolReplacements ?? null,
    conversation_control_map_sha256: controlMap?.map_sha256 ?? null,
    listen_first: listenFirst, applied: false, status: "prepared_not_applied",
  };
  await writeFile(path.join(receiptDir, "before.json"), JSON.stringify(safeReceipt(writableBody(current)), null, 2));
  await writeFile(path.join(receiptDir, "candidate.json"), JSON.stringify(safeReceipt(body), null, 2));
  await writeFile(path.join(receiptDir, "receipt.json"), JSON.stringify(receipt, null, 2));
  if (!apply) { console.log(JSON.stringify({ ...receipt, dry_run: true })); return; }

  // Recheck immediately before the versioned write; fail closed on concurrent edits.
  const { data: check } = await client.get(endpoint, { params });
  if (check.version_id !== current.version_id || digest(check.conversation_config.agent.prompt.prompt) !== digest(beforePrompt)) {
    throw new Error("Agent changed after candidate creation; nothing applied");
  }
  verifyReleaseReadback(current, writableBody(current), check);
  if (contactToolMap) await verifyContactToolMap(client, contactToolMap, check, expectedContactToolMap);
  if (controlMap) await verifyConversationControlMap(client, controlMap, check, config.elevenLabs.toolSecret, expectedControlMap);
  await writeFile(path.join(receiptDir, "receipt.json"), JSON.stringify({ ...receipt, status: "patch_attempted_readback_required" }, null, 2));
  await client.patch(endpoint, body, { params: { ...params, enable_versioning_if_not_enabled: true } });
  const { data: after } = await client.get(endpoint, { params });
  await writeFile(path.join(receiptDir, "after.json"), JSON.stringify(safeReceipt(writableBody(after)), null, 2));
  if (digest(after.conversation_config.agent.prompt.prompt) !== digest(prompt)) throw new Error("Prompt readback mismatch after write");
  verifyReleaseReadback(current, body, after);
  const { data: effective } = await client.get(endpoint);
  if (effective.version_id !== after.version_id || effective.branch_id !== branchId) throw new Error("Default published version differs from branch readback");
  verifyReleaseReadback(current, body, effective);
  if (contactToolMap) await verifyContactToolMap(client, contactToolMap, current, expectedContactToolMap);
  if (controlMap) await verifyConversationControlMap(client, controlMap, effective, config.elevenLabs.toolSecret, expectedControlMap);
  const finalReceipt = { ...receipt, applied: true, status: "live_config_verified", after_version: after.version_id, verified_at: new Date().toISOString() };
  await writeFile(path.join(receiptDir, "receipt.json"), JSON.stringify(finalReceipt, null, 2));
  console.log(JSON.stringify(finalReceipt));
}

if (require.main === module) void main().catch(error => {
  console.error(JSON.stringify({ error: axios.isAxiosError(error)
    ? `ElevenLabs request failed (${error.response?.status ?? "network"})`
    : error instanceof Error ? error.message : "Conversation sync failed" }));
  process.exitCode = 1;
});
