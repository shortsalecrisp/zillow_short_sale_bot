import { mkdir, readFile, writeFile } from "node:fs/promises";
import path from "node:path";
import { applyConversationOpeningWorkflow } from "../lib/elevenLabsOpeningWorkflow";
import { bodyDigest, extractPromptSection, writableBody } from "./syncElevenLabsAgent";

const OPENING_PATHS = [
  ["nodes", "opening_listener", "conversation_config", "agent", "prompt", "prompt"],
  ["nodes", "main_conversation", "additional_prompt"],
  ["edges", "opening_to_main", "forward_condition"],
] as const;

function parentAt(object: any, keys: readonly string[]) {
  let parent = object;
  for (const key of keys.slice(0, -1)) {
    if (!parent || typeof parent[key] !== "object") throw new Error(`Missing reviewed workflow path: ${keys.join(".")}`);
    parent = parent[key];
  }
  return parent;
}

export function openingProtectedHashes(agent: any) {
  const body = writableBody(agent);
  delete body.conversation_config.agent.prompt.prompt;
  for (const keys of OPENING_PATHS) delete parentAt(body.workflow, keys)[keys[keys.length - 1]];
  return {
    unchanged_conversation_and_workflow: bodyDigest(body),
    platform: bodyDigest(agent.platform_settings ?? null),
    phone_numbers: bodyDigest(agent.phone_numbers ?? null),
    whatsapp_accounts: bodyDigest(agent.whatsapp_accounts ?? null),
    procedures: bodyDigest(agent.procedures ?? null),
  };
}

export function buildOpeningRelease(agent: any, prompt: string) {
  if (agent.conversation_config?.agent?.first_message !== ""
    || agent.workflow?.edges?.start_to_main?.target !== "opening_listener"
    || agent.workflow?.nodes?.opening_listener?.entry_behavior !== "wait_for_user") {
    throw new Error("Expected the reviewed listen-first opening path; refusing a different workflow");
  }
  const original = writableBody(agent);
  const updated = structuredClone(original);
  updated.conversation_config.agent.prompt.prompt = prompt;
  const proposed = applyConversationOpeningWorkflow(updated);
  // Copy only reviewed leaves. Preserve terminal guards, tools, TTS, ASR and turn settings byte-for-byte.
  for (const keys of OPENING_PATHS) {
    const key = keys[keys.length - 1];
    parentAt(updated.workflow, keys)[key] = structuredClone(parentAt(proposed.workflow, keys)[key]);
  }
  if (bodyDigest(openingProtectedHashes(agent)) !== bodyDigest(openingProtectedHashes({ ...agent, ...updated }))) {
    throw new Error("Opening release tried to change a protected setting");
  }
  return updated;
}

type Options = {
  apiKey: string;
  agentId: string;
  branchId: string;
  expectedVersion: string;
  expectedBodyHash: string;
  expectedCandidateHash?: string;
  promptPath: string;
  receiptDir: string;
  apply?: boolean;
};

export async function syncOpeningRelease(options: Options, fetcher: typeof fetch = fetch) {
  const { apiKey, agentId, branchId, expectedVersion, expectedBodyHash, receiptDir, apply } = options;
  if (!apiKey || !/^agent_[a-z0-9]+$/.test(agentId) || !/^agtbrch_[a-z0-9]+$/.test(branchId)
    || !expectedVersion || !/^[a-f0-9]{64}$/.test(expectedBodyHash) || !receiptDir
    || (apply && !/^[a-f0-9]{64}$/.test(options.expectedCandidateHash ?? ""))) {
    throw new Error("Exact reviewed agent/branch/version/body and candidate guards are required");
  }
  const endpoint = `https://api.elevenlabs.io/v1/convai/agents/${agentId}?branch_id=${encodeURIComponent(branchId)}`;
  async function call(method = "GET", body?: any) {
    const response = await fetcher(endpoint + (method === "PATCH" ? "&enable_versioning_if_not_enabled=true" : ""), {
      method,
      headers: { "xi-api-key": apiKey, ...(body ? { "Content-Type": "application/json" } : {}) },
      body: body ? JSON.stringify(body) : undefined,
      signal: AbortSignal.timeout(60000),
    });
    if (!response.ok) throw new Error(`ElevenLabs ${method} returned HTTP ${response.status}; read back before retrying`);
    return response.json();
  }
  function assertReviewed(agent: any) {
    if (agent.agent_id !== agentId || agent.branch_id !== branchId || agent.version_id !== expectedVersion
      || bodyDigest(writableBody(agent)) !== expectedBodyHash) throw new Error("Reviewed provider version/configuration drifted");
  }
  const before = await call();
  assertReviewed(before);
  const prompt = extractPromptSection(await readFile(options.promptPath, "utf8"));
  const candidate = buildOpeningRelease(before, prompt);
  const candidateHash = bodyDigest(candidate);
  const protectedBefore = openingProtectedHashes(before);
  const receipt: Record<string, any> = {
    checked_at: new Date().toISOString(), agent_id: agentId, branch_id: branchId,
    before_version: expectedVersion, before_body_sha256: expectedBodyHash,
    candidate_sha256: candidateHash, protected_before: protectedBefore,
    allowed_changes: ["conversation_config.agent.prompt.prompt", ...OPENING_PATHS.map(keys => `workflow.${keys.join(".")}`)],
    status: "prepared_not_applied", real_calls_placed: false, audio_behavior_verified: false,
  };
  await mkdir(receiptDir, { recursive: true, mode: 0o700 });
  const save = () => writeFile(path.join(receiptDir, "receipt.json"), JSON.stringify(receipt, null, 2), { mode: 0o600 });
  await writeFile(path.join(receiptDir, "before-config.json"), JSON.stringify(writableBody(before), null, 2), { mode: 0o600 });
  await writeFile(path.join(receiptDir, "candidate-config.json"), JSON.stringify(candidate, null, 2), { mode: 0o600 });
  await save();
  if (!apply) return receipt;
  if (candidateHash !== options.expectedCandidateHash) throw new Error("Candidate does not match approved preparation");
  const fresh = await call();
  assertReviewed(fresh);
  if (bodyDigest(openingProtectedHashes(fresh)) !== bodyDigest(protectedBefore)) throw new Error("Protected provider settings drifted");
  if (candidateHash === expectedBodyHash) {
    receipt.status = "live_config_already_matches_audio_not_verified";
    await save();
    return receipt;
  }
  receipt.status = "patch_attempted_readback_required";
  await save();
  try {
    await call("PATCH", { conversation_config: { agent: { prompt: { prompt } } }, workflow: candidate.workflow });
    const after = await call();
    receipt.after_version = after.version_id;
    receipt.after_body_sha256 = bodyDigest(writableBody(after));
    receipt.protected_after = openingProtectedHashes(after);
    if (after.agent_id !== agentId || after.branch_id !== branchId || receipt.after_body_sha256 !== candidateHash
      || bodyDigest(receipt.protected_after) !== bodyDigest(protectedBefore)) throw new Error("Effective workflow readback differs from approved release");
    receipt.status = "live_opening_and_workflow_verified_audio_not_verified";
    receipt.verified_at = new Date().toISOString();
    await save();
    return receipt;
  } catch (error) {
    receipt.status = "needs_readback_review_do_not_repeat_patch";
    await save();
    throw error;
  }
}

if (require.main === module) {
  const value = (key: string) => process.argv.find(arg => arg.startsWith(`--${key}=`))?.slice(key.length + 3) ?? "";
  void syncOpeningRelease({ apiKey: process.env.ELEVENLABS_API_KEY ?? "", agentId: value("agent-id"),
    branchId: value("branch-id"), expectedVersion: value("expected-version"), expectedBodyHash: value("expected-body-sha"),
    expectedCandidateHash: value("expected-candidate-sha"), receiptDir: value("receipt-dir"),
    promptPath: path.resolve(__dirname, "../../docs/elevenlabs-agent-prompt.md"), apply: process.argv.includes("--apply"),
  }).then(receipt => console.log(JSON.stringify(receipt))).catch(error => { console.error(error.message); process.exitCode = 1; });
}
