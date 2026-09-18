import axios from "axios";
import { mkdir, open, readFile } from "node:fs/promises";
import path from "node:path";
import {
  conversationControlTools,
  normalizeControlToolReadback,
  verifyConversationControlMap,
} from "./elevenLabsConversationControlTools";
import { contactToolDigest } from "./prepareElevenLabsContactTools";

type ToolClient = {
  get(endpoint: string, options?: any): Promise<{ data: any }>;
  post(endpoint: string, body: any): Promise<{ data: any }>;
};

type Identity = {
  agent_id: string;
  branch_id: string;
  main_branch_id: string;
};

type PrepareOptions = {
  identity: Identity;
  receiptDir: string;
  create?: boolean;
  expectedPlanHash?: string;
};

function requireId(value: unknown): asserts value is string {
  if (typeof value !== "string" || !/^[A-Za-z0-9_-]+$/.test(value)) throw new Error("A reviewed provider identifier is required");
}

function requireHash(value: unknown): asserts value is string {
  if (typeof value !== "string" || !/^[a-f0-9]{64}$/.test(value)) throw new Error("An exact reviewed SHA256 is required");
}

async function durableJson(file: string, value: any, append = false): Promise<void> {
  const handle = await open(file, append ? "a" : "wx", 0o600);
  try {
    await handle.writeFile(JSON.stringify(value) + "\n");
    await handle.sync();
  } finally {
    await handle.close();
  }
  const directory = await open(path.dirname(file), "r");
  try {
    await directory.sync();
  } finally {
    await directory.close();
  }
}

function verifyIdentity(agent: any, expected: Identity): void {
  for (const key of ["agent_id", "branch_id", "main_branch_id"] as const) {
    requireId(expected[key]);
    if (agent[key] !== expected[key]) throw new Error(`Expected production identity drifted: ${key}`);
  }
  if (expected.branch_id !== expected.main_branch_id) throw new Error("The effective main branch is required");
}

async function readOwner(client: ToolClient, identity: Identity): Promise<any> {
  const endpoint = `/v1/convai/agents/${identity.agent_id}`;
  const { data: branch } = await client.get(endpoint, { params: { branch_id: identity.branch_id } });
  verifyIdentity(branch, identity);
  const { data: effective } = await client.get(endpoint);
  verifyIdentity(effective, identity);
  return effective;
}

export async function prepareConversationControlTools(options: PrepareOptions, client: ToolClient, secret: string | undefined): Promise<any> {
  if (options.create) requireHash(options.expectedPlanHash);
  Object.values(options.identity).forEach(requireId);
  const owner = await readOwner(client, options.identity);
  const configs = conversationControlTools(secret);
  const plan = {
    schema_version: 1,
    operation: "create_conversation_control_tools",
    voice_only: true,
    agent_id: owner.agent_id,
    branch_id: owner.main_branch_id,
    tools: {
      reset: { name: configs.reset.name, config_sha256: contactToolDigest(configs.reset) },
      validate: { name: configs.validate.name, config_sha256: contactToolDigest(configs.validate) },
    },
  };
  const planHash = contactToolDigest(plan);
  if (options.expectedPlanHash && options.expectedPlanHash !== planHash) {
    throw new Error("Conversation-control plan changed since review; nothing created");
  }
  await mkdir(options.receiptDir, { recursive: true });
  const planFile = path.join(options.receiptDir, "plan.json");
  const document = { plan, plan_sha256: planHash };
  try {
    await durableJson(planFile, document);
  } catch (error: any) {
    if (error.code !== "EEXIST") throw error;
    if (contactToolDigest(JSON.parse(await readFile(planFile, "utf8"))) !== contactToolDigest(document)) {
      throw new Error("Receipt directory already contains another reviewed conversation-control plan");
    }
  }
  if (!options.create) {
    return { status: "prepared_not_created", dry_run: true, plan_sha256: planHash, receipt_dir: options.receiptDir };
  }

  try {
    await durableJson(path.join(options.receiptDir, "create-attempt.json"), {
      plan_sha256: planHash,
      status: "creation_attempt_started_no_automatic_retry",
      started_at: new Date().toISOString(),
    });
  } catch (error: any) {
    if (error.code === "EEXIST") throw new Error("Creation was already attempted; reconcile its journal without retrying");
    throw error;
  }

  const journal = path.join(options.receiptDir, "journal.jsonl");
  const tools: Record<"reset" | "validate", { id: string; config_sha256: string }> = {} as any;
  try {
    for (const kind of ["reset", "validate"] as const) {
      await readOwner(client, options.identity);
      await durableJson(journal, {
        event: "create_intent",
        kind,
        plan_sha256: planHash,
        payload_sha256: contactToolDigest({ tool_config: configs[kind] }),
        at: new Date().toISOString(),
      }, true);
      const { data: created } = await client.post("/v1/convai/tools", { tool_config: configs[kind] });
      requireId(created?.id);
      await durableJson(journal, {
        event: "created_id_readback_required",
        kind,
        id: created.id,
        at: new Date().toISOString(),
      }, true);
      const { data: readback } = await client.get(`/v1/convai/tools/${created.id}`);
      if (readback.id !== created.id) throw new Error("Control tool readback identity mismatch");
      const normalized = normalizeControlToolReadback(configs[kind], readback.tool_config);
      tools[kind] = { id: created.id, config_sha256: contactToolDigest(normalized) };
      await durableJson(journal, { event: "control_tool_verified", kind, id: created.id, at: new Date().toISOString() }, true);
    }
    const mapPayload = {
      schema_version: 1,
      voice_only: true,
      agent_id: owner.agent_id,
      branch_id: owner.main_branch_id,
      tools,
    };
    const map = { ...mapPayload, map_sha256: contactToolDigest(mapPayload) };
    await verifyConversationControlMap(client, map, owner, secret, map.map_sha256);
    const mapPath = path.join(options.receiptDir, "candidate-map.json");
    await durableJson(mapPath, map);
    await durableJson(journal, { event: "candidate_map_verified_not_attached", map_sha256: map.map_sha256, at: new Date().toISOString() }, true);
    return { status: "controls_verified_not_attached", map_sha256: map.map_sha256, map_path: mapPath, plan_sha256: planHash };
  } catch {
    await durableJson(journal, { event: "manual_reconciliation_required_do_not_retry", verified_tools: tools, at: new Date().toISOString() }, true);
    throw new Error("Conversation-control creation/readback is uncertain or incomplete; reconcile the journal and do not retry creation");
  }
}

async function main(): Promise<void> {
  const value = (name: string) => process.argv.find(arg => arg.startsWith(`--${name}=`))?.slice(name.length + 3);
  const { config } = await import("../lib/config");
  const identity = {
    agent_id: value("expected-agent") ?? config.elevenLabs.agentId!,
    branch_id: value("expected-main-branch") ?? config.elevenLabs.branchId!,
    main_branch_id: value("expected-main-branch") ?? config.elevenLabs.branchId!,
  };
  if (!config.elevenLabs.apiKey || !config.elevenLabs.toolSecret || !config.elevenLabs.agentId || !config.elevenLabs.branchId) {
    throw new Error("ElevenLabs key, agent, branch and tool-secret configuration required");
  }
  if (config.elevenLabs.agentId !== identity.agent_id || config.elevenLabs.branchId !== identity.branch_id) {
    throw new Error("Configured production agent/main branch must match the reviewed identity");
  }
  const client = axios.create({
    baseURL: config.elevenLabs.baseUrl,
    timeout: 45_000,
    headers: { "Content-Type": "application/json", "xi-api-key": config.elevenLabs.apiKey },
  });
  console.log(JSON.stringify(await prepareConversationControlTools({
    identity,
    create: process.argv.includes("--create"),
    expectedPlanHash: value("expected-plan-sha"),
    receiptDir: path.resolve(value("receipt-dir") ?? "tmp/elevenlabs-conversation-control"),
  }, client, config.elevenLabs.toolSecret)));
}

if (require.main === module) void main().catch(error => {
  console.error(JSON.stringify({ error: axios.isAxiosError(error)
    ? `ElevenLabs request failed (${error.response?.status ?? "network"})`
    : error instanceof Error ? error.message : "Conversation-control preparation failed" }));
  process.exitCode = 1;
});
