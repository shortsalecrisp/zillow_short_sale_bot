import axios from "axios";
import { createHash } from "node:crypto";
import { mkdir, open, readFile } from "node:fs/promises";
import path from "node:path";
import { applyContactToolDescriptions, VOICE_CONVERSATION_POLICY_VERSION } from "../lib/elevenLabsConversationPolicy";

export const CONTACT_TOOL_NAMES = ["callback_requested", "not_interested", "information_requested"] as const;
type ToolName = typeof CONTACT_TOOL_NAMES[number];
type ToolConfig = { name: string; type: string; [key: string]: any };
type Identity = { agent_id: string; branch_id: string; main_branch_id: string; version_id: string };
type ToolClient = {
  get(endpoint: string, options?: any): Promise<{ data: any }>;
  post(endpoint: string, body: any): Promise<{ data: any }>;
};
type Binding = { path: Array<string | number>; tool_id: string };
type Baseline = { agent: any; plan: any; tools: Record<ToolName, { id: string; before: ToolConfig; after: ToolConfig }> };
export type ContactToolPreparationOptions = {
  identity: Identity;
  toolIds: Record<ToolName, string>;
  receiptDir: string;
  create?: boolean;
  expectedPlanHash?: string;
};

function canonical(value: any): any {
  if (Array.isArray(value)) return value.map(canonical);
  if (value && typeof value === "object") return Object.fromEntries(Object.keys(value).sort().map(key => [key, canonical(value[key])]));
  return value;
}
export const contactToolDigest = (value: any) => createHash("sha256").update(JSON.stringify(canonical(value))).digest("hex");
function requireId(value: unknown): asserts value is string {
  if (typeof value !== "string" || !/^[A-Za-z0-9_-]+$/.test(value)) throw new Error("A reviewed provider identifier is required");
}
function requireHash(value: unknown): asserts value is string {
  if (typeof value !== "string" || !/^[a-f0-9]{64}$/.test(value)) throw new Error("An exact reviewed SHA256 is required");
}

export function contactAgentDigest(agent: any): string {
  return contactToolDigest(Object.fromEntries([
    "conversation_config", "workflow", "platform_settings", "phone_numbers", "whatsapp_accounts", "procedures",
  ].map(key => [key, agent[key] ?? null])));
}

function verifyIdentity(agent: any, expected: Identity): void {
  for (const key of ["agent_id", "branch_id", "main_branch_id", "version_id"] as const) {
    requireId(expected[key]);
    if (agent[key] !== expected[key]) throw new Error(`Expected production identity drifted: ${key}`);
  }
  if (expected.branch_id !== expected.main_branch_id) throw new Error("The effective main branch is required");
}

export function contactToolBindings(agent: any, ids: string[]): Binding[] {
  if (ids.length !== CONTACT_TOOL_NAMES.length || new Set(ids).size !== CONTACT_TOOL_NAMES.length) throw new Error("Exactly three distinct contact tool IDs are required");
  const globalIds = agent.conversation_config?.agent?.prompt?.tool_ids;
  if (!Array.isArray(globalIds) || new Set(globalIds).size !== globalIds.length ||
      ids.some(id => globalIds.filter(item => item === id).length !== 1)) {
    throw new Error("All reviewed contact tools must be attached exactly once globally");
  }
  const bindings: Binding[] = [];
  function walk(value: any, at: Array<string | number>): void {
    if (Array.isArray(value)) return value.forEach((item, index) => walk(item, [...at, index]));
    if (value && typeof value === "object") {
      for (const [key, item] of Object.entries(value)) walk(item, [...at, key]);
      return;
    }
    if (!ids.includes(value)) return;
    const global = at.length === 5 && at.slice(0, 4).join(".") === "conversation_config.agent.prompt.tool_ids" && typeof at[4] === "number";
    const workflow = at[0] === "workflow" && (at.at(-1) === "tool_id" ||
      (["tool_ids", "additional_tool_ids"].includes(String(at.at(-2))) && typeof at.at(-1) === "number"));
    if (!global && !workflow) throw new Error("A contact tool ID appears outside a supported binding path");
    bindings.push({ path: at, tool_id: value });
  }
  walk({ conversation_config: agent.conversation_config, workflow: agent.workflow }, []);
  return bindings.sort((a, b) => JSON.stringify(a.path).localeCompare(JSON.stringify(b.path)));
}

export function replaceContactToolBindings<T>(body: T, replacements: Record<string, string>): T {
  const oldIds = Object.keys(replacements);
  const newIds = Object.values(replacements);
  if (oldIds.length !== CONTACT_TOOL_NAMES.length || new Set(newIds).size !== CONTACT_TOOL_NAMES.length || newIds.some(id => oldIds.includes(id))) {
    throw new Error("The reviewed map must replace exactly three IDs with distinct new IDs");
  }
  [...oldIds, ...newIds].forEach(requireId);
  const globalIds = (body as any).conversation_config?.agent?.prompt?.tool_ids ?? [];
  if (newIds.some(id => globalIds.includes(id))) throw new Error("A candidate clone is already attached");
  const bindings = contactToolBindings(body, oldIds);
  const result: any = structuredClone(body);
  for (const binding of bindings) {
    let parent = result;
    for (const key of binding.path.slice(0, -1)) parent = parent[key];
    parent[binding.path.at(-1)!] = replacements[binding.tool_id];
  }
  return result;
}

function descriptionPaths(name: ToolName): string[][] {
  return [
    ["description"], ["api_schema", "request_body_schema", "description"],
    ["api_schema", "request_body_schema", "properties", name === "callback_requested" ? "callbackTime" : "conversationSummary", "description"],
    ...(name === "callback_requested" ? [["api_schema", "request_body_schema", "properties", "conversationSummary", "description"]] : []),
  ];
}

export function verifyContactDescriptionOnlyChange(before: ToolConfig, after: ToolConfig): string[][] {
  if (before.type !== "webhook" || !CONTACT_TOOL_NAMES.includes(before.name as ToolName)) {
    throw new Error("Only the three reviewed webhook contact tools may be cloned");
  }
  const paths = descriptionPaths(before.name as ToolName);
  const stripped = [before, after].map(tool => {
    const copy = structuredClone(tool);
    for (const at of paths) {
      let parent: any = copy;
      for (const key of at.slice(0, -1)) {
        if (!parent || typeof parent[key] !== "object" || Array.isArray(parent[key])) throw new Error("Expected contact tool schema is missing");
        parent = parent[key];
      }
      delete parent[at.at(-1)!];
    }
    return copy;
  });
  if (contactToolDigest(stripped[0]) !== contactToolDigest(stripped[1])) {
    throw new Error("Contact clone changed a field outside the allowed description paths");
  }
  return paths.filter(at => contactToolDigest(at.reduce((v, key) => v?.[key], before) ?? null) !==
    contactToolDigest(at.reduce((v, key) => v?.[key], after) ?? null));
}

async function readTool(client: ToolClient, id: string, name: ToolName): Promise<ToolConfig> {
  requireId(id);
  const { data } = await client.get(`/v1/convai/tools/${id}`);
  if (data?.id !== id || data.tool_config?.type !== "webhook" || data.tool_config?.name !== name) {
    throw new Error("Fresh contact tool identity/type does not match the reviewed binding");
  }
  return data.tool_config;
}

export async function readContactToolBaseline(client: ToolClient, identity: Identity, toolIds: Record<ToolName, string>): Promise<Baseline> {
  for (const value of Object.values(identity)) requireId(value);
  const endpoint = `/v1/convai/agents/${identity.agent_id}`;
  const { data: agent } = await client.get(endpoint, { params: { branch_id: identity.branch_id } });
  verifyIdentity(agent, identity);
  const { data: effective } = await client.get(endpoint);
  verifyIdentity(effective, identity);
  if (contactAgentDigest(agent) !== contactAgentDigest(effective)) throw new Error("Main-branch and effective production config differ");
  const ids = CONTACT_TOOL_NAMES.map(name => toolIds[name]);
  ids.forEach(requireId);
  const bindings = contactToolBindings(agent, ids);
  const tools = {} as Baseline["tools"];
  for (const name of CONTACT_TOOL_NAMES) {
    const id = toolIds[name];
    const before = await readTool(client, id, name);
    const expanded = agent.conversation_config.agent.prompt.tools?.filter((tool: ToolConfig) => tool.name === name);
    if (!Array.isArray(expanded) || expanded.length !== 1 || contactToolDigest(expanded[0]) !== contactToolDigest(before)) {
      throw new Error("Attached expanded tool config differs from its fresh resource readback");
    }
    const after = applyContactToolDescriptions(before);
    if (!verifyContactDescriptionOnlyChange(before, after).length) throw new Error("Contact tool already has the proposed descriptions; review instead of cloning");
    tools[name] = { id, before, after };
  }
  const plan = {
    schema_version: 1, operation: "clone_contact_tool_descriptions", policy: VOICE_CONVERSATION_POLICY_VERSION,
    identity, agent_config_sha256: contactAgentDigest(agent), bindings,
    tools: CONTACT_TOOL_NAMES.map(name => ({
      name, old_id: tools[name].id, before_config_sha256: contactToolDigest(tools[name].before),
      after_config_sha256: contactToolDigest(tools[name].after),
      changed_description_paths: verifyContactDescriptionOnlyChange(tools[name].before, tools[name].after),
    })),
  };
  return { agent, tools, plan };
}

async function durableJson(file: string, value: any, append = false): Promise<void> {
  const handle = await open(file, append ? "a" : "wx", 0o600);
  try { await handle.writeFile(JSON.stringify(value) + "\n"); await handle.sync(); } finally { await handle.close(); }
  const directory = await open(path.dirname(file), "r");
  try { await directory.sync(); } finally { await directory.close(); }
}

async function savePlan(dir: string, plan: any): Promise<string> {
  await mkdir(dir, { recursive: true });
  const file = path.join(dir, "plan.json");
  const document = { plan, plan_sha256: contactToolDigest(plan) };
  try { await durableJson(file, document); } catch (error: any) {
    if (error.code !== "EEXIST") throw error;
    if (contactToolDigest(JSON.parse(await readFile(file, "utf8"))) !== contactToolDigest(document)) {
      throw new Error("Receipt directory already contains another reviewed plan; nothing created");
    }
  }
  return document.plan_sha256;
}

function mapPayload(map: any): any {
  const { map_sha256: _hash, ...payload } = map;
  return payload;
}

export async function verifyContactToolMap(
  client: ToolClient, map: any, current: any, expectedMapHash?: string,
): Promise<Record<string, string>> {
  if (map?.schema_version !== 1 || map.status !== "clones_verified_not_attached") throw new Error("A verified contact-tool clone map is required");
  requireHash(map.map_sha256);
  if (contactToolDigest(mapPayload(map)) !== map.map_sha256 || (expectedMapHash && expectedMapHash !== map.map_sha256)) {
    throw new Error("Reviewed contact-tool map hash mismatch");
  }
  requireHash(map.plan_sha256);
  if (contactToolDigest(map.plan) !== map.plan_sha256) throw new Error("Contact tool plan hash mismatch");
  if (map.plan.schema_version !== 1 || map.plan.operation !== "clone_contact_tool_descriptions" || map.plan.policy !== VOICE_CONVERSATION_POLICY_VERSION) {
    throw new Error("Contact-tool map was not built for the current reviewed description policy");
  }
  verifyIdentity(current, map.plan.identity);
  if (contactAgentDigest(current) !== map.plan.agent_config_sha256) throw new Error("Production configuration changed since contact-tool review");
  if (!Array.isArray(map.plan.tools) || map.plan.tools.length !== CONTACT_TOOL_NAMES.length ||
      !Array.isArray(map.tools) || map.tools.length !== CONTACT_TOOL_NAMES.length ||
      Object.keys(map.replacements ?? {}).length !== CONTACT_TOOL_NAMES.length) throw new Error("Contact-tool map must contain exactly three reviewed replacements");
  const ids = map.plan.tools.map((tool: any) => tool.old_id);
  if (contactToolDigest(contactToolBindings(current, ids)) !== contactToolDigest(map.plan.bindings)) throw new Error("Reviewed contact-tool bindings drifted");
  const newIds = new Set<string>();
  for (const name of CONTACT_TOOL_NAMES) {
    const planned = map.plan.tools.filter((tool: any) => tool.name === name);
    const cloned = map.tools.filter((tool: any) => tool.name === name);
    if (planned.length !== 1 || cloned.length !== 1) throw new Error("Contact-tool map names must be unique and exact");
    const entry = planned[0], clone = cloned[0];
    const newId = map.replacements[entry.old_id];
    requireId(newId);
    if (ids.includes(newId) || newIds.has(newId) || clone.old_id !== entry.old_id || clone.new_id !== newId) throw new Error("Invalid cloned contact tool identity");
    newIds.add(newId);
    const before = await readTool(client, entry.old_id, name);
    const desired = applyContactToolDescriptions(before);
    const changedPaths = verifyContactDescriptionOnlyChange(before, desired);
    const after = await readTool(client, newId, name);
    if (contactToolDigest(before) !== entry.before_config_sha256 || contactToolDigest(desired) !== entry.after_config_sha256 ||
        contactToolDigest(after) !== entry.after_config_sha256 || clone.before_config_sha256 !== entry.before_config_sha256 ||
        clone.after_config_sha256 !== entry.after_config_sha256 || contactToolDigest(changedPaths) !== contactToolDigest(entry.changed_description_paths)) {
      throw new Error("Original or cloned contact tool config drifted from the reviewed plan");
    }
  }
  replaceContactToolBindings(current, map.replacements);
  return { ...map.replacements };
}

export async function prepareContactTools(options: ContactToolPreparationOptions, client: ToolClient): Promise<any> {
  if (options.create) requireHash(options.expectedPlanHash);
  const baseline = await readContactToolBaseline(client, options.identity, options.toolIds);
  const planHash = contactToolDigest(baseline.plan);
  if (options.expectedPlanHash && options.expectedPlanHash !== planHash) throw new Error("Contact tool plan changed since review; nothing created");
  await savePlan(options.receiptDir, baseline.plan);
  if (!options.create) return { status: "prepared_not_created", dry_run: true, plan_sha256: planHash, receipt_dir: options.receiptDir };

  // An exclusive durable marker precedes every possible creation. A failed or
  // uncertain attempt needs manual reconciliation, never an automatic retry.
  try {
    await durableJson(path.join(options.receiptDir, "create-attempt.json"), {
      plan_sha256: planHash, status: "creation_attempt_started_no_automatic_retry", started_at: new Date().toISOString(),
    });
  } catch (error: any) {
    if (error.code === "EEXIST") throw new Error("Creation was already attempted; reconcile its journal without retrying");
    throw error;
  }
  const journal = path.join(options.receiptDir, "journal.jsonl");
  const clones: any[] = [];
  try {
    for (const name of CONTACT_TOOL_NAMES) {
      const fresh = await readContactToolBaseline(client, options.identity, options.toolIds);
      if (contactToolDigest(fresh.plan) !== planHash) throw new Error("Production tools changed before creation");
      const tool = fresh.tools[name];
      await durableJson(journal, { event: "create_intent", name, old_id: tool.id, plan_sha256: planHash,
        payload_sha256: contactToolDigest({ tool_config: tool.after }), at: new Date().toISOString() }, true);
      const { data: created } = await client.post("/v1/convai/tools", { tool_config: tool.after });
      requireId(created?.id);
      if (Object.values(options.toolIds).includes(created.id) || clones.some(clone => clone.new_id === created.id)) throw new Error("Provider did not return a distinct new tool ID");
      await durableJson(journal, { event: "created_id_readback_required", name, new_id: created.id, at: new Date().toISOString() }, true);
      const readback = await readTool(client, created.id, name);
      if (contactToolDigest(readback) !== contactToolDigest(tool.after)) throw new Error("New contact tool readback differs from the exact requested config");
      const clone = { name, old_id: tool.id, new_id: created.id,
        before_config_sha256: contactToolDigest(tool.before), after_config_sha256: contactToolDigest(readback) };
      clones.push(clone);
      await durableJson(journal, { event: "clone_verified", ...clone, at: new Date().toISOString() }, true);
    }
    const { agent, plan } = await readContactToolBaseline(client, options.identity, options.toolIds);
    if (contactToolDigest(plan) !== planHash) throw new Error("Production config changed during cloning");
    const payload = {
      schema_version: 1, status: "clones_verified_not_attached", created_at: new Date().toISOString(),
      plan: baseline.plan, plan_sha256: planHash, tools: clones,
      replacements: Object.fromEntries(clones.map(clone => [clone.old_id, clone.new_id])),
    };
    const map = { ...payload, map_sha256: contactToolDigest(payload) };
    await verifyContactToolMap(client, map, agent, map.map_sha256);
    await durableJson(path.join(options.receiptDir, "candidate-map.json"), map);
    await durableJson(journal, { event: "candidate_map_verified_not_attached", map_sha256: map.map_sha256, at: new Date().toISOString() }, true);
    return { status: map.status, plan_sha256: planHash, map_sha256: map.map_sha256,
      map_path: path.join(options.receiptDir, "candidate-map.json"), replacements: map.replacements };
  } catch {
    await durableJson(journal, { event: "manual_reconciliation_required_do_not_retry", verified_clones: clones, at: new Date().toISOString() }, true);
    throw new Error("Contact-tool creation/readback is uncertain or incomplete; reconcile the journal and do not retry creation");
  }
}

async function main(): Promise<void> {
  const value = (name: string) => process.argv.find(arg => arg.startsWith(`--${name}=`))?.slice(name.length + 3);
  const identity = {
    agent_id: value("expected-agent")!, branch_id: value("expected-main-branch")!,
    main_branch_id: value("expected-main-branch")!, version_id: value("expected-version")!,
  };
  Object.values(identity).forEach(requireId);
  const toolIds = { callback_requested: value("callback-tool-id")!, not_interested: value("not-interested-tool-id")!,
    information_requested: value("information-tool-id")! };
  Object.values(toolIds).forEach(requireId);
  const create = process.argv.includes("--create");
  if (create) requireHash(value("expected-plan-sha"));
  const { config } = await import("../lib/config");
  if (!config.elevenLabs.apiKey || config.elevenLabs.agentId !== identity.agent_id || config.elevenLabs.branchId !== identity.branch_id) {
    throw new Error("Configured production agent/main branch must match the reviewed identity");
  }
  const client = axios.create({ baseURL: config.elevenLabs.baseUrl, timeout: 45_000,
    headers: { "Content-Type": "application/json", "xi-api-key": config.elevenLabs.apiKey } });
  console.log(JSON.stringify(await prepareContactTools({ identity, toolIds, create,
    expectedPlanHash: value("expected-plan-sha"),
    receiptDir: path.resolve(value("receipt-dir") ?? "tmp/elevenlabs-contact-tools"),
  }, client)));
}

if (require.main === module) void main().catch(error => {
  console.error(JSON.stringify({ error: axios.isAxiosError(error)
    ? `ElevenLabs request failed (${error.response?.status ?? "network"})`
    : error instanceof Error ? error.message : "Contact-tool preparation failed" }));
  process.exitCode = 1;
});
