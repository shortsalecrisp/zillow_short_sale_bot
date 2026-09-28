import axios from "axios";
import { createHash } from "node:crypto";
import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";
import {
  buildTurnV3Patch,
  buildV4TurboBranchOverrides,
  ELEVENLABS_TTS_CONTROL_MODEL,
  ELEVENLABS_TTS_EXPERIMENT_BRANCH_NAME,
  ELEVENLABS_TTS_TEST_MODEL,
  ELEVENLABS_TURN_MODEL,
  expectedRuntimeConfig,
} from "../lib/elevenLabsRuntimeExperiment";

const canonical = (value: any): any => {
  if (Array.isArray(value)) return value.map(canonical);
  if (value && typeof value === "object") {
    return Object.fromEntries(Object.keys(value).sort().map((key) => [key, canonical(value[key])]));
  }
  return value;
};

const digest = (value: unknown) =>
  createHash("sha256").update(JSON.stringify(canonical(value)) ?? "undefined").digest("hex");

type AgentResponse = {
  agent_id: string;
  branch_id: string;
  main_branch_id: string;
  version_id: string;
  conversation_config: Record<string, any>;
};

type BranchSummary = {
  id: string;
  name: string;
  is_archived?: boolean;
  current_live_percentage?: number;
};

function value(name: string): string | undefined {
  return process.argv.find((arg) => arg.startsWith(`--${name}=`))?.slice(name.length + 3);
}

function assertAgentIdentity(agent: AgentResponse, agentId: string, mainBranchId: string): void {
  if (
    agent.agent_id !== agentId ||
    agent.branch_id !== mainBranchId ||
    agent.main_branch_id !== mainBranchId
  ) {
    throw new Error("Configured agent did not resolve to its effective main branch");
  }
}

function differingPaths(actual: any, expected: any, prefix = "conversation_config"): string[] {
  if (digest(actual) === digest(expected)) return [];
  if (
    !actual ||
    !expected ||
    typeof actual !== "object" ||
    typeof expected !== "object" ||
    Array.isArray(actual) ||
    Array.isArray(expected)
  ) {
    return [prefix];
  }
  const keys = new Set([...Object.keys(actual), ...Object.keys(expected)]);
  return [...keys].flatMap((key) => differingPaths(actual[key], expected[key], `${prefix}.${key}`));
}

function assertRuntimeConfig(agent: AgentResponse, expected: Record<string, any>): void {
  if (digest(agent.conversation_config) !== digest(expected)) {
    throw new Error(
      `ElevenLabs runtime configuration readback mismatch at: ${differingPaths(agent.conversation_config, expected).join(", ")}`,
    );
  }
}

async function main(): Promise<void> {
  const { config } = await import("../lib/config");
  const { apiKey, agentId, branchId: mainBranchId } = config.elevenLabs;
  if (!apiKey || !agentId || !mainBranchId) {
    throw new Error("ElevenLabs key, agent and main branch configuration required");
  }

  const apply = process.argv.includes("--apply");
  const expectedVersion = value("expected-version");
  const expectedConfigSha = value("expected-config-sha");
  const receiptDir = path.resolve(value("receipt-dir") ?? "tmp/elevenlabs-v4-turn-v3-rollout");
  if (apply && (!expectedVersion || !expectedConfigSha)) {
    throw new Error("Apply requires reviewed --expected-version and --expected-config-sha guards");
  }

  const client = axios.create({
    baseURL: config.elevenLabs.baseUrl,
    timeout: 45_000,
    headers: { "Content-Type": "application/json", "xi-api-key": apiKey },
  });
  const endpoint = `/v1/convai/agents/${agentId}`;
  const { data: current } = await client.get<AgentResponse>(endpoint, {
    params: { branch_id: mainBranchId },
  });
  assertAgentIdentity(current, agentId, mainBranchId);

  const currentConfigSha = digest(current.conversation_config);
  if (expectedVersion && current.version_id !== expectedVersion) {
    throw new Error("Live main agent version drifted; review again");
  }
  if (expectedConfigSha && currentConfigSha !== expectedConfigSha) {
    throw new Error("Live main conversation config drifted; review again");
  }

  const { data: listed } = await client.get<{ results?: BranchSummary[] }>(`${endpoint}/branches`);
  const existingBranch = (listed.results ?? []).find(
    (branch) => branch.name === ELEVENLABS_TTS_EXPERIMENT_BRANCH_NAME && !branch.is_archived,
  );
  const plan = {
    checkedAt: new Date().toISOString(),
    mainBranchId,
    mainVersionId: current.version_id,
    mainConversationConfigSha256: currentConfigSha,
    mainModelBefore: current.conversation_config.tts?.model_id ?? null,
    controlModel: ELEVENLABS_TTS_CONTROL_MODEL,
    testModel: ELEVENLABS_TTS_TEST_MODEL,
    turnModel: ELEVENLABS_TURN_MODEL,
    experimentBranchName: ELEVENLABS_TTS_EXPERIMENT_BRANCH_NAME,
    existingExperimentBranchId: existingBranch?.id ?? null,
    targetTrafficPercent: { control: 50, test: 50 },
    applied: false,
    status: "prepared_not_applied",
  };
  await mkdir(receiptDir, { recursive: true });
  await writeFile(path.join(receiptDir, "plan.json"), JSON.stringify(plan, null, 2));
  if (!apply) {
    console.log(JSON.stringify({ ...plan, dryRun: true }));
    return;
  }

  const expectedMainAfter = expectedRuntimeConfig(
    current.conversation_config,
    ELEVENLABS_TTS_CONTROL_MODEL,
  );
  let experimentBranchId = existingBranch?.id;
  let experimentVersionId: string | undefined;

  if (!experimentBranchId) {
    const { data: created } = await client.post<{
      created_branch_id: string;
      created_version_id: string;
    }>(`${endpoint}/branches`, {
      parent_version_id: current.version_id,
      name: ELEVENLABS_TTS_EXPERIMENT_BRANCH_NAME,
      description:
        "Crisp Maya/Eryn 50/50 TTS model test: Eleven v4 Turbo versus Flash v2, with turn_v3 on both arms.",
      ...buildV4TurboBranchOverrides(current.conversation_config),
    });
    experimentBranchId = created.created_branch_id;
    experimentVersionId = created.created_version_id;
  }

  const { data: experimentAgent } = await client.get<AgentResponse>(endpoint, {
    params: { branch_id: experimentBranchId },
  });
  const expectedExperiment = expectedRuntimeConfig(
    current.conversation_config,
    ELEVENLABS_TTS_TEST_MODEL,
  );
  assertRuntimeConfig(experimentAgent, expectedExperiment);

  if (digest(current.conversation_config) !== digest(expectedMainAfter)) {
    await client.patch(endpoint, buildTurnV3Patch(current.conversation_config), {
      params: { branch_id: mainBranchId, enable_versioning_if_not_enabled: true },
    });
  }

  const { data: mainAfter } = await client.get<AgentResponse>(endpoint, {
    params: { branch_id: mainBranchId },
  });
  assertAgentIdentity(mainAfter, agentId, mainBranchId);
  assertRuntimeConfig(mainAfter, expectedMainAfter);

  const { data: deployment } = await client.post<{
    traffic_percentage_branch_id_map?: Record<string, number>;
  }>(`${endpoint}/deployments`, {
    deployment_request: {
      requests: [
        {
          branch_id: mainBranchId,
          deployment_strategy: { type: "percentage", traffic_percentage: 50 },
        },
        {
          branch_id: experimentBranchId,
          deployment_strategy: { type: "percentage", traffic_percentage: 50 },
        },
      ],
    },
  });
  const traffic = deployment.traffic_percentage_branch_id_map ?? {};
  if (traffic[mainBranchId] !== 50 || traffic[experimentBranchId] !== 50) {
    throw new Error("ElevenLabs traffic deployment readback did not confirm a 50/50 split");
  }

  const { data: branchesAfter } = await client.get<{ results?: BranchSummary[] }>(`${endpoint}/branches`);
  const livePercentages = Object.fromEntries(
    (branchesAfter.results ?? []).map((branch) => [branch.id, branch.current_live_percentage ?? 0]),
  );
  if (livePercentages[mainBranchId] !== 50 || livePercentages[experimentBranchId] !== 50) {
    throw new Error("Branch listing did not confirm the deployed 50/50 traffic split");
  }

  const receipt = {
    ...plan,
    applied: true,
    status: "live_config_verified",
    verifiedAt: new Date().toISOString(),
    mainAfterVersionId: mainAfter.version_id,
    mainAfterConversationConfigSha256: digest(mainAfter.conversation_config),
    experimentBranchId,
    experimentVersionId: experimentVersionId ?? experimentAgent.version_id,
    experimentConversationConfigSha256: digest(experimentAgent.conversation_config),
    trafficPercentageBranchIdMap: traffic,
  };
  await writeFile(path.join(receiptDir, "receipt.json"), JSON.stringify(receipt, null, 2));
  console.log(JSON.stringify(receipt));
}

if (require.main === module) {
  void main().catch((error) => {
    console.error(
      JSON.stringify({
        error: axios.isAxiosError(error)
          ? `ElevenLabs request failed (${error.response?.status ?? "network"}): ${JSON.stringify(error.response?.data ?? {})}`
          : error instanceof Error
            ? error.message
            : "Runtime rollout failed",
      }),
    );
    process.exitCode = 1;
  });
}
