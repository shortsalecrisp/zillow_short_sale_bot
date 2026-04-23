import axios from "axios";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { config } from "../lib/config";
import { logger } from "../lib/logger";

type AgentResponse = {
  agent_id: string;
  conversation_config: {
    turn: {
      turn_timeout?: number;
      initial_wait_time?: number;
      turn_eagerness?: string;
      speculative_turn?: boolean;
      retranscribe_on_turn_timeout?: boolean;
      soft_timeout_config?: {
        timeout_seconds?: number;
        message?: string;
        use_llm_generated_message?: boolean;
      };
      [key: string]: unknown;
    };
    tts: {
      optimize_streaming_latency?: number;
      [key: string]: unknown;
    };
    agent: {
      disable_first_message_interruptions?: boolean;
      backup_llm_config?: {
        preference?: string;
        [key: string]: unknown;
      };
      cascade_timeout_seconds?: number | null;
      prompt: {
        prompt?: string;
        llm?: string;
        temperature?: number;
        max_tokens?: number;
        tool_ids?: string[];
        tools?: Array<Record<string, unknown>>;
        [key: string]: unknown;
      };
      [key: string]: unknown;
    };
    [key: string]: unknown;
  };
};

const PROMPT_PATH = path.resolve(__dirname, "../../docs/elevenlabs-agent-prompt.md");
const FIRST_MESSAGE = "Hi, is this {{firstName}}?";
const INITIAL_WAIT_TIME_SECONDS = 2.0;
const TURN_TIMEOUT_SECONDS = 1.0;
const TURN_EAGERNESS = "normal";
const SPECULATIVE_TURN = false;
const SOFT_TIMEOUT_SECONDS = -1;
const RETRANSCRIBE_ON_TURN_TIMEOUT = true;
const DISABLE_FIRST_MESSAGE_INTERRUPTION = false;
const TEMPERATURE = 0.1;
const MAX_TOKENS = 60;
const BACKUP_LLM_PREFERENCE = "disabled";
const CASCADE_TIMEOUT_SECONDS: number | null = null;

function requireElevenLabsSyncConfig(): { apiKey: string; agentId: string; branchId: string } {
  const missing: string[] = [];

  if (!config.elevenLabs.apiKey) {
    missing.push("ELEVENLABS_API_KEY");
  }

  if (!config.elevenLabs.agentId) {
    missing.push("ELEVENLABS_AGENT_ID");
  }

  if (!config.elevenLabs.branchId) {
    missing.push("ELEVENLABS_BRANCH_ID");
  }

  if (missing.length > 0) {
    throw new Error(`Missing ElevenLabs sync config: ${missing.join(", ")}`);
  }

  return {
    apiKey: config.elevenLabs.apiKey as string,
    agentId: config.elevenLabs.agentId as string,
    branchId: config.elevenLabs.branchId as string,
  };
}

function extractPromptSection(markdown: string): string {
  const marker = "## Prompt";
  const markerIndex = markdown.indexOf(marker);

  if (markerIndex === -1) {
    throw new Error(`Unable to find "${marker}" in ${PROMPT_PATH}`);
  }

  return markdown.slice(markerIndex + marker.length).trim();
}

async function readPrompt(): Promise<string> {
  const markdown = await readFile(PROMPT_PATH, "utf8");
  return extractPromptSection(markdown);
}

async function main(): Promise<void> {
  const { apiKey, agentId, branchId } = requireElevenLabsSyncConfig();
  const prompt = await readPrompt();

  const client = axios.create({
    baseURL: config.elevenLabs.baseUrl,
    timeout: 45_000,
    headers: {
      "Content-Type": "application/json",
      "xi-api-key": apiKey,
    },
  });

  const { data: currentAgent } = await client.get<AgentResponse>(`/v1/convai/agents/${agentId}`, {
    params: {
      branch_id: branchId,
    },
  });

  const updatedConversationConfig = {
    ...currentAgent.conversation_config,
    turn: {
      ...currentAgent.conversation_config.turn,
      initial_wait_time: INITIAL_WAIT_TIME_SECONDS,
      turn_timeout: TURN_TIMEOUT_SECONDS,
      turn_eagerness: TURN_EAGERNESS,
      speculative_turn: SPECULATIVE_TURN,
      retranscribe_on_turn_timeout: RETRANSCRIBE_ON_TURN_TIMEOUT,
      soft_timeout_config: {
        ...currentAgent.conversation_config.turn.soft_timeout_config,
        timeout_seconds: SOFT_TIMEOUT_SECONDS,
        message: "One sec.",
        use_llm_generated_message: false,
      },
    },
    tts: {
      ...currentAgent.conversation_config.tts,
      ...(config.elevenLabs.voiceId ? { voice_id: config.elevenLabs.voiceId } : {}),
      ...(config.elevenLabs.ttsModel ? { model_id: config.elevenLabs.ttsModel } : {}),
    },
    agent: {
      ...currentAgent.conversation_config.agent,
      first_message: FIRST_MESSAGE,
      disable_first_message_interruptions: DISABLE_FIRST_MESSAGE_INTERRUPTION,
      backup_llm_config: {
        ...currentAgent.conversation_config.agent.backup_llm_config,
        preference: BACKUP_LLM_PREFERENCE,
      },
      cascade_timeout_seconds: CASCADE_TIMEOUT_SECONDS,
      prompt: {
        ...currentAgent.conversation_config.agent.prompt,
        prompt,
        llm: config.elevenLabs.primaryLlm,
        temperature: TEMPERATURE,
        max_tokens: MAX_TOKENS,
      },
    },
  };

  delete updatedConversationConfig.agent.prompt.tools;

  await client.patch(
    `/v1/convai/agents/${agentId}`,
    {
      conversation_config: updatedConversationConfig,
    },
    {
      params: {
        branch_id: branchId,
        enable_versioning_if_not_enabled: true,
      },
    },
  );

  logger.info("Synced ElevenLabs agent from local prompt source", {
    agentId,
    branchId,
    llm: config.elevenLabs.primaryLlm,
    voiceId: config.elevenLabs.voiceId,
    firstMessage: FIRST_MESSAGE,
    initialWaitTime: INITIAL_WAIT_TIME_SECONDS,
    turnTimeout: TURN_TIMEOUT_SECONDS,
    turnEagerness: TURN_EAGERNESS,
    speculativeTurn: SPECULATIVE_TURN,
    retranscribeOnTurnTimeout: RETRANSCRIBE_ON_TURN_TIMEOUT,
    disableFirstMessageInterruptions: DISABLE_FIRST_MESSAGE_INTERRUPTION,
    backupLlmPreference: BACKUP_LLM_PREFERENCE,
    cascadeTimeoutSeconds: CASCADE_TIMEOUT_SECONDS,
    softTimeoutSeconds: SOFT_TIMEOUT_SECONDS,
    temperature: TEMPERATURE,
    maxTokens: MAX_TOKENS,
  });
}

void main().catch((error) => {
  const axiosMessage =
    axios.isAxiosError(error) && error.response
      ? {
          status: error.response.status,
          statusText: error.response.statusText,
          data: error.response.data,
        }
      : undefined;
  logger.error("Unable to sync ElevenLabs agent", {
    message: error instanceof Error ? error.message : String(error),
    ...(axiosMessage ? { elevenLabs: axiosMessage } : {}),
  });
  process.exitCode = 1;
});
