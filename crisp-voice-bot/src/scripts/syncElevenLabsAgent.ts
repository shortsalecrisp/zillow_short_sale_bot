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
    conversation: {
      client_events?: string[];
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
  workflow?: Record<string, unknown>;
};

type ToolResponse = {
  id: string;
  tool_config: {
    name?: string;
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
const MAX_TOKENS = 80;
const BACKUP_LLM_PREFERENCE = "disabled";
const CASCADE_TIMEOUT_SECONDS: number | null = null;

function asStringArray(value: unknown): string[] {
  if (!Array.isArray(value)) {
    return [];
  }

  return value.filter((item): item is string => typeof item === "string");
}

function buildWarmTransferWorkflow(options: {
  liveTransferToolId: string;
  liveTransferNumber: string;
  baseClientEvents: string[];
}): Record<string, unknown> {
  const patchNodeClientEvents = options.baseClientEvents.filter((event) => event !== "interruption");

  return {
    edges: {
      start_to_main: {
        source: "start_node",
        target: "main_conversation",
        forward_condition: {
          type: "unconditional",
        },
      },
      main_to_patch_after_accepted_result: {
        source: "main_conversation",
        target: "patch_transfer",
        forward_condition: {
          type: "llm",
          condition:
            "Route to patch_transfer only when the most recent live_transfer_requested tool result clearly shows approvalStatus is accepted or transferApproved is true. Do not use this edge before a live_transfer_requested tool result exists.",
        },
      },
      main_to_callback_after_unavailable_result: {
        source: "main_conversation",
        target: "callback_after_unavailable",
        forward_condition: {
          type: "llm",
          condition:
            "Route to callback_after_unavailable only when the most recent live_transfer_requested tool result clearly shows Yoni was not available, approvalStatus is anything other than accepted, or transferApproved is false. Do not use this edge before a live_transfer_requested tool result exists.",
        },
      },
      main_to_transfer_check: {
        source: "main_conversation",
        target: "transfer_check",
        forward_condition: {
          type: "llm",
          condition:
            "Route to transfer_check only when the caller clearly wants to talk to Yoni right now, asks if he is available right now, or agrees to trying him now, and there is not already a fresh live_transfer_requested tool result waiting to be handled for this same handoff moment.",
        },
      },
      transfer_check_to_patch: {
        source: "transfer_check",
        target: "patch_transfer",
        forward_condition: {
          type: "result",
          successful: true,
        },
      },
      transfer_check_to_callback: {
        source: "transfer_check",
        target: "callback_after_unavailable",
        forward_condition: {
          type: "result",
          successful: false,
        },
      },
      patch_to_phone: {
        source: "patch_transfer",
        target: "phone_transfer",
        forward_condition: {
          type: "llm",
          condition:
            "Route to phone_transfer only after the assistant has already told the caller that Yoni is available and that it is patching him in now.",
        },
      },
    },
    nodes: {
      start_node: {
        type: "start",
        label: "Start",
        edge_order: ["start_to_main"],
        position: { x: 0, y: 0 },
      },
      main_conversation: {
        type: "override_agent",
        label: "Main Conversation",
        additional_prompt: "",
        additional_tool_ids: [],
        additional_knowledge_base: [],
        conversation_config: {},
        edge_order: [
          "main_to_patch_after_accepted_result",
          "main_to_callback_after_unavailable_result",
          "main_to_transfer_check",
        ],
        position: { x: 280, y: 0 },
      },
      transfer_check: {
        type: "tool",
        edge_order: ["transfer_check_to_patch", "transfer_check_to_callback"],
        position: { x: 560, y: 0 },
        tools: [
          {
            tool_id: options.liveTransferToolId,
          },
        ],
      },
      patch_transfer: {
        type: "override_agent",
        label: "Patch Transfer",
        additional_prompt: [
          "You are in the live-transfer patching step.",
          "Your one and only spoken line in this node is exactly:",
          "\"Ok good news, I've got him. Patching him in now.\"",
          "Do not answer interruptions, questions, filler, coughs, or acknowledgments.",
          "Do not use any tools from this node.",
          "Do not say anything before or after that exact line.",
        ].join("\n"),
        additional_tool_ids: [],
        additional_knowledge_base: [],
        conversation_config: {
          conversation: {
            client_events: patchNodeClientEvents,
          },
        },
        edge_order: ["patch_to_phone"],
        position: { x: 840, y: -120 },
      },
      callback_after_unavailable: {
        type: "override_agent",
        label: "Callback Fallback",
        additional_prompt: [
          "You are in the live-transfer unavailable fallback step.",
          "Immediately say exactly:",
          "\"Sorry, he was not available right now, but I will text him and ask him to call you back ASAP. Is that ok?\"",
          "Then wait for the caller to respond.",
          "If they say yes, sure, ok, sounds good, or thanks, call callback_requested with callbackTime set to asap.",
          "After that tool succeeds, say exactly: \"Ok, thanks, sounds good. Bye!\"",
          "Then immediately call end_call.",
          "If they ask one brief follow-up question, answer it briefly, then still call callback_requested with callbackTime asap and end the call.",
          "Do not offer another live transfer from this node.",
        ].join("\n"),
        additional_tool_ids: [],
        additional_knowledge_base: [],
        conversation_config: {},
        edge_order: [],
        position: { x: 840, y: 120 },
      },
      phone_transfer: {
        type: "phone_number",
        edge_order: [],
        position: { x: 1120, y: -120 },
        transfer_destination: {
          type: "phone",
          phone_number: options.liveTransferNumber,
        },
        transfer_type: "conference",
      },
    },
    prevent_subagent_loops: false,
  };
}

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

  const toolIds = asStringArray(currentAgent.conversation_config.agent.prompt.tool_ids);
  const toolResponses = await Promise.all(
    toolIds.map(async (toolId) => {
      const { data } = await client.get<ToolResponse>(`/v1/convai/tools/${toolId}`);
      return data;
    }),
  );

  const liveTransferTool = toolResponses.find((tool) => tool.tool_config.name === "live_transfer_requested");

  if (!liveTransferTool) {
    throw new Error("Unable to find live_transfer_requested tool for ElevenLabs workflow sync");
  }

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

  const workflow = buildWarmTransferWorkflow({
    liveTransferToolId: liveTransferTool.id,
    liveTransferNumber: config.liveTransferNumber,
    baseClientEvents: asStringArray(currentAgent.conversation_config.conversation?.client_events),
  });

  delete updatedConversationConfig.agent.prompt.tools;

  await client.patch(
    `/v1/convai/agents/${agentId}`,
    {
      conversation_config: updatedConversationConfig,
      workflow,
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
    workflowEnabled: true,
    liveTransferToolId: liveTransferTool.id,
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
