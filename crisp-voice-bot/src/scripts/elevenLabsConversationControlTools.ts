import { isDeepStrictEqual } from "node:util";
import { contactToolDigest } from "./prepareElevenLabsContactTools";

export const CONVERSATION_CONTROL_NAMES = { reset: "reset_call_end_permission", validate: "validate_call_ending" } as const;
export const CONVERSATION_CONTROL_BASE_URL = "https://crisp-voice-bot.onrender.com/elevenlabs/conversation-control";

export function conversationControlTools(secret: string | undefined): Record<"reset" | "validate", any> {
  if (typeof secret !== "string" || !secret.trim()) throw new Error("Configured voice-tool authentication is required");
  const property = (name: string) => ({ type: "string", dynamic_variable: "system__" + name });
  return Object.fromEntries((["reset", "validate"] as const).map(kind => [kind, {
    type: "webhook", name: CONVERSATION_CONTROL_NAMES[kind],
    description: kind === "reset" ? "Reset permission before the guarded call-ending validation." : "Validate the latest provider-bound caller history before ending this live call. This has no contact-action side effects.",
    response_timeout_secs: 10, interruption_mode: "allow", pre_tool_speech: "off", execution_mode: "immediate",
    assignments: [
      { source: "response", dynamic_variable: "terminal_permission", value_path: "permission", sanitize: true, preserve_native_type: true },
      { source: "response", dynamic_variable: "terminal_decision", value_path: "decision", sanitize: true, preserve_native_type: true },
    ],
    dynamic_variables: { dynamic_variable_placeholders: {} },
    api_schema: { url: CONVERSATION_CONTROL_BASE_URL + (kind === "reset" ? "/reset-ending" : "/validate-ending"),
      method: "POST", request_headers: { "x-crisp-elevenlabs-secret": secret },
      path_params_schema: {}, query_params_schema: null,
      request_body_schema: { type: "object", required: kind === "reset" ? ["conversation_id"] : ["conversation_id", "history"],
        properties: { conversation_id: property("conversation_id"), ...(kind === "validate" ? { history: property("conversation_history") } : {}) } },
    },
    follow_redirects: false, follow_redirects_allowed_domains: [],
  }])) as Record<"reset" | "validate", any>;
}

export function normalizeControlToolReadback(expected: any, readback: any): any {
  const result = structuredClone(readback);
  const defaults = (value: any, wanted: any, allowed: Record<string, any>) => {
    for (const [key, neutral] of Object.entries(allowed)) if (!(key in wanted) && key in value) {
      if (!isDeepStrictEqual(value[key], neutral)) throw new Error("Non-neutral control tool default: " + key);
      delete value[key];
    }
  };
  if (!result?.api_schema?.request_body_schema?.properties) throw new Error("Control tool schema missing");
  defaults(result, expected, { disable_interruptions: false, force_pre_tool_speech: false, tool_call_sound: null,
    tool_call_sound_behavior: "auto", tool_error_handling_mode: "auto" });
  defaults(result.api_schema, expected.api_schema, { kind: "webhook", response_body_schema: null, response_filter: null,
    content_type: "application/json", auth_resolved_params: [], auth_connection: null, mtls_auth_connection: null });
  const schema = result.api_schema.request_body_schema, wanted = expected.api_schema.request_body_schema;
  defaults(schema, wanted, { description: "", dynamic_variable: "", is_omitted: false });
  for (const [name, property] of Object.entries(wanted.properties)) {
    if (!schema.properties[name]) throw new Error("System-bound control parameter missing");
    defaults(schema.properties[name], property, { description: "", enum: null, is_system_provided: false,
      allowed_values: null, allowed_values_dynamic_variable: "", constant_value: "", is_omitted: false });
  }
  if (!isDeepStrictEqual(result, expected)) throw new Error("Control tool readback differs from its exact authenticated system-bound contract");
  return result;
}

export async function verifyConversationControlMap(client: { get: (url: string) => Promise<{ data: any }> },
  map: any, current: any, secret: string | undefined, expectedHash?: string): Promise<{ resetToolId: string; validateToolId: string; mainNodeId: string }> {
  const { map_sha256: recordedHash, ...payload } = map ?? {};
  if (!/^[a-f0-9]{64}$/.test(recordedHash ?? "") || contactToolDigest(payload) !== recordedHash
    || (expectedHash && expectedHash !== recordedHash) || map.agent_id !== current.agent_id || map.branch_id !== current.main_branch_id
    || map.voice_only !== true || map.schema_version !== 1) throw new Error("Exact reviewed conversation-control map required");
  const expected = conversationControlTools(secret), ids: string[] = [];
  for (const kind of ["reset", "validate"] as const) {
    const entry = map.tools?.[kind], id = entry?.id;
    if (!/^tool_[a-z0-9]+$/.test(id ?? "") || ids.includes(id)) throw new Error("Distinct verified control tool IDs required");
    ids.push(id);
    const { data } = await client.get("/v1/convai/tools/" + id);
    const normalized = normalizeControlToolReadback(expected[kind], data.tool_config);
    if (data.id !== id || contactToolDigest(normalized) !== entry.config_sha256) throw new Error("Conversation-control tool changed");
  }
  return { resetToolId: ids[0], validateToolId: ids[1], mainNodeId: "main_conversation" };
}
