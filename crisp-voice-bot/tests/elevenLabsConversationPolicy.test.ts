import assert from "node:assert/strict";
import test from "node:test";
import {
  applyConversationConsentPolicy, applyConversationListeningPolicy, applyConversationToolPolicy, applyContactToolDescriptions,
} from "../src/lib/elevenLabsConversationPolicy";
import {
  buildConversationRelease, bodyDigest, safeReceipt, verifyReleaseReadback, writableBody,
} from "../src/scripts/syncElevenLabsAgent";

function baseline(): any {
  return {
    conversation_config: {
      agent: { first_message: "Original intro", disable_first_message_interruptions: true,
        prompt: { prompt: "Current prompt", llm: "unchanged-model", tool_ids: ["live-tool"],
          built_in_tools: {
            skip_turn: { name: "skip_turn", description: "old skip", disable_interruptions: true },
            end_call: { name: "end_call", description: "old end", params: { system_tool_type: "end_call" } },
            voicemail_detection: { name: "voicemail_detection", description: "existing voicemail" },
          },
          tools: [{ request_headers: { Authorization: "test-secret" } }] } },
      turn: { initial_wait_time: 1.6, turn_timeout: 1.5, silence_end_call_timeout: 45,
        transcribe_on_disabled_interruptions: false },
      tts: { voice_id: "existing", speed: 0.95, model_id: "existing-model" },
      conversation: { client_events: ["audio", "interruption", "user_transcript"] },
    },
    workflow: { nodes: {
      main_conversation: { conversation_config: { conversation: { client_events: ["audio", "user_transcript"] } } },
      patch_transfer: { additional_prompt: "old", edge_order: ["patch_to_phone"] },
      callback_after_unavailable: { additional_prompt: "old" },
      phone_transfer: { transfer_destination: { phone_number: "+12025550123" } },
    }, edges: {
      main_to_patch_after_accepted_result: { source: "main_conversation", target: "patch_transfer" },
      main_to_callback_after_unavailable_result: { source: "main_conversation", target: "callback_after_unavailable" },
      main_to_transfer_check: { source: "main_conversation", target: "transfer_check", forward_condition: { condition: "existing consent" } },
      transfer_check_to_patch: { source: "transfer_check", target: "patch_transfer", forward_condition: { type: "result", successful: true } },
      transfer_check_to_callback: { source: "transfer_check", target: "callback_after_unavailable", forward_condition: { type: "result", successful: false } },
      patch_to_phone: { source: "patch_transfer", target: "phone_transfer" },
    }, prevent_subagent_loops: false },
    platform_settings: { overrides: { voice: true } }, phone_numbers: [{ id: "existing-phone" }],
    whatsapp_accounts: [], procedures: {},
  };
}

test("listening settings preserve all unrelated config and do not mutate input", () => {
  const before = baseline(), copy = structuredClone(before);
  const after = applyConversationListeningPolicy(before, { listenFirst: true });
  assert.deepEqual(before, copy);
  assert.equal(after.conversation_config.agent.first_message, "");
  assert.equal(after.conversation_config.agent.disable_first_message_interruptions, false);
  assert.equal(after.conversation_config.turn.transcribe_on_disabled_interruptions, true);
  assert.ok(after.workflow.nodes.main_conversation.conversation_config.conversation.client_events.includes("interruption"));
  assert.deepEqual(after.conversation_config.tts, before.conversation_config.tts);
  assert.deepEqual(after.conversation_config.agent.prompt, before.conversation_config.agent.prompt);
  assert.equal(after.conversation_config.turn.turn_timeout, 1.5);
  assert.equal(after.conversation_config.turn.silence_end_call_timeout, 45);
});

test("paired startup arms differ only in first_message", () => {
  const fixed = buildConversationRelease(baseline(), "Candidate", false);
  const listen = buildConversationRelease(baseline(), "Candidate", true);
  assert.equal(fixed.conversation_config.agent.first_message, "Original intro");
  fixed.conversation_config.agent.first_message = "";
  assert.deepEqual(fixed, listen);
});

test("tool descriptions follow listening and request rules without changing tool behavior", () => {
  const before = baseline(), copy = structuredClone(before), after = applyConversationToolPolicy(before);
  assert.deepEqual(before, copy);
  const tools = after.conversation_config.agent.prompt.built_in_tools;
  assert.match(tools.end_call.description, /tool success is not permission to end/);
  assert.match(tools.end_call.description, /Never end over an unanswered question/);
  assert.match(tools.skip_turn.description, /Spoken automated hold or connecting words are still a reason to wait/);
  assert.match(tools.skip_turn.description, /speak the exact base-prompt screener sentence first/);
  tools.end_call.description = before.conversation_config.agent.prompt.built_in_tools.end_call.description;
  tools.skip_turn.description = before.conversation_config.agent.prompt.built_in_tools.skip_turn.description;
  assert.deepEqual(after, before);
  delete before.conversation_config.agent.prompt.built_in_tools.end_call;
  assert.throws(() => applyConversationToolPolicy(before), /required/);
});

test("guarded waiting distinguishes new caller turns from already-handled turns and failed checks", () => {
  const before = baseline(), copy = structuredClone(before);
  const after = applyConversationToolPolicy(before, { guardedEnding: true });
  const tools = after.conversation_config.agent.prompt.built_in_tools;
  assert.deepEqual(before, copy);
  assert.equal(tools.end_call, null);
  assert.equal(tools.transfer_to_number, null);
  assert.match(tools.skip_turn.description, /NEW or still-unanswered/);
  assert.match(tools.skip_turn.description, /required brief answer or receipt acknowledgment has already been given/);
  assert.match(tools.skip_turn.description, /denied or failed ending check/);
  assert.match(tools.skip_turn.description, /handle any genuinely pending caller turn, otherwise wait silently/);
  assert.match(tools.skip_turn.description, /A tool result or placeholder is not a new caller turn/);
  assert.match(tools.skip_turn.description, /Do not repeat an answer or acknowledgment/);
  assert.equal(tools.skip_turn.disable_interruptions, before.conversation_config.agent.prompt.built_in_tools.skip_turn.disable_interruptions);
  assert.doesNotMatch(applyConversationToolPolicy(before).conversation_config.agent.prompt.built_in_tools.skip_turn.description, /denied or failed ending check/);
});

test("contact tool metadata changes only descriptions for webhook and client schemas", () => {
  for (const type of ["client", "webhook"]) {
    for (const name of ["callback_requested", "information_requested", "not_interested"]) {
      const schema = { type: "object", description: "old schema", required: ["rowNumber"], properties: {
        rowNumber: { type: "integer", dynamic_variable: "rowNumber" },
        email: { type: "string", description: "existing email", dynamic_variable: "email" },
        callbackTime: { type: "string", description: "old timing" },
        conversationSummary: { type: "string", description: "old summary" },
      } };
      const tool: any = { type, name, description: "old description", response_timeout_secs: 20,
        ...(type === "client" ? { parameters: schema, expects_response: true } : {
          api_schema: { url: "https://test.invalid/tool", method: "POST", request_headers: { Authorization: "test-only" }, request_body_schema: schema },
        }) };
      const before = structuredClone(tool), after: any = applyContactToolDescriptions(tool);
      assert.deepEqual(tool, before);
      const actual = type === "client" ? after.parameters : after.api_schema.request_body_schema;
      if (name === "callback_requested") {
        assert.match(after.description, /not a booked appointment/);
        assert.match(actual.properties.callbackTime.description, /requested timing words verbatim/);
        assert.match(actual.properties.callbackTime.description, /Do not convert words to digits, add an unspoken AM or PM/);
        assert.match(actual.properties.callbackTime.description, /uncertainty separately in conversationSummary for human review/);
        assert.match(actual.properties.callbackTime.description, /never permission to infer ASAP/);
        assert.doesNotMatch(actual.properties.callbackTime.description, /Ask once|AM\/PM unconfirmed|two p\.m\./i);
        assert.match(actual.properties.conversationSummary.description, /Retain earlier supplied day, time zone, person or number context when a correction is partial/);
        assert.match(actual.properties.conversationSummary.description, /Note unresolved timing for human review/);
        assert.match(actual.properties.conversationSummary.description, /Do not leave out supplied context or uncertainty to save time/);
      } else if (name === "information_requested") {
        assert.match(after.description, /caller-confirmed or clearly supplied/);
        assert.match(after.description, /execute this tool before acknowledging receipt/);
        assert.match(after.description, /Confirm a stored address with the caller once/);
        assert.match(after.description, /do not repeat an address they just clearly supplied/);
        assert.match(after.description, /Do not claim receipt without a successful requestCaptured result/);
        assert.match(actual.description, /a stored address alone is not confirmation/);
        assert.match(actual.description, /not a sent or delivered email/);
        assert.match(actual.properties.conversationSummary.description, /actual request, email-only preference/);
        assert.match(actual.properties.conversationSummary.description, /receipt from a tool that did not return confirmation/);
      } else {
        assert.match(after.description, /end only the current call/);
        assert.match(actual.properties.conversationSummary.description, /CALL ENDED BY REQUEST/);
      }
      assert.deepEqual(actual.properties.email, schema.properties.email);
      after.description = before.description;
      actual.description = schema.description;
      const changedProperty = name === "callback_requested" ? "callbackTime" : "conversationSummary";
      actual.properties[changedProperty].description = schema.properties[changedProperty].description;
      if (name === "callback_requested") actual.properties.conversationSummary.description = schema.properties.conversationSummary.description;
      assert.deepEqual(after, before);
    }
  }
  assert.throws(() => applyContactToolDescriptions({ name: "other", type: "client" }), /required/);
  assert.throws(() => applyContactToolDescriptions({
    name: "information_requested", type: "client", parameters: { properties: { email: { type: "string" } } },
  }), /Information request summary schema is required/);
  assert.throws(() => applyContactToolDescriptions({
    name: "callback_requested", type: "client", parameters: { properties: { callbackTime: { type: "string" } } },
  }), /Callback request summary schema is required/);
});

test("HTTP tool success returns to business-result validation, not directly to a phone patch", () => {
  const after = applyConversationConsentPolicy(baseline());
  assert.equal(after.workflow.edges.transfer_check_to_patch.target, "transfer_result_review");
  assert.equal(after.workflow.edges.transfer_result_to_main.target, "main_conversation");
  assert.equal(after.workflow.edges.transfer_result_to_main.forward_condition.type, "unconditional");
  assert.equal(after.workflow.edges.transfer_check_to_callback.target, "callback_after_unavailable");
  const pairs = Object.values(after.workflow.edges).map((edge: any) => [edge.source, edge.target].sort().join(":"));
  assert.equal(new Set(pairs).size, pairs.length);
  assert.match(after.workflow.nodes.patch_transfer.additional_prompt, /transferApproved true AND approvalStatus accepted/);
  assert.match(after.workflow.nodes.patch_transfer.additional_prompt, /in_progress, missing approval fields, declined/);
  assert.match(after.workflow.edges.main_to_patch_after_accepted_result.forward_condition.condition, /BOTH approvalStatus accepted and transferApproved true/);
  assert.match(after.workflow.edges.main_to_callback_after_unavailable_result.forward_condition.condition, /Do not route on in_progress or missing fields/);
});

test("changed preference edge has priority and fallback never infers callback from a question", () => {
  const before = baseline(), after = applyConversationConsentPolicy(before);
  assert.deepEqual(after.workflow.nodes.patch_transfer.edge_order, ["main_to_patch_after_accepted_result", "patch_to_phone"]);
  assert.match(after.workflow.edges.main_to_patch_after_accepted_result.backward_condition.condition, /withdraws live-transfer consent/);
  assert.match(after.workflow.nodes.callback_after_unavailable.additional_prompt, /Do not create a callback merely because you answered a question/);
  assert.deepEqual(after.workflow.nodes.phone_transfer, before.workflow.nodes.phone_transfer);
  assert.deepEqual(after.workflow.edges.main_to_transfer_check, before.workflow.edges.main_to_transfer_check);
  assert.deepEqual(applyConversationConsentPolicy(after), after);
});

test("release preserves tool IDs and removes expanded credential-bearing GET schemas", () => {
  const before = baseline(), body = buildConversationRelease(before, "Candidate", true);
  assert.equal(body.conversation_config.agent.prompt.tools, undefined);
  assert.deepEqual(body.conversation_config.agent.prompt.tool_ids, ["live-tool"]);
  assert.deepEqual(body.conversation_config.agent.prompt.built_in_tools,
    applyConversationToolPolicy(before).conversation_config.agent.prompt.built_in_tools);
  assert.ok(!JSON.stringify(safeReceipt(before)).includes("test-secret"));
  assert.ok(!JSON.stringify(writableBody(before)).includes("test-secret"));
});

test("readback compares the exact reviewed candidate and catches unrelated drift", () => {
  const before = baseline(), candidate = buildConversationRelease(before, "Candidate", true);
  const after = { ...structuredClone(before), ...structuredClone(candidate) };
  verifyReleaseReadback(before, candidate, after);
  after.conversation_config.tts.speed = 1.1;
  assert.throws(() => verifyReleaseReadback(before, candidate, after), /readback mismatch/);
  after.conversation_config.tts.speed = 0.95;
  after.platform_settings.overrides.voice = false;
  assert.throws(() => verifyReleaseReadback(before, candidate, after), /Unrelated provider field/);
  assert.equal(bodyDigest({ a: 1, b: 2 }), bodyDigest({ b: 2, a: 1 }));
});

test("unknown workflow layouts fail closed", () => {
  const b = baseline(); delete b.workflow.nodes.main_conversation;
  assert.throws(() => applyConversationListeningPolicy(b, { listenFirst: true }), /required/);
  delete b.workflow.edges.transfer_check_to_patch;
  assert.throws(() => applyConversationConsentPolicy(b), /required/);
});
