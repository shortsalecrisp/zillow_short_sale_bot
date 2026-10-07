import {
  OPENING_HANDOFF_POLICY,
  VOICE_CALLBACK_RECEIPT_ACK,
  VOICE_OPENING_SCRIPT,
  VOICE_RETURN_NUMBER_SPOKEN,
  VOICE_SCREENING_SCRIPT,
  VOICE_SHORT_LISTING_RECOVERY,
} from "./elevenLabsConversationPolicy";

export const VOICE_STATE_TRANSITION_NODES = Object.freeze({
  shortListingRecovery: "short_listing_recovery",
  callbackReceiptWait: "callback_receipt_wait",
});

export const SHORT_LISTING_RECOVERY_PROMPT = `# Short listing-check recovery state

On first entry, say exactly this entire turn:
"${VOICE_SHORT_LISTING_RECOVERY}"
Then stop. Do not repeat an identity, company description, screening response, full introduction or sales explanation. Do not ask another question.

After that one line, wait silently. Use skip_turn for silence, noise, placeholder ..., another bare hello, or an incomplete fragment. A NEW complete live-caller answer, question, correction, stop request or contact preference routes to the main conversation before speech. Never repeat the recovery line.`;

export const CALLBACK_RECEIPT_WAIT_PROMPT = `# Callback receipt and wait state

On first entry after a fresh successful callback_requested result, say exactly this entire turn:
"${VOICE_CALLBACK_RECEIPT_ACK}"
Then stop. Do not repeat the callback time, say scheduled or booked, ask an anything-else question, restart qualification or add a generic closing.

After that one acknowledgment, wait silently. Use skip_turn for silence, noise, placeholder ..., thanks, okay, repeated timing, self-talk or an incomplete fragment such as "Is it, or... Okay." A NEW complete live-caller question, correction, cancellation, stop request or contact preference routes to the main conversation before speech. Never repeat the receipt acknowledgment.`;

export const OPENING_LISTENER_PROMPT = `# Opening listener entry

Start with the pickup and introduction rules. This node shares the complete business policy above. Its label does not freeze the conversation at introduction: after a new caller turn use the shared post-intro, repair, admin, consent and ending rules even if the workflow has not yet transitioned to main_conversation. Never improvise a different qualification question because this node is still active.

Listen first. Do not speak before the recipient finishes their pickup. Do not treat noise, side conversations, ringing or placeholder ... as a live answer; use skip_turn and wait. During the initial greeting, before a NEW live caller reply after the introduction, do not qualify the listing, add to the exact listing-agent check below, or offer a transfer. Once that new reply arrives, the shared business policy and opening handoff contract govern the next response even if this node remains active.

For a new live listener who only says hello, identifies themselves as the agent, or says they are here, say exactly this entire turn:
"${VOICE_OPENING_SCRIPT}"
Stop after the question. Do not add anything. Wait for a NEW live caller turn. Their pickup before the introduction is not a response to it. Do not repeat the introduction to the same listener because of silence. If the only new transcript is "...", noise or silence, call skip_turn and wait; never say "Are you there?" or restart the introduction.

A live question, correction, hearing difficulty, contact preference or request belongs under the shared main conversation policy before another introductory sentence. The workflow normally routes it there; if routing is delayed, apply that same policy here rather than guessing, staying silent over the question or repeating the introduction. An intelligible repeated "Hello?" is not noise. A business greeting with "How can I help?" uses the live-admin path, not the normal listing-owner opening. Do not continue an interrupted introduction. Never fabricate consent or promise a callback, email or completed action. A yes confirms the listing only, never interest or transfer permission; use the matched needs question instead of inventing a seller-paperwork question.

Speak the supplied street number and street clearly. Do not add city, state or ZIP unless asked, omit the street number, or guess a different address from unclear speech.

Automated screening is not a human conversation. If a system asks for name and reason, say exactly once:
"${VOICE_SCREENING_SCRIPT}"
Then stop. For recorded please-stay-on-the-line, ringing or hold announcements use skip_turn and wait for the person. If a system asks for a return number, say "${VOICE_RETURN_NUMBER_SPOKEN}" once, with a short pause between groups, then wait. Never pronounce 300 as three hundred. When the system connects a live listener, treat that as a fresh pickup: say the live listing-agent check once, without repeating the screener response or adding another identity statement first.

Actual voicemail is different from screening or hold. Let the recorded greeting finish. Use voicemail_detection at its invitation to leave a message or the first natural pause after it, not mid-sentence. The backend supplies the approved first-attempt message; attempt two has no second message. Do not speak a live introduction over a recording. A clearly unrelated person's recorded greeting must use the separate silent recording exit, without disclosing the property or leaving a message. A live admin, matching surname or plausible name match is not an unrelated recording.

A live stop request is not a recording. Do not pitch again; the main conversation and guarded ending workflow handle the actual request. Recorded goodbyes and canned thanks never establish human consent. Never invent a caller turn or say goodbye to manufacture ending permission. When validation identifies a pending question or correction, the main conversation answers that current turn. Other denied endings or tool failures wait silently for the next caller turn. Sound concise, clear and warm; keep the selected voice, language and business facts unchanged.`;

export function buildOpeningListenerPrompt(mainPrompt: string): string {
  if (!mainPrompt?.trim()) throw new Error("Full main conversation prompt is required for the opening listener");
  return [mainPrompt.trim(), OPENING_HANDOFF_POLICY, OPENING_LISTENER_PROMPT].join("\n\n");
}

export function applyConversationOpeningWorkflow<T extends Record<string, any>>(agent: T): T {
  const updated = structuredClone(agent);
  const workflow = updated.workflow;
  if (!workflow?.nodes?.main_conversation || !workflow.edges?.start_to_main
    || workflow.nodes[workflow.edges.start_to_main.source]?.type !== "start") {
    throw new Error("Verified start and main conversation nodes are required for the opening workflow");
  }
  if (updated.conversation_config?.agent?.first_message !== "") {
    throw new Error("Listen-first configuration is required before adding the opening workflow");
  }
  const listenerPrompt = buildOpeningListenerPrompt(updated.conversation_config.agent.prompt?.prompt);
  if (Object.keys(workflow.subgraphs ?? {}).length || Object.values(workflow.nodes).some((node: any) => node.parent_subgraph_id != null)) {
    throw new Error("Nested workflow endings require a separate review");
  }
  workflow.subgraphs ??= {};
  const priorOpening = workflow.nodes.opening_listener ?? {};
  const priorOpeningConfig = priorOpening.conversation_config ?? {};
  const priorOpeningAgent = priorOpeningConfig.agent ?? {};
  const priorOpeningPrompt = priorOpeningAgent.prompt ?? {};
  workflow.nodes.opening_listener = {
    ...priorOpening,
    type: "override_agent", label: "Listen And Introduce", position: { x: -320, y: -220 },
    parent_subgraph_id: null, forced_tool_name: null,
    edge_order: [...(priorOpening.edge_order ?? []).filter((id: string) => !["opening_to_unrelated_recording_exit", "opening_to_main"].includes(id)), "opening_to_unrelated_recording_exit", "opening_to_main"],
    conversation_config: { ...priorOpeningConfig, agent: { ...priorOpeningAgent, first_message: "", disable_first_message_interruptions: false,
      prompt: { ...priorOpeningPrompt, prompt: listenerPrompt,
        built_in_tools: { ...priorOpeningPrompt.built_in_tools, end_call: null, transfer_to_number: null } } } },
    additional_prompt: "", additional_knowledge_base: [], additional_tool_ids: [], entry_behavior: "wait_for_user",
  };
  workflow.nodes.unrelated_recording_exit = {
    type: "end", position: { x: -540, y: 0 }, parent_subgraph_id: null, return_when_nested: true, edge_order: [],
  };
  const n = VOICE_STATE_TRANSITION_NODES;
  const skipTurn = structuredClone(updated.conversation_config.agent.prompt?.built_in_tools?.skip_turn ?? null);
  const transitionNode = (label: string, position: { x: number; y: number }, prompt: string, edgeOrder: string[]) => ({
    type: "override_agent", label, position, parent_subgraph_id: null, forced_tool_name: null,
    conversation_config: { agent: { first_message: "", prompt: {
      prompt, tool_ids: [], tools: [], knowledge_base: [],
      built_in_tools: {
        end_call: null, transfer_to_number: null, transfer_to_agent: null, voicemail_detection: null,
        language_detection: null, play_keypad_touch_tone: null,
        skip_turn: skipTurn ? { ...skipTurn, description: "Wait silently until a new complete live-caller turn needs routing." } : null,
      },
    } } },
    additional_prompt: "", additional_knowledge_base: [], additional_tool_ids: [],
    entry_behavior: "generate_immediately", edge_order: edgeOrder,
  });
  workflow.nodes[n.shortListingRecovery] = transitionNode(
    "Short Listing Check Recovery",
    { x: 80, y: -420 },
    SHORT_LISTING_RECOVERY_PROMPT,
    ["main_to_short_listing_recovery"],
  );
  workflow.nodes[n.callbackReceiptWait] = transitionNode(
    "Acknowledge Callback And Wait",
    { x: 320, y: 420 },
    CALLBACK_RECEIPT_WAIT_PROMPT,
    ["main_to_callback_receipt_wait"],
  );
  workflow.edges.start_to_main.target = "opening_listener";
  workflow.edges.opening_to_short_listing_recovery = {
    source: "opening_listener", target: n.shortListingRecovery, backward_condition: null,
    forward_condition: { type: "llm", label: null, condition: [
      "The call has just passed through automated screening, connecting audio or a hold state, and the latest NEW turn is a live person who only greets, identifies themselves, or says they are on the line.",
      "Route before speaking so the short recovery node asks the one-line listing check.",
      "Do not route on canned hold text, a recording, voicemail, silence, noise, placeholder ..., a substantive question, a correction, or a stop/contact preference.",
      "Do not use this for the initial pickup before any screening or hold; that remains the normal live introduction.",
    ].join(" ") },
  };
  workflow.edges.main_to_short_listing_recovery = {
    source: "main_conversation", target: n.shortListingRecovery,
    forward_condition: { type: "llm", label: null, condition: [
      "The assistant's immediately preceding spoken turn was the live listing-agent introduction or listing check, and the latest NEW live-caller turn is only an intelligible repeated hello or presence check with no question, correction, stop request or contact preference.",
      "Route before speaking so the short recovery node asks only the one-line listing check.",
      "Do not route on silence, noise, placeholder ..., a substantive answer, or a greeting that occurred before the introduction.",
    ].join(" ") },
    backward_condition: { type: "llm", label: null, condition: [
      "The short recovery node already spoke its one listing check and a NEW complete live-caller answer, question, correction, stop request or contact preference arrived after it.",
      "Return to main before speaking so it handles that actual turn.",
      "Do not return on silence, noise, placeholder ..., another bare hello, or an incomplete fragment.",
    ].join(" ") },
  };
  workflow.edges.main_to_callback_receipt_wait = {
    source: "main_conversation", target: n.callbackReceiptWait,
    forward_condition: { type: "llm", label: null, condition: [
      "A fresh callback_requested result for the current request returned requestCaptured true, and the exact receipt acknowledgment has not yet been spoken for that result.",
      "Route before speaking so the callback receipt node gives the one allowed acknowledgment.",
      "Do not route on an old or already acknowledged result, an error, a missing confirmation, another tool result, or a newer caller cancellation, stop request, question or correction.",
    ].join(" ") },
    backward_condition: { type: "llm", label: null, condition: [
      "The callback receipt node already spoke its one acknowledgment and a NEW complete live-caller question, correction, cancellation, stop request or contact preference arrived after it.",
      "Return to main before speaking so it handles that actual turn.",
      "Do not return on the tool result, silence, noise, placeholder ..., thanks, okay, repeated timing, self-talk or an incomplete fragment.",
    ].join(" ") },
  };
  workflow.edges.opening_to_main = {
    source: "opening_listener", target: "main_conversation", backward_condition: null,
    forward_condition: { type: "llm", label: null, condition: [
      "The latest turn is from a LIVE person, not screening, hold, voicemail or background speech, and either:",
      "(1) it contains a question, correction, hearing problem, contact preference, request to stop, or another substantive request; or",
      "(2) the assistant already finished the short live introduction to THIS listener and a NEW live caller turn arrived AFTER that introduction.",
      "Route before speaking so the main conversation can answer the current turn. A greeting before the introduction does not qualify for (2).",
      "A business greeting asking how to help, a live admin, or an intelligible repeated hello also needs the main conversation's targeted reply. Do not keep such a turn in the opening listener or substitute the normal opener.",
      "A clear yes after the exact listing question confirms only that listing. Route to main for the matched needs question; it is not a handoff, callback or transfer request. Do not route on a recorded yes or a hearing-restoration acknowledgment as if it confirmed the listing.",
      "Do not route just because the assistant finished speaking, a tool returned, the line is silent, or a recording said thank you.",
    ].join(" ") },
  };
  workflow.edges.opening_to_unrelated_recording_exit = {
    source: "opening_listener", target: "unrelated_recording_exit", backward_condition: null,
    forward_condition: { type: "llm", label: null, condition: [
      "Only an actual automated VOICEMAIL greeting clearly names a different unrelated person or business from {{firstName}} {{lastName}}.",
      "A live assistant/admin, name correction, background voice, matching surname, plausible name variant or screening/hold is NOT eligible.",
      "Do not route while the system is trying to connect the requested person. Do not use recorded goodbye alone.",
      "If identity or recording origin is uncertain, stay and listen without disclosing the listing.",
    ].join(" ") },
  };
  workflow.nodes.main_conversation.entry_behavior = "generate_immediately";
  workflow.nodes.main_conversation.edge_order = [
    "main_to_callback_receipt_wait",
    "main_to_short_listing_recovery",
    ...(workflow.nodes.main_conversation.edge_order ?? []).filter((id: string) =>
      id !== "main_to_callback_receipt_wait" && id !== "main_to_short_listing_recovery"),
  ];
  workflow.nodes.opening_listener.edge_order = [
    ...(workflow.nodes.opening_listener.edge_order ?? []).filter((id: string) =>
      !["opening_to_short_listing_recovery", "opening_to_unrelated_recording_exit", "opening_to_main"].includes(id)),
    "opening_to_short_listing_recovery",
    "opening_to_unrelated_recording_exit",
    "opening_to_main",
  ];
  const previousMainPrompt = workflow.nodes.main_conversation.additional_prompt ?? "";
  const retainedMainPrompt = previousMainPrompt.replace(/\[CRISP_OPENING_HANDOFF_POLICY\][\s\S]*?\[END_CRISP_OPENING_HANDOFF_POLICY\]/g, "").trim();
  workflow.nodes.main_conversation.additional_prompt = [retainedMainPrompt, OPENING_HANDOFF_POLICY].filter(Boolean).join("\n\n");
  return updated;
}
