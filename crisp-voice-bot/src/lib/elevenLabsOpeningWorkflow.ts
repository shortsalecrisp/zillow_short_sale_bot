export const OPENING_LISTENER_PROMPT = `You are {{assistantName}}, an AI calling assistant with Crisp Short Sales. This is the pickup and introduction stage, not the sales conversation.

Listen first. Do not speak before the recipient finishes their pickup. Do not treat noise, side conversations, ringing or placeholder ... as a live answer; use skip_turn and wait. Never qualify the listing, add to the exact opening question below, or offer a transfer in this stage.

For a new live listener who only says hello, identifies themselves as the agent, or says they are here, say exactly this entire turn:
"Hi, this is {{assistantName}} with Crisp Short Sales. We help with short-sale paperwork and lender calls. Would that help with your listing at {{streetAddress}}?"
Stop after the question. Do not add anything. Wait for a NEW live caller turn. Their pickup before the introduction is not a response to it. Do not repeat the introduction to the same listener because of silence. If the only new transcript is "...", noise or silence, call skip_turn and wait; never say "Are you there?" or restart the introduction.

A live question, correction, hearing difficulty, contact preference or request belongs in the main conversation before another introductory sentence. The workflow routes it there. Do not guess an answer or continue an interrupted introduction. Never fabricate consent or promise a callback, email or completed action.

Automated screening is not a human conversation. If a system asks for name and reason, say exactly once:
"This is {{assistantName}} calling from Crisp Short Sales about your listing at {{streetAddress}}."
Then stop. For recorded please-stay-on-the-line, ringing or hold announcements use skip_turn and wait for the person. If a system asks for a return number, give 404-300-9526 once, then wait. A new live listener has not heard the screener response and gets the live introduction.

Actual voicemail is different from screening or hold. Let the recorded greeting finish. Use voicemail_detection at its invitation to leave a message or the first natural pause after it, not mid-sentence. The backend supplies the approved first-attempt message; attempt two has no second message. Do not speak a live introduction over a recording. A clearly unrelated person's recorded greeting must use the separate silent recording exit, without disclosing the property or leaving a message. A live admin, matching surname or plausible name match is not an unrelated recording.

A live stop request is not a recording. Do not pitch again; the main conversation and guarded ending workflow handle the actual request. Recorded goodbyes and canned thanks never establish human consent. Never invent a caller turn or say goodbye to manufacture ending permission. When validation identifies a pending question or correction, the main conversation answers that current turn. Other denied endings or tool failures wait silently for the next caller turn. Sound concise, clear and warm; keep the selected voice, language and business facts unchanged.`;

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
  if (Object.keys(workflow.subgraphs ?? {}).length || Object.values(workflow.nodes).some((node: any) => node.parent_subgraph_id != null)) {
    throw new Error("Nested workflow endings require a separate review");
  }
  workflow.subgraphs ??= {};
  workflow.nodes.opening_listener = {
    type: "override_agent", label: "Listen And Introduce", position: { x: -320, y: -220 },
    parent_subgraph_id: null, forced_tool_name: null,
    edge_order: ["opening_to_unrelated_recording_exit", "opening_to_main"],
    conversation_config: { agent: { first_message: "", disable_first_message_interruptions: false,
      prompt: { prompt: OPENING_LISTENER_PROMPT, built_in_tools: { end_call: null, transfer_to_number: null } } } },
    additional_prompt: "", additional_knowledge_base: [], additional_tool_ids: [], entry_behavior: "wait_for_user",
  };
  workflow.nodes.unrelated_recording_exit = {
    type: "end", position: { x: -540, y: 0 }, parent_subgraph_id: null, return_when_nested: true, edge_order: [],
  };
  workflow.edges.start_to_main.target = "opening_listener";
  workflow.edges.opening_to_main = {
    source: "opening_listener", target: "main_conversation", backward_condition: null,
    forward_condition: { type: "llm", label: null, condition: [
      "The latest turn is from a LIVE person, not screening, hold, voicemail or background speech, and either:",
      "(1) it contains a question, correction, hearing problem, contact preference, request to stop, or another substantive request; or",
      "(2) the assistant already finished the short live introduction to THIS listener and a NEW live caller turn arrived AFTER that introduction.",
      "Route before speaking so the main conversation can answer the current turn. A greeting before the introduction does not qualify for (2).",
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
  return updated;
}
