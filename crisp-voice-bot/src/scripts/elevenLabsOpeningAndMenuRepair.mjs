const suffixMarker = '\n\n[CRISP_OPENING_HANDOFF_POLICY]';
const opening = 'Before identity has been spoken, an unscreened live hello, name or readiness cue requires the entire Maya/Crisp introduction below. Silence or skip_turn never counts as identity or screening.';
const terminal = 'On attempt two, a recorded screener ending with “not available” and an additional-message invitation goes directly to recorded_no_voicemail_exit. On either attempt, the same completed automated phone menu played twice without an intervening human also goes directly to recorded_no_voicemail_exit. End silently before skip_turn. Preserve a real hold, later live speech and normal mailbox handling.';

export function applyOpeningAndMenuRepair(agent) {
  const result = structuredClone(agent);
  const global = result.conversation_config.agent.prompt;
  const listener = result.workflow.nodes.opening_listener.conversation_config.agent.prompt;
  const main = result.workflow.nodes.main_conversation;
  const [embedded, suffix, extra] = listener.prompt.split(suffixMarker);
  if (extra !== undefined || !suffix || embedded !== global.prompt ||
      result.workflow.nodes.recorded_no_voicemail_exit?.type !== 'end') {
    throw new Error('The opening and existing silent exit must match the reviewed release');
  }
  const previous = 'Without earlier screening/hold, the first live hello, name or readiness cue requires this entire opening once. Names, repeated hellos or skipped silence never prove delivered identity:';
  if (global.prompt.split(previous).length !== 2) throw new Error('Expected opening instruction missing');
  global.prompt = global.prompt.replace(previous, opening);
  listener.prompt = global.prompt + suffixMarker + '\n\n' + terminal + '\n\n' + suffix;
  main.additional_prompt = opening + '\n\n' + terminal + '\n\n' + main.additional_prompt;
  for (const source of ['opening_listener', 'main_conversation']) {
    const id = source + '_to_recorded_no_voicemail_exit';
    const edge = result.workflow.edges[id];
    if (edge?.source !== source || edge.target !== 'recorded_no_voicemail_exit' ||
        !result.workflow.nodes[source].edge_order.includes(id)) throw new Error('Expected existing exit edge missing');
    edge.forward_condition.condition = terminal + ' ' + edge.forward_condition.condition;
  }
  return result;
}
