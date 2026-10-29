import { digest } from './verifyElevenLabsComparison.mjs';

export const TRANSFER_CONTEXTS = ['opening_listener', 'main_conversation', 'patch_transfer'];
const marker = '[CRISP_ACCEPTED_TRANSFER_V1]';
export const ACCEPTED_TRANSFER_POLICY = `${marker}
The availability tool and the actual phone connection are different actions. A fresh live_transfer_requested result with transferApproved true AND approvalStatus accepted explicitly returns control to this node for the enabled transfer_to_number action, even if this is still opening_listener.
Read the latest caller turn first. Only if the caller explicitly chose Yoni on this call now and that choice remains current, with no unanswered question, correction, request to wait, callback-only or email-only preference, opt-out, refusal, cancellation or human takeover, invoke transfer_to_number exactly once for the configured Yoni destination. Use its client_message to say "Please hold while I connect you to Yoni." Do not merely say the line, skip_turn, wait for another yes or depend on moving to main_conversation. An availability check alone or a yes to a different question is not transfer consent.
Never call transfer_to_number on pending, declined, timeout, call_failed, missing fields or stale approval. Never use a different destination. A new question is answered first; a changed preference or revocation supersedes earlier approval. Preserve all callback, information, opt-out, takeover and guarded-ending rules.
Any attempted phone connection consumes this handoff attempt, even if it fails or returns control. Do not repeat transfer_to_number, enter phone_transfer as another attempt, or repeat live_transfer_requested. A failed connection permits only the existing one permission-first callback offer, not an automatic callback or ending. An announcement, approval or tool firing is not proof of connection; require successful provider transfer evidence.
[END_CRISP_ACCEPTED_TRANSFER_V1]`;

function withoutPolicy(value = '') {
  return value.replace(/\s*\[CRISP_ACCEPTED_TRANSFER_V1\][\s\S]*?\[END_CRISP_ACCEPTED_TRANSFER_V1\]/g, '').trim();
}

export function transferProtectedHashes(agent) {
  const config = structuredClone(agent.conversation_config), workflow = structuredClone(agent.workflow);
  delete config.agent.prompt.tools;
  for (const id of TRANSFER_CONTEXTS) {
    const node = workflow.nodes[id];
    delete node.conversation_config?.agent?.prompt?.built_in_tools?.transfer_to_number;
    node.additional_prompt = withoutPolicy(node.additional_prompt);
  }
  return { unchanged: digest({ conversation_config: config, workflow }),
    ...Object.fromEntries(['platform_settings', 'phone_numbers', 'whatsapp_accounts', 'procedures'].map(key => [key, digest(agent[key] ?? null)])) };
}

export function applyAcceptedTransferWorkflow(agent) {
  const next = structuredClone(agent), nodes = next.workflow?.nodes;
  const phone = nodes?.phone_transfer;
  if (next.workflow?.edges?.start_to_main?.target !== 'opening_listener'
    || phone?.type !== 'phone_number' || phone.transfer_type !== 'conference'
    || phone.transfer_destination?.type !== 'phone' || !/^\+[1-9]\d{7,14}$/.test(phone.transfer_destination.phone_number)) {
    throw new Error('Reviewed listen-first and conference destination are required');
  }
  const condition = 'Only after a fresh live_transfer_requested result has BOTH transferApproved true and approvalStatus accepted, clear current caller consent to speak with Yoni now, and no unanswered question, correction, wait request, callback-only or email-only preference, cancellation, opt-out, refusal or human takeover. Never after any previous phone-transfer attempt, including a failure; never from an in-progress, declined, timeout, call_failed, missing or stale result.';
  const tool = { type: 'system', name: 'transfer_to_number', description: condition,
    response_timeout_secs: 20, disable_interruptions: false, interruption_mode: 'allow',
    force_pre_tool_speech: false, pre_tool_speech: 'auto', assignments: [], tool_call_sound: null,
    tool_call_sound_behavior: 'auto', tool_error_handling_mode: 'auto',
    params: { system_tool_type: 'transfer_to_number', enable_client_message: true,
      transfers: [{ custom_sip_headers: [], transfer_destination: structuredClone(phone.transfer_destination),
        transfer_type: 'conference', sip_refer_play_dialtone: true, uui: null, post_dial_digits: null,
        phone_number: phone.transfer_destination.phone_number, condition }] } };
  for (const id of TRANSFER_CONTEXTS) {
    const node = nodes[id];
    if (node?.type !== 'override_agent' || !node.conversation_config?.agent?.prompt?.built_in_tools
      || node.conversation_config.agent.prompt.built_in_tools.end_call !== null) {
      throw new Error('Reviewed guarded conversation context required: ' + id);
    }
    node.conversation_config.agent.prompt.built_in_tools.transfer_to_number = structuredClone(tool);
    node.additional_prompt = [withoutPolicy(node.additional_prompt), ACCEPTED_TRANSFER_POLICY].filter(Boolean).join('\n\n');
  }
  if (digest(transferProtectedHashes(agent)) !== digest(transferProtectedHashes(next))) throw new Error('Transfer release changed protected configuration');
  return next;
}
