import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { extractPrompt } from '../src/scripts/syncElevenLabsPromptOnly.mjs';
import { applySharedBusinessPrompt, sharedBusinessPromptProtectedHashes } from '../src/scripts/elevenLabsSharedBusinessPrompt.mjs';
import { applyNumberLoopRepair, LOOP_ROUTE, loopProtectedHashes } from '../src/scripts/elevenLabsNumberLoopRepair.mjs';

test('number readback policy reaches both conversation states without changing protected settings', async () => {
  const prompt = extractPrompt(await readFile(new URL('../docs/elevenlabs-agent-prompt.md', import.meta.url), 'utf8'));
  const old = '# Role and style\nPrevious policy';
  const suffix = '\n\n[CRISP_OPENING_HANDOFF_POLICY]\nExisting safeguards';
  const agent = { conversation_config: { agent: { prompt: { prompt: old } }, tts: { model_id: 'eleven_v4_turbo' } }, workflow: { nodes: { opening_listener: { type: 'override_agent', conversation_config: { agent: { prompt: { prompt: old + suffix } } } } } } };
  const after = applySharedBusinessPrompt(agent, prompt);
  assert.match(prompt, /"three hundred" represents 300/);
  assert.match(prompt, /If all digits match, confirm the number is correct/);
  assert.match(prompt, /the same exact correct number and "is that right" question repeats four times/);
  assert.match(prompt, /A new question, correction, wait request, opt-out or contact request retains priority/);
  assert.equal(after.workflow.nodes.opening_listener.conversation_config.agent.prompt.prompt, prompt + suffix);
  assert.deepEqual(sharedBusinessPromptProtectedHashes(after), sharedBusinessPromptProtectedHashes(agent));
  assert.equal(agent.conversation_config.agent.prompt.prompt, old);
});

test('guarded loop routing changes only the reviewed prompt and six guarded edges', () => {
  const old = '# Role and style\nPrevious policy', marker = '\n\n[CRISP_OPENING_HANDOFF_POLICY]\nGuard';
  const contexts = ['main_conversation','callback_after_unavailable','patch_transfer','opening_listener','short_listing_recovery','callback_receipt_wait'];
  const edges = Object.fromEntries(contexts.map(source => ['terminal_guard_from_' + source, {source,target:'terminal_guard_reset',forward_condition:{type:'llm',condition:'A NEW live caller turn contains a genuine goodbye'}}]));
  const before = {conversation_config:{agent:{prompt:{prompt:old}}},workflow:{nodes:{opening_listener:{type:'override_agent',conversation_config:{agent:{prompt:{prompt:old+marker}}}}},edges}};
  const after = applyNumberLoopRepair(before, '# Role and style\nUpdated policy');
  assert.deepEqual(loopProtectedHashes(after), loopProtectedHashes(before));
  for (const source of contexts) assert.ok(after.workflow.edges['terminal_guard_from_'+source].forward_condition.condition.endsWith(LOOP_ROUTE));
  assert.throws(() => applyNumberLoopRepair(after, '# Role and style\nUpdated policy'));
});
