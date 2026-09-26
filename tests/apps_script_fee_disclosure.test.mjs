import fs from 'node:fs';
import vm from 'node:vm';
import assert from 'node:assert/strict';

const source = process.argv[2] || new URL('../apps_script/sms_chatbot.js', import.meta.url);
const c = {console, Logger: {log() {}}};
vm.createContext(c);
vm.runInContext(fs.readFileSync(source, 'utf8'), c);
const row = {
  mailshake_status: 'Y', auto_reply_count: '1', last_outbound_text: c.buildSpecificFeeReply_(),
  history_json: JSON.stringify([{role: 'assistant', text: c.buildSpecificFeeReply_(), response_id: 'fee_specific'}]),
};
for (const message of [
  'I need to know how you would market that', 'How do we market your fee?',
  'How should I present that to buyers?', 'How do I explain your fee to a buyer?',
  'Where should the fee be disclosed?', 'Should I include your fee in the listing?',
  'How would you advertise the extra cost?', 'How do I market the $5,000?',
]) {
  const result = c.applyFastRules_(message, row);
  assert.equal(result.reply_text, c.buildBuyerFeeDisclosureReply_(), message);
  assert.equal(result.handoff_needed, false, message);
  assert.equal(c.applyRepeatGuard_(result, row, message).reply_text, result.reply_text, message);
}
for (const message of [
  'Can you market my property?', 'How do you market the listing?', 'How do you get paid?',
  'How can you help me?', 'What does your fee include?', 'Can you explain your fee?',
  'Can you market my property for that fee?',
]) {
  assert.equal(c.isBuyerFeeDisclosureQuestion_(message, row.last_outbound_text), false, message);
}
assert.equal(c.isBuyerFeeDisclosureQuestion_('How would you market that?', 'I handle lender paperwork.'), false);
assert.equal(c.applyFastRules_('How should I disclose your fee to buyers?', {}).reply_text, c.buildBuyerFeeDisclosureReply_());
assert.equal(c.applyFastRules_('How do we market your fee? Can we talk tomorrow?', row).handoff_needed, true);
assert.equal(c.applyFastRules_('How should I disclose your fee, and can you discount it?', row).block_reply, true);
console.log('19 Apps Script fee disclosure regressions passed.');
