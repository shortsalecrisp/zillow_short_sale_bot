import fs from 'node:fs';
import vm from 'node:vm';
import assert from 'node:assert/strict';

const context = {console, Logger: {log() {}}};
vm.createContext(context);
vm.runInContext(
  fs.readFileSync(new URL('../apps_script/sms_chatbot.js', import.meta.url), 'utf8'),
  context,
);

const inbound = [
  'How much is the fee and would you be able to get things expedited?',
  "We don't have offers yet but he's hoping to be able to close by December 31st",
  'to use as a tax write off',
].join(' ');
const decision = context.applyFastRules_(inbound, {});

assert.match(decision.reply_text, /\$5,000/);
assert.match(decision.reply_text, /The lender still controls review timing\./);
assert.match(decision.reply_text, /Where is this file getting held up\?/);
assert.equal(decision.lead_status, 'Y');
assert.equal(decision.handoff_needed, false);
assert.equal(decision.block_reply, false);
assert.equal(decision.reason, 'Answered all bounded supported questions in one message');
assert.equal(context.isSpeedQuestionSignal_('Would you be able to get things expediting?'), true);
assert.equal(context.isSpeedQuestionSignal_('We received an expedited title report.'), false);

console.log('Apps Script expedited compound question regression passed.');
