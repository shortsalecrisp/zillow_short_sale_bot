import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const source = await readFile(new URL('../docs/elevenlabs-agent-prompt.md', import.meta.url), 'utf8');
const prompt = source.split('## Prompt\n')[1].trim();
const from = prompt.indexOf('# Answer library\n');
const to = prompt.indexOf('\n# Contact preferences and endings', from);
assert.ok(from >= 0 && to > from);
const library = prompt.slice(from, to);

// Consolidation changes structure, not approved facts. These static checks
// preserve the answer inventory; they are not provider or human-listening tests.
test('all meaningful approved identity, purpose and property answers survive consolidation', () => {
  const expected = [
    "I'm {{assistantName}}, an AI assistant with Crisp Short Sales.",
    "Yes, I'm an AI assistant with Crisp Short Sales.",
    "I'm with Crisp Short Sales. I work with Yoni Kutler, our short sale specialist.",
    "We help prepare the short-sale paperwork and follow up with the lender.",
    "I'm asking whether help with the short-sale paperwork and lender calls would be useful on your listing.",
    "Sorry. This is {{assistantName}} with Crisp Short Sales. We help with short-sale paperwork and lender calls on your listing.",
    "Yes, I'm an AI assistant with Crisp Short Sales. I'm calling to see whether help with the short-sale paperwork and lender calls would be useful on your listing.",
    "The one at {{streetAddress}}.",
    "Yes. We help prepare the short-sale paperwork and follow up with the lender.",
    "We can help with paperwork, lender follow-up, document collection, and title coordination through the short-sale approval process.",
  ];
  for (const text of expected) assert.ok(library.includes(text), text);
});
test('fee answers preserve the payer, closing contingency and unknown economics', () => {
  for (const text of [
    "There is no charge to you or the seller. The buyer typically pays a flat fee only if the deal closes.",
    "The buyer typically pays the flat fee, only if the deal closes.",
    "I don't have the applicable fee amount for your file. Yoni can explain the terms before you decide.",
    "It can affect the buyer's total budget. Yoni can explain the fee and offer structure before you decide.",
  ]) assert.ok(library.includes(text), text);
  assert.match(library, /Never describe the service simply as free/);
  assert.match(library, /Do not promise an unchanged offer, lender net, commission, or approval/);
  assert.doesNotMatch(library, /\$[\d,]+/);
});
test('responsibility, experience, proof, buyer sourcing, seller and location answers remain bounded', () => {
  for (const text of [
    "I don't have the exact responsibility split for your file. Yoni can go through that with you.",
    "I understand why you'd want to check the scope and terms first. What would you need to see?",
    "Yoni Kutler has worked on short sales for more than fifteen years.",
    "I don't have a verified answer on that. Yoni can confirm what he can handle for your file.",
    "I'm calling about short-sale processing help, not with a buyer offer.",
    "What would help you explain it to your seller?",
    "We're based in Atlanta, but we work all across the US.",
    "He's our short sale specialist here at Crisp. He's been doing this for over fifteen years.",
  ]) assert.ok(library.includes(text), text);
  assert.match(library, /Experience is not a license or certification/);
  assert.match(library, /Do not invent duties or promise no work remains/);
  assert.match(library, /do not invent documents, references or success rates/);
  assert.match(library, /Do not infer seller consent or a callback/);
  assert.match(library, /Do not imply an existing relationship/);
});
test('general scope and timing keep every approved material fact without a file guarantee', () => {
  for (const fact of ['paperwork', 'bank calls', 'title coordination', 'buyer and seller document collection',
    'liens', 'mortgages', 'backend approval process']) assert.ok(library.includes(fact), fact);
  assert.match(library, /60 to 90 days after a full package is submitted/);
  assert.match(library, /this is not a guarantee for their file/);
  assert.match(library, /not guarantees or an exact division of duties for a specific file/);
  assert.match(library, /Do not invent a fee amount, guaranteed approval or closing/);
  assert.doesNotMatch(library, /approval process end to end/);
});
test('answer and repair contracts retain complete questions without repeated handoff pitches', () => {
  assert.match(library, /A clarification answer is a complete turn/);
  assert.match(library, /Answer all questions asked, then wait/);
  assert.match(library, /Do not attach a qualification, anything-else question, or transfer pitch/);
  assert.match(prompt, /We organize the documents the bank needs and follow up on its review/);
  assert.match(prompt, /Sorry, can you hear me now\?/);
  assert.match(prompt, /not automatically the service or current conversation|not necessarily the service or current conversation/);
  assert.doesNotMatch(prompt, /then pivot to Yoni|Then pivot back to Yoni|Treat that as re-engagement and offer Yoni once/);
});
test('explicit opt-out and current-call ending markers remain verbatim', () => {
  for (const marker of [
    'DO NOT CALL: caller explicitly requested no further calls.',
    'CALL ENDED BY REQUEST: caller asked to end the current call only.',
    'DEFERRED CONTACT: caller said they will initiate future contact.',
  ]) assert.ok(prompt.includes(marker), marker);
  assert.match(prompt, /remove me from your list/);
  assert.match(prompt, /standalone "STOP"/);
  assert.match(prompt, /A declined transfer, time, channel, or appointment is not a rejection of all service/);
  assert.match(prompt, /I don't have a verified reason for that label. Thanks for correcting it/);
});
test('attempt-one voicemail wording is exactly preserved and not reused on attempt two', () => {
  const expected = "Hi, this is {{assistantName}} with Crisp Short Sales calling about the short sale listing at {{streetAddress}}. We specialize in helping agents with the short sale process and can handle the paperwork, phone calls, and the whole process with the lender to take that work off your shoulders. Yoni is our short sale specialist, and he can answer any questions you have. Give him a call back at 404-300-9526 when you get a chance. Thanks.";
  const lines = prompt.split('\n').filter(line => line.startsWith('"Hi, this is {{assistantName}} with Crisp Short Sales calling about'));
  assert.deepEqual(lines, ['"' + expected + '"']);
  assert.match(prompt, /Attempt 2: leave no second voicemail/);
  assert.match(prompt, /Wrong-person voicemail protection applies only to a recording, not a live admin/);
});
