import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const prompt = await readFile(new URL('../docs/elevenlabs-agent-prompt.md', import.meta.url), 'utf8');
const section = (start, end) => {
  const from = prompt.indexOf(start); const to = end ? prompt.indexOf(end, from) : prompt.length;
  assert.ok(from >= 0 && to > from, 'Expected section boundaries');
  return prompt.slice(from, to);
};
const repairs = section('Clarification answers, before returning', 'Business facts you can use briefly:');

test('clarification distinguishes identity, purpose, property and hearing', () => {
  for (const text of ["an AI assistant with Crisp Short Sales", 'short-sale paperwork and follow up with the lender',
    'The one at {{streetAddress}}.', 'Sorry, can you hear me now?', 'Do not claim you repaired the connection']) {
    assert.ok(repairs.includes(text), text);
  }
  assert.match(repairs, /Then stop/);
  assert.match(repairs, /without a qualification or transfer question/);
});

test('partial recognition preserves understood fragments and never guesses consent', () => {
  assert.match(repairs, /I heard tomorrow\. What time did you say\?/);
  assert.match(repairs, /Never guess a name, email, consent, or callback time/);
  assert.match(repairs, /two short clarification attempts/);
  assert.match(repairs, /An unclear yes to this choice is not consent/);
  assert.match(repairs, /explicit opt-out or wrong-number instruction takes priority/);
});

test('all fee answers retain buyer payment and closing contingency', () => {
  const quotes = prompt.split('\n').filter(line => /^\s*"/.test(line) && /no (?:cost|charge)/i.test(line));
  assert.equal(quotes.length, 4);
  for (const quote of quotes) {
    assert.match(quote, /buyer typically pays a flat fee only if the deal closes/i);
  }
  assert.match(prompt, /It can affect the buyer's total budget/);
  assert.match(prompt, /Do not promise an unchanged offer, lender net, or approval/);
});

test('scope, buyer sourcing and proof answers stay within approved facts', () => {
  assert.match(prompt, /I don't have the exact responsibility split for your file/);
  assert.match(prompt, /not with a buyer offer/);
  assert.match(prompt, /results, credentials, or Equator capabilities not supplied in the approved facts/);
  assert.match(prompt, /Do not present experience as a license or certification/);
  assert.doesNotMatch(prompt, /approval process end to end/);
});

test('question-first policy removes mandatory repeated handoff pitches', () => {
  assert.match(prompt, /answer each briefly in the order asked/);
  assert.match(prompt, /Do not append a Yoni offer to every answer/);
  assert.doesNotMatch(prompt, /then pivot to Yoni|Then pivot back to Yoni|Treat that as re-engagement and offer Yoni once|engage at all, then say/);
  assert.match(prompt, /Do not add that sentence when the selected continuation already explains the service/);
});

test('reviewed conversation contracts retain exact protected section boundaries', () => {
  // The callback repair changes only its own protected range. The separate
  // transfer, opt-out, voicemail and receptionist guards retain their prior bytes.
  const guards = [
    ['Transfer rule:', 'Callback flow:', '0177996bc5a4bd602d0fbb3ade84b0485b5873127be63c527d5db98e6d9347c1'],
    ['Callback flow:', 'Voicemail and no-answer:', 'd61293165ef5324f3ddfb284d44c2c34d3c473146e25ac99b9ee60a906e4a92b'],
    ['Voicemail and no-answer:', null, '7607352d5d23ef01dc8466497c49a1297a0e06a33c4ff502945aff90f8f9afd6'],
    ['If a receptionist, office assistant,', 'Main conversation:', '00a96ef89909b44e2696e0141480adc2f720bffafbf343bb8c023529436ec2a6'],
    ['If a live person says "do not call"', 'If they say they are not worried about it', '7a6069399dc217299ee89829278d7cb260eca1368e9b96b806f934ea98d5acee'],
  ];
  for (const [start, end, expected] of guards) {
    assert.equal(createHash('sha256').update(section(start, end)).digest('hex'), expected, start);
  }
});
