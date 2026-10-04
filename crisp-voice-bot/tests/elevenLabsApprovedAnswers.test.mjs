import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const source = await readFile(new URL('../docs/elevenlabs-agent-prompt.md', import.meta.url), 'utf8');
const prompt = source.split('## Prompt\n')[1].trim();
const from = prompt.indexOf('# Answer library\n');
const to = prompt.indexOf('\n# Contact preferences and endings', from);
assert.ok(from >= 0 && to > from);
const library = prompt.slice(from, to);
const repair = prompt.split('# Listening and repair\n')[1].split('\n# Intro only')[0];
const postIntro = prompt.split('# Post-intro conversation\n')[1].split('\n# Answer library')[0];

// Consolidation changes structure, not approved facts. These static checks
// preserve the answer inventory; they are not provider or human-listening tests.
test('all meaningful approved identity, purpose and property answers survive consolidation', () => {
  const expected = [
    "I'm {{assistantName}}, an AI assistant with Crisp Short Sales.",
    "Yes, I'm an AI assistant with Crisp Short Sales.",
    "I'm with Crisp Short Sales. I work with Yoni Kutler, our short sale specialist.",
    "We help prepare the short-sale paperwork and follow up with the lender.",
    "No, we're a separate short-sale processing service.",
    "The one at {{streetAddress}}.",
    "Yes. We help prepare the short-sale paperwork and follow up with the lender.",
    "We can help with paperwork, lender follow-up, document collection, and title coordination through the short-sale approval process.",
  ];
  for (const text of expected) assert.ok(library.includes(text), text);
  assert.match(library, /Why are you calling \/ what do you want from me: use the specific purpose answer in Listening and repair, then wait/);
  assert.match(library, /AI identity plus purpose[\s\S]*?identify yourself as AI, then answer the purpose\. Answer both points, then wait/);
  assert.ok(repair.includes("We help listing agents with short-sale lender paperwork and calls."));
  assert.ok(repair.includes("I'm calling about the short-sale listing at {{streetAddress}}."));
});

test('the single listing-agent check is answerable while other compound answers defer disposition', () => {
  assert.match(prompt, /A plain "yes" to the single live listing-agent check clearly confirms they are the listing agent/);
  assert.match(prompt, /A yes or no after any other compound property or identity question is ambiguous\./);
  assert.match(prompt, /Do you have the listing at \{\{streetAddress\}\}\?/);
  assert.match(prompt, /If a later caller turn clearly confirms they have the listing, that latest confirmation overrides the earlier ambiguous answer or wrong-listing inference\./);
  assert.match(postIntro, /A clear yes confirms listing ownership only, not interest in help, a transfer or callback/);
  assert.match(postIntro, /Would help with lender paperwork or calls be useful for this listing\?/);
  assert.match(postIntro, /If the caller confirms they have the listing and asks what the call is about, answer only the purpose from the answer library and wait/);
  assert.match(postIntro, /Do not attach the needs question to that answer/);
  assert.doesNotMatch(postIntro, /Are you handling those yourself\?/);
  assert.match(prompt, /Their confirmed listing ownership cancels an earlier ambiguous "no" or wrong-listing inference/);
});

test('explicit no-contact wording has priority over earlier callback or transfer intent', () => {
  assert.match(prompt, /"do not contact me," "do not reach out to me,"/);
  assert.match(prompt, /has priority over every pitch and action, including an earlier callback or transfer request/);
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
  assert.ok(repair.includes("I'm {{assistantName}} with Crisp Short Sales. We help listing agents with short-sale lender paperwork."));
  assert.match(repair, /Answer the specific missing point, then wait; do not append permission, qualification or a handoff offer/);
  assert.match(repair, /If the next turn specifies a missing point, answer only that point/);
  assert.match(repair, /Did you miss who I am, or what we help with\?/);
  assert.match(repair, /A second clarification alone is never permission to say goodbye/);
  assert.match(repair, /For an audio problem say only "Sorry, can you hear me now\?" and wait/);
  assert.match(repair, /Do not claim the connection or volume was fixed/);
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
  assert.match(prompt, /standalone "STOP,?"/);
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
