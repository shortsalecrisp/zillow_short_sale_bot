const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");

function readPrompt() {
  return fs.readFileSync(
    path.resolve(__dirname, "../docs/elevenlabs-agent-prompt.md"),
    "utf8",
  );
}

function extractSection(text, startMarker, endMarker) {
  const start = text.indexOf(startMarker);
  assert.notEqual(start, -1, `Missing prompt marker: ${startMarker}`);

  const end = text.indexOf(endMarker, start);
  assert.notEqual(end, -1, `Missing prompt marker: ${endMarker}`);

  return text.slice(start, end);
}

test("opening generic pickup moves directly to the selected continuation", () => {
  const prompt = readPrompt();
  const openingSection = extractSection(
    prompt,
    "Opening delivery rule, highest priority for every live-human opener:",
    "If the caller corrects the name",
  );

  assert.match(openingSection, /normal greeting such as "hello", "hi", "yeah", "speaking"/i);
  assert.match(openingSection, /Say `{{openerScript}}` immediately/);
  assert.match(openingSection, /Do not ask "Is this {{firstName}}\?"/);
});

test("prompt uses a clear reason-first opening and two dynamic continuation variants", () => {
  const prompt = readPrompt();
  const openingSection = extractSection(
    prompt,
    "Opening delivery rule, highest priority for every live-human opener:",
    "If the caller corrects the name",
  );

  assert.match(openingSection, /The backend first says exactly/);
  assert.match(openingSection, /Hi, this is {{assistantName}} with Crisp Short Sales\. I'm calling about your short sale listing\./);
  assert.match(openingSection, /Do not say "Hello\?" as the opener/);
  assert.match(openingSection, /selected plain-language continuation/);
  assert.match(openingSection, /"{{openerScript}}"/);
  assert.match(openingSection, /two-variant test/);
  assert.match(openingSection, /passes `{{openerVariant}}` for analysis/);
  assert.match(openingSection, /do not deliver `{{openerScript}}`/);
  assert.match(prompt, /If they ask which listing, which property/);
  assert.match(prompt, /answer before any explanation or follow-up question/);
});

test("prompt repairs an unclear opener once without stacking another question", () => {
  const prompt = readPrompt();
  const openingSection = extractSection(
    prompt,
    "Opening delivery rule, highest priority for every live-human opener:",
    "If the caller corrects the name",
  );

  assert.match(openingSection, /who is this\?/i);
  assert.match(openingSection, /This is {{assistantName}} with Crisp Short Sales, calling about your short sale listing\./);
  assert.match(openingSection, /Stop after the repair line/);
  assert.match(openingSection, /no more than once/);
  assert.match(openingSection, /never repeat the introduction a third time/);
});

test("prompt waits for voicemail greeting handoff before leaving voicemail", () => {
  const prompt = readPrompt();
  const marker = "Voicemail and no-answer:";
  const start = prompt.indexOf(marker);
  assert.notEqual(start, -1, `Missing prompt marker: ${marker}`);
  const voicemailSection = prompt.slice(start);

  assert.match(voicemailSection, /do not say `{{openerScript}}` first/);
  assert.match(voicemailSection, /do not call `voicemail_detection` while the mailbox greeting is still mid-sentence/);
  assert.match(voicemailSection, /clear pause or the greeting has already asked for a message/);
});

test("prompt ends wrong-person and unrelated-business voicemail without a sales pitch", () => {
  const prompt = readPrompt();
  const start = prompt.indexOf("Wrong-person or unrelated-business voicemail hard stop:");
  const end = prompt.indexOf("Main conversation:", start);
  const section = prompt.slice(start, end);

  assert.ok(start >= 0);
  assert.match(section, /sounds plausibly similar/);
  assert.match(section, /clearly nothing alike/);
  assert.match(section, /do not leave the normal voicemail/);
  assert.match(section, /business greeting is target-matching/);
  assert.match(section, /`\{\{lastName\}\}`/);
  assert.match(section, /Say nothing further and immediately call `end_call`/);
  assert.match(section, /Never request a callback, live transfer, or human sales handoff/);
});

test("prompt starts with identity plus reason and avoids repeated introductions", () => {
  const prompt = readPrompt();
  const oneIntroRule = extractSection(
    prompt,
    "Opening delivery rule, highest priority for every live-human opener:",
    "If the caller corrects the name",
  );

  assert.match(oneIntroRule, /caller name, company, and reason/);
  assert.match(oneIntroRule, /Hi, this is {{assistantName}} with Crisp Short Sales/);
  assert.match(oneIntroRule, /short sale listing/);
  assert.match(oneIntroRule, /Do not repeat your name, Crisp Short Sales, or the listing reason/);
  assert.match(oneIntroRule, /no more than once/);
});

test("prompt treats not-a-short-sale objections as a clear no", () => {
  const prompt = readPrompt();
  const notShortSaleBranch = extractSection(
    prompt,
    "If they say the listing is not a short sale",
    "If they say they are not worried about it",
  );

  assert.match(notShortSaleBranch, /clean closeout/i);
  assert.match(notShortSaleBranch, /Do not pitch/);
  assert.match(
    notShortSaleBranch,
    /Ahh, ok, thanks for letting me know\. Good luck with your listing!/,
  );
  assert.match(notShortSaleBranch, /Then call `not_interested`/);
  assert.match(notShortSaleBranch, /conversationSummary[\s\S]{0,120}not a short sale/);
  assert.match(notShortSaleBranch, /`not_short_sale`/);
});

test("prompt turns human-only objections into immediate Yoni transfer rescue", () => {
  const prompt = readPrompt();
  const humanRescueBranch = extractSection(
    prompt,
    "If they object to automation",
    "If they ask whether you are with another person",
  );

  assert.match(
    humanRescueBranch,
    /Totally fair\. Yoni is our live short sale specialist/,
  );
  assert.match(humanRescueBranch, /bring him onto this call right now/);
  assert.match(humanRescueBranch, /Want me to try him\?/);
  assert.match(humanRescueBranch, /move directly into the live transfer flow/);
  assert.doesNotMatch(humanRescueBranch, /callback_requested/);
});

test("prompt answers AI questions truthfully and offers immediate Yoni transfer", () => {
  const prompt = readPrompt();
  const aiBranch = extractSection(
    prompt,
    "If they ask whether you are AI:",
    "If they object to automation",
  );

  assert.match(aiBranch, /Yes, I'm an AI calling assistant\./);
  assert.match(aiBranch, /Yoni is our live short sale specialist/);
  assert.match(aiBranch, /bring him onto this call right now/);
  assert.match(aiBranch, /Want me to try him\?/);
});

test("prompt redirects unknown affiliation questions to a plain-language handling question", () => {
  const prompt = readPrompt();

  assert.match(
    prompt,
    /I'm with Crisp Short Sales\. We help agents with short sale paperwork and lender calls\. Are you handling that work yourself\?/,
  );
});

test("prompt treats placeholder-only user turns as background noise and skips speaking", () => {
  const prompt = readPrompt();

  assert.match(prompt, /If the latest caller message is exactly "\.\.\."/);
  assert.match(prompt, /background noise/i);
  assert.match(prompt, /call `skip_turn`/);
  assert.match(prompt, /Do not say[\s\S]{0,120}Are you still there\?/);
});

test("prompt stops speaking when the caller interrupts", () => {
  const prompt = readPrompt();
  const interruptionSection = extractSection(
    prompt,
    "If the caller interrupts:",
    "If the caller's speech sounds like background conversation",
  );

  assert.match(interruptionSection, /Stop speaking and listen/);
  assert.match(interruptionSection, /Do not finish the sentence over them/);
  assert.match(interruptionSection, /Respond to the latest thing they said/);
  assert.doesNotMatch(interruptionSection, /still kind of new/i);
});

test("prompt keeps third-party callback timing questions direct and name-specific", () => {
  const prompt = readPrompt();
  const callbackFlow = extractSection(
    prompt,
    "Callback flow:",
    "If they say no, all set, thanks, bye, ok, or similar, say:",
  );

  assert.match(callbackFlow, /What time should Yoni call \[name\]\?/);
  assert.match(callbackFlow, /Do not say:[\s\S]*Great, what time should Yoni call her\?/);
  assert.match(prompt, /use plain human phrasing and the real name when you know it/i);
});

test("prompt waits through office robots and gatekeeper transfer attempts", () => {
  const prompt = readPrompt();
  const gatekeeperSection = extractSection(
    prompt,
    "If a receptionist, office assistant, automated attendant, answering service, phone tree, or transfer robot answers:",
    "If it is the wrong person",
  );

  assert.match(gatekeeperSection, /automated attendant/i);
  assert.match(gatekeeperSection, /AI call assistant/i);
  assert.match(gatekeeperSection, /record your name and reason for calling/i);
  assert.match(
    gatekeeperSection,
    /This is {{assistantName}} calling from Crisp Short Sales about your listing at {{streetAddress}}\./,
  );
  assert.match(gatekeeperSection, /Do not call `skip_turn` as the response to that screener prompt/);
  assert.match(gatekeeperSection, /The response to the screener must be the spoken sentence above/);
  assert.match(gatekeeperSection, /After you have spoken that sentence, stay quiet/);
  assert.match(gatekeeperSection, /Do not pitch, do not ask a callback question/);
  assert.match(gatekeeperSection, /phone rings, transfers, or waits for the agent/);
  assert.match(gatekeeperSection, /to see if they wanted help with the bank paperwork and approval side/i);
  assert.match(gatekeeperSection, /Do not give Yoni's callback number in that sentence/i);
  assert.match(gatekeeperSection, /Yoni's direct callback number is 404-300-9526/i);
  assert.doesNotMatch(gatekeeperSection, /What's the best time or direct number for {{firstName}}\?/i);
  assert.match(gatekeeperSection, /Please stay on the line/i);
  assert.match(gatekeeperSection, /Sure, I'll wait\./);
  assert.match(gatekeeperSection, /follow the Voicemail\/no-answer rules and leave the full voicemail on attempt 1/i);
  assert.match(gatekeeperSection, /restart the normal live-human opener from scratch/i);
  assert.match(gatekeeperSection, /This is {{assistantName}} with Crisp Short Sales\./);
  assert.match(gatekeeperSection, /Do not call `end_call`[\s\S]{0,160}transferred/i);
});

test("prompt blocks recording fragments from creating human outcomes", () => {
  const prompt = readPrompt();

  assert.match(prompt, /Recording\/automated-system gate, highest priority/i);
  assert.match(prompt, /Canned fragments such as "as soon as possible"/i);
  assert.match(
    prompt,
    /Never call `callback_requested`, `information_requested`, `not_interested`, or `live_transfer_requested`/,
  );
  assert.match(prompt, /automated system asks for a callback number[\s\S]{0,100}404-300-9526/i);
});

test("prompt gives explicit do-not-call requests a non-sales closeout", () => {
  const prompt = readPrompt();
  const branch = extractSection(
    prompt,
    'If a live person says "do not call"',
    "If they say they are not worried about it",
  );

  assert.match(branch, /priority over every pitch/i);
  assert.match(branch, /DO NOT CALL: caller explicitly requested no further calls/);
  assert.match(branch, /Understood\. We won't call again\. Goodbye\./);
  assert.match(branch, /Do not pitch, mention future help, ask another question, offer Yoni/);
});

test("prompt records self-initiated future contact as deferred without a callback", () => {
  const prompt = readPrompt();
  const section = extractSection(
    prompt,
    "Self-initiated future contact, before generic not-interested or callback handling:",
    "If the caller interrupts:",
  );

  assert.match(section, /I'm gonna get back to you as soon as I can/);
  assert.match(section, /DEFERRED CONTACT: caller said they will initiate future contact/);
  assert.match(section, /backend records `deferred_contact`, not a rejection/);
  assert.match(section, /do not call `callback_requested`/);
  assert.match(section, /do not create a handoff/);
  assert.match(section, /call me later/);
});

test("prompt pitches admins who answer instead of only taking a message", () => {
  const prompt = readPrompt();
  const receptionistBranch = extractSection(
    prompt,
    "If a receptionist, office assistant",
    "If it is the wrong person",
  );

  assert.match(receptionistBranch, /admin or assistant says {{firstName}} is not available/i);
  assert.match(receptionistBranch, /treat them as a valid person to pitch/i);
  assert.match(receptionistBranch, /Do not only ask them to relay a message/i);
  assert.match(
    receptionistBranch,
    /No problem\. We help agents with short sale paperwork, lender calls, and approval\. Do you know whether they're handling that work themselves\?/,
  );
});

test("prompt treats not-worried responses as a soft no instead of pitching", () => {
  const prompt = readPrompt();

  assert.match(prompt, /not worried/i);
  assert.match(prompt, /soft no/i);
  assert.match(
    prompt,
    /Ok, well thanks for letting me know\. If anything changes in the future and you're looking for some additional help, please just keep me in mind\. Thanks!/,
  );
  assert.match(prompt, /Then pause briefly and listen/);
  assert.match(prompt, /answer it instead of calling `not_interested`/);
});

test("prompt soft-closes when the caller already has short sale help but still answers questions", () => {
  const prompt = readPrompt();
  const coveredBranch = extractSection(
    prompt,
    "If they say they already have a short sale negotiator",
    "If they say they are not worried about it",
  );

  assert.match(coveredBranch, /attorney, specialist, someone handling it/);
  assert.match(coveredBranch, /Do not pitch/);
  assert.match(coveredBranch, /please just keep me in mind\. Thanks!/);
  assert.match(coveredBranch, /If they ask any question after this/i);
  assert.match(coveredBranch, /answer it instead of calling `not_interested`/i);
  assert.match(coveredBranch, /If they do not ask a question/i);
});

test("prompt treats direct or self-handling answers as a soft value-pitch opportunity", () => {
  const prompt = readPrompt();
  const selfHandlingBranch = extractSection(
    prompt,
    "If they answer the handling question with \"yes\"",
    "If they say they already have a short sale negotiator",
  );

  assert.match(selfHandlingBranch, /handling it themselves/i);
  assert.match(selfHandlingBranch, /figuring it out as I go/i);
  assert.match(selfHandlingBranch, /plain yes/i);
  assert.match(selfHandlingBranch, /Do not repeat the handling question/);
  assert.match(selfHandlingBranch, /acknowledge that first/i);
  assert.match(selfHandlingBranch, /Do not treat this as a hard no/);
  assert.match(selfHandlingBranch, /short sale paperwork and lender calls/);
  assert.match(selfHandlingBranch, /no cost to you or the seller/);
  assert.match(selfHandlingBranch, /Are you looking for help with this one\?/);
  assert.match(selfHandlingBranch, /Interest-to-Yoni sequence/);
  assert.match(selfHandlingBranch, /bring Yoni, our live short sale specialist, onto this call right now/);
  assert.match(selfHandlingBranch, /Do not launch a live transfer only because they answered the earlier handling question/);
  assert.match(selfHandlingBranch, /call `information_requested`/);
  assert.match(selfHandlingBranch, /Do not call `callback_requested`/);
  assert.match(selfHandlingBranch, /do not invent a callback time/);
  assert.match(selfHandlingBranch, /Is \{\{email\}\} the best email for the information\?/);
  assert.match(selfHandlingBranch, /What time should he call you\?/);
});

test("prompt exposes email and keeps information requests out of callback handling", () => {
  const prompt = readPrompt();

  assert.match(prompt, /- `email`/);
  assert.match(prompt, /capture a request for information/);
  assert.match(prompt, /After `information_requested` succeeds/);
  assert.doesNotMatch(prompt, /callbackTime` set to `send info/);
});

test("prompt keeps self-handling uncertainty out of the hard-no examples", () => {
  const prompt = readPrompt();
  const notInterestedExamples = extractSection(
    prompt,
    "Treat all of these as not interested:",
    "Say:",
  );

  assert.doesNotMatch(notInterestedExamples, /- "I'm handling it myself"/);
  assert.match(notInterestedExamples, /Do not include "I'm handling it myself"/);
  assert.match(notInterestedExamples, /"I'm figuring it out as I go"/);
});

test("prompt answers service questions after a soft-no closeout instead of ending", () => {
  const prompt = readPrompt();
  const softNoBranch = extractSection(
    prompt,
    "If they say they are not worried about it",
    "If they ask whether you handle the full short sale process",
  );

  assert.match(softNoBranch, /If they ask any question after this/i);
  assert.match(softNoBranch, /how much do you charge/i);
  assert.match(softNoBranch, /answer it instead of calling `not_interested`/i);
  assert.match(softNoBranch, /no cost to the agent or seller/i);
  assert.match(softNoBranch, /buyer pays a flat fee only if the deal closes/i);
  assert.match(softNoBranch, /treat that as re-engagement/i);
});

test("prompt does not treat overlapped okay or busy later/callback language as live-transfer consent", () => {
  const prompt = readPrompt();
  const transferRule = extractSection(
    prompt,
    "Transfer rule:",
    "If they want Yoni now, or say",
  );

  assert.match(transferRule, /clearly and unambiguously agrees/);
  assert.match(transferRule, /explicit offer to bring Yoni onto this call right now/);
  assert.match(transferRule, /earlier qualification or help question is interest only/i);
  assert.match(transferRule, /Do not treat a vague or overlapped "okay okay"/);
  assert.match(transferRule, /"I, so\.\.\. okay"/);
  assert.match(transferRule, /in a meeting/);
  assert.match(transferRule, /afternoon\/tomorrow\/later/);
  assert.match(transferRule, /Sorry, I may have talked over you/);
  assert.match(transferRule, /Do you want me to try to bring Yoni onto this call now, or should he call you at a specific time\?/);
  assert.match(transferRule, /No problem\. What time should he call you\?/);
});

test("prompt does not force another identity check after a normal live pickup", () => {
  const prompt = readPrompt();
  const openingSection = extractSection(
    prompt,
    "Opening delivery rule, highest priority for every live-human opener:",
    "If the caller corrects the name",
  );

  assert.match(openingSection, /gives their name/);
  assert.match(openingSection, /do not ask for their identity again/i);
  assert.match(openingSection, /Say `{{openerScript}}` immediately/);
});

test("prompt treats corrected realtor identity as the active agent", () => {
  const prompt = readPrompt();
  const correctedIdentityBranch = extractSection(
    prompt,
    "If the caller corrects the name",
    "If they ask which listing",
  );

  assert.match(correctedIdentityBranch, /I'?m the realtor/i);
  assert.match(correctedIdentityBranch, /treat the current speaker as the agent/i);
  assert.match(correctedIdentityBranch, /Do not ask to speak with `{{firstName}}`/);
  assert.match(correctedIdentityBranch, /do not route back to the original lead name/i);
  assert.match(
    correctedIdentityBranch,
    /Got it\. Are you handling the short sale paperwork and lender calls yourself\?/,
  );
});

test("prompt clarifies noisy background speech before treating it as consent", () => {
  const prompt = readPrompt();
  const noisySpeechBranch = extractSection(
    prompt,
    "If the caller's speech sounds like background conversation",
    "If they sound skeptical",
  );

  assert.match(noisySpeechBranch, /hair|unrelated personal conversation/i);
  assert.match(noisySpeechBranch, /Do not treat a single yes, sure, or okay inside that noisy turn as consent/i);
  assert.match(
    noisySpeechBranch,
    /Sorry, I may be catching background conversation\. Just to confirm, do you want Yoni to call you about the short sale\?/,
  );
});

test("prompt reserves the property address for an explicit listing question", () => {
  const prompt = readPrompt();
  const openingSection = extractSection(
    prompt,
    "Opening delivery rule, highest priority for every live-human opener:",
    "If the caller corrects the name",
  );
  const propertyBranch = extractSection(prompt, "If they ask which listing", "If the caller says they are busy");

  assert.match(openingSection, /do not lead with the property address/i);
  assert.doesNotMatch(openingSection, /short sale listing at {{streetAddress}}/);
  assert.match(propertyBranch, /The one at {{streetAddress}}\./);
  assert.match(propertyBranch, /Then stop and let them respond/);
});

test("prompt removes bank-side jargon from the live-human pitch", () => {
  const prompt = readPrompt();

  assert.match(prompt, /short sale paperwork and lender calls/);
  assert.doesNotMatch(prompt, /bank side/i);
});

test("prompt repairs confusion with the offer before mentioning Yoni or prior text", () => {
  const prompt = readPrompt();
  const confusionBranch = extractSection(
    prompt,
    "If they say they are not really sure what you are calling about",
    "Business facts you can use briefly:",
  );

  assert.match(confusionBranch, /Do not lead with Yoni/);
  assert.match(confusionBranch, /Do not mention the earlier text yet/);
  assert.match(
    confusionBranch,
    /Sorry if I wasn't clear\. Crisp Short Sales can handle the short sale paperwork and lender calls for you\. Are you looking for help with that\?/,
  );
  assert.doesNotMatch(confusionBranch, /reached out earlier by text/);
  assert.doesNotMatch(confusionBranch, /what your plan/i);
});

test("prompt clearly explains purpose before callback when caller is busy or cannot hear", () => {
  const prompt = readPrompt();
  const busyNoiseBranch = extractSection(
    prompt,
    "If the caller says they are busy, out to dinner, driving, cannot hear you well",
    "If a receptionist, office assistant",
  );

  assert.match(busyNoiseBranch, /Do not ask for a callback before explaining why you called/);
  assert.match(busyNoiseBranch, /Do not only say that Yoni can explain it better/);
  assert.match(
    busyNoiseBranch,
    /No worries, I'll be quick\. We help with the paperwork and lender calls on your short sale\. Should Yoni call you at a better time\?/,
  );
  assert.doesNotMatch(busyNoiseBranch, /I'm {{assistantName}} with Crisp Short Sales/);
  assert.match(busyNoiseBranch, /paperwork and lender calls/);
  assert.match(busyNoiseBranch, /call `callback_requested`/);
});

test("prompt uses the per-call assistant name instead of hard-coding Emmy in spoken lines", () => {
  const prompt = readPrompt();

  assert.match(prompt, /You are {{assistantName}}, a warm/);
  assert.match(prompt, /this is {{assistantName}} with Crisp Short Sales/);
  assert.doesNotMatch(prompt, /this is Emmy with Crisp Short Sales/i);
  assert.doesNotMatch(prompt, /I'm Emmy with Crisp Short Sales/i);
  assert.doesNotMatch(prompt, /let .* know Emmy from Crisp Short Sales/i);
});
