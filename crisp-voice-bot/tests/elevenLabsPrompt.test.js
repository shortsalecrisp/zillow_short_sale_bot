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

test("opening generic pickup fallback moves forward instead of repeating quick-second", () => {
  const prompt = readPrompt();
  const fallbackConfirmation = extractSection(
    prompt,
    "- If they give any clear yes-type answer after that fallback line, continue immediately with:",
    "- Do not ask for {{firstName}} a third time.",
  );

  const spokenLines = fallbackConfirmation
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line.startsWith('"'));

  assert.deepEqual(spokenLines, [
    '"Thanks. I was calling about your short sale listing. Are you handling the bank side yourself?"',
  ]);
  assert.doesNotMatch(spokenLines.join("\n"), /{{streetAddress}}/);
  assert.doesNotMatch(spokenLines.join("\n"), /Crisp Short Sales/);
  assert.doesNotMatch(spokenLines.join("\n"), /Got a quick second/);
});

test("prompt uses dynamic opener scripts and keeps the address out of the first opener", () => {
  const prompt = readPrompt();
  const openingSection = extractSection(
    prompt,
    "Opening:",
    "If the caller corrects the name",
  );
  const openerBranch = extractSection(
    prompt,
    "- If the caller confirms identity after the opener, your very next line must be:",
    "- Do not ask \"Hey, is this {{firstName}}?\" twice after a clear identity confirmation.",
  );
  const genericFallbackBranch = extractSection(
    prompt,
    "- Instead say once:",
    "- If they give any clear yes-type answer after that, do not repeat",
  );

  assert.match(openingSection, /The backend first says a short pickup probe/);
  assert.match(openingSection, /"Hello\?"/);
  assert.match(openingSection, /That pickup probe is only to avoid dumping the full opener before the line is ready/);
  assert.match(openingSection, /After the first real live-human response, deliver the selected opener/);
  assert.match(prompt, /"{{openerScript}}"/);
  assert.match(prompt, /The backend chooses `{{openerScript}}` for the opener test/);
  assert.match(prompt, /passes `{{openerVariant}}` for analysis/);
  assert.match(prompt, /Do not add another long pause after the pickup probe/);
  assert.match(prompt, /Do not say `{{streetAddress}}` in the first line/);
  assert.match(openingSection, /do not deliver `{{openerScript}}`/);
  assert.match(
    openerBranch,
    /Thanks\. I was calling about your short sale listing\. Are you handling the bank side yourself\?/,
  );
  assert.doesNotMatch(openerBranch, /this is {{assistantName}} with Crisp Short Sales/);
  assert.doesNotMatch(openerBranch, /short sale listing at {{streetAddress}}/);
  assert.match(
    genericFallbackBranch,
    /Sorry, is this {{firstName}}\?/,
  );
  assert.doesNotMatch(genericFallbackBranch, /this is {{assistantName}} with Crisp Short Sales/);
  assert.doesNotMatch(genericFallbackBranch, /{{streetAddress}}/);
  assert.match(prompt, /If they ask which listing, which property, or what address/);
}
);

test("prompt repairs pickup-probe confusion with a clean identity before the pitch", () => {
  const prompt = readPrompt();
  const openingSection = extractSection(
    prompt,
    "Opening:",
    "If the caller corrects the name",
  );

  assert.match(openingSection, /Hello\? Hello\? What\?/);
  assert.match(openingSection, /do not repeat the full opener over them/);
  assert.match(openingSection, /This is {{assistantName}} with Crisp Short Sales\./);
  assert.match(openingSection, /Do not put the pitch in the same repair turn/);
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
  assert.match(section, /recorded greeting explicitly names someone other than/);
  assert.match(section, /do not leave the normal voicemail/);
  assert.match(section, /Say nothing further and immediately call `end_call`/);
  assert.match(section, /Never request a callback, live transfer, or human sales handoff/);
});

test("prompt starts with a complete identity and allows one identity repair before the pitch", () => {
  const prompt = readPrompt();
  const oneIntroRule = extractSection(
    prompt,
    "Identity-first delivery rule, highest priority for every live-human opener:",
    "Opening:",
  );

  assert.match(oneIntroRule, /first complete phrase of every live-human opener must be exactly/i);
  assert.match(oneIntroRule, /This is {{assistantName}} with Crisp Short Sales\./);
  assert.match(oneIntroRule, /Do not put "Hi", "Hey", the caller's name/);
  assert.match(oneIntroRule, /clipped, incomplete, garbled, talked over/);
  assert.match(oneIntroRule, /repeat exactly once before any pitch/i);
  assert.match(oneIntroRule, /After the identity has been delivered clearly, do not repeat/);
  assert.match(oneIntroRule, /a new person comes onto the call/i);
  assert.match(oneIntroRule, /voicemail/i);
  assert.match(oneIntroRule, /gatekeeper/i);
  assert.match(oneIntroRule, /no more than once as an audio repair/i);
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
    /Totally fair\. I am an AI calling assistant for Crisp Short Sales\./,
  );
  assert.match(humanRescueBranch, /Yoni is our live short sale specialist\./);
  assert.match(humanRescueBranch, /I can get him on the phone right now\./);
  assert.match(humanRescueBranch, /Would you like me to bring him in to the call\?/);
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

  assert.match(aiBranch, /Yes, I am an AI calling assistant for Crisp Short Sales\./);
  assert.match(aiBranch, /Yoni is our live short sale specialist\./);
  assert.match(aiBranch, /He can answer any questions you have/);
  assert.match(aiBranch, /I can get him on the phone right now/);
  assert.match(aiBranch, /Would you like me to bring him in to the call\?/);
});

test("prompt redirects unknown affiliation questions back to the bank-side help offer", () => {
  const prompt = readPrompt();

  assert.match(
    prompt,
    /I'm with Crisp Short Sales, working with Yoni Kutler, our short sale specialist\. We help agents with short sale bank paperwork and lender calls\. Are you handling the bank side yourself\?/,
  );
});

test("prompt treats placeholder-only user turns as background noise and skips speaking", () => {
  const prompt = readPrompt();

  assert.match(prompt, /If the latest caller message is exactly "\.\.\."/);
  assert.match(prompt, /background noise/i);
  assert.match(prompt, /call `skip_turn`/);
  assert.match(prompt, /Do not say[\s\S]{0,120}Are you still there\?/);
});

test("prompt confirms identity before repeating the pitch when caller is confused", () => {
  const prompt = readPrompt();
  const confusedRepair = extractSection(
    prompt,
    "- If the caller sounds confused right after the opener",
    "- If the first response after the opener is clipped",
  );

  assert.match(confusedRepair, /what\?/i);
  assert.match(
    confusedRepair,
    /This is {{assistantName}} with Crisp Short Sales\./,
  );
  assert.match(
    confusedRepair,
    /Thanks\. I was calling about your short sale listing\. Are you handling the bank side yourself\?/,
  );
  assert.match(confusedRepair, /identity repair turn/);
  assert.match(confusedRepair, /{{streetAddress}}/);
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

  assert.match(prompt, /automated attendant/i);
  assert.match(prompt, /phone tree/i);
  assert.match(prompt, /record your name and reason for calling/i);
  assert.match(prompt, /what's the best time for Yoni to call back/i);
  assert.match(prompt, /Please stay on the line/i);
  assert.match(prompt, /Sure, I'll wait\./);
  assert.match(prompt, /Do not call `end_call`[\s\S]{0,160}transferred/i);
});

test("prompt blocks recording fragments from creating human outcomes", () => {
  const prompt = readPrompt();

  assert.match(prompt, /Recording\/automated-system gate, highest priority/i);
  assert.match(prompt, /Canned fragments such as "as soon as possible"/i);
  assert.match(prompt, /Never call `callback_requested`, `not_interested`, or `live_transfer_requested`/);
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
    /No problem\. We help agents with short sale bank paperwork, lender calls, and approval\. I was calling to see if {{firstName}} wanted help with that\. Do you know if {{firstName}} is handling the bank side personally\?/,
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
    "If they answer the plan question with \"yes\"",
    "If they say they already have a short sale negotiator",
  );

  assert.match(selfHandlingBranch, /handling it themselves/i);
  assert.match(selfHandlingBranch, /figuring it out as I go/i);
  assert.match(selfHandlingBranch, /plain yes/i);
  assert.match(selfHandlingBranch, /Do not repeat the bank-side question/);
  assert.match(selfHandlingBranch, /acknowledge that first/i);
  assert.match(selfHandlingBranch, /Do not treat this as a hard no/);
  assert.match(selfHandlingBranch, /lender paperwork and follow-up/);
  assert.match(selfHandlingBranch, /no cost to you or the seller/);
  assert.match(selfHandlingBranch, /Worth a quick call with Yoni/);
  assert.match(selfHandlingBranch, /do not start a transfer yet/i);
  assert.match(selfHandlingBranch, /Do you want me to try to bring Yoni onto this call now, or should he call you at a specific time\?/);
  assert.match(selfHandlingBranch, /interest only/i);
  assert.match(selfHandlingBranch, /What time should he call you\?/);
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
  assert.match(transferRule, /Worth a quick call with Yoni/);
  assert.match(transferRule, /not a clear live-transfer yes/i);
  assert.match(transferRule, /Do not treat a vague or overlapped "okay okay"/);
  assert.match(transferRule, /"I, so\.\.\. okay"/);
  assert.match(transferRule, /in a meeting/);
  assert.match(transferRule, /afternoon\/tomorrow\/later/);
  assert.match(transferRule, /Sorry, I may have talked over you/);
  assert.match(transferRule, /Do you want me to try to bring Yoni onto this call now, or should he call you at a specific time\?/);
  assert.match(transferRule, /No problem\. What time should he call you\?/);
});

test("prompt treats partial this-is identity replies as confirmed", () => {
  const prompt = readPrompt();

  assert.match(prompt, /partial identity/i);
  assert.match(prompt, /"this is"/i);
  assert.match(prompt, /first recognized human response/i);
  assert.match(prompt, /Do not wait for the caller to repeat/i);
});

test("prompt treats corrected realtor identity as the active agent", () => {
  const prompt = readPrompt();
  const correctedIdentityBranch = extractSection(
    prompt,
    "If the caller corrects the name",
    "If they confirm they are `{{firstName}}` after the opener",
  );

  assert.match(correctedIdentityBranch, /I'?m the realtor/i);
  assert.match(correctedIdentityBranch, /treat the current speaker as the agent/i);
  assert.match(correctedIdentityBranch, /Do not ask to speak with {{firstName}}/);
  assert.match(correctedIdentityBranch, /do not ask whether {{firstName}} is handling the bank side/i);
  assert.match(
    correctedIdentityBranch,
    /Got it\. We help agents with short sale bank paperwork, lender calls, and approval\. Are you handling the bank side yourself\?/,
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

test("prompt skips the address when identity confirmation already asks how to help", () => {
  const prompt = readPrompt();
  const identityHelpBranch = extractSection(
    prompt,
    'If the caller confirms identity and asks "how may I help you?"',
    "- If the caller answers your name question with something like",
  );

  assert.match(identityHelpBranch, /already invited the reason for the call/);
  assert.match(identityHelpBranch, /Do not say {{streetAddress}}/);
  assert.match(identityHelpBranch, /Do not ask "Got a quick second\?"/);
  assert.match(
    identityHelpBranch,
    /I was calling about your short sale listing to see whether you're handling the bank side yourself or already have someone on it\./,
  );
  assert.doesNotMatch(identityHelpBranch, /this is {{assistantName}} with Crisp Short Sales/);
  assert.doesNotMatch(identityHelpBranch, /short sale listing at {{streetAddress}}/);
});

test("prompt answers quick-second how-can-I-help turns immediately", () => {
  const prompt = readPrompt();
  const quickHelpBranch = extractSection(
    prompt,
    'If the caller answers "Got a quick second?" with a yes plus "how can I help you?"',
    "If they ask who is calling",
  );

  assert.match(quickHelpBranch, /Treat that as permission to continue/);
  assert.match(quickHelpBranch, /Do not pause to acknowledge it/);
  assert.match(
    quickHelpBranch,
    /I was calling to see if you're handling the bank side of the short sale yourself, or if you already have someone helping with that\./,
  );
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
    /Sorry if I wasn't clear\. We help agents with short sale bank paperwork, lender calls, and approval\. I was just calling to see if you wanted help with that\./,
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
    /No worries, I'll be quick\. I was calling about your short sale listing at {{streetAddress}}\./,
  );
  assert.doesNotMatch(busyNoiseBranch, /I'm {{assistantName}} with Crisp Short Sales/);
  assert.match(busyNoiseBranch, /paperwork, lender calls, and approval process/);
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
