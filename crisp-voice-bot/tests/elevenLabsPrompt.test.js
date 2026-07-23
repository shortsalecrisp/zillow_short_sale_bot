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

  assert.match(prompt, /"{{openerScript}}"/);
  assert.match(prompt, /The backend chooses `{{openerScript}}` for the opener test/);
  assert.match(prompt, /passes `{{openerVariant}}` for analysis/);
  assert.match(prompt, /Do not add a long pause before the opener/);
  assert.match(prompt, /Do not say `{{streetAddress}}` in the first line/);
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

test("prompt introduces Maya once and forbids repeated self-introductions after the opener", () => {
  const prompt = readPrompt();
  const oneIntroRule = extractSection(
    prompt,
    "One self-introduction rule:",
    "Opening:",
  );

  assert.match(oneIntroRule, /Say your name and Crisp Short Sales once in the opener/);
  assert.match(oneIntroRule, /After that, do not repeat your name, Crisp Short Sales, or the listing reason/);
  assert.match(oneIntroRule, /unless the caller asks who is calling/i);
  assert.match(oneIntroRule, /a new person comes onto the call/i);
  assert.match(oneIntroRule, /voicemail/i);
  assert.match(oneIntroRule, /gatekeeper/i);
  assert.match(oneIntroRule, /Never use a repeat self-introduction as a repair phrase/);
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

test("prompt turns human-only objections into direct Yoni callback rescue", () => {
  const prompt = readPrompt();
  const humanRescueBranch = extractSection(
    prompt,
    "If they object to automation",
    "If they ask whether you are with another person",
  );

  assert.match(
    humanRescueBranch,
    /Totally understand\. Yoni is the person who handles these\. Do you want him to call you directly\?/,
  );
  assert.match(humanRescueBranch, /call `callback_requested` with `callbackTime` set to `asap`/);
  assert.match(humanRescueBranch, /handoff-ready interested callback/);
  assert.match(humanRescueBranch, /direct human callback/);
  assert.match(humanRescueBranch, /Ok, I'll have Yoni call you directly\. Thanks\./);
  assert.match(humanRescueBranch, /immediately call `end_call`/);
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
    "- If the first response is clipped",
  );

  assert.match(confusedRepair, /what\?/i);
  assert.match(
    confusedRepair,
    /Sorry, I may have caught you fast\. Is this {{firstName}}\?/,
  );
  assert.match(
    confusedRepair,
    /Thanks\. I was calling about your short sale listing\. Are you handling the bank side yourself\?/,
  );
  assert.match(confusedRepair, /Do not repeat Crisp Short Sales/);
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
  assert.match(selfHandlingBranch, /acknowledge that first/i);
  assert.match(selfHandlingBranch, /Do not treat this as a hard no/);
  assert.match(selfHandlingBranch, /whole short sale process with the bank/);
  assert.match(selfHandlingBranch, /no cost to you or the seller/);
  assert.match(selfHandlingBranch, /Do you have any interest in talking with Yoni/);
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
  assert.match(transferRule, /Do not treat a vague or overlapped "okay okay"/);
  assert.match(transferRule, /"I, so\.\.\. okay"/);
  assert.match(transferRule, /in a meeting/);
  assert.match(transferRule, /afternoon\/tomorrow\/later/);
  assert.match(transferRule, /Sorry, I may have talked over you/);
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
