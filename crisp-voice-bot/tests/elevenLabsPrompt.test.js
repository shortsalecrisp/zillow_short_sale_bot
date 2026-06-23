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
    '"Thanks. Are you handling the bank side yourself?"',
  ]);
  assert.doesNotMatch(spokenLines.join("\n"), /{{streetAddress}}/);
  assert.doesNotMatch(spokenLines.join("\n"), /Crisp Short Sales/);
  assert.doesNotMatch(spokenLines.join("\n"), /Got a quick second/);
});

test("prompt uses dynamic opener scripts and keeps the address out of the first opener", () => {
  const prompt = readPrompt();
  const openerBranch = extractSection(
    prompt,
    "- If the caller confirms identity after the first name-only opener, your very next line must be:",
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
    /Hey {{firstName}}, this is {{assistantName}} with Crisp Short Sales about your short sale listing\. Are you handling the bank side yourself\?/,
  );
  assert.doesNotMatch(openerBranch, /short sale listing at {{streetAddress}}/);
  assert.match(
    genericFallbackBranch,
    /Hey, this is {{assistantName}} with Crisp Short Sales about a short sale listing\. Is this {{firstName}}\?/,
  );
  assert.doesNotMatch(genericFallbackBranch, /{{streetAddress}}/);
  assert.match(prompt, /If they ask which listing, which property, or what address/);
}
);

test("prompt treats not-a-short-sale objections as a clear no", () => {
  const prompt = readPrompt();
  assert.match(prompt, /not a short sale/i);
  assert.match(prompt, /Then call `not_interested`/);
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
  assert.match(prompt, /Then call `not_interested`/);
});

test("prompt closes immediately when the caller already has short sale help", () => {
  const prompt = readPrompt();
  const coveredBranch = extractSection(
    prompt,
    "If they say they already have a short sale negotiator",
    "If they say they are not worried about it",
  );

  assert.match(coveredBranch, /attorney, specialist, someone handling it/);
  assert.match(coveredBranch, /Do not pitch/);
  assert.match(coveredBranch, /please just keep me in mind\. Thanks!/);
  assert.match(coveredBranch, /Then immediately call `not_interested`/);
  assert.match(coveredBranch, /After the tool returns, call `end_call`/);
});

test("prompt treats direct or self-handling answers as a soft value-pitch opportunity", () => {
  const prompt = readPrompt();
  const selfHandlingBranch = extractSection(
    prompt,
    "If they answer the plan question with \"direct\"",
    "If they say they already have a short sale negotiator",
  );

  assert.match(selfHandlingBranch, /handling it themselves/i);
  assert.match(selfHandlingBranch, /Do not treat this as a hard no/);
  assert.match(selfHandlingBranch, /whole short sale process with the bank/);
  assert.match(selfHandlingBranch, /no cost to you or the seller/);
  assert.match(selfHandlingBranch, /Do you have any interest in talking with Yoni/);
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
    /Hey {{firstName}}, this is {{assistantName}} with Crisp Short Sales about your short sale listing\. Are you handling the bank side yourself\?/,
  );
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
    /No worries, I'll be quick\. I'm {{assistantName}} with Crisp Short Sales, calling for Yoni Kutler about your short sale listing at {{streetAddress}}\./,
  );
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
