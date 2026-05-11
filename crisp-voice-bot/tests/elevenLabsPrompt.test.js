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

test("opening fallback confirmation uses a short continuation instead of repeating the address", () => {
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

  assert.deepEqual(spokenLines, ['"Perfect, thanks. Got a quick second?"']);
  assert.doesNotMatch(spokenLines.join("\n"), /{{streetAddress}}/);
  assert.doesNotMatch(spokenLines.join("\n"), /Crisp Short Sales/);
});

test("prompt treats not-a-short-sale objections as a clear no", () => {
  const prompt = readPrompt();
  assert.match(prompt, /not a short sale/i);
  assert.match(prompt, /Then call `not_interested`/);
});

test("prompt redirects unknown affiliation questions back to the short-sale plan", () => {
  const prompt = readPrompt();

  assert.match(
    prompt,
    /I'm with Crisp Short Sales, working with Yoni Kutler who is our short sale specialist\. What's your plan for handling the short sale with the bank\?/,
  );
});

test("prompt treats placeholder-only user turns as background noise and skips speaking", () => {
  const prompt = readPrompt();

  assert.match(prompt, /If the latest caller message is exactly "\.\.\."/);
  assert.match(prompt, /background noise/i);
  assert.match(prompt, /call `skip_turn`/);
  assert.match(prompt, /Do not say[\s\S]{0,120}Are you still there\?/);
});

test("prompt waits through office robots and gatekeeper transfer attempts", () => {
  const prompt = readPrompt();

  assert.match(prompt, /automated attendant/i);
  assert.match(prompt, /phone tree/i);
  assert.match(prompt, /Please stay on the line/i);
  assert.match(prompt, /Sure, I'll wait\./);
  assert.match(prompt, /Do not call `end_call`[\s\S]{0,160}transferred/i);
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
    /Hi {{firstName}}, Emmy with Crisp Short Sales\. I'm calling about your short sale listing\. What's your plan for handling it with the bank\?/,
  );
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
    /I was calling to see what your plan is for handling the short sale with the bank\./,
  );
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
    /No worries, I'll be quick\. I'm Emmy with Crisp Short Sales, calling for Yoni Kutler about your short sale listing at {{streetAddress}}\./,
  );
  assert.match(busyNoiseBranch, /paperwork, lender calls, and approval process/);
  assert.match(busyNoiseBranch, /call `callback_requested`/);
});
