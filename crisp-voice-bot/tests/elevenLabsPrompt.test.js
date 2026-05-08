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
  assert.match(prompt, /Crisp Short Sales specializes in helping agents/i);
  assert.match(prompt, /Then call `not_interested`/);
});

test("prompt treats partial this-is identity replies as confirmed", () => {
  const prompt = readPrompt();

  assert.match(prompt, /partial identity/i);
  assert.match(prompt, /"this is"/i);
  assert.match(prompt, /first recognized human response/i);
  assert.match(prompt, /Do not wait for the caller to repeat/i);
});
