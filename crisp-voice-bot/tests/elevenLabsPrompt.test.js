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
