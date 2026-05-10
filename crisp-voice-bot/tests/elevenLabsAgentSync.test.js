const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");

function readSyncScript() {
  return fs.readFileSync(
    path.resolve(__dirname, "../src/scripts/syncElevenLabsAgent.ts"),
    "utf8",
  );
}

test("agent sync enables skip_turn for placeholder-only noise turns", () => {
  const script = readSyncScript();

  assert.match(script, /const TURN_TIMEOUT_SECONDS = 2\.0;/);
  assert.match(script, /const TURN_EAGERNESS = "eager";/);
  assert.match(script, /skip_turn:\s*{/);
  assert.match(script, /name:\s*"skip_turn"/);
  assert.match(script, /system_tool_type:\s*"skip_turn"/);
});
