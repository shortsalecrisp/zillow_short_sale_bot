const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const source = fs.readFileSync(path.resolve(__dirname, "../src/scripts/syncElevenLabsAgent.ts"), "utf8");

test("sync preserves provider-owned model, voice, tools, and unrelated settings", () => {
  assert.match(source, /conversation_config: agent.conversation_config/);
  assert.match(source, /workflow: agent.workflow/);
  assert.doesNotMatch(source, /llm: config|TTS_SPEED|buildWarmTransferWorkflow|client.get<ToolResponse>/);
});
test("sync defaults to dry run and guards writes with reviewed live identifiers", () => {
  assert.match(source, /process.argv.includes\("--apply"\)/);
  assert.match(source, /Apply requires the reviewed live version and prompt SHA guards/);
  assert.match(source, /Agent changed after candidate creation; nothing applied/);
  assert.match(source, /enable_versioning_if_not_enabled: true/);
});
test("sync saves rollback and candidate receipts and verifies readback", () => {
  for (const file of ["before.json", "candidate.json", "after.json", "receipt.json"]) assert.ok(source.includes(file));
  assert.match(source, /Prompt readback mismatch after write/);
  assert.match(source, /Conversation policy readback mismatch after write/);
  assert.match(source, /if \(require.main === module\)/);
});
