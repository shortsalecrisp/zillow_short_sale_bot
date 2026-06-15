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

function readConfig() {
  return fs.readFileSync(
    path.resolve(__dirname, "../src/lib/config.ts"),
    "utf8",
  );
}

test("agent sync enables skip_turn for placeholder-only noise turns", () => {
  const script = readSyncScript();

  assert.match(script, /const FIRST_MESSAGE = "{{openerScript}}";/);
  assert.match(script, /const INITIAL_WAIT_TIME_SECONDS = -1;/);
  assert.match(script, /const TURN_TIMEOUT_SECONDS = 1\.2;/);
  assert.match(script, /const TURN_EAGERNESS = "eager";/);
  assert.match(script, /skip_turn:\s*{/);
  assert.match(script, /name:\s*"skip_turn"/);
  assert.match(script, /system_tool_type:\s*"skip_turn"/);
});

test("agent sync uses natural low-latency TTS settings", () => {
  const script = readSyncScript();
  const config = readConfig();

  assert.match(config, /ttsModel:\s*readEnv\("ELEVENLABS_TTS_MODEL", "eleven_flash_v2"\)/);
  assert.match(config, /voiceAbTestEnabled:\s*readBoolean\("ELEVENLABS_VOICE_AB_TEST_ENABLED", false\)/);
  assert.match(script, /const TTS_SPEED = 1\.0;/);
  assert.match(script, /optimize_streaming_latency: 0,/);
  assert.doesNotMatch(script, /const TTS_SPEED = 1\.05;/);
});

test("agent sync enables per-call voice overrides for A/B testing", () => {
  const script = readSyncScript();

  assert.match(script, /platform_settings:\s*updatedPlatformSettings,/);
  assert.match(script, /conversation_config_override/);
  assert.match(script, /tts:\s*{\s*\.\.\.currentTtsOverrides,\s*voice_id:\s*true,/s);
});

test("agent sync blocks ambiguous okay-style replies from the transfer-check edge", () => {
  const script = readSyncScript();

  assert.match(script, /clearly and unambiguously wants to talk to Yoni right now/);
  assert.match(script, /vague or overlapped replies like okay okay/);
  assert.match(script, /caller is busy, in a meeting, wants later\/tomorrow, will call back/);
  assert.match(script, /clarify callback versus trying Yoni now/);
});
