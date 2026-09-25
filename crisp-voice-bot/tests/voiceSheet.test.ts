import assert from "node:assert/strict";
import test from "node:test";
import {
  appendVoiceNotesValue,
  getVoiceBotFirstAttemptWindowName,
  getNextVoiceBotFirstAttemptWindowStart,
  getNextVoiceBotFollowupAttemptWindowStart,
  getVoiceBotPreferredCallWindowName,
  isRetryableVoiceBotResult,
  normalizePhoneToE164,
} from "../src/lib/voiceSheet";

test("replacement voice sheet helpers preserve the active call windows", () => {
  assert.equal(getVoiceBotPreferredCallWindowName(new Date("2026-05-04T13:30:00Z"), "America/New_York"), "morning_probe");
  assert.equal(getVoiceBotPreferredCallWindowName(new Date("2026-05-04T17:00:00Z"), "America/New_York"), "");
  assert.equal(getVoiceBotPreferredCallWindowName(new Date("2026-05-04T18:00:00Z"), "America/New_York"), "mid_afternoon");
  assert.equal(getVoiceBotPreferredCallWindowName(new Date("2026-05-04T18:45:00Z"), "America/New_York"), "mid_afternoon");
  assert.equal(getVoiceBotPreferredCallWindowName(new Date("2026-05-04T19:59:00Z"), "America/New_York"), "mid_afternoon");
  assert.equal(
    getVoiceBotPreferredCallWindowName(new Date("2026-05-04T20:15:00Z"), "America/New_York"),
    "",
  );
});

test("replacement voice sheet helpers retry provider start failures once", () => {
  assert.equal(isRetryableVoiceBotResult("call_start_failed"), true);
  assert.equal(isRetryableVoiceBotResult("voicemail_reached"), true);
  assert.equal(isRetryableVoiceBotResult("voicemail_reached_final_attempt"), false);
});

test("replacement voice sheet helpers bias first attempts toward afternoon and alternate second attempts", () => {
  assert.equal(getVoiceBotFirstAttemptWindowName(6001), "mid_afternoon");
  assert.equal(getVoiceBotFirstAttemptWindowName(6003), "morning_probe");
  assert.equal(
    getNextVoiceBotFirstAttemptWindowStart(new Date("2026-05-04T16:00:00Z"), "America/New_York", 6001).toISOString(),
    "2026-05-04T18:00:00.000Z",
  );
  assert.equal(
    getNextVoiceBotFirstAttemptWindowStart(new Date("2026-05-04T16:00:00Z"), "America/New_York", 6003).toISOString(),
    "2026-05-05T13:00:00.000Z",
  );
  assert.equal(
    getNextVoiceBotFollowupAttemptWindowStart(new Date("2026-05-04T20:45:00Z"), "America/New_York").toISOString(),
    "2026-05-05T13:00:00.000Z",
  );
  assert.equal(
    getNextVoiceBotFollowupAttemptWindowStart(new Date("2026-05-04T13:15:00Z"), "America/New_York").toISOString(),
    "2026-05-05T18:00:00.000Z",
  );
});

test("replacement voice sheet helpers normalize phones and append AP notes", () => {
  assert.equal(normalizePhoneToE164("(954) 205-3205"), "+19542053205");
  assert.equal(appendVoiceNotesValue("first", "second"), "first\n\n---\n\nsecond");
});
