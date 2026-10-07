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
  getVoiceBotAgentTimeZone,
} from "../src/lib/voiceSheet";

test("cold outreach stays in the new weekday experiment windows", () => {
  assert.equal(getVoiceBotPreferredCallWindowName(new Date("2026-05-04T13:30:00Z"), "America/New_York"), "reach_morning_v1");
  assert.equal(getVoiceBotPreferredCallWindowName(new Date("2026-05-04T17:00:00Z"), "America/New_York"), "");
  assert.equal(getVoiceBotPreferredCallWindowName(new Date("2026-05-04T18:00:00Z"), "America/New_York"), "");
  assert.equal(getVoiceBotPreferredCallWindowName(new Date("2026-05-04T19:00:00Z"), "America/New_York"), "reach_afternoon_v1");
  assert.equal(getVoiceBotPreferredCallWindowName(new Date("2026-05-04T19:45:00Z"), "America/New_York"), "");
  assert.equal(getVoiceBotPreferredCallWindowName(new Date("2026-05-09T13:30:00Z"), "America/New_York"), "");
  assert.equal(
    getVoiceBotPreferredCallWindowName(new Date("2026-05-04T20:15:00Z"), "America/New_York"),
    "",
  );
});

test("replacement voice sheet helpers retry provider start failures once", () => {
  assert.equal(isRetryableVoiceBotResult("call_start_failed"), true);
  assert.equal(isRetryableVoiceBotResult("voicemail_reached"), true);
  assert.equal(isRetryableVoiceBotResult("human_answered_no_response_first_attempt"), true);
  assert.equal(isRetryableVoiceBotResult("human_answered_no_bot_response"), true);
  assert.equal(isRetryableVoiceBotResult("voicemail_reached_final_attempt"), false);
});

test("stable phone assignment is balanced and retries use the next opposite local slot", () => {
  const firstPhone = "+12175550100";
  const otherPhone = "+12175550101";
  assert.equal(getVoiceBotFirstAttemptWindowName(firstPhone), "reach_afternoon_v1");
  assert.equal(getVoiceBotFirstAttemptWindowName(otherPhone), "reach_morning_v1");
  assert.equal(getVoiceBotFirstAttemptWindowName("(217) 555-0100"), getVoiceBotFirstAttemptWindowName(firstPhone));
  assert.equal(
    getNextVoiceBotFirstAttemptWindowStart(new Date("2026-05-04T16:00:00Z"), "America/New_York", firstPhone).toISOString(),
    "2026-05-04T19:00:00.000Z",
  );
  assert.equal(
    getNextVoiceBotFirstAttemptWindowStart(new Date("2026-05-04T16:00:00Z"), "America/New_York", otherPhone).toISOString(),
    "2026-05-05T13:15:00.000Z",
  );
  assert.equal(
    getNextVoiceBotFollowupAttemptWindowStart(new Date("2026-05-04T20:45:00Z"), "America/New_York").toISOString(),
    "2026-05-05T13:15:00.000Z",
  );
  assert.equal(
    getNextVoiceBotFollowupAttemptWindowStart(new Date("2026-05-04T13:15:00Z"), "America/New_York").toISOString(),
    "2026-05-04T19:00:00.000Z",
  );
  assert.equal(getNextVoiceBotFollowupAttemptWindowStart(new Date("2026-05-08T19:15:00Z"), "America/New_York").toISOString(), "2026-05-11T13:15:00.000Z");
  assert.equal(getNextVoiceBotFollowupAttemptWindowStart(new Date("2026-05-08T19:15:00Z"), "America/New_York", 1).toISOString(), "2026-05-11T13:15:00.000Z");
  assert.equal(getVoiceBotAgentTimeZone(Array(42).fill("")), "");
});

test("next-slot retries preserve local time across timezones, weekends and DST", () => {
  const cases = [
    ["2026-10-07T13:30:00Z", "America/New_York", "2026-10-07T19:00:00.000Z"],
    ["2026-10-07T19:30:00Z", "America/New_York", "2026-10-08T13:15:00.000Z"],
    ["2026-10-09T13:30:00Z", "America/New_York", "2026-10-09T19:00:00.000Z"],
    ["2026-10-09T22:30:00Z", "America/Los_Angeles", "2026-10-12T16:15:00.000Z"],
    ["2026-10-07T16:30:00Z", "America/Phoenix", "2026-10-07T22:00:00.000Z"],
    ["2026-10-07T22:30:00Z", "America/Phoenix", "2026-10-08T16:15:00.000Z"],
    ["2026-10-30T19:30:00Z", "America/New_York", "2026-11-02T14:15:00.000Z"],
    ["2026-03-06T20:30:00Z", "America/New_York", "2026-03-09T13:15:00.000Z"],
  ];
  for (const [firstAttemptAt, timeZone, expected] of cases) {
    assert.equal(getNextVoiceBotFollowupAttemptWindowStart(new Date(firstAttemptAt), timeZone).toISOString(), expected);
  }
  assert.throws(() => getNextVoiceBotFollowupAttemptWindowStart(new Date(), ""), /time zone/);
});

test("replacement voice sheet helpers normalize phones and append AP notes", () => {
  assert.equal(normalizePhoneToE164("(954) 205-3205"), "+19542053205");
  assert.equal(appendVoiceNotesValue("first", "second"), "first\n\n---\n\nsecond");
});

test("replacement voice sheet helpers do not duplicate an identical AP receipt", () => {
  const receipt = "--- CODEX_VOICE_CALL_METRICS_V1 ---\n{\"call\":{\"conversationId\":\"conv_123\"}}";
  assert.equal(appendVoiceNotesValue(receipt, receipt), receipt);
  assert.equal(appendVoiceNotesValue(`older\n\n---\n\n${receipt}`, receipt), `older\n\n---\n\n${receipt}`);
});
