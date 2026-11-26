import assert from "node:assert/strict";
import test from "node:test";
import { getStartCallWindowBlockReason } from "../src/lib/callWindowGuard";
import { getNextVoiceBotFirstAttemptWindowStart, getNextVoiceBotFollowupAttemptWindowStart,
  getVoiceBotPreferredCallWindowName } from "../src/lib/voiceSheet";

test("Thanksgiving blocks every scheduled outbound window in the listing local date, Friday stays open", () => {
  for (const [zone, offset] of [["America/New_York", "-05:00"], ["America/Chicago", "-06:00"],
    ["America/Denver", "-07:00"], ["America/Los_Angeles", "-08:00"], ["Pacific/Honolulu", "-10:00"]]) {
    for (const [window, hour] of [["reach_morning_v1", "09:15"], ["reach_afternoon_v1", "15:00"],
      ["morning_probe", "09:15"], ["mid_afternoon", "15:00"]]) {
      const payload = { agentTimeZone: zone, scheduledWindow: window };
      assert.match(getStartCallWindowBlockReason(payload, new Date(`2026-11-26T${hour}:00${offset}`))!, /Thanksgiving/);
      assert.equal(getStartCallWindowBlockReason(payload, new Date(`2026-11-27T${hour}:00${offset}`)), null);
    }
    assert.equal(getVoiceBotPreferredCallWindowName(new Date(`2026-11-26T09:20:00${offset}`), zone), "");
    const next = getNextVoiceBotFirstAttemptWindowStart(new Date(`2026-11-26T08:00:00${offset}`), zone, "+12025550100", "reach_morning_v1");
    assert.equal(next.toISOString(), new Date(`2026-11-27T09:15:00${offset}`).toISOString());
    const retry = getNextVoiceBotFollowupAttemptWindowStart(new Date(`2026-11-25T15:05:00${offset}`), zone);
    assert.equal(retry.toISOString(), new Date(`2026-11-27T09:15:00${offset}`).toISOString());
  }
});
