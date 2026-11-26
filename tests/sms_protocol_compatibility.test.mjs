import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import test from "node:test";

const root = path.join(import.meta.dirname, "..");
const frozen = JSON.parse(fs.readFileSync(path.join(import.meta.dirname, "fixtures/sms_tasker_protocol_v370.json"), "utf8"));
// The owner-approved holiday gate may only insert these reviewed no-send checks.
// Hash the remaining original function body so every other protocol byte stays frozen.
const holidayInsertions = {
  isUnifiedSmsAction_: ["    owner_holiday_policy: true,\n    apply_owner_holiday_policy: true,\n"],
  handleUnifiedSmsPost_: [
    '    if (action === "owner_holiday_policy") return jsonOutput_({ ok: true, read_only: true, policy: getOwnerThanksgivingPolicyV38_() });\n' +
    '    if (action === "apply_owner_holiday_policy") return jsonOutput_(applyOwnerThanksgivingPolicyV38_(body));\n'
  ],
  claimPendingSmsSendOnceV36_: [
    '      var activeHoliday = thanksgivingFollowupPendingDecisionV38_(rows[activeIndex], new Date(now));\n' +
    '      if (activeHoliday) {\n' +
    '        rows[activeIndex] = deferThanksgivingFollowupV38_(sheet, activeIndex + 2, rows[activeIndex], activeHoliday);\n' +
    '        continue;\n      }\n',
    '      var followupHoliday = thanksgivingFollowupPendingDecisionV38_(rows[i], new Date(now));\n' +
    '      if (followupHoliday) {\n' +
    '        rows[i] = deferThanksgivingFollowupV38_(sheet, sheetRow, rows[i], followupHoliday);\n' +
    '        retryableFeaturePending = true;\n        continue;\n      }\n'
  ],
  markPendingSmsSendStartedV10_: [
    '  var holiday = thanksgivingFollowupPendingDecisionV38_(match.values, new Date());\n' +
    '  if (holiday) {\n' +
    '    if (String(match.values[1]) === "send_started" || match.values[13]) return {\n' +
    '      ok: false, should_send: false, retryable: false, status: String(match.values[1]),\n' +
    '      reason: "Prior send authorization needs reconciliation; Thanksgiving hold never authorizes a resend"\n' +
    '    };\n' +
    '    deferThanksgivingFollowupV38_(match.sheet, match.row, match.values, holiday);\n' +
    '    return { ok: false, should_send: false, retryable: true, status: "queued", reason: holiday.reason,\n' +
    '      retry_after_seconds: Math.max(60, Math.ceil((holiday.not_before.getTime() - Date.now()) / 1000)) };\n' +
    '  }\n'
  ]
};
for (const [file, names] of Object.entries(frozen.functions)) {
  const after = fs.readFileSync(path.join(root, file), "utf8");
  for (const [name, expected] of Object.entries(names)) test(`Tasker and safety contract remains byte-identical: ${name}`, () => {
    const pattern = new RegExp(`^function ${name}\\([\\s\\S]*?^}`, "m");
    assert.ok(after.match(pattern));
    let body = after.match(pattern)[0];
    for (const insertion of holidayInsertions[name] || []) {
      assert.equal(body.split(insertion).length, 2, `${name}: exact holiday guard must occur once`);
      body = body.replace(insertion, "");
    }
    assert.equal(createHash("sha256").update(body).digest("hex"), expected);
  });
}
