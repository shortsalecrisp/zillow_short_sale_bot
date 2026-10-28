import assert from "node:assert/strict";
import test from "node:test";
import { isRecordingOrScreeningArtifact, isRepeatedPhoneMenuFragment } from "../src/lib/elevenLabsRecordingState";

const menu = "Thank you for calling Synthetic Realty. For buying or selling, press one. For property management, press two.";

test("repeated IVR greeting fragments do not invent a later live pickup", () => {
  for (const fragment of ["Thank you for calling. Good", "Thank you for calling.", "Thank you for calling Synthetic Realty."]) {
    assert.equal(isRepeatedPhoneMenuFragment(fragment, [menu]), true);
    assert.equal(isRecordingOrScreeningArtifact([{ role: "user", message: menu }, { role: "user", message: fragment }]), true);
    assert.equal(isRepeatedPhoneMenuFragment(fragment, []), false);
    assert.equal(isRecordingOrScreeningArtifact([{ role: "user", message: fragment }]), false);
  }
});

test("a later real responder or contact preference is never swallowed as a menu fragment", () => {
  for (const message of ["Thank you for calling. This is Ellen. How may I help?", "This is Ellen, that is my listing.",
    "Please stop calling me.", "Can you email me instead?", "No thanks, I already have help.", "Call me tomorrow."]) {
    assert.equal(isRepeatedPhoneMenuFragment(message, [menu]), false);
    assert.equal(isRecordingOrScreeningArtifact([{ role: "user", message: menu }, { role: "user", message }]), false);
  }
});
