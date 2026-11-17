import assert from "node:assert/strict";
import test from "node:test";
import { looksLikeDoNotCall } from "../src/lib/elevenLabsDoNotCall";
import { isRecordingOrScreeningArtifact } from "../src/lib/elevenLabsRecordingState";

test("plural list-removal requests are opt-outs, not service refusals", () => {
  for (const text of ["No, we already got it. Take us off your list.", "Please remove us from the calling list.", "Take me off your list."]) {
    assert.equal(looksLikeDoNotCall(text), true, text);
  }
  for (const text of ["Stop foreclosure.", "Take us to the listing.", "Don't take us off your list."]) {
    assert.equal(looksLikeDoNotCall(text), false, text);
  }
});

const ana = [
  { role: "user", message: "Thank you for calling Benchmark McCabe Mortgage Group. If you know your party's extension, dial it now. For Tina dial 43. For Danielle dial 42. For immediate assistance dial zero." },
  { role: "user", message: "Recording." },
  { role: "agent", message: "(No action needed - waiting silently, as this is a recording and not a live person.)" },
  { role: "user", message: "No action needed. Waiting silently as this is a recording and not a live person." },
  { role: "agent", message: "Understood. I'll remain silent and wait for a live person or further instructions." },
  { role: "user", message: "Understood. I'll remain silent and wait for a live person or further instructions." },
];
const broadcast = "My name is William Antebon, the junior here, and I came here to yield a few basis points. Hello. Hey, Louis, in energy, it's crude and Brent more than 3% each. I hope that movements are slowly starting to recover, and we continue to see in Vegas, home has found another 4% today, 23% so far in the month of November, just a few dollars above the low it made on Thursday. We're examining every angle of the NVIDIA story.";

test("IVR commentary echo does not create a human pickup; later live intent wins", () => {
  assert.equal(isRecordingOrScreeningArtifact(ana), true);
  for (const message of ["This is Ana. What is this about?", "Please call me back tomorrow at ten.", "Do not call us again."]) {
    assert.equal(isRecordingOrScreeningArtifact([...ana, { role: "user", message }]), false, message);
  }
});

test("an unaddressed sustained financial broadcast is not a listing-agent conversation", () => {
  const recording = [{ role: "user", message: broadcast }, { role: "agent", message: "Thanks for sharing that, William." }, { role: "user", message: "..." }];
  assert.equal(isRecordingOrScreeningArtifact(recording), true);
  assert.equal(isRecordingOrScreeningArtifact([...recording, { role: "user", message: "Hi, this is Daryl. How can I help?" }]), false);
  assert.equal(isRecordingOrScreeningArtifact([{ role: "user", message: broadcast + " Can you tell me why you're calling?" }]), false);
});
