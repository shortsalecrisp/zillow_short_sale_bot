import assert from "node:assert/strict";
import test from "node:test";
import { isAutomatedMessageTakerConversation, isRecordedNoVoicemailGreeting, isRecordingOrScreeningArtifact, isRepeatedPhoneMenuFragment } from "../src/lib/elevenLabsRecordingState";

const menu = "Thank you for calling Synthetic Realty. For buying or selling, press one. For property management, press two.";
const noMessage = "Hola. Thank you for calling Jana. I am so sorry I'm not able to take your call, but please send me a text. Please do not leave me a voicemail as I hardly ever listen to those. Have a great day.";

test("an explicit recorded no-message instruction is not live contact intent", () => {
  assert.equal(isRecordedNoVoicemailGreeting(noMessage), true);
  assert.equal(isRecordingOrScreeningArtifact([{ role: "user", message: noMessage }]), true);
  for (const text of [
    "You've reached Jana. I cannot answer your call. Don't leave a message; text me instead.",
    "This is Jana. I can't take your call. Please text me instead of voicemail.",
  ]) assert.equal(isRecordedNoVoicemailGreeting(text), true);
  for (const text of [
    "Hi, this is Jana. Please leave a message, or text me for a faster response.",
    "Please don't leave a voicemail. Can you email me instead?",
    "My voicemail says don't leave a message. I'm here now. What is this about?",
    "Thanks, Maya. Please stay on the line.",
    "This is Jana. I cannot take your call. Do you want me to call you back?",
  ]) assert.equal(isRecordedNoVoicemailGreeting(text), false);
});

test("later actual callback, opt-out and admin questions supersede a no-message recording", () => {
  for (const text of ["This is Jana. Can you call me back tomorrow?", "Please stop calling me.",
    "I'm the assistant. Which property are you calling about?", "Please email me instead."]) {
    assert.equal(isRecordingOrScreeningArtifact([{ role: "user", message: noMessage }, { role: "user", message: text }]), false);
  }
});

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

test("a first-person voicemail promise plus mailbox menu is not live callback consent", () => {
  const greeting = "Hi, this is Carolyn. Sorry I missed your call. I will call you back. Thank you.";
  const mailbox = "To review, rerecord, or add to your message, press one. To send your message as is, press pound.";
  const transcript = [{ role: "user", message: greeting }, { role: "user", message: mailbox }];
  assert.equal(isRecordingOrScreeningArtifact(transcript), true);
  for (const message of ["Sorry I missed your call--what's this about?", "Can you call me back tomorrow?",
    "Please stop calling me.", "No thanks, I already have help.", "Please email me instead."]) {
    assert.equal(isRecordingOrScreeningArtifact([...transcript, { role: "user", message }]), false);
  }
});

test("a staged receptionist that confirms a message then closes is an automated artifact", () => {
  const transcript = [
    { role: "user", message: "Would you like me to pass a message to Sold By Harrison? If so, what is the best number to reach you back?" },
    { role: "agent", message: "Yoni at Crisp Short Sales can answer questions. His number is 404-300-9526." },
    { role: "user", message: "Just to confirm, you want me to let Sold By Harrison know Yoni can help with the short-sale process?" },
    { role: "agent", message: "That is correct, thank you." },
    { role: "user", message: "Understood. I'll pass that message along. Someone from Sold By Harrison will get back to you if needed. Thanks for calling. Have a good day." },
  ];
  assert.equal(isAutomatedMessageTakerConversation(transcript), true);
  assert.equal(isRecordingOrScreeningArtifact(transcript), true);
  const liveAdmin = transcript.slice(0, 2).concat([
    { role: "user", message: "Which property is this about, and what service do you provide?" },
  ]);
  assert.equal(isAutomatedMessageTakerConversation(liveAdmin), false);
  assert.equal(isRecordingOrScreeningArtifact(liveAdmin), false);
});
