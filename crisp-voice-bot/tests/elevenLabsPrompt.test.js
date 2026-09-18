const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const source = fs.readFileSync(path.resolve(__dirname, "../docs/elevenlabs-agent-prompt.md"), "utf8");
const prompt = source.split("## Prompt\n")[1].trim();
function section(heading) {
  const start = prompt.indexOf("# " + heading + "\n");
  assert.ok(start >= 0, "Missing prompt section: " + heading);
  const end = prompt.indexOf("\n# ", start + 2);
  return prompt.slice(start, end < 0 ? prompt.length : end);
}
// Static source contracts only. These do not prove model adherence, native gate
// execution, audio delivery, or production deployment.
test("prompt has one extractable body and distinct concise state sections", () => {
  assert.equal(source.split("## Prompt\n").length, 2);
  const headings = [...prompt.matchAll(/^# (.+)$/gm)].map(match => match[1]);
  assert.equal(new Set(headings).size, headings.length);
  for (const heading of ["Shared turn priority", "Listening and repair", "Intro only",
    "Post-intro conversation", "Answer library", "Contact preferences and endings",
    "Request records and receipts", "Callback request", "Information request",
    "Live transfer request", "Live admins and wrong contacts", "Automated screening and hold",
    "Voicemail and recorded exits"]) assert.ok(headings.includes(heading), heading);
  assert.ok(prompt.split(/\s+/).length < 5500, "Consolidation must not reintroduce the duplicated 9,899-word body");
  assert.match(source.split("## Prompt\n")[0], /not a verified deployment/);
});
test("priority considers the whole turn and hearing precedes questions and actions", () => {
  const s = section("Shared turn priority");
  assert.match(s, /Read the whole latest intelligible turn in context, including restrictions/);
  assert.match(s, /not a first-match rule that discards additional questions or actions/);
  assert.ok(s.indexOf("Honor explicit future opt-out") < s.indexOf("Resolve hearing difficulty"));
  assert.ok(s.indexOf("Resolve hearing difficulty") < s.indexOf("Address corrections"));
  assert.ok(s.indexOf("Address corrections") < s.indexOf("Follow the caller's explicitly chosen next step"));
  assert.match(s, /Preserve an accompanying email\/callback request/);
});
test("intro is its own entire turn and exposes no dynamic continuation script", () => {
  const s = section("Intro only");
  assert.match(s, /your entire spoken turn is:\n"Hi, this is {{assistantName}} with Crisp Short Sales\. I was calling about the short-sale paperwork and lender calls for your listing at \{\{streetAddress\}\}\. Is it okay if I ask one quick question about that\?"/);
  assert.match(s, /Stop after the question\. Wait for a NEW live-caller turn/);
  assert.match(s, /occurred before the introduction and does not count as a response/);
  assert.match(s, /Do not append a second qualification question, Yoni offer, or callback question/);
  assert.match(s, /A placeholder "\.\.\." is not that new turn: use skip_turn, without saying "Are you there\?"/);
  assert.doesNotMatch(s, /{{openerScript}}|Are you handling the short sale paperwork|Are you looking for help with that/);
  assert.doesNotMatch(prompt, /{{openerScript}}/);
});
test("post-intro handles an answer to the new opener without repeating the pitch", () => {
  const s = section("Post-intro conversation");
  assert.match(s, /Enter only when a NEW live-caller turn/);
  assert.match(s, /contained no question, correction, hearing issue or requested next step when it arrived/);
  assert.match(s, /A further caller turn is required before qualification/);
  assert.match(s, /openerVariant continuation is retired; do not speak a second opener/);
  assert.match(s, /A clear yes permits one short qualification question, not a transfer or callback/);
  assert.match(s, /A clear no to the quick-question permission is a scoped decline of the question, not a future-contact opt-out/);
  assert.match(s, /For a neutral acknowledgment or an unclear answer, ask at most once/);
});
test("a question keeps the entire response answer-only even after it is answered", () => {
  const s = section("Shared turn priority");
  assert.match(s, /Choose the response mode from the caller's entire turn AS RECEIVED/);
  assert.match(s, /remains answer-only for your whole response/);
  assert.match(s, /Answering it does not make qualification eligible later in that same response/);
  assert.match(s, /only an explicitly requested contact action may follow; otherwise stop and wait/);
});
test("live identity and first-turn questions do not restart the pitch", () => {
  assert.match(prompt, /You are {{assistantName}}, an AI calling assistant for Crisp Short Sales/);
  const s = section("Intro only");
  assert.match(s, /answer only those points from the answer library instead of delivering the full introduction/);
  assert.match(s, /Do not repeat identity or purpose already answered/);
  assert.match(s, /a different live listener gets their own short introduction/);
  assert.doesNotMatch(prompt, /this is Emmy with Crisp Short Sales/i);
});
test("name corrections and authorized admins outrank the stored lead name", () => {
  const s = section("Intro only");
  assert.match(s, /use the corrected name if clear and treat them as the current contact/);
  assert.match(s, /Do not ask for {{firstName}} or repeat an already answered handling question/);
  assert.match(s, /Never guess a name/);
  assert.match(section("Live admins and wrong contacts"), /If the admin can discuss the listing, speak with them instead of insisting on a transfer/);
});
test("repeated purpose repair and hearing restoration cannot become qualification", () => {
  const s = section("Listening and repair");
  assert.match(s, /On the first purpose challenge[^]+Would you like me to explain\?/);
  assert.match(s, /If the caller's next intelligible turn still asks the same purpose/);
  assert.match(s, /I'm sorry I wasn't clear\. I'll let you go\. Goodbye\./);
  assert.match(s, /For an audio problem say only "Sorry, can you hear me now\?" and wait/);
  assert.match(s, /hearing-restoration answer[^\n]+is not qualification consent/);
  assert.match(s, /repeat only the missed short sentence, then wait again/);
  assert.match(s, /Do not claim the connection or volume was fixed/);
  assert.match(s, /including "What\?", "I don't understand", "Why are you calling\?", or "What do you want from me\?"/);
  assert.match(s, /Never respond with "How can I help you today\?"/);
});
test("bounded repair preserves understood fragments and never guesses consent", () => {
  const s = section("Listening and repair");
  assert.match(s, /retain that part and ask only for what was missing/);
  assert.match(s, /Do not repeat the pitch, offer a person, callback or transfer/);
  assert.match(s, /This narrow repeated-purpose exit is not permission for future contact/);
});
test("noise and partial turns cannot create contact decisions", () => {
  const s = section("Listening and repair");
  assert.match(s, /If the latest caller message is exactly "\.\.\."/);
  assert.match(s, /An intelligible question is not noise/);
  assert.match(s, /Do not infer a name, yes, callback, rejection, or transfer from background speech/);
  assert.match(s, /do not finish over the caller/);
  assert.match(s, /A thinking pause is not an invitation to complete their sentence/);
});
test("processed stops and denied ending checks do not trigger presence questions", () => {
  const s = section("Listening and repair");
  assert.match(s, /After a processed stop, opt-out, refusal, receipt or ending-check failure, do not ask whether they are still there/);
  assert.match(s, /Wait silently unless a new intelligible caller turn needs an answer/);
});
test("payer corrections get the substantive answer while preserving other questions or requests", () => {
  assert.match(section("Shared turn priority"), /A payer correction needs the payer answer, not just "Understood\."/);
  assert.match(section("Answer library"), /The buyer typically pays the flat fee, only if the deal closes\." If another question or request accompanies it, address that too/);
  assert.match(section("Answer library"), /Answer all questions asked, then wait/);
});
test("a short yes or no is scoped to the latest single permission question", () => {
  const s = section("Shared turn priority");
  assert.match(s, /latest single permission question/);
  assert.match(s, /compound question, overlap, noise, or yes followed by a restriction is not clear permission/);
  assert.match(s, /a mentioned time, or "not now" alone is not callback or transfer consent/);
});
test("the declined object is explicit and callback-only is not terminal", () => {
  const s = section("Contact preferences and endings");
  assert.match(s, /Determine what was declined, not just whether the sentence contains "no"/);
  assert.match(s, /A declined transfer, time, channel, or appointment is not a rejection of all service/);
  for (const phrase of ["No transfer now", "callback only", "email only", "not a callback",
    "use this number", "That is the callback request"]) assert.ok(s.includes('"' + phrase + '"'), phrase);
  assert.match(s, /preferences or corrections, not farewells/);
  assert.match(s, /Thanks, okay, understood, silence, and tool completion alone do not/);
});
test("genuine service rejection can end promptly but questions and alternatives survive", () => {
  const s = section("Contact preferences and endings");
  assert.match(s, /Questions and alternate requested next steps must be handled before a service-decline closeout/);
  assert.match(s, /may close the call without asking for another goodbye/);
  assert.match(s, /clearly refers to all offered help, not a time, correction or transfer choice/);
  assert.match(s, /Do not insist on another question or pitch/);
  assert.doesNotMatch(prompt, /If they say no or not interested, call|After a clear rejection or request to end, give/);
});
test("current-call ending and future opt-out retain distinct recording markers", () => {
  const s = section("Contact preferences and endings");
  assert.match(s, /standalone "STOP" has priority over every pitch and action/);
  assert.match(s, /remove me from your list/);
  assert.match(s, /DO NOT CALL: caller explicitly requested no further calls\./);
  assert.match(s, /CALL ENDED BY REQUEST: caller asked to end the current call only\./);
  assert.match(s, /Do not erase earlier genuine interest or a requested callback unless revoked/);
  assert.match(s, /Ending is not permission for another automated call/);
  assert.doesNotMatch(s, /We won't call again/);
});
test("third-party restriction does not become caller opt-out and mixed opt-out is protected", () => {
  const s = section("Contact preferences and endings");
  assert.match(s, /"Do not call Yoni now" declines a third-party\/live-transfer action/);
  assert.match(s, /not the caller's future opt-out unless they also reject calls to themselves/);
  assert.match(s, /Honor a genuine mixed caller opt-out/);
});
test("live hangup is delegated to a fresh native gate rather than direct end_call", () => {
  const s = section("Contact preferences and endings");
  assert.match(s, /Request a live-call ending only through the guarded ending workflow/);
  assert.match(s, /Do not call end_call directly from the main conversation/);
  assert.match(s, /validate provider-bound raw history using validate_call_ending/);
  assert.match(s, /never fabricate or rewrite caller history to obtain permission/);
  assert.match(s, /Only permission: true from a successful validation of the latest observed user history/);
  assert.match(s, /denied permission, missing result, or error is not authorization/);
  assert.match(s, /history fingerprint does not guarantee that no newer speech has arrived/);
  assert.match(s, /Do not announce goodbye before terminal permission is established/);
  assert.match(s, /validator has no contact-action side effects/);
  assert.equal((prompt.match(/end_call/g) || []).length, 1, "No alternative direct hangup instruction");
});
test("new contact preferences use the native recording path before ending checks", () => {
  const s = section("Contact preferences and endings");
  assert.match(s, /Contact recording, permission reset and validation are native workflow steps, not separate model-selected tools/);
  assert.match(s, /must take the contact-recording path before the generic goodbye path/);
  assert.match(s, /Recording completion or failure flows directly into reset and current-history validation/);
  assert.match(s, /A genuine goodbye without a contact preference uses the generic guarded-ending path/);
  assert.match(s, /Do not repeat a recording or failed\/denied check for the same caller turn/);
  assert.match(s, /do not make a separate recording-tool call or add closing speech/);
});
test("request receipt is not completion or a new caller turn", () => {
  const s = section("Request records and receipts");
  assert.match(s, /requestCaptured: true permits a receipt acknowledgment/);
  assert.match(s, /queued: true is receipt\/queueing, not proof of durable persistence or completion/);
  assert.match(s, /I'm sorry, I couldn't confirm that request/);
  assert.match(s, /Then wait for a NEW caller turn/);
  assert.match(s, /A correction after a receipt does not upgrade a request to a promise/);
});
test("correction acknowledgments are time-free and cannot promise calls or sends", () => {
  const s = section("Request records and receipts");
  assert.match(s, /Thanks\. I've received your callback request/);
  assert.match(s, /Thanks\. I've received your request for information/);
  assert.match(s, /Keep acknowledgments time-free/);
  assert.match(s, /Never say "I'll make sure Yoni calls", "Yoni will call"/);
  assert.match(s, /"I'll ensure it is sent", or "sent" without explicit proof/);
  assert.match(s, /Do not ask an anything-else question/);
  assert.match(s, /If asked what timing you heard, quote the caller's words, not unverified tool arguments/);
  assert.match(s, /A later cancellation supersedes earlier permission, including while a tool is pending/);
});
test("classification prefixes cannot replace full context or imply later correction capture", () => {
  const s = section("Request records and receipts");
  assert.match(s, /Every conversationSummary must preserve actual caller wording, request, restrictions, corrections, unanswered questions and unresolved details/);
  assert.match(s, /classification prefix, never as the entire summary/);
  assert.match(s, /An earlier successful request is not proof a later correction was captured/);
});
test("callback timing is literal, uncertain details are retained, and consent is separate", () => {
  const s = section("Callback request");
  assert.match(s, /only after a live caller explicitly requests[^\n]+or clearly accepts a single callback offer/);
  assert.match(s, /mere unavailability or a mentioned time does not qualify/);
  assert.match(s, /Copy the caller's requested timing words verbatim into callbackTime/);
  assert.match(s, /Do not convert words to digits, add an unspoken AM or PM, resolve a relative day into a date, or expand\/substitute a time zone/);
  assert.match(s, /retain earlier context in conversationSummary/);
  assert.match(s, /Do not ask for AM or PM solely to complete the field/);
  assert.match(s, /Use ASAP only if the caller actually requested or agreed to that timing/);
  assert.doesNotMatch(s, /What time should[^\n]+call|Is that two|Two in the afternoon/);
});
test("email request must execute with caller-confirmed address before acknowledgment", () => {
  const s = section("Information request");
  assert.match(s, /is an action request, not just a question about capability/);
  assert.match(s, /address the caller clearly supplied or already confirmed without asking them to repeat it/);
  assert.match(s, /A stored {{email}} alone is not confirmation/);
  assert.match(s, /Is {{email}} the best email for the information\?/);
  assert.match(s, /call information_requested[^\n]+BEFORE acknowledging receipt/);
  assert.match(s, /"I've noted it" or "I'll ensure" is not execution/);
  assert.match(s, /Information-only is not callback or live-transfer permission/);
  assert.match(s, /A repeated "email only" is not a second send request/);
});
test("self-initiated contact does not silently create future calls or an automatic hangup", () => {
  const s = section("Contact preferences and endings");
  assert.match(s, /DEFERRED CONTACT: caller said they will initiate future contact/);
  assert.match(s, /Do not create a callback or transfer/);
  assert.match(s, /This preference alone does not authorize ending; after recording, wait for a new turn unless a pending question needs an answer/);
});
test("live transfer consent, business approval, questions and revocation remain distinct", () => {
  const s = section("Live transfer request");
  assert.match(s, /onto this call right now\. Want me to try him\?/);
  assert.match(s, /Qualification answers are not live-transfer consent/);
  assert.match(s, /transferApproved: true and approvalStatus: accepted plus current caller consent/);
  assert.match(s, /A new restriction or unanswered question blocks the handoff even after approval/);
  assert.match(s, /Do not manually call transfer_to_number from the main conversation/);
  assert.match(s, /Failed transfer alone authorizes neither callback nor ending/);
  assert.match(s, /A question, thanks, silence, or correction is not callback permission/);
});
test("admin callbacks use shared permission rules and the return number has correct ownership", () => {
  const s = section("Live admins and wrong contacts");
  assert.match(s, /Busy, out, unavailable, or a time mentioned alone does not authorize a callback/);
  assert.match(s, /Yoni's direct number is 404-300-9526/);
  assert.match(s, /a number to reach Yoni; do not promise he will call/);
  assert.match(s, /A message-taking offer alone is not a goodbye/);
  assert.doesNotMatch(prompt, /If they give a time, ask for a callback, or say Yoni can call later|Yoni can call back at 404/);
});
test("recordings and hold never create human contact outcomes", () => {
  const s = section("Automated screening and hold");
  assert.match(s, /never authorize callback_requested, information_requested, not_interested or live_transfer_requested/);
  assert.match(s, /give this spoken response once/);
  assert.match(s, /Do not use skip_turn instead of answering that request/);
  assert.match(s, /For automated[^\n]+use skip_turn; do not pitch, qualify or end/);
  assert.match(s, /Never treat canned hold text as a new live greeting/);
});
test("voicemail keeps attempt limits, live-admin distinction and safe mismatch handling", () => {
  const s = section("Voicemail and recorded exits");
  assert.match(s, /only to a recording, not a live admin/);
  assert.match(s, /same\/similar first name, same last name/);
  assert.match(s, /say nothing further, disclose no lead\/property\/Yoni details/);
  assert.match(s, /Attempt 1:[^\n]+Let the greeting finish/);
  assert.match(s, /do not call voicemail_detection mid-sentence without a clear pause/);
  assert.match(s, /a beep is not required/);
  assert.match(s, /Attempt 2: leave no second voicemail/);
  assert.match(s, /For a matching voicemail greeting, use voicemail_detection after the greeting finishes/);
  assert.match(s, /empty voicemailMessage so this path ends without a second message/);
  assert.match(s, /Screening\/hold that is still trying to reach a person is not voicemail and is not an exit reason/);
});
