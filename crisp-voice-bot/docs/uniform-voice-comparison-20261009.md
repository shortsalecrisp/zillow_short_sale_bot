# Uniform Conversion Comparison

Yoni authorized uniform prompts and workflows on October 9, 2026 and delegated the conversion-oriented prompt decision. Both provider arms were verified identical except for the TTS model at 2026-10-09T17:32:52.487Z (1:32:52 PM New York).

Flash v2 and v4 Turbo remain 50/50. Both use Eryn as Maya, gpt-4.1 and turn_v3. Scheduling, tools, phone routing and protected provider settings were retained. Finch/Finn remains retired.

## Shared Conversation

- Listen to the pickup, then identify Crisp and its purpose: "Hi, this is Maya with Crisp Short Sales. We help with short-sale lender paperwork. Is [street address] your listing?"
- After a fresh confirmation: "Would help with lender paperwork or calls be useful for this listing?"
- For a clear yes: "We help collect the documents and follow up with the lender, so you can stay focused on the sale."
- Answer any question first. Then offer once: "I can try to bring Yoni, our live short sale specialist, onto this call right now. Want me to try him?"
- Require explicit live-now consent and Yoni's acceptance before bridging. Later callbacks and information requests require their own consent. Respect rejection, email-only, wrong-listing corrections and self-initiated contact; never count those as live-transfer consent.

The canonical provider prompt is `elevenlabs-agent-prompt.md`. Both workflow listeners inherit that full policy. Both now have the short-listing recovery and callback-receipt wait states that the prompt references. Existing provider-verified ending guards remain, with guarded entry conditions extended to the new states and guard priority made explicit.

## Evidence And Gates

Control version: agtvrsn_7201m4gvk2q2fx3bm45pft78zhqz.
Test version: agtvrsn_3501m4gvk4hhf2gt2a26wbgw11e0.
Shared writable-body hash, excluding only TTS model: 8fab2d51d913c90ccc68d90a3223632bebd501a83ae08a140b801055d7fbd3f1.

Require a matching final provider receipt, exact branch/version, non-test positive-duration call and start after the new boundary. Earlier calls stay historical and do not count toward the clean comparison. An unknown or future version requires fresh review; the cohort is not silently extended. No winner before 15 verified true live-agent conversations per arm; prefer the dedicated review after 100 genuine calls. Compare lead quality and listing-local windows; separate screeners, gatekeepers, voicemail and no-answer. Primary outcome is verified positive handoffs per genuine call, with consent and completion evidence, not tool fires. Audio clarity and overlap still require playback. No outbound call, SMS or email was manually initiated for this release.

Provider writes are not atomic across branches. The first write stopped on provider-normalized edge ordering; its successful receipt was reconciled, the guarded ordering was made explicit, and both branches were then applied and read back exactly. Calls before the final shared boundary are excluded.
