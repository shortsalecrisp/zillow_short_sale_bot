# Uniform Conversion Comparison

Current revision is r5, verified at 2026-10-17T14:36:32.752Z (10:36:32 AM New York on October 17). Earlier releases below are historical and must not be pooled with r5. Both arms are verified identical except for TTS model.

## Approved Revision 5

Yoni approved the Lead Intelligence opening recovery refinement. After a skipped silence or placeholder, a new live "Go ahead" or "I'm listening" resumes the pending short listing check once; it does not repeat the full opener or restart an answered question. The release also preserves the shared current-call ending and authoritative terminal-outcome rules in the backend.

The guarded publisher reconciled a provider-added neutral `exit_slot: any` readback field after the first sequential branch write, refreshed both live branches, then completed and verified the two-arm release. Tools remained unchanged, native traffic remains 50/50, and no real call or metered session was initiated.

- Cohort: `maya_uniform_lead_conversion_20261017_r5`.
- Start: `2026-10-17T14:36:32.752Z`, after the final provider uniformity readback.
- Control: branch `agtbrch_1101kpkhs3b4eg5v0q8ppq4hayks`, version `agtvrsn_9501m554nxfaee2rps63c9zznh6k`, Flash v2.
- Test: branch `agtbrch_8401m3n23qvbewvtr9kfzx7nw4gv`, version `agtvrsn_1801m554p0e8edbshc6dkrjcf3rj`, v4 Turbo.
- Shared writable-body hash, excluding only TTS model and expanded GET-only root tool schemas: `2bc5c0ae40c7fa64283dd9e630b2d32401a2763e94f5bee015d3be56c672e563`.
- Workflow hash: `31e9628299acf53fda9153f686a003bb1a25cf1a7c00a4bd9e89bcf9d2af0a35`.

Require the exact r5 final branch and version, genuine non-test positive duration and provider start after the r5 boundary. The minimum 15 verified true-live conversations per arm and preferred 100-genuine-call review restart for r5. Previous cohorts remain historical controls only.

## Approved Revision 4

Yoni approved repairing the prompt mismatch and starting a clean comparison. The October 14 deferred-contact refinement had reached Flash only. The canonical updated prompt now also runs on v4: "I'll let you know when I'm ready" and "I'll let you know as soon as possible" mean caller-initiated contact, not owner callback consent. After recording that preference, do not restart the listing check, needs question, pitch or qualification. Existing consent and guarded-ending rules remain.

The coordinated prompt-only publisher verified both arms before and after applying the stale v4 prompt. Flash already matched the approved canonical prompt and was not rewritten. Voice settings, effective opening/recovery workflow, tools, LLM, turn model, platform settings, phone routing and native 50/50 traffic were preserved. No real call or metered session was initiated.

- Cohort: `maya_uniform_lead_conversion_20261016_r4`.
- Start: `2026-10-16T12:57:55.149Z`, after the final provider uniformity readback.
- Control: branch `agtbrch_1101kpkhs3b4eg5v0q8ppq4hayks`, version `agtvrsn_4601m4yhj7xsejw8wcm77xkcn08x`, Flash v2.
- Test: branch `agtbrch_8401m3n23qvbewvtr9kfzx7nw4gv`, version `agtvrsn_3401m52cmpgcecsbsfd89s046bsp`, v4 Turbo.
- Shared writable-body hash, excluding only TTS model and expanded GET-only root tool schemas: `7f8f349d8b4e16f3aa5263933707d2fe1f2fd464de88cfa9acdd37611d38cab4`.
- Workflow hash: `f2d992bbd8c2b0d082910bf13490ee8305a3080eee32a38b204aeef9f98a785b`.

Require exact final branch AND version, genuine non-test positive duration and provider start after this boundary. The 100-genuine-call preferred review and minimum 15 verified true-live conversations per arm restart for r4; at 200 calls without enough live conversations, report a reachability limitation, not a winner. Preserve the last verified uniform r3 checkpoint of 66 calls (Flash 40, v4 26) as historical evidence. Calls during the later unequal-prompt interval do not count toward either clean comparison.

Future prompt releases must use `src/scripts/syncElevenLabsComparisonPrompts.mjs` with reviewed versions/hashes, a saved dry-run plan and final two-arm verification. The single-arm prompt publisher now rejects updates when multiple branches carry live traffic. Never retry an ambiguous write without reconciling the live version and saved receipt. Provider updates are sequential, so no uniformity or new comparison boundary is claimed until both final readbacks and the unchanged 50/50 allocation are verified. The review automation remains read-only.

### Shared backend ending correction, October 16

Yoni approved repairing the shared ending validator without changing either provider arm. A bare "Cancel" during the listing conversation now permits the guarded current-call ending. If a callback, information request or transfer is in context, a bare cancellation remains ambiguous and cannot authorize a hangup; an explicit "Cancel this call" retains its current-call scope. List-removal wording now includes "caller list" as well as "call list" and "calling list". Recorded/quoted speech, hold, newer questions or revocations, authentication, and the permission-before-ending requirement remain guarded. A cancellation of this call does not create a future-contact opt-out.

The r4 provider boundary, pinned branch/version identities, prompt/workflow hashes and native 50/50 allocation remain unchanged. Record the shared backend deployment cutover separately when comparing ending and tool behavior before and after this repair; do not attribute that behavior change to either TTS model. The weekly review is pinned to r4 and must verify live configuration plus approval provenance before alleging drift from a stale r2 checkout.

## Approved Revision 3

Yoni approved the Lead Intelligence callback-direction and confirmed-state repair. Both provider arms now preserve an already-confirmed listing after a hearing problem and distinguish caller-initiated "Can I call you back?" from owner-directed "Can you call me back?" Prompt-only guarded writes preserved every protected provider setting and the 50/50 branch split.

Current control version: agtvrsn_7501m4ppqfkhfs28xarn71t6hw23.
Current test version: agtvrsn_7501m4ppqmhef29ttr75z3dbm6cq.
Current shared writable-body hash excluding only TTS model: e74c45f09142028e12f027334ae0a706b33fc984fed5016f2064c05ffcb0ac89.

Require the exact r3 final branch/version and provider start after the r3 boundary. The 15-true-live-conversations-per-arm and preferred 100-genuine-call gates restart for r3. Previous versions remain historical.

## Approved Revision 2

Yoni explicitly approved all three recommended refinements and requested immediate implementation:

- For a genuinely new listener after screening or hold who has not heard the identity: "Hi, Maya with Crisp Short Sales. Is [street address] your listing?" For the same listener repeating hello, retain only "Is [street address] your listing?" The main prompt, inherited opening-listener prompt and native recovery prompt use the same distinction; no full pitch is repeated.
- Live handoff offer: "Would you like me to see if Yoni, our short-sale specialist, can join this call now?" The human-only request path uses the same offer after "Totally fair." Explicit consent, current caller preferences and Yoni's acceptance remain required. No availability is implied.
- Cost answer: "Under our usual arrangement, there is no fee to you or the seller; the buyer pays a flat fee at closing. Yoni can confirm the terms for this file." Exact fees remain file-specific. Timing answer: "Timing depends on the lender and the file. Yoni can assess where this one stands." Do not present an unreviewed file as having a 60-to-90-day estimate.

Current control version: agtvrsn_6701m4gxgnk9esvs3rj67qxtzdnj.
Current test version: agtvrsn_2301m4gxgraheq7s7dvby2rmwbya.
Current shared writable-body hash excluding only TTS model: f9df6688ca48ec8f6fa1ef7316e883c4b94c5ace9e06b9c8e0788c9994bd31aa.

Require the exact r2 final branch/version and provider start after the r2 boundary. The same 15-true-live-conversations-per-arm and preferred 100-genuine-call gates apply to r2 only. Previous uniform versions are historical, not automatic members of this cohort. Playback/model adherence and conversion improvement remain unproven until genuine calls are reviewed.

## Original Uniform Release

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
