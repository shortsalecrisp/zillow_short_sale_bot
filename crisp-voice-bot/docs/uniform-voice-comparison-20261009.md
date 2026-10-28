# Uniform Conversion Comparison

Current revision is r8, verified at 2026-10-28T01:14:41.028Z (9:14:41 PM New York on October 27). Earlier releases below are historical and must not be pooled with r8. Both arms are verified identical except for TTS model.

## Approved Revision 8

Yoni approved the October 27 Lead Intelligence fixes. The shared prompt now treats a clear no to the exact needs question, including "No, I'm doing/handling everything," as a terminal service refusal: acknowledge once, record it, and use the guarded ending without another pitch, question, callback offer, transfer offer or Yoni offer. The backend release separately improves terminal voicemail, target/gatekeeper and no-assistant-turn diagnostics.

The guarded prompt-only publisher reread both exact r7 versions, prompt hashes and 50/50 allocation, then changed only `conversation_config.agent.prompt.prompt`. It preserved and reread workflow, phone bindings, platform settings and every other provider setting. No real call or metered session was started.

- Cohort: `maya_uniform_lead_conversion_20261027_r8`.
- Start: `2026-10-28T01:14:41.028Z`, after final two-arm provider readback.
- Control: branch `agtbrch_1101kpkhs3b4eg5v0q8ppq4hayks`, version `agtvrsn_1101m6015jj3e7r8xem6crdzbv08`, Flash v2.
- Test: branch `agtbrch_8401m3n23qvbewvtr9kfzx7nw4gv`, version `agtvrsn_8601m6015mjxf1f8m8gqqygf3n33`, v4 Turbo.
- Canonical prompt hash: `b4e68945fcec6952784d195235d80098502016dc523855d7292854142088038e`.
- Shared normalized configuration hash: `698e298abf335fc7a596a54f11634796c8fd5cf037ea934aefcd1ed495ab1a48`.
- Workflow hash: `31e9628299acf53fda9153f686a003bb1a25cf1a7c00a4bd9e89bcf9d2af0a35`.

Require the exact r8 branch/version pair, a matching final receipt, a genuine non-test positive duration, and a provider start at or after the r8 boundary. The 15 verified true-live conversations per arm minimum and preferred 100-genuine-call review restart at r8; all earlier cohorts remain historical.

## Approved Revision 7

Yoni approved the October 22 Lead Intelligence fixes. The shared prompt now treats an explicit statement that another negotiator, attorney, specialist or provider handles the file as terminal existing coverage rather than self-handling, closing without another value pitch, Yoni offer, callback or transfer. Backend contact evidence now accepts a contextual `It is`, `That is`, `It's mine` or `That's mine` only when it immediately answers the exact listing question; the same words after timing, needs or property-status questions do not authenticate the target.

The guarded prompt-only publisher reread both exact r6 versions, prompt hashes and 50/50 allocation, then changed only `conversation_config.agent.prompt.prompt`. It preserved and reread workflow, phone bindings, platform settings and every other provider setting. The provider had added one identical neutral `built_in_tools.identity_challenge: null` field to both immutable r6 responses; the r7 canonical baseline preserves that null field. No real call or metered session was started.

- Cohort: `maya_uniform_lead_conversion_20261023_r7`.
- Start: `2026-10-23T12:27:33.399Z`, after final two-arm provider readback.
- Control: branch `agtbrch_1101kpkhs3b4eg5v0q8ppq4hayks`, version `agtvrsn_5901m5mbp1pafv1skkvbhzh4cqfn`, Flash v2.
- Test: branch `agtbrch_8401m3n23qvbewvtr9kfzx7nw4gv`, version `agtvrsn_4301m5mbp4jgfn5ayjenb7yjjtby`, v4 Turbo.
- Canonical prompt hash: `0ef6a6f8ca9008cd8a5d21732d1df829be3808e33de2d7d9ce5534818a873629`.
- Shared normalized configuration hash: `18d8a334dbb09f6f6c196d84dac6331c0a4a45882aea4f89c591d820197f90a8`.
- Workflow hash: `31e9628299acf53fda9153f686a003bb1a25cf1a7c00a4bd9e89bcf9d2af0a35`.

Require the exact r7 branch/version pair, a matching final receipt, a genuine non-test positive duration, and a provider start at or after the r7 boundary. The 15 verified true-live conversations per arm minimum and preferred 100-genuine-call review restart at r7; all earlier cohorts remain historical.

### Approved Identity-Tool Response Reconciliation, October 23

Yoni approved a validator-only repair for `conversation_config.agent.prompt.built_in_tools.identity_challenge: null`. Investigation of the then-pinned r6 responses reproduced the original `d2fee79aa371e5fec4e7b5426f5932139f39d2ca48b21a6fbd7d8ac3f0ed6770` digest by removing ONLY that new null property from the otherwise identical bodies (before removal: `b11ec29044f9add6b0cb38bb87386f764f75f27452cc795bbbb97c2f27c25f4b`). During validation, the separately approved r7 prompt release above established its own exact two-arm boundary and null-shaped canonical baseline. This repair preserves that approved r7 release rather than reverting its prompt or replacing its hash.

For the current r8 baseline, the canonical validator reconstructs `identity_challenge: null` ONLY when `built_in_tools` is a non-array object without that own property. An explicit null remains null. Configured objects, arrays, booleans, numbers, strings, malformed or missing containers and every other new tool/default remain visible and fail the pinned digest. Both absent and explicit-null responses must reproduce the r8 `698e298abf335fc7a596a54f11634796c8fd5cf037ea934aefcd1ed495ab1a48` hash. Exact r8 versions, workflow, protected settings and 50/50 allocation remain mandatory. The future approved publisher imports this same canonical recipe; it is not invoked by ordinary reviews. Tests cover null/omitted equality, input immutability, configured/invalid/other-tool rejection and exactly three GET-only verification requests. Preserve all earlier narrow normalization rules below.

## October 23 Backend Recovery Repair (No Provider Release)

October 23 follow-on transport/classification repair: terminal failed starts no longer cause an immediate provider redial or same-window replacement. Exact matching receipt reconciliation precedes a later normal guarded queue slot; a missing receipt stays held. Structured per-request terminal resolution markers allow proven failed historical starts to stop blocking the original two-business-day overdue fallback. Network-blocked and country-whitelist failures are destination-review holds, not voicemail or genuine calls; unplaced expiry still requires fresh complete provider history. No country whitelist, spending limit, calling window, concurrency, voice, prompt, branch, version or r7 boundary is changed. A contextual "Oh, no, thank you" after a clarified help offer is a decline, not a hangup. Meaningful human engagement followed by silence/connection loss goes to contact review without a new cold retry, invented callback or transfer consent. Any approved historical correction preserves exact sent timestamps and requires current identity/attempt/preference checks and owning-cell readback.

Yoni approved repairing Denise row 6355's missing final receipt/next-slot retry and Clarissa row 5985's contextual decline. The shared backend now polls post-call outcomes for up to 61 minutes instead of stopping at 3.5 minutes, and reconstructs pending final-outcome recovery from durable sent/attempt fields after a process restart. Recovery requires an exact terminal non-test outbound provider receipt, matching agent, row, normalized phone, attempt and bounded start time; it rereads current phone/attempt, contact preferences, same-phone duplicates and inbound quiet state before a guarded direct write and owning-cell readback. Recovery never dials, runs a recursive refill, fabricates sent history, or replaces a newer disposition. Missing/initiated receipts remain held rather than authorizing a replacement call; the prior direct unconnected-call redial is removed. Only the normal queue can dispatch a later eligible attempt through unchanged suppression, capacity and listing-local window guards.

An unresolved extension menu is unavailable, not a delivered voicemail merely because it says "leave a message." A context-bound "No, that's okay" immediately answering the completed lender-help question is a decline even if the provider subsequently reports failure. It is not an opt-out, callback or transfer; qualified follow-up questions remain distinct. Retry scheduling now uses fresh guarded direct readback before any relay can overwrite a protected caller preference or existing K decision. Concurrent fallback/webhook/recovery processing is single-flight per conversation. These are common backend repairs, not a TTS effect, prompt publication, new provider version or r7 cohort reset. The exact r7 pins, hashes and boundary above remain unchanged.

## Approved Revision 6

Yoni approved the October 19 Lead Intelligence voice fixes. The shared prompt now gives the exact fee answer: $5,000, buyer-paid at closing only if the deal closes, with no fee to the agent or seller; it then waits without adding a Yoni pitch. It never tells a caller to move because of background noise. Backend review now accepts provider `agent` as the assistant role for a bare no after the help question, recognizes named office gatekeepers without confusing the expected agent, and lets a later live human override an earlier automated screen or hold artifact.

The guarded prompt-only publisher reread both pinned r5 branches, verified exact versions, prompt hashes and native 50/50 traffic, and changed only `conversation_config.agent.prompt.prompt`. It then reread both branches and verified the same workflow, phone bindings, platform settings and protected provider configuration. No real call or metered session was started.

- Cohort: `maya_uniform_lead_conversion_20261019_r6`.
- Start: `2026-10-20T01:13:01.709Z`, after the final provider uniformity readback.
- Control: branch `agtbrch_1101kpkhs3b4eg5v0q8ppq4hayks`, version `agtvrsn_8701m5bdwq8af46r1rpnzmb04b4g`, Flash v2.
- Test: branch `agtbrch_8401m3n23qvbewvtr9kfzx7nw4gv`, version `agtvrsn_6801m5bdwvh2fnkb9hfycj82vzj0`, v4 Turbo.
- Canonical prompt hash: `31c94d5c1728936a8004c7cd749ed5c8c4b563ede6d65a908146555a4228a64b`.
- Shared normalized configuration hash: `d2fee79aa371e5fec4e7b5426f5932139f39d2ca48b21a6fbd7d8ac3f0ed6770`.
- Workflow hash: `31e9628299acf53fda9153f686a003bb1a25cf1a7c00a4bd9e89bcf9d2af0a35`.

Require the exact r6 branch/version pair, a matching final receipt, a genuine non-test positive duration, and a provider start at or after the r6 boundary. The 15 verified true-live conversations per arm minimum and preferred 100-genuine-call review restart at r6; all earlier cohorts remain historical.

### Approved Disabled-Noise Response Reconciliation, October 20

Yoni approved investigating and repairing the shared hash discrepancy while preserving the comparison and calling schedule. The two pinned r6 versions now return two additional `conversation_config.turn` fields: `noisy_environment_detection: false` and `noisy_environment_message: "Uhh ... sorry, I'm picking up some noise in the background. Could you move somewhere quieter?"`. These fields were absent from the saved provider baseline. Reconstructing r6 from that baseline with the exact approved r6 prompt reproduces the original `d2fee79aa371e5fec4e7b5426f5932139f39d2ca48b21a6fbd7d8ac3f0ed6770` hash. The live normalized body differs only by this exact pair; without reconciliation its hash is `12eb75b1db88bf9601a2e65f9d5d6e5aa576a94adf80cfc46a162d1fae21f3b1` on both arms.

The read-only validator removes BOTH fields only when both are own properties, detection is strictly boolean `false`, and the message exactly equals the string above. It leaves absent fields absent and never normalizes an enabled, partial, null, numeric, string-valued or customized configuration. All other fields, branch/version pins, prompt/workflow hashes, protected configuration and 50/50 traffic checks remain binding. This disables nothing in production: detection was already false. No provider publication, voice change, prompt change or cohort reset is needed. The stored default message is not evidence it was spoken; the approved prompt continues to prohibit telling a caller to move because of noise.

The comparison publisher now imports the SAME canonical body/digest functions as the read-only validator so future approved release plans cannot use a divergent hash recipe. Ordinary reviews must still never invoke a publisher. Preserve the original r6 boundary, versions, canonical hashes, accumulated sample, calling windows and next-slot retry policy. Live proof and the exact baseline comparison are retained under `voice-hash-reconciliation-20261020/` in the task's analysis artifacts.

## October 19 Queue Capacity Repair

Yoni subsequently approved increasing capacity to avoid carrying 40 Eastern agents past the October 20 morning window. The follow-on `capacity-refill-v2` release raises the shared queue cap from two to four in both local windows, allows at most two provider starts in flight, reserves unpersisted starts against capacity, and queues completion-triggered refills behind an active batch instead of discarding the refill. Failed workers stop new dispatches and all workers settle before the single-flight lock releases. New queue starts require a 60-second margin before the unchanged local end boundary. No subscription upgrade or provider prompt/settings change is part of this backend release. The live Telnyx outbound profile was enabled without an explicit concurrency cap; the live Maya provider agent had no per-agent cap. Official ElevenAgents pricing documented at least four concurrent calls on every current tier. The scoped API key could not read subscription billing and the internal billing page did not render, so do not describe the account's exact plan concurrency as independently verified.

Offline capacity replays use 40 unique eligible synthetic Eastern agents, 45-second initiation, 45-second conversations, 60-second final-receipt delay, the normal minute check plus post-call refills, and the unchanged 09:15-09:45 window. These are simulated load/queue tests, not live call proof or a conversion experiment. The backend deployment receipt and live `/experiment-status.queueDispatch` identify the exact capacity cutover; record the tested completion times in the deployment verification. Many longer live conversations, provider stalls or new arrivals can still exhaust a finite window. Never hang up on a conversation, exceed the cap, duplicate attempts or extend the window to satisfy a numerical target. The October 20 verification must report real dispatched attempts and any residual backlog.

The two-slot sequential replay started 24 of 40 before the cutoff. Four slots with two starts in flight started all 40 by 09:39:45; a separate replay with two five-minute conversations started all 40 by 09:41:30. These times describe initiation, not completion of every conversation. The batch, reservation, cancellation-on-error, headroom and replay tests passed with the full 568-test suite and the TypeScript build. No synthetic call reached a provider or real phone.

Yoni approved repairing missed-window queue throughput. The `capacity-refill-v1` backend release checks capacity every 60 seconds during supported listing-local weekday windows, uses the existing two-call concurrency cap in both morning and afternoon, and refills directly through the guarded backend after final call processing instead of treating an Apps Script acknowledgement as dispatch proof. Idle maintenance retains the configured interval. Each start rechecks current eligibility and the provider route still enforces the local window. Dry-run inspections cannot join a dispatching run or emit circuit alerts.

This is a shared scheduler change, not a prompt or TTS release. Both r5 provider versions, policies, tools, voice/LLM/turn settings and 50/50 allocation remain unchanged; do not reset or pool cohorts. Stratify reachability and local-window results before/after the exact live deployment of the commit containing this section, using Render's deployment receipt and `/experiment-status.queueDispatch.version`. Deployment or dry-run eligibility is not evidence that any particular agent was called. Jennifer row 6270's missed first attempt remains scheduled for October 20 at 09:15 Eastern, not misrepresented as a second attempt. Existing next-slot retry, campaign/suppression, historical freeze and uncertain-start protections remain in force. Two concurrent calls are still a capacity ceiling, not a guarantee that an unbounded backlog will fit in one window.

## Approved Revision 5

### Approved Read-Only Validator Repair, October 19

The provider GET response changed without a branch/version or prompt/workflow release. Both pinned r5 versions now return `tts.clone_strength: 0` instead of `tts.pvc_strength: 0`, omit the former empty `agent.subagents: []`, and return `agent.subagent_config: {agents: [], self_subagent: null}`. Yoni approved reconciling these exact neutral response shapes. Under the prior normalization the shared digest was `d02de83773de340c5a8e67250067e05e204342e0e0ac3faa9d6ee9fb8efb99b2`; the repaired validator reconstructs the original approved digest `2bc5c0ae40c7fa64283dd9e630b2d32401a2763e94f5bee015d3be56c672e563` on both arms. Keep the original r5 boundary, versions, workflow digest, sample and 50/50 allocation. This is not a cohort reset or a reason to republish an agent.

The canonical executable review validator is `src/scripts/verifyElevenLabsComparison.mjs`. Run it with `ELEVENLABS_API_KEY` already supplied securely in the environment. It makes three GETs only: both exact live branches and complete branch allocation. It never imports or invokes a publisher. It checks exact agent/branch/version, TTS model, both pinned hashes, direct normalized body equality, protected platform/phone/WhatsApp/procedure equality and the complete exact 50/50 allocation. Unknown versions fail even when their bodies look identical. Ordinary reviews must not run a publisher, accept a new digest or alter the cohort.

Normalization deep-clones only `{conversation_config, workflow}` and removes only the TTS model ID and expanded GET-only root prompt tool schemas. Remove `agent.subagent_config` only for the exact two-key non-array object with empty `agents` and null `self_subagent`. Reconstruct `tts.pvc_strength: 0` only when an own `clone_strength` property is numeric zero and there is no own `pvc_strength`; delete only that exact zero-valued alias. If both aliases exist, neither is removed. Do not fill in a missing strength field or normalize nonzero, null or string values. Restore `agent.subagents: []` only when that own property is absent; explicit null, other types or configured arrays remain visible. Preserve every other field and array ordering, recursively sort object keys and hash JSON with SHA-256. These rules are validated against pinned versions, not permission to ignore new provider fields.

Focused tests prove the exact old/new response shapes agree without mutation and that configured subagents, non-neutral/ambiguous strengths, prompts, tool IDs, voices/speeds, LLM/turn settings, workflow order, new fields, unknown versions and traffic/protected-setting differences fail closed. Calling windows, next-slot retries, scheduler capacity, provider voice configuration and all contact/receipt safeguards are unchanged.

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

## Approved Overdue Voice Fallback, October 19

Blank K rows with an unplaced scheduled first or retry attempt expire two Monday-Friday business days after the end of their original listing-local call window. Automatic no-start rescheduling cannot restart that clock. A missed Friday morning window expires Tuesday at 09:45 local. Native Sheets date serials retain their full timestamp in the verified spreadsheet timezone rather than using date-only display formatting.

The shared backend checks final provider history, fresh owning cells, duplicate-phone rows, inbound quiet protection, and existing suppression/human preferences before writing K=N, clearing AD/AE/AF and appending CODEX_VOICE_EXPIRED_TO_MAILSHAKE_V1 in AP. Existing K codes and real AG/AH/AN/AO history are preserved. Accepted/uncertain starts, pending results, callbacks, handoffs, information requests, opt-outs and unresolved identity/timezone evidence hold release. Incomplete provider evidence fails closed. A terminal expiry cannot be reopened by automatic no-start recovery or a delayed generic result. Pre-existing cohort eligibility limits remain unchanged.

Provider history warms in the background; fallback maintenance never places a call and must not delay unrelated guarded dispatch. This unblocks the existing Mailshake route; K=N is not proof of enrollment or email delivery. Track expired-to-Mailshake exits separately from actual calls in queue reviews. Both TTS arms retain the same approved r5 prompt, settings, versions, hashes and split; no prompt release or cohort reset accompanies this shared scheduling change. Record the backend deployment cutover separately for reachability and volume comparisons. SMS, Apps Script, Tasker and AutoRemote are unchanged.

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
