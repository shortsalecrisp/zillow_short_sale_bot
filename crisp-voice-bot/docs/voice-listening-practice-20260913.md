# Voice Listening Practice And Wording Release

## Scope

Yoni approved the reviewed service-introduction and specific-answer improvements on September 13, 2026. This release changes the ElevenLabs prompt only. It does not apply the research policy package wholesale, change the first prerecorded message, voice, model, turn settings, provider tools, workflow, phone bindings, scheduler, CRM, SMS, Tasker, or AutoRemote.

The prompt now explains the service before a bare handling question, answers questions before selling, preserves the buyer-paid fee and closing condition, avoids unsupported scope/credential claims, and uses distinct short clarification answers. The two existing opener variants remain selected by the backend. The new prompt can add a service explanation before the bare handling variant; its stored `openerScript` remains the selected continuation, not a verbatim record of everything spoken. Compare this release separately by the actual ElevenLabs prompt version and delivered transcript. Do not pool it with the old opener baseline.

The following remain unresolved and are not certified by this release: business-result transfer routing, in-flight cancellation, information-recipient validation, durable follow-up capture and fulfillment wording, and operational outcome labeling. Existing protected transfer, voicemail, screening/admin and opt-out sections are pinned by tests. Preserving a section is not endorsing every current behavior in it.

## Findings Behind The Practice Round

The September 13 synthetic audio study observed unanswered early questions and incomplete clarification. It did not establish whether the problem occurred in generation, playout, recognition, turn detection, model response, or shutdown timing. The longer common research notice also confounded early-turn results. These were synthetic participants, not real realtor preferences or proven conversion effects.

A fresh main-branch read on September 13 confirmed a protected first message, `transcribe_on_disabled_interruptions=false`, normal turn eagerness, a 1.6-second initial wait, and a 1.5-second turn timeout. The main workflow node also removes `interruption` from its client events. These are diagnostic candidates, not established causes. The timeout is not a measurement of acoustic reply latency.

## Practice Design: Fixed Input First

Use a private research clone with no phone bindings, initiation/post-call webhooks, or external action tools. All handoffs, information requests and suppression must remain local mocks. No real agents, CRM writes, emails, messages, or phone calls. Verify isolation before every session and preserve the production baseline hashes.

Use consenting staff recordings, or explicitly scripted synthetic utterances, at fixed offsets in the bot's actual transmitted audio. Reuse the same audio bytes in every arm. Do not rely on an autonomous persona deciding when to interrupt or whether to ask the required question. Tag file hashes, voice/model/settings, identity/recording notice, and the actual sent/received sample ranges.

### Initial Diagnostic Matrix

Proposed only; no new metered sessions are authorized or launched by this document.

Six short fixtures, four arms, two repeats: 48 sessions maximum. Cap each at 45 seconds. Proposed total cap: 40,000 included credits and 45 combined provider minutes, including setup. Stop on either cap and reconcile receipts before continuing. Do not buy credits or use overages. Recheck billing and preserve at least 250,000 credits and 300 included minutes for production. These are ceilings, not predicted costs; unfinished cells remain unrun, not failures or successful tests.

Hold the approved prompt, exact voice ID/speed, model, notice, initial wait, tool mocks, and caller audio constant. Counterbalance order. Test each setting independently against the same control before testing a combination:

| Arm | Only Difference From Control |
| --- | --- |
| A | Current effective production settings, including workflow overrides |
| B | Enable transcription while interruptions are disabled |
| C | Allow interruption of the first message |
| D | Restore the `interruption` client event in the main conversation node |

Do not infer that C works if the workflow/client still cannot cancel playout. Inspect the effective configuration and actual events in each arm. Keep required notices consistent; do not make a mandatory notice interruptible just to shorten a test. If that notice must be protected, its boundary must be modeled separately before executing C.

| Fixture | Fixed Caller Audio And Timing | Required Behavior |
| --- | --- | --- |
| F1 | "Hello? Who is this?" beginning 300 ms after the bot's opening starts | Retain the question; give identity only, not a handling question |
| F2 | "What is this about?" halfway through the opening; follow with "I mean, what do you do?" after the next reply | Give the short service explanation; do not repeat name/reason or pitch a transfer |
| F3 | "I can't hear you. You're breaking up." during the opening; then "I can hear you now" | Check hearing; repeat only the missed short sentence after confirmation |
| F4 | "I handle it myself, but..." then an 800 ms pause and "what do you charge?" | Do not close or interrupt at the thinking pause; answer fee payer and closing condition |
| F5 | "I'm Michael, not Taylor. I said tomorrow at two Central, not now." immediately after a bot question | Retain corrected name and time; no NOW-consent inference |
| F6 | A brief cough, followed by a quieter question "Who pays you?" with neutral background noise | Distinguish noise from speech; answer the actual fee question and avoid inventing consent |

Record stimulus onset against actual transmitted bot playout, not generated-text timestamps. Each repeat must replay identical stimulus bytes. Stop the session early only on a documented safety condition or natural end, preserving why. Harness crashes or premature administrative time limits are exclusions, not agent rejections.

### Measurements And Proposed Gates

Measure separately: speech delivered to the bot, receiver recognition, finalized user turn, model start, first generated audio, first audible relevant reply, interruption event, and last bot audio after caller speech begins. Preserve missing events as unknown. Provider model/TTS latency alone is not the heard response gap.

- Target: every intelligible clarification receives an answer to that question on the next bot turn. Review both repeats, including negative cases.
- Target: no replay of the entire pitch and no handling question before the clarification is resolved.
- Target: p90 of stop-speaking delay below 500 ms for meaningful interruptions, and p90 of complete-user-speech to relevant audible reply below 2.5 seconds in the clean lab. These are proposed engineering targets, not provider guarantees or established results. Report per-case values as well as aggregates; a short filler is not a relevant reply.
- Hard gate: zero inferred consent from noise, zero incorrect NOW transfers, and zero ignored explicit opt-outs or known corrections. Check requested mock actions, not only the final friendly wording.
- Hard gate: never treat absent live ASR alone as proof the audio was unheard; reconcile final provider transcripts and actual transmitted samples.
- Human review: listen without transcripts first and state who called, why, and what the bot heard. Then rate listening, clarity and frustration. Models receiving ASR text cannot provide genuine acoustic voice preferences.

Two repeats screen for failure; they do not prove reliability. Before production turn-setting changes, expand the strongest candidate to at least 30 relevant interruption/clarification trials, including clean and noisy samples, and compare against control. Report counts and exceptions without claiming a conversion lift.

## Second Stage: Full Conversations

After the fixed-input diagnosis, test the winning settings combination on complete conversations with the existing skeptical, busy, self-handler, email-first and newcomer personas. Add deliberately scripted missing branches: repeated confusion, late questions during goodbye, corrected email spelling, a long caller thinking pause, background spoken "yes", Spanish preference, opt-out during speech, screening/hold/new-human pickup, and voicemail greeting/beep.

Test user-first versus a short interruptible opening separately after resolving interruption delivery. Normal versus patient turn eagerness, collection-specific patience, voice choice and speaking speed belong in separate comparisons. Do not make every pause longer, add filler, slow all speech, or change voice/model at the same time and attribute the result to listening.

Then run voluntary owner/staff microphone practice through the isolated browser agent, with explicit recording permission and mock tools. Staff feedback is not an independent realtor sample. Real-agent pilot calls require separate approval and normal consent, scheduling, quota and opt-out protections.

## Release Method And Verification

Use `src/scripts/syncElevenLabsPromptOnly.mjs`, not the full agent sync, for this approved copy release. It defaults to read-only, requires a reviewed version and prompt hash, patches one prompt leaf only, and verifies the default main-branch readback plus protected configuration hashes. It never creates a conversation. A timeout or mismatched readback blocks any claim of completion and must not be blindly retried.

Keep source changes in the voice service's Git branch with a `[skip render]` commit message. No Render restart, deployment, Apps Script push or clasp action is needed for an ElevenLabs prompt-only update. The existing backend still controls its unchanged variants and scheduled traffic. Preserve the prior prompt and the post-update receipt for rollback review.

The live prompt readback proves saved instructions, not better real call behavior. No new metered audio session is part of this wording release. Obtain approval of the next round's cap and recording inputs before executing it.

### Verified Prompt Release

On September 13 at 20:10:59 UTC, the effective default main branch returned version `agtvrsn_1801m2e69z64f8y8p0kq5bd0wdhv` with prompt SHA-256 `02e8a8f3ad089a2dcfeb356e008a81688f906b02b6f7ede370c2babe59d26687`. The former version was `agtvrsn_1201m1mzp10nfqbsb22vvh7zvxde`.

All protected hashes matched before and after: other conversation settings, workflow, platform settings, phone bindings, WhatsApp bindings and procedures. The patch changed only `conversation_config.agent.prompt.prompt`. No recording, phone call, paid test, CRM operation or deployment was initiated by that update.

Run the 46 focused offline checks from `crisp-voice-bot`:

```sh
node --test tests/elevenLabsPrompt.test.js tests/elevenLabsApprovedAnswers.test.mjs tests/elevenLabsPromptOnlySync.test.mjs
```

These are source-contract and mocked API checks, not simulated conversation or audio-performance results. The source-approved before prompt and detailed protected-hash receipt are retained in this task's `output/voice-wording-20260913/release/` directory.
