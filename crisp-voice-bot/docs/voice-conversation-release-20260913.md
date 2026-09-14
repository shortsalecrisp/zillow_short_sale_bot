# Voice Conversation Release

Scope: approved voice-only conversation improvements. No SMS, Tasker, AutoRemote,
calling-window, voice-rotation, billing-tier or historical CRM changes.

## Script Contract

- Answer the caller's actual question, then wait. Do not attach qualification or
  a transfer pitch to a clarification answer.
- Retain clear names and timing details; ask only for the missing part.
- Check hearing before repeating a missed sentence. Do not claim an audio repair.
- Distinguish a request to end this call from a future-contact opt-out. A standalone
  STOP remains an opt-out. Do not erase genuine earlier interest on a normal ending.
- Require explicit live-transfer-now consent and a real accepted availability result.
  New questions and changed preferences take priority over a stale approval.
- Treat callback and information tools as requests, not confirmed appointments or
  delivered messages unless the tool explicitly confirms that fact.
- Keep the same policy in model-facing tool descriptions. Callback success is not
  permission to hang up, spoken hold messages are a reason to wait, and Yoni being
  unavailable is not permission to invent an ASAP callback.
- Preserve buyer-paid-fee disclosure, truthful AI identity, recording gates,
  voicemail behavior, opt-out protection and approved business facts.

The complete editable script is `elevenlabs-agent-prompt.md`.

## Outcome Handling

The live end/rejection tool uses attributable caller words from the matching
conversation and attempt. Its bounded provider read has no retry. Missing or
unavailable evidence goes to `contact_request_review`, not an invented opt-out,
rejection or callback. A failed write is reported as unconfirmed, with a neutral
goodbye that does not promise suppression or future contact.

`call_ended_by_request` means end this conversation only. A standalone STOP or an
explicit future-contact opt-out remains `do_not_call`. A late generic hangup or
provider-failure result cannot downgrade a protected terminal contact outcome or
reopen its automatic cadence. Same-row direct Sheets writes are serialized within
one service process; this is not a distributed transaction across instances.

A failed transfer cannot automatically become an ASAP callback. Callback consent
must come from an explicit caller request or a clear answer to a single callback
offer, with later corrections or withdrawal respected. A genuinely interested
caller without callback permission can be retained as
`interested_followup_review` with lead status Y, without sending a new callback
notification or authorizing an automated retry. These changes do not rewrite
historical CRM records or prove every unrelated callback route is consent-gated.

For an ordinary callback tool result, final attributable caller timing can now
correct the earlier model-supplied Sheet time and its displayed outcome. The
callback result and request flag are reconciled too; existing callback outcome
handling clears automated retry eligibility, but does not book the requested call.
Later incidental meeting, showing or closing times cannot replace the request.
This does not convert words, infer AM/PM or resolve a date. Missing caller timing
leaves the earlier value untouched; it is
not evidence that the earlier value was independently verified. Active provider
reads in the private tests were empty, so no reliable live-transcript validation
of the initial callback tool argument is claimed.

## Configuration And Release Safety

`syncElevenLabsAgent.ts` starts from the current provider configuration. It changes
only the prompt and the narrowly defined conversation policies. It does not
reconstruct model, TTS, phone bindings or platform settings. The end-call and
skip-turn descriptions are aligned with the prompt without changing their tool
parameters. Three newly cloned callback/contact-outcome/information tools replace only conflicting
descriptions; the existing shared resources and webhook transport remain unchanged.

Default execution is a dry run. An apply requires all of:

- Reviewed current main-branch version and prompt SHA.
- Exact reviewed candidate body SHA from the dry run.
- A reviewed, freshly verified replacement map if cloned contact tools are used.
- Explicit `--listen-first` or `--keep-first-message` selection.
- A fresh pre-write reread and exact post-write branch/default readbacks.

The receipt directory contains before/candidate/after configurations with expanded
credential-bearing tool schemas omitted and sensitive fields redacted. Do not
blindly apply an old receipt after another live edit. Restore only the reviewed
changed fields against a fresh provider readback.

## Validation Boundaries

Static prompt tests check instructions, not model adherence. Unit tests check
outcome classification, no-retry scheduling, concurrency and release guards.
Private audio sessions use synthetic PCM callers and local client-tool mocks;
they cannot prove actual telephone audio, human preference, completed transfers,
email delivery or conversion improvement. Keep those claims separate.

Startup selection must follow the private paired comparison. Both arms use the
same revised script, voice and settings; only `first_message` differs. No production
configuration change is justified merely because the candidate can be created.

The first 18 sessions are retained unchanged, including failures. The addendum
uses the original audio fixtures and a separately frozen revised package. It tests
the combined script, tool-description and acknowledgment changes, not each change
as an isolated causal effect. Its cap includes the original usage; any unrun cell
at the remaining-headroom gate stays unobserved. Audio tests use Eryn at the existing
0.95 speed and do not establish a preferred voice or calling time.

The five executed addendum sessions were not approved for production. Callback
timing gained an unconfirmed PM, and an email-only case claimed handling without
calling its capture tool. A separate six-cell repair round retains these failures
and archives the tested source before changes. Its own additional limits are
15,000 included credits and 12 provider minutes, not a retroactive increase to the
closed original-plus-addendum pool. Root selected these conservative numeric caps
under the user's general authorization for additional private tests.
