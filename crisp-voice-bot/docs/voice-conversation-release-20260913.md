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

## Configuration And Release Safety

`syncElevenLabsAgent.ts` starts from the current provider configuration. It changes
only the prompt and the narrowly defined conversation policies. It does not
reconstruct model, TTS, tool definitions, phone bindings or platform settings.

Default execution is a dry run. An apply requires all of:

- Reviewed current main-branch version and prompt SHA.
- Exact reviewed candidate body SHA from the dry run.
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
