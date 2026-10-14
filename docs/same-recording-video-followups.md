# Same-recording video follow-ups

## Release boundary

The owner approved the final different-name samples and live activation on
October 14, 2026. This code integration does not itself enable agent outreach;
deployment and explicit activation are still separate release steps. Keep all
three flags false until those steps are deliberately performed and verified:

- `VIDEO_FOLLOWUP_ENABLED`
- `VIDEO_FOLLOWUP_GENERATION_ENABLED`
- `VIDEO_FOLLOWUP_LIVE_APPROVED`

Tasker, AutoRemote, six-business-hour scheduling, recipient qualification and the
existing SMS receipt protocol are unchanged. Delivery remains explanatory SMS
text plus an opaque branded `go.crispshortsales.com/v/...` link, not an MMS file.
Phone link previews cannot be guaranteed.

## Approved source and providers

The sole visual source is the approved second recording, SHA-256
`01efd28101e8b016d51d571cc87d38d13ea623389663bc7a88cf26cce39bd60b`.
`VIDEO_FOLLOWUP_SAME_TAKE_MASTER_KEY` identifies its immutable private GCS object.
Both generation and composition verify this hash.

Fal endpoint `fal-ai/chatterbox/text-to-speech/turbo` creates only
`Hey {first_name}!` narration, using the consented voice from the approved second
recording, fixed seed 4411 and temperature 0.5. The new worker does not use HeyGen
or an avatar preset, and has no old-provider fallback. Existing HeyGen credentials
need not be changed or deleted.

The exact reference WAV must be present in the same private object store:

`assets/voice-references/5fb066ad56f37021ce47d7ea5d182c2f9f097de4e1d21a3ffd22470f7a5f7387/real-recording.wav`

Its SHA-256 must be
`5fb066ad56f37021ce47d7ea5d182c2f9f097de4e1d21a3ffd22470f7a5f7387`.
The provider checks the reference and master hashes before narration and again
before lip-sync. Missing or mismatched assets fail closed.

The existing Fal endpoint `fal-ai/kling-video/lipsync/audio-to-video` lip-syncs a
short excerpt from that same recording. Local composition changes only its mouth
region and preserves the entire original body. Longer greetings retime only the
original greeting window; no other recording is spliced in.

Required secrets: separate restricted `FAL_TTS_API_KEY` for the exact Turbo
narration endpoint, existing Kling-only `FAL_API_KEY`, and the existing GCS service
account. Do not replace or broaden the Kling key. Limit each key to its exact
endpoint and required request submission/read/cancellation plus usage read where
available. Never print keys, signed asset URLs, or service-account JSON in logs or
receipts.

## Cost and duplicate controls

The observed Turbo minimum is 2 cents per narration request; Kling costs 7 cents
for the prepared 2-5-second input block, for an estimated total of 9 cents per
video. Check current provider pricing before paid tests or activation and verify
the resulting owner-test debits. Prices can change, and this estimate is not a
provider-enforced spending cap.

Reserve 10 cents per job within the approved $3/day and $75/month ledger. Only
greetings at most three seconds proceed, and the prepared Kling input must stay
within its 2-5-second block. The local acoustic guard screens raw silence,
clipping, timing and coarse pitch before normalization; it does not prove speaker
identity, exact name pronunciation or naturalness. Long, ambiguous, malformed or
failed greetings use the normal text follow-up at the existing scheduled time,
not another paid generation attempt.

The private pipeline record is created before narration submission. A second
compare-and-swap claim precedes the Fal POST. Both uncertain outcomes are held,
never resubmitted automatically; `X-Fal-No-Retry: 1` disables Fal retries.
Unknown outcomes keep their original request identity and reservation; they are
reconciled through safe status reads, not another paid submission. If no verified
ready video exists at the follow-up deadline, normal text is used. Accounting-only
inspection after cancellation cannot buy the second stage.
Only independently verified actual charges may reduce reservations; estimates
and unknown charges retain the full reservation.

The same stable follow-up key freezes the first video or normal-text variant.
Current recipient/name/initial receipt and inbound activity are rechecked before
generation and sending. A late render cannot add another follow-up. Old full-
avatar jobs and earlier v2 requests cannot be reused by the new
`face-to-text-same-take-turbo-v3` worker. Previously published links remain
viewable. The old owner sample generation CLI is retired.

## Owner-test acceptance

Test only the exact names, count and budget currently approved by the owner, from
existing credits, with no generation retries or purchases and only `9542053205`.
The earlier Sue/Christopher two-test allowance was not a renewable allowance;
reconcile any prior spend before a new paid attempt. Do not enable production
flags to perform owner tests, and obtain owner approval of the final videos
before enabling agent follow-ups.

Require all of the following before reporting the integration ready:

- Master and reference signed private-asset GET readbacks match their fixed hashes.
- Every approved test uses the correct spoken name and is watched at normal speed for
  natural greeting cadence, lip-sync, and transition to the original body.
- Composition QA preserves original body frames/audio and non-mouth pixels
  before lossy encoding; dimensions, timeline and background checks pass.
- Branded pages, poster, captions and range playback work after deployment.
- Owner probe phone, name, nonce, frozen text and exact handset `sent` receipt
  agree. Queue acceptance is not proof of SMS sending.
- Provider debits and the final disabled agent flags are read back.
- The owner approves these final tests before agent activation.
