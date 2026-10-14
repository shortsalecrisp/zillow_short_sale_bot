# Same-recording video follow-ups

## Release boundary

This integration does not enable agent outreach. Keep all three flags explicitly
false until the owner approves the final different-name phone tests:

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

HeyGen Avatar III creates only `Hey {first_name}!` narration in the existing
consented voice. None of that avatar's video frames enter the result. Fal endpoint
`fal-ai/kling-video/lipsync/audio-to-video` lip-syncs a short excerpt from the
approved second recording. Local composition changes only its mouth region and
preserves the entire original body. Longer greetings retime only the original
greeting window; no other recording is spliced in.

Required secrets: existing `HEYGEN_API_KEY`, new restricted `FAL_API_KEY`, and
existing GCS service account. Fal should allow only the exact lip-sync endpoint
and required request submission/read/cancellation plus usage read. Never print
keys, signed asset URLs, or service-account JSON in logs or receipts.

## Cost and duplicate controls

Reserve 10 cents per job within the approved $3/day and $75/month ledger. This is
a conservative software reservation, not a provider-enforced billing cap.
Only greetings at most three seconds proceed to the 2-5-second Fal price block.
Long, ambiguous, malformed or failed greetings use normal text instead. Provider
prices and actual owner-test debits must be checked before activation.

The private pipeline record is created before narration submission. A second
compare-and-swap claim precedes the Fal POST. Both uncertain outcomes are held,
never resubmitted automatically; `X-Fal-No-Retry: 1` disables Fal retries.
Accounting-only inspection after cancellation cannot buy the second stage.
Only independently verified actual charges may reduce reservations; estimates
and unknown charges retain the full reservation.

The same stable follow-up key freezes the first video or normal-text variant.
Current recipient/name/initial receipt and inbound activity are rechecked before
generation and sending. A late render cannot add another follow-up. Old full-
avatar jobs cannot be reused by the same-recording worker. The old owner sample
generation CLI is retired.

## Owner-test acceptance

The approved test scope is two names, Sue and Christopher, at most 20 cents from
existing credits, no generation retries or purchases, only `9542053205`.
Do not enable production flags to perform these tests.

Require all of the following before reporting the integration ready:

- Source and signed private-asset GET readback match the fixed hash.
- Both videos use the correct spoken name and are watched at normal speed for
  natural greeting cadence, lip-sync, and transition to the original body.
- Composition QA preserves original body frames/audio and non-mouth pixels
  before lossy encoding; dimensions, timeline and background checks pass.
- Branded pages, poster, captions and range playback work after deployment.
- Owner probe phone, name, nonce, frozen text and exact handset `sent` receipt
  agree. Queue acceptance is not proof of SMS sending.
- Provider debits and the final disabled agent flags are read back.
- The owner approves these final tests before agent activation.
