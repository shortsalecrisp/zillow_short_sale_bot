# Personalized video follow-up

## Release state

Implemented behind default-off flags. Not deployed, funded, or activated for
agents. No Tasker files, initial outreach copy, response prompts, follow-up
hours, scheduler cadence, or carrier transport were changed. Production API,
bucket, domain, and handset behavior still require live verification.

Yoni approved up to **$3/day and $75/month for generation**, with separate
approval required before a credit purchase. There is no authorization to buy a
subscription, create paid hosting resources, or bulk backfill earlier leads.

The preserved source scheduler is six **business** hours, with its existing
weekday/window/grace/cadence rules. This is not a change to four hours or six
elapsed hours. Compare deployed environment settings with source before release.

## What an agent receives

One existing scheduled follow-up SMS is replaced by:

> Hey Jim, just putting a face to my text: [branded video link] Happy to answer any questions here.

The video says:

> Hey Jim, it's Yoni Kutler with Crisp Short Sales. Just wanted to put a face to my text about your short sale listing. If you have any questions, text me back. Happy to help.

Only the canonical first name changes. Each follow-up identity gets a separate
render, not a shared clip by first name. Missing or uncertain names use the
unchanged normal text. No street address, email, or phone appears in the URL.
No suggestion that Yoni personally recorded each version or reviewed the file.

The mobile page uses an opaque 128-bit random ID, a face poster, tap-to-play,
captions, no login/form, and the disclosure: "Personalized with AI using my
recorded likeness and voice." It asks the agent to reply to the original SMS.
No automatic sound or external analytics. The link is a bearer link: anyone
who receives it can view that first-name video. It contains no listing details.

## Architecture and safety

- `bot_min._follow_up_pass` keeps all existing eligibility and timing checks.
  Before a not-yet-due eligible follow-up, it prepares a durable job. It never
  waits for a render before the scheduled send opportunity.
- `video_followup.py` stores job JSON and immutable media in a **private GCS
  bucket**, using generation-match compare-and-swap. Nothing depends on Render's
  ephemeral disk. Google service-account storage permission is separate from
  its existing Sheets permission. The code does not create buckets or ACLs.
- The web service's opt-in worker polls every minute. It never sends SMS.
  Paid submissions require a fresh authenticated Apps Script eligibility read.
  Two provider submissions may be outstanding across workers and restarts.
- The fixed approved avatar/voice, Avatar III engine, speed 1.05, 720p, and exact
  script are explicit in `video_followup_provider.py`. A 1280x720 source gets
  exactly `crop=960:540:304:180,setsar=1`, H.264 output and unchanged AAC audio.
  Other source dimensions fail to text fallback instead of silently reframing.
  Original watermark pixels are retained. Confirm production rights and
  watermark requirements against the actual API account before activation.
- Provider media URLs are validated, downloaded with size/time limits, and
  copied to private durable storage. Expiring HeyGen URLs are not sent to agents.
  Native SRT captions become VTT; if unavailable, phrase timing is approximate.
- The logical identity hashes stable listing ID, normalized phone, and the
  confirmed initial-send timestamp. It excludes row position, video/text choice,
  script version and text hash. Row moves do not create another follow-up.
- The due-time selection uses one `enqueue_followup_sms` request through the
  existing scheduled lane. Apps Script freezes the first accepted payload under
  its script lock. Failed storage uses the original text with the **same ID**.
  Late video completion cannot add another SMS. Feature on/off transitions also
  reconcile existing pending attempts rather than generating another message.
- Current phone, initial timestamp, canonical video name, conversation notes,
  takeover, replies including known unprocessed inbound, and existing sends are
  checked before generation, enqueue, claim and send-start as applicable.
  Unknown transient eligibility leaves work retryable, not silently superseded.
- Standard I/X follow-up markers remain on the correlated Tasker receipt path.
  Queue acceptance and a completed render do not mean the SMS was sent.
  A missing send receipt does not automatically repeat a feature SMS.

This cannot cancel an SMS already handed to Android, or detect an inbound SMS
that never reached a monitored backend. No absolute exactly-once carrier or
zero-race guarantee is made. Existing manual takeover and receipt reconciliation
remain necessary.

## Spending

The global CAS ledger reserves 50 cents before each submission, at most two
inflight. On a confirmed short Avatar III result, it settles to the ceiling of
duration in seconds in cents, using the verified $0.60/minute rate. Daily limits
use America/New_York. Failed requests retain the conservative reservation;
unknown submissions are **never automatically resubmitted** and retain capacity
until reconciled. This can deliberately reduce video volume; normal text still
goes through. A timeout is not proof that HeyGen did not charge.

These are conservative application admission limits at the verified rate, not
a provider billing guarantee. Configure a provider-side spending limit where
available. Reverify price before activation; tax, hosting and provider price
changes are not included. Never raise the hard-coded approved caps or engine
without new approval. An unexpected duration over 30 seconds remains reserved
and must be reviewed rather than assumed cheap.

## Configuration and release order

1. Keep `VIDEO_FOLLOWUP_ENABLED`, `VIDEO_FOLLOWUP_GENERATION_ENABLED`, and
   `VIDEO_FOLLOWUP_LIVE_APPROVED` false. Run the offline readiness script.
2. Compare current live Apps Script and Render revisions/configuration. Merge
   only this scoped patch; do not overwrite unrelated production changes.
3. Configure a private bucket and narrowly scoped service-account access after
   confirming hosting costs. Do not make the bucket public. Configure the
   approved branded HTTPS origin (intended: `https://go.crispshortsales.com`) on
   the web service, without overwriting an existing site at that hostname.
   The feature does not manage DNS or purchase a domain.
4. Configure the programmatic `HEYGEN_API_KEY` securely, verify funded API
   access and current price, and obtain approval before any credit purchase.
   A connected HeyGen preview account is not evidence of unattended API funding.
5. Deploy/test the authenticated Apps Script eligibility/outbox contract first,
   then the Python code with flags still off. No Tasker restore is required.
6. Configure only the owner test phone in `VIDEO_FOLLOWUP_TEST_PHONES`; keep
   `VIDEO_FOLLOWUP_LIVE_APPROVED=false`. Set an explicit timezone-aware
   `VIDEO_FOLLOWUP_INITIALS_AFTER` cutoff, not a historical mass-backfill date.
   Both scheduler and web worker need the same bucket/configuration. Ensure
   the scheduler which actually runs `_follow_up_pass` receives these settings.
7. Enable the feature/generation only for the isolated owner test. Verify one
   ready-video case and one delayed-video text-fallback case through the real
   scheduled outbox, using an owner test identity, not a real agent. Check
   correlated receipt, correct SMS body/name, mobile playback/audio/captions,
   reply cancellation, and no later second send. Also test a temporary Sheets
   read failure and a webhook restart.
8. Only after owner confirmation and explicit release approval, enable
   `VIDEO_FOLLOWUP_LIVE_APPROVED` with a fresh activation cutoff and a small
   controlled batch. Verify actual sent receipts and follow-up markers, then
   widen. Do not replay past leads or change normal follow-up qualification.

Use `.env.example` for the full setting names. The default limits already match
the approved $3/day and $75/month. The code never purchases credits.

## Rollback and operations

Disable generation first to stop new costs. Disable the feature to restore the
original text selection. Keep this Apps Script dedupe/receipt patch deployed
until all feature sends are reconciled; reverting it prematurely would remove
the cross-mode duplicate guard. Already queued sends must be reviewed/cancelled
in the existing outbox, not blindly replayed. Keep hosting online for links
already sent; disabling new generation must not break existing playback.

Job states distinguish prepared, submitting, submitted, processing, ready,
frozen_text, frozen_video, cancelled, failed and uncertain. `active/` indexes
are paginated and repaired after submission/cancellation races. Uncertain
provider IDs must be reconciled with HeyGen using the stored callback ID before
releasing their budget/capacity reservation. There is no automatic paid retry.
Logs contain job prefixes and error types rather than API keys/provider URLs.

The storage lifecycle must keep job/dedupe metadata at least as long as any
replay/reconciliation window; do not delete it simply because a video is old.
Choose and approve a media-retention policy separately. No lifecycle deletion
policy is created by this patch.

## Verification

Use the isolated test environment, never import production credentials for
unit tests. New tests block real network access or use explicit HTTP mocks.

```sh
python -m pytest tests/test_video_followup.py tests/test_video_followup_provider.py tests/test_video_followup_player.py tests/test_video_followup_storage.py tests/test_video_followup_scheduler.py -q -p no:cacheprovider
python -m pytest tests/test_followup_phone_refresh.py tests/test_voice_followup_template.py tests/test_sms_outbox_source.py -q -p no:cacheprovider
node --test tests/video_followup_outbox.test.mjs tests/crm_followup_outbox.test.mjs
node --test tests/owner_video_probe.test.mjs
python scripts/video_followup_readiness.py
git diff --check
```

Tests cover same-identity fallback, name changes, cancelled media, concurrent
freezes, budget rollover/capacity, restart/unknown-submit behavior, no extra
paid retry, late completion, historical cutoff, feature rollback, raw inbound,
transient claim failures, standard I/X receipts, player ranges/security, and
the exact local crop/audio preservation. Unit tests are not handset proof.

### Local verification receipt, October 13, 2026

- 326 Python video/core/provider/player/storage/scheduler/template/outbox-source
  checks passed together, and all 30 existing phone-refresh/timing tests passed.
- 34 focused JavaScript video/CRM outbox checks passed.
- The production media-preparation function was run offline against the actual
  approved Hey Jim source. Its output is byte-for-byte identical to the approved
  reframed MP4: 960x540, 9.26 seconds, 853760 bytes. No provider request or render
  charge was involved.
- Independent code reviews found and fixed configuration-isolation, name-change,
  pagination, day/month budget, and cancellation-accounting races.
- `git diff --check` passed. Tasker, SMS transport/provider modules, existing
  scheduler worker, Render blueprint, and chatbot response source are unchanged.
- The broader JavaScript response-policy suite has two pre-existing failures
  (repeated-answer handoff expectations in automated-loop and final-opener tests),
  reproduced against the untouched base outbox. Those unrelated responses were
  not changed in this feature.
- Offline configuration check reports feature/generation/live flags false.
  No usable local unattended HeyGen key or video bucket/origin was found in the
  inspected configuration. Current connected HeyGen profile reports Free, with
  no wallet or usage-based billing shown. This does not verify API funding.
- No Apps Script or Render deployment, production Sheet mutation, agent SMS,
  owner SMS, domain setup, bucket creation, or credit purchase occurred.

Sources checked October 13, 2026:
- https://developers.heygen.com/reference/create-video
- https://developers.heygen.com/reference/get-video
- https://help.heygen.com/en/articles/10060327-heygen-api-pricing-explained
- https://docs.cloud.google.com/storage/docs/request-preconditions

### Live connection preflight, October 13, 2026

- User confirmed Render workspace `tea-d0jt5p7fte5s7385mieg` (Yoni's workspace).
  Existing web service `srv-d0jtpfbuibrs7391jtu0` is Standard Python, running
  `bash ./start.sh`; its live revision and remote `main` are both `e1b4a2a`.
  The feature commit `5475de3` is not deployed. `main` auto-deploys on push.
- The current Render environment visibly has `FOLLOW_UP_HOURS=6` and
  `FOLLOWUP_INTERVAL_MIN=10`; preserve both. The second variable is legacy:
  the deployed source does not read it, and `_next_scheduler_run` still selects
  top-of-hour slots. Do not mistake its presence for a 10-minute effective poll
  interval. Neither value was edited during verification.
- Primary Apps Script deployment is currently immutable version 308. A fresh
  read-only clone is `/private/tmp/crisp-video-live-v308-3LzLG0`. Historical v307
  staging is not the release baseline. Preserve all ten live project files and
  existing properties when merging the scoped patch.
- `go.crispshortsales.com` is already verified on Vercel project
  `prj_J0rNUQKJwckcQAfhRPhatuAScUsE`, team `team_LqDcECeX4vMkeIFF18LB4fCW`.
  It has valid HTTPS and currently redirects the root to `/login`. Do not move
  its DNS or replace the marketing application. Use a narrow, host-scoped
  project rewrite for `/v/<32 lowercase hex>` and the three known asset paths
  to the existing Render service, preserving all other application routes.
  No project-level routes were present at preflight. No rewrite is live yet.
- HeyGen's signed-in API onboarding requires a minimum $5 top-up before the
  first key can be generated. User approved exactly $5 one-time, no auto-reload
  or subscription. Checkout was opened but payment is NOT confirmed; Stripe
  requires the user's verification. No API key is configured yet.
- Google Cloud account `ygkutler@gmail.com`, project `zillowbot-459722`, has no
  bucket and billing is disabled. Its only accessible billing account is closed.
  User approved private storage setup with a $5/month operating budget, aware
  usage billing is not a hard cap. Console reauthentication and billing setup
  remain required. No bucket or IAM changes have been made.
- The sole approved SMS test destination is `9542053205`. Do not use a real
  agent CRM row as the test fixture. A receipt-backed owner-only probe must
  update only its outbox record; it must not update agent I/X markers or trigger
  chatbot handling. A transport probe is not proof of real-agent scheduling.
- No live configuration, DNS, hosting routing, outreach activation, Tasker
  restore, owner SMS, or agent SMS was changed by this preflight.

### Isolated owner transport probe

The local Apps Script patch adds authenticated `enqueue_owner_video_probe` and
`get_owner_video_probe_status` actions. Use one new random 32-character lowercase
hex `probe_nonce` per explicitly authorized test; preserve that nonce after an
unknown HTTP outcome instead of creating another send. Enqueue accepts only
`phone=9542053205` (or its exact country-code form) and a
`video_url=https://go.crispshortsales.com/v/<32 lowercase hex>` URL. The server
constructs `Owner video test: <video_url>`; arbitrary text and CRM identifiers
are rejected. Status takes only action, token, phone and probe_nonce.

The probe expires 30 minutes after its first enqueue and freezes that first
payload/expiry in every state. It uses the unchanged Tasker claim, send-start
and receipt protocol. Only an exact started send with matching lease,
request/message IDs, destination and body marks the outbox sent, including a
genuine late receipt after the outcome became uncertain. It never writes CRM,
I/X, or bot conversation history, and never automatically resends. An uncertain
receipt requires handset inspection, not a new nonce. Do not reply to the test
SMS: inbound owner messages still follow ordinary inbound handling; confirm
receipt and playback in this chat instead.

This probe and its tests are local preparation, not deployed or sent. It proves
handset transport/playback only after a real receipt and owner confirmation;
the scheduler eligibility, cancellation and fallback tests are separate.
