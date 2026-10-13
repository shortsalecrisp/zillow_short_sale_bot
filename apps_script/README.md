# Production SMS automation sources

These files mirror the Apps Script source deployed for the production SMS bot and the Tasker restore package used by the Android sender.

- `sms_chatbot.js` contains intent classification and response behavior.
- `zz_unified_post.js` contains webhook routing and exact send-receipt correlation.
- `sms_outbox.js` contains the durable inbound queue, leased reply outbox, stale-reply suppression, retry handling, and watchdogs.
- The Tasker V10 template snapshots each inbound sender/body immediately, enqueues it without waiting for the AI response, and uses one sequential outbox dispatcher. SMS Success and SMS Failure profiles post an exact request/message/phone/reply/lease receipt. Replace `__SMS_BOT_TOKEN__` with the production token before importing. The operator-delivered V10 file already contains the configured token and must not be committed.
- The Tasker V12 template keeps the durable queue and one-minute reconciler but removes the permission-gated SMS Success/Failure event profiles. Its single dispatcher posts the correlated receipt inline after Android accepts the Send SMS action, and every added profile/task is explicitly assigned to the restored Tasker project.

V9 remains compatible during the rollout. The durable queue/outbox path becomes active after both the Apps Script deployment and complete Tasker V10 restore are live.

Tasker V11 keeps the V10 durable outbox and adds a one-minute reconciliation
profile for the monitored last SMS. This recovers an inbound SMS if Android
updated Tasker's `%SMSR*` variables but Tasker's real-time profile task was
rejected during a queue burst. Apps Script's phone/body dedupe makes the
real-time and reconciliation paths safe to run together.

## Personalized Scheduled Follow-up Contract

Personalized video uses the existing `enqueue_followup_sms` action and Tasker
text dispatcher, not the owner-approved `render-followup-*` exception. The
scheduler retains its existing timing and selects one SMS body, either a hosted
video link or the original text. No Tasker changes are required.

Feature requests add `followup_key` (64 lowercase hex characters),
`initial_sent_at` (an ISO timestamp with timezone), `video_variant` (`text` or
`video`), and `video_job_id` (an opaque alphanumeric/dash/underscore ID). They
require `crm_row`, `phone`, and a nonempty `stable_id`. IDs must be
`message_id=followup-video-<first 24 key characters>` and
`request_id=scheduler-<message_id>`. Video requests also require `first_name`
matching the current canonical Sheet value and a nonempty `video_job_id`.

Authenticated `check_video_followup_eligibility` accepts that same identity
without a message body or a completed job ID. It performs no writes and returns
`{ok, eligible, reason, crm_row}`. An unavailable read returns `ok=false`,
`eligible=false`, and `retryable=true`; it is not permission to render or send.
The read checks current initial-send identity, pending/processed inbound,
takeover, conversation notes, suppression, contact holds, and prior attempts.
The scheduler remains responsible for the due time and sending window.

The first accepted payload is frozen across text/video retries and feature
on/off transitions. A duplicate response names the authoritative existing
`request_id`, `message_id`, `video_variant`, and `pending_row`. Uncertain or
terminal failed attempts require reconciliation, not a fresh message. Feature
claim/send-start checks retain work on transient read failures, but supersede
known-ineligible sends. Missing handset receipts do not automatically resend a
feature payload. Standard scheduled I/X markers are written only on the existing
correlated receipt path and only while the original initial identity still
matches. An inbound arriving after the final guard can still race the handset.
