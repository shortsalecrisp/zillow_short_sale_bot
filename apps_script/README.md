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
