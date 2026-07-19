# Production SMS automation sources

These files mirror the Apps Script source deployed for the production SMS bot and the Tasker restore package used by the Android sender.

- `sms_chatbot.js` contains intent classification and response behavior.
- `zz_unified_post.js` contains webhook ingestion and pending-send receipt correlation.
- The Tasker V9 template captures phone, inbound text, bot reply, delay, handoff state, request ID, and message ID into task-local variables before waiting or sending. Every dynamic inbound and receipt field is URL-encoded before it enters the form body. It also preserves concurrent task execution and uses strictly sequential Tasker action IDs so restored capture actions are not skipped or reordered. Replace `__SMS_BOT_TOKEN__` with the production token before importing. The operator-delivered V9 file already contains the configured token and must not be committed.

The transport and cross-send fix is fully active only after both the Apps Script deployment and Tasker V9 restore are live.
