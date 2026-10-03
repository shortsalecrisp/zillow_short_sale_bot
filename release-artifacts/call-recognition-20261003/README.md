# Approved Call Recognition Follow-up

The production Apps Script immutable deployment, not the older local Apps Script
copy, is the authoritative release base. The reviewed change is limited to the
existing follow-up invitation and its context-bound channel-choice response.
It does not create another SMS campaign, schedule Maya appointments, change the
transport, or change outbound idempotency.

`apps-script-live291.patch` applies only the response helper and its entry point
to production version 291 `sms_chatbot` source. Expected original SHA-256:
`d6be6bbfa7c1127ce3ccf39e95b9b184a47f74a4d11f6aa0a76b56fb4030a5fc`.
Expected patched SHA-256:
`9086b40d70e87531ccd291e74703584cb5341bf7db7809d38bdb7c62e18a43e8`.

Before deployment, re-read HEAD and immutable source and require equality; abort
on drift. Preserve every other file except independently approved scheduler and
timezone changes. Do not run a broad clasp push from the repository's older copy.
Production credentials and full remote-source snapshots are intentionally absent.

Validate the patched live-source candidate without accessing production:

```bash
APPS_SCRIPT_CANDIDATE_PATH=/absolute/path/to/patched-sms_chatbot.js node --test release-artifacts/call-recognition-20261003/voice_followup_choice.test.mjs
python3 tests/test_voice_followup_template.py
```

Email requests retain the existing owner-approval path. Requested human callbacks
preserve the preferred date, time, and timezone verbatim for Yoni to confirm; they
are not booked appointments. Written-only and no-automated-call preferences
suppress cold calls; explicit opt-outs remain terminal. Vague agreement is not
treated as callback consent. All new eligible follow-ups receive this version,
so before/after findings must not be presented as a randomized invitation test.
