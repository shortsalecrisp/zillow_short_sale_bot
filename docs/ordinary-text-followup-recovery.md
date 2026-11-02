# Receipt-pinned ordinary-text follow-up recovery

`ORDINARY_TEXT_FOLLOWUPS_ONLY=true` keeps the existing hourly follow-up pass
text-only. Set `VIDEO_FOLLOWUP_ENABLED`, `VIDEO_FOLLOWUP_GENERATION_ENABLED` and
`VIDEO_FOLLOWUP_LIVE_APPROVED` to false while personalized videos are paused.
No timer, cohort expansion, spending increase or safety override is introduced.

The authenticated `POST /internal/reconcile-text-followup-sms` accepts 1–20 rows
and mandatory `expected_episodes` containing exactly row, ten-digit phone,
stable_id and initial_sent_at from a fresh complete read-only survey. It uses
the existing scheduler lock and unchanged row, reply, opt-out, duplicate,
recipient-local-hours and six-business-hour checks. An incomplete survey,
changed episode, missing initial readback, or any existing attempt fails closed.

The pure receipt validator builds the existing episode identity with variant
`text` and an empty media job. Historical `followup-video-` ID spelling is
retained for cross-route deduplication; it does not make the message a video.
No media selection, preparation, provider, generation or job retry is called.
Never use the older video-aware recovery endpoint as a substitute.

Queued/claimed/uncertain attempts are reconcile-only. Do not reset a lease or
create a second request. Require exact current-phone outbox `status=sent` and
fresh Sheet I/X/L readback, not an evaluated/queued HTTP response. The standard
manual reminder endpoint may still defer; use this explicit pinned text route.
