# Future-only video restart boundary

`VIDEO_FOLLOWUP_FUTURE_ONLY_AFTER` is an optional UTC timestamp boundary. When
configured, video preparation and selection require a literal valid initial
receipt whose send-start, initial timestamp and sent timestamp are all at or
after the boundary. Worker processing additionally requires the job creation
timestamp at or after the boundary. Invalid or missing identity/time evidence
fails closed. An invalid configured timestamp prevents video initialization.

The worker checks this boundary before source retirement, media audits, provider
accounting, terminal-capacity settlement and active-pointer deletion. The service
does not recreate pointers for retained jobs. Timestamp aliases cannot migrate
old jobs into the new generation population. Without this optional setting,
existing behavior is unchanged.

Deploy the guard while ordinary-text-only mode remains enabled. Before a
separately approved activation, inspect the budget ledger read-only: historical
in-flight entries still count against the existing two-slot limit. The boundary
does not clear or discount those reservations. If capacity is exhausted, obtain
specific authority for any accounting reconciliation; do not raise limits,
retry providers or reopen old jobs to make capacity.

At activation, set both this boundary and `VIDEO_FOLLOWUP_INITIALS_AFTER` to the
fresh activation timestamp and explicitly set `ORDINARY_TEXT_FOLLOWUPS_ONLY=false`
through the existing deployment path. Preserve all other configuration and the
startup speech-guard fail-closed behavior. Verify effective flags and boundaries
using authenticated GET `/internal/text-followup-policy`; no scheduler, worker,
send or generation invocation is needed for configuration verification.

The six-business-hour calendar, reply/opt-out/manual/takeover/duplicate checks,
receipt ownership, pronunciation/audio/media proof, ordinary-text fallback and
10-cent/$3-daily/$75-monthly caps remain unchanged. Configuration success is not
an organic video-send receipt. Keep the separate video review paused.
