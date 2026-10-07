# Next-slot cold retry

Approved October 7, 2026. This supersedes the two-business-day cold retry
cadence described in `call-reach-opening-release-20261003.md`.

- An unanswered/retryable morning attempt schedules its only second attempt
  for the same weekday afternoon, at 15:00 listing-local time.
- An unanswered/retryable afternoon attempt schedules its only second attempt
  for the next weekday morning, at 09:15 listing-local time. Friday goes to Monday.
- Dispatch remains within 09:15-09:45 and 15:00-15:45 local windows; these are
  eligibility windows, not guarantees of a call at the exact slot start.
- Existing automatically marked `voice_call_2_due` / `call_eligible=yes` rows
  use the new cadence without a bulk historical Sheet rewrite. Explicit
  non-cadence dates, handoffs, callbacks, Mailshake exclusions, phone-scoped
  contact guards, and pre-monitor historical freezes remain protected.
- Two attempts maximum. Definitively failed/receipt-missing provider starts
  retain their separate one-business-day recovery and reconciliation guards.
- Render owns dispatch and direct Sheet writeback. The deprecated Apps Script
  source is kept consistent for offline parity tests; do not activate a second
  dispatcher or redeploy the unrelated SMS project for this change.

Verification is offline helper, queue, Sheet-writeback and legacy parity tests,
TypeScript build, and live commit / experiment-status readback. No forced queue
run, call or production data rewrite is part of release verification. A future
organic second-call receipt is separate evidence of actual dispatch.
