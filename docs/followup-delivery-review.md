# Follow-up delivery reconciliation

The video-generation outcome and SMS-delivery outcome are separate. A failed,
unfrozen `receipt-owned-selection-v1` video job may supply ordinary text under
its original receipt/job/request/message identity. It stays failed; selection
does not rewrite, regenerate, reserve money, or retry the old video. Cancelled,
uncertain, frozen or sent alias jobs remain closed. Apps remains authoritative
under lock at enqueue, claim and send_start, including all suppression checks.

## Read-only survey

Authenticated `GET /internal/review-followup-sms` reads the full bounded native
CRM/outbox ranges, including old pending rows and AQ-verified leads outside the
scheduler lookback. It checks capacities and the complete outbox status index
again at the end. Header drift fails closed; racing captures return
`complete=false` and no recoverable candidates. It never calls the scheduler,
media worker, recognizer, decoder or provider, and never sends or edits CRM.

Each episode has a receipt-based `episode_key`, current row/phone/stable listing,
initial time, status, reason and available request IDs. `due_unsent` means only
that a fresh locked eligibility check is required, not permission to bypass it.
Pending or uncertain attempts must be reconciled, never blindly retried.
`verified_sent` requires the matching sent outbox receipt and Sheet I/X/L
readback, including the timestamp interval. Non-atomic/unseen-history limits
remain explicit; a survey is not a global media/phase audit.

## Three-hour verifier

Run `scripts/review_followup_delivery.py --output NEW_RUN_JSON --ledger SHARED_LEDGER_JSON
--retry-after NEXT_EXISTING_VERIFIER_SLOT_ISO` at the start of each existing
verifier slot. It performs one authenticated GET, saves the raw survey and
atomically merges a private local carry-forward ledger under a file lock.
Incomplete captures cannot advance that ledger. Missing items are retained;
only positive sent-receipt/readback evidence closes an episode. No token is
saved in output and this script contains no sending operation.

Re-resolve candidates immediately before recovery. POST
`/internal/reconcile-followup-sms` in batches of at most 20 row pointers, with
`expected_episodes` containing each row, phone, stable_id and initial_sent_at.
The scheduler compares that reviewed identity and rechecks the current phone;
its weekday, six-business-hour, recipient-local-hours and Apps controls remain.
Bounded recovery neither prepares new video jobs nor invokes ready-audio audits.
An already verified video can still use the existing delivery path; otherwise
ordinary text is the safe fallback. No old video is retried.

The response contains per-row outcomes and is always `sent=false`: evaluated,
queued and claimed are not completion. Read the exact outbox and Sheet I/X/L,
then repeat the read-only survey to close the ledger. Carry precise blocked
reasons and a next-review slot. Replies, opt-outs, manual takeover, identity
conflicts and wrong-agent records require resolution, not forced sending.

This does not resume the separate video-review automation or modify spending
caps, speech acceptance profiles, Tasker, initial outreach, or the calendar.
