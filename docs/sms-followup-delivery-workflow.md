# Current-Phone SMS Delivery Workflow

## Receipt Ownership Comes Before Follow-Up

An H/W marker belongs to the phone and initial receipt that produced it. A later
phone correction does not transfer that send to the replacement phone. A missing
current-phone receipt is not permission to send a reminder or retry an old video.
Rows 6510/6513 on October 30 exposed the operational gap: the survey correctly
blocked reminders, but the exception had no explicit initial-contact work queue.

## Existing Verifier Slots Own The Queue

At each existing verifier slot, before discovery, run the installed read-only
`scripts/review_followup_delivery.py` with a new private output, the shared
follow-up ledger, and the next actual existing verifier slot as `retry_after`.
Require `complete=true`, current capacities and stable end-index checks. A racing
or inaccessible survey is an open verification blocker, never zero missing work.

Review all due-unsent and pending episodes AND the ledger's `remediation_queue`.
The helper prints both current and carried corrected-phone review rows. A missing
row in a later capture never closes an unresolved item. Do not report SMS-complete
just because `due_unsent_rows` is empty while this queue contains open work.

## Corrected-Phone Exception Handling

1. Resolve the current stable listing, individual agent, address and verified
   direct phone; reread current native controls and all same-phone attempts.
   The survey's former-phone receipt requires a unique literal request/message,
   matching stable listing and the original W inside its actual send interval.
   This classification is evidence of an issue, not permission to contact anyone.
2. Any current-phone queued, claimed, started, uncertain, failed, superseded or
   sent attempt is reconcile-only. Do not manufacture a new request or clear a
   hold because the previous message did not look complete. Check initial queue,
   pending takeover, queued/recorded inbound, opt-out, manual conversation,
   source mutes, duplicate phone/listing and recipient-local hours.
3. When no current-phone attempt exists, this is initial-contact review, not
   follow-up recovery. Verify contact attribution and sending authority under the
   existing initial-SMS rules. Historical or ambiguous exceptions remain owner
   review required. The October 30 named recovery authorized only Carolyn and
   Heather's verified corrected phones, not a bulk campaign to older exceptions.
4. Only if authorized and safe, use the authenticated production initial-SMS
   route with a proper introduction. Stale H from a different phone can require
   `force_resend=true` to select the phone-bound initial ID; it does not grant
   `allow_prior_contact_resend` or bypass any other guard. Save an exact intent
   before one submission. Never clear H/W/I/X manually, contact the former number,
   call Tasker directly or reinterpret the former send as a current-phone send.
5. Queued/evaluated/HTTP success is not delivery. Reconcile the exact request to
   Tasker `status=sent`, correct phone/stable identity, and fresh H/W/L readback.
   AQ remains a separate full-verifier completion marker, not an SMS shortcut.
   Ambiguous submission means inspect that request, not resubmit blindly.

## Positive Closure And Escalation

A confirmed current-phone initial closes only the former missing-initial issue
as `resolved_initial_contact`; its former status/reason and receipt references
remain in history. This must not mark a reminder sent. The new receipt-bound
follow-up episode remains deferred until the unchanged six-business-hour calendar
and recipient-local send window allow it. New replies, opt-outs or takeover may
legitimately prevent a reminder entirely.

Every unresolved exception needs an exact reason, responsible verifier, next
action and next existing review time. Carry it forward before new discovery. If
it survives the next scheduled verifier slot without positive receipt closure or
a documented legitimate suppression, include its exact row in the existing
pending-SMS/blocker closeout and notify the owner once with the precise action
needed. Do not repeatedly alert unchanged historical blockers or label them sent.

No new monitor, timer, generation retry, video review, safety-policy exception or
spending increase is part of this process. Existing verifier schedules, strict
identity/name/reply/opt-out/takeover protections and all spending caps remain.
