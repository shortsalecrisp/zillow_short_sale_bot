# Shared Pilot verifier write and receipt contract

Approved August 31, 2026. Implementation: `pilot_verifier_contract.py`, exposed by
the authenticated `POST /internal/pilot-verifier-contract` route. This is not a
source search, contact researcher, Sheet1 writer, audit runner, or SMS endpoint.
The same existing Codex automation bearer token authorizes it. Never print tokens.

## Pilot state/owner writes

Routine verifiers must use `action=update` for Pilot status, promotion, duplicate,
and owner fields. Direct connector writes to these fields are no longer the
routine path. Contact research, whole-Sheet phone checks, and any authorized
Sheet1 append still precede this call under the existing qualification policy.

Supply `automation_id` (the saved routine automation ID), `expected` (the exact
live `synthetic_zpid`, `listing_address`, `state`, `status`, `promotion_status`,
`import_ready`, plus any other cells being replaced), `fields` (only approved
Pilot bookkeeping fields), and `adjudication_reason`. Do not supply a row number
as identity. The service resolves the ID/address across Pilot, maps live headers,
checks the current owner, rereads immediately before writing, and verifies the
entire Pilot row plus Sheet1 owner afterward. Notes are appended, not truncated.

For a promoted owner, set `promotion_status=promoted`, `import_ready=promoted`, and
`matched_main_row` to the just-reread owner; keep `status=qualified`. A durable
source receipt and exact stable-ID/address owner are required. `duplicate_key`
must remain the normalized street/state key, never a Sheet1 row number.

For an existing-listing duplicate use `status=duplicate`,
`failure_reason=duplicate_existing_listing`,
`promotion_status=skipped_duplicate_listing`, `import_ready=skip`, and its exact
owner pointer. Include the verified owner's full individual name and phone in
`adjudication_reason`; the service checks the exact name/phone, address, and city
against the owner. Existing-agent/different-listing suppression must have no
listing-owner pointer. A hold must identify the exact missing/conflicting evidence.

Any failed readback is a handoff blocker: do not send. Follow the existing narrow
rollback rule only for the row this verifier just appended; never delete an older
owner. Reread after any shift. The API never edits or deletes Sheet1 itself.

## One terminal receipt per organic routine slot

Record the actual run start at the beginning of each routine run. After the final
readbacks, call the same endpoint with:

```json
{
  "action": "receipt",
  "automation_id": "lead-verifier-8-am",
  "run_date": "YYYY-MM-DD",
  "run_receipt_id": "a-unique-id-reused-on-retry",
  "run_kind": "organic_scheduled",
  "started_at": "actual timezone-aware ISO run start",
  "global_sms_blockers": []
}
```

`global_sms_blockers` is required; supply exact current Sheet1 blocker rows or an
empty array. Do not compute or submit `pipeline_complete`. The writer reads the
source terminal, same-day Pilot adjudications, durable evidence records, and
full-Sheet owner IDs/addresses/contact presence itself. Explicit contact holds
are reviewed; source staging alone is not. Missing/misplaced pointers fail closed.
Global SMS blockers remain in detail but do not downgrade a green Pilot cohort.

The writer accepts only the current New York date and a start within 30 minutes
of the saved routine slot. It never backfills or rewrites an existing slot. If
the response is uncertain, retry with the same run receipt ID once and reread the
owning receipt. A missing or failed receipt remains a visible blocker; do not
fall back to handwritten flags, a fabricated start time, or connector appends.
No historical receipt is repaired by this deployment.

For deployment verification only, `action=preview` with `automation_id`, current
`run_date`, and explicit `global_sms_blockers` returns the calculated receipt and
gaps with `writes=0`. A preview is not an organic verifier run, audit pass, send,
or a qualifying green date for the daily-to-weekly transition.

## Release checks

Run `python3 -m unittest tests.test_pilot_verifier_contract
tests.test_free_short_sale_source_pilot`. Deploy with source/audit startup catch-up
temporarily suppressed; restore its saved configuration afterward without a
second deployment. Keep normal schedules unchanged. Verify the live commit,
health, authenticated preview, unchanged receipt history and Sheet1 owners, and
startup logs. The next organic verifier receipt remains the behavioral proof.
