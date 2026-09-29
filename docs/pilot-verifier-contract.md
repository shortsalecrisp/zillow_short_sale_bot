# Shared Pilot verifier write and receipt contract

Approved August 31, 2026 and extended September 2, 2026. Implementation:
`pilot_verifier_contract.py`, exposed by the authenticated
`POST /internal/pilot-verifier-contract` route. This is not a source search,
contact researcher, audit runner, or SMS endpoint. It owns only guarded Pilot
bookkeeping, deterministic receipts, and the atomic Sheet1 owner promotion below.
The same existing Codex automation bearer token authorizes it. Never print tokens.

## Atomic Sheet1 owner promotion

Routine Pilot promotions must use `action=promote_owner`; direct connector
`append`, `appendCells`, table append, and caller-selected Sheet1 rows are not the
routine path. Supply:

- the saved routine `automation_id`;
- `expected` with the exact live `synthetic_zpid`, `listing_address`, `city`,
  `state`, `status=qualified`, `promotion_status=verifier_held`, and
  `import_ready=verify`;
- `owner` with exactly `agent_name`, `last_name`, `phone`, `email`,
  `phone_confidence`, `contact_verification_note`, and `email_confidence`; and
- a precise `adjudication_reason`.

The service rereads the Pilot row and the whole current Sheet1, rejects existing
stable-ID/address or phone ownership, resolves the first row after the highest
real owner below the legacy artifact floor at row 9000, confirms that exact row
is empty, and writes only the complete A:G owner fields plus Y:AB
confidence/evidence/stable-ID fields. The caller cannot provide a row number,
address, or stable ID for the Sheet1 write.

After writing, the service re-resolves the owner by stable ID plus normalized
address/unit, confirms name/phone/email, updates the Pilot promotion and owner
pointer, and rereads both surfaces again. A failed readback blocks outreach. If
the newly written owner is still uniquely proven at the intended row, the
service deletes only that just-created row before returning the failure; it
never deletes an older owner or guesses after a shift. A successful response
returns `sheet1_writes=1`, `readback=true`, and `sends=0`. SMS remains a separate
authorized production-endpoint step after this response.

## Pilot state/owner writes

Routine verifiers must use `action=update` for Pilot status, promotion, duplicate,
and owner fields. Direct connector writes to these fields are no longer the
routine path. Contact research precedes this call under the existing
qualification policy. Whole-Sheet identity, phone, active-tail placement, owner
write, and pointer readback are performed by `action=promote_owner`.

Supply `automation_id` (the saved routine automation ID), `expected` (the exact
live `synthetic_zpid`, `listing_address`, `state`, `status`, `promotion_status`,
`import_ready`, plus any other cells being replaced), `fields` (only approved
Pilot bookkeeping fields), and `adjudication_reason`. Do not supply a row number
as identity. The service resolves the ID/address across Pilot, maps live headers,
checks the current owner, rereads immediately before writing, and verifies the
entire Pilot row plus Sheet1 owner afterward. Notes are appended, not truncated.

For reconciliation to an owner that already exists, `action=update` may set
`promotion_status=promoted`, `import_ready=promoted`, and `matched_main_row` to
the just-reread owner while keeping `status=qualified`. Creating a new owner must
use `action=promote_owner`. A durable source receipt and exact stable-ID/address
owner are required. `duplicate_key` must remain the normalized street/state key,
never a Sheet1 row number.

For an existing-listing duplicate use `status=duplicate`,
`failure_reason=duplicate_existing_listing`,
`promotion_status=skipped_duplicate_listing`, `import_ready=skip`, and its exact
owner pointer. Include `owner_agent=Full Individual Name; owner_phone=number` in
`adjudication_reason`; the service checks the exact name/phone, address, and city
against the owner. Existing-agent/different-listing suppression must have no
listing-owner pointer. A hold must identify the exact missing/conflicting evidence.

Any failed readback is a handoff blocker: do not send. The API edits Sheet1 only
for `action=promote_owner` and the explicit legacy repair below. Promotion may
delete only its own just-created owner during rollback. Reread after any shift.

## Narrow reconciliation actions

`action=reconcile_pilot_duplicate` repairs only an exact later Pilot repeat. The
caller supplies exact `canonical_expected` and `duplicate_expected` identities,
including each row's `first_seen_at`; row numbers are never accepted as
identity. The service requires identical stable IDs and canonical
address/state/unit, an earlier terminal canonical row, and then applies the
existing-agent duplicate terminal state with no Sheet1 write. If that outcome
is already present, the action is an idempotent zero-write readback.

`action=relocate_legacy_owner` is limited to already-promoted owners currently
resolved at or below the historical artifact floor (row 9000 or later). Supply
the exact live Pilot identity/state and current pointer plus an adjudication
reason. The service re-resolves the owner by stable ID and address, chooses the
next active owner row itself, copies the complete A:BT row with formulas,
formats, history, and validation, deletes the exact legacy source row in the
same Sheets batch, and then rereads all 72 displayed cells before updating the
Pilot pointer. If the owner has already moved below row 9000, the same action
only relinks the pointer when needed. It never sends outreach.

Source intake independently indexes all existing Pilot stable IDs and canonical
address/state/unit keys before discovery and adds each accepted candidate to
both sets before the next result. A repeated listing from a different URL is
therefore stopped before evidence persistence or row append.

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

`action=owner_write_preview` is the separate read-only placement check. It maps
the live owner headers, resolves the next operational row below 9000, confirms
whether that row is empty, and returns the exact A:G and Y:AB ranges with
`writes=0` and `sends=0`. It never creates or promotes an owner and is likewise
not organic behavioral proof.

## Durable queue fallback

When the scheduled verifier cannot reach the authenticated Render endpoint, it
must enqueue the exact same contract request in the hidden `Pilot Verifier Queue`
tab instead of writing Pilot or Sheet1 directly. The queue columns are:

`request_id`, `submitted_at`, `automation_id`, `payload_json`, `status`,
`claimed_at`, `completed_at`, `result_json`, and `error`.

Append one row per action in execution order. Use a stable unique request ID,
the actual timezone-aware submission time, the saved automation ID, compact JSON
for the complete endpoint payload, and `pending`; leave the remaining cells
blank. Reread the appended row. Never place bearer tokens in the queue.

Render polls the queue every minute, claims pending rows, and calls this contract
locally with production Google credentials. The contract still performs every
identity, evidence, duplicate, owner-row, and readback check. The queue cannot
send SMS. A request is complete only after the same queue row reads `completed`
and `result_json` has `ok=true`; `failed`, a nonempty `error`, or stale
`processing` is a blocker. Enqueue the terminal receipt only after all earlier
adjudication rows completed successfully. The 10:05 audit continues to require
the ordinary green receipt and does not treat queue acceptance as completion.

The worker processes at most three requests per poll to stay below Google Sheets
burst quotas. Transient rate limits and service timeouts are requeued only after
the worker proves that the requested mutation did not land; if the mutation did
land, it records a recovered completion instead of replaying it. Processing rows
older than ten minutes receive the same state-aware recovery, while ambiguous
state fails closed for manual review.

The authenticated `POST /internal/pilot-verifier-queue/process` route provides
an immediate drain for verification and recovery. The background Render worker
is the durable path and does not depend on that route being called.

## Release checks

Run `python3 -m unittest tests.test_pilot_verifier_contract
tests.test_pilot_verifier_queue tests.test_free_short_sale_source_pilot`. Deploy with source/audit startup catch-up
temporarily suppressed; restore its saved configuration afterward without a
second deployment. Keep normal schedules unchanged. Verify the live commit,
health, authenticated preview, unchanged receipt history and Sheet1 owners, and
startup logs. The next organic verifier receipt remains the behavioral proof.
