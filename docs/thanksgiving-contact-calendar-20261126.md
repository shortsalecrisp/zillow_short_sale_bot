# Owner Thanksgiving Follow-Up Hold

Yoni approved November 25, 2026: initial SMS may continue, but follow-up texts
must remain paused until Friday November 27 at 9:00 AM in each agent's local
time. Ordinary inbound replies, opt-outs, human takeover, original templates,
receipt ownership, duplicate guards, six-business-hour eligibility, spending caps,
explicit recovery expiry and uncertain-send protections are unchanged.

The deployed Apps Script policy is the fixed property
`OWNER_THANKSGIVING_FOLLOWUP_20261126_V1`. Its exact version is
`thanksgiving-20261126-v1`, with `resume_local=2026-11-27 09:00:00`.
The normal SMS token is required to inspect `owner_holiday_policy` or activate
`apply_owner_holiday_policy` with the exact approved confirmation. These two paths
run after authentication and before any logging, trigger installation, queueing or
message handling. Activation writes only the fixed policy property, not lead rows.

Enqueue, existing active lease, fresh claim and final send authorization each
apply the hold. A fresh exact current-phone row and the existing city/state listing
time-zone resolver determine release time, never an area code. Unknown or shifted
identity remains held rather than guessed. All existing freshness, opt-out and
takeover checks still run after the release; release is permission to evaluate,
not a promise or proof of sending. Short-lived owner/recovery authorizations can
expire normally and are never extended or blindly resent by this holiday policy.

The policy does not select initial SMS or ordinary inbound replies. It expires
after this one-off holiday so later campaigns retain their prior hours. Held rows
keep their IDs and attempt counts; not-before is never brought earlier. Already
authorized, sent or uncertain attempts are reconciled, not rewritten or reopened.

The phone scheduler independently excludes November 26 and carries the six
currently booked Thursday calls to November 27 alongside Friday's normal queue.
The native App Script scheduler and final backend call-window guard both reject
Thanksgiving. V4 model allocation and all provider prompts/configuration stay unchanged.
