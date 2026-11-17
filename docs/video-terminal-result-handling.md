# Future-only terminal result handling

New jobs pin `provider_result_handling_version=terminal-result-v1` at creation.
Only newly created pipelines copy the marker. Existing jobs and pipelines are
not backfilled, migrated or reopened. Generator, audio/name/media acceptance,
receipt ownership, scheduling, suppression and spending contracts are unchanged.

For marked pipelines, a canonical existing-request status GET must say
`COMPLETED` and bind the exact stored request ID before a result error can be
classified as terminal. Known typed provider/model errors from the same result
GET become terminal failures. Unbound status, unknown/plain gateway errors,
authentication/access failures, throttling, transport failures and malformed or
oversized responses remain holds rather than evidence of completion. There is
no paid retry, resubmission, cancellation or output-media fetch in this path.

The failed pipeline retains HTTP status, request ID, whitelisted error type and
response-body SHA256; raw provider messages, inputs, media URLs, credentials and
headers are not persisted by the new diagnostic fields. Normal accounting for a
future terminal job frees its concurrency slot while retaining the complete
10-cent reservation debit. No refund or measured billing amount is inferred.

New jobs also retain an atomic reservation-denial reason (`inflight_limit`,
`daily_limit` or `monthly_limit`) and the observed aggregate counts. Repeated
identical holds do not create repeated job writes or logs. If the unchanged
follow-up deadline arrives first, the existing single-message text freeze wins;
its metadata explains the budget/capacity cause. No late extra video is sent.

Legacy unresolved reservations, including the protected private TTS submission,
are not cleared by this release. Any one-off historical slot release requires
specific owner authority and preserves every original debit and message.

fal references: https://fal.ai/docs/api-reference/inference-api/queue/status
and https://fal.ai/docs/api-reference/inference-api/queue/result
