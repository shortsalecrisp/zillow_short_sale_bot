# Apify scraping diagnosis

## Observed behavior (render logs)
- 15:00 ET run: request to `acts/yoni.kutler/my-actor` returned `404` because Apify expects identifiers to use the `owner~actor` form. The ingestion thread logged that failure and immediately went back to sleep.
- 16:00 ET run: identifier normalization succeeded (`yoni.kutler~my-actor`), the actor run completed with status `SUCCEEDED`, but `_process_dataset` logged `apify-hook: fetched 0 rows from dataset ORmwwoijfDyMHIcwk`.

Because `fetch_rows` returned an empty list, `_process_incoming_rows` never ran, so there were no downstream `process_rows`/SMS debug statements in the logs.

## Root cause
`webhook_server._trigger_apify_run` only sends a JSON payload when `APIFY_RUN_INPUT` is populated **and** we are invoking an actor directly (not a saved task). In the current environment there is no `APIFY_ZILLOW_TASK_ID` configured *and* neither `APIFY_ZILLOW_INPUT` nor `APIFY_ZILLOW_INPUT_FILE` is set, so the actor receives an empty/default input and produces zero listings.

Relevant code paths:
- `_trigger_apify_run` (lines 286‑334) builds the POST request and only attaches `kwargs["json"] = APIFY_RUN_INPUT` when that dict exists. Without it, the actor runs with no search parameters.
- `_process_dataset` (lines 420‑425) logs the `apify-hook: fetched 0 rows...` message and returns early, so no listing-level debug statements appear.

## Next steps
1. Provide a valid Apify input payload via `APIFY_ZILLOW_INPUT` or `APIFY_ZILLOW_INPUT_FILE`, or point `APIFY_ZILLOW_TASK_ID` at a task that already contains the input. This will allow the actor to scrape and emit listings.
2. Optionally enhance logging when zero rows are returned so it is clearer that we bailed out before `process_rows` (future improvement).
