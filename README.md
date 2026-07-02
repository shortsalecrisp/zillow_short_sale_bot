# Zillow Short Sale Bot

This repository contains scripts for scraping Zillow short sale listings and contacting agents via SMS or email.

## SMS

SMS sending uses [SMS Gateway for Android](https://api.smstext.app).  Provide the API key via the `sms_gateway_api_key` field in `config.json` or the `SMS_GATEWAY_API_KEY` environment variable.  The provider can be set with `sms_provider`, but currently only `android_gateway` is supported.

Test mode is disabled by default. To enable it explicitly, set `SMS_TEST_MODE=true` (and optionally `SMS_TEST_NUMBER` to route test messages to a specific device/number).

## Google Custom Search

Agent contact details are looked up using Google Custom Search first (when credentials are present), and the bot only
falls back to DuckDuckGo via the Jina Reader (`https://r.jina.ai/...`) when Google returns no results or is blocked.
Search results are cached locally to minimize repeated queries. Provide credentials via environment variables:

* `CS_API_KEY` or `GOOGLE_API_KEY` – your Google API key
* `CS_CX` or `GOOGLE_CX` – the Custom Search Engine ID

When a search source returns HTTP 403/429, the bot will pause further requests to that domain for a configurable cool‑off
window (`CSE_BLOCK_SECONDS` for Google, `JINA_BLOCK_SECONDS` for Jina/DuckDuckGo). This prevents retry storms when
search engines rate‑limit the deployment.

## Apify scheduler control

By default the webhook server launches the hourly Apify scheduler. Deployments that already receive listings via the
`/apify-hook` webhook can disable the scheduler (and avoid the hourly Apify run) by setting:

* `DISABLE_APIFY_SCHEDULER=true`

## Additional AK/HI Apify state searches

The bot can optionally run two extra Apify actor tasks directly from inside `webhook_server.py` and append their
results into the same downstream pipeline used by the existing Zillow webhook ingestion (normalize → dedupe → qualify
→ detail/retry → sheet/contact/SMS).

Set these environment variables:

* `APIFY_TASK_AK`
* `APIFY_TASK_HI`
* `APIFY_TASK_MAIN` (optional override; defaults to the current original-search task)
* `APIFY_STATE_SEARCH_ENABLED=true` (default)
* `APIFY_STATE_SEARCH_LIMIT=5` (default)
* `APIFY_STATE_SEARCH_FETCH_LIMIT=25` (default for AK/HI)
* `APIFY_STATE_SEARCH_BACKGROUND=true` (default)
* `APIFY_STATE_DETAIL_TASK_ID=VI5izq8RGAL14zM75` (default)

These extra state tasks are intentionally **not** webhook-driven. The bot fetches them via Apify
`/v2/actor-tasks/{APIFY_TASK_*}/run-sync-get-dataset-items`, applies queue de-dupe locally, and then calls
`/v2/actor-tasks/{APIFY_STATE_DETAIL_TASK_ID}/run-sync-get-dataset-items` only for the selected unseen zpids.
`APIFY_STATE_SEARCH_FETCH_LIMIT` controls how many raw state rows are checked before queue de-dupe, while
`APIFY_STATE_SEARCH_LIMIT` caps the combined AK/HI rows detailed and enqueued per primary webhook run. Michigan is
intentionally not part of the extra state scrape because Zillow's Michigan short-sale search is too noisy and expensive.
Keep AK's saved Zillow
search URL free of the `doz` days-on-Zillow filter so valid low-volume active listings older than seven days are not
hidden behind Apify's `No results found.` error row.

The optional daily coverage backstop runs once per local day at `APIFY_BACKSTOP_HOUR` (default `18`). It fetches wider
search-only windows from the original task and AK/HI tasks, de-dupes against already-seen and queued zpids, and only
then calls the detail task for selected unseen rows. Defaults:

* `APIFY_BACKSTOP_ENABLED=true`
* `APIFY_BACKSTOP_MAIN_FETCH_LIMIT=100`
* `APIFY_BACKSTOP_MAIN_LIMIT=10`
* `APIFY_BACKSTOP_STATE_FETCH_LIMIT=50`
* `APIFY_BACKSTOP_STATE_LIMIT=10`
* `APIFY_BACKSTOP_LOCK_PATH=/tmp/apify_coverage_backstop.txt`

## Free-source lead pilot

The free-source pilot checks public search results for non-Zillow short sale listings and writes review candidates to
the `Lead Source Pilot` tab. It runs once per day by default at 9:00 AM ET, scanning the configured source buckets
across all 50 states.

The pilot searches the configured source queries with Google Custom Search when `GOOGLE_API_KEY`/`CS_API_KEY` and
`GOOGLE_CX`/`CS_CX` are present. Production is configured to use Google CSE only, with `dateRestrict=d1`, so the
daily run spends about 100 search calls on the two selected buckets: `idx_broker_pages` and `realtor.com`. It fetches
each result page, uses a bounded Playwright fallback for allowed portal detail pages that return HTTP 403/429/451,
keeps only active listing pages with listing-text short sale evidence, and appends qualified rows after each source
query so partial daily runs still leave observable output. Render logs should include `pilot_query_start`,
`pilot_query_results`,
`pilot_headless_fetch_*` when browser fallback is used, `pilot_candidate_qualified` or rejection/duplicate events,
`pilot_query_done`, and a final `pilot_run_done` stats record.

The pilot does not write to `Sheet1` or send SMS. Candidate rows are first qualified as active short sale listings, then
deduped against `Sheet1` by listing address before writing to the pilot tab. New rows can be added without agent contact
fields; listing-page parsing fills contact fields when visible, and incomplete rows stay in pilot review for later
verifier/contact enrichment. If visible contact fields match an already-known phone or possible existing agent, the row
is still written for listing review and the match is recorded in the duplicate/review columns. Rows include
`synthetic_zpid` and `pending_queue_listing_json` so reviewed net-new candidates can later be promoted into the same
PendingQueue-style shape used by the Zillow scraper.

Configuration:

* `FREE_SOURCE_PILOT_ENABLED=true`
* `FREE_SOURCE_PILOT_TAB=Lead Source Pilot`
* `FREE_SOURCE_PILOT_FORCE_ALL_STATES=true`
* `FREE_SOURCE_PILOT_STATES=AL,AK,AZ,AR,CA,CO,CT,DE,FL,GA,HI,ID,IL,IN,IA,KS,KY,LA,ME,MD,MA,MI,MN,MS,MO,MT,NE,NV,NH,NJ,NM,NY,NC,ND,OH,OK,OR,PA,RI,SC,SD,TN,TX,UT,VT,VA,WA,WV,WI,WY`
* `FREE_SOURCE_PILOT_HEADLESS_FALLBACK=true`
* `FREE_SOURCE_PILOT_HEADLESS_BUDGET=12`
* `FREE_SOURCE_PILOT_HEADLESS_DOMAIN_BUDGET=4`
* `FREE_SOURCE_PILOT_RESULTS_PER_QUERY=10`
* `FREE_SOURCE_PILOT_RUN_HOUR=9`
* `FREE_SOURCE_PILOT_RUN_MINUTE=0`
* `FREE_SOURCE_PILOT_SLEEP_SECONDS=1.0`
* `FREE_SOURCE_PILOT_SEARCH_ENGINE=cse`
* `FREE_SOURCE_PILOT_SOURCE_BUCKETS=idx_broker_pages,realtor.com`
* `FREE_SOURCE_PILOT_DATE_RESTRICT=d1`

If your deployment does **not** run `webhook_server.py` (for example, it only calls
`bot_min.process_rows` directly), run `python scheduler_worker.py` alongside the main
process so the hourly follow-up scheduler stays active.

To have the worker also run the Apify hourly scrape (instead of the web service), set:

* `ENABLE_APIFY_HOURLY=true`

When this flag is set the worker will import the Apify hourly task from `webhook_server.py`. If that import fails, the
worker logs a warning and continues with follow-up scheduling only.

### Render deployment guidance

On Render, prefer running the scheduler as a **Worker** process using `python scheduler_worker.py` so it is not tied to
HTTP traffic or subject to web-service auto-suspend. When doing so:

1) Create a Worker service that runs `python scheduler_worker.py` with the same environment variables as the web service.
   * Set `ENABLE_APIFY_HOURLY=true` on the Worker if you want the hourly Apify scrape to run from the worker.
2) Set `DISABLE_APIFY_SCHEDULER=true` on the Web Service so the background thread does not duplicate the Worker runs.
3) If you cannot run a Worker, either disable auto-suspend (paid plan) or keep the web process warm with an uptime
   monitor so the scheduler thread inside `webhook_server.py` hits each top-of-hour slot.

Headless reviews download a Chromium bundle on first run via Playwright. You can control the cache location and download
behavior with:

* `HEADLESS_BROWSER_CACHE` (default: `~/.cache/playwright`)
* `HEADLESS_BROWSER_DOWNLOAD` (default: `true`)

### How to recreate service on Render using Blueprint

1) In the Render dashboard, click **New** → **Blueprint**.
2) Connect the GitHub repository for this project if it is not already connected.
3) Select the repository and branch, then click **Continue**.
4) Review the services preview (it should show a Docker web service), then click **Apply**.
5) In the service settings, update any `REPLACE_ME` environment variables with real values, then click **Save Changes**.
6) Trigger a deploy from **Manual Deploy** → **Deploy latest commit**.

## Follow-up scheduling

Follow-up passes run during configured work hours. They now skip weekends by default to avoid contacting agents on
Saturday or Sunday. To include weekends, explicitly opt in by setting the environment variable:

* `FOLLOWUP_INCLUDE_WEEKENDS=true`

After a follow-up text is sent, the hourly scheduler now waits two hours before marking column `K` with the Mailshake
code `N`. Rows are only released to Mailshake when `I=x`, the follow-up timestamp column is at least two hours old, and
both `J` and `K` are still blank. If a matching inbound SMS reply exists on the `Replies` tab, the row is treated as
replied instead and is not marked `N`.

You can tune this handoff with:

* `MAILSHAKE_AFTER_FOLLOWUP_HOURS` (default `2`)
* `MAILSHAKE_AFTER_FOLLOWUP_CODE` (default `N`)
* `GSHEET_REPLIES_TAB` (default `Replies`)

## RapidAPI Zillow data

The bot fetches listing details from the RapidAPI Zillow data source. Set your credentials via the environment:

* `RAPID_KEY` – your RapidAPI key
* `RAPID_HOST` – the RapidAPI host (defaults to `us-housing-market-data1.p.rapidapi.com`)

If you previously used the old host (`zillow-com1.p.rapidapi.com`), update your environment to the new default so the RapidAPI requests continue to work. The bot will now log a warning and automatically switch to `us-housing-market-data1.p.rapidapi.com` if it sees the deprecated host in the environment.
