# Zillow Short Sale Bot

This repository contains scripts for scraping Zillow short sale listings and contacting agents via SMS or email.

## SMS

SMS sending uses [SMS Gateway for Android](https://api.smstext.app).  Provide the API key via the `sms_gateway_api_key` field in `config.json` or the `SMS_GATEWAY_API_KEY` environment variable.  The provider can be set with `sms_provider`, but currently only `android_gateway` is supported.

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

For reliable Playwright installs, deploy this service to Render as a **Docker** service using the repo `Dockerfile`. The
Docker build runs `python -m playwright install --with-deps chromium` and a smoke test that launches Chromium, so the
container will fail to build if Playwright cannot start.

If you see `PLAYWRIGHT_MISSING` in the logs, confirm the service is using the Docker deployment and that no overrides are
skipping the Docker build.

## Follow-up scheduling

Follow-up passes run during configured work hours. They now skip weekends by default to avoid contacting agents on
Saturday or Sunday. To include weekends, explicitly opt in by setting the environment variable:

* `FOLLOWUP_INCLUDE_WEEKENDS=true`

## RapidAPI Zillow data

The bot fetches listing details from the RapidAPI Zillow data source. Set your credentials via the environment:

* `RAPID_KEY` – your RapidAPI key
* `RAPID_HOST` – the RapidAPI host (defaults to `us-housing-market-data1.p.rapidapi.com`)

If you previously used the old host (`zillow-com1.p.rapidapi.com`), update your environment to the new default so the RapidAPI requests continue to work. The bot will now log a warning and automatically switch to `us-housing-market-data1.p.rapidapi.com` if it sees the deprecated host in the environment.
