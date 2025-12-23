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

## Follow-up scheduling

Follow-up passes run during configured work hours. They now skip weekends by default to avoid contacting agents on
Saturday or Sunday. To include weekends, explicitly opt in by setting the environment variable:

* `FOLLOWUP_INCLUDE_WEEKENDS=true`

## RapidAPI Zillow data

The bot fetches listing details from the RapidAPI Zillow data source. Set your credentials via the environment:

* `RAPID_KEY` – your RapidAPI key
* `RAPID_HOST` – the RapidAPI host (defaults to `us-housing-market-data1.p.rapidapi.com`)

If you previously used the old host (`zillow-com1.p.rapidapi.com`), update your environment to the new default so the RapidAPI requests continue to work. The bot will now log a warning and automatically switch to `us-housing-market-data1.p.rapidapi.com` if it sees the deprecated host in the environment.
