# Zillow Short Sale Bot

This repository contains scripts for scraping Zillow short sale listings and contacting agents via SMS or email.

## SMS

SMS sending uses [SMS Gateway for Android](https://api.smstext.app).  Provide the API key via the `sms_gateway_api_key` field in `config.json` or the `SMS_GATEWAY_API_KEY` environment variable.  The provider can be set with `sms_provider`, but currently only `android_gateway` is supported.

## Google Custom Search

Agent contact details are looked up using the Google Custom Search API with responses fetched through the Jina Reader
(`https://r.jina.ai/...`) to avoid rate limits. Search results are cached locally to minimize repeated queries. Provide
credentials via environment variables:

* `CS_API_KEY` or `GOOGLE_API_KEY` – your Google API key
* `CS_CX` or `GOOGLE_CX` – the Custom Search Engine ID

With these variables set, the bot queries Google directly instead of relying on Apify for search results. If the
credentials are missing or a request fails, the bot will fall back to the Apify Google Search actor.

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
