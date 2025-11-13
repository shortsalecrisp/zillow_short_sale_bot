# Zillow Short Sale Bot

This repository contains scripts for scraping Zillow short sale listings and contacting agents via SMS or email.

## SMS

SMS sending uses [SMS Gateway for Android](https://api.smstext.app).  Provide the API key via the `sms_gateway_api_key` field in `config.json` or the `SMS_GATEWAY_API_KEY` environment variable.  The provider can be set with `sms_provider`, but currently only `android_gateway` is supported.

## Google Custom Search

Agent contact details are looked up using the Google Custom Search API. Provide credentials via environment variables:

* `CS_API_KEY` or `GOOGLE_API_KEY` – your Google API key
* `CS_CX` or `GOOGLE_CX` – the Custom Search Engine ID

With these variables set, the bot queries Google directly instead of relying on Apify for search results.

## Hourly Apify ingestion

To have the service trigger an Apify run at the top of every hour between 8 AM and 8 PM (local bot timezone), configure the following environment variables:

* `APIFY_API_TOKEN` – your Apify API token (or set `APIFY_TOKEN`).
* One of `APIFY_ZILLOW_TASK_ID` or `APIFY_ZILLOW_ACTOR_ID` – the task/actor that scrapes Zillow short sales.

Optional overrides:

* `APIFY_RUN_START_HOUR` / `APIFY_RUN_END_HOUR` – restrict the ingestion window (defaults 8 and 20 to run 8 PM inclusive).
* `APIFY_WAIT_FOR_FINISH` – seconds to wait for the task/actor to complete before processing the dataset (default 240).
* `APIFY_ZILLOW_INPUT` – JSON payload passed to the actor run (ignored when using tasks that already encapsulate input).

Once configured, the webhook server's background thread runs the task/actor, pulls the resulting dataset, and feeds it through the same processing pipeline used for manual Apify webhooks.
