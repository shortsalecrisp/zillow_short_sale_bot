# Zillow Short Sale Bot

This repository contains scripts for scraping Zillow short sale listings and contacting agents via SMS or email.

## SMS

SMS sending uses [SMS Gateway for Android](https://api.smstext.app).  Provide the API key via the `sms_gateway_api_key` field in `config.json` or the `SMS_GATEWAY_API_KEY` environment variable.  The provider can be set with `sms_provider`, but currently only `android_gateway` is supported.

## Google Custom Search

Agent contact details are looked up using the Google Custom Search API. Provide credentials via environment variables:

* `CS_API_KEY` or `GOOGLE_API_KEY` – your Google API key
* `CS_CX` or `GOOGLE_CX` – the Custom Search Engine ID

With these variables set, the bot queries Google directly instead of relying on Apify for search results.
