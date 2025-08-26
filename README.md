# Zillow Short Sale Bot

This repository contains scripts for scraping Zillow short sale listings and contacting agents via SMS or email.

## SMS Providers

SMS sending is pluggable. Set the provider in `config.json` (`sms_provider`) or with the `SMS_PROVIDER` environment variable.

* `android_gateway` (default) — uses [SMS Gateway for Android](https://api.smstext.app).
  * Requires `sms_gateway_api_key` or `SMS_GATEWAY_API_KEY` env var.
* `smsmobile` — uses the SMSMobile API.
  * Requires `smsmobile_api_key`/`smsmobile_from` or env vars `SMSMOBILE_API_KEY` and `SMSMOBILE_FROM`.

Switching providers is as simple as updating the config or environment and restarting the bot.

## Google Custom Search

Agent contact details are looked up using the Google Custom Search API. Provide credentials via environment variables:

* `CS_API_KEY` or `GOOGLE_API_KEY` – your Google API key
* `CS_CX` or `GOOGLE_CX` – the Custom Search Engine ID

With these variables set, the bot queries Google directly instead of relying on Apify for search results.
