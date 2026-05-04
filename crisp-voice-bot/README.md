# crisp-voice-bot

Webhook-driven outbound voice backend between Google Sheets and Telnyx Voice API.

This MVP version is intentionally small:

- `POST /start-call` receives a row payload and places a Telnyx outbound Call Control call.
- `POST /telnyx/webhook` receives Telnyx lifecycle webhooks, speaks Emmy's opening script, gathers keypad input, simulates a live-transfer fallback, logs the outcome, speaks a closing line, then hangs up.
- Sheet updates are sent to your Google Apps Script webhook, which writes the result into Google Sheets.
- Live-transfer requests can send a real Telnyx SMS alert when explicitly enabled.
- `GET /health` returns a simple service health check.

## What This Pass Does

When a call is answered, Emmy says:

```text
Hey, is this {{firstName}}? Hi, this is Emmy. I work with Yoni Kutler. He sent you a quick text yesterday about your short sale at {{listingAddress}}. Do you have a second right now to talk?

If you want to speak with Yoni now, press 1.
If you're not interested, press 2.
If you want him to call you back later, press 3.
```

If `firstName` is blank, the first-name greeting is omitted naturally.

The first keypad prompt outcomes are:

- `1`: live transfer requested, logs `liveTransferRequested: "yes"`, simulates checking Yoni's availability, then asks for a fallback callback slot
- `2`: not interested, logs `responseStatus: "R"`
- `3`: callback requested, logs `responseStatus: "Y"`, `callbackRequested: "yes"`, and `callbackTime: "unspecified"`
- no digit before timeout: no response to prompt

After pressing `1`, the transfer fallback prompt outcomes are:

- `1`: callback later today
- `2`: callback tomorrow
- `3`: no longer needed
- no digit before timeout: callback requested, time unknown

Each outcome sends a writeback payload for row `rowNumber`.

## Safety First

`TEST_MODE=true` is the default. In test mode, `/start-call` ignores the lead phone number and calls `TEST_DESTINATION_NUMBER` instead.

Only set `TEST_MODE=false` after the call script, sheet integration, compliance review, and transfer logic are ready.

## Setup

```bash
cd crisp-voice-bot
npm install
cp .env.example .env
```

Edit `.env` and set:

- `TELNYX_API_KEY`
- `BASE_URL`
- `TEST_DESTINATION_NUMBER`

Google Apps Script writeback:

- `GOOGLE_APPS_SCRIPT_WEBHOOK_URL`
- `GOOGLE_APPS_SCRIPT_TOKEN`

Optional SMS alerts:

- `YONI_ALERT_SMS_ENABLED`
- `YONI_ALERT_DESTINATION_NUMBER`
- `TELNYX_MESSAGING_PROFILE_ID`
- `TELNYX_ALERT_FROM_NUMBER`

The sample `.env.example` includes the caller ID, connection ID, and outbound voice profile ID supplied for this project.

## Local Development

```bash
npm run dev
```

Health check:

```bash
curl http://localhost:3000/health
```

Start a test call:

```bash
curl -X POST http://localhost:3000/start-call \
  -H "Content-Type: application/json" \
  -d '{
    "rowNumber": 2451,
    "firstName": "John",
    "lastName": "Smith",
    "fullName": "John Smith",
    "phone": "+19542053205",
    "email": "john@example.com",
    "listingAddress": "123 Main St",
    "createdAt": "2026-04-18 10:00:00",
    "scheduledForEt": "2026-04-19 16:30:00",
    "responseStatus": "",
    "notes": "",
    "sheetName": "Sheet1"
  }'
```

With `TEST_MODE=true`, the `phone` field is still validated, but the app dials `TEST_DESTINATION_NUMBER`.

Test safely first with:

```env
TEST_MODE=true
YONI_ALERT_SMS_ENABLED=false
GOOGLE_APPS_SCRIPT_WEBHOOK_URL=https://script.google.com/macros/s/YOUR_DEPLOYMENT_ID/exec
```

After you answer the call on the verified test phone:

- press `1` to simulate a live transfer request
- after the transfer fallback prompt, press `1` for later today, `2` for tomorrow, or `3` for no longer needed
- press `2` on the first prompt to simulate a not-interested seller
- press `3` to simulate a callback request
- press nothing to test the timeout fallback

## Expected Sheet Writeback Payloads

Press `1`, then `1`:

```json
{
  "rowNumber": 2451,
  "callResult": "callback_requested",
  "responseStatus": "Y",
  "callbackRequested": "yes",
  "callbackTime": "later_today",
  "voiceNotes": "Test call: requested callback later today after transfer fallback"
}
```

Press `1`, then `2`:

```json
{
  "rowNumber": 2451,
  "callResult": "callback_requested",
  "responseStatus": "Y",
  "callbackRequested": "yes",
  "callbackTime": "tomorrow",
  "voiceNotes": "Test call: requested callback tomorrow after transfer fallback"
}
```

Press `2`:

```json
{
  "rowNumber": 2451,
  "callResult": "answered_not_interested",
  "responseStatus": "R",
  "voiceNotes": "Test call: not interested via keypad"
}
```

Press `3`:

```json
{
  "rowNumber": 2451,
  "callResult": "callback_requested",
  "responseStatus": "Y",
  "callbackRequested": "yes",
  "callbackTime": "unspecified",
  "voiceNotes": "Test call: callback requested via keypad"
}
```

Pressing `1` also sends an earlier transfer-request payload:

```json
{
  "rowNumber": 2451,
  "callResult": "live_transfer_requested",
  "liveTransferRequested": "yes",
  "voiceNotes": "Test call: live transfer requested via keypad"
}
```

## Google Apps Script Writeback

Google Apps Script writeback is the active, stable path. The voice bot posts call-result payloads to:

```env
GOOGLE_APPS_SCRIPT_WEBHOOK_URL=https://script.google.com/macros/s/YOUR_DEPLOYMENT_ID/exec
GOOGLE_APPS_SCRIPT_TOKEN=replace_with_shared_secret
```

The Apps Script web app validates the shared token and writes to the `ShortSaleLeads` Google Sheet. This avoids local Google OAuth, ADC, and service-account setup for the voice bot backend.

Current Apps Script column mapping:

- `responseStatus` -> `J` (`response_status`)
- `callResult` -> `AH` (`voice_call_1_result`)
- `voiceNotes` -> `AP` (`voice_notes`)
- `callbackRequested` -> `AL` (`callback_requested`)
- `callbackTime` -> `AM` (`callback_time`)

When any valid call result is processed, `AG` (`voice_call_1_sent`) is stamped if it is blank. `K` is `mailshake_status` and must never be overwritten.

## Callback Email Alerts

When a caller requests a callback (`callResult: "callback_requested"`), the bot sends the sheet update first, then queues an email alert. Email failures are logged and do not interrupt the call flow.

Required env vars:

```env
ALERT_EMAIL_TO=you@example.com
ALERT_EMAIL_FROM=bot@example.com
SMTP_HOST=smtp.example.com
SMTP_PORT=587
SMTP_USER=bot@example.com
SMTP_PASS=replace_with_smtp_password
```

Email subject:

```text
🔥 Callback Requested - New Lead
```

Email body format:

```text
Agent: John Smith
Phone: (954) 205-3205
Property: 123 Main St, Tampa, FL
Callback Time: ASAP

Conversation Outline:
Short summary of what the lead requested.
```

## SMS Alerts

Keep SMS disabled while testing the call flow:

```env
YONI_ALERT_SMS_ENABLED=false
```

To enable real SMS alerts later:

```env
YONI_ALERT_SMS_ENABLED=true
YONI_ALERT_DESTINATION_NUMBER=+19542053205
TELNYX_ALERT_FROM_NUMBER=+12176341017
TELNYX_MESSAGING_PROFILE_ID=
```

The alert SMS body is:

```text
Live transfer requested. Row 2451. John Smith. 123 Main St. Call target: +19542053205. Call back if available.
```

Your Telnyx alert sender number must be SMS-capable and assigned to a Telnyx Messaging Profile.

## Public Webhook Testing With ngrok

Telnyx must reach your local webhook over HTTPS. One common path:

```bash
ngrok http 3000
```

Set `BASE_URL` in `.env` to the HTTPS ngrok URL, for example:

```bash
BASE_URL=https://abc123.ngrok-free.app
```

Then restart the dev server:

```bash
npm run dev
```

The app passes `BASE_URL/telnyx/webhook` to Telnyx when creating each outbound call.

## Render Deployment

Use these settings for a simple Render Web Service:

- Runtime: Node
- Build command: `npm install && npm run build`
- Start command: `npm start`
- Environment: add every key from `.env.example`
- `BASE_URL`: your Render service URL, for example `https://crisp-voice-bot.onrender.com`

Keep `TEST_MODE=true` for the first deployed smoke test.

## Working with Codex

This repo is the working project for both sides of the Crisp voice bot:

- Backend code lives in `src/`.
- The deprecated local Apps Script callback source lives in `apps-script/voice-bot-callback.gs`.
- Setup and deployment notes live in `docs/`.

Apps Script is no longer required for voice-bot writeback. If you keep the Apps Script file for older tooling, make changes locally first, then manually paste the updated `apps-script/voice-bot-callback.gs` into the bound Google Apps Script project and redeploy the web app.

Secrets belong in `.env`, never in git. Keep `.env.example` as the safe template.

For future Codex tasks, start by editing the relevant local source file, then run:

```bash
npm install
npm run build
```

Use `docs/setup-checklist.md` before changing deployment settings, Telnyx webhook behavior, Google Sheets credentials, or `TEST_MODE`.

## Route Summary

### `POST /start-call`

Required fields:

- `rowNumber`
- `phone`
- `listingAddress`

Phone numbers must be E.164, for example `+19542053205`.

### `POST /telnyx/webhook`

Handles:

- `call.initiated`
- `call.answered`
- `call.dtmf.received`
- `call.gather.ended`
- `call.speak.ended`
- `call.hangup`

Other Telnyx events are logged and safely ignored.

### `POST /sheet-update`

Local stub endpoint retained for manual testing. Active Telnyx writeback no longer uses this route.

## Files Updated In This MVP Pass

- `.env.example`
- `src/types.ts`
- `src/lib/callState.ts`
- `src/lib/sheetUpdateClient.ts`
- `src/lib/telnyx.ts`
- `src/lib/transferNotification.ts`
- `src/routes/startCall.ts`
- `src/routes/telnyxWebhook.ts`
- `src/routes/sheetUpdate.ts`
- `README.md`

## Next Files To Edit

Start with `src/lib/transferNotification.ts` to add push alerts or escalation routing beyond SMS.

For sheet writeback changes, edit `src/lib/sheetUpdateClient.ts`.

The next production step is to add webhook signature verification, real transfer dialing, and production-grade Google Sheets credential handling.

TODOs are already placed for:

- Future AI conversation flow
- Live transfer
- Direct Google Sheets writeback
