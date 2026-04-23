# Current Working Call Flow

This is the known-good baseline for the current ElevenLabs test system.

## Current Test Setup

- Backend: Node.js + TypeScript + Express.
- Telephony: ElevenLabs outbound agent over the Telnyx SIP number, with the older Telnyx Call Control flow still available as a fallback.
- Public webhook: ngrok URL in `BASE_URL`.
- Sheet writeback: Apps Script callback endpoint through `postSheetUpdate()`.
- Email: Gmail SMTP through the existing callback email helper.
- `TEST_MODE=true` keeps outbound lead calls pointed at `TEST_DESTINATION_NUMBER`.

## Known-Good Flow

1. Google Sheets / local test sends `POST /start-call`.
2. Backend places an outbound ElevenLabs call when `VOICE_PROVIDER=elevenlabs`.
3. In test mode, the call goes to the configured test destination.
4. Emmy waits for the person to answer first, asks if she reached the first name, then starts the opening script after confirmation.

## Option 1: Live Transfer

1. Caller says they want to speak with Yoni now.
2. Emmy says: `Ok, let me see if he is available for the call right now, and if he is, I'll patch him in.`
3. Emmy calls the `live_transfer_requested` tool.
4. Backend places a Telnyx approval call to `LIVE_TRANSFER_NUMBER`.
5. Yoni hears the lead name/address context and presses `1` to accept.
6. If Yoni accepts, Emmy says: `Ok, I got Yoni on the phone. I'll connect you guys right now.`
7. Emmy uses the ElevenLabs native `transfer_to_number` tool to bridge the call.
8. If Yoni does not accept, Emmy plays the ASAP fallback and the backend sends the callback email through the normal callback tool path.

Note: ElevenLabs native SIP transfers do not reliably support whisper playback during the final bridge. The backend now handles the warm context with a separate Telnyx approval call before allowing the ElevenLabs bridge.

## Option 3: Callback Requested

1. Caller asks for a callback or agrees to schedule one.
2. Emmy asks what time Yoni should call.
3. Caller says a callback time.
4. Emmy calls the `callback_requested` tool with that callback time.
5. Backend updates the sheet and sends the callback email with the captured callback time.

## ElevenLabs Post-Call Protection

When `VOICE_PROVIDER=elevenlabs`, the backend schedules a post-call transcript check after each outbound call. If Emmy collected a callback time but did not call the backend tool during the live conversation, the backend uses the completed ElevenLabs transcript to update the sheet and send the callback email.

## Missed Live Transfer Fallback

1. Caller asks to speak with Yoni now.
2. The ElevenLabs transfer fails, Yoni does not answer, or transfer control returns to Emmy.
3. Emmy says the full fallback message: `Sorry, he was not available. I will text him now and have him call you back ASAP. Thanks, talk to you soon.`
4. Backend updates the sheet with `callbackTime=asap`.
5. Backend sends the ASAP callback email.

## Important Env Flags

- `TEST_MODE`: Forces lead calls to `TEST_DESTINATION_NUMBER` when true.
- `TEST_DESTINATION_NUMBER`: Safe test destination for outbound lead calls.
- `BASE_URL`: Public URL for Telnyx webhooks.
- `LIVE_TRANSFER_NUMBER`: Destination number for Yoni transfer approval calls.
- `LIVE_TRANSFER_TEST_MODE`: Simulates accepted transfer when true.
- `LIVE_TRANSFER_FORCE_RESULT`: `none`, `accept`, or `fail` for controlled transfer testing.
- `GOOGLE_APPS_SCRIPT_WEBHOOK_URL`: Current sheet update target.
- `ALERT_EMAIL_TO`, `ALERT_EMAIL_FROM`, `SMTP_HOST`, `SMTP_PORT`, `SMTP_USER`, `SMTP_PASS`: Callback email settings.

## Baseline Protection

Before ElevenLabs changes, retest:

- `POST /start-call` returns accepted Telnyx call metadata.
- Option `1` connects to Yoni after he accepts.
- Option `3` captures callback time and sends email.
- Missed Yoni answer plays the fallback message and sends ASAP email.
- Apps Script sheet updates still succeed.
