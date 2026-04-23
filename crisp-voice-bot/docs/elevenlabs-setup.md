# ElevenLabs Setup

The code is ready for ElevenLabs, but the ElevenLabs account currently has no imported phone numbers. The API returned an empty list from `/v1/convai/phone-numbers`, so outbound ElevenLabs calls cannot start until a SIP trunk phone number exists.

## Current Safe State

- `VOICE_PROVIDER=telnyx`
- Current Telnyx DTMF flow remains the active working fallback.
- ElevenLabs config is present but inactive until `VOICE_PROVIDER=elevenlabs`.

## Required Dashboard Setup

1. In Telnyx, create or use a SIP trunk connection for ElevenLabs.
2. In ElevenLabs, go to Phone Numbers.
3. Import the Telnyx SIP trunk phone number.
4. Assign the imported number to the published `Emmy` agent.
5. Copy the generated ElevenLabs phone number ID.
6. Set:

```text
ELEVENLABS_AGENT_PHONE_NUMBER_ID=<copied phone_number_id>
VOICE_PROVIDER=elevenlabs
```

Keep `TEST_MODE=true` for the first tests.

## Backend Endpoints for ElevenLabs Tools

Use `ELEVENLABS_TOOL_SECRET` as either:

- `Authorization: Bearer <secret>`
- `x-crisp-elevenlabs-secret: <secret>`
- JSON body field `token`

Tool URLs:

```text
POST {{BASE_URL}}/elevenlabs/tool/live-transfer-requested
POST {{BASE_URL}}/elevenlabs/tool/callback-requested
POST {{BASE_URL}}/elevenlabs/tool/not-interested
POST {{BASE_URL}}/elevenlabs/post-call
```

## Tool Payloads

Live transfer:

```json
{
  "rowNumber": 3250,
  "agentName": "John Smith",
  "phone": "+14043009526",
  "listingAddress": "123 Main St, Tampa, FL",
  "conversationSummary": "Lead wants to speak with Yoni about short sale processing."
}
```

Callback:

```json
{
  "rowNumber": 3250,
  "agentName": "John Smith",
  "phone": "+14043009526",
  "listingAddress": "123 Main St, Tampa, FL",
  "callbackTime": "today at 4 PM",
  "conversationSummary": "Lead asked Yoni to call back today at 4 PM."
}
```

Not interested:

```json
{
  "rowNumber": 3250,
  "agentName": "John Smith",
  "phone": "+14043009526",
  "listingAddress": "123 Main St, Tampa, FL",
  "conversationSummary": "Lead said they have the short sale handled."
}
```

## Transfer Strategy

Primary approach:

- Use ElevenLabs native transfer-to-human for live transfers.
- Transfer destination: `LIVE_TRANSFER_NUMBER`.
- Caller message before transfer attempt: `Ok, let me see if he is available for the call right now, and if he is, I'll patch him in.`
- Backend warm approval call to Yoni: `You have a live transfer request from agent {{agentName}} at {{streetAddress}}. Press 1 to accept.`
- Caller message after Yoni accepts: `Ok, I got Yoni on the phone. I'll connect you guys right now.`
- Human message during final ElevenLabs transfer, if SIP supports it: `Live transfer from {{agentName}} about {{streetAddress}}.`
- Missed transfer fallback: `Sorry, he was not available. I will text him now and have him call you back ASAP. Thanks, talk to you soon.`

Note: ElevenLabs native SIP transfers can call the transfer destination, but warm transfer whisper playback to the human operator is not supported for SIP-based transfers. The backend therefore calls Yoni first with a Telnyx approval prompt before allowing the ElevenLabs bridge.

Fallback approach:

- If ElevenLabs transfer does not behave correctly, set `VOICE_PROVIDER=telnyx`.
- The current custom Telnyx transfer flow remains available and tested.

## Test Plan

1. Confirm `/health`.
2. Set `VOICE_PROVIDER=elevenlabs`.
3. Start a test call through `/start-call`.
4. Confirm the response includes `provider: "elevenlabs"` and a `conversationId`.
5. Test not interested path.
6. Test callback path and confirm sheet + email.
7. Test live transfer path.
8. If live transfer fails, switch `VOICE_PROVIDER=telnyx` and keep using the tested custom transfer flow.
