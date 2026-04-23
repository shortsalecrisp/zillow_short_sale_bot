# ElevenLabs Integration Contract

This document defines the stable payload shape the backend should expect when ElevenLabs handles natural voice intent later.

The current system is working with Telnyx, DTMF test prompts, Apps Script sheet updates, live transfer bridging, and callback emails. ElevenLabs should plug into the same decision points without changing the downstream sheet/email/transfer semantics.

## Current ElevenLabs Config

- Agent name: `Emmy`
- Agent ID: `agent_9901kpkhs1cmfgj8w9vm199nc92j`
- Branch ID: `agtbrch_1101kpkhs3b4eg5v0q8ppq4hayks`
- Voice: `Liza`
- Voice ID: `bb2q8Tox15YHJ7ceE9tu`
- TTS model: `eleven_v3_conversational`
- Primary LLM: `Qwen3-30B-A3B`
- Agent timezone: `Eastern Time (US)`
- API base URL: `https://api.elevenlabs.io`
- Auth header: `xi-api-key`

The actual ElevenLabs API key belongs only in `.env` or deployment secrets as `ELEVENLABS_API_KEY`.

## Expected Payload

```json
{
  "rowNumber": 3250,
  "agentName": "John Smith",
  "phone": "+14043009526",
  "listingAddress": "123 Main St, Tampa, FL",
  "intent": "callback_requested",
  "callbackTime": "tomorrow at 4 PM",
  "conversationSummary": "Lead asked Yoni to call back tomorrow at 4 PM about the short sale.",
  "transferRequested": false,
  "callbackRequested": true
}
```

## Fields

- `rowNumber`: Google Sheet row number to update.
- `agentName`: Lead or agent name collected from the start-call metadata.
- `phone`: Phone number used for the call.
- `listingAddress`: Property address from the lead row.
- `intent`: Normalized intent enum.
- `callbackTime`: Spoken callback time converted to text. Use an empty string when not applicable.
- `conversationSummary`: Short text summary for the callback email and sheet notes. Do not send audio recordings.
- `transferRequested`: `true` when the caller wants to speak with Yoni now.
- `callbackRequested`: `true` when the caller wants a later callback.

## Intents

- `live_transfer`: Caller wants to speak with Yoni now.
- `callback_requested`: Caller wants Yoni to call later.
- `not_interested`: Caller is not interested.
- `unknown`: Intent could not be determined confidently.

## Backend Mapping

- `live_transfer` should start the existing live transfer flow.
- `callback_requested` should send sheet update + callback email with `callbackTime` and `conversationSummary`.
- `not_interested` should update the sheet as not interested.
- `unknown` should use a safe fallback and avoid transferring unless the caller clearly requested it.

## Backend Tool Endpoints

Configure ElevenLabs server tools to call:

- `POST {{BASE_URL}}/elevenlabs/tool/live-transfer-requested`
- `POST {{BASE_URL}}/elevenlabs/tool/callback-requested`
- `POST {{BASE_URL}}/elevenlabs/tool/not-interested`

Send `ELEVENLABS_TOOL_SECRET` in one of these ways:

- `Authorization: Bearer <secret>`
- `x-crisp-elevenlabs-secret: <secret>`
- JSON body `token`

Post-call webhook endpoint:

- `POST {{BASE_URL}}/elevenlabs/post-call`

## Notes

- The backend should receive text and structured intent from ElevenLabs, not voice recordings.
- The existing Telnyx call control flow remains responsible for transfer, bridge, hangup, and webhook lifecycle events.
- Keep the payload small and stable so the voice layer can evolve without changing the sheet/email contracts.
