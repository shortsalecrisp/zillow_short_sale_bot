# ElevenLabs Readiness Checklist

Use this before changing the voice layer.

- Opening confirms the person first: `Hi, I'm trying to reach {{firstName}}.`
- Transfer attempt script ready: `Ok, let me see if he is available for the call right now, and if he is, I'll patch him in.`
- Backend warm approval call to Yoni enabled before final ElevenLabs transfer.
- Missed transfer fallback script ready: `Sorry, he was not available. I will text him now and have him call you back ASAP. Thanks, talk to you soon.`
- ElevenLabs agent, branch, voice, model, and timezone env vars recorded.
- ElevenLabs API key stored only in `.env` or deployment secrets.
- Intent mapping defined: `live_transfer`, `callback_requested`, `not_interested`, `unknown`.
- Callback time capture rules defined: send callback time as text, not a recording.
- Transfer and callback payload contract defined in `docs/elevenlabs-integration-contract.md`.
- Fallback behavior defined for no answer, decline, timeout, and unknown intent.
- Current baseline tested before integration.
- Apps Script sheet updates verified.
- Callback email verified.

## Post-Integration Regression Tests

- Start a test call with `TEST_MODE=true`.
- Caller asks to speak with Yoni now; live transfer still bridges after Yoni accepts.
- Caller asks for a callback later; email includes callback time and conversation summary.
- Caller says they are not interested; sheet updates without email/transfer.
- Yoni does not answer; caller hears fallback and ASAP callback email sends.
- Unknown or unclear intent does not trigger a live transfer.
