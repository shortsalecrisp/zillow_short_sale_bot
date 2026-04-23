# Setup Checklist

Use this as the shared checklist for the Telnyx backend and the Apps Script callback.

## Telnyx Values

- `TELNYX_API_KEY` is set in local and deployed environments.
- `TELNYX_CALLER_ID` is the Telnyx voice caller ID.
- `TELNYX_CONNECTION_ID` matches the Telnyx Call Control connection.
- `TELNYX_OUTBOUND_VOICE_PROFILE_ID` is recorded for reference.
- `TELNYX_ALERT_FROM_NUMBER` is SMS-capable before SMS alerts are enabled.
- `TELNYX_MESSAGING_PROFILE_ID` is available if Telnyx requires it for alert SMS.

## Backend `.env`

- Copy `.env.example` to `.env`.
- Keep `.env` out of git.
- Set `BASE_URL` to the public backend URL.
- Keep `TEST_MODE=true` until production calling is explicitly approved.
- Set `TEST_DESTINATION_NUMBER` to the verified safe test number.
- Set `GOOGLE_APPS_SCRIPT_WEBHOOK_URL` after deploying the Apps Script web app.
- Set `GOOGLE_APPS_SCRIPT_TOKEN` to the same value as `VOICE_BOT_SHARED_TOKEN`.
- Keep `YONI_ALERT_SMS_ENABLED=false` until SMS alert testing is intentional.

## Apps Script

- Edit `apps-script/voice-bot-callback.gs` locally first.
- Copy the local Apps Script file into the bound Google Apps Script project.
- Set `VOICE_BOT_SHARED_TOKEN` in Apps Script to match `GOOGLE_APPS_SCRIPT_TOKEN`.
- Confirm columns O-X exist in the sheet and match the documented voice-call headers.
- Deploy or redeploy the Apps Script web app after every callback code change.
- Use the deployed web app URL as `GOOGLE_APPS_SCRIPT_WEBHOOK_URL`.

## Telnyx Webhook

- Confirm `BASE_URL` is public, such as an ngrok URL locally or the Render URL in production.
- The backend sends Telnyx the per-call webhook URL: `${BASE_URL}/telnyx/webhook`.
- If Telnyx dashboard-level webhook settings are used later, keep them aligned with the same endpoint.

## Local Checks

- Run `npm install` after dependency changes or on a fresh worktree.
- Run `npm run build` before handing off backend changes.
- Start locally with `npm run dev`.
- Test `/health` before placing a call.
- In test mode, verify outbound calls go only to `TEST_DESTINATION_NUMBER`.

## Moving Beyond Test Calls

- Keep `TEST_MODE=true` until the script, compliance checks, SMS alerts, and Apps Script writeback have been verified.
- Before switching to real lead calls, confirm consent/compliance rules, Telnyx account limits, and calling windows.
- Set `TEST_MODE=false` only after the production call path is approved.
- Review the transfer flow before replacing the simulated fallback with a real live transfer.
