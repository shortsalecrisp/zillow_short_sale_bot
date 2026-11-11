# Phone Callback Owner Alerts

Production Maya/ElevenLabs callback notifications use `voiceCallbackAlerts`, independently
of the optional all-call transcript-email toggle. A live callback tool first persists a
minimal, conversation-bound notification candidate. After a genuine final receipt, the
worker independently verifies current row/phone/attempt identity and actual caller words.
Post-call fallback also captures requests whose callback tool never fired, before the
generic interest/hangup classifications can consume them.

The email contains the contact, phone, listing, caller-requested timing, consent wording,
summary, transcript and protected recording link. Timing is not a booked appointment;
no absent AM/PM, date, zone or ASAP is invented. Caller-initiated future contact, recordings,
revocations and information-only preferences cannot authorize a callback alert. New
suppression or pending inbound blocks sending. Human takeover is retained; an internal
owner notice never places outreach or changes takeover fields.

BotState prefix `voice_callback_owner_alert_v1:` holds notification work, not lead
eligibility. A per-conversation lock and durable `sending` claim precede SMTP. Only explicit
recipient acceptance is recorded as `accepted`; that is not inbox-delivery proof. An
uncertain send or interrupted `sending` claim is never automatically resent. Reconcile
the deterministic Message-ID before any manually approved recovery. Restart recovery
polls only this new outbox every 60 seconds and never runs a call queue. Production must
remain single-instance because BotState does not provide compare-and-swap.

The authenticated `/callback-alerts/status` surface shows readiness, configured owner,
immutable activation boundary, delivery counts and exceptions. `/callback-alerts/activate`
requires explicit activation and creates that boundary once. Pre-activation calls require
duplicate review rather than automatic historical backfill. This email-only boundary does
not reset the r19 TTS cohort. No provider configuration, cold calling window, call capacity,
two-attempt cap, automation schedule or notification preference is changed.

The owner-approved `/callback-alerts/self-test` uses fixed synthetic content and a single
durable test identifier. It requires the configured recipient to match the approved owner
recipient, cannot address a lead, changes no lead rows and places no calls. Do not repeat
an uncertain response. Verify the exact Message-ID in the owner's mailbox.

The older keypad/Telnyx-only implementation remains explicitly named
`sendLegacyTelnyxCallbackEmail`; it is not used by production Maya and is not enabled by
this release. No fallback or provider expansion is authorized by this email repair.
