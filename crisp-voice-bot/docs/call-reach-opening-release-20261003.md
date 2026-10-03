# Approved reach and opening release

## Dispatch contract

- Cold outreach uses the listing's resolved city/state IANA timezone, not the agent's phone area code or a universal Eastern clock.
- Weekdays only: `reach_morning_v1` 09:15 inclusive to 09:45 exclusive; `reach_afternoon_v1` 15:00 inclusive to 15:45 exclusive.
- First attempts use a stable normalized-phone hash with equal assignment probability. This is not a promise of exactly equal daily counts.
- One cold retry uses the opposite actual first-call time bucket after two business days. Definitively failed provider starts retain separate one-business-day recovery. Uncertain starts must reconcile before retry.
- Phone-level preferences, terminal status, active calls and attempt history apply across duplicate rows. Existing Mailshake and inbound-quiet protections remain in place.
- Unresolved or conflicting listing locations are held, never silently assigned Eastern. Inspect `timeZoneReviewRequired` in the read-only queue dry run. See timezone provenance and boundary limitations in `config/listing-timezones-provenance.md`.
- Broader `morning_probe` and `mid_afternoon` route guard names remain for existing authorized non-cold paths; they are not the new cold queue's assignment windows.
- Render owns dispatch. Do not unpause the separate legacy Apps Script queue or create a second dispatcher.

## Conversation contract

Policy `maya-service-first-recovery-20261003` updates the root prompt AND the effective opening-listener override. Delayed node transitions cannot revert to an older introductory script. A listing confirmation is not interest or transfer consent.

Identity, service purpose, affiliation and audibility questions require targeted answers. Repeated confusion does not authorize hanging up. Explicit stop requests and independently guarded transfer/ending tools retain priority.

Maya/Eryn remains the only production voice. The Flash v2/v4 split and turn_v3, voice speed, ASR and timeout settings are unchanged. An opening-only release must preserve every protected provider setting and verify both branch readbacks with `syncElevenLabsOpeningRelease.ts`.

## Measurement contract

New V1 metrics carry `measurementRevision: contact-evidence-v2`. Prefer `contactEvidence`, separating apparent humans, verified target identity, admins, screeners, IVR and voicemail. Preserve unknown evidence as unknown. Historical speech-presence `liveAnswered` flags must not be used as verified human answers; reclassify old transcripts when comparing periods.

Transcript-derived timing does not measure perceived audio latency or prove interruption quality. Isolated WebSocket audio tests do not validate the Telnyx telephone path, carrier caller-name display, or return-call routing.

## Related approved SMS change

The existing follow-up identifies Maya as automated and names the actual calling number. It offers information by email or a requested human call with Yoni, not a guaranteed bot appointment. No extra SMS batch is introduced. Channel preferences and opt-outs must be persisted before any further cold call.

Deploy Apps Script from the freshly read live source, merging only the reviewed scheduler and SMS changes plus the generated timezone helper. Preserve all unrelated files, credentials, manifest and the legacy queue's existing pause. Verify immutable version and active deployment equality.
