# Approved SMS focus-group rollout

## Final opener

Hey [First Name], this is Yoni Kutler with Crisp Short Sales. I saw [Property Address] listed as a short sale. I can take the lender paperwork, calls, and follow-up off your plate. There's no service fee to you or the seller; my fee is buyer-paid at closing. Are you handling the lender side yourself, or do you already have help?

This is the selected A opener with Yoni's final "I can take" revision. It replaces the shared initial-SMS template for future outreach. Existing recorded initial messages and queued text are not rewritten or resent.

## Approved response coverage

The earlier approved reply release and this final opener release together cover:

| Area | Final behavior |
| --- | --- |
| Handling it themselves | Acknowledge their position, preserve listing/client control, explain lender-side relief and buyer-paid fee, offer one brief call. Do not repeat the pitch after a refusal. |
| Exact fee question | Disclose $5,000 immediately when the amount is asked, buyer-paid at closing only if the deal closes, no service fee to agent/seller or commission split. |
| How Crisp gets paid | Answer payer and closing contingency. New payer/commission or buyer-cost questions are not automatically repeated amount questions. |
| Buyer-cost concern | Acknowledge possible offer economics and discuss disclosure before deciding. No buyer-acceptance assurance. |
| Prior failed provider | Acknowledge the bad experience and explain specific responsibilities before inviting a brief call. |
| Experience | Use the owner-approved 15+ years and exclusive short-sale focus; no invented recent closing count or guarantee. Unknown counts retain named human follow-up. |
| Multiple questions | Combine supported answers rather than dropping the material questions or treating question count alone as a handoff. |
| Documents and speed | Preserve useful process answers; only filter unsupported delivery promises, not ordinary document-related words. |
| Buyer requests | Clearly say Crisp does not bring the buyer; remove the contradictory buyer-finding pitch, including its old sanitizer exception. |
| Equator | Offer help managing lender-side tasks and communication, not all tasks in the system. Refusal still takes priority. |
| Email requests | Collect missing email or acknowledge the requested overview only after the durable approval request exists. Keep owner approval and the approved sender/template. Email-only/future interest is not call consent. |
| Who is this? | Repeat the actual recorded initial message. If unavailable, reconstruct the approved opener using known name/address only. |
| New handoff | Allow an approved safe answer with a newly queued handoff when appropriate; never reopen an existing manual takeover. |
| Callback/deadline | Preserve full requested callback date/time/qualifier/timezone; auction dates alone are not call consent. |
| Refusals and opt-outs | Preserve R/O distinctions and silent removal requests. Reply cap limits text, not terminal classification. Negated rejection is not rejection. |
| Repetition | Use delivered response evidence, not queued text, for fee progression and one-time self-handling replies. |

## Preserved boundaries

No Tasker XML, profile, task, AutoRemote setting, phone configuration, credential, endpoint, transport payload, retry schedule, business hours, follow-up timing, or reply-limit changes. No historical replay, manual SMS, test SMS, CRM edits, or agent email sends are part of this release.

Existing good location, number-confirmation, not-a-short-sale apology, no-help, client-consultation, Spanish truthfulness, and unusual-request policies remain in place. Alternative focus-group drafts and the separately proposed AI-transparency/public-business-card policies are not silently substituted for the selected recommendations.

## Verification

The offline tests exercise the actual Apps Script decision pipeline and the Python webhook with external services replaced by test doubles. They cover message text, state, handoffs, approval-queue effects, delivered-response history, cap/opt-out/manual-lock boundaries, and the exact shared initial template.

This validates the selected code paths, not real carrier delivery or conversion improvement. The focus group was simulated, not a measured real-agent conversion experiment. Production completion requires active Apps Script immutable-source readback and Render live-commit/health verification; release evidence is stored separately under `output/releases/2026-09-12-final-sms-opener/`.
