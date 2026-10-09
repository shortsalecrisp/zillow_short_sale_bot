# Approved Call and Reply Memory

Yoni approved the October 9 weekly review recommendations, five review decisions,
and reusable script additions in the Crisp Call + Reply Learning Loop chat.

- Keep Maya/Eryn, Flash v2 versus v4 Turbo at 50/50, both turn_v3. No winner until
  each arm has at least 15 confirmed live-agent conversations. Separate policy,
  opener, local window and lead-quality strata; playback is needed for audio claims.
- Alert Yoni when three distinct zero-duration provider starts fail within 30
  minutes. Limit reminders to once per hour; retry failed alert delivery after ten
  minutes. This monitor starts fresh after a process restart and does not pause or
  retry calls. Keep the existing quota, LLM and provider-circuit alerts.
- Report target agents, live admins, apparent humans, screeners, voicemail and IVR
  separately. A recorded opt-out is not a target agent's request.
- Record callback consent, request receipt and completed callback separately.
  A tool call or a status label is insufficient. Keep the caller's actual timing;
  ask once if none was given, preserve unspecified timing when unresolved, and use
  ASAP only when requested or accepted. A callback request is not an appointment.
- Confirmed wrong listing or unrelated contact: "Sorry, I reached the wrong
  contact. Thanks for correcting me." Answer pending questions and wait for the
  caller. Preserve the guarded ending; a correction is not a future opt-out.
- Self-handler who remains open: one value line about taking lender paperwork and
  follow-up off their plate while they keep the listing and client relationship.
  Do not repeat the value pitch after a refusal.
- Already has help and needs nothing: acknowledge and close respectfully under
  the existing ending guard. Email-only can be a correct result.
- Approved standard fee memory: $5,000, typically paid by the buyer at closing.
  Confirm file-specific terms through Yoni; do not guarantee approval, lender net,
  commission, or an unchanged buyer budget. This memory approval does not turn a
  generic standard fee into a verified quote for every file.
- "Are you the lender?": "No, we're a separate short-sale processing service."
  Crisp helps prepare paperwork and follows up with the lender.

Evidence reviewed: callback request at Sheet1 row 5998; screeners at rows 5973,
5987, 6000, 6010 and 6027; wrong-listing conversation at row 6010 on October 9;
respectful completed-paperwork closeout at row 5960; self-handler decline at row
6033. Each call must be reviewed independently; a row can contain multiple calls.

Live provider readback during implementation found different existing prompts:
Flash version agtvrsn_6001m4gap3b7e6d8p8n5rw1j7fbg and v4 version
agtvrsn_1001m420dgt0esja98740xzrpb91. Apply only the identical approved wrong-listing
section to each branch's existing prompt using the prompt-only release guard.
Preserve all other prompt text and protected provider settings. The underlying
prompt mismatch remains an experiment confound; do not call the arms comparable
based on shared code policy labels alone. Verify final provider versions per call.
