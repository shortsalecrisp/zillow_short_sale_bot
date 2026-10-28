# Phone Identity And Terminal Declines

Cold-call attempt history is phone-specific. AG/AH and AN/AO remain immutable
historical row slots; changing C never relabels their destination or resets them.

- Queue starts persist `CODEX_VOICE_ATTEMPT_IDENTITY_V1` with normalized phone,
  slot and sent time before requesting a provider start. This is a reservation,
  not proof of an accepted call.
- Final `CODEX_VOICE_CALL_METRICS_V1` blocks bind each actual destination to its
  provider conversation. Only positive-duration final receipts contribute to
  `currentPhoneAttempts`. Former-number calls do not contribute to that count.
  Their phone identity still blocks a conflicting cadence on another listing;
  correcting C never deletes the former phone's duplicate protection.
- Changed/conflicting/malformed phone evidence holds the row for review. Missing
  historical attempt identity also prevents an automatic retry. Dry-runs expose
  `phoneIdentityReviewRequired`; neither the hold nor phone correction grants
  permission to contact the corrected number.
- Known identity mismatches cannot create a final-unanswered K=N closeout or an
  unplaced-call expiry. Existing K, callbacks, opt-outs and human takeover stay
  binding. Prior transport failures cannot release a different number.
- Legacy `CODEX_VOICE_CALL_START_RESOLUTION_V1` records also bind their original
  destination after unaccepted-start recovery clears its sent slot. They count
  as zero genuine calls, retain former-phone duplicate protection and hold a
  corrected number for review instead of inheriting old SMS/schedule proof.
- Final provider receipts are checked against fresh owning identity even when K
  is blank. A delayed old-number result cannot overwrite the corrected row.

Reconciliation requires exact receipt/phone ownership, current consent/campaign
and same-phone history review. Preserve real sent fields and provider evidence.
Do not automatically clear K, reset slots, create a third attempt, reuse old-number
SMS eligibility or remove a human preference. Any separately authorized future
contact must still satisfy the unchanged listing-local weekday windows, stable
lane, inbound quiet gate, duplicate locks and dispatch headroom.

Final transcript evidence takes priority over technical failure and generic
hangup labels. A contextual refusal after the completed help question or a
clarified service explanation closes as `answered_not_interested` / K=R. A bare
No to listing status, or a standalone refusal to transfer, is not such a decline.
Explicit callback consent, removal requests, existing preferences and takeover
remain protected. This backend classification changes no TTS arm, prompt, voice,
traffic allocation or comparison boundary.
