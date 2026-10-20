# Private Owner Call Announcements

Owner-approved October 20, 2026: identify the agent and full listing before
Yoni accepts return calls and live transfers. Do not change public caller
messaging, office destination, recording rules, consent, or the TTS cohort.

- Return calls look up the stored inbound caller number, never the office
  leg's From field. Read only matching Sheet1 records; normalize phones,
  reread each match's phone, and retain street number, unit, city and state.
- A unique matching record produces: "Crisp return call. Caller number
  matches agent [full name]. Listing: [full address]. Press 1 to connect."
  A phone match is not authenticated caller identity.
- Multiple agents or properties sharing a number are announced as ambiguous
  rather than selecting the newest row or guessing a property. Unknown,
  withheld and unavailable lookups remain answerable with explicit unknown
  context. The read is bounded to 2.5 seconds and must not break routing.
- Freeze the announcement in the existing durable per-parent return-call
  state, so callback replays cannot substitute a different contact. Play
  it only inside the office leg's private Gather, never on the caller leg.
  Press 1, no-answer voicemail and do-not-record behavior remain unchanged.
- Both live-transfer backend paths enrich the bot's name/address from the
  exact current row AND phone before dialing the owner. If that lookup is
  unavailable, preserve the existing call-specific payload without inventing
  missing city/state data or using a global latest-call context.
- The owner-only GET /return-calls/announcement-preview uses X-Crisp-Token,
  disables caching, and runs these same builders/lookups without calling,
  sending mail, or changing CRM/BotState. It is operational text/XML proof,
  not acoustic or real-phone end-to-end proof.
- Provider prompts, voices, models, tool configuration, cohort boundary,
  windows, retry policy and concurrency are unchanged. Full-name/address
  content is backend-owned and shared by both TTS arms.

Verification: ownerCallAnnouncement, returnCalls, returnCallFlow and
returnCallState regression tests; complete test suite and TypeScript build;
exact live revision, authenticated matched and unknown previews, anonymous
preview denial and unchanged provider comparison validation. No test phone
call is authorized or required for this release; next organic call remains
the phone-audio confirmation.
