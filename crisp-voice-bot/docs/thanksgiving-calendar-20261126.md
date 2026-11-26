# Owner Thanksgiving Calendar Exception

Yoni approved on November 25, 2026: no scheduled phone calls on Thanksgiving,
November 26 listing-local date. Carry Thursday calls to Friday November 27 in
the same local windows, alongside Friday's ordinary queue. No additional attempts,
duplicate calls, provider tests, new capacity, wider windows or model changes.

The one-off date is excluded from native and backend scheduling helpers. The
backend checks the blackout again at the final start-call boundary, including
manual scheduled windows, so overdue or newly scheduled work cannot start Thursday.
The two-business-day missed-call deadline excludes the closed day rather than
prematurely expiring holiday-deferred work. Friday and all later dates retain
their existing rules. Incoming return calls and owner/caller safety controls are
not disabled. V4 remains 100%, Flash 0%, with exact r28 provider versions unchanged.

Six currently booked Thursday second attempts are carried forward by changing
only their AF schedule cells: rows 7151, 7152, 7154, 7156, 7158 and 7159. Keep their
first attempts, preferences, callback fields, current-phone history and two-attempt
cap unchanged. Preserve already scheduled Friday calls and normal fresh queue work.

The separately deployed Apps Script SMS holiday policy holds follow-up text
authorization until Friday 9:00 AM in each verified listing time zone. Initial
SMS and actual inbound replies retain their existing rules. Claim and final-send
checks cover already queued messages; sent/uncertain history is not reopened.

All checks are configuration, source, native Sheet and local-test evidence. No
test calls or test messages are authorized. Read fresh final organic receipts
after the holiday rather than claiming that schedules prove actual delivery.
