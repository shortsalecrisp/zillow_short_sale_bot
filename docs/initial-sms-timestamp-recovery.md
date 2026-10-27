# Initial SMS timestamp recovery

Column W must describe a confirmed handset send, never an outbox creation time
or its recipient-local `not_before` time. Initial enqueues do not mark H/W/L.
The handset receipt owns those fields. Verifier reconciliation must use that
exact current-phone receipt, not the request's creation or scheduled time.

The follow-up scheduler now repairs the narrow historical error where an ISO W
marker equals a unique initial request's creation or not-before time but precedes
its confirmed send interval. It requires the same current phone and stable listing
ID, valid deterministic request/message IDs, a sole sent initial attempt, and no
existing scheduled follow-up attempt. It rechecks the complete A:AB row before
writing only W and the evidence note, then rereads W. Competing or uncertain
attempts, changed contacts, arbitrary timestamps and follow-up history stay held.

The previous W value is retained as `initial_sms_reply_cutoff` in Z. Reply checks
use the earliest saved cutoff so correcting a timestamp cannot hide an earlier
reply. Receipt interval validation and video/text selection remain unchanged.

Authorized bounded recovery uses `POST /internal/reconcile-followup-sms` with
the existing automation bearer token and `{"rows":[6413,6414]}`. At most twenty
operational row pointers are accepted. This takes the existing scheduler lock,
evaluates only those rows through the standard eligibility/duplicate/video policy,
and enforces 08:00 inclusive to 20:00 exclusive in the recipient's state timezone.
It does not queue after hours; the normal next hourly window owns deferred work.
The response reports evaluation only. Verify exact outbox `sent` and fresh I/X/L
before reporting a follow-up sent. Never resend the initial message for recovery.
