import copy
import json
from datetime import datetime

import pytest

from initial_sms_timestamp import receipt_time_for_queue_marker, recipient_zone, within_recipient_hours


def parse(value):
    return datetime.fromisoformat(value) if value else None


def fixture():
    row = [""] * 28
    row[2], row[7], row[22], row[27] = "904-541-0180", "x", "2026-10-25T08:01:21-04:00", "44486664"
    e = [""] * 19
    e[0], e[1], e[2], e[3], e[4] = row[22], "sent", "scheduler-initial-6412-9df9ef38c2fc8954", "initial-6412-9df9ef38c2fc8954", "9045410180"
    e[6] = "__initial_outreach__:" + json.dumps({"crm_row": 6412, "stable_id": row[27]})
    e[8], e[13], e[14], e[17] = row[22], "2026-10-25T08:02:21-04:00", "2026-10-25T08:02:30-04:00", 6412
    return row, e


def test_incident_queue_time_recovered_only_from_exact_sent_receipt():
    row, e = fixture()
    assert receipt_time_for_queue_marker(6412, row, [e], parse) == parse(e[14])
    # Deferred Pacific recipients had W=not_before, not created_at.
    e[0] = "2026-10-25T05:01:21-04:00"
    assert receipt_time_for_queue_marker(6412, row, [e], parse) == parse(e[14])


@pytest.mark.parametrize("column,value", [(1,"queued"),(1,"send_started"),(4,"1112223333"),
    (6,'__initial_outreach__:{"crm_row":6412,"stable_id":"other"}'),(17,"bad"),
    (13,"2026-10-25T08:00:00-04:00"),(14,"2026-10-24T08:02:30-04:00")])
def test_uncertain_mismatched_or_invalid_attempt_never_recovers(column, value):
    row, e = fixture()
    e[column] = value
    assert receipt_time_for_queue_marker(6412, row, [e], parse) is None


def test_arbitrary_marker_multiple_initials_and_existing_followup_never_recover():
    row, e = fixture()
    changed = row.copy()
    changed[22] = "2026-10-25T08:01:45-04:00"
    assert receipt_time_for_queue_marker(6412, changed, [e], parse) is None
    assert receipt_time_for_queue_marker(6412, row, [e, copy.deepcopy(e)], parse) is None
    followup = e.copy()
    followup[6] = "__scheduled_followup__:{}"
    assert receipt_time_for_queue_marker(6412, row, [e, followup], parse) is None
    row[8] = "x"
    assert receipt_time_for_queue_marker(6412, row, [e], parse) is None


def test_recovery_hours_use_recipient_zone_and_fail_closed_for_unknown_state():
    assert recipient_zone("WA") == "America/Los_Angeles"
    assert recipient_zone("FL") == "America/New_York"
    assert recipient_zone("") is None
    assert within_recipient_hours("WA", parse("2026-10-27T00:30:00+00:00"))
    assert not within_recipient_hours("FL", parse("2026-10-27T00:00:00+00:00"))
    assert within_recipient_hours("FL", parse("2026-10-27T12:00:00+00:00"))
    assert not within_recipient_hours("FL", parse("2026-10-27T11:59:59+00:00"))
    assert not within_recipient_hours("", parse("2026-10-27T12:00:00+00:00"))
