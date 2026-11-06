from copy import deepcopy
from datetime import datetime

import pytest

from active_file_outreach_exception import (
    EXCEPTION_ID,
    approved_active_file_outreach_exception,
    approved_exception_initial_window_open,
)


def approved_row():
    row = [""] * 43
    row[:7] = ["Ashleigh", "Camberg", "360-513-9034", "acamberg@pcshomegroup.com",
               "9336 Pacific Highway SE", "Olympia", "WA"]
    row[27] = "49354863"
    return row


def approved_lookup():
    return {"configured": True, "active_file": True, "match": {
        "fileId": "cmp1zhrd6026uky04so10navp", "reference": "V1-2000690",
        "propertyAddress": "14471 100th Ave SE,", "matchReasons": ["agent_email"],
    }}


def test_exact_owner_exception_retains_actual_relationship_and_is_row_number_independent():
    row, lookup = approved_row(), approved_lookup()
    before = deepcopy(lookup)
    assert approved_active_file_outreach_exception(row, lookup) == EXCEPTION_ID
    assert lookup == before and lookup["active_file"] is True
    row[2] = "+1 (360) 513-9034"
    row[3] = "Acamberg@pcshomegroup.com"
    assert approved_active_file_outreach_exception(row, lookup) == EXCEPTION_ID


@pytest.mark.parametrize("index,value", [
    (0, "Another"), (1, "Other"), (2, "3605139035"), (3, "other@example.com"),
    (4, "9338 Pacific Highway SE"), (5, "Yelm"), (6, "OR"), (27, "different"),
])
def test_any_changed_identity_stays_suppressed(index, value):
    row = approved_row()
    row[index] = value
    assert not approved_active_file_outreach_exception(row, approved_lookup())


@pytest.mark.parametrize("key,value", [
    ("fileId", "another-file"), ("reference", "another-reference"),
    ("propertyAddress", "9336 Pacific Highway SE"),
    ("matchReasons", ["property_address"]), ("matchReasons", ["agent_phone"]),
    ("matchReasons", ["agent_email", "property_address"]),
    ("matchReasons", []), ("matchReasons", "agent_email"),
])
def test_any_changed_or_property_match_stays_suppressed(key, value):
    lookup = approved_lookup()
    lookup["match"][key] = value
    assert not approved_active_file_outreach_exception(approved_row(), lookup)


@pytest.mark.parametrize("change", [{"unavailable": True}, {"configured": False},
                                    {"active_file": False}, {"match": None}])
def test_missing_or_unavailable_lookup_never_uses_exception(change):
    lookup = {**approved_lookup(), **change}
    assert not approved_active_file_outreach_exception(approved_row(), lookup)


@pytest.mark.parametrize("stamp,expected", [
    ("2026-11-06T15:59:59+00:00", False), ("2026-11-06T16:00:00+00:00", True),
    ("2026-11-07T03:59:59+00:00", True), ("2026-11-07T04:00:00+00:00", False),
    ("2026-11-07T18:00:00+00:00", False), ("2026-11-08T18:00:00+00:00", False),
    ("2026-07-17T14:59:59+00:00", False), ("2026-07-17T15:00:00+00:00", True),
    ("2026-11-06T08:00:00", False),
])
def test_pacific_initial_window_keeps_weekdays_boundaries_and_dst(stamp, expected):
    assert approved_exception_initial_window_open(datetime.fromisoformat(stamp)) is expected
