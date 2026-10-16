"""Offline scheduler parity with the authoritative Apps Script inbound guard."""

import sys
import types
from unittest.mock import patch

import pytest

from test_followup_conversation_guard import harness, recipient


def column_letter(index):
    result = ""
    index += 1
    while index:
        index, remainder = divmod(index - 1, 26)
        result = chr(ord("A") + remainder) + result
    return result


def run_pass(*, columns=None, headers=None, elapsed_hours=6, header_response=None, header_error=None,
             raw_columns=None, control_response=None, control_error=None):
    row = recipient() + [""] * 40
    for index, value in (columns or {}).items():
        row[index] = value
    if headers is None:
        headers = [""] * 45
        headers[0], headers[2] = "agent_name", "phone"
        headers[20] = headers[21] = "last_message_id"
        headers[43], headers[44] = "last_inbound_text", "last_inbound_at"
    h = harness(row, elapsed_hours)
    h.namespace["_col_index_to_letter"] = column_letter
    h.api = h.namespace["sheets_service"].spreadsheets().values()
    h.api.get.return_value.execute.return_value = (
        header_response if header_response is not None else {"values": [headers]}
    )
    control_indices = {
        header: index for index, header in enumerate(headers)
        if header in {"last_message_id", "last_inbound_text"}
    }
    raw_row = dict(columns or {}) if raw_columns is None else raw_columns
    raw_response = {"valueRanges": [
        {"values": [[raw_row.get(index, "")]]} for index in control_indices.values()
    ]}
    responses = list(h.api.batchGet.return_value.execute.side_effect)
    responses.append(control_error or (control_response if control_response is not None else raw_response))
    h.api.batchGet.return_value.execute.side_effect = responses
    if header_error:
        h.api.get.return_value.execute.side_effect = header_error
    with patch.dict(sys.modules, {"video_followup": h.module}):
        h.namespace["_follow_up_pass"]()
    return h


@pytest.fixture(autouse=True)
def no_network():
    with patch("socket.socket.connect", side_effect=AssertionError("Network is forbidden")), \
            patch("socket.create_connection", side_effect=AssertionError("Network is forbidden")):
        yield


def assert_no_followup_side_effects(h):
    h.prepare.assert_not_called()
    h.send.assert_not_called()
    h.namespace["check_reply"].assert_not_called()
    h.namespace["mark_reply"].assert_not_called()
    h.namespace["_pace_followup_sms_batch"].assert_not_called()
    h.namespace["_current_sheet_phone_for_row"].assert_not_called()
    h.api.batchUpdate.assert_not_called()


@pytest.mark.parametrize("elapsed_hours", [1, 6, 24])
@pytest.mark.parametrize("columns", [
    {43: "2026-10-09T17:10:23-04:00", 44: "lead-verifier-5-pm"},
    {43: "Please call me tomorrow"},
    {21: "inbound-message-id"},
    {21: 12345},
])
def test_same_inbound_controls_block_preparation_and_enqueue_without_reinterpreting_values(columns, elapsed_hours):
    h = run_pass(columns=columns, elapsed_hours=elapsed_hours)
    assert_no_followup_side_effects(h)
    assert h.api.batchGet.call_args_list[1].kwargs["ranges"] == ["Sheet1!A2:AB2"]
    assert h.api.batchGet.call_args.kwargs == {
        "spreadsheetId": "local-test-only", "ranges": ["Sheet1!V2:V2", "Sheet1!AR2:AR2"],
        "majorDimension": "COLUMNS", "valueRenderOption": "UNFORMATTED_VALUE",
    }


@pytest.mark.parametrize("value", ["", " \t ", False, 0, None])
def test_blank_controls_preserve_normal_legacy_reminder(value):
    h = run_pass(columns={21: value, 43: value})
    h.send.assert_called_once()
    h.namespace["mark_reply"].assert_not_called()
    h.api.batchUpdate.assert_not_called()


def test_timestamp_control_alone_does_not_create_a_new_policy():
    h = run_pass(columns={44: "2026-10-16T14:00:00Z"})
    h.send.assert_called_once()


@pytest.mark.parametrize("last_value,blocked", [("", False), ("inbound-id", True)])
def test_duplicate_headers_use_last_occurrence_exactly_like_apps_script(last_value, blocked):
    h = run_pass(columns={20: "stale-first-duplicate", 21: last_value})
    if blocked:
        assert_no_followup_side_effects(h)
    else:
        h.send.assert_called_once()


def test_moved_control_headers_extend_bounded_read_without_hard_coded_offsets():
    headers = [""] * 61
    headers[0], headers[2] = "agent_name", "phone"
    headers[55] = "last_inbound_text"
    headers[60] = "last_message_id"
    h = run_pass(headers=headers, columns={55: "Yes, interested"})
    assert_no_followup_side_effects(h)
    assert h.api.batchGet.call_args_list[1].kwargs["ranges"] == ["Sheet1!A2:AB2"]
    assert h.api.batchGet.call_args.kwargs["ranges"] == ["Sheet1!BD2:BD2", "Sheet1!BI2:BI2"]


def test_absent_optional_headers_preserve_existing_read_bound_and_behavior():
    h = run_pass(headers=["agent_name", "last_name", "phone"], columns={43: "unmapped"})
    h.send.assert_called_once()
    assert h.api.batchGet.call_args.kwargs["ranges"] == ["Sheet1!A2:AB2"]


@pytest.mark.parametrize("response", [
    {}, {"values": []}, {"values": [[]]}, {"values": [[""]]},
    {"values": "bad"}, {"values": ["bad"]}, {"values": [[{}]]},
    {"values": [["phone"], ["unexpected second row"]]},
])
def test_unreadable_or_malformed_headers_fail_closed(response):
    h = run_pass(header_response=response)
    assert_no_followup_side_effects(h)
    h.api.batchGet.assert_not_called()


def test_header_read_error_fails_closed_before_any_candidate_action():
    h = run_pass(header_error=RuntimeError("read-only test failure"))
    assert_no_followup_side_effects(h)
    h.api.batchGet.assert_not_called()


@pytest.mark.parametrize("raw,display", [(False, "FALSE"), (0, "0")])
def test_formatted_false_and_zero_do_not_create_new_inbound_suppression(raw, display):
    h = run_pass(columns={21: display, 43: display}, raw_columns={21: raw, 43: raw})
    h.send.assert_called_once()


@pytest.mark.parametrize("index", [21, 43])
def test_display_format_cannot_hide_a_nonempty_raw_inbound_control(index):
    h = run_pass(columns={index: ""}, raw_columns={index: "actual-inbound-control"})
    assert_no_followup_side_effects(h)


@pytest.mark.parametrize("response", [
    {}, {"valueRanges": []}, {"valueRanges": [{}, {}, {}]},
    {"valueRanges": [{"values": None}, {}]},
    {"valueRanges": [{"values": "bad"}, {}]},
    {"valueRanges": [{"values": ["bad"]}, {}]},
    {"valueRanges": [{"values": [[{}]]}, {}]},
    {"valueRanges": [{"values": [["too", "long"]]}, {}]},
    {"valueRanges": [{"values": [["one"], ["two"]]}, {}]},
    {"valueRanges": [None, {}]},
])
def test_malformed_raw_control_read_fails_closed(response):
    h = run_pass(control_response=response)
    assert_no_followup_side_effects(h)


def test_raw_control_read_error_fails_closed():
    h = run_pass(control_error=RuntimeError("raw controls unavailable"))
    assert_no_followup_side_effects(h)


@pytest.mark.parametrize("blank_range", [{}, {"values": []}, {"values": [[]]}])
def test_google_omitted_blank_raw_cells_preserve_eligibility(blank_range):
    h = run_pass(control_response={"valueRanges": [blank_range, blank_range]})
    h.send.assert_called_once()


def test_current_control_recovery_does_not_create_a_permanent_superseded_dedupe_ban():
    blocked = run_pass(columns={43: "2026-10-09T17:10:23-04:00"})
    assert_no_followup_side_effects(blocked)
    cleared = run_pass()
    cleared.send.assert_called_once()
    cleared.api.batchUpdate.assert_not_called()


def test_bounded_raw_column_span_maps_discontiguous_candidates_to_their_own_cells():
    first, last = recipient(), recipient()
    last[0] = "Jane"
    h = harness(first, 6)
    h.namespace["_col_index_to_letter"] = column_letter
    h.namespace["ws"] = types.SimpleNamespace(row_count=5)
    api = h.namespace["sheets_service"].spreadsheets().values()
    headers = ["agent_name"] + [""] * 43
    headers[21], headers[43] = "last_message_id", "last_inbound_text"
    api.get.return_value.execute.return_value = {"values": [headers]}
    api.batchGet.return_value.execute.side_effect = [
        {"valueRanges": [
            {"values": [[first[22], "", "", last[22]]]}, {"values": [[]]},
        ]},
        {"valueRanges": [{"values": [first]}, {"values": [last]}]},
        {"valueRanges": [{}, {"values": [["", "not-a-candidate", "", "actual-reply"]]}]},
    ]
    with patch.dict(sys.modules, {"video_followup": h.module}):
        h.namespace["_follow_up_pass"]()
    assert api.batchGet.call_args.kwargs["ranges"] == ["Sheet1!V2:V5", "Sheet1!AR2:AR5"]
    h.send.assert_called_once()
    assert h.send.call_args.kwargs["row_idx"] == 2
    h.namespace["mark_reply"].assert_not_called()
    api.batchUpdate.assert_not_called()
