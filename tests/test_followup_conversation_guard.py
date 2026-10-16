import ast
import json
import logging
import re
import sys
import types
import unittest
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple
from unittest.mock import Mock, patch


ROOT = Path(__file__).resolve().parents[1]
SOURCE = ast.parse((ROOT / "bot_min.py").read_text())
INITIAL = datetime(2026, 10, 13, 12, tzinfo=timezone.utc)


def harness(row, elapsed_hours):
    names = {"_follow_up_pass", "_followup_has_recorded_conversation"}
    functions = [node for node in SOURCE.body
                 if isinstance(node, ast.FunctionDef) and node.name in names]
    responses = [
        {"valueRanges": [{"values": [[INITIAL.isoformat()]]}, {"values": [[""]]}]},
        {"valueRanges": [{"values": [row]}]},
    ]
    batch = Mock()
    batch.return_value.execute.side_effect = responses
    service = Mock()
    service.spreadsheets.return_value.values.return_value.batchGet = batch
    service.spreadsheets.return_value.values.return_value.get.return_value.execute.return_value = {
        "values": [["agent_name", "last_name", "phone"]]
    }
    prepare = Mock()
    send = Mock()
    namespace = {
        "Any": Any, "Dict": Dict, "List": List, "Tuple": Tuple,
        "datetime": datetime, "re": re, "json": json,
        "LOG": logging.getLogger(__name__), "SCHEDULER_TZ": timezone.utc,
        "ws": types.SimpleNamespace(row_count=2), "sheets_service": service,
        "GSHEET_ID": "local-test-only", "GSHEET_TAB": "Sheet1",
        "COL_FIRST": 0, "COL_PHONE": 2, "COL_STREET": 4,
        "COL_REPLY_FLAG": 8, "COL_MANUAL_NOTE": 9, "COL_REPLY_TS": 10,
        "COL_MSG_ID": 11, "COL_CONVERSATION_SUMMARY": 12,
        "COL_AI_STATE": 13, "COL_HISTORY_JSON": 17,
        "COL_INIT_TS": 22, "COL_FU_TS": 23, "COL_PHONE_CONF": 24,
        "COL_CONTACT_REASON": 25, "MIN_COLS": 30, "FU_LOOKBACK_ROWS": 500,
        "FOLLOWUP_READ_END_COL": "AB", "FOLLOWUP_INCLUDE_WEEKENDS": False,
        "FOLLOWUP_DUE_GRACE_MINUTES": 2, "FU_HOURS": 6,
        "SMS_ENABLE": True, "SMS_TEST_MODE": False,
        "_col_index_to_letter": lambda index: "W" if index == 22 else "I",
        "_parse_sheet_datetime": datetime.fromisoformat,
        "_recover_initial_sent_at_from_receipts": Mock(return_value={}),
        "_redact_followup_phone": lambda phone: "test-phone",
        "_followup_has_contact_hold": Mock(return_value=False),
        "check_reply": Mock(return_value=False), "mark_reply": Mock(),
        "business_hours_elapsed": Mock(return_value=elapsed_hours),
        "os": types.SimpleNamespace(getenv=lambda key, default: "true"),
        "_current_sheet_phone_for_row": Mock(return_value="2125550100"),
        "_digits_only": lambda value: re.sub(r"\D", "", value),
        "_pace_followup_sms_batch": Mock(), "send_sms": send,
    }
    exec(compile(ast.Module(body=functions, type_ignores=[]), str(ROOT / "bot_min.py"), "exec"), namespace)
    module = types.ModuleType("video_followup")
    module.prepare_for_followup = prepare
    return types.SimpleNamespace(namespace=namespace, prepare=prepare, send=send, module=module)


def recipient():
    row = [""] * 30
    row[0], row[2], row[4], row[7], row[22], row[27] = (
        "Jim", "2125550100", "123 Main Street", "x", INITIAL.isoformat(), "listing-test")
    return row


class FollowupConversationGuardTests(unittest.TestCase):
    def setUp(self):
        for target in ("socket.socket.connect", "socket.socket.connect_ex", "socket.create_connection"):
            guard = patch(target, side_effect=AssertionError("Network is forbidden in follow-up unit tests"))
            guard.start()
            self.addCleanup(guard.stop)

    def run_pass(self, changes, elapsed_hours):
        row = recipient()
        for index, value in changes.items():
            row[index] = value
        h = harness(row, elapsed_hours)
        with patch.dict(sys.modules, {"video_followup": h.module}):
            h.namespace["_follow_up_pass"]()
        return h

    def test_recorded_conversation_never_prepares_or_sends_a_no_response_followup(self):
        cases = [
            {10: "H"}, {10: " h "},
            {13: "active"}, {13: "handoff"}, {13: " DONE "},
            {17: json.dumps([{"role": "agent", "text": "I already have help"}])},
            {17: json.dumps([{"role": "user", "content": "What is the fee?"}])},
            {12: "Spoke with agent; callback requested"},
            {12: "Talked to the seller about the short sale"},
            {12: "Agent requested a callback tomorrow"},
            {12: "The agent asked for a call"},
        ]
        for changes in cases:
            for elapsed_hours in (2, 6):
                with self.subTest(changes=changes, elapsed_hours=elapsed_hours):
                    h = self.run_pass(changes, elapsed_hours)
                    h.prepare.assert_not_called()
                    h.send.assert_not_called()
                    h.namespace["mark_reply"].assert_not_called()

    def test_operational_metadata_and_bot_only_history_do_not_suppress_followup(self):
        cases = [
            {}, {13: "new"}, {13: "initial"}, {13: "verifier_complete"},
            {17: "not valid JSON"}, {17: "{}"}, {17: "[]"},
            {17: json.dumps([{"role": "agent", "text": "  "}])},
            {17: json.dumps([{"role": "assistant", "text": "Initial introduction"}])},
            {17: json.dumps([{"role": "bot", "content": "Initial introduction"}])},
            {17: json.dumps([{"role": "system", "text": "Agent requested a callback"}])},
            {12: "Verifier checked the agent phone and listing"},
            {12: "Initial SMS queued; awaiting agent response"},
            {12: "No answer on first call"},
            {12: "No conversation yet; agent has not replied"},
            {9: "left voicemail"},
        ]
        for changes in cases:
            for elapsed_hours in (2, 6):
                with self.subTest(changes=changes, elapsed_hours=elapsed_hours):
                    h = self.run_pass(changes, elapsed_hours)
                    self.assertEqual(h.prepare.call_count, int(elapsed_hours < 6))
                    self.assertEqual(h.send.call_count, int(elapsed_hours >= 6))
                    h.namespace["mark_reply"].assert_not_called()

    def test_existing_suppression_and_single_followup_guards_are_unchanged(self):
        cases = [{10: status} for status in ("R", "Y", "G", "O")]
        cases += [{8: "x"}, {9: "Do not call"}, {23: "2026-10-13T18:00:00+00:00"}]
        for changes in cases:
            with self.subTest(changes=changes):
                h = self.run_pass(changes, 6)
                h.prepare.assert_not_called()
                h.send.assert_not_called()

    def test_existing_business_hour_due_threshold_is_unchanged(self):
        for hours, expected_send in ((5.95, False), (5.97, True), (6, True)):
            with self.subTest(hours=hours):
                h = self.run_pass({}, hours)
                self.assertEqual(h.send.call_count, int(expected_send))
                self.assertEqual(h.prepare.call_count, int(not expected_send))
                self.assertFalse(h.namespace["business_hours_elapsed"].call_args.kwargs["include_weekends"])


if __name__ == "__main__":
    unittest.main()
