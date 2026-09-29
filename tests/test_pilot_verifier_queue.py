import datetime as dt
import json
import unittest
from unittest import mock

import pilot_verifier_queue as queue


class PilotVerifierQueueTest(unittest.TestCase):
    def setUp(self):
        self.now = dt.datetime(2026, 9, 29, 17, 0, tzinfo=dt.timezone.utc)
        self.payload = {
            "action": "preview",
            "automation_id": "lead-verifier-8-am",
            "run_date": "2026-09-29",
            "global_sms_blockers": [],
        }
        self.row = {
            "request_id": "queue-request-123456",
            "submitted_at": self.now.isoformat(),
            "automation_id": "lead-verifier-8-am",
            "payload_json": json.dumps(self.payload),
            "status": "pending",
            "claimed_at": "",
            "completed_at": "",
            "result_json": "",
            "error": "",
        }

    def test_mapped_updates_use_exact_queue_columns(self):
        updates = queue._mapped_updates(2, {"status": "processing", "error": ""})
        self.assertEqual(
            updates,
            [
                {"range": "'Pilot Verifier Queue'!E2", "values": [["processing"]]},
                {"range": "'Pilot Verifier Queue'!I2", "values": [[""]]},
            ],
        )
        with self.assertRaisesRegex(ValueError, "unknown_queue_field"):
            queue._mapped_updates(2, {"unapproved": "value"})

    def test_queue_tab_is_created_hidden_with_exact_headers(self):
        with mock.patch.object(queue, "_sheet_properties", return_value=None), \
             mock.patch.object(queue.pilot, "sheets_request") as request, \
             mock.patch.object(queue.pilot, "update_values") as update:
            queue.ensure_queue_tab("token", "sheet")
        request.assert_called_once()
        properties = request.call_args.args[3]["requests"][0]["addSheet"]["properties"]
        self.assertTrue(properties["hidden"])
        self.assertEqual(
            properties["gridProperties"]["columnCount"],
            len(queue.QUEUE_HEADERS),
        )
        update.assert_called_once_with(
            "token",
            "sheet",
            "'Pilot Verifier Queue'!A1:I1",
            [queue.QUEUE_HEADERS],
        )

    def test_existing_queue_fails_closed_on_schema_drift(self):
        with mock.patch.object(queue, "_sheet_properties", return_value={"sheetId": 1, "hidden": True}), \
             mock.patch.object(queue.pilot, "get_values", return_value=[["wrong"]]):
            with self.assertRaisesRegex(ValueError, "schema_drift"):
                queue.ensure_queue_tab("token", "sheet")

    def test_pending_request_executes_contract_and_persists_completion(self):
        result = {"ok": True, "preview": True, "writes": 0, "sends": 0}
        with mock.patch.object(queue, "ensure_queue_tab"), \
             mock.patch.object(queue, "read_queue", return_value=[(2, self.row)]), \
             mock.patch.object(queue, "_claim_request", return_value=True) as claim, \
             mock.patch.object(queue, "_finish_request") as finish, \
             mock.patch("pilot_verifier_contract.handle", return_value=result) as handle:
            stats = queue.process_pending("token", "sheet", now=self.now)
        self.assertEqual(stats, {"pending": 1, "claimed": 1, "completed": 1, "failed": 0})
        claim.assert_called_once()
        handle.assert_called_once_with("token", "sheet", self.payload)
        self.assertEqual(finish.call_args.kwargs["status"], queue.COMPLETED)
        self.assertEqual(finish.call_args.kwargs["result"], result)

    def test_contract_rejection_is_a_terminal_failed_queue_row(self):
        with mock.patch.object(queue, "ensure_queue_tab"), \
             mock.patch.object(queue, "read_queue", return_value=[(2, self.row)]), \
             mock.patch.object(queue, "_claim_request", return_value=True), \
             mock.patch.object(queue, "_finish_request") as finish, \
             mock.patch("pilot_verifier_contract.handle", side_effect=ValueError("pilot_changed")):
            stats = queue.process_pending("token", "sheet", now=self.now)
        self.assertEqual(stats, {"pending": 1, "claimed": 1, "completed": 0, "failed": 1})
        self.assertEqual(finish.call_args.kwargs["status"], queue.FAILED)
        self.assertIn("pilot_changed", finish.call_args.kwargs["error"])

    def test_payload_automation_must_match_queue_owner(self):
        row = dict(self.row, automation_id="lead-verifier-11-am")
        with self.assertRaisesRegex(ValueError, "queue_automation_mismatch"):
            queue._validated_payload(row)


if __name__ == "__main__":
    unittest.main()
