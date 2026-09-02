import ast
import datetime as dt
import json
from pathlib import Path
import unittest
from unittest import mock

import pilot_verifier_contract as contract


class PilotVerifierContractTest(unittest.TestCase):
    def setUp(self):
        self.now = dt.datetime(2026, 8, 31, 12, 25, tzinfo=dt.timezone.utc)
        self.source = [(2, dict(schedule_slot_id="source:2026-08-31", run_mode="scheduled_source",
                                status="completed", pipeline_complete="true", observed_at="2026-08-31T11:10:00Z"))]
        self.row = dict(first_seen_at="2026-08-31T11:02:00Z", synthetic_zpid="free-12345678",
                        first_name="Jane", last_name="Agent", phone="5552223333", email="jane@example.test",
                        listing_address="123 Main Street", city="Dover", state="DE", status="qualified",
                        promotion_status="promoted", import_ready="promoted", matched_main_row="22",
                        promotion_notes="Promoted by Lead verifier 8 AM after owner readback",
                        pending_queue_listing_json=json.dumps({"sourceEvidenceState": "durable_reopenable", "sourceEvidenceReceipt": "evidence-1"}))
        self.owner = dict(agent_name="Jane", last_name="Agent", phone="5552223333", email="jane@example.test",
                          listing_address="123 Main Street", city="Dover", state="DE", created_at="free-12345678")

    def build(self, rows=None, receipts=None, blockers=None, **kwargs):
        return contract.build_receipt(pilot_rows=[(1172, self.row)] if rows is None else rows,
            main_rows=[(22, self.owner)], receipts=self.source if receipts is None else receipts,
            date="2026-08-31", now=self.now, automation_id="lead-verifier-8-am", run_receipt_id="receipt-123456",
            global_sms_blockers=[] if blockers is None else blockers, evidence_ok=lambda *args: True, **kwargs)

    def test_live_headers_map_pointer_not_duplicate_key(self):
        headers = ["matched_main_row", "duplicate_key"]
        result = contract.mapped_updates("Lead Source Pilot", headers, 1172, {"matched_main_row": "22"})
        self.assertEqual(result, [{"range": "'Lead Source Pilot'!A1172", "values": [["22"]]}])
        canonical = contract.mapped_updates("Lead Source Pilot", contract.pilot.PILOT_HEADERS, 1172, {"matched_main_row": "22"})
        self.assertEqual(canonical[0]["range"], "'Lead Source Pilot'!X1172")

    def test_ambiguous_missing_header_and_unapproved_field_rejected(self):
        for headers in [["duplicate_key"], ["matched_main_row", "matched-main-row"]]:
            with self.assertRaises(ValueError):
                contract.mapped_updates("Pilot", headers, 2, {"matched_main_row": "3"})
        with self.assertRaises(ValueError):
            contract.mapped_updates("Pilot", ["phone"], 2, {"phone": "wrong"})

    def test_global_blockers_do_not_poison_green_pilot(self):
        receipt, gaps = self.build(blockers=[5470, 5472])
        self.assertEqual(receipt["pipeline_complete"], "true")
        self.assertEqual(receipt["status"], "completed")
        self.assertIn("global_sms_blockers=5470,5472", receipt["detail"])
        self.assertIn("pilot_pipeline_complete=true", receipt["detail"])
        self.assertFalse(gaps)

    def test_blank_pointer_even_with_owner_and_global_blockers_is_degraded(self):
        self.row["matched_main_row"] = ""
        receipt, gaps = self.build(blockers=[5470])
        self.assertEqual(receipt["pipeline_complete"], "false")
        self.assertIn(1172, gaps)
        self.assertIn("global_sms_blockers=5470", receipt["detail"])

    def test_wrong_pointer_missing_source_and_wrong_id_are_not_green(self):
        for field, value in [("matched_main_row", "21"), ("pending_queue_listing_json", "{}"), ("synthetic_zpid", "free-abcdefgh")]:
            with self.subTest(field=field):
                original = self.row[field]
                self.row[field] = value
                self.assertEqual(self.build()[0]["pipeline_complete"], "false")
                self.row[field] = original

    def test_explicit_hold_counts_reviewed_but_source_staging_does_not(self):
        self.row.update(promotion_status="verifier_held", import_ready="verify", matched_main_row="",
                        promotion_notes="verifier_reviewed_by=lead-verifier-8-am; missing agent-specific email")
        self.assertEqual(self.build()[0]["pipeline_complete"], "true")
        self.row["promotion_notes"] = "Qualified listing staged for the lead verifier."
        self.assertEqual(self.build()[0]["pipeline_complete"], "false")

    def test_empty_cohort_requires_source_completion(self):
        self.assertEqual(self.build(rows=[])[0]["pipeline_complete"], "true")
        self.assertEqual(self.build(rows=[], receipts=[])[0]["pipeline_complete"], "false")

    def test_future_and_manual_source_receipt_do_not_count(self):
        self.source[0][1]["run_mode"] = "manual"
        self.assertEqual(self.build()[0]["pipeline_complete"], "false")
        self.source[0][1]["run_mode"] = "scheduled_source"
        self.source[0][1]["observed_at"] = "2026-08-31T15:10:00Z"
        self.assertEqual(self.build()[0]["pipeline_complete"], "false")

    def test_duplicate_requires_exact_current_owner_and_separate_state(self):
        self.row.update(status="duplicate", promotion_status="skipped_duplicate_listing", import_ready="skip")
        self.assertEqual(self.build()[0]["pipeline_complete"], "true")
        self.owner["phone"] = "5559999999"
        self.assertEqual(self.build()[0]["pipeline_complete"], "false")

    def test_missing_global_blocker_input_is_rejected(self):
        with self.assertRaises(ValueError):
            self.build(blockers="none")

    def test_historical_blank_contacts_require_explicit_named_phone_note(self):
        self.row.update(first_name="", last_name="", phone="", email="", status="duplicate",
                        promotion_status="skipped_duplicate_listing", import_ready="skip",
                        promotion_notes="Lead verifier 8 AM linked exact listing to Jane Agent, 555-222-3333")
        self.assertEqual(self.build()[0]["pipeline_complete"], "true")
        self.row["promotion_notes"] = "Lead verifier 8 AM linked exact listing to Janet Agent, 555-222-3333"
        self.assertEqual(self.build()[0]["pipeline_complete"], "false")

    def test_existing_listing_place_pl_alias_matches_without_dropping_unit(self):
        self.row.update(listing_address="20111 Caroline Creek Place", status="duplicate",
                        promotion_status="skipped_duplicate_listing", import_ready="skip")
        self.owner["listing_address"] = "20111 Caroline Creek PL"
        self.assertEqual(self.build()[0]["pipeline_complete"], "true")
        self.owner["listing_address"] += " Unit 2"
        self.assertEqual(self.build()[0]["pipeline_complete"], "false")
        self.owner["listing_address"] = "20111 Caroline Creek Road"
        self.assertEqual(self.build()[0]["pipeline_complete"], "false")

    def test_similar_longer_name_in_note_is_not_identity(self):
        self.row.update(first_name="", last_name="", phone="", email="", status="duplicate",
                        promotion_status="skipped_duplicate_listing", import_ready="skip",
                        promotion_notes="Lead verifier 8 AM linked exact listing to Mary Jane Agent, 555-222-3333")
        self.assertEqual(self.build()[0]["pipeline_complete"], "false")

    def test_details_are_not_truncated_at_500_characters(self):
        receipt, _ = self.build(blockers=list(range(3000, 3100)))
        self.assertGreater(len(receipt["detail"]), 500)
        self.assertTrue(receipt["detail"].endswith("unresolved_rows=none"))

    def test_shifted_pilot_before_write_is_rejected(self):
        request = dict(action="update", automation_id="lead-verifier-8-am", expected=self.row.copy(),
                       fields={"promotion_status": "verifier_held", "import_ready": "verify", "matched_main_row": ""},
                       adjudication_reason="missing agent-specific email")
        with mock.patch.object(contract, "snapshot", return_value=self.snapshot()), \
             mock.patch.object(contract, "read_table", return_value=(contract.pilot.PILOT_HEADERS, [(1173, self.row)])), \
             mock.patch.object(contract.pilot, "batch_update_values") as write:
            with self.assertRaisesRegex(ValueError, "pilot_changed_before_write"):
                contract.handle("token", "sheet", request, now=self.now)
        write.assert_not_called()

    def test_pilot_write_must_reread_all_changed_and_neighbor_fields(self):
        request = dict(action="update", automation_id="lead-verifier-8-am", expected=self.row.copy(),
                       fields={"promotion_status": "verifier_held", "import_ready": "verify", "matched_main_row": ""},
                       adjudication_reason="missing agent-specific email")
        with mock.patch.object(contract, "snapshot", return_value=self.snapshot()), \
             mock.patch.object(contract, "read_table", return_value=(contract.pilot.PILOT_HEADERS, [(1172, self.row)])), \
             mock.patch.object(contract.pilot, "batch_update_values") as write:
            with self.assertRaisesRegex(ValueError, "pilot_write_readback_failed"):
                contract.handle("token", "sheet", request, now=self.now)
        self.assertEqual(write.call_count, 1)
        self.assertTrue(all("Lead Source Pilot" in item["range"] for item in write.call_args.args[2]))

    def request(self, action="preview"):
        return dict(action=action, automation_id="lead-verifier-8-am", run_date="2026-08-31",
                    run_receipt_id="receipt-123456", global_sms_blockers=[5470],
                    started_at="2026-08-31T12:00:00Z", run_kind="organic_scheduled")

    def snapshot(self, receipts=None):
        return (contract.pilot.PILOT_HEADERS, [(1172, self.row)], [(22, self.owner)],
                contract.pilot.RUN_RECEIPT_HEADERS, self.source if receipts is None else receipts)

    def owner_headers(self):
        headers = [""] * 45
        for index, value in enumerate([
            "agent_name", "last_name", "phone", "email", "listing_address", "city", "state"
        ]):
            headers[index] = value
        headers[25] = "contact_verification_note"
        headers[27] = "created-at"
        headers[42] = "codex-verified"
        return headers

    def promotion_request(self):
        return {
            "action": "promote_owner", "automation_id": "lead-verifier-8-am",
            "expected": {key: self.row[key] for key in (
                "synthetic_zpid", "listing_address", "city", "state", "status",
                "promotion_status", "import_ready",
            )},
            "owner": {
                "agent_name": "Jane", "last_name": "Smith", "phone": "555-222-3333",
                "email": "jane@example.test", "phone_confidence": "verified_direct_mobile",
                "contact_verification_note": "Official listing and brokerage profile agree",
                "email_confidence": "verified_agent_specific_email",
            },
            "adjudication_reason": "exact listing agent and contact contract passed",
        }

    def promotion_snapshots(self):
        unrelated = dict(agent_name="Prior", last_name="Owner", phone="5551110000",
                         email="prior@example.test", listing_address="1 Other Road", city="Dover",
                         state="DE", created_at="free-other-owner")
        new_owner = dict(agent_name="Jane", last_name="Smith", phone="555-222-3333",
                         email="jane@example.test", listing_address="123 Main Street", city="Dover",
                         state="DE", created_at="free-12345678")
        before = (contract.pilot.PILOT_HEADERS, [(1172, self.row)], [(30, unrelated)],
                  contract.pilot.RUN_RECEIPT_HEADERS, self.source)
        after_owner = (contract.pilot.PILOT_HEADERS, [(1172, self.row)],
                       [(30, unrelated), (31, new_owner)], contract.pilot.RUN_RECEIPT_HEADERS, self.source)
        promoted = {**self.row, "promotion_status": "promoted", "import_ready": "promoted",
                    "matched_main_row": "31",
                    "promotion_notes": self.row["promotion_notes"] +
                    "; verifier_reviewed_by=lead-verifier-8-am; exact listing agent and contact contract passed" +
                    "; owner Sheet1 row 31 has Jane Smith; owner_phone=555-222-3333" +
                    "; owner append and identity readback passed"}
        after_promotion = (contract.pilot.PILOT_HEADERS, [(1172, promoted)],
                           [(30, unrelated), (31, new_owner)], contract.pilot.RUN_RECEIPT_HEADERS, self.source)
        return before, after_owner, after_promotion

    def test_preview_has_no_mutations(self):
        with mock.patch.object(contract, "snapshot", return_value=self.snapshot()), \
             mock.patch.object(contract.pilot, "resolve_source_evidence_receipt", return_value="https://example.test/listing"), \
             mock.patch.object(contract.pilot, "append_values") as append, \
             mock.patch.object(contract.pilot, "batch_update_values") as write:
            result = contract.handle("token", "sheet", self.request(), now=self.now)
        self.assertTrue(result["preview"])
        append.assert_not_called()
        write.assert_not_called()

    def test_promote_owner_uses_exact_active_row_then_rereads_identity_pointer(self):
        self.row.update(promotion_status="verifier_held", import_ready="verify", matched_main_row="",
                        promotion_notes="Qualified listing staged for the lead verifier",
                        source_url=contract.pilot.safe_source_reference("https://example.test/listing"))
        before, after_owner, after_promotion = self.promotion_snapshots()
        writes = []
        with mock.patch.object(contract, "snapshot", side_effect=[before, before, after_owner, after_promotion]), \
             mock.patch.object(contract, "owner_row_is_empty", return_value=True), \
             mock.patch.object(contract.pilot, "get_values", return_value=[self.owner_headers()]), \
             mock.patch.object(contract.pilot, "resolve_source_evidence_receipt", return_value="https://example.test/listing"), \
             mock.patch.object(contract.pilot, "batch_update_values", side_effect=lambda *args: writes.append(args[2])), \
             mock.patch.object(contract, "delete_owner_row") as delete:
            result = contract.handle("token", "sheet", self.promotion_request(), now=self.now)
        self.assertEqual(result["owner_row"], 31)
        self.assertTrue(result["readback"])
        self.assertEqual(result["sheet1_writes"], 1)
        self.assertEqual(result["sends"], 0)
        self.assertEqual([item["range"] for item in writes[0]], ["'Sheet1'!A31:G31", "'Sheet1'!Y31:AB31"])
        self.assertTrue(all("Lead Source Pilot" in item["range"] for item in writes[1]))
        delete.assert_not_called()

    def test_promote_owner_rejects_duplicate_phone_and_artifact_tail(self):
        self.row.update(promotion_status="verifier_held", import_ready="verify", matched_main_row="")
        request = self.promotion_request()
        prior = dict(agent_name="Prior", last_name="Owner", phone="555-222-3333",
                     email="prior@example.test", listing_address="1 Other Road", city="Dover",
                     state="DE", created_at="free-other-owner")
        with self.assertRaisesRegex(ValueError, "existing_phone_owner:30"):
            contract.preappend_checks(1172, self.row, [(30, prior)], request["owner"])
        with self.assertRaisesRegex(ValueError, "no_safe_operational_owner_row"):
            contract.owner_row_number([(8999, prior)])

    def test_owner_confidence_columns_support_named_headers_or_anchored_blanks(self):
        blank_positions = contract.owner_header_positions(self.owner_headers())
        self.assertEqual(blank_positions["phone_confidence"], 24)
        self.assertEqual(blank_positions["email_confidence"], 26)
        named = self.owner_headers()
        named[24] = "phone_confidence"
        named[26] = "email_confidence"
        self.assertEqual(contract.owner_header_positions(named), blank_positions)

    def test_owner_write_preview_is_read_only_and_below_artifact_floor(self):
        unrelated = dict(agent_name="Prior", last_name="Owner", phone="5551110000",
                         email="prior@example.test", listing_address="1 Other Road", city="Dover",
                         state="DE", created_at="free-other-owner")
        current = (contract.pilot.PILOT_HEADERS, [(1172, self.row)], [(5586, unrelated)],
                   contract.pilot.RUN_RECEIPT_HEADERS, self.source)
        request = {"action": "owner_write_preview", "automation_id": "lead-verifier-8-am"}
        with mock.patch.object(contract, "snapshot", return_value=current), \
             mock.patch.object(contract.pilot, "get_values", return_value=[self.owner_headers()]), \
             mock.patch.object(contract, "owner_row_is_empty", return_value=True), \
             mock.patch.object(contract.pilot, "batch_update_values") as write:
            result = contract.handle("token", "sheet", request, now=self.now)
        self.assertEqual(result["owner_row"], 5587)
        self.assertEqual(result["write_ranges"], ["'Sheet1'!A5587:G5587", "'Sheet1'!Y5587:AB5587"])
        self.assertTrue(result["owner_row_empty"])
        self.assertEqual(result["writes"], 0)
        self.assertEqual(result["sends"], 0)
        write.assert_not_called()

    def test_promote_owner_rolls_back_exact_owned_row_when_pointer_readback_fails(self):
        self.row.update(promotion_status="verifier_held", import_ready="verify", matched_main_row="",
                        promotion_notes="Qualified listing staged for the lead verifier",
                        source_url=contract.pilot.safe_source_reference("https://example.test/listing"))
        before, after_owner, _ = self.promotion_snapshots()
        with mock.patch.object(contract, "snapshot", side_effect=[before, before, after_owner, after_owner, after_owner]), \
             mock.patch.object(contract, "owner_row_is_empty", return_value=True), \
             mock.patch.object(contract.pilot, "get_values", return_value=[self.owner_headers()]), \
             mock.patch.object(contract.pilot, "resolve_source_evidence_receipt", return_value="https://example.test/listing"), \
             mock.patch.object(contract.pilot, "batch_update_values"), \
             mock.patch.object(contract, "delete_owner_row") as delete:
            with self.assertRaisesRegex(ValueError, "promotion_owner_readback_failed_do_not_send"):
                contract.handle("token", "sheet", self.promotion_request(), now=self.now)
        delete.assert_called_once_with("token", "sheet", 31)

    def test_existing_terminal_slot_cannot_be_rewritten(self):
        existing, _ = self.build()
        existing["run_receipt_id"] = "old-receipt-123"
        with mock.patch.object(contract, "snapshot", return_value=self.snapshot(self.source + [(3, existing)])), \
             mock.patch.object(contract.pilot, "append_values") as append:
            with self.assertRaisesRegex(ValueError, "terminal_slot_already_exists"):
                contract.handle("token", "sheet", self.request("receipt"), now=self.now)
        append.assert_not_called()

    def test_same_receipt_id_retry_returns_owner_without_append(self):
        existing, _ = self.build()
        with mock.patch.object(contract, "snapshot", return_value=self.snapshot([(3, existing)])), \
             mock.patch.object(contract.pilot, "append_values") as append:
            result = contract.handle("token", "sheet", self.request("receipt"), now=self.now)
        self.assertTrue(result["duplicate"])
        append.assert_not_called()

    def test_backfill_is_forbidden(self):
        request = self.request("receipt")
        request["run_date"] = "2026-08-30"
        with mock.patch.object(contract, "snapshot", return_value=self.snapshot()):
            with self.assertRaisesRegex(ValueError, "historical_receipt_forbidden"):
                contract.handle("token", "sheet", request, now=self.now)

    def test_terminal_append_is_header_mapped_complete_and_reread(self):
        written = []
        headers = list(reversed(contract.pilot.RUN_RECEIPT_HEADERS))
        snapshot = list(self.snapshot())
        snapshot[3] = headers
        self.row["source_url"] = contract.pilot.safe_source_reference("https://example.test/listing")

        def append(token, sheet, range_name, values):
            written.append(dict(zip(headers, values[0])))

        def reread(*args):
            return headers, [(3, written[0])]

        with mock.patch.object(contract, "snapshot", return_value=tuple(snapshot)), \
             mock.patch.object(contract.pilot, "resolve_source_evidence_receipt", return_value="https://example.test/listing"), \
             mock.patch.object(contract.pilot, "append_values", side_effect=append), \
             mock.patch.object(contract, "read_table", side_effect=reread), \
             mock.patch.object(contract.pilot, "batch_update_values") as update:
            result = contract.handle("token", "sheet", self.request("receipt"), now=self.now)
        self.assertEqual(result["receipt_row"], 3)
        self.assertEqual(result["receipt"]["pipeline_complete"], "true")
        self.assertIn("global_sms_blockers=5470", written[0]["detail"])
        update.assert_not_called()

    def test_terminal_readback_failure_is_not_success(self):
        with mock.patch.object(contract, "snapshot", return_value=self.snapshot()), \
             mock.patch.object(contract.pilot, "resolve_source_evidence_receipt", return_value="https://example.test/listing"), \
             mock.patch.object(contract.pilot, "append_values") as append, \
             mock.patch.object(contract, "read_table", return_value=(contract.pilot.RUN_RECEIPT_HEADERS, [])):
            with self.assertRaisesRegex(ValueError, "terminal_receipt_readback_failed"):
                contract.handle("token", "sheet", self.request("receipt"), now=self.now)
        self.assertEqual(append.call_count, 1)

    def test_manual_start_cannot_create_organic_receipt(self):
        request = self.request("receipt")
        request["run_kind"] = "manual"
        with mock.patch.object(contract, "snapshot", return_value=self.snapshot()), \
             mock.patch.object(contract.pilot, "append_values") as append:
            with self.assertRaisesRegex(ValueError, "organic_scheduled_run_required"):
                contract.handle("token", "sheet", request, now=self.now)
        append.assert_not_called()

    def test_pre_source_start_cannot_be_green(self):
        receipt, _ = self.build(source_cutoff=dt.datetime(2026, 8, 31, 11, 5, tzinfo=dt.timezone.utc))
        self.assertEqual(receipt["pipeline_complete"], "false")

    def test_successful_pilot_write_returns_only_after_owner_readback(self):
        request = dict(action="update", automation_id="lead-verifier-8-am", expected=self.row.copy(),
                       fields={"promotion_status": "verifier_held", "import_ready": "verify", "matched_main_row": ""},
                       adjudication_reason="missing agent-specific email")
        updated = {**self.row, **request["fields"], "promotion_notes": self.row["promotion_notes"] +
                   "; verifier_reviewed_by=lead-verifier-8-am; missing agent-specific email"}
        after = list(self.snapshot())
        after[1] = [(1172, updated)]
        with mock.patch.object(contract, "snapshot", side_effect=[self.snapshot(), tuple(after)]), \
             mock.patch.object(contract, "read_table", return_value=(contract.pilot.PILOT_HEADERS, [(1172, self.row)])), \
             mock.patch.object(contract.pilot, "batch_update_values") as write:
            result = contract.handle("token", "sheet", request, now=self.now)
        self.assertTrue(result["readback"])
        self.assertEqual(result["sheet1_writes"], 0)
        self.assertEqual(write.call_count, 1)

    def test_route_authenticates_before_processing_and_has_no_send_call(self):
        tree = ast.parse((Path(__file__).resolve().parents[1] / "webhook_server.py").read_text())
        route = next(n for n in tree.body if isinstance(n, ast.AsyncFunctionDef) and n.name == "internal_pilot_verifier_contract")
        calls = [n.func.id for n in ast.walk(route) if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)]
        self.assertIn("_auth_internal_request", calls)
        self.assertNotIn("_send_initial_sms_from_payload", calls)


if __name__ == "__main__":
    unittest.main()
