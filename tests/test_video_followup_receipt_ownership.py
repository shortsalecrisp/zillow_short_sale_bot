"""Offline ownership regressions; no provider, transport or production storage."""

import copy
import importlib.util
import json
import unittest
from datetime import timedelta
from pathlib import Path
from unittest.mock import Mock, patch


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("receipt_ownership_helpers", ROOT / "tests/test_video_followup.py")
helpers = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(helpers)
video = helpers.video


class InitialReceiptOwnershipTests(unittest.TestCase):
    setUp = helpers.VideoFollowupTests.setUp
    make_config = helpers.VideoFollowupTests.make_config
    make_context = helpers.VideoFollowupTests.make_context
    worker = helpers.VideoFollowupTests.worker
    job = helpers.VideoFollowupTests.job
    set_job = helpers.VideoFollowupTests.set_job
    concurrent = helpers.VideoFollowupTests.concurrent

    def alias(self, seconds=1):
        receipt = self.context["initial_receipt"]
        initial = video.datetime.fromisoformat(self.context["initial_sent_at"].replace("Z", "+00:00"))
        return self.make_context(initial_sent_at=video.iso(initial + timedelta(seconds=seconds)),
                                 initial_receipt=receipt)

    def legacy_job(self, context, status="prepared"):
        job = {key: value for key, value in context.items() if key not in video._RECEIPT_IDENTITY_FIELDS}
        job.update(id="a" * 32, status=status, script_version=video.SCRIPT_VERSION,
                   created_at=video.iso(helpers.NOW))
        if status.startswith("frozen_"):
            job.update(variant=status.split("_")[1], message=helpers.FALLBACK)
        self.store.write("jobs/" + context["followup_key"], job, 0)
        return job

    def assert_unpaid(self):
        self.assertEqual(self.objects.list("budget/"), [])
        self.assertEqual(self.provider.submissions, [])

    def test_new_generation_requires_complete_exact_receipt(self):
        absent = self.make_context(initial_receipt=None)
        with self.assertRaises(video.InitialReceiptHold):
            self.store.prepare(absent)
        receipt = self.context["initial_receipt"]
        for change in ({"status": "queued"}, {"phone": "2125550199"}, {"stable_id": "other"},
                       {"request_id": "different"}, {"message_id": "initial-1-" + "a" * 16},
                       {"sent_at": receipt["send_started_at"]}, {"created_at": receipt["sent_at"]},
                       {"created_at": "2026-10-13T09:59:50"}, {"extra": "unapproved"}):
            with self.subTest(change=change), self.assertRaises(video.InitialReceiptHold):
                self.make_context(initial_receipt={**receipt, **change})
        self.assertEqual(self.objects.list(""), [])
        self.assert_unpaid()

    def test_same_receipt_timestamp_aliases_create_one_job_and_submission(self):
        contexts = [self.context, self.alias()]
        def prepare(index):
            try:
                return self.store.prepare(contexts[index % 2])
            except video.InitialReceiptHold:
                return None
        jobs = [job for job in self.concurrent(12, prepare) if job]
        self.assertTrue(jobs)
        self.assertEqual(len({job["id"] for job in jobs}), 1)
        self.assertEqual(len(self.objects.list("jobs/")), 1)
        self.assertEqual(len(self.objects.list("generation-owners/")), 1)
        self.concurrent(8, lambda _: self.worker().tick())
        self.assertEqual(len(self.provider.submissions), 1)
        self.assertEqual(len(self.store.read("budget/ledger")[0]["reservations"]), 1)

    def test_every_legacy_status_blocks_alias_without_rewriting_job(self):
        for status in ("prepared", "submitted", "ready", "failed", "uncertain", "cancelled", "frozen_text", "frozen_video"):
            with self.subTest(status=status):
                self.objects = helpers.MemoryObjects()
                self.store = video.Store(self.objects)
                self.legacy_job(self.context, status)
                before = self.objects.read("jobs/" + self.context["followup_key"])
                with self.assertRaises(video.InitialReceiptHold):
                    self.store.prepare(self.alias())
                self.assertEqual(self.objects.read("jobs/" + self.context["followup_key"]), before)
                self.assertEqual(self.objects.list("generation-owners/"), [])
                self.assert_unpaid()

    def test_legacy_prepared_job_is_not_upgraded_or_submitted(self):
        self.legacy_job(self.context)
        before = self.objects.read("jobs/" + self.context["followup_key"])
        self.store.prepare(self.context)
        self.worker().run_job(self.context["followup_key"])
        self.assertEqual(self.objects.read("jobs/" + self.context["followup_key"]), before)
        self.assertEqual(self.objects.list("generation-owners/"), [])
        self.assert_unpaid()

    def test_distinct_confirmed_initials_phone_and_listing_keep_distinct_owners(self):
        contexts = [self.context, self.make_context(initial_sent_at="2026-10-14T10:00:00Z"),
                    self.make_context(phone="2125550101"), self.make_context(stable_id="different-listing")]
        for context in contexts:
            self.store.prepare(context)
        self.assertEqual(len(self.objects.list("jobs/")), 4)
        self.assertEqual(len(self.objects.list("generation-owners/")), 4)

    def test_owner_namespaces_are_read_but_distinct_explicit_owner_is_not_collision(self):
        private_job = {"kind": "owner_video_sample", "owner_phone": "9542053205", "first_name": "Jim",
                       "id": "a" * 32, "status": "ready", "script_version": "face-to-text-v1"}
        for key in ("owner-probe-" + "a" * 32, "owner-veed-sources-" + "b" * 32, "c" * 64):
            record = {**private_job, "job_key": key}
            if key.startswith("owner-veed-sources-"):
                record.pop("id")
                record["kind"] = "owner_veed_sources"
            if key == "c" * 64:
                record.update(phone=record.pop("owner_phone"), stable_id="owner-release-example",
                              initial_sent_at="2026-10-14T06:53:39.136Z")
            self.store.write("jobs/" + key, record, 0)
        self.store.prepare(self.context)
        self.worker().run_job(self.context["followup_key"])
        self.assertEqual(len(self.provider.submissions), 1)

    def test_owner_like_namespace_does_not_hide_organic_timestamp_collision(self):
        old = {key: value for key, value in self.context.items() if key not in video._RECEIPT_IDENTITY_FIELDS}
        self.store.write("jobs/owner-probe-" + "a" * 32, {**old, "id": "a" * 32, "status": "failed"}, 0)
        with self.assertRaises(video.InitialReceiptHold):
            self.store.prepare(self.alias())
        self.assertEqual(self.objects.list("generation-owners/"), [])
        self.assert_unpaid()

    def test_collision_added_after_creation_blocks_before_reservation(self):
        self.store.prepare(self.context)
        self.legacy_job(self.alias(), "failed")
        self.worker().run_job(self.context["followup_key"])
        self.assertEqual(self.job()["status"], "prepared")
        self.assert_unpaid()

    def test_reply_or_phone_change_during_scan_rechecks_before_any_reservation(self):
        for changed in (False, None):
            with self.subTest(changed=changed):
                self.objects = helpers.MemoryObjects()
                self.store = video.Store(self.objects)
                self.store.prepare(self.context)
                control = {"eligible": True}
                original_list = self.objects.list
                def scan(*args, **kwargs):
                    result = original_list(*args, **kwargs)
                    control["eligible"] = changed
                    return result
                with patch.object(self.objects, "list", side_effect=scan):
                    self.worker(eligible=lambda _: control["eligible"]).run_job(self.context["followup_key"])
                self.assertEqual(self.job()["status"], "cancelled" if changed is False else "prepared")
                self.assert_unpaid()

    def test_only_required_scans_and_fresh_submission_clock_after_scan(self):
        original_list = self.objects.list
        clock = {"now": helpers.NOW}
        scans = []
        def scan(prefix, **kwargs):
            if prefix == "jobs/":
                scans.append(prefix)
                if len(scans) == 2:
                    clock["now"] = helpers.NOW + timedelta(days=1)
            return original_list(prefix, **kwargs)
        with patch.object(self.objects, "list", side_effect=scan):
            self.store.prepare(self.context)
            video.Worker(self.store, self.provider, self.config, lambda _: True,
                         now=lambda: clock["now"]).run_job(self.context["followup_key"])
        self.assertEqual(scans, ["jobs/", "jobs/"])
        self.assertEqual(self.job()["lease_at"], video.iso(clock["now"]))
        self.assertEqual(self.store.read("budget/ledger")[0]["reservations"][self.context["followup_key"]]["day"], "2026-10-14")
        self.assertEqual(len(self.provider.submissions), 1)

    def test_missing_or_tampered_owner_blocks_before_payment(self):
        self.store.prepare(self.context)
        path = "generation-owners/" + self.context["initial_receipt_key"]
        owner, generation = self.store.read(path)
        self.store.write(path, {**owner, "followup_key": "f" * 64}, generation)
        self.worker().run_job(self.context["followup_key"])
        self.assert_unpaid()
        _, generation = self.objects.read(path)
        self.objects.delete(path, generation)
        self.worker().run_job(self.context["followup_key"])
        self.assert_unpaid()

    def test_corrupt_existing_owner_is_never_overwritten_or_reclaimed(self):
        path = "generation-owners/" + self.context["initial_receipt_key"]
        for value in ({}, None, {"schema": "old-unknown-owner"}):
            with self.subTest(value=value):
                self.objects = helpers.MemoryObjects()
                self.store = video.Store(self.objects)
                self.store.write(path, value, 0)
                before = self.objects.read(path)
                with self.assertRaises(video.InitialReceiptHold):
                    self.store.prepare(self.context)
                self.assertEqual(self.objects.read(path), before)
                self.assertEqual(self.objects.list("jobs/"), [])
                self.assert_unpaid()

    def test_incomplete_inventory_and_storage_errors_never_create_owner(self):
        with patch.object(self.objects, "list", return_value=["jobs/" + "f" * 64]):
            with self.assertRaises(video.InitialReceiptHold):
                self.store.prepare(self.context)
        with patch.object(self.objects, "list", side_effect=OSError("unavailable")):
            with self.assertRaises(OSError):
                self.store.prepare(self.context)
        self.assertEqual(self.objects.list(""), [])

    def test_owner_crash_is_resumable_only_by_original_key(self):
        original_write = self.objects.write
        def fail_job(key, *args, **kwargs):
            if key.startswith("jobs/"):
                raise OSError("crash after ownership")
            return original_write(key, *args, **kwargs)
        with patch.object(self.objects, "write", side_effect=fail_job), self.assertRaises(OSError):
            self.store.prepare(self.context)
        self.assertEqual(len(self.objects.list("generation-owners/")), 1)
        with self.assertRaises(video.InitialReceiptHold):
            self.store.prepare(self.alias())
        self.store.prepare(self.context)
        self.assertEqual(len(self.objects.list("jobs/")), 1)
        self.worker().run_job(self.context["followup_key"])
        self.assertEqual(len(self.provider.submissions), 1)

    def test_fallback_freeze_keeps_owner_and_never_late_video_or_alias_job(self):
        self.store.prepare(self.context)
        message, metadata = self.service.select(self.context, helpers.FALLBACK)
        self.assertEqual((message, metadata["video_variant"]), (helpers.FALLBACK, "text"))
        before = self.objects.read("jobs/" + self.context["followup_key"])
        message, metadata = self.service.select(self.alias(), helpers.FALLBACK)
        self.assertEqual((message, metadata["video_variant"]), (None, "text"))
        self.worker().tick()
        self.assertEqual(self.objects.read("jobs/" + self.context["followup_key"]), before)
        self.assertEqual(len(self.objects.list("jobs/")), 1)
        self.assertEqual(len(self.objects.list("generation-owners/")), 1)
        self.assert_unpaid()

    def test_invalid_supplied_descriptor_cannot_be_downgraded_to_legacy(self):
        self.store.prepare(self.context)
        corrupted = copy.deepcopy(self.context)
        corrupted["initial_receipt"]["sent_at"] = "2026-10-13T10:00:04.000Z"
        with self.assertRaises(video.InitialReceiptHold):
            self.store.prepare(corrupted)
        corrupted = {**self.context, "initial_receipt_key": "f" * 64}
        with self.assertRaises(video.InitialReceiptHold):
            self.store.prepare(corrupted)
        self.assert_unpaid()

    def test_pure_resolution_requires_eligible_unique_receipt_and_no_mutating_action(self):
        response, session = Mock(), Mock()
        session.post.return_value = response
        resolver = video.Eligibility("https://example.invalid", "secret", session=session)
        for result in ({"ok": True, "eligible": True},
                       {"ok": True, "eligible": False, "initial_receipt": self.context["initial_receipt"]},
                       {"ok": True, "eligible": True, "retryable": True, "initial_receipt": self.context["initial_receipt"]}):
            response.json.return_value = result
            self.assertIsNone(resolver.resolve_initial_receipt(self.context))
        response.json.return_value = {"ok": True, "eligible": True, "initial_receipt": self.context["initial_receipt"]}
        self.assertEqual(resolver.resolve_initial_receipt(self.context), self.context["initial_receipt"])
        body = session.post.call_args.kwargs["json"]
        self.assertEqual(body["action"], "check_video_followup_eligibility")
        self.assertEqual(body["video_variant"], "text")
        self.assertEqual(body["video_job_id"], "")
        self.assertEqual(self.objects.list(""), [])

    def test_prepare_wrapper_holds_without_fresh_receipt_and_pins_success(self):
        kwargs = {key: self.context[key] for key in ("row", "stable_id", "phone", "first_name", "initial_sent_at")}
        env = {"VIDEO_FOLLOWUP_ENABLED": "true", "VIDEO_FOLLOWUP_GENERATION_ENABLED": "true",
               "VIDEO_FOLLOWUP_TEST_PHONES": helpers.PHONE, "VIDEO_FOLLOWUP_INITIALS_AFTER": "2026-10-13T00:00:00Z",
               "TASKER_TRANSPORT_HEALTH_URL": "https://example.invalid", "SMS_CHATBOT_ALLOWED_TOKEN": "secret"}
        with patch.dict(video.os.environ, env, clear=True), patch.object(video, "Eligibility") as resolver, \
                patch.object(video, "get_service", return_value=self.service) as get_service:
            resolver.return_value.resolve_initial_receipt.return_value = None
            video.prepare_for_followup(**kwargs)
            get_service.assert_not_called()
            self.assertEqual(self.objects.list(""), [])
            resolver.return_value.resolve_initial_receipt.return_value = self.context["initial_receipt"]
            video.prepare_for_followup(**kwargs)
            self.assertEqual(self.job()["initial_receipt"], self.context["initial_receipt"])
            self.assertTrue(self.store.generation_owned(self.job()))
        self.assert_unpaid()

    def test_due_time_selection_keeps_existing_job_receipt_without_new_prepare_query(self):
        self.store.prepare(self.context)
        legacy_shape = self.make_context(initial_receipt=None)
        message, metadata = self.service.select(legacy_shape, helpers.FALLBACK)
        self.assertEqual(message, helpers.FALLBACK)
        self.assertEqual(metadata["initial_receipt"], self.context["initial_receipt"])
        self.assertEqual(metadata["initial_receipt_key"], self.context["initial_receipt_key"])
        self.assertEqual(self.job()["status"], "frozen_text")
        self.assert_unpaid()

    def test_worker_bool_eligibility_preserved_and_pinned_receipt_rechecked(self):
        job = self.store.prepare(self.context)
        response, session = Mock(), Mock()
        session.post.return_value = response
        resolver = video.Eligibility("https://example.invalid", "secret", session=session)
        response.json.return_value = {"ok": True, "eligible": True, "initial_receipt": job["initial_receipt"]}
        self.assertIs(resolver(job), True)
        self.assertEqual(session.post.call_args.kwargs["json"]["initial_receipt"], job["initial_receipt"])
        response.json.return_value = {"ok": True, "eligible": True}
        self.assertIsNone(resolver(job))
        response.json.return_value = {"ok": True, "eligible": False}
        self.assertIs(resolver(job), False)

    def test_bounded_gcs_listing_does_not_accept_truncation_or_repeated_cursor(self):
        response, session = Mock(), Mock()
        response.json.return_value = {"items": [{"name": "video-followup/jobs/" + "a" * 64}], "nextPageToken": "same"}
        session.get.return_value = response
        objects = video.GCSObjects("private-test-bucket", session=session)
        with self.assertRaises(video.InitialReceiptHold):
            objects.list("jobs/", limit=1000, max_items=10000)
        self.assertEqual(session.get.call_count, 2)
        response.json.return_value = {"items": [{"name": "video-followup/jobs/" + str(i)} for i in range(3)]}
        with self.assertRaises(video.InitialReceiptHold):
            objects.list("jobs/", limit=1000, max_items=2)

    def test_receipt_owner_addition_does_not_raise_cost_or_change_old_keys(self):
        absent = self.make_context(initial_receipt=None)
        for field in ("followup_key", "request_id", "message_id"):
            self.assertEqual(absent[field], self.context[field])
        self.assertEqual((self.config.reserve_cents, self.config.daily_cents, self.config.monthly_cents), (10, 300, 7500))
        self.store.prepare(self.context)
        self.worker().run_job(self.context["followup_key"])
        self.assertEqual(self.store.read("budget/ledger")[0]["reservations"][self.context["followup_key"]]["cents"], 10)


if __name__ == "__main__":
    unittest.main()
