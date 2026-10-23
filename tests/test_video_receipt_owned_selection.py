"""Prospective alias selection only; all storage/media are in-memory fixtures."""

import copy
from unittest.mock import Mock, patch

import unittest
import test_video_followup as helpers

FALLBACK, video = helpers.FALLBACK, helpers.video


class ReceiptOwnedSelectionTests(unittest.TestCase):
    setUp = helpers.VideoFollowupTests.setUp
    make_config = helpers.VideoFollowupTests.make_config
    make_context = helpers.VideoFollowupTests.make_context
    set_job = helpers.VideoFollowupTests.set_job
    make_ready = helpers.VideoFollowupTests.make_ready
    job = helpers.VideoFollowupTests.job
    concurrent = helpers.VideoFollowupTests.concurrent

    def alias(self, **overrides):
        return self.make_context(initial_sent_at=self.context["initial_receipt"]["sent_at"],
                                 initial_receipt=self.context["initial_receipt"], **overrides)

    def test_receipt_processing_timestamp_selects_original_ready_job(self):
        ready = self.make_ready()
        alias = self.alias(row=44)
        self.assertNotEqual(alias["followup_key"], ready["followup_key"])
        message, metadata = self.service.select(alias, FALLBACK)
        self.assertIn("/v/" + ready["id"], message)
        self.assertEqual(metadata["followup_key"], ready["followup_key"])
        self.assertEqual(metadata["initial_sent_at"], ready["initial_sent_at"])
        self.assertEqual(metadata["crm_row"], 44)
        self.assertEqual(metadata["initial_receipt"], ready["initial_receipt"])
        self.assertEqual(len(self.objects.list("jobs/")), 1)
        self.assertEqual(self.provider.submissions, [])

    def test_no_descriptor_uses_pure_resolver_then_preserves_frozen_receipt(self):
        ready = self.make_ready()
        alias = self.alias()
        alias = {key: value for key, value in alias.items() if key not in video._RECEIPT_IDENTITY_FIELDS}
        resolver = Mock(return_value=self.context["initial_receipt"])
        service = video.Service(self.config, self.store, resolve_initial_receipt=resolver)
        _, metadata = service.select(alias, FALLBACK)
        resolver.assert_called_once_with(alias)
        self.assertEqual(metadata["video_job_id"], ready["id"])

    def test_old_jobs_and_terminal_jobs_never_reopened_or_rekeyed(self):
        for status in ("ready", "failed", "cancelled", "uncertain", "frozen_text", "frozen_video",
                       "claimed", "send_started", "sent"):
            for legacy in (False, True):
                with self.subTest(status=status, legacy=legacy):
                    self.objects = helpers.MemoryObjects()
                    self.store = video.Store(self.objects)
                    self.service = video.Service(self.config, self.store)
                    self.make_ready()
                    def update(job):
                        job["status"] = status
                        if status.startswith("frozen_"):
                            job["variant"] = status.split("_")[1]
                        if legacy:
                            job.pop("receipt_selection_version")
                        return job
                    self.store.mutate("jobs/" + self.context["followup_key"], update)
                    before = copy.deepcopy(self.objects._objects)
                    if status == "ready" and not legacy:
                        continue
                    message, metadata = self.service.select(self.alias(), FALLBACK)
                    self.assertNotEqual(metadata["video_variant"], "video")
                    self.assertNotIn("/v/", message or "")
                    self.assertEqual(self.objects._objects, before)
                    self.assertEqual(self.provider.submissions, [])

    def test_already_frozen_text_alias_returns_no_extra_reminder(self):
        self.store.prepare(self.context)
        self.service.select(self.context, FALLBACK)
        before = copy.deepcopy(self.objects._objects)
        message, _ = self.service.select(self.alias(), FALLBACK)
        self.assertIsNone(message)
        self.assertEqual(self.objects._objects, before)

    def test_changed_recipient_name_or_missing_owner_or_receipt_fails_closed(self):
        self.make_ready()
        for field, value in (("first_name", "James"), ("stable_id", "different"), ("phone", "2125550199")):
            alias = self.alias()
            alias[field] = value
            before = copy.deepcopy(self.objects._objects)
            message, metadata = self.service.select(alias, FALLBACK)
            self.assertEqual(message, FALLBACK)
            self.assertNotEqual(metadata.get("video_variant"), "video")
            self.assertEqual(self.objects._objects, before)
        owner_path = "generation-owners/" + self.context["initial_receipt_key"]
        owner, generation = self.store.read(owner_path)
        self.store.write(owner_path, {**owner, "initial_receipt": {}}, generation)
        message, metadata = self.service.select(self.alias(), FALLBACK)
        self.assertEqual((message, metadata["video_variant"]), (FALLBACK, "text"))
        self.assertEqual(len(self.objects.list("jobs/")), 1)

    def test_incomplete_inventory_and_storage_failures_cannot_select_video(self):
        self.make_ready()
        for side_effect in (OSError("unavailable"), video.InitialReceiptHold("incomplete")):
            with patch.object(self.store, "generation_owned", side_effect=side_effect):
                message, metadata = self.service.select(self.alias(), FALLBACK)
            self.assertEqual((message, metadata["video_variant"]), (FALLBACK, "text"))
        self.assertEqual(self.provider.submissions, [])

    def test_unmodified_claim_and_send_start_guards_still_own_authorization(self):
        self.make_ready()
        with patch.object(self.store, "freeze", return_value=None) as freeze:
            message, _ = self.service.select(self.alias(), FALLBACK)
        self.assertIsNone(message)
        self.assertEqual(freeze.call_args.args[0]["followup_key"], self.context["followup_key"])
        self.assertEqual(self.provider.submissions, [])

    def test_deleted_or_terminal_job_during_alias_selection_is_not_recreated(self):
        for change in (None, "cancelled", "uncertain", "sent", "claimed", "send_started"):
            with self.subTest(change=change):
                self.objects = helpers.MemoryObjects()
                self.store = video.Store(self.objects)
                self.service = video.Service(self.config, self.store)
                self.make_ready()
                original_freeze = self.store.freeze
                def race(*args, **kwargs):
                    path = "jobs/" + self.context["followup_key"]
                    if change is None:
                        _, generation = self.objects.read(path)
                        self.objects.delete(path, generation)
                    else:
                        self.store.mutate(path, lambda job: {**job, "status": change})
                    before = copy.deepcopy(self.objects._objects)
                    result = original_freeze(*args, **kwargs)
                    self.assertEqual(self.objects._objects, before)
                    return result
                with patch.object(self.store, "freeze", side_effect=race):
                    message, _ = self.service.select(self.alias(), FALLBACK)
                self.assertIsNone(message)
                self.assertEqual(self.provider.submissions, [])

    def test_concurrent_aliases_only_freeze_one_canonical_attempt(self):
        self.make_ready()
        results = self.concurrent(8, lambda _: self.service.select(self.alias(), FALLBACK))
        videos = [(message, meta) for message, meta in results if meta["video_variant"] == "video"]
        self.assertEqual(len(videos), 1)
        self.assertEqual(videos[0][1]["followup_key"], self.context["followup_key"])
        self.assertEqual(len(self.objects.list("jobs/")), 1)
        self.assertEqual(self.provider.submissions, [])

    def test_alias_cannot_cross_activation_or_allowlist_boundary(self):
        self.make_ready()
        service = video.Service(self.make_config(VIDEO_FOLLOWUP_INITIALS_AFTER=self.alias()["initial_sent_at"]),
                                self.store)
        before = copy.deepcopy(self.objects._objects)
        message, metadata = service.select(self.alias(), FALLBACK)
        self.assertIsNone(message)
        self.assertNotEqual(metadata.get("video_variant"), "video")
        self.assertEqual(self.objects._objects, before)
