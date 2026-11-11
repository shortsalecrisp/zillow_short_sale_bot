"""Offline regression coverage for restarting without touching retained work."""
import copy
import unittest
from unittest.mock import Mock

import test_video_followup as fixtures

FALLBACK, NOW, video = fixtures.FALLBACK, fixtures.NOW, fixtures.video


CUTOFF = "2026-10-13T15:00:00.000Z"


class FutureOnlyResumeTests(unittest.TestCase):
    setUp = fixtures.VideoFollowupTests.setUp
    make_context = fixtures.VideoFollowupTests.make_context
    worker = fixtures.VideoFollowupTests.worker

    def make_config(self, **overrides):
        return fixtures.VideoFollowupTests.make_config(self, VIDEO_FOLLOWUP_FUTURE_ONLY_AFTER=CUTOFF, **overrides)

    def future_context(self):
        return self.make_context(initial_sent_at="2026-10-13T15:05:00Z")

    def test_preboundary_context_never_creates_owner_job_or_pointer(self):
        self.assertIsNone(self.service.prepare(self.context))
        self.assertEqual(self.service.select(self.context, FALLBACK), (FALLBACK, {}))
        self.assertEqual(self.objects.writes, [])

    def test_all_old_statuses_are_read_only_before_any_worker_path(self):
        statuses = ("prepared", "submitting", "submitted", "processing", "ready", "failed",
                    "uncertain", "cancelled", "frozen_text")
        for status in statuses:
            for obsolete_source in (False, True):
                with self.subTest(status=status, obsolete_source=obsolete_source):
                    self.store.prepare(self.context)
                    key = self.context["followup_key"]
                    self.store.reserve(key, self.config, NOW)
                    self.store.mutate("jobs/" + key, lambda old: {**old, "status": status,
                        "provider_id": "old-request", "reserved_at": video.iso(NOW),
                        "script_version": "old-version" if obsolete_source else video.SCRIPT_VERSION})
                    before = copy.deepcopy(self.objects._objects)
                    eligible = Mock(side_effect=AssertionError("Old work must not reach eligibility"))
                    worker = self.worker(eligible=eligible)
                    worker.audit_ready(key)
                    worker.run_job(key)
                    worker.tick()
                    self.assertEqual(self.objects._objects, before)
                    eligible.assert_not_called()
                    self.assertEqual(self.provider.submissions, [])
                    self.assertEqual(self.provider.inspections, [])
                    self.assertEqual(self.provider.preparations, [])

    def test_new_receipt_cannot_requalify_a_preboundary_job(self):
        context = self.future_context()
        self.store.prepare(context)
        self.store.mutate("jobs/" + context["followup_key"], lambda old:
            {**old, "created_at": "2026-10-13T14:59:59.999Z", "status": "ready"})
        pointer = "active/" + context["followup_key"]
        _, generation = self.store.read(pointer)
        self.objects.delete(pointer, generation)
        before = copy.deepcopy(self.objects._objects)
        audit = Mock(side_effect=AssertionError("Old ready media must not be audited"))
        self.service.audit_ready = audit
        text, metadata = self.service.select(context, FALLBACK)
        self.assertEqual(text, FALLBACK)
        self.assertEqual(metadata["video_variant"], "text")
        self.assertIsNone(self.service.prepare(context))
        self.worker().tick()
        self.assertEqual(self.objects._objects, before)
        audit.assert_not_called()

    def test_boundary_straddling_receipt_cannot_create_or_freeze_a_job(self):
        context = self.make_context(initial_sent_at=CUTOFF)
        self.assertFalse(self.config.future_context_allowed(context))
        self.assertIsNone(self.service.prepare(context))
        text, _ = self.service.select(context, FALLBACK)
        self.assertEqual(text, FALLBACK)
        self.assertEqual(self.objects.writes, [])

    def test_missing_or_corrupt_receipt_or_creation_time_fails_closed(self):
        valid = self.future_context()
        for field in ("initial_receipt", "initial_sent_at", "created_at"):
            job = {**valid, "created_at": video.iso(NOW)}
            job.pop(field)
            self.assertFalse(self.config.future_job_allowed(job))
        for corrupt in (None, {}, {"initial_receipt": []}, {**valid, "created_at": "not-a-time"}):
            self.assertFalse(self.config.future_job_allowed(corrupt))

    def test_valid_new_episode_uses_existing_generation_and_eligibility(self):
        context = self.future_context()
        self.service.prepare(context)
        job, _ = self.store.read("jobs/" + context["followup_key"])
        self.assertTrue(self.config.future_job_allowed(job))
        eligible = Mock(return_value=True)
        self.worker(eligible).run_job(context["followup_key"])
        self.assertEqual(len(self.provider.submissions), 1)
        self.assertGreater(eligible.call_count, 0)
        ledger, _ = self.store.read("budget/ledger")
        self.assertEqual(ledger["reservations"][context["followup_key"]]["cents"], 10)

    def test_future_episode_still_cancels_on_real_control_block(self):
        context = self.future_context()
        self.service.prepare(context)
        self.worker(eligible=lambda job: False).run_job(context["followup_key"])
        job, _ = self.store.read("jobs/" + context["followup_key"])
        self.assertEqual(job["status"], "cancelled")
        self.assertEqual(self.provider.submissions, [])

    def test_retained_private_and_old_reservations_still_use_both_slots(self):
        self.store.prepare(self.context)
        old_key = self.context["followup_key"]
        self.store.reserve(old_key, self.config, NOW)
        self.store.mutate("jobs/" + old_key, lambda old: {**old, "status": "failed"})
        self.store.reserve("private-audio-unknown", self.config, NOW)
        before, _ = self.store.read("budget/ledger")
        self.worker().tick()
        self.assertFalse(self.store.reserve(self.future_context()["followup_key"], self.config, NOW))
        after, _ = self.store.read("budget/ledger")
        self.assertEqual(after, before)
        self.assertEqual(sum(x["cents"] for x in after["reservations"].values()), 20)
        self.assertTrue(all(x["inflight"] for x in after["reservations"].values()))

    def test_caps_and_legacy_unconfigured_behavior_are_unchanged(self):
        self.assertEqual((self.config.reserve_cents, self.config.daily_cents, self.config.monthly_cents),
                         (10, 300, 7500))
        legacy = fixtures.VideoFollowupTests.make_config(self)
        self.assertTrue(legacy.future_context_allowed(self.context))
        self.assertTrue(legacy.future_job_allowed(None))

    def test_invalid_cutoff_is_not_silently_ignored(self):
        with self.assertRaises(ValueError):
            fixtures.VideoFollowupTests.make_config(self, VIDEO_FOLLOWUP_FUTURE_ONLY_AFTER="invalid")
