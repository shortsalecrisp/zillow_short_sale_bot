import importlib.util
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import Mock, patch


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("video_followup_under_test", ROOT / "video_followup.py")
video = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(video)

NOW = datetime(2026, 10, 13, 16, 0, tzinfo=timezone.utc)
PHONE = "2125550100"
FALLBACK = "The existing text follow-up."
MEDIA = {
    "media_bytes": b"fake mp4 bytes",
    "poster_bytes": b"fake jpeg bytes",
    "vtt_bytes": b"WEBVTT\n\n00:00.000 --> 00:10.000\nHey, it is Yoni Kutler.\n",
    "duration": 10.0,
}


class MemoryObjects:
    """Byte storage with atomic generation checks matching the GCS adapter."""

    def __init__(self):
        self._lock = threading.Lock()
        self._objects = {}
        self._generation = 0
        self.writes = []
        self.fail_reads = False
        self.fail_writes = False

    def read(self, key):
        with self._lock:
            if self.fail_reads:
                raise OSError("Storage is unavailable")
            value = self._objects.get(key)
            return (value[0], value[1]) if value else (None, 0)

    def write(self, key, body, version, content_type="application/json"):
        if not isinstance(body, bytes):
            raise TypeError("Object storage accepts bytes only")
        with self._lock:
            if self.fail_writes:
                raise OSError("Storage is unavailable")
            existing = self._objects.get(key)
            if (existing[1] if existing else 0) != version:
                raise video.Conflict()
            self._generation += 1
            self._objects[key] = (body, self._generation, content_type)
            self.writes.append((key, body, self._generation, content_type))
            return self._generation

    def delete(self, key, version):
        with self._lock:
            existing = self._objects.get(key)
            if existing and existing[1] == version:
                del self._objects[key]

    def list(self, prefix, limit=50):
        with self._lock:
            return sorted(key for key in self._objects if key.startswith(prefix))


class FakeProvider:
    def __init__(self):
        self._lock = threading.Lock()
        self.submissions = []
        self.inspections = []
        self.accounting_inspections = []
        self.preparations = []
        self.submit_error = None
        self.on_submit = None
        self.on_prepare = None
        self.status = {"status": "completed", "duration": 10.0}

    def submit(self, first_name, key):
        with self._lock:
            self.submissions.append((first_name, key))
        if self.on_submit:
            self.on_submit()
        if self.submit_error:
            raise self.submit_error
        return "render-" + key[:12]

    def inspect(self, provider_id, *, accounting_only=False):
        with self._lock:
            self.inspections.append(provider_id)
            if accounting_only:
                self.accounting_inspections.append(provider_id)
        return dict(self.status)

    def prepare_media(self, status, first_name):
        with self._lock:
            self.preparations.append((dict(status), first_name))
        if self.on_prepare:
            self.on_prepare()
        return dict(MEDIA)


class VideoFollowupTests(unittest.TestCase):
    def setUp(self):
        for target in ("socket.socket.connect", "socket.socket.connect_ex", "socket.create_connection"):
            guard = patch(target, side_effect=AssertionError("Network is forbidden in video unit tests"))
            guard.start()
            self.addCleanup(guard.stop)
        clock = patch.object(video, "utcnow", return_value=NOW)
        clock.start()
        self.addCleanup(clock.stop)
        self.objects = MemoryObjects()
        self.store = video.Store(self.objects)
        self.provider = FakeProvider()
        self.config = self.make_config()
        self.service = video.Service(self.config, self.store)
        self.context = self.make_context()

    def make_config(self, **overrides):
        env = {
            "VIDEO_FOLLOWUP_ENABLED": "true",
            "VIDEO_FOLLOWUP_GENERATION_ENABLED": "true",
            "VIDEO_FOLLOWUP_TEST_PHONES": PHONE,
            "VIDEO_FOLLOWUP_INITIALS_AFTER": "2026-10-13T00:00:00Z",
            "VIDEO_FOLLOWUP_PUBLIC_ORIGIN": "https://go.crispshortsales.com",
        }
        env.update(overrides)
        return video.Config(env)

    def make_context(self, **overrides):
        values = {
            "row": 12,
            "stable_id": "listing-owner-test-12",
            "phone": PHONE,
            "first_name": "Jim",
            "initial_sent_at": "2026-10-13T10:00:00Z",
        }
        values.update(overrides)
        return video.snapshot(**values)

    def worker(self, eligible=None, now=NOW):
        return video.Worker(self.store, self.provider, self.config,
                            eligible if eligible is not None else lambda job: True,
                            now=lambda: now)

    def job(self, context=None):
        return self.store.read("jobs/" + (context or self.context)["followup_key"])[0]

    def set_job(self, **updates):
        self.store.prepare(self.context)
        if updates.get("status") in {"submitting", "submitted", "processing"}:
            self.store.reserve(self.context["followup_key"], self.config, NOW)
            updates.setdefault("reserved_at", video.iso(NOW))
        return self.store.mutate("jobs/" + self.context["followup_key"],
                                 lambda old: {**old, **updates})

    def make_ready(self):
        self.set_job(status="submitted", provider_id="render-ready")
        self.assertTrue(self.store.finish(self.context["followup_key"], "render-ready", MEDIA))
        return self.job()

    def concurrent(self, count, action):
        barrier = threading.Barrier(count)

        def run(index):
            barrier.wait(timeout=10)
            return action(index)

        with ThreadPoolExecutor(max_workers=count) as executor:
            return list(executor.map(run, range(count)))

    def test_memory_objects_enforce_byte_storage_cas_delete_and_listing(self):
        self.assertEqual(self.objects.read("a/1"), (None, 0))
        first = self.objects.write("a/1", b"one", 0)
        with self.assertRaises(video.Conflict):
            self.objects.write("a/1", b"stale", 0)
        second = self.objects.write("a/1", b"two", first)
        self.assertGreater(second, first)
        self.objects.write("a/2", b"three", 0)
        self.objects.write("b/1", b"four", 0)
        self.assertEqual(self.objects.list("a/", limit=1), ["a/1", "a/2"])
        self.objects.delete("a/1", first)
        self.assertEqual(self.objects.read("a/1"), (b"two", second))
        self.objects.delete("a/1", second)
        self.assertEqual(self.objects.read("a/1"), (None, 0))
        with self.assertRaises(TypeError):
            self.objects.write("text", "not bytes", 0)

    def test_gcs_list_follows_pagination_beyond_first_fifty_active_jobs(self):
        first, second = Mock(), Mock()
        first.json.return_value = {
            "items": [{"name": "video-followup/active/" + str(index)} for index in range(50)],
            "nextPageToken": "next-page",
        }
        second.json.return_value = {
            "items": [{"name": "video-followup/active/50"}, {"name": "video-followup/active/51"}],
        }
        session = Mock()
        session.get.side_effect = [first, second]
        objects = video.GCSObjects("private-test-bucket", session=session)
        result = objects.list("active/", limit=50)
        self.assertEqual(result, ["active/" + str(index) for index in range(52)])
        self.assertEqual(session.get.call_count, 2)
        self.assertNotIn("pageToken", session.get.call_args_list[0].kwargs["params"])
        self.assertEqual(session.get.call_args_list[1].kwargs["params"]["pageToken"], "next-page")
        self.assertEqual(session.get.call_args_list[1].kwargs["params"]["maxResults"], 50)

    def test_defaults_disable_preparation_selection_and_provider_work(self):
        config = video.Config({})
        self.assertFalse(config.enabled)
        self.assertFalse(config.generation_enabled)
        self.assertFalse(config.live)
        self.assertFalse(config.accepts(self.context))
        service = video.Service(config, self.store)
        self.assertIsNone(service.prepare(self.context))
        self.assertEqual(service.select(self.context, FALLBACK), (FALLBACK, {}))
        video.Worker(self.store, self.provider, config, lambda job: True).tick()
        self.assertEqual(self.objects.list(""), [])
        self.assertEqual(self.provider.submissions, [])
        with patch.dict(video.os.environ, {}, clear=True), patch.object(video, "get_service") as get_service:
            self.assertIsNone(video.prepare_for_followup())
            self.assertEqual(video.select_for_followup(FALLBACK), (FALLBACK, {}))
            get_service.assert_not_called()

    def test_owner_allowlist_and_activation_cutoff_gate_generation(self):
        self.assertTrue(self.config.accepts(self.context))
        outside = self.make_context(phone="2125550101")
        before = self.make_context(initial_sent_at="2026-10-12T23:59:59.999Z")
        boundary = self.make_context(initial_sent_at="2026-10-13T00:00:00Z")
        self.assertFalse(self.config.accepts(outside))
        self.assertFalse(self.config.accepts(before))
        self.assertTrue(self.config.accepts(boundary))
        self.assertFalse(self.make_config(VIDEO_FOLLOWUP_INITIALS_AFTER="").accepts(self.context))
        live = self.make_config(VIDEO_FOLLOWUP_LIVE_APPROVED="true")
        self.assertTrue(live.accepts(outside))
        self.assertFalse(live.accepts(before))
        for context in (outside, before):
            self.assertIsNone(self.service.prepare(context))
            self.store.prepare(context)
            self.worker().run_job(context["followup_key"])
        self.assertEqual(self.provider.submissions, [])
        self.assertEqual(self.objects.list("budget/"), [])

    def test_generation_has_separate_disabled_gate(self):
        self.config = self.make_config(VIDEO_FOLLOWUP_GENERATION_ENABLED="false")
        self.store.prepare(self.context)
        self.worker().run_job(self.context["followup_key"])
        self.assertEqual(self.job()["status"], "prepared")
        self.assertEqual(self.provider.submissions, [])
        self.assertEqual(self.objects.list("budget/"), [])

    def test_identity_survives_row_name_format_and_script_changes(self):
        shifted = self.make_context(row=6172, first_name="James", phone="+1 (212) 555-0100",
                                    initial_sent_at="2026-10-13T06:00:00-04:00")
        for field in ("followup_key", "message_id", "request_id"):
            self.assertEqual(self.context[field], shifted[field])
        self.assertEqual(shifted["row"], 6172)
        original = self.store.prepare(self.context)
        with patch.object(video, "SCRIPT_VERSION", "future-copy-version"):
            again = self.store.prepare(shifted)
        self.assertEqual(original["id"], again["id"])
        self.assertEqual(original["script_version"], again["script_version"])
        self.assertEqual(len(self.objects.list("jobs/")), 1)

    def test_phone_initial_timestamp_and_listing_each_change_identity(self):
        changed = [self.make_context(phone="2125550101"),
                   self.make_context(initial_sent_at="2026-10-13T10:00:01Z"),
                   self.make_context(stable_id="different-listing")]
        for context in changed:
            for field in ("followup_key", "message_id", "request_id"):
                self.assertNotEqual(context[field], self.context[field])

    def test_invalid_identity_is_rejected(self):
        for override in ({"stable_id": " "}, {"row": 1}, {"phone": "not-a-phone"},
                         {"initial_sent_at": "2026-10-13T10:00:00"}):
            with self.subTest(override=override), self.assertRaises(ValueError):
                self.make_context(**override)

    def test_invalid_canonical_names_never_render_or_reserve_budget(self):
        invalid = ("", None, "J", "unknown", "AGENT", "there", "test", "Jim Smith",
                   "Jim2", "Jim_", "Jim / Jane", "<Jim>", "a" * 33)
        for index, first_name in enumerate(invalid):
            with self.subTest(first_name=first_name):
                context = self.make_context(stable_id="bad-name-" + str(index), first_name=first_name)
                self.assertFalse(video.valid_name(first_name))
                self.assertIsNone(self.service.prepare(context))
                self.store.prepare(context)
                self.worker().run_job(context["followup_key"])
                self.assertEqual(self.job(context)["status"], "failed")
        self.assertEqual(self.provider.submissions, [])
        self.assertEqual(self.objects.list("budget/"), [])
        for name in ("Jim", "Anne-Marie", "O'Neil"):
            self.assertTrue(video.valid_name(name))

    def test_names_normalize_before_storage_generation_and_metadata(self):
        for raw, expected in ((" \uff2a\uff49\uff4d ", "Jim"), ("O\u2019Neil", "O'Neil"), ("Jose\u0301", "Jos\u00e9")):
            with self.subTest(raw=raw):
                context = self.make_context(stable_id="name-" + expected, first_name=raw)
                self.assertEqual(context["first_name"], expected)
                self.assertTrue(video.valid_name(raw))
                self.assertEqual(self.store.prepare(context)["first_name"], expected)
                _, metadata = self.service.select(context, FALLBACK)
                self.assertEqual(metadata["first_name"], expected)
        self.assertFalse(video.valid_name("Jim\u00a0Smith"))
        self.assertFalse(video.valid_name("Mary Jane"))

    def test_old_generation_versions_never_enter_new_provider(self):
        for state in ("prepared", "submitted", "processing", "cancelled", "frozen_text"):
            with self.subTest(state=state):
                self.set_job(status=state, script_version="face-to-text-v1", provider_id="old-avatar-id",
                             reserved_at=video.iso(NOW), lease_at=video.iso(NOW))
                self.worker().run_job(self.context["followup_key"])
        self.assertEqual(self.provider.submissions, [])
        self.assertEqual(self.provider.inspections, [])
        self.assertEqual(self.provider.preparations, [])

    def test_old_ready_or_frozen_avatar_video_never_selected_for_new_followup(self):
        self.make_ready()
        self.store.mutate("jobs/" + self.context["followup_key"],
                          lambda old: {**old, "script_version": "face-to-text-v1"})
        message, metadata = self.service.select(self.context, FALLBACK)
        self.assertEqual((message, metadata["video_variant"]), (FALLBACK, "text"))
        self.store.mutate("jobs/" + self.context["followup_key"], lambda old: {
            **old, "status": "frozen_video", "variant": "video", "message": "Old avatar URL"})
        message, metadata = self.service.select(self.context, FALLBACK)
        self.assertEqual((message, metadata["video_variant"]), (FALLBACK, "text"))

    def test_text_freeze_rejects_late_video_completion_and_reuses_payload(self):
        self.set_job(status="submitted", provider_id="render-late")
        first_message, first_meta = self.service.select(self.context, FALLBACK)
        self.assertEqual(first_message, FALLBACK)
        self.assertEqual(first_meta["video_variant"], "text")
        self.assertFalse(self.store.finish(self.context["followup_key"], "render-late", MEDIA))
        second_message, second_meta = self.service.select(self.make_context(row=44), "Changed fallback")
        self.assertEqual(second_message, first_message)
        self.assertEqual(second_meta["message_id"], first_meta["message_id"])
        self.assertEqual(second_meta["video_job_id"], first_meta["video_job_id"])
        self.assertEqual(self.job()["status"], "frozen_text")
        self.assertEqual(self.objects.list("assets/"), [])
        self.assertIsNone(self.store.get_public_video(first_meta["video_job_id"]))

    def test_ready_video_freezes_branded_name_only_wrapper_once(self):
        ready = self.make_ready()
        message, meta = self.service.select(self.context, FALLBACK)
        self.assertEqual(meta["video_variant"], "video")
        self.assertEqual(message, "Hey Jim, it's Yoni Kutler with Crisp Short Sales. "
                         "Here's a quick video to put a face to my text. Happy to help if you have questions.\n"
                         "https://go.crispshortsales.com/v/" + ready["id"])
        self.assertEqual(self.service.select(self.context, "New fallback"), (message, meta))
        self.assertNotIn(PHONE, message)
        self.assertNotIn(self.context["stable_id"], message)

    def test_existing_frozen_video_keeps_its_original_copy_and_identity(self):
        ready = self.make_ready()
        old_message = "Hey Jim, just putting a face to my text: https://go.crispshortsales.com/v/" + ready["id"]
        self.store.mutate("jobs/" + self.context["followup_key"], lambda old: {
            **old, "status": "frozen_video", "variant": "video", "message": old_message,
            "frozen_at": old["created_at"],
        })
        message, meta = self.service.select(self.context, FALLBACK)
        self.assertEqual(message, old_message)
        self.assertEqual(meta["message_id"], self.context["message_id"])
        self.assertEqual(meta["video_job_id"], ready["id"])

    def test_name_change_or_missing_origin_uses_text_even_when_media_is_ready(self):
        self.make_ready()
        message, meta = self.service.select(self.make_context(first_name="James"), FALLBACK)
        self.assertEqual(message, FALLBACK)
        self.assertEqual(meta["video_variant"], "text")
        other = self.make_context(stable_id="no-origin")
        self.store.prepare(other)
        self.store.mutate("jobs/" + other["followup_key"],
                          lambda old: {**old, "status": "ready"})
        config = self.make_config(VIDEO_FOLLOWUP_PUBLIC_ORIGIN="")
        message, meta = video.Service(config, self.store).select(other, FALLBACK)
        self.assertEqual((message, meta["video_variant"]), (FALLBACK, "text"))

    def test_concurrent_freezes_choose_exactly_one_payload_and_variant(self):
        self.make_ready()

        def freeze(index):
            context = self.make_context(first_name="Jim" if index % 2 else "James")
            return self.store.freeze(context, "Fallback " + str(index), self.config.origin)

        results = self.concurrent(12, freeze)
        self.assertEqual(len({job["message"] for job in results}), 1)
        self.assertEqual(len({job["variant"] for job in results}), 1)
        self.assertEqual(len({job["id"] for job in results}), 1)
        self.assertEqual(len({job["message_id"] for job in results}), 1)
        self.assertEqual(self.job()["status"], "frozen_" + results[0]["variant"])

    def test_corrected_name_never_selects_previously_frozen_old_name_video(self):
        self.make_ready()
        video_message, video_meta = self.service.select(self.context, FALLBACK)
        self.assertEqual(video_meta["video_variant"], "video")
        text_message, text_meta = self.service.select(self.make_context(first_name="James"), FALLBACK)
        self.assertEqual(text_message, FALLBACK)
        self.assertNotEqual(text_message, video_message)
        self.assertEqual(text_meta["video_variant"], "text")
        self.assertEqual(text_meta["video_job_id"], "")
        self.assertEqual(text_meta["message_id"], video_meta["message_id"])
        self.assertEqual(text_meta["request_id"], video_meta["request_id"])

    def test_storage_failure_keeps_same_outbox_identity_for_text_fallback(self):
        for attribute in ("fail_reads", "fail_writes"):
            with self.subTest(attribute=attribute):
                setattr(self.objects, attribute, True)
                message, meta = self.service.select(self.context, FALLBACK)
                shifted_message, shifted_meta = self.service.select(self.make_context(row=90), FALLBACK)
                setattr(self.objects, attribute, False)
                self.assertEqual(message, FALLBACK)
                self.assertEqual(shifted_message, FALLBACK)
                self.assertEqual(meta["video_variant"], "text")
                self.assertEqual(meta["video_job_id"], "")
                for field in ("followup_key", "message_id", "request_id"):
                    self.assertEqual(meta[field], self.context[field])
                    self.assertEqual(shifted_meta[field], meta[field])

    def test_service_initialization_failure_keeps_stable_fallback_identity(self):
        env = {
            "VIDEO_FOLLOWUP_ENABLED": "true",
            "VIDEO_FOLLOWUP_INITIALS_AFTER": "2026-10-13T00:00:00Z",
            "VIDEO_FOLLOWUP_TEST_PHONES": PHONE,
        }
        with patch.dict(video.os.environ, env, clear=True), patch.object(video, "get_service", side_effect=OSError("offline")):
            message, metadata = video.select_for_followup(FALLBACK, row=12,
                stable_id=self.context["stable_id"], phone=PHONE, first_name="Jim",
                initial_sent_at=self.context["initial_sent_at"])
        self.assertEqual(message, FALLBACK)
        self.assertEqual(metadata["message_id"], self.context["message_id"])
        self.assertEqual(metadata["request_id"], self.context["request_id"])
        self.assertEqual(metadata["video_variant"], "text")

    def test_concurrent_capacity_accepts_only_two_inflight_reservations(self):
        accepted = self.concurrent(24, lambda index: self.store.reserve("job-" + str(index), self.config, NOW))
        self.assertEqual(sum(accepted), 2)
        ledger, _ = self.store.read("budget/ledger")
        self.assertEqual(sum(item["inflight"] for item in ledger["reservations"].values()), 2)
        key = next(iter(ledger["reservations"]))
        self.assertTrue(self.store.reserve(key, self.config, NOW))
        self.assertEqual(self.store.read("budget/ledger")[0], ledger)
        self.store.settle(key, video.iso(NOW))
        self.assertTrue(self.store.reserve("replacement", self.config, NOW))

    def test_invalid_budget_or_origin_config_preserves_same_identity_fallback(self):
        base_env = {
            "VIDEO_FOLLOWUP_ENABLED": "true",
            "VIDEO_FOLLOWUP_INITIALS_AFTER": "2026-10-13T00:00:00Z",
            "VIDEO_FOLLOWUP_TEST_PHONES": PHONE,
        }
        for override in ({"VIDEO_FOLLOWUP_DAILY_CENTS": "301"},
                         {"VIDEO_FOLLOWUP_MONTHLY_CENTS": "7501"},
                         {"VIDEO_FOLLOWUP_DAILY_CENTS": "not-a-number"},
                         {"VIDEO_FOLLOWUP_PUBLIC_ORIGIN": "https://unapproved.example"},
                         {"VIDEO_FOLLOWUP_INITIALS_AFTER": "invalid-date"}):
            with self.subTest(override=override), patch.dict(video.os.environ, {**base_env, **override}, clear=True), \
                    patch.object(video, "get_service") as get_service:
                message, metadata = video.select_for_followup(FALLBACK, row=12,
                    stable_id=self.context["stable_id"], phone=PHONE, first_name="Jim",
                    initial_sent_at=self.context["initial_sent_at"])
                self.assertEqual(message, FALLBACK)
                for field in ("message_id", "request_id", "followup_key"):
                    self.assertEqual(metadata[field], self.context[field])
                self.assertEqual(metadata["video_variant"], "text")
                get_service.assert_not_called()

    def test_concurrent_daily_budget_is_hard_capped_after_capacity_settles(self):
        for batch in range(15):
            accepted = self.concurrent(16, lambda index: self.store.reserve(
                "batch-%d-%d" % (batch, index), self.config, NOW))
            self.assertEqual(sum(accepted), 2)
            for index, success in enumerate(accepted):
                if success:
                    self.store.settle("batch-%d-%d" % (batch, index), video.iso(NOW))
        rejected = self.concurrent(16, lambda index: self.store.reserve("over-day-" + str(index), self.config, NOW))
        self.assertFalse(any(rejected))
        ledger, _ = self.store.read("budget/ledger")
        self.assertEqual(len(ledger["reservations"]), 30)
        self.assertEqual(sum(item["cents"] for item in ledger["reservations"].values()), 300)

    def test_concurrent_monthly_budget_never_exceeds_approved_cap(self):
        first_day = NOW.replace(day=1)
        for day in range(24):
            for slot in range(30):
                self.assertTrue(self.store.reserve("prior-%d-%d" % (day, slot), self.config,
                                                   first_day + timedelta(days=day)))
                self.store.settle("prior-%d-%d" % (day, slot), video.iso(first_day + timedelta(days=day)))
        for batch in range(15):
            day = first_day + timedelta(days=24)
            accepted = self.concurrent(18, lambda index: self.store.reserve(
                "final-%d-%d" % (batch, index), self.config, day))
            self.assertEqual(sum(accepted), 2)
            for index, success in enumerate(accepted):
                if success:
                    self.store.settle("final-%d-%d" % (batch, index), video.iso(day))
        ledger, _ = self.store.read("budget/ledger")
        self.assertEqual(len(ledger["reservations"]), 750)
        self.assertEqual(sum(item["cents"] for item in ledger["reservations"].values()), 7500)
        self.assertFalse(self.store.reserve("over-month", self.config, first_day + timedelta(days=30)))
        self.assertTrue(self.store.reserve("new-month", self.config, NOW.replace(month=11, day=1)))

    def test_settlement_uses_verified_cost_only_and_preserves_unknown_cost(self):
        self.assertTrue(self.store.reserve("complete", self.config, NOW))
        self.assertTrue(self.store.reserve("failed", self.config, NOW))
        self.store.settle("complete", video.iso(NOW), 7.01, "provider_verified")
        self.store.settle("failed", video.iso(NOW))
        ledger, _ = self.store.read("budget/ledger")
        self.assertEqual(ledger["reservations"]["complete"]["cents"], 8)
        self.assertEqual(ledger["reservations"]["failed"]["cents"], 10)
        self.assertFalse(any(entry["inflight"] for entry in ledger["reservations"].values()))
        self.store.settle("complete", video.iso(NOW), 7.01, "provider_verified")
        self.assertEqual(self.store.read("budget/ledger")[0], ledger)
        self.store.settle("failed", video.iso(NOW), 1, "estimated")
        self.assertEqual(self.store.read("budget/ledger")[0], ledger)
        for cost in (-1, True, "7", float("nan"), float("inf")):
            with self.subTest(cost=cost), self.assertRaises(ValueError):
                self.store.settle("complete", video.iso(NOW), cost, "provider_verified")
        self.assertEqual(self.store.read("budget/ledger")[0], ledger)

    def test_daily_and_monthly_budget_boundaries_use_eastern_time(self):
        config = self.make_config(VIDEO_FOLLOWUP_DAILY_CENTS="10")
        before = datetime(2026, 10, 14, 3, 59, tzinfo=timezone.utc)
        after = before + timedelta(minutes=1)
        self.assertTrue(self.store.reserve("before-midnight", config, before))
        self.store.settle("before-midnight", video.iso(before))
        self.assertFalse(self.store.reserve("same-eastern-day", config, before))
        self.assertTrue(self.store.reserve("after-midnight", config, after))
        ledger, _ = self.store.read("budget/ledger")
        self.assertEqual(ledger["reservations"]["before-midnight"]["day"], "2026-10-13")
        self.assertEqual(ledger["reservations"]["after-midnight"]["day"], "2026-10-14")
        self.store.settle("after-midnight", video.iso(after))
        before_month = datetime(2026, 11, 1, 3, 59, tzinfo=timezone.utc)
        self.assertTrue(self.store.reserve("october-last", config, before_month))
        self.store.settle("october-last", video.iso(before_month))
        ledger, _ = self.store.read("budget/ledger")
        self.assertFalse(any(entry["day"].startswith("2026-11") for entry in ledger["reservations"].values()))
        self.assertTrue(self.store.reserve("november-first", config, before_month + timedelta(minutes=1)))
        ledger, _ = self.store.read("budget/ledger")
        self.assertEqual(ledger["reservations"]["october-last"]["day"], "2026-10-31")
        self.assertEqual(ledger["reservations"]["november-first"]["day"], "2026-11-01")

    def test_inflight_capacity_remains_global_across_month_boundary(self):
        before = datetime(2026, 11, 1, 3, 59, tzinfo=timezone.utc)
        self.assertTrue(self.store.reserve("october-a", self.config, before))
        self.assertTrue(self.store.reserve("october-b", self.config, before))
        self.assertFalse(self.store.reserve("november-extra", self.config, before + timedelta(minutes=1)))

    def test_orphan_reservation_cannot_bypass_next_day_exhausted_or_zero_budget(self):
        config = self.make_config(VIDEO_FOLLOWUP_DAILY_CENTS="10")
        tomorrow = NOW + timedelta(days=1)
        self.assertTrue(self.store.reserve("orphan", config, NOW))
        self.assertTrue(self.store.reserve("tomorrow-spent", config, tomorrow))
        self.store.settle("tomorrow-spent", video.iso(tomorrow))
        before, _ = self.store.read("budget/ledger")
        self.assertFalse(self.store.reserve("orphan", config, tomorrow))
        zero = self.make_config(VIDEO_FOLLOWUP_DAILY_CENTS="0")
        self.assertFalse(self.store.reserve("orphan", zero, tomorrow))
        self.assertEqual(self.store.read("budget/ledger")[0], before)
        self.assertEqual(before["reservations"]["orphan"]["day"], "2026-10-13")

    def test_prepared_job_orphan_reservation_is_charged_on_actual_submission_day(self):
        self.store.prepare(self.context)
        key = self.context["followup_key"]
        self.assertTrue(self.store.reserve(key, self.config, NOW))
        tomorrow = NOW + timedelta(days=1)
        self.worker(now=tomorrow).run_job(key)
        self.assertEqual(self.provider.submissions, [("Jim", key)])
        self.assertEqual(self.job()["reserved_at"], video.iso(tomorrow))
        ledger, _ = self.store.read("budget/ledger")
        self.assertEqual(ledger["reservations"], {
            key: {"day": "2026-10-14", "cents": 10, "inflight": True},
        })

    def test_orphan_reservation_revalidates_month_cap_before_moving_charge(self):
        config = self.make_config(VIDEO_FOLLOWUP_MONTHLY_CENTS="10")
        october = datetime(2026, 11, 1, 3, 59, tzinfo=timezone.utc)
        november = october + timedelta(minutes=1)
        self.assertTrue(self.store.reserve("orphan", config, october))
        self.assertTrue(self.store.reserve("november-spent", config, november))
        self.store.settle("november-spent", video.iso(november))
        self.assertFalse(self.store.reserve("orphan", config, november))
        ledger, _ = self.store.read("budget/ledger")
        self.assertEqual(ledger["reservations"]["orphan"]["day"], "2026-10-31")
        higher = self.make_config(VIDEO_FOLLOWUP_MONTHLY_CENTS="20")
        self.assertTrue(self.store.reserve("orphan", higher, november))
        ledger, _ = self.store.read("budget/ledger")
        self.assertEqual(ledger["reservations"]["orphan"]["day"], "2026-11-01")
        self.assertEqual(sum(entry["cents"] for entry in ledger["reservations"].values()), 20)

    def test_budget_caps_cannot_be_raised_and_reservation_is_fixed(self):
        self.assertEqual((self.config.daily_cents, self.config.monthly_cents, self.config.reserve_cents),
                         (300, 7500, 10))
        for override in ({"VIDEO_FOLLOWUP_DAILY_CENTS": "301"},
                         {"VIDEO_FOLLOWUP_MONTHLY_CENTS": "7501"},
                         {"VIDEO_FOLLOWUP_DAILY_CENTS": "-1"},
                         {"VIDEO_FOLLOWUP_MONTHLY_CENTS": "-1"}):
            with self.subTest(override=override), self.assertRaises(ValueError):
                self.make_config(**override)
        lower = self.make_config(VIDEO_FOLLOWUP_DAILY_CENTS="9", VIDEO_FOLLOWUP_RESERVE_CENTS="1")
        self.assertEqual(lower.reserve_cents, 10)
        self.assertFalse(self.store.reserve("under-reservation", lower, NOW))
        zero = self.make_config(VIDEO_FOLLOWUP_DAILY_CENTS="0", VIDEO_FOLLOWUP_MONTHLY_CENTS="0")
        self.assertFalse(self.store.reserve("disabled-budget", zero, NOW))

    def test_budget_exhaustion_does_not_submit_provider_work(self):
        self.config = self.make_config(VIDEO_FOLLOWUP_DAILY_CENTS="0")
        self.store.prepare(self.context)
        self.worker().tick()
        self.assertEqual(self.job()["status"], "prepared")
        self.assertEqual(self.provider.submissions, [])
        self.assertEqual(len(self.objects.list("active/")), 1)
        message, meta = self.service.select(self.context, FALLBACK)
        self.assertEqual((message, meta["video_variant"]), (FALLBACK, "text"))
        self.worker().tick()
        self.assertEqual(self.objects.list("active/"), [])

    def test_submit_timeout_is_uncertain_never_resubmitted_and_keeps_reservation(self):
        self.provider.submit_error = TimeoutError("Unknown provider outcome")
        self.store.prepare(self.context)
        self.worker().tick()
        self.assertEqual(self.job()["status"], "uncertain")
        self.worker(now=NOW + timedelta(days=1)).run_job(self.context["followup_key"])
        self.worker(now=NOW + timedelta(days=1)).tick()
        self.assertEqual(self.provider.submissions, [("Jim", self.context["followup_key"])])
        self.assertEqual(self.provider.inspections, [])
        ledger, _ = self.store.read("budget/ledger")
        self.assertEqual(ledger["reservations"][self.context["followup_key"]]["cents"], 10)
        self.assertFalse(ledger["reservations"][self.context["followup_key"]]["inflight"])
        self.assertTrue(self.job()["budget_settled"])
        self.assertEqual(self.objects.list("active/"), [])

    def test_two_unknown_submissions_do_not_permanently_block_new_video_jobs(self):
        failed = [self.make_context(stable_id="unknown-" + str(index)) for index in range(2)]
        self.provider.submit_error = TimeoutError("Unknown paid outcome")
        for context in failed:
            self.store.prepare(context)
        self.worker().tick()
        self.assertEqual(self.objects.list("active/"), [])
        self.assertEqual(len(self.provider.submissions), 2)

        self.provider.submit_error = None
        recovered = self.make_context(stable_id="recovered")
        self.store.prepare(recovered)
        self.worker(now=NOW + timedelta(days=40)).tick()
        self.assertEqual(self.job(recovered)["status"], "submitted")
        self.assertEqual(len(self.provider.submissions), 3)
        ledger, _ = self.store.read("budget/ledger")
        for context in failed:
            self.assertEqual(self.job(context)["status"], "uncertain")
            self.assertTrue(self.job(context)["budget_settled"])
            self.assertEqual(ledger["reservations"][context["followup_key"]],
                             {"day": "2026-10-13", "cents": 10, "inflight": False})

    def test_released_terminal_capacity_does_not_refund_daily_budget(self):
        self.config = self.make_config(VIDEO_FOLLOWUP_DAILY_CENTS="20")
        self.provider.submit_error = TimeoutError("Unknown provider outcome")
        for index in range(2):
            self.store.prepare(self.make_context(stable_id="unknown-" + str(index)))
        self.worker().tick()
        self.provider.submit_error = None
        waiting = self.make_context(stable_id="waiting-for-budget")
        self.store.prepare(waiting)
        self.worker().tick()
        self.assertEqual(self.job(waiting)["status"], "prepared")
        self.assertEqual(len(self.provider.submissions), 2)
        ledger, _ = self.store.read("budget/ledger")
        self.assertEqual(sum(entry["cents"] for entry in ledger["reservations"].values()), 20)
        self.assertFalse(any(entry["inflight"] for entry in ledger["reservations"].values()))
        self.worker(now=NOW + timedelta(days=1)).tick()
        self.assertEqual(self.job(waiting)["status"], "submitted")

    def test_terminal_capacity_cleanup_does_not_release_active_paid_work(self):
        active = [self.make_context(stable_id="active-" + str(index)) for index in range(2)]
        self.provider.status = {"status": "processing"}
        for context in active:
            self.store.prepare(context)
        self.worker().tick()
        waiting = self.make_context(stable_id="waiting-for-capacity")
        self.store.prepare(waiting)
        self.worker(now=NOW + timedelta(days=1)).tick()
        self.assertEqual(self.job(waiting)["status"], "prepared")
        self.assertEqual(len(self.provider.submissions), 2)
        ledger, _ = self.store.read("budget/ledger")
        self.assertEqual(sum(entry["inflight"] for entry in ledger["reservations"].values()), 2)

    def test_terminal_capacity_cleanup_retries_storage_failure_without_resubmitting(self):
        self.provider.submit_error = TimeoutError("Unknown paid outcome")
        self.store.prepare(self.context)
        self.worker().tick()
        self.objects.fail_writes = True
        with self.assertRaises(OSError):
            self.worker().tick()
        self.objects.fail_writes = False
        self.worker().tick()
        self.assertEqual(len(self.provider.submissions), 1)
        self.assertEqual(self.job()["status"], "uncertain")
        entry = self.store.read("budget/ledger")[0]["reservations"][self.context["followup_key"]]
        self.assertEqual(entry["cents"], 10)
        self.assertFalse(entry["inflight"])

    def test_cancelled_or_frozen_orphan_keeps_submit_lease_then_releases_capacity(self):
        contexts = []
        for status in ("cancelled", "frozen_text"):
            context = self.make_context(stable_id=status)
            contexts.append(context)
            self.store.prepare(context)
            self.store.reserve(context["followup_key"], self.config, NOW)
            self.store.mutate("jobs/" + context["followup_key"], lambda old: {**old,
                "status": status, "reserved_at": video.iso(NOW), "lease_at": video.iso(NOW)})
        self.worker(now=NOW + timedelta(seconds=599)).tick()
        ledger, _ = self.store.read("budget/ledger")
        self.assertEqual(sum(entry["inflight"] for entry in ledger["reservations"].values()), 2)
        self.assertEqual(self.objects.list("active/"), [])

        self.worker(now=NOW + timedelta(seconds=600)).tick()
        ledger, _ = self.store.read("budget/ledger")
        for context in contexts:
            self.assertTrue(self.job(context)["budget_settled"])
            self.assertEqual(ledger["reservations"][context["followup_key"]],
                             {"day": "2026-10-13", "cents": 10, "inflight": False})
        self.assertEqual(self.provider.submissions, [])
        self.assertEqual(self.provider.inspections, [])

    def test_cancelled_known_request_keeps_capacity_until_accounting_completes(self):
        self.set_job(status="submitted", provider_id="cancelled-render", lease_at=video.iso(NOW))
        self.store.cancel(self.context["followup_key"], "Recipient replied")
        self.provider.status = {"status": "processing"}
        self.worker(now=NOW + timedelta(days=1)).tick()
        entry = self.store.read("budget/ledger")[0]["reservations"][self.context["followup_key"]]
        self.assertTrue(entry["inflight"])
        self.assertEqual(self.provider.accounting_inspections, ["cancelled-render"])
        self.assertFalse(self.job().get("budget_settled"))

    def test_cancelled_or_frozen_prelease_orphan_uses_existing_ledger_without_refund(self):
        contexts = []
        for status in ("cancelled", "frozen_text"):
            context = self.make_context(stable_id="prelease-" + status)
            contexts.append(context)
            self.store.prepare(context)
            self.store.reserve(context["followup_key"], self.config, NOW)
            self.store.mutate("jobs/" + context["followup_key"], lambda old: {**old, "status": status})
            self.assertNotIn("reserved_at", self.job(context))
        self.worker().tick()
        ledger, _ = self.store.read("budget/ledger")
        for context in contexts:
            self.assertTrue(self.job(context)["budget_settled"])
            self.assertEqual(ledger["reservations"][context["followup_key"]],
                             {"day": "2026-10-13", "cents": 10, "inflight": False})
        self.assertEqual(self.provider.submissions, [])
        self.assertEqual(self.provider.inspections, [])

    def test_concurrent_terminal_cleanup_preserves_debits_and_submits_new_job_once(self):
        self.provider.submit_error = TimeoutError("Unknown provider outcome")
        for index in range(2):
            self.store.prepare(self.make_context(stable_id="unknown-" + str(index)))
        self.worker().tick()
        self.provider.submit_error = None
        self.provider.status = {"status": "processing"}
        recovered = self.make_context(stable_id="recovered")
        self.store.prepare(recovered)
        self.concurrent(12, lambda index: self.worker().tick())
        ledger, _ = self.store.read("budget/ledger")
        self.assertEqual(sum(entry["cents"] for entry in ledger["reservations"].values()), 30)
        self.assertEqual(sum(entry["inflight"] for entry in ledger["reservations"].values()), 1)
        self.assertEqual(self.provider.submissions.count(("Jim", recovered["followup_key"])), 1)
        self.assertEqual(len(self.provider.submissions), 3)

    def test_concurrent_workers_submit_one_render_for_a_stable_identity(self):
        self.store.prepare(self.context)
        self.provider.status = {"status": "processing"}
        self.concurrent(12, lambda index: self.worker().tick())
        self.assertEqual(self.provider.submissions, [("Jim", self.context["followup_key"])])
        self.assertEqual(self.job()["status"], "submitted")
        ledger, _ = self.store.read("budget/ledger")
        self.assertEqual(len(ledger["reservations"]), 1)
        self.assertEqual(ledger["reservations"][self.context["followup_key"]]["cents"], 10)

    def test_fifty_waiting_jobs_do_not_starve_later_eligible_work(self):
        for index in range(52):
            self.store.prepare(self.make_context(stable_id="listing-" + str(index)))
        paths = self.objects.list("active/")
        self.assertEqual(len(paths), 52)
        eligible_key = paths[-1].split("/")[-1]
        self.worker(eligible=lambda job: True if job["followup_key"] == eligible_key else None).tick()
        self.assertEqual(self.provider.submissions, [("Jim", eligible_key)])
        self.assertEqual(self.store.read("jobs/" + eligible_key)[0]["status"], "submitted")
        self.assertEqual(len(self.objects.list("active/")), 52)

    def test_definite_submit_rejection_releases_capacity_without_assuming_refund(self):
        class Rejected(Exception):
            uncertain = False

        self.provider.submit_error = Rejected("Provider rejected the request")
        self.store.prepare(self.context)
        self.worker().tick()
        self.assertEqual(self.job()["status"], "failed")
        ledger, _ = self.store.read("budget/ledger")
        entry = ledger["reservations"][self.context["followup_key"]]
        self.assertEqual(entry["cents"], 10)
        self.assertFalse(entry["inflight"])
        self.worker(now=NOW + timedelta(hours=1)).run_job(self.context["followup_key"])
        self.assertEqual(len(self.provider.submissions), 1)
        self.assertEqual(self.objects.list("active/"), [])

    def test_provider_failure_releases_capacity_and_keeps_reservation(self):
        self.set_job(status="submitted", provider_id="failed-render")
        self.provider.status = {"status": "failed"}
        self.worker().tick()
        self.assertEqual(self.job()["status"], "failed")
        ledger, _ = self.store.read("budget/ledger")
        entry = ledger["reservations"][self.context["followup_key"]]
        self.assertEqual(entry["cents"], 10)
        self.assertFalse(entry["inflight"])
        self.assertEqual(self.provider.preparations, [])
        self.assertEqual(self.objects.list("assets/"), [])

    def test_completed_video_duration_does_not_infer_replacement_cost(self):
        self.set_job(status="submitted", provider_id="short-greeting-long-video")
        self.provider.status = {"status": "completed", "duration": 18.0,
                                "cost_cents": 8, "accounting_method": "provider_usage"}
        self.worker().tick()
        self.assertEqual(self.job()["status"], "ready")
        ledger, _ = self.store.read("budget/ledger")
        self.assertEqual(ledger["reservations"][self.context["followup_key"]]["cents"], 8)

    def test_unverified_estimated_cost_keeps_ten_cent_reservation(self):
        self.set_job(status="submitted", provider_id="estimated-greeting")
        self.provider.status = {"status": "completed", "duration": 8.5,
                                "billed_cents": 1, "accounting_method": "estimated"}
        self.worker().tick()
        ledger, _ = self.store.read("budget/ledger")
        self.assertEqual(ledger["reservations"][self.context["followup_key"]]["cents"], 10)

    def test_verified_over_ceiling_cost_is_recorded_and_never_published(self):
        self.set_job(status="submitted", provider_id="unexpected-cost")
        self.provider.status = {"status": "completed", "duration": 8.5,
                                "billed_cents": 12, "accounting_method": "provider_verified"}
        self.worker().tick()
        self.assertEqual(self.job()["status"], "failed")
        self.assertTrue(self.job()["budget_settled"])
        ledger, _ = self.store.read("budget/ledger")
        self.assertEqual(ledger["reservations"][self.context["followup_key"]]["cents"], 12)
        self.assertEqual(self.provider.preparations, [])
        self.assertEqual(self.objects.list("assets/"), [])

    def test_worker_restart_preserves_unknown_submission_without_retry(self):
        self.set_job(status="submitting", lease_at=video.iso(NOW))
        self.store.reserve(self.context["followup_key"], self.config, NOW)
        self.worker(now=NOW + timedelta(seconds=599)).run_job(self.context["followup_key"])
        self.assertEqual(self.job()["status"], "submitting")
        self.worker(now=NOW + timedelta(seconds=600)).tick()
        self.assertEqual(self.job()["status"], "uncertain")
        self.worker(now=NOW + timedelta(days=1)).tick()
        self.assertEqual(self.provider.submissions, [])
        self.assertEqual(self.provider.inspections, [])
        self.assertTrue(self.job()["budget_settled"])
        entry = self.store.read("budget/ledger")[0]["reservations"][self.context["followup_key"]]
        self.assertEqual(entry["cents"], 10)
        self.assertFalse(entry["inflight"])

    def test_submitted_job_polling_resumes_without_new_submission(self):
        self.set_job(status="submitted", provider_id="existing-render")
        self.provider.status = {"status": "processing"}
        self.worker().tick()
        self.assertEqual(self.job()["status"], "submitted")
        self.provider.status = {"status": "completed", "duration": 10.0}
        self.worker(now=NOW + timedelta(minutes=1)).tick()
        self.assertEqual(self.job()["status"], "ready")
        self.assertEqual(self.provider.inspections, ["existing-render", "existing-render"])
        self.assertEqual(self.provider.submissions, [])
        self.assertEqual(self.objects.list("active/"), [])

    def test_stale_processing_lease_resumes_known_render_id(self):
        self.set_job(status="processing", provider_id="existing-render", lease_at=video.iso(NOW))
        restarted = self.worker(now=NOW + timedelta(minutes=10))
        restarted.run_job(self.context["followup_key"])
        self.assertEqual(self.job()["status"], "submitted")
        restarted.run_job(self.context["followup_key"])
        self.assertEqual(self.job()["status"], "ready")
        self.assertEqual(self.provider.submissions, [])
        self.assertEqual(self.provider.inspections, ["existing-render"])

    def test_ineligible_or_unavailable_preflight_never_submits(self):
        self.store.prepare(self.context)
        self.worker(eligible=lambda job: None).run_job(self.context["followup_key"])
        self.assertEqual(self.job()["status"], "prepared")
        self.worker(eligible=lambda job: False).run_job(self.context["followup_key"])
        self.assertEqual(self.job()["status"], "cancelled")
        self.assertEqual(self.provider.submissions, [])
        self.assertEqual(self.objects.list("budget/"), [])
        message, meta = self.service.select(self.context, FALLBACK)
        self.assertIsNone(message)
        self.assertEqual(meta["message_id"], self.context["message_id"])

    def test_cancellation_during_submit_cannot_be_undone_by_provider_return(self):
        self.store.prepare(self.context)
        self.provider.on_submit = lambda: self.store.cancel(self.context["followup_key"], "Inbound reply")
        self.worker().run_job(self.context["followup_key"])
        self.assertEqual(self.job()["status"], "cancelled")
        self.assertEqual(len(self.provider.submissions), 1)
        self.worker(now=NOW + timedelta(hours=1)).run_job(self.context["followup_key"])
        self.assertEqual(len(self.provider.submissions), 1)
        self.assertEqual(len(self.provider.inspections), 1)
        self.assertTrue(self.job()["budget_settled"])
        self.assertEqual(self.job()["status"], "cancelled")
        self.assertEqual(self.objects.list("assets/"), [])

    def test_cancellation_during_media_preparation_prevents_ready_and_publication(self):
        self.set_job(status="submitted", provider_id="existing-render")
        self.provider.on_prepare = lambda: self.store.cancel(self.context["followup_key"], "Inbound reply")
        self.worker().tick()
        self.assertEqual(self.job()["status"], "cancelled")
        self.assertEqual(self.objects.list("assets/"), [])
        self.assertIsNone(self.store.get_public_video(self.job()["id"]))

    def test_cancelled_submission_keeps_accounting_active_after_second_worker_tick(self):
        self.store.prepare(self.context)

        def cancel_and_run_second_worker():
            self.store.cancel(self.context["followup_key"], "Inbound reply during submit")
            self.worker().tick()

        self.provider.on_submit = cancel_and_run_second_worker
        self.worker().tick()
        self.assertEqual(self.job()["status"], "cancelled")
        self.assertEqual(len(self.provider.submissions), 1)
        self.assertEqual(len(self.objects.list("active/")), 1)
        self.worker().tick()
        self.assertTrue(self.job()["budget_settled"])
        self.assertEqual(self.provider.preparations, [])
        self.assertEqual(self.objects.list("assets/"), [])
        self.assertEqual(self.objects.list("active/"), [])

    def test_cancelled_pending_render_is_polled_only_for_accounting(self):
        self.set_job(status="submitted", provider_id="cancelled-render")
        self.store.cancel(self.context["followup_key"], "Inbound reply")
        self.provider.status = {"status": "processing"}
        self.worker().tick()
        self.assertEqual(len(self.objects.list("active/")), 1)
        self.provider.status = {"status": "completed", "duration": 10.2}
        self.worker().tick()
        self.assertEqual(self.job()["status"], "cancelled")
        self.assertTrue(self.job()["budget_settled"])
        self.assertEqual(self.provider.preparations, [])
        self.assertEqual(self.objects.list("assets/"), [])
        self.assertEqual(self.objects.list("active/"), [])
        ledger, _ = self.store.read("budget/ledger")
        self.assertEqual(ledger["reservations"][self.context["followup_key"]]["cents"], 10)
        self.assertEqual(self.provider.accounting_inspections, ["cancelled-render", "cancelled-render"])
        self.assertFalse(ledger["reservations"][self.context["followup_key"]]["inflight"])

    def test_frozen_text_pending_render_settles_without_preparing_or_publishing(self):
        self.set_job(status="submitted", provider_id="late-render")
        message, meta = self.service.select(self.context, FALLBACK)
        self.worker().tick()
        self.assertEqual((message, meta["video_variant"]), (FALLBACK, "text"))
        self.assertEqual(self.job()["status"], "frozen_text")
        self.assertTrue(self.job()["budget_settled"])
        self.assertEqual(self.provider.preparations, [])
        self.assertEqual(self.objects.list("assets/"), [])
        self.assertEqual(self.objects.list("active/"), [])

    def test_inbound_reply_detected_after_render_cancels_before_publication(self):
        self.set_job(status="submitted", provider_id="existing-render")
        checks = iter((True, False))
        self.worker(eligible=lambda job: next(checks)).tick()
        self.assertEqual(self.job()["status"], "cancelled")
        self.assertEqual(self.objects.list("assets/"), [])
        self.assertIsNone(self.store.get_public_video(self.job()["id"]))

    def test_unknown_eligibility_after_render_does_not_publish_media(self):
        self.set_job(status="submitted", provider_id="existing-render")
        checks = iter((True, None))
        self.worker(eligible=lambda job: next(checks)).tick()
        self.assertEqual(len(self.provider.preparations), 1)
        self.assertNotEqual(self.job()["status"], "ready")
        self.assertIsNone(self.store.get_public_video(self.job()["id"]))
        self.assertEqual(self.objects.list("assets/"), [])

    def test_text_selected_during_render_stays_text_after_completion(self):
        self.set_job(status="submitted", provider_id="existing-render")
        self.provider.on_prepare = lambda: self.service.select(self.context, FALLBACK)
        self.worker().tick()
        self.assertEqual(self.job()["status"], "frozen_text")
        message, meta = self.service.select(self.context, "Later fallback")
        self.assertEqual((message, meta["video_variant"]), (FALLBACK, "text"))
        self.assertEqual(self.objects.list("assets/"), [])

    def test_duplicate_completions_are_idempotent_and_provider_id_must_match(self):
        self.set_job(status="submitted", provider_id="expected-render")
        key = self.context["followup_key"]
        self.assertFalse(self.store.finish(key, "different-render", MEDIA))
        self.assertEqual(self.objects.list("assets/"), [])
        self.assertTrue(self.store.finish(key, "expected-render", MEDIA))
        writes_before = list(self.objects.writes)
        self.assertFalse(self.store.finish(key, "expected-render", MEDIA))
        self.assertEqual(self.objects.writes, writes_before)
        self.assertEqual(len(self.objects.list("assets/")), 3)
        self.assertEqual(len(self.objects.list("public/")), 1)

    def test_concurrent_duplicate_completions_store_one_asset_set(self):
        self.set_job(status="submitted", provider_id="expected-render")
        self.concurrent(8, lambda index: self.store.finish(self.context["followup_key"], "expected-render", MEDIA))
        self.assertEqual(self.job()["status"], "ready")
        asset_writes = [entry for entry in self.objects.writes if entry[0].startswith("assets/")]
        self.assertEqual(len(asset_writes), 3)
        self.assertEqual(len({entry[0] for entry in asset_writes}), 3)

    def test_public_assets_are_typed_and_hidden_immediately_after_cancellation(self):
        ready = self.make_ready()
        opaque_id = ready["id"]
        self.assertEqual(set(self.store.get_public_video(opaque_id)), {"first_name", "duration", "status"})
        for kind, body, content_type in (("video", MEDIA["media_bytes"], "video/mp4"),
                                         ("poster", MEDIA["poster_bytes"], "image/jpeg"),
                                         ("captions", MEDIA["vtt_bytes"], "text/vtt; charset=utf-8")):
            with self.subTest(kind=kind):
                asset = self.store.get_public_asset(opaque_id, kind)
                self.assertEqual(asset["body"], body)
                self.assertEqual(asset["content_type"], content_type)
                self.assertTrue(asset["etag"].isdigit())
        self.assertIsNone(self.store.get_public_asset(opaque_id, "../jobs"))
        self.assertIsNone(self.store.get_public_video("not-an-opaque-id"))
        self.store.cancel(self.context["followup_key"], "Inbound reply")
        self.assertIsNone(self.store.get_public_video(opaque_id))
        for kind in ("video", "poster", "captions"):
            self.assertIsNone(self.store.get_public_asset(opaque_id, kind))
        self.assertEqual(len(self.objects.list("assets/")), 3)

    def test_unapproved_public_origins_are_rejected(self):
        for origin in ("http://go.crispshortsales.com", "https://example.com",
                       "https://go.crispshortsales.com/path", "https://go.crispshortsales.com?x=1",
                       "https://go.crispshortsales.com#x", "https://user@go.crispshortsales.com",
                       "https://go.crispshortsales.com:443"):
            with self.subTest(origin=origin), self.assertRaises(ValueError):
                self.make_config(VIDEO_FOLLOWUP_PUBLIC_ORIGIN=origin)


if __name__ == "__main__":
    unittest.main()
