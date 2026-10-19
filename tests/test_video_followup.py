import importlib.util
import hashlib
import json
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import Mock, patch

import video_followup_speech_guard as speech


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


def speech_proof(name="Jim", media=MEDIA["media_bytes"]):
    return {"accepted": True, "policy_version": speech.POLICY_VERSION, "model_id": speech.MODEL_ID,
        "model_revision": speech.MODEL_REVISION, "model_manifest_sha256": speech.MODEL_MANIFEST_SHA256,
        "config_sha256": speech.CONFIG_SHA256, "source_audio_sha256": hashlib.sha256(b"greeting").hexdigest(),
        "media_sha256": hashlib.sha256(media).hexdigest(), "greeting_seconds": 1.5,
        "expected_name": name, "normalized_transcript": "hey " + name.casefold(),
        "confidence": [{"avg_logprob": -0.1, "no_speech_prob": 0.01}]}


def planned_speech_proof(name="Jim", media=MEDIA["media_bytes"]):
    proof = speech_proof(name, media)
    original = {key: value for key, value in proof.items() if key not in {"media_sha256", "greeting_seconds"}}
    lineage = {"accepted": True, "lineage_version": speech.PLANNED_AUDIO_VERIFICATION_CONFIG["lineage_version"],
        "first_name": name, "final_media_sha256": proof["media_sha256"],
        "checked_audio_sha256": proof["source_audio_sha256"], "greeting_seconds": proof["greeting_seconds"],
        "speech_proof_sha256": hashlib.sha256(json.dumps(original, sort_keys=True, separators=(",", ":")).encode()).hexdigest(),
        **{field: hashlib.sha256(field.encode()).hexdigest() for field in (
            "manifest_sha256", "planned_audio_sha256", "full_pcm_sha256", "greeting_pcm_sha256", "aac_track_sha256")}}
    return {**proof, "verification_method": speech.PLANNED_AUDIO_VERIFICATION_METHOD,
            "verification_config_sha256": speech.PLANNED_AUDIO_VERIFICATION_CONFIG_SHA256, "audio_lineage": lineage}


MEDIA["speech_proof"] = speech_proof()


def initial_receipt(context):
    initial = datetime.fromisoformat(video.iso(context["initial_sent_at"]).replace("Z", "+00:00"))
    suffix = hashlib.sha256(json.dumps([context["stable_id"], video.canonical_phone(context["phone"]),
        video.iso(initial)], separators=(",", ":")).encode()).hexdigest()[:16]
    message_id = "initial-12-" + suffix
    return {"schema": video.INITIAL_RECEIPT_SCHEMA, "status": "sent", "message_id": message_id,
        "request_id": "scheduler-" + message_id, "phone": video.canonical_phone(context["phone"]),
        "stable_id": context["stable_id"], "created_at": video.iso(initial - timedelta(seconds=10)),
        "send_started_at": video.iso(initial - timedelta(seconds=5)), "sent_at": video.iso(initial + timedelta(seconds=5))}


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

    def list(self, prefix, limit=50, max_items=None):
        with self._lock:
            result = sorted(key for key in self._objects if key.startswith(prefix))
            if max_items is not None and len(result) > max_items:
                raise video.InitialReceiptHold("Test inventory exceeds its complete-scan bound")
            return result


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
        return {**MEDIA, "speech_proof": speech_proof(first_name)}

    def audit_media(self, job, media_bytes):
        return speech_proof(job["first_name"], media_bytes)


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
        if "initial_receipt" not in values:
            values["initial_receipt"] = initial_receipt(values)
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

    def test_unverified_legacy_ready_falls_back_under_same_identity(self):
        ready = self.make_ready()
        self.set_job(speech_proof=None)
        self.assertIsNotNone(self.store.get_public_video(ready["id"]))
        message, metadata = self.service.select(self.context, FALLBACK)
        self.assertEqual(message, FALLBACK)
        self.assertEqual(metadata["followup_key"], self.context["followup_key"])
        self.assertEqual(metadata["video_variant"], "text")
        self.assertEqual(self.job()["status"], "frozen_text")
        self.assertEqual(self.provider.submissions, [])

    def test_selection_free_audit_can_reuse_legacy_ready_video(self):
        self.make_ready()
        self.set_job(speech_proof=None)
        worker = self.worker()
        self.service.audit_ready = worker.audit_ready
        message, metadata = self.service.select(self.context, FALLBACK)
        self.assertIn("https://go.crispshortsales.com/v/", message)
        proof = metadata["video_speech_proof"]
        for field in ("followup_key", "phone", "initial_sent_at", "stable_id"):
            self.assertEqual(proof[field], self.context[field])
        self.assertEqual(proof["video_job_id"], self.job()["id"])
        self.assertEqual(self.provider.submissions, [])

    def test_audit_timeout_uses_text_without_changing_followup_identity(self):
        self.make_ready()
        self.set_job(speech_proof=None)
        self.service.audit_ready = Mock(side_effect=speech.SpeechGuardError("speech_guard_timeout"))
        message, metadata = self.service.select(self.context, FALLBACK)
        self.assertEqual(message, FALLBACK)
        self.assertEqual(metadata["message_id"], self.context["message_id"])
        self.assertEqual(self.job()["status"], "frozen_text")

    def test_legacy_ready_is_reindexed_and_audited_without_generation_enabled(self):
        self.make_ready()
        self.set_job(speech_proof=None)
        path = "active/" + self.context["followup_key"]
        _, version = self.objects.read(path)
        self.objects.delete(path, version)
        self.store.prepare(self.context)
        self.assertIsNotNone(self.objects.read(path)[0])
        self.config.generation_enabled = False
        self.assertTrue(self.worker().audit_ready(self.context["followup_key"]))
        self.assertTrue(self.store.verified_video(self.job()))
        self.assertEqual(self.provider.submissions, [])

    def test_audit_never_overwrites_concurrent_freeze_or_cancellation(self):
        for mode in ("frozen_text", "cancelled", "frozen_video"):
            with self.subTest(mode=mode):
                self.set_job(variant=None, ready_audit_token=None, ready_audit_lease_at=None,
                             ready_audit_attempts=0)
                self.make_ready()
                self.set_job(speech_proof=None, variant=None)
                def audit(job, content):
                    self.set_job(status=mode, variant=mode.removeprefix("frozen_"))
                    return speech_proof(job["first_name"], content)
                self.provider.audit_media = audit
                self.assertFalse(self.worker().audit_ready(self.context["followup_key"]))
                self.assertEqual(self.job()["status"], mode)
                self.assertIsNone(self.job()["speech_proof"])

    def test_unverified_frozen_video_returns_same_key_text_without_mutating_record(self):
        self.make_ready()
        self.service.select(self.context, FALLBACK)
        self.set_job(speech_proof=None)
        before = self.job()
        message, metadata = self.service.select(self.context, FALLBACK)
        self.assertEqual(message, FALLBACK)
        self.assertEqual(metadata["followup_key"], self.context["followup_key"])
        self.assertEqual(metadata["video_variant"], "text")
        self.assertEqual(before, self.job())
        self.assertIsNotNone(self.store.get_public_video(before["id"]))

    def test_final_proof_must_match_actual_stored_media_on_conflict(self):
        job = self.set_job(status="submitted", provider_id="render-ready")
        self.objects.write(f"assets/{job['id']}/video", b"other video", 0, "video/mp4")
        with self.assertRaises(speech.SpeechGuardError):
            self.store.finish(self.context["followup_key"], "render-ready", MEDIA)
        self.assertEqual(self.job()["status"], "submitted")
        self.assertIsNone(self.store.get_public_video(job["id"]))

    def test_changed_served_bytes_invalidate_proof_before_freeze(self):
        job = self.make_ready()
        path = f"assets/{job['id']}/video"
        _, version = self.objects.read(path)
        self.objects.write(path, b"changed after proof", version, "video/mp4")
        message, metadata = self.service.select(self.context, FALLBACK)
        self.assertEqual(message, FALLBACK)
        self.assertEqual(metadata["video_variant"], "text")

    def test_final_name_failure_is_terminal_without_local_or_paid_retry(self):
        self.store.prepare(self.context)
        worker = self.worker()
        worker.tick()
        self.provider.prepare_media = Mock(side_effect=speech.SpeechGuardError("speech_guard_name_mismatch"))
        worker.tick()
        self.assertEqual(self.job()["status"], "failed")
        self.assertTrue(self.job()["budget_settled"])
        worker.tick()
        self.assertEqual(len(self.provider.submissions), 1)
        self.provider.prepare_media.assert_called_once()
        message, _ = self.service.select(self.context, FALLBACK)
        self.assertEqual(message, FALLBACK)

    def test_transient_final_verifier_failure_resumes_same_paid_render_after_backoff(self):
        self.set_job(status="submitted", provider_id="already-paid-render")
        self.provider.prepare_media = Mock(side_effect=[
            speech.SpeechGuardError("speech_guard_timeout"), MEDIA])
        self.worker().tick()
        self.assertEqual(self.job()["status"], "submitted")
        self.assertEqual(self.job()["provider_id"], "already-paid-render")
        self.assertEqual(self.job()["finalization_attempts"], 1)
        self.assertEqual(self.job()["underlying_code"], "speech_guard_timeout")
        self.assertEqual(self.job()["failure_stage"], "final_media_prepare")
        self.worker(now=NOW + timedelta(seconds=59)).tick()
        self.assertEqual(len(self.provider.inspections), 1)
        self.worker(now=NOW + timedelta(seconds=60)).tick()
        self.assertEqual(self.job()["status"], "ready")
        self.assertEqual(self.job()["finalization_attempts"], 2)
        self.assertEqual(self.job()["last_verification_failure"]["underlying_code"], "speech_guard_timeout")
        for field in video._ACTIVE_VERIFICATION_FAILURE_FIELDS:
            self.assertNotIn(field, self.job())
        self.assertEqual(self.provider.submissions, [])
        self.assertEqual(self.provider.prepare_media.call_count, 2)
        self.assertEqual(self.objects.list("active/"), [])

    def test_transient_final_verifier_retry_is_bounded_without_spend_or_publication(self):
        self.set_job(status="submitted", provider_id="already-paid-render")
        self.provider.prepare_media = Mock(side_effect=speech.SpeechGuardError("speech_guard_busy"))
        for attempt in range(3):
            self.worker(now=NOW + timedelta(seconds=attempt * 60)).tick()
        self.assertEqual(self.job()["status"], "failed")
        self.assertEqual(self.job()["finalization_attempts"], 3)
        self.assertTrue(self.job()["verification_retry_exhausted"])
        self.assertEqual(self.job()["underlying_code"], "speech_guard_busy")
        self.worker(now=NOW + timedelta(days=1)).tick()
        self.assertEqual(self.provider.prepare_media.call_count, 3)
        self.assertEqual(self.provider.submissions, [])
        self.assertEqual(self.objects.list("assets/"), [])
        entry = self.store.read("budget/ledger")[0]["reservations"][self.context["followup_key"]]
        self.assertEqual(entry["cents"], 10)
        self.assertFalse(entry["inflight"])

    def test_final_speech_rejection_retains_safe_diagnostics_without_retry(self):
        self.set_job(status="submitted", provider_id="already-paid-render")
        diagnostic = {**speech_proof(), "observed_transcript": "Hey Jimmy!",
                      "provider_url": "https://private.example/?secret=token"}
        self.provider.prepare_media = Mock(side_effect=speech.SpeechGuardError(
            "speech_guard_name_mismatch", diagnostics=diagnostic))
        self.worker().tick()
        job = self.job()
        self.assertEqual(job["status"], "failed")
        self.assertEqual(job["underlying_code"], "speech_guard_name_mismatch")
        self.assertEqual(job["failure_stage"], "final_media_prepare")
        self.assertEqual(job["speech_diagnostics"]["observed_transcript"], "Hey Jimmy!")
        self.assertNotIn("provider_url", job["speech_diagnostics"])
        self.assertNotIn("secret=token", repr(job))
        self.worker(now=NOW + timedelta(minutes=1)).tick()
        self.provider.prepare_media.assert_called_once()

    def test_frozen_text_wins_over_a_pending_free_verification_retry(self):
        self.set_job(status="submitted", provider_id="already-paid-render")
        self.provider.prepare_media = Mock(side_effect=speech.SpeechGuardError("speech_guard_busy"))
        self.worker().tick()
        self.service.select(self.context, FALLBACK)
        frozen = self.job()
        self.worker(now=NOW + timedelta(minutes=1)).tick()
        self.assertEqual(self.job(), frozen)
        self.assertEqual(self.provider.prepare_media.call_count, 1)
        self.assertEqual(self.provider.submissions, [])
        self.assertEqual(self.objects.list("public/"), [])

    def test_final_verification_error_cannot_overwrite_a_newer_processing_lease(self):
        self.set_job(status="submitted", provider_id="already-paid-render")
        def stale_failure(*args):
            self.store.mutate("jobs/" + self.context["followup_key"], lambda old: {
                **old, "lease_at": video.iso(NOW + timedelta(minutes=11)), "finalization_attempts": 2})
            raise speech.SpeechGuardError("speech_guard_name_mismatch")
        self.provider.prepare_media = Mock(side_effect=stale_failure)
        self.worker().tick()
        self.assertEqual(self.job()["status"], "processing")
        self.assertNotIn("failure_stage", self.job())
        self.assertEqual(self.job()["lease_at"], video.iso(NOW + timedelta(minutes=11)))

    def test_legacy_ready_transient_audit_keeps_active_index_for_bounded_free_retry(self):
        self.make_ready()
        self.set_job(speech_proof=None)
        self.provider.audit_media = Mock(side_effect=[
            speech.SpeechGuardError("speech_guard_busy"), speech_proof()])
        self.worker().tick()
        self.assertEqual(self.job()["status"], "ready")
        self.assertEqual(self.job()["ready_audit_attempts"], 1)
        self.assertTrue(self.objects.list("active/"))
        self.worker(now=NOW + timedelta(seconds=59)).tick()
        self.provider.audit_media.assert_called_once()
        self.worker(now=NOW + timedelta(seconds=60)).tick()
        self.assertTrue(self.store.verified_video(self.job()))
        self.assertEqual(self.job()["last_verification_failure"]["underlying_code"], "speech_guard_busy")
        for field in video._ACTIVE_VERIFICATION_FAILURE_FIELDS:
            self.assertNotIn(field, self.job())
        self.assertEqual(self.provider.audit_media.call_count, 2)
        self.assertEqual(self.provider.submissions, [])
        self.assertEqual(self.objects.list("active/"), [])

    def test_legacy_ready_audit_runtime_failures_exhaust_without_restarting_paid_job(self):
        self.make_ready()
        self.set_job(speech_proof=None)
        self.provider.audit_media = Mock(side_effect=speech.SpeechGuardError("speech_guard_unavailable"))
        for attempt in range(3):
            self.worker(now=NOW + timedelta(seconds=60 * attempt)).tick()
        self.assertEqual(self.job()["status"], "failed")
        self.assertEqual(self.job()["ready_audit_attempts"], 3)
        self.assertEqual(self.job()["failure_stage"], "ready_audit")
        self.worker(now=NOW + timedelta(days=1)).tick()
        self.assertEqual(self.provider.audit_media.call_count, 3)
        self.assertEqual(self.provider.submissions, [])

    def test_stale_eligibility_cancellation_cannot_rewrite_frozen_text(self):
        self.set_job(status="submitted", provider_id="already-paid-render")
        def became_ineligible(job):
            self.service.select(self.context, FALLBACK)
            return False
        self.worker(eligible=became_ineligible).tick()
        self.assertEqual(self.job()["status"], "frozen_text")
        self.assertEqual(self.job()["message"], FALLBACK)
        self.assertEqual(self.provider.preparations, [])

    def test_ready_audit_lease_allows_only_one_concurrent_local_attempt(self):
        self.make_ready()
        self.set_job(speech_proof=None)
        entered, release = threading.Event(), threading.Event()
        def audit(job, content):
            entered.set()
            self.assertTrue(release.wait(timeout=5))
            return speech_proof(job["first_name"], content)
        self.provider.audit_media = Mock(side_effect=audit)
        with ThreadPoolExecutor(max_workers=1) as executor:
            first = executor.submit(self.worker().audit_ready, self.context["followup_key"])
            try:
                self.assertTrue(entered.wait(timeout=5))
                self.assertFalse(self.worker().audit_ready(self.context["followup_key"]))
                self.assertEqual(self.job()["ready_audit_attempts"], 1)
                self.provider.audit_media.assert_called_once()
            finally:
                release.set()
            self.assertTrue(first.result(timeout=5))
        self.assertEqual(self.provider.submissions, [])

    def test_expired_third_processing_attempt_does_not_start_a_fourth_verification(self):
        self.set_job(status="processing", provider_id="already-paid-render", lease_at=video.iso(NOW),
                     finalization_attempts=3, budget_settled=True)
        self.store.settle(self.context["followup_key"], video.iso(NOW))
        worker = self.worker(now=NOW + timedelta(minutes=10))
        worker.tick()
        self.assertEqual(self.job()["status"], "submitted")
        worker.tick()
        self.assertEqual(self.job()["status"], "failed")
        self.assertEqual(self.job()["finalization_attempts"], 3)
        self.assertTrue(self.job()["verification_retry_exhausted"])
        self.assertEqual(self.job()["underlying_code"], "local_verification_attempt_limit")
        self.assertEqual(self.provider.inspections, [])
        self.assertEqual(self.provider.preparations, [])
        self.assertEqual(self.provider.submissions, [])

    def test_expired_third_ready_audit_does_not_start_a_fourth_verification(self):
        self.make_ready()
        self.set_job(speech_proof=None, ready_audit_attempts=3,
                     ready_audit_token="crashed-audit", ready_audit_lease_at=video.iso(NOW))
        self.provider.audit_media = Mock()
        self.worker(now=NOW + timedelta(minutes=10)).tick()
        self.assertEqual(self.job()["status"], "failed")
        self.assertEqual(self.job()["ready_audit_attempts"], 3)
        self.assertEqual(self.job()["underlying_code"], "local_verification_attempt_limit")
        self.provider.audit_media.assert_not_called()
        self.assertEqual(self.provider.submissions, [])

    def test_stale_successful_finalization_cannot_publish_for_a_newer_lease(self):
        self.set_job(status="submitted", provider_id="already-paid-render")
        self.provider.on_prepare = lambda: self.set_job(
            lease_at=video.iso(NOW + timedelta(minutes=11)), finalization_attempts=2)
        self.worker().tick()
        self.assertEqual(self.job()["status"], "processing")
        self.assertEqual(self.job()["finalization_attempts"], 2)
        self.assertEqual(self.objects.list("assets/"), [])
        self.assertEqual(self.objects.list("public/"), [])

    def test_nontransient_speech_failures_are_terminal_on_first_attempt(self):
        for code in ("speech_guard_name_mismatch", "speech_guard_low_confidence",
                     "speech_guard_model_mismatch", "speech_guard_dependency_mismatch",
                     "speech_guard_output_invalid", "speech_guard_audio_invalid"):
            with self.subTest(code=code):
                self.set_job(status="submitted", provider_id="already-paid-render", finalization_attempts=0)
                self.provider.prepare_media = Mock(side_effect=speech.SpeechGuardError(code))
                self.worker().run_job(self.context["followup_key"])
                self.assertEqual(self.job()["status"], "failed")
                self.assertEqual(self.job()["finalization_attempts"], 1)
                self.assertEqual(self.job()["underlying_code"], code)
                self.assertFalse(self.job()["verification_retry_pending"])
                self.provider.prepare_media.assert_called_once()
        self.assertEqual(self.provider.submissions, [])

    def test_failed_provider_status_keeps_only_sanitized_provider_diagnostics(self):
        from video_followup_same_take import SameTakeVideoProvider
        self.set_job(status="submitted", provider_id="already-paid-render")
        self.provider.safe_failure_diagnostics = SameTakeVideoProvider.safe_failure_diagnostics
        self.provider.status = {"status": "failed", "failure_code": "greeting_preparation_failed",
            "failure_stage": "audio_screen", "underlying_code": "narration_pitch_outlier",
            "exception_type": "VideoProviderError", "media_url": "https://private.example/?secret=token"}
        self.worker().tick()
        self.assertEqual(self.job()["status"], "failed")
        self.assertEqual(self.job()["failure_stage"], "audio_screen")
        self.assertEqual(self.job()["underlying_code"], "narration_pitch_outlier")
        self.assertEqual(self.job()["failure_code"], "greeting_preparation_failed")
        self.assertNotIn("secret=token", repr(self.job()))
        self.assertTrue(self.job()["budget_settled"])
        self.assertEqual(self.provider.submissions, [])

    def test_finish_rejects_malformed_media_before_writing_any_assets(self):
        self.set_job(status="submitted", provider_id="already-paid-render")
        for media in (None, {**MEDIA, "poster_bytes": None}, {**MEDIA, "vtt_bytes": "not-bytes"},
                      {**MEDIA, "duration": float("nan")}, {**MEDIA, "duration": True}):
            with self.subTest(media=media), self.assertRaises(speech.SpeechGuardError):
                self.store.finish(self.context["followup_key"], "already-paid-render", media)
            self.assertEqual(self.objects.list("assets/"), [])

    def unpaid_preflight_error(self, code="speech_guard_busy", *, first_name="Jim", job_id=None):
        from video_followup_same_take import SameTakeVideoProvider, _UnpaidPreflightTransient
        self.provider.safe_unpaid_preflight_failure = SameTakeVideoProvider.safe_unpaid_preflight_failure
        return _UnpaidPreflightTransient(speech.SpeechGuardError(code), first_name,
                                         job_id or self.context["followup_key"])

    def test_confirmed_unpaid_preflight_reuses_one_reservation_after_backoff(self):
        self.store.prepare(self.context)
        original_id = self.job()["id"]
        self.provider.submit_error = self.unpaid_preflight_error()
        self.worker().tick()
        self.assertEqual(self.job()["status"], "prepared")
        self.assertEqual(self.job()["preflight_attempts"], 1)
        self.assertTrue(self.job()["preflight_confirmed_unpaid"])
        entry = self.store.read("budget/ledger")[0]["reservations"][self.context["followup_key"]]
        self.assertEqual(entry["cents"], 10)
        self.assertTrue(entry["inflight"])
        self.worker(now=NOW + timedelta(seconds=59)).tick()
        self.assertEqual(len(self.provider.submissions), 1)
        self.provider.submit_error = None
        self.worker(now=NOW + timedelta(seconds=60)).tick()
        job = self.job()
        self.assertEqual(job["status"], "submitted")
        self.assertEqual(job["id"], original_id)
        self.assertEqual(job["preflight_attempts"], 2)
        self.assertEqual(job["last_preflight_failure"]["underlying_code"], "speech_guard_busy")
        for field in video._ACTIVE_VERIFICATION_FAILURE_FIELDS + ("preflight_retry_at",):
            self.assertNotIn(field, job)
        ledger = self.store.read("budget/ledger")[0]["reservations"]
        self.assertEqual(list(ledger), [self.context["followup_key"]])
        self.assertEqual(ledger[self.context["followup_key"]]["cents"], 10)

    def test_confirmed_unpaid_preflight_stops_after_three_attempts_without_refund(self):
        self.store.prepare(self.context)
        self.provider.submit_error = self.unpaid_preflight_error("speech_guard_timeout")
        for attempt in range(3):
            self.worker(now=NOW + timedelta(seconds=attempt * 60)).tick()
        self.assertEqual(self.job()["status"], "failed")
        self.assertEqual(self.job()["preflight_attempts"], 3)
        self.assertTrue(self.job()["verification_retry_exhausted"])
        self.worker(now=NOW + timedelta(days=1)).tick()
        self.assertEqual(len(self.provider.submissions), 3)
        ledger = self.store.read("budget/ledger")[0]["reservations"]
        self.assertEqual(ledger[self.context["followup_key"]]["cents"], 10)
        self.assertFalse(ledger[self.context["followup_key"]]["inflight"])
        self.assertEqual(self.objects.list("assets/"), [])

    def test_generic_confirmed_rejection_does_not_gain_a_submit_retry(self):
        self.store.prepare(self.context)
        self.unpaid_preflight_error()
        self.provider.submit_error = speech.SpeechGuardError("speech_guard_busy")
        self.worker().tick()
        self.assertEqual(self.job()["status"], "failed")
        self.worker(now=NOW + timedelta(minutes=1)).tick()
        self.assertEqual(len(self.provider.submissions), 1)

    def test_preflight_failure_for_another_identity_never_retries(self):
        self.store.prepare(self.context)
        self.provider.submit_error = self.unpaid_preflight_error(first_name="Sue")
        self.worker().tick()
        self.assertEqual(self.job()["status"], "failed")
        self.worker(now=NOW + timedelta(minutes=1)).tick()
        self.assertEqual(len(self.provider.submissions), 1)

    def test_confirmed_unpaid_preflight_cannot_overwrite_concurrent_text_freeze(self):
        self.store.prepare(self.context)
        self.provider.submit_error = self.unpaid_preflight_error()
        self.provider.on_submit = lambda: self.service.select(self.context, FALLBACK)
        self.worker().tick()
        frozen = self.job()
        self.assertEqual(frozen["status"], "frozen_text")
        self.assertNotIn("preflight_retry_at", frozen)
        self.worker(now=NOW + timedelta(seconds=60)).tick()
        self.assertEqual(self.job(), frozen)
        self.assertEqual(len(self.provider.submissions), 1)

    def test_confirmed_unpaid_preflight_cannot_overwrite_newer_submit_lease(self):
        self.store.prepare(self.context)
        self.provider.submit_error = self.unpaid_preflight_error()
        self.provider.on_submit = lambda: self.set_job(
            lease_at=video.iso(NOW + timedelta(minutes=11)), preflight_attempts=2)
        self.worker().tick()
        self.assertEqual(self.job()["status"], "submitting")
        self.assertEqual(self.job()["preflight_attempts"], 2)
        self.assertNotIn("preflight_retry_at", self.job())

    def test_unpaid_preflight_never_rearms_an_already_settled_reservation(self):
        self.store.prepare(self.context)
        self.provider.submit_error = self.unpaid_preflight_error()
        self.worker().tick()
        self.store.settle(self.context["followup_key"], video.iso(NOW))
        before = self.store.read("budget/ledger")[0]
        self.provider.submit_error = None
        self.worker(now=NOW + timedelta(seconds=60)).tick()
        self.assertEqual(self.job()["status"], "failed")
        self.assertEqual(self.job()["underlying_code"], "unpaid_preflight_reservation_invalid")
        self.assertEqual(len(self.provider.submissions), 1)
        self.assertEqual(self.store.read("budget/ledger")[0], before)

    def test_corrupt_local_attempt_counters_fail_closed_without_provider_work(self):
        for field, status in (("preflight_attempts", "prepared"),
                              ("finalization_attempts", "submitted")):
            for value in (-1, True, "1", 1.5, None, 4):
                with self.subTest(field=field, value=value):
                    self.set_job(**{"status": status,
                        "provider_id": None if status == "prepared" else "already-paid-render",
                        "preflight_attempts": 0, "finalization_attempts": 0, field: value})
                    self.worker().run_job(self.context["followup_key"])
                    self.assertEqual(self.job()["status"], "failed")
                    self.assertEqual(self.job()["underlying_code"], "invalid_verification_attempt_count")
        self.assertEqual(self.provider.submissions, [])
        self.assertEqual(self.provider.inspections, [])

    def test_corrupt_ready_audit_counter_fails_closed_without_audit(self):
        self.make_ready()
        self.provider.audit_media = Mock()
        for value in (-1, True, "1", 1.5, None, 4):
            with self.subTest(value=value):
                self.set_job(status="ready", speech_proof=None, ready_audit_attempts=value)
                self.assertFalse(self.worker().audit_ready(self.context["followup_key"]))
                self.assertEqual(self.job()["status"], "failed")
                self.assertEqual(self.job()["underlying_code"], "invalid_verification_attempt_count")
        self.provider.audit_media.assert_not_called()

    def test_stale_definite_submit_rejection_cannot_settle_a_newer_lease(self):
        self.store.prepare(self.context)
        self.provider.submit_error = speech.SpeechGuardError("speech_guard_name_mismatch")
        self.provider.on_submit = lambda: self.set_job(
            lease_at=video.iso(NOW + timedelta(minutes=11)), preflight_attempts=2)
        self.worker().tick()
        self.assertEqual(self.job()["status"], "submitting")
        self.assertEqual(self.job()["preflight_attempts"], 2)
        entry = self.store.read("budget/ledger")[0]["reservations"][self.context["followup_key"]]
        self.assertTrue(entry["inflight"])
        self.assertEqual(entry["cents"], 10)

    def test_worker_pins_planned_method_before_final_media_processing(self):
        self.set_job(status="submitted", provider_id="already-paid-render")
        binding = {"verification_method": speech.PLANNED_AUDIO_VERIFICATION_METHOD,
                   "verification_config_sha256": speech.PLANNED_AUDIO_VERIFICATION_CONFIG_SHA256}
        self.provider.status = {"status": "completed", "provider_id": "already-paid-render", **binding}
        def prepare(status, name):
            self.assertEqual(self.job()["status"], "processing")
            for field, value in binding.items():
                self.assertEqual(self.job()[field], value)
            return {**MEDIA, "speech_proof": planned_speech_proof(name)}
        self.provider.prepare_media = Mock(side_effect=prepare)
        self.worker().tick()
        self.assertEqual(self.job()["status"], "ready")
        self.assertTrue(self.store.verified_video(self.job()))
        self.assertEqual(self.provider.submissions, [])

    def test_worker_rejects_partial_unknown_or_downgraded_verification_identity(self):
        binding = {"verification_method": speech.PLANNED_AUDIO_VERIFICATION_METHOD,
                   "verification_config_sha256": speech.PLANNED_AUDIO_VERIFICATION_CONFIG_SHA256}
        cases = (({}, {"verification_method": binding["verification_method"]}),
                 ({}, {"verification_method": None}),
                 ({}, {"verification_config_sha256": binding["verification_config_sha256"]}),
                 ({}, {**binding, "verification_method": "unapproved-method"}),
                 ({}, {**binding, "verification_config_sha256": "0" * 64}),
                 (binding, {}),
                 ({"verification_method": binding["verification_method"]}, binding))
        for job_binding, status_binding in cases:
            with self.subTest(job_binding=job_binding, status_binding=status_binding):
                self.objects = MemoryObjects()
                self.store = video.Store(self.objects)
                self.set_job(status="submitted", provider_id="already-paid-render", **job_binding)
                self.provider.status = {"status": "completed", "provider_id": "already-paid-render", **status_binding}
                self.provider.prepare_media = Mock(return_value=MEDIA)
                self.worker().tick()
                self.assertEqual(self.job()["status"], "failed")
                self.assertEqual(self.job()["underlying_code"], "speech_guard_output_invalid")
                self.assertTrue(self.job()["budget_settled"])
                self.provider.prepare_media.assert_not_called()
                self.assertEqual(self.objects.list("assets/"), [])
        self.assertEqual(self.provider.submissions, [])

    def test_planned_job_rejects_stripped_or_wrong_method_proof_before_assets(self):
        self.set_job(status="submitted", provider_id="already-paid-render",
                     verification_method=speech.PLANNED_AUDIO_VERIFICATION_METHOD,
                     verification_config_sha256=speech.PLANNED_AUDIO_VERIFICATION_CONFIG_SHA256)
        complete = planned_speech_proof()
        stripped = {key: value for key, value in complete.items()
                    if key not in {"verification_method", "verification_config_sha256", "audio_lineage"}}
        self.assertTrue(speech.validate_speech_proof(stripped, first_name="Jim", media_sha256=complete["media_sha256"]))
        for proof in (stripped, speech_proof(), {**complete, "verification_config_sha256": "0" * 64},
                      {**complete, "verification_method": "unapproved-method"}):
            with self.subTest(proof=proof), self.assertRaises(speech.SpeechGuardError):
                self.store.finish(self.context["followup_key"], "already-paid-render", {**MEDIA, "speech_proof": proof})
            self.assertEqual(self.objects.list("assets/"), [])

    def test_planned_ready_job_cannot_send_a_proof_downgraded_to_legacy(self):
        self.set_job(status="submitted", provider_id="already-paid-render",
                     verification_method=speech.PLANNED_AUDIO_VERIFICATION_METHOD,
                     verification_config_sha256=speech.PLANNED_AUDIO_VERIFICATION_CONFIG_SHA256)
        self.assertTrue(self.store.finish(self.context["followup_key"], "already-paid-render",
                                         {**MEDIA, "speech_proof": planned_speech_proof()}))
        self.set_job(speech_proof=speech_proof())
        self.assertFalse(self.store.verified_video(self.job()))
        message, metadata = self.service.select(self.context, FALLBACK)
        self.assertEqual(message, FALLBACK)
        self.assertEqual(metadata["video_variant"], "text")

    def test_legacy_completed_job_without_method_fields_keeps_existing_rules(self):
        self.set_job(status="submitted", provider_id="already-paid-render")
        self.worker().tick()
        self.assertEqual(self.job()["status"], "ready")
        self.assertNotIn("verification_method", self.job())
        self.assertNotIn("verification_config_sha256", self.job())
        self.assertTrue(self.store.verified_video(self.job()))

    def test_final_publication_cas_cannot_change_expected_verification_identity(self):
        self.set_job(status="processing", provider_id="already-paid-render", lease_at=video.iso(NOW),
                     finalization_attempts=1, verification_method=speech.PLANNED_AUDIO_VERIFICATION_METHOD,
                     verification_config_sha256=speech.PLANNED_AUDIO_VERIFICATION_CONFIG_SHA256)
        expected = self.job()
        original_write = self.objects.write
        def raced_write(key, body, version, content_type="application/json"):
            result = original_write(key, body, version, content_type)
            if key.startswith("public/"):
                self.set_job(verification_config_sha256="0" * 64)
            return result
        with patch.object(self.objects, "write", side_effect=raced_write):
            result = self.store.finish(self.context["followup_key"], "already-paid-render",
                {**MEDIA, "speech_proof": planned_speech_proof()}, expected_job=expected)
        self.assertFalse(result)
        self.assertEqual(self.job()["status"], "processing")
        self.assertEqual(self.job()["verification_config_sha256"], "0" * 64)
        self.assertIsNone(self.store.get_public_video(self.job()["id"]))

    def test_planned_processing_text_freeze_preserves_expected_method(self):
        binding = {"verification_method": speech.PLANNED_AUDIO_VERIFICATION_METHOD,
                   "verification_config_sha256": speech.PLANNED_AUDIO_VERIFICATION_CONFIG_SHA256}
        self.set_job(status="submitted", provider_id="already-paid-render")
        self.provider.status = {"status": "completed", "provider_id": "already-paid-render", **binding}
        def prepare(*args):
            self.service.select(self.context, FALLBACK)
            return {**MEDIA, "speech_proof": planned_speech_proof()}
        self.provider.prepare_media = Mock(side_effect=prepare)
        self.worker().tick()
        self.assertEqual(self.job()["status"], "frozen_text")
        for field, value in binding.items():
            self.assertEqual(self.job()[field], value)
        self.assertEqual(self.objects.list("assets/"), [])

    def test_legacy_publication_cas_rejects_new_partial_null_method_field(self):
        self.set_job(status="submitted", provider_id="already-paid-render")
        original_write = self.objects.write
        def raced_write(key, body, version, content_type="application/json"):
            result = original_write(key, body, version, content_type)
            if key.startswith("public/"):
                self.set_job(verification_method=None)
            return result
        with patch.object(self.objects, "write", side_effect=raced_write):
            result = self.store.finish(self.context["followup_key"], "already-paid-render", MEDIA)
        self.assertFalse(result)
        self.assertEqual(self.job()["status"], "submitted")
        self.assertIsNone(self.store.get_public_video(self.job()["id"]))


if __name__ == "__main__":
    unittest.main()
