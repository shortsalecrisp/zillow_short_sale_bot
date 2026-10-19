"""Adversarial coverage for the exact assets recipients will open."""

import hashlib
import importlib.util
import json
from pathlib import Path
from unittest.mock import patch

import pytest


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "video_readiness_test_helpers", ROOT / "tests" / "test_video_followup.py"
)
helpers = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(helpers)
video = helpers.video

CAPTIONS = (
    b"WEBVTT\n\n00:00:00.000 --> 00:00:01.500\nHey Jim,\n\n"
    b"00:00:01.500 --> 00:00:08.000\nIt's Yoni Kutler with Crisp Short Sales.\n"
)


@pytest.fixture
def ready_video():
    with patch("socket.socket.connect", side_effect=AssertionError("No remote tests")), \
            patch.object(video, "utcnow", return_value=helpers.NOW):
        objects = helpers.MemoryObjects()
        store = video.Store(objects)
        context = video.snapshot(
            row=12, stable_id="verified-listing-12", phone=helpers.PHONE,
            first_name="Jim", initial_sent_at="2026-10-13T10:00:00Z",
        )
        context.update(video.initial_receipt_binding(context, helpers.initial_receipt(context)))
        config = video.Config({
            "VIDEO_FOLLOWUP_ENABLED": "true",
            "VIDEO_FOLLOWUP_GENERATION_ENABLED": "true",
            "VIDEO_FOLLOWUP_TEST_PHONES": helpers.PHONE,
            "VIDEO_FOLLOWUP_INITIALS_AFTER": "2026-10-13T00:00:00Z",
            "VIDEO_FOLLOWUP_PUBLIC_ORIGIN": "https://go.crispshortsales.com",
        })
        store.prepare(context)
        store.mutate("jobs/" + context["followup_key"], lambda old: {
            **old, "status": "submitted", "provider_id": "paid-render-already-completed",
        })
        media = {**helpers.MEDIA, "vtt_bytes": CAPTIONS}
        assert store.finish(context["followup_key"], "paid-render-already-completed", media)
        job, _ = store.read("jobs/" + context["followup_key"])
        yield objects, store, video.Service(config, store), context, job


def replace(objects, path, content):
    _, generation = objects.read(path)
    objects.write(path, content, generation)


def delete(objects, path):
    _, generation = objects.read(path)
    objects.delete(path, generation)


def assert_text_only(service, context):
    message, metadata = service.select(context, helpers.FALLBACK)
    assert message == helpers.FALLBACK
    assert metadata["video_variant"] == "text"
    assert "https://" not in message
    job, _ = service.store.read("jobs/" + context["followup_key"])
    assert job["status"] == "frozen_text"
    assert job["variant"] == "text"


def test_complete_published_asset_set_selects_one_correct_branded_link(ready_video):
    objects, store, service, context, job = ready_video
    before_assets = objects.list("assets/")
    message, metadata = service.select(context, helpers.FALLBACK)
    assert message.count("https://") == 1
    assert message.endswith("https://go.crispshortsales.com/v/" + job["id"])
    assert message.startswith("Hey Jim, it's Yoni Kutler")
    assert metadata["video_variant"] == "video"
    assert metadata["video_speech_proof"]["media_sha256"] == hashlib.sha256(helpers.MEDIA["media_bytes"]).hexdigest()
    assert store.get_public_video(job["id"])["first_name"] == "Jim"
    assert store.get_public_asset(job["id"], "captions")["body"] == CAPTIONS
    assert objects.list("assets/") == before_assets
    assert service.select(context, helpers.FALLBACK) == (message, metadata)


@pytest.mark.parametrize("target", ["public", "video", "poster", "captions"])
def test_missing_public_pointer_or_required_asset_never_selects_link(ready_video, target):
    objects, store, service, context, job = ready_video
    path = "public/" + job["id"] if target == "public" else f"assets/{job['id']}/{target}"
    delete(objects, path)
    assert_text_only(service, context)


@pytest.mark.parametrize("target", ["video", "poster", "captions"])
def test_empty_required_asset_never_selects_link(ready_video, target):
    objects, store, service, context, job = ready_video
    replace(objects, f"assets/{job['id']}/{target}", b"")
    assert_text_only(service, context)


@pytest.mark.parametrize("target", ["poster", "captions"])
def test_oversized_preview_asset_never_selects_link(ready_video, target):
    objects, store, service, context, job = ready_video
    replace(objects, f"assets/{job['id']}/{target}", b"x" * (video.MAX_MEDIA_BYTES + 1))
    assert_text_only(service, context)


@pytest.mark.parametrize("pointer", [
    {}, {"key": "absent-job"}, {"key": "../jobs/private"},
    {"key": "https://example.invalid/private"},
])
def test_public_pointer_must_resolve_exact_current_job(ready_video, pointer):
    objects, store, service, context, job = ready_video
    replace(objects, "public/" + job["id"], json.dumps(pointer).encode())
    assert_text_only(service, context)


def test_pointer_to_another_valid_name_does_not_publish_this_recipients_link(ready_video):
    objects, store, service, context, job = ready_video
    other_key = "a" * 64
    other = {**job, "followup_key": other_key, "first_name": "Sue", "phone": "2125550101"}
    store.write("jobs/" + other_key, other, 0)
    replace(objects, "public/" + job["id"], json.dumps({"key": other_key}).encode())
    assert_text_only(service, context)


def test_finish_cannot_mark_ready_when_existing_public_pointer_belongs_elsewhere(ready_video):
    objects, store, service, context, job = ready_video
    replace(objects, "public/" + job["id"], json.dumps({"key": "a" * 64}).encode())
    store.mutate("jobs/" + context["followup_key"], lambda old: {
        **old, "status": "submitted", "provider_id": "paid-render-already-completed",
    })
    try:
        finished = store.finish(context["followup_key"], "paid-render-already-completed", {
            **helpers.MEDIA, "vtt_bytes": CAPTIONS,
        })
    except video.SpeechGuardError:
        finished = False
    assert finished is False
    current, _ = store.read("jobs/" + context["followup_key"])
    assert current["status"] != "ready"
    pointer, _ = store.read("public/" + job["id"])
    assert pointer == {"key": "a" * 64}


def test_broken_assets_cannot_rewrite_an_already_frozen_video_choice(ready_video):
    objects, store, service, context, job = ready_video
    first_message, first_metadata = service.select(context, helpers.FALLBACK)
    assert first_metadata["video_variant"] == "video"
    delete(objects, "public/" + job["id"])
    message, metadata = service.select(context, helpers.FALLBACK)
    assert message == helpers.FALLBACK
    assert metadata["followup_key"] == first_metadata["followup_key"]
    assert metadata["request_id"] == first_metadata["request_id"]
    frozen, _ = store.read("jobs/" + context["followup_key"])
    assert frozen["variant"] == "video"
    assert frozen["status"] == "frozen_video"
    assert frozen["message"] == first_message
    assert len(objects.list("jobs/")) == 1
