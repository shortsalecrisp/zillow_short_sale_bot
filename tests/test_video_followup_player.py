import base64
import copy
import hashlib
import io
import re
import sys
from html.parser import HTMLParser
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from PIL import Image

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import video_followup_player as player
from video_followup_player import create_video_followup_router


OPAQUE_ID = "0123456789abcdef0123456789abcdef"
BASE = f"/v/{OPAQUE_ID}"
ASSET_PATHS = {"video": "/media.mp4", "poster": "/poster.jpg", "captions": "/captions.vtt"}
OPENING_SUFFIX = "/poster.jpg?preview=opening-v1"
OPENING_PATH = ROOT / "assets/video-preview/yoni-opening-v1.jpg"
OPENING_SHA256 = "38807e8294d09a059bccbbbfe547910381c9b60f78c57a53f8129ebe47cd04c5"
PATHS = ["", *ASSET_PATHS.values(), OPENING_SUFFIX]
VIDEO = b"0123456789"


class FakeStore:
    def __init__(self):
        self.video = {
            "first_name": "Jim",
            "duration": 10,
            "status": "ready",
            "phone": "+15555550123",
            "email": "private-agent@example.test",
            "address": "123 Private Street",
        }
        self.assets = {
            "video": {"body": VIDEO, "content_type": "video/mp4", "etag": "video-v1"},
            "poster": {"body": b"jpeg-data", "content_type": "image/jpeg", "etag": "poster-v1"},
            "captions": {"body": b"WEBVTT\n\n00:00.000 --> 00:01.000\nHey Jim.\n", "content_type": "text/vtt", "etag": "captions-v1"},
        }
        self.video_calls = []
        self.asset_calls = []
        self.video_error = False
        self.asset_error = False

    def get_public_video(self, opaque_id):
        self.video_calls.append(opaque_id)
        if self.video_error:
            raise RuntimeError("secret database connection string")
        return self.video

    def get_public_asset(self, opaque_id, kind):
        self.asset_calls.append((opaque_id, kind))
        if self.asset_error:
            raise RuntimeError("secret storage key")
        return self.assets.get(kind)


def make_client(store_factory):
    app = FastAPI()
    app.include_router(create_video_followup_router(store_factory))
    return TestClient(app)


@pytest.fixture
def setup():
    store = FakeStore()
    with make_client(lambda: store) as client:
        yield client, store


def assert_security_headers(response):
    assert response.headers["cache-control"] == "no-store"
    assert response.headers["referrer-policy"] == "no-referrer"
    assert response.headers["x-content-type-options"] == "nosniff"
    assert response.headers["x-frame-options"] == "DENY"
    assert response.headers["x-robots-tag"] == "noindex, nofollow, noarchive"
    assert response.headers["cross-origin-resource-policy"] == "same-origin"
    assert "autoplay=()" in response.headers["permissions-policy"]
    csp = response.headers["content-security-policy"]
    for directive in ("default-src 'none'", "script-src 'none'", "frame-ancestors 'none'", "base-uri 'none'", "form-action 'none'", "media-src 'self'"):
        assert directive in csp
    assert "unsafe-inline" not in csp


def test_factory_is_lazy_and_invalid_id_does_not_open_store():
    calls = []

    def factory():
        calls.append(True)
        return FakeStore()

    with make_client(factory) as client:
        assert calls == []
        assert client.get("/v/not-an-id").status_code == 404
        assert calls == []
        assert client.get(BASE).status_code == 200
        assert calls == [True]


def test_player_is_local_accessible_and_uses_no_private_metadata(setup):
    client, _ = setup
    response = client.get(BASE + "?phone=query-secret&name=QueryName")
    assert response.status_code == 200
    assert response.headers["content-type"] == "text/html; charset=utf-8"
    content = response.text
    assert "A quick hello, Jim." in content
    assert "Crisp Short Sales" in content
    assert "Yoni Kutler" in content
    assert "Reply to my original text message." in content
    assert "Personalized with AI using my recorded likeness and voice." not in content
    assert "ai-disclosure" not in content
    assert ".disclosure" not in content
    assert 'aria-label="A quick hello from Yoni Kutler at Crisp Short Sales"' in content
    assert 'preload="none"' in content
    assert "<video controls playsinline" in content
    assert 'kind="captions"' in content and 'srclang="en"' in content and 'label="English" default' in content
    assert f'poster="{BASE}{OPENING_SUFFIX}"' in content
    assert f'src="{BASE}/media.mp4"' in content
    assert f'src="{BASE}/captions.vtt"' in content
    for forbidden in ("autoplay", "<script", "<form", "http://", "private-agent", "+15555550123", "123 Private Street", "query-secret", "QueryName"):
        assert forbidden not in content
    assert_security_headers(response)
    style = re.search(r"<style>(.*?)</style>", content, re.S).group(1)
    digest = base64.b64encode(hashlib.sha256(style.encode()).digest()).decode()
    assert f"'sha256-{digest}'" in response.headers["content-security-policy"]


class MetadataParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.tags = {}

    def handle_starttag(self, tag, attrs):
        values = dict(attrs)
        if tag == "meta" and "content" in values:
            self.tags[values.get("property", values.get("name"))] = values["content"]


@pytest.mark.parametrize("user_agent", ["Mozilla/5.0", "facebookexternalhit/1.1", "Twitterbot/1.0"])
def test_preview_has_public_face_poster_without_exposing_recipient_or_trusting_host(setup, user_agent):
    client, _ = setup
    response = client.get(BASE + "?phone=query-secret&name=QueryName", headers={
        "Host": "untrusted.example", "X-Forwarded-Host": "also-untrusted.example",
        "User-Agent": user_agent,
    })
    parser = MetadataParser()
    parser.feed(response.text)
    metadata = parser.tags
    public_url = "https://go.crispshortsales.com" + BASE
    assert metadata["og:url"] == public_url
    assert metadata["og:image"] == metadata["twitter:image"] == public_url + OPENING_SUFFIX
    assert metadata["og:image:type"] == "image/jpeg"
    assert metadata["og:image:width"] == "960"
    assert metadata["og:image:height"] == "540"
    assert metadata["og:title"] == metadata["twitter:title"] == "A quick hello from Yoni"
    assert metadata["og:site_name"] == "Crisp Short Sales"
    assert metadata["twitter:card"] == "summary_large_image"
    for private in ("Jim", "QueryName", "query-secret", "untrusted", "15555550123", "Private Street"):
        assert private not in repr(metadata)
    assert "og:video" not in metadata  # Do not auto-download/play video in previews.
    poster = client.get(BASE + OPENING_SUFFIX, headers={"User-Agent": user_agent})
    assert poster.status_code == 200
    assert poster.headers["content-type"] == "image/jpeg"
    assert hashlib.sha256(poster.content).hexdigest() == OPENING_SHA256


def test_first_name_is_escaped_and_not_used_in_resource_urls(setup):
    client, store = setup
    store.video["first_name"] = '<img src=x onerror="alert(1)"> & Jim'
    response = client.get(BASE)
    assert '<img src=x' not in response.text
    assert '&lt;img src=x onerror=&quot;alert(1)&quot;&gt; &amp; Jim' in response.text
    resources = re.findall(r'(?:src|poster|href)="([^"]+)"', response.text)
    assert resources
    assert all(url == BASE + OPENING_SUFFIX or url.startswith(BASE + "/") and "?" not in url for url in resources)


@pytest.mark.parametrize("first_name", [None, "", "  ", 123])
def test_missing_name_has_natural_generic_heading(setup, first_name):
    client, store = setup
    store.video["first_name"] = first_name
    assert "<h1>A quick hello.</h1>" in client.get(BASE).text


@pytest.mark.parametrize("suffix", PATHS)
@pytest.mark.parametrize("status", ["ready", "frozen_video"])
def test_ready_statuses_and_assets(setup, suffix, status):
    client, store = setup
    store.video["status"] = status
    response = client.get(BASE + suffix)
    assert response.status_code == 200
    assert_security_headers(response)
    assert store.video_calls == [OPAQUE_ID]
    if suffix == OPENING_SUFFIX:
        assert store.asset_calls == []
        assert hashlib.sha256(response.content).hexdigest() == OPENING_SHA256
        assert response.headers["content-type"] == "image/jpeg"
    elif suffix:
        kind = next(kind for kind, path in ASSET_PATHS.items() if path == suffix)
        assert store.asset_calls == [(OPAQUE_ID, kind)]
        assert response.content == store.assets[kind]["body"]
        assert response.headers["content-type"].startswith(store.assets[kind]["content_type"])


@pytest.mark.parametrize("suffix", PATHS)
@pytest.mark.parametrize("status", ["pending", "rendering", "cancelled", "frozen_text", "failed", None])
def test_unready_or_cancelled_video_never_exposes_any_asset(setup, suffix, status):
    client, store = setup
    store.video["status"] = status
    response = client.get(BASE + suffix)
    assert response.status_code == 404
    assert response.text == "Video not available."
    assert store.asset_calls == []
    assert_security_headers(response)


@pytest.mark.parametrize("suffix", PATHS)
@pytest.mark.parametrize("bad_id", ["0" * 31, "0" * 33, "g" * 32, "A" * 32, "123@example.test"])
def test_invalid_ids_are_404_without_store_reads(setup, suffix, bad_id):
    client, store = setup
    response = client.get(f"/v/{bad_id}{suffix}")
    assert response.status_code == 404
    assert store.video_calls == [] and store.asset_calls == []
    assert_security_headers(response)


@pytest.mark.parametrize("suffix", PATHS)
def test_missing_video_is_404(setup, suffix):
    client, store = setup
    store.video = None
    response = client.get(BASE + suffix)
    assert response.status_code == 404
    assert store.asset_calls == []


@pytest.mark.parametrize("kind", ASSET_PATHS)
def test_missing_asset_is_404(setup, kind):
    client, store = setup
    del store.assets[kind]
    response = client.get(BASE + ASSET_PATHS[kind])
    assert response.status_code == 404
    assert_security_headers(response)


@pytest.mark.parametrize("suffix", PATHS)
def test_head_matches_full_get_headers_with_no_body(setup, suffix):
    client, _ = setup
    get = client.get(BASE + suffix)
    head = client.head(BASE + suffix, headers={"Range": "bytes=1-2"})
    assert head.status_code == get.status_code
    assert head.headers == get.headers
    assert head.content == b""
    assert int(head.headers["content-length"]) == len(get.content)


@pytest.mark.parametrize("range_header, expected, content_range", [
    ("bytes=0-3", b"0123", "bytes 0-3/10"),
    ("bytes=4-", b"456789", "bytes 4-9/10"),
    ("bytes=-3", b"789", "bytes 7-9/10"),
    ("bytes=-100", VIDEO, "bytes 0-9/10"),
    ("bytes=8-100", b"89", "bytes 8-9/10"),
    ("bytes=0-0", b"0", "bytes 0-0/10"),
    ("BYTES=1-2", b"12", "bytes 1-2/10"),
])
def test_single_video_ranges(setup, range_header, expected, content_range):
    client, _ = setup
    response = client.get(BASE + "/media.mp4", headers={"Range": range_header})
    assert response.status_code == 206
    assert response.content == expected
    assert response.headers["content-range"] == content_range
    assert response.headers["content-length"] == str(len(expected))
    assert response.headers["accept-ranges"] == "bytes"
    assert response.headers["etag"] == '"video-v1"'
    assert_security_headers(response)


@pytest.mark.parametrize("range_header", [
    "bytes=10-", "bytes=20-30", "bytes=5-4", "bytes=-0", "bytes=-", "bytes=", "bytes",
    "bytes=a-b", "bytes=0-1,3-4", "bytes=--1", "bytes=0-" + "9" * 5000,
])
def test_invalid_or_unsatisfiable_byte_ranges_are_safe_416(setup, range_header):
    client, _ = setup
    response = client.get(BASE + "/media.mp4", headers={"Range": range_header})
    assert response.status_code == 416
    assert response.headers["content-range"] == "bytes */10"
    assert_security_headers(response)


def test_unknown_range_unit_is_ignored(setup):
    client, _ = setup
    response = client.get(BASE + "/media.mp4", headers={"Range": "items=1-2"})
    assert response.status_code == 200 and response.content == VIDEO
    assert "content-range" not in response.headers


@pytest.mark.parametrize("if_range, status", [('"video-v1"', 206), ('"other"', 200), ('W/"video-v1"', 200), ("Tue, 13 Oct 2026 00:00:00 GMT", 200)])
def test_if_range_requires_matching_strong_etag(setup, if_range, status):
    client, _ = setup
    response = client.get(BASE + "/media.mp4", headers={"Range": "bytes=0-2", "If-Range": if_range})
    assert response.status_code == status
    assert response.content == (b"012" if status == 206 else VIDEO)


def test_weak_stored_etag_does_not_match_if_range(setup):
    client, store = setup
    store.assets["video"]["etag"] = 'W/"video-v1"'
    response = client.get(BASE + "/media.mp4", headers={"Range": "bytes=0-2", "If-Range": 'W/"video-v1"'})
    assert response.status_code == 200 and response.content == VIDEO


@pytest.mark.parametrize("kind", ["poster", "captions"])
def test_non_video_assets_ignore_range(setup, kind):
    client, store = setup
    response = client.get(BASE + ASSET_PATHS[kind], headers={"Range": "bytes=0-1"})
    assert response.status_code == 200
    assert response.content == store.assets[kind]["body"]
    assert "content-range" not in response.headers


def test_empty_video_range_is_416(setup):
    client, store = setup
    store.assets["video"]["body"] = b""
    assert client.get(BASE + "/media.mp4").content == b""
    response = client.get(BASE + "/media.mp4", headers={"Range": "bytes=0-"})
    assert response.status_code == 416
    assert response.headers["content-range"] == "bytes */0"


@pytest.mark.parametrize("suffix", PATHS)
@pytest.mark.parametrize("method", ["get", "head"])
def test_store_outage_returns_generic_503(setup, suffix, method):
    client, store = setup
    store.video_error = True
    response = getattr(client, method)(BASE + suffix)
    assert response.status_code == 503
    assert "secret" not in response.text
    assert response.headers["retry-after"] == "60"
    assert_security_headers(response)
    if method == "head":
        assert response.content == b""


def test_factory_outage_returns_generic_503():
    def broken_factory():
        raise RuntimeError("secret credentials")

    with make_client(broken_factory) as client:
        response = client.get(BASE)
    assert response.status_code == 503
    assert "secret" not in response.text
    assert_security_headers(response)


@pytest.mark.parametrize("suffix", ASSET_PATHS.values())
def test_asset_outage_returns_generic_503(setup, suffix):
    client, store = setup
    store.asset_error = True
    response = client.get(BASE + suffix)
    assert response.status_code == 503
    assert "secret" not in response.text
    assert_security_headers(response)


@pytest.mark.parametrize("bad_asset", [
    {"body": "not bytes", "content_type": "video/mp4"},
    {"body": VIDEO, "content_type": "text/html"},
    {"body": VIDEO, "content_type": "video/mp4\r\nSet-Cookie: secret"},
    {},
])
def test_malformed_asset_is_not_served(setup, bad_asset):
    client, store = setup
    store.assets["video"] = bad_asset
    response = client.get(BASE + "/media.mp4")
    assert response.status_code == 503
    assert "set-cookie" not in response.headers


def test_etag_cannot_inject_headers(setup):
    client, store = setup
    store.assets["video"]["etag"] = 'bad\r\nSet-Cookie: secret'
    response = client.get(BASE + "/media.mp4")
    assert response.status_code == 200
    assert response.headers["etag"] == f'"{hashlib.sha256(VIDEO).hexdigest()}"'
    assert "set-cookie" not in response.headers


def test_cancelled_after_page_read_is_not_served_from_application_cache(setup):
    client, store = setup
    assert client.get(BASE).status_code == 200
    assert client.get(BASE + "/media.mp4").status_code == 200
    store.video["status"] = "cancelled"
    response = client.get(BASE + "/media.mp4")
    assert response.status_code == 404
    assert_security_headers(response)


def test_opening_preview_pins_approved_jpeg_bytes_and_dimensions(setup):
    client, store = setup
    assert player._OPENING_POSTER_PATH == OPENING_PATH
    assert player._OPENING_POSTER_SHA256 == OPENING_SHA256
    assert player._OPENING_POSTER_SUFFIX == OPENING_SUFFIX
    before = OPENING_PATH.read_bytes()
    assert hashlib.sha256(before).hexdigest() == OPENING_SHA256
    with Image.open(io.BytesIO(before)) as image:
        assert image.format == "JPEG"
        assert image.size == (960, 540)
        assert image.mode == "RGB"
        image.verify()
    assets_before = copy.deepcopy(store.assets)
    metadata_before = copy.deepcopy(store.video)
    response = client.get(BASE + OPENING_SUFFIX)
    assert response.status_code == 200
    assert response.content == before
    assert store.asset_calls == []
    assert store.assets == assets_before and store.video == metadata_before
    assert OPENING_PATH.read_bytes() == before
    assert_security_headers(response)


def test_opening_preview_ignores_recipient_query_host_and_user_agent(setup):
    client, store = setup
    responses = []
    for user_agent in ("Mozilla/5.0", "facebookexternalhit/1.1", "Twitterbot/1.0"):
        store.video["first_name"] = "PrivateRecipient"
        response = client.get(BASE + OPENING_SUFFIX + "&phone=query-secret&name=QueryName&image=https://untrusted.example/face.jpg", headers={
            "Host": "untrusted.example", "X-Forwarded-Host": "also-untrusted.example", "User-Agent": user_agent,
        })
        assert response.status_code == 200
        assert hashlib.sha256(response.content).hexdigest() == OPENING_SHA256
        for private in (b"PrivateRecipient", b"QueryName", b"query-secret", b"untrusted.example", b"15555550123", b"Private Street"):
            assert private not in response.content
            assert private.decode() not in repr(dict(response.headers))
        responses.append(response)
    assert all(response.content == responses[0].content and response.headers == responses[0].headers for response in responses)
    assert store.asset_calls == []


@pytest.mark.parametrize("fault", ["missing", "corrupt", "directory"])
@pytest.mark.parametrize("method", ["get", "head"])
def test_opening_preview_missing_or_corrupt_fails_closed_without_legacy_fallback(setup, monkeypatch, tmp_path, fault, method):
    client, store = setup
    image_path = tmp_path / "opening.jpg"
    if fault == "corrupt":
        image_path.write_bytes(b"not-the-approved-image")
    elif fault == "directory":
        image_path.mkdir()
    monkeypatch.setattr(player, "_OPENING_POSTER_PATH", image_path)
    before = image_path.read_bytes() if fault == "corrupt" else None
    response = getattr(client, method)(BASE + OPENING_SUFFIX)
    assert response.status_code == 503
    assert response.headers["retry-after"] == "60"
    assert store.asset_calls == []
    assert "opening.jpg" not in response.text and "not-the-approved-image" not in response.text
    if method == "head":
        assert response.content == b""
    else:
        assert response.text == "Video temporarily unavailable. Please try again later."
    assert_security_headers(response)
    assert image_path.exists() == (fault != "missing")
    if before is not None:
        assert image_path.read_bytes() == before
    # A broken new preview must not alter the legacy assets or media path.
    for kind, suffix in ASSET_PATHS.items():
        old = client.get(BASE + suffix)
        assert old.status_code == 200 and old.content == store.assets[kind]["body"]


def test_opening_preview_is_independent_of_legacy_asset_outage(setup):
    client, store = setup
    store.asset_error = True
    response = client.get(BASE + OPENING_SUFFIX, headers={"Range": "bytes=0-1"})
    assert response.status_code == 200
    assert hashlib.sha256(response.content).hexdigest() == OPENING_SHA256
    assert "content-range" not in response.headers
    assert store.asset_calls == []
    assert client.get(BASE + "/poster.jpg").status_code == 503


def test_opening_preview_rechecks_publication_status_after_success(setup):
    client, store = setup
    assert client.get(BASE + OPENING_SUFFIX).status_code == 200
    store.video["status"] = "cancelled"
    for method in ("get", "head"):
        response = getattr(client, method)(BASE + OPENING_SUFFIX)
        assert response.status_code == 404
        assert_security_headers(response)
    assert store.asset_calls == []


@pytest.mark.parametrize("query", [
    "", "?preview=", "?preview=OPENING-V1", "?preview=opening-v1-extra",
    "?preview=opening-v2", "?Preview=opening-v1", "?phone=query-secret&name=QueryName",
])
@pytest.mark.parametrize("method", ["get", "head"])
def test_only_exact_opening_preview_selector_replaces_legacy_poster(setup, query, method):
    client, store = setup
    response = getattr(client, method)(BASE + "/poster.jpg" + query)
    assert response.status_code == 200
    assert response.content == (b"jpeg-data" if method == "get" else b"")
    assert response.headers["content-length"] == str(len(b"jpeg-data"))
    assert response.headers["etag"] == '"poster-v1"'
    assert store.asset_calls == [(OPAQUE_ID, "poster")]
    assert_security_headers(response)


def test_opening_preview_uses_existing_poster_route_not_new_gateway_path(setup):
    client, store = setup
    response = client.get(BASE + "/preview-opening-v1.jpg")
    assert response.status_code == 404
    assert store.video_calls == [] and store.asset_calls == []
