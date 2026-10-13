import base64
import hashlib
import re
import sys
from html.parser import HTMLParser
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from video_followup_player import create_video_followup_router


OPAQUE_ID = "0123456789abcdef0123456789abcdef"
BASE = f"/v/{OPAQUE_ID}"
ASSET_PATHS = {"video": "/media.mp4", "poster": "/poster.jpg", "captions": "/captions.vtt"}
PATHS = ["", *ASSET_PATHS.values()]
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


def test_player_is_local_accessible_disclosed_and_uses_no_private_metadata(setup):
    client, _ = setup
    response = client.get(BASE + "?phone=query-secret&name=QueryName")
    assert response.status_code == 200
    assert response.headers["content-type"] == "text/html; charset=utf-8"
    content = response.text
    assert "A quick hello, Jim." in content
    assert "Crisp Short Sales" in content
    assert "Yoni Kutler" in content
    assert "Reply to my original text message." in content
    assert "Personalized with AI using my recorded likeness and voice." in content
    assert 'preload="none"' in content
    assert "<video controls playsinline" in content
    assert 'kind="captions"' in content and 'srclang="en"' in content and 'label="English" default' in content
    assert f'poster="{BASE}/poster.jpg"' in content
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
    assert metadata["og:image"] == metadata["twitter:image"] == public_url + "/poster.jpg"
    assert metadata["og:image:type"] == "image/jpeg"
    assert metadata["og:image:width"] == "960"
    assert metadata["og:image:height"] == "540"
    assert metadata["og:title"] == metadata["twitter:title"] == "A quick hello from Yoni"
    assert metadata["og:site_name"] == "Crisp Short Sales"
    assert metadata["twitter:card"] == "summary_large_image"
    for private in ("Jim", "QueryName", "query-secret", "untrusted", "15555550123", "Private Street"):
        assert private not in repr(metadata)
    assert "og:video" not in metadata  # Do not auto-download/play video in previews.
    poster = client.get(BASE + "/poster.jpg", headers={"User-Agent": user_agent})
    assert poster.status_code == 200
    assert poster.headers["content-type"] == "image/jpeg"
    assert poster.content == b"jpeg-data"


def test_first_name_is_escaped_and_not_used_in_resource_urls(setup):
    client, store = setup
    store.video["first_name"] = '<img src=x onerror="alert(1)"> & Jim'
    response = client.get(BASE)
    assert '<img src=x' not in response.text
    assert '&lt;img src=x onerror=&quot;alert(1)&quot;&gt; &amp; Jim' in response.text
    resources = re.findall(r'(?:src|poster|href)="([^"]+)"', response.text)
    assert resources
    assert all(url.startswith(BASE + "/") and "?" not in url for url in resources)


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
    if suffix:
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
