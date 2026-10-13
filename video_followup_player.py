"""Privacy-conscious, same-origin player for ready personalized follow-up videos."""

from __future__ import annotations

import base64
import hashlib
import html
import re
from collections.abc import Callable, Mapping
from typing import Any, Protocol

from fastapi import APIRouter, Request
from fastapi.responses import Response


class PublicVideoStore(Protocol):
    def get_public_video(self, opaque_id: str) -> Mapping[str, Any] | None:
        ...

    def get_public_asset(self, opaque_id: str, kind: str) -> Mapping[str, Any] | None:
        ...


_OPAQUE_ID = re.compile(r"[0-9a-f]{32}\Z")
_READY_STATUSES = frozenset({"ready", "frozen_video"})
_CONTENT_TYPES = {"video": "video/mp4", "poster": "image/jpeg", "captions": "text/vtt"}
_STYLE = """
:root { color-scheme: light; --ink: #173c32; --paper: #faf7ee; --muted: #52645c; }
* { box-sizing: border-box; }
body { margin: 0; color: var(--ink); background: radial-gradient(ellipse at top left, #e4ecdd, transparent 65%), var(--paper); font-family: 'Trebuchet MS', sans-serif; line-height: 1.5; }
main { width: min(100% - 32px, 760px); margin: 0 auto; padding: 40px 0 48px; }
.brand { margin: 0 0 28px; font-size: 0.85rem; font-weight: bold; letter-spacing: 0.12em; text-transform: uppercase; }
.brand::before { content: ''; display: inline-block; width: 22px; height: 3px; margin: 0 10px 4px 0; background: #a67c37; }
h1 { font-family: Georgia, serif; font-size: clamp(2rem, 7vw, 3.2rem); font-weight: normal; line-height: 1.15; margin: 0 0 12px; overflow-wrap: anywhere; }
.intro { margin: 0 0 22px; }
video { display: block; width: 100%; aspect-ratio: 16 / 9; object-fit: contain; background: #14231e; border-radius: 12px; box-shadow: 0 12px 32px #173c321c; }
video:focus-visible, a:focus-visible { outline: 3px solid #946820; outline-offset: 5px; }
.reply { font-size: 1.1rem; margin: 24px 0 8px; }
.disclosure { max-width: 58ch; color: var(--muted); font-size: 0.85rem; margin: 20px 0 0; }
a { color: inherit; }
@media (max-width: 480px) { main { padding-top: 26px; } .brand { margin-bottom: 24px; } }
"""
_STYLE_HASH = base64.b64encode(hashlib.sha256(_STYLE.encode("utf-8")).digest()).decode("ascii")
_SECURITY_HEADERS = {
    "Cache-Control": "no-store",
    "Content-Security-Policy": (
        "default-src 'none'; base-uri 'none'; form-action 'none'; "
        "frame-ancestors 'none'; object-src 'none'; script-src 'none'; "
        f"style-src 'sha256-{_STYLE_HASH}'; img-src 'self'; media-src 'self'; "
        "connect-src 'none'"
    ),
    "Referrer-Policy": "no-referrer",
    "X-Content-Type-Options": "nosniff",
    "X-Frame-Options": "DENY",
    "X-Robots-Tag": "noindex, nofollow, noarchive",
    "Cross-Origin-Resource-Policy": "same-origin",
    "Permissions-Policy": "autoplay=(), camera=(), microphone=(), geolocation=(), payment=()",
}


def _response(
    request: Request,
    content: bytes,
    *,
    status_code: int = 200,
    media_type: str = "text/plain",
    headers: Mapping[str, str] | None = None,
) -> Response:
    response_headers = {**_SECURITY_HEADERS, **(headers or {})}
    response_headers["Content-Length"] = str(len(content))
    return Response(
        content=b"" if request.method == "HEAD" else content,
        status_code=status_code,
        media_type=media_type,
        headers=response_headers,
    )


def _unavailable(request: Request, status_code: int = 404) -> Response:
    if status_code == 503:
        return _response(
            request,
            b"Video temporarily unavailable. Please try again later.",
            status_code=503,
            headers={"Retry-After": "60"},
        )
    return _response(request, b"Video not available.", status_code=404)


def _load_video(
    request: Request, opaque_id: str, store_factory: Callable[[], PublicVideoStore]
) -> tuple[PublicVideoStore, Mapping[str, Any]] | Response:
    if not _OPAQUE_ID.fullmatch(opaque_id):
        return _unavailable(request)
    try:
        store = store_factory()
        video = store.get_public_video(opaque_id)
        if video is not None and not isinstance(video, Mapping):
            return _unavailable(request, 503)
        if video is None or video.get("status") not in _READY_STATUSES:
            return _unavailable(request)
        return store, video
    except Exception:
        return _unavailable(request, 503)


def _etag(value: Any, body: bytes) -> str:
    # Never pass arbitrary store data through as an HTTP header.
    if isinstance(value, str) and len(value) <= 200:
        if re.fullmatch(r'(?:W/)?"[\x21\x23-\x7e]*"', value):
            return value
        if re.fullmatch(r"[A-Za-z0-9._:-]+", value):
            return f'"{value}"'
    return f'"{hashlib.sha256(body).hexdigest()}"'


def _byte_range(value: str, size: int) -> tuple[int, int] | None:
    # Unknown units are ignored. Multipart byte ranges are deliberately unsupported.
    unit, separator, positions = value.strip().partition("=")
    if unit.lower() != "bytes":
        return None
    if not separator or len(positions) > 128:
        raise ValueError("Invalid range")
    match = re.fullmatch(r"([0-9]*)-([0-9]*)", positions)
    if not match or not any(match.groups()) or size == 0:
        raise ValueError("Invalid range")
    first, last = match.groups()
    if not first:
        suffix = int(last)
        if suffix == 0:
            raise ValueError("Invalid range")
        return max(0, size - suffix), size - 1
    start = int(first)
    end = int(last) if last else size - 1
    if start >= size or end < start:
        raise ValueError("Unsatisfiable range")
    return start, min(end, size - 1)


def _player_html(opaque_id: str, video: Mapping[str, Any]) -> bytes:
    first_name = video.get("first_name")
    name = html.escape(first_name.strip()[:80], quote=True) if isinstance(first_name, str) else ""
    heading = f"A quick hello, {name}." if name else "A quick hello."
    base = f"/v/{opaque_id}"
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta name="robots" content="noindex, nofollow, noarchive">
  <title>A quick hello | Crisp Short Sales</title>
  <style>{_STYLE}</style>
</head>
<body>
  <main>
    <p class="brand">Crisp Short Sales</p>
    <h1>{heading}</h1>
    <p class="intro">From Yoni Kutler. Just putting a face to my text.</p>
    <video controls playsinline preload="none" poster="{base}/poster.jpg"
           aria-label="A quick hello from Yoni Kutler at Crisp Short Sales" aria-describedby="ai-disclosure">
      <source src="{base}/media.mp4" type="video/mp4">
      <track kind="captions" src="{base}/captions.vtt" srclang="en" label="English" default>
      Your browser does not support this video. <a href="{base}/media.mp4">Open the video</a>.
    </video>
    <p class="reply">Have a question? Reply to my original text message.</p>
    <p>No need to sign in. Happy to help.</p>
    <p class="disclosure" id="ai-disclosure">Personalized with AI using my recorded likeness and voice.</p>
  </main>
</body>
</html>
""".encode("utf-8")


def create_video_followup_router(store_factory: Callable[[], PublicVideoStore]) -> APIRouter:
    """Build routes without opening the store until an eligible request arrives."""
    router = APIRouter()

    @router.api_route("/v/{opaque_id}", methods=["GET", "HEAD"], include_in_schema=False)
    def player(opaque_id: str, request: Request) -> Response:
        loaded = _load_video(request, opaque_id, store_factory)
        if isinstance(loaded, Response):
            return loaded
        _, video = loaded
        return _response(request, _player_html(opaque_id, video), media_type="text/html")

    def serve_asset(opaque_id: str, request: Request, kind: str) -> Response:
        loaded = _load_video(request, opaque_id, store_factory)
        if isinstance(loaded, Response):
            return loaded
        store, _ = loaded
        try:
            asset = store.get_public_asset(opaque_id, kind)
            if asset is None:
                return _unavailable(request)
            body = asset["body"]
            media_type = _CONTENT_TYPES[kind]
            if not isinstance(body, bytes) or asset["content_type"].split(";", 1)[0].strip().lower() != media_type:
                return _unavailable(request, 503)
            etag = _etag(asset.get("etag"), body)
        except Exception:
            return _unavailable(request, 503)

        headers = {"ETag": etag}
        status_code = 200
        if kind == "video":
            headers["Accept-Ranges"] = "bytes"
            range_header = request.headers.get("range")
            if_range = request.headers.get("if-range")
            # Range applies only to GET; weak validators cannot authorize a partial response.
            if request.method == "GET" and range_header and (
                if_range is None or (not etag.startswith("W/") and if_range == etag)
            ):
                try:
                    selected = _byte_range(range_header, len(body))
                except ValueError:
                    return _response(
                        request,
                        b"Requested range not satisfiable.",
                        status_code=416,
                        headers={**headers, "Content-Range": f"bytes */{len(body)}"},
                    )
                if selected is not None:
                    start, end = selected
                    headers["Content-Range"] = f"bytes {start}-{end}/{len(body)}"
                    body = body[start : end + 1]
                    status_code = 206
        return _response(request, body, status_code=status_code, media_type=media_type, headers=headers)

    @router.api_route("/v/{opaque_id}/media.mp4", methods=["GET", "HEAD"], include_in_schema=False)
    def media(opaque_id: str, request: Request) -> Response:
        return serve_asset(opaque_id, request, "video")

    @router.api_route("/v/{opaque_id}/poster.jpg", methods=["GET", "HEAD"], include_in_schema=False)
    def poster(opaque_id: str, request: Request) -> Response:
        return serve_asset(opaque_id, request, "poster")

    @router.api_route("/v/{opaque_id}/captions.vtt", methods=["GET", "HEAD"], include_in_schema=False)
    def captions(opaque_id: str, request: Request) -> Response:
        return serve_asset(opaque_id, request, "captions")

    return router
