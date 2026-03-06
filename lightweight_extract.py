from __future__ import annotations

import html
import re
from typing import Any, Dict, List


_TAG_RE = re.compile(r"<[^>]+>")
_SCRIPT_RE = re.compile(r"<script\b[^>]*>.*?</script>", re.I | re.S)
_STYLE_RE = re.compile(r"<style\b[^>]*>.*?</style>", re.I | re.S)
_WS_RE = re.compile(r"\s+")
_MAILTO_RE = re.compile(r"href=[\"'](mailto:[^\"']+)[\"']", re.I)
_TEL_RE = re.compile(r"href=[\"'](tel:[^\"']+)[\"']", re.I)
_JS_HINTS = (
    "enable javascript",
    "please enable javascript",
    "javascript required",
    "requires javascript",
    "data-reactroot",
    "__next_data__",
    "__next_data",
    "__nuxt",
)


def extract_lightweight_snapshot(html_text: str, *, final_url: str = "") -> Dict[str, Any]:
    body = html_text or ""
    if not body.strip():
        return {"html": "", "visible_text": "", "mailto_links": [], "tel_links": [], "final_url": final_url}

    mailto_links: List[str] = [m.strip() for m in _MAILTO_RE.findall(body)]
    tel_links: List[str] = [t.strip() for t in _TEL_RE.findall(body)]

    stripped = _SCRIPT_RE.sub(" ", body)
    stripped = _STYLE_RE.sub(" ", stripped)
    text = _TAG_RE.sub(" ", stripped)
    text = html.unescape(_WS_RE.sub(" ", text)).strip()

    return {
        "html": body,
        "visible_text": text,
        "mailto_links": mailto_links,
        "tel_links": tel_links,
        "final_url": final_url,
        "js_required_hint": any(hint in body.lower() for hint in _JS_HINTS),
    }
