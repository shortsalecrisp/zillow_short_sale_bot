#!/usr/bin/env python3
"""
bot_min.py – minimal but functional crawler / enrichment helper
for the Render deployment that:

• Builds reasonable follow-up URLs from a seed listing link
• Falls back to a read-only proxy (“backup fetch”) when direct fetch fails
• Uses Google Programmable Search to discover deeper contact pages
• Extracts phone numbers and emails with loose but effective regexes
• Exposes a process_rows() helper imported by webhook_server.py
• May be executed as a CLI utility (`python bot_min.py <url> [-d N]`)
"""

from __future__ import annotations

###############################################################################
# Imports
###############################################################################

import argparse
import json
import os
import re
import sys
import time
import urllib.parse
from collections import OrderedDict
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import requests
from bs4 import BeautifulSoup

###############################################################################
# Constants & Regexes
###############################################################################

USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) "
    "AppleWebKit/537.36 "
    "(KHTML, like Gecko) "
    "Chrome/123.0 Safari/537.36"
)
REQ_HEADERS = {"User-Agent": USER_AGENT}

GOOGLE_CX = os.getenv("GOOGLE_CX", "")
GOOGLE_KEY = os.getenv("GOOGLE_KEY", "")

EMAIL_RE = re.compile(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+")
PHONE_RE = re.compile(
    r"(?<!\d)"
    r"(?:\+?1[\s./-]*)?"
    r"(?:\(?\d{3}\)?[\s./-]*)?"
    r"\d{3}[\s./-]*\d{4}"
    r"(?!\d)"
)

###############################################################################
# Low-level helper: URL handling
###############################################################################


def normalize_url(raw: str) -> str:
    """Ensure scheme, collapse duplicate prefixes, & quote unsafe parts."""
    url = raw.strip()
    if not url:
        return url
    url = re.sub(r"^(https?://)+", "https://", url, flags=re.I)
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    return urllib.parse.quote(url, safe=":/?=&%#.")


def build_candidate_urls(seed: str) -> List[str]:
    """
    Very light weight “URL builder” that tries obvious sibling paths that
    frequently host contact info:

        /about, /contact, /bio, /profile, /team, /meet-the-team
    """
    base = normalize_url(seed)
    parts = urllib.parse.urlparse(base)
    root = f"{parts.scheme}://{parts.netloc}"
    candidates = [
        base,
        root + "/about",
        root + "/contact",
        root + "/profile",
        root + "/bio",
        root + "/team",
        root + "/meet-the-team",
    ]
    return list(OrderedDict.fromkeys(candidates))  # preserve order, dedupe


###############################################################################
# Fetching
###############################################################################


def fetch(url: str, timeout: float = 15.0) -> Optional[str]:
    url = normalize_url(url)
    try:
        r = requests.get(url, headers=REQ_HEADERS, timeout=timeout)
        if r.ok and r.text:
            return r.text
    except requests.RequestException:
        pass
    return None


def backup_fetch(url: str, timeout: float = 15.0) -> Optional[str]:
    """
    Fallback to jina.ai raw mirror which often escapes bot protections.
    """
    url = normalize_url(url)
    p = urllib.parse.urlparse(url)
    raw = "http://" + p.netloc + p.path
    if p.query:
        raw += "?" + p.query
    mirror = f"https://r.jina.ai/http://{raw}"
    try:
        r = requests.get(mirror, headers=REQ_HEADERS, timeout=timeout)
        if r.ok and r.text:
            return r.text
    except requests.RequestException:
        pass
    return None


###############################################################################
# Google programmable search
###############################################################################


def google_search(query: str, num: int = 6) -> Iterable[str]:
    if not (GOOGLE_CX and GOOGLE_KEY):
        return []
    params = {"key": GOOGLE_KEY, "cx": GOOGLE_CX, "q": query, "num": num}
    try:
        resp = requests.get(
            "https://www.googleapis.com/customsearch/v1",
            params=params,
            headers=REQ_HEADERS,
            timeout=12,
        )
        data = resp.json()
        for item in data.get("items", []):
            link = item.get("link")
            if link:
                yield link
    except Exception:
        pass


###############################################################################
# Content extraction
###############################################################################


def extract_contacts(html: str) -> Tuple[List[str], List[str]]:
    phones: "OrderedDict[str, None]" = OrderedDict()
    emails: "OrderedDict[str, None]" = OrderedDict()

    # phone numbers
    for m in PHONE_RE.finditer(html):
        digits = re.sub(r"[^\d]", "", m.group())
        if len(digits) == 10:
            fmt = f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
            phones[fmt] = None

    # emails
    for m in EMAIL_RE.finditer(html):
        emails[m.group()] = None

    return list(phones.keys()), list(emails.keys())


###############################################################################
# Scraper class
###############################################################################


class Scraper:
    def __init__(self, max_depth: int = 2):
        self.max_depth = max_depth
        self.visited: set[str] = set()

    # --------------------------------------------------------------------- #
    # internal helpers
    # --------------------------------------------------------------------- #

    def _get_source(self, url: str) -> Optional[str]:
        if url in self.visited:
            return None
        self.visited.add(url)
        html = fetch(url) or backup_fetch(url)
        return html

    def _search_contacts_recursive(
        self, url: str, depth: int
    ) -> Tuple[List[str], List[str]]:
        if depth > self.max_depth:
            return [], []

        html = self._get_source(url)
        if not html:
            return [], []

        phones, emails = extract_contacts(html)
        if phones or emails:
            return phones, emails

        # google based expansion
        u = urllib.parse.urlparse(url)
        tokens = filter(None, re.split(r"[/-]", u.path))
        query = " ".join(list(tokens)[:4] + ["contact"])
        for link in google_search(query):
            p2, e2 = self._search_contacts_recursive(link, depth + 1)
            if p2 or e2:
                return p2, e2
        return [], []

    # --------------------------------------------------------------------- #
    # public API
    # --------------------------------------------------------------------- #

    def crawl(self, seed_url: str) -> Dict[str, object]:
        result = {"url": seed_url, "phones": [], "emails": []}
        for candidate in build_candidate_urls(seed_url):
            p, e = self._search_contacts_recursive(candidate, 0)
            result["phones"].extend(p)
            result["emails"].extend(e)
            if p or e:
                break
        # dedupe while preserving order
        result["phones"] = list(OrderedDict.fromkeys(result["phones"]))
        result["emails"] = list(OrderedDict.fromkeys(result["emails"]))
        return result


###############################################################################
# Sheet helper (imported by webhook_server.py)
###############################################################################


def process_rows(rows: Sequence[str], depth: int = 2) -> List[Dict[str, object]]:
    """
    Process a sequence of URL strings, returning contact info dicts.

    Each row is expected to contain a single URL. Blank rows ignored.
    """
    scraper = Scraper(max_depth=depth)
    out = []
    for raw in rows:
        url = raw.strip()
        if not url:
            continue
        out.append(scraper.crawl(url))
    return out


###############################################################################
# CLI
###############################################################################


def _cli_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="bot_min")
    parser.add_argument("url", nargs="?", help="Seed URL (listing / agent page)")
    parser.add_argument("-d", "--depth", type=int, default=2, help="crawl depth")
    return parser.parse_args(argv)


def _run_cli() -> None:
    ns = _cli_args()
    if not ns.url:
        print("bot_min: no URL provided; exiting", file=sys.stderr)
        sys.exit(0)
    data = Scraper(max_depth=ns.depth).crawl(ns.url)
    print(json.dumps(data, indent=2, ensure_ascii=False))


###############################################################################
# Module self-test
###############################################################################


def _self_test() -> None:
    url = "https://www.example.com"
    s = Scraper(max_depth=0)
    res = s.crawl(url)
    assert res["url"] == url
    assert "phones" in res and "emails" in res
    assert isinstance(process_rows([url]), list)


###############################################################################
# Entry point
###############################################################################

if __name__ == "__main__":
    if os.getenv("DEBUG"):
        _self_test()
    _run_cli()

