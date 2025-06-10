#!/usr/bin/env python3

import argparse
import json
import os
import re
import sys
import time
import urllib.parse
from collections import OrderedDict
from typing import Iterable, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

# basic headers
USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) "
    "AppleWebKit/537.36 "
    "(KHTML, like Gecko) "
    "Chrome/123.0 Safari/537.36"
)
HEADERS = {"User-Agent": USER_AGENT}

GOOGLE_CX = os.getenv("GOOGLE_CX", "")
GOOGLE_KEY = os.getenv("GOOGLE_KEY", "")

EMAIL_RE = re.compile(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+")
PHONE_RE = re.compile(
    r"(?<!\d)(?:\+?1[\s.-]*)?(?:\(?\d{3}\)?[\s.-]*)\d{3}[\s.-]*\d{4}(?!\d)"
)

###########################################################################
# utility
###########################################################################


def _normalize_url(raw: str) -> str:
    u = raw.strip()
    if not u:
        return u
    u = re.sub(r"^(https?://)+", "https://", u, flags=re.I)
    if not re.match(r"^https?://", u):
        u = "https://" + u
    return urllib.parse.quote(u, safe=":/?=&%#.")


def fetch(url: str, timeout: float = 15.0) -> Optional[str]:
    url = _normalize_url(url)
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout)
        if r.ok and r.text:
            return r.text
    except requests.RequestException:
        pass
    return None


def backup_fetch(url: str, timeout: float = 15.0) -> Optional[str]:
    url = _normalize_url(url)
    p = urllib.parse.urlparse(url)
    stripped = "http://" + p.netloc + p.path
    if p.query:
        stripped += "?" + p.query
    snap = f"https://r.jina.ai/raw/{stripped}"
    try:
        r = requests.get(snap, headers=HEADERS, timeout=timeout)
        if r.ok and r.text and "<html" in r.text[:200].lower():
            return r.text
    except requests.RequestException:
        pass
    return None


def google_expand(query: str, num: int = 4, timeout: float = 10.0) -> Iterable[str]:
    if not (GOOGLE_CX and GOOGLE_KEY):
        return []
    params = {"key": GOOGLE_KEY, "cx": GOOGLE_CX, "q": query, "num": num}
    try:
        r = requests.get(
            "https://www.googleapis.com/customsearch/v1",
            params=params,
            headers=HEADERS,
            timeout=timeout,
        )
        data = r.json()
        for item in data.get("items", []):
            link = item.get("link")
            if link:
                yield link
    except Exception:
        pass


def extract_contacts(html_text: str) -> Tuple[List[str], List[str]]:
    phones = OrderedDict()
    emails = OrderedDict()
    for m in PHONE_RE.finditer(html_text):
        digits = re.sub(r"[^\d]", "", m.group())
        if len(digits) == 10:
            fmt = f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
            phones[fmt] = None
    for m in EMAIL_RE.finditer(html_text):
        emails[m.group()] = None
    return list(phones.keys()), list(emails.keys())


###########################################################################
# scraper
###########################################################################


class Scraper:
    def __init__(self, max_depth: int = 2):
        self.visited = set()
        self.max_depth = max_depth

    def crawl(self, url: str, depth: int = 0) -> dict:
        if depth > self.max_depth or url in self.visited:
            return {}
        self.visited.add(url)

        src = fetch(url) or backup_fetch(url)
        if not src:
            return {}

        phones, emails = extract_contacts(src)
        result = {"url": url, "phones": phones, "emails": emails}
        if phones or emails:
            return result

        tokens = list(filter(None, re.split(r"[/-]+", urllib.parse.urlparse(url).path)))
        q = " ".join(tokens[:4] + ["contact"])
        for link in google_expand(q):
            child = self.crawl(link, depth + 1)
            if child.get("phones") or child.get("emails"):
                result["phones"] = child["phones"]
                result["emails"] = child["emails"]
                break
        return result


###########################################################################
# command-line
###########################################################################


def parse_args(args: List[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="bot_min")
    p.add_argument("url")
    p.add_argument("-d", "--depth", type=int, default=2)
    return p.parse_args(args)


def main(argv: Optional[List[str]] = None) -> None:
    ns = parse_args(argv or sys.argv[1:])
    scraper = Scraper(max_depth=ns.depth)
    data = scraper.crawl(ns.url)
    print(json.dumps(data, indent=2, ensure_ascii=False))


###########################################################################
# rudimentary self-test
###########################################################################


def _self_test() -> None:
    sample = "https://www.zillow.com/homedetails/2401-Stella-Ln-Northlake-TX-76247/2062683461_zpid/"
    out = Scraper(max_depth=0).crawl(sample)
    assert "url" in out and out["url"] == sample
    assert "phones" in out and "emails" in out


if __name__ == "__main__":
    if os.getenv("DEBUG"):
        _self_test()
    main()

