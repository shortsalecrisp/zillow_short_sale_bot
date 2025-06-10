#!/usr/bin/env python3
from __future__ import annotations
import argparse, os, sys, time, json, re, logging, requests
from typing import List, Sequence, Iterable
from pathlib import Path
from html import unescape
from dataclasses import dataclass, field
from bs4 import BeautifulSoup
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from twilio.rest import Client

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "DEBUG").upper(),
    format="%(asctime)s [%(levelname)-8s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger(__name__)

UA = "Mozilla/5.0 (X11; Linux x86_64)"
HEADERS = {"User-Agent": UA, "Accept-Language": "en-US,en;q=0.9"}
TIMEOUT = 15
BACKUPS = ["r.jina.ai/http://", "r.jina.ai/http://www."]
PHONES = re.compile(r"(\+?1[\s.\-]?)?(\(?\d{3}\)?[\s.\-]?)?\d{3}[\s.\-]?\d{4}")
EMAILS = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.I)
GOOGLE_ID = os.getenv("GOOGLE_CSE_ID", "")
GOOGLE_KEY = os.getenv("GOOGLE_API_KEY", "")
GOOGLE_EP = "https://www.googleapis.com/customsearch/v1"
SCOPE = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
SHEET_ID = os.getenv("SHEET_ID", "")
TW_SID = os.getenv("TWILIO_SID", "")
TW_TOKEN = os.getenv("TWILIO_TOKEN", "")
TW_FROM = os.getenv("TWILIO_FROM", "")
TW_TO = os.getenv("TWILIO_TO", "")

@dataclass
class Listing:
    zpid: str = ""
    url: str = ""
    phones: List[str] = field(default_factory=list)
    emails: List[str] = field(default_factory=list)
    elapsed: float = 0.0

class Scraper:
    def __init__(self, depth: int = 2):
        self.s = requests.Session()
        self.depth = depth

    @staticmethod
    def build(raw: str) -> str:
        raw = raw.strip()
        return raw if raw.startswith("http") else f"https://www.zillow.com/homedetails/{raw}_zpid/"

    def fetch(self, url: str) -> str:
        try:
            r = self.s.get(url, headers=HEADERS, timeout=TIMEOUT)
            r.raise_for_status()
            return r.text
        except Exception as e:
            log.debug("primary fetch fail %s %s", url, e)
        for p in BACKUPS:
            alt = f"{p}{url}"
            try:
                r = self.s.get(alt, headers=HEADERS, timeout=TIMEOUT)
                r.raise_for_status()
                return r.text
            except Exception as e:
                log.debug("backup fetch fail %s %s", alt, e)
        return ""

    def google_html(self, query: str) -> str:
        if not GOOGLE_ID or not GOOGLE_KEY:
            return ""
        try:
            j = self.s.get(GOOGLE_EP, params={"q": query, "cx": GOOGLE_ID, "key": GOOGLE_KEY, "num": 1}, timeout=TIMEOUT).json()
            link = j["items"][0]["link"]
            return self.fetch(link)
        except Exception as e:
            log.debug("google fail %s", e)
            return ""

    def crawl(self, raw: str) -> Listing:
        start = time.time()
        url = self.build(raw)
        zpid = Path(url).stem.split("_")[0]
        html = self.fetch(url)
        if not html and self.depth > 1:
            html = self.google_html(zpid)
        soup = BeautifulSoup(html, "html.parser")
        text = unescape(soup.get_text(" ", strip=True))
        phones = ["".join(m).strip() for m in PHONES.findall(text)]
        emails = EMAILS.findall(text)
        listing = Listing(zpid=zpid, url=url, phones=phones, emails=emails, elapsed=time.time() - start)
        log.debug("crawl %s phones=%d emails=%d %.2fs", zpid, len(phones), len(emails), listing.elapsed)
        return listing

def sheet_client():
    creds_json = os.getenv("GSERVICE_JSON", "")
    creds = ServiceAccountCredentials.from_json_keyfile_dict(json.loads(creds_json), SCOPE)
    return gspread.authorize(creds).open_by_key(SHEET_ID).sheet1

def existing_zpids(sh):
    try:
        return set(cell.value for cell in sh.col_values(1)[1:])
    except Exception:
        return set()

def write_sheet(sh, rows: Sequence[Listing]):
    for l in rows:
        sh.append_row([l.zpid, l.url, ", ".join(l.phones), ", ".join(l.emails), f"{l.elapsed:.2f}"])

def send_sms(msg: str):
    if not (TW_SID and TW_TOKEN and TW_FROM and TW_TO):
        log.debug("twilio skip, env missing")
        return
    try:
        Client(TW_SID, TW_TOKEN).messages.create(body=msg, from_=TW_FROM, to=TW_TO)
    except Exception as e:
        log.debug("twilio fail %s", e)

def process_rows(rows: Iterable[str | dict]):
    rows = list(rows)
    raw_inputs = []
    for r in rows:
        if isinstance(r, dict):
            raw_inputs.append(r.get("url") or r.get("zpid", ""))
        else:
            raw_inputs.append(str(r))
    sc = Scraper()
    sh = sheet_client()
    seen = existing_zpids(sh)
    new_listings = []
    for raw in raw_inputs:
        zpid = raw if raw.isdigit() else Path(raw).stem.split("_")[0]
        if zpid in seen:
            log.debug("skip existing %s", zpid)
            continue
        res = sc.crawl(raw)
        new_listings.append(res)
    if new_listings:
        write_sheet(sh, new_listings)
        msg = f"Added {len(new_listings)} listings: " + ", ".join(l.zpid for l in new_listings)
        send_sms(msg)
        log.info(msg)
    else:
        log.info("no new listings")

def cli():
    ap = argparse.ArgumentParser()
    ap.add_argument("url")
    ap.add_argument("-d", "--depth", type=int, default=2)
    args = ap.parse_args()
    sc = Scraper(depth=args.depth)
    j = sc.crawl(args.url).__dict__
    print(json.dumps(j, indent=2, default=str))

if __name__ == "__main__":
    cli()

