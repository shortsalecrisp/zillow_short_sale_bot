#!/usr/bin/env python3
"""
bot_min.py
----------

 • Receives Zillow (or webhook) listing dictionaries.
 • Filters for SHORT-SALE opportunities.
 • Tries to discover phone / e-mail for the listing agent.
 • Writes the data to Google Sheets.
 • Sends an SMS and, on *error == "0"* (success), drops an  **“x”** in column H
   of the same row.
 • Runs continuously every hour from 08:00 → 20:00 US-Eastern.
 • Exports `process_rows()` so `webhook_server.py` can call the same pipeline
   without spinning up a second scheduler.

Environment variables expected
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
GOOGLE_SVC_JSON  – service-account JSON (or path to the file)
SHEET_ID         – target Google Sheet ID
RAPIDAPI_KEY     – key for   https://zillow-com1.p.rapidapi.com/property
GOOGLE_API_KEY   – key for   https://www.googleapis.com/customsearch/v1
GOOGLE_CSE_ID    – Custom Search Engine CX id (scoped to real-estate sites)
SMS_API_KEY      – key for smsmobileapi.com
SMS_API_URL      – default: https://api.smsmobileapi.com/sendsms/
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
from datetime import datetime
from typing import Dict, List, Tuple, Optional

import pytz
import requests
from apscheduler.schedulers.background import BackgroundScheduler
from bs4 import BeautifulSoup
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build as gcs_build

# ---------------------------------------------------------------------------#
#  Logging
# ---------------------------------------------------------------------------#
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "DEBUG"),
    format="%(asctime)s %(levelname)-8s: %(message)s",
)
logger = logging.getLogger("bot_min")

# ---------------------------------------------------------------------------#
#  Constants & block-lists
# ---------------------------------------------------------------------------#
ET_TZ = pytz.timezone("US/Eastern")

RELAXED_BLOCKED_DOMAINS = {
    # hard “never scrape” list
    "zillow.com",
    "www.zillow.com",
    "realtor.com",
    "www.realtor.com",
    "linkedin.com",
    "www.linkedin.com",
}

FUNERAL_OBIT_RE = re.compile(r"\b(funeral home|obituary|obituaries?)\b", re.I)
EDU_RE = re.compile(r"\.edu\b", re.I)

EMAIL_RE   = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.I)
PHONE_RE   = re.compile(r"\(?\+?1?[- )]*(\d{3})[- )]*(\d{3})[- ]*(\d{4})")
SHORT_SALE = re.compile(r"\bshort\s+sale\b", re.I)

GSHEET_COL_H = 8  # 1-based indexing (A=1 … H=8)


# ---------------------------------------------------------------------------#
#  Google Sheets helpers
# ---------------------------------------------------------------------------#
def get_sheet() -> gspread.Worksheet:
    creds_json = os.getenv("GOOGLE_SVC_JSON")
    if not creds_json:
        raise RuntimeError("GOOGLE_SVC_JSON not defined")

    if os.path.exists(creds_json):
        creds = Credentials.from_service_account_file(creds_json, scopes=["https://www.googleapis.com/auth/spreadsheets"])
    else:
        creds = Credentials.from_service_account_info(json.loads(creds_json), scopes=["https://www.googleapis.com/auth/spreadsheets"])

    gc = gspread.authorize(creds)
    sh = gc.open_by_key(os.environ["SHEET_ID"])
    return sh.sheet1


SHEET = get_sheet()


def append_listing_row(listing: dict, phone: str | None, email: str | None) -> int:
    """Append a new row; return its 1-based index (row number)."""
    row = [
        listing.get("zpid", ""),
        listing.get("street", ""),
        listing.get("city", ""),
        listing.get("state", ""),
        listing.get("zip", ""),
        phone or "",
        email or "",
        "",  # column H (sent marker) – blank for now
        listing.get("agentName", ""),
        listing.get("description", "")[:250],  # truncate to keep sheet readable
    ]
    result = SHEET.append_row(row, value_input_option="RAW")
    row_idx = int(result["updates"]["updatedRange"].split("!")[1].split(":")[0][1:])
    logger.info("Row appended to sheet (row %s)", row_idx)
    return row_idx


def mark_sms_sent(row_idx: int) -> None:
    SHEET.update_cell(row_idx, GSHEET_COL_H, "x")
    logger.debug("Marked row %s column H as sent", row_idx)


# ---------------------------------------------------------------------------#
#  Data discovery
# ---------------------------------------------------------------------------#
def zillow_property(zpid: str) -> dict:
    url = "https://zillow-com1.p.rapidapi.com/property"
    headers = {
        "X-RapidAPI-Key": os.environ["RAPIDAPI_KEY"],
        "X-RapidAPI-Host": "zillow-com1.p.rapidapi.com",
    }
    try:
        resp = requests.get(url, headers=headers, params={"zpid": zpid}, timeout=10)
        resp.raise_for_status()
        return resp.json()
    except Exception as exc:  # noqa: BLE001
        logger.debug("Zillow fetch failed %s: %s", zpid, exc)
        return {}


def google_custom_search(query: str, num: int = 10) -> List[dict]:
    svc = gcs_build("customsearch", "v1", developerKey=os.environ["GOOGLE_API_KEY"])
    try:
        res = svc.cse().list(q=query, cx=os.environ["GOOGLE_CSE_ID"], num=num).execute()
        return res.get("items", [])
    except Exception as exc:  # noqa: BLE001
        logger.debug("CSE fail for %s: %s", query, exc)
        return []


def allowed_url(url: str) -> bool:
    host = requests.utils.urlparse(url).netloc.lower()
    if host in RELAXED_BLOCKED_DOMAINS:
        return False
    if FUNERAL_OBIT_RE.search(url) or EDU_RE.search(url):
        return False
    return True


def extract_email_phone_from_html(html: str) -> Tuple[Optional[str], Optional[str]]:
    email  = next(iter(EMAIL_RE.findall(html)), None)
    phone_match = PHONE_RE.search(html)
    phone = None
    if phone_match:
        phone = f"{phone_match.group(1)}-{phone_match.group(2)}-{phone_match.group(3)}"
    return email, phone


def discover_contact(listing: dict) -> Tuple[Optional[str], Optional[str]]:
    """
    Try Zillow first, then Google Custom Search (relaxed mode – with block-list),
    finally Facebook pages (explicitly allowed).
    """
    zpid = str(listing.get("zpid", ""))
    j    = zillow_property(zpid) if zpid else {}
    phone = None
    email = None

    # Zillow phone (often in "listed_by")
    contact = j.get("listed_by", {}) if isinstance(j.get("listed_by"), dict) else {}
    if contact:
        phone = contact.get("phone")
        email = contact.get("email")

    if phone and email:
        logger.debug("PHONE/EMAIL from Zillow direct")
        return email, phone

    # --- Google CSE pass --------------------------------------------------- #
    name = listing.get("agentName", "")
    location = f'{listing.get("city","")}, {listing.get("state","")}'
    queries = [
        f'"{name}" {location} realtor email',
        f'"{name}" {location} real estate phone',
        f'"{name}" {location} "@gmail.com"',
    ]
    for q in queries:
        for item in google_custom_search(q):
            link = item.get("link", "")
            if not allowed_url(link):
                continue
            try:
                html = requests.get(link, timeout=10).text
            except Exception:  # noqa: BLE001
                continue
            email2, phone2 = extract_email_phone_from_html(html)
            email  = email  or email2
            phone  = phone  or phone2
            if email and phone:
                break
        if email and phone:
            break

    # --- Facebook pass ----------------------------------------------------- #
    if not (email or phone):
        fb_query = f'"{name}" site:facebook.com email phone realtor "{listing.get("state","")}"'
        for item in google_custom_search(fb_query):
            link = item.get("link", "")
            if "facebook.com" not in link.lower():
                continue
            try:
                html = requests.get(link, timeout=10, headers={"User-Agent": "Mozilla/5.0"}).text
            except Exception:  # noqa: BLE001
                continue
            email, phone = extract_email_phone_from_html(html)
            if email or phone:
                break

    return email, phone


# ---------------------------------------------------------------------------#
#  SMS
# ---------------------------------------------------------------------------#
def send_sms(phone: str, body: str) -> bool:
    url = os.getenv("SMS_API_URL", "https://api.smsmobileapi.com/sendsms/")
    payload = {
        "key": os.environ["SMS_API_KEY"],
        "to": phone,
        "msg": body,
    }
    try:
        r = requests.post(url, data=payload, timeout=10).json()
    except Exception as exc:  # noqa: BLE001
        logger.error("SMS request error: %s", exc)
        return False

    err = str(r.get("error", ""))
    logger.debug("SMS response: %s", r)
    return err == "0"


# ---------------------------------------------------------------------------#
#  Main per-listing pipeline
# ---------------------------------------------------------------------------#
def handle_single_listing(listing: dict) -> Dict[str, object]:
    zpid = listing.get("zpid", "")
    address = f'{listing.get("street","")}, {listing.get("city","")}, {listing.get("state","")} {listing.get("zip","")}'
    desc = listing.get("description", "")

    if not SHORT_SALE.search(desc):
        logger.debug("SKIP non-short-sale %s (%s)", address, zpid)
        return {"ok": False, "reason": "not short-sale"}

    email, phone = discover_contact(listing)

    row_idx = append_listing_row(listing, phone, email)

    sms_sent = False
    if phone:
        msg = f"Hi, {listing.get('agentName','Agent')} – I’m interested in your SHORT-SALE at {address}. Please call me back!"
        sms_sent = send_sms(phone if phone.startswith("+") else f"+1{re.sub(r'\\D','',phone)}", msg)
        if sms_sent:
            mark_sms_sent(row_idx)

    return {"ok": True, "row": row_idx, "sms": sms_sent}


# ---------------------------------------------------------------------------#
#  Webhook entry-point  (imported by webhook_server.py)
# ---------------------------------------------------------------------------#
def process_rows(payload: dict) -> dict:
    """Webhook payload → same pipeline used by the hourly scheduler."""
    listings = payload.get("listings", [])
    processed, successes = 0, 0
    for lst in listings:
        res = handle_single_listing(lst)
        processed += 1
        successes += int(res.get("ok"))
    return {"processed": processed, "success": successes}


# ---------------------------------------------------------------------------#
#  Continuous scheduler (08-19 ET ever hour on the hour)
# ---------------------------------------------------------------------------#
def continuous_loop() -> None:
    """
    Pull fresh Zillow short-sale listings (your own code / API / crawler).
    This stub just logs – replace with your real scrape + call process_rows().
    """
    logger.info("continuous_loop tick – implement your scrape here")
    # Example:
    # new_listings = scrape_zillow_short_sales_since(last_timestamp)
    # process_rows({"listings": new_listings})


def start_scheduler() -> None:
    sched = BackgroundScheduler(timezone=ET_TZ)
    # run at minute 0 of each hour, between 08 and 19 (08:00 – 19:00 inclusive),
    # which is “8 AM → 8 PM” Eastern.
    sched.add_job(
        continuous_loop,
        "cron",
        hour="8-19",
        minute=0,
        id="hourly_short_sale",
        misfire_grace_time=300,
        coalesce=True,
        max_instances=1,
    )
    sched.start()
    logger.info("Continuous scheduler started (08-19 ET hourly)")


# ---------------------------------------------------------------------------#
#  Main module start-up  (only when *executed*, not when *imported*)
# ---------------------------------------------------------------------------#
if __name__ == "__main__":
    # Don’t start the scheduler when the module is imported by Uvicorn;
    # only start it when *this* process is `python bot_min.py`.
    start_scheduler()

    # Keep the process alive – APScheduler runs in background threads.
    while True:
        time.sleep(60)

