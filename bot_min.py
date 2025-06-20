import os, re, json, time, random, logging, itertools
from typing import Dict, List, Tuple, Optional

import requests
from bs4 import BeautifulSoup
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build
from dotenv import load_dotenv

# ── logging setup 
────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

load_dotenv()  # bring in all .env / Render VARS

# 
─────────────────────────────────────────────────────────────────────────────
# CONSTANTS & ENV
# 
─────────────────────────────────────────────────────────────────────────────
GOOGLE_CREDS_JSON   = os.getenv("GOOGLE_CREDS_JSON", "google_creds.json")
SPREADSHEET_ID      = os.getenv("SHEET_ID")
SMS_API_KEY         = os.getenv("SMSM_API_KEY")             # *** fixed name ***
RAPID_KEY           = os.getenv("RAPIDAPI_KEY")
CSE_KEY             = os.getenv("GOOGLE_CSE_KEY")
CSE_ID              = os.getenv("GOOGLE_CSE_ID")

# brokerage domains we ALLOW in Google searches
BROKER_DOMAINS = {
    "redfin.com",
    "compass.com",
    "exprealty.com",
    "mlslistings.com",
    "har.com",
    "realtyonegroup.com",
    "brightmlshomes.com"
}

DISALLOWED_DOMAINS = {
    "zillow.com",
    "realtor.com",
    "linkedin.com"
}

LABEL_TABLE = {
    "mobile": 3,
    "cell":   3,
    "direct": 2,
    "office": 1,
    "fax":   -3
}

PHONE_RE = re.compile(r"(\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4})")
EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")

# ── global HTTP session ──────────────────────────────────────────────────────
session = requests.Session()
session.headers.update(
    {"User-Agent": "Mozilla/5.0 (compatible; short-sale-bot/1.0)"}
)

# 
─────────────────────────────────────────────────────────────────────────────
# GOOGLE SHEETS helpers
# 
─────────────────────────────────────────────────────────────────────────────
def _sheet_service():
    """Return an authorized Google Sheets service object."""
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]
    # creds JSON may be stored inline in an ENV var *or* be the name of a file
    raw = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    if raw:
        creds_dict = json.loads(raw)
    else:
        with open(GOOGLE_CREDS_JSON, "r", encoding="utf-8") as f:
            creds_dict = json.load(f)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    gc = gspread.authorize(creds)
    return build("sheets", "v4", credentials=creds), gc.open_by_key(SPREADSHEET_ID).sheet1

def append_row(sheet, values: List[str]):
    body = {"majorDimension": "ROWS", "values": [values]}
    service, _ = _sheet_service()
    service.spreadsheets().values().append(
        spreadsheetId=SPREADSHEET_ID,
        range="Sheet1!A1",
        valueInputOption="RAW",
        body=body
    ).execute()

# 
─────────────────────────────────────────────────────────────────────────────
# PHONE / EMAIL PARSING UTILITIES
# 
─────────────────────────────────────────────────────────────────────────────
def fmt_phone(raw: str) -> str:
    digits = re.sub(r"\D", "", raw)
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    if len(digits) != 10:
        return ""
    return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"

def valid_phone(p: str) -> bool:
    return bool(PHONE_RE.fullmatch(p)) and not p.endswith("-0000")

def score_phones(text: str) -> List[str]:
    """Return list of phones ordered by quality score."""
    scores: Dict[str, int] = {}
    for m in PHONE_RE.finditer(text):
        raw = m.group(1)
        p = fmt_phone(raw)
        if not p:
            continue
        score = 1
        prefix = text[max(0, m.start() - 32):m.start()].lower()
        for lab, w in LABEL_TABLE.items():
            if lab in prefix:
                score += w
        if score < 1:
            continue
        scores[p] = max(scores.get(p, 0), score)
    return sorted(scores, key=lambda ph: -scores[ph])

def extract_phone(html_text: str) -> Optional[str]:
    picks = score_phones(html_text)
    return picks[0] if picks else None

def extract_email(html_text: str) -> Optional[str]:
    matches = EMAIL_RE.findall(html_text)
    if not matches:
        return None
    # prefer brokerage-domain addresses first
    for e in matches:
        host = e.split("@")[-1].lower()
        if any(host.endswith(dom) for dom in BROKER_DOMAINS):
            return e
    return matches[0]

# 
─────────────────────────────────────────────────────────────────────────────
# NETWORK HELPERS
# 
─────────────────────────────────────────────────────────────────────────────
def fetch(url: str, timeout: int = 10) -> str:
    try:
        r = session.get(url, timeout=timeout)
        if 200 <= r.status_code < 300:
            r.encoding = r.apparent_encoding
            return r.text
    except Exception as e:
        logging.debug("Fetch %s failed: %s", url, e)
    return ""

def google_search(q: str, num: int = 10) -> List[str]:
    api = "https://www.googleapis.com/customsearch/v1"
    params = {"key": CSE_KEY, "cx": CSE_ID, "q": q, "num": num}
    try:
        js = session.get(api, params=params, timeout=8).json()
        return [item["link"] for item in js.get("items", [])]
    except Exception as e:
        logging.debug("CSE error %s", e)
        return []

def rapid_listing_info(zpid: str) -> Tuple[Optional[str], Optional[str]]:
    url = f"https://zillow-com1.p.rapidapi.com/property?zpid={zpid}"
    headers = {
        "X-RapidAPI-Key": RAPID_KEY,
        "X-RapidAPI-Host": "zillow-com1.p.rapidapi.com"
    }
    try:
        data = session.get(url, headers=headers, timeout=8).json()
    except Exception:
        return None, None
    info = data.get("listed_by", {}) if isinstance(data, dict) else {}
    phone = fmt_phone(info.get("phone", "")) or None
    email = info.get("email") or None
    return phone, email

# 
─────────────────────────────────────────────────────────────────────────────
# CONTACT DISCOVERY PIPE
# 
─────────────────────────────────────────────────────────────────────────────
def scrape_contact(listing: Dict) -> Tuple[Optional[str], Optional[str]]:
    """
    Try rapid-API first.  If missing data, broaden with Google queries but
    honour allowed / blocked domains.
    """
    zpid  = listing.get("zpid", "")
    agent = listing.get("agentName", "")
    state = listing.get("state", "")

    phone, email = rapid_listing_info(zpid)

    broker_sites = " OR ".join(f"site:{d}" for d in BROKER_DOMAINS)

    if not phone:
        q = f'"{agent}" {state} realtor phone number {broker_sites}'
        for link in google_search(q):
            if any(bad in link for bad in DISALLOWED_DOMAINS):
                continue
            html = fetch(link)
            phone = extract_phone(html)
            if phone:
                break

    if not email:
        q = f'"{agent}" {state} realtor email address {broker_sites}'
        for link in google_search(q):
            if any(bad in link for bad in DISALLOWED_DOMAINS):
                continue
            html = fetch(link)
            email = extract_email(html)
            if email:
                break

    return phone, email

# 
─────────────────────────────────────────────────────────────────────────────
# SMS SENDER
# 
─────────────────────────────────────────────────────────────────────────────
def send_sms(to_number: str, text: str) -> bool:
    if not SMS_API_KEY:
        logging.warning("SMSM_API_KEY not set; skip SMS send.")
        return False
    api = "https://api.smsmobileapi.com/sendsms/"
    payload = {
        "apikey":    SMS_API_KEY,
        "recipients": to_number,
        "message":    text,
        "sendsms":    1
    }
    try:
        resp = session.post(api, data=payload, timeout=6).json()
    except Exception as e:
        logging.error("SMS request failed: %s", e)
        return False
    ok = resp.get("result", {}).get("error") == "0"
    if not ok:
        logging.error("SMS send error %s", resp)
    return ok

# 
─────────────────────────────────────────────────────────────────────────────
# MAIN PUBLIC ENTRY – imported by webhook_server & callable CLI
# 
─────────────────────────────────────────────────────────────────────────────
def process_rows(listings: List[Dict]):
    sheets_service, sheet = _sheet_service()
    for lst in listings:
        # skip if not a short-sale
        if "short sale" not in lst.get("description", "").lower():
            continue
        phone, email = scrape_contact(lst)

        new_row = [
            lst.get("street", ""),
            lst.get("city",   ""),
            lst.get("state",  ""),
            lst.get("zip",    ""),
            phone or "",
            email or "",
            lst.get("agentName", ""),
            f'https://www.zillow.com/homedetails/{lst["zpid"]}_zpid/'
        ]
        append_row(sheet, new_row)
        logging.info("Row appended for %s  (phone=%s email=%s)", lst["zpid"], phone, email)

