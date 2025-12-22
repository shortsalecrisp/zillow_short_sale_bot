from __future__ import annotations

import concurrent.futures
import html
import json
import logging
import os
import re
import sqlite3
import sys
import threading
import importlib.util
from collections import Counter, defaultdict, deque
from datetime import datetime, timedelta
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode, unquote

import time, random
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import gspread
import pytz
import requests
from requests.adapters import HTTPAdapter, Retry
from requests import exceptions as req_exc
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build as gapi_build
from sms_providers import get_sender
try:
    from playwright.sync_api import sync_playwright
except ImportError:  # pragma: no cover - optional dependency
    sync_playwright = None
try:
    import dns.resolver  # type: ignore
except ImportError:  # pragma: no cover - optional dependency
    dns = None
else:
    dns = dns.resolver

_session = requests.Session()
_retries = Retry(
    total=5, connect=5, read=5,
    backoff_factor=0.6,
    status_forcelist=(429, 500, 502, 503, 504),
    allowed_methods=("GET",),
    raise_on_status=False,
)
_adapter = HTTPAdapter(max_retries=_retries)
_session.mount("http://", _adapter)
_session.mount("https://", _adapter)

_DEFAULT_TIMEOUT = 25
_PROXY_TARGETS = {"zillow.com", "www.zillow.com", "kw.com", "kellerwilliams.com"}

def _parse_proxy_pool(env_name: str, fallback: str = "") -> List[str]:
    raw = os.getenv(env_name, fallback) or ""
    return [v.strip() for v in raw.split(",") if v.strip()]

_RESIDENTIAL_PROXIES = _parse_proxy_pool("RESIDENTIAL_PROXIES")
_MOBILE_PROXIES = _parse_proxy_pool("MOBILE_PROXIES")
_PROXY_POOL = _MOBILE_PROXIES + _RESIDENTIAL_PROXIES
_PROXY_REDACT = os.getenv("PROXY_LOG_REDACT", "***")
_SCRAPER_COOKIE_POOL = [c.strip() for c in os.getenv("SCRAPER_COOKIE_POOL", "").split("|||") if c.strip()]
_ACCEPT_LANGUAGE_POOL = [
    "en-US,en;q=0.9",
    "en-US,en;q=0.8,es;q=0.5",
    "en-CA,en;q=0.8",
    "en-GB,en;q=0.9",
]
HEADLESS_ENABLED = os.getenv("HEADLESS_FALLBACK", "true").lower() == "true"
HEADLESS_TIMEOUT_MS = int(os.getenv("HEADLESS_FETCH_TIMEOUT_MS", "12000"))
HEADLESS_WAIT_MS = int(os.getenv("HEADLESS_FETCH_WAIT_MS", "1200"))

_CONNECTION_ERRORS = (
    req_exc.ConnectionError,
    req_exc.ConnectTimeout,
    req_exc.ReadTimeout,
    req_exc.Timeout,
    req_exc.ProxyError,
    req_exc.SSLError,
    req_exc.RetryError,
)

_blocked_until: Dict[str, float] = {}
_cse_blocked_until: float = 0.0
cache_p: Dict[str, Any] = {}

def _http_get(
    url: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    extra_headers: Optional[Dict[str, str]] = None,
    timeout: int | float = _DEFAULT_TIMEOUT,
    rotate_user_agent: bool = False,
    respect_block: bool = True,
    proxy: Optional[str] = None,
) -> requests.Response:
    dom = urlparse(url).netloc

    def _build_headers() -> Dict[str, str]:
        hdrs = {}
        if headers:
            hdrs.update(headers)
        if extra_headers:
            hdrs.update(extra_headers)
        if rotate_user_agent:
            hdrs["User-Agent"] = random.choice(_USER_AGENT_POOL)
        return hdrs

    if respect_block and dom in _blocked_until and _blocked_until[dom] > time.time():
        raise req_exc.RetryError(f"blocked: {dom}")

    attempts = 0
    while True:
        attempts += 1
        hdrs = _build_headers()
        proxy_cfg = {"http": proxy, "https": proxy} if proxy else None
        resp = _session.get(
            url,
            params=params,
            headers=hdrs or None,
            timeout=timeout,
            proxies=proxy_cfg,
        )
        status = resp.status_code
        if status in (403, 429) and dom:
            block_for = CSE_BLOCK_SECONDS if "googleapis.com" in dom else BLOCK_SECONDS
            _mark_block(dom, seconds=block_for)
        if status == 429 and attempts <= 5:
            ra = resp.headers.get("Retry-After")
            sleep_s = int(ra) if ra and ra.isdigit() else min(30, 2 ** attempts) + random.uniform(0, 0.5)
            time.sleep(sleep_s)
            continue
        try:
            resp.raise_for_status()
        except requests.HTTPError:
            if status in (403, 429):
                if attempts < 3:
                    time.sleep(min(5.0, 1.5 * attempts) + random.uniform(0, 0.35))
                    continue
            if attempts <= 1 and status in (500, 502, 503, 504):
                time.sleep(0.5 + random.uniform(0, 0.25))
                continue
            raise
        return resp


def _search_sleep() -> None:
    low, high = SEARCH_BACKOFF_RANGE
    if high <= 0:
        return
    jitter = random.uniform(low, high) if high > low else low
    if jitter > 0:
        time.sleep(jitter)


def _pick_search_proxy() -> Optional[str]:
    if not _PROXY_POOL:
        return None
    return random.choice(_PROXY_POOL)


def _search_disabled(engine: str) -> bool:
    circuit = _SEARCH_CIRCUIT.get(engine)
    return bool(circuit and circuit.get("disabled"))


def _record_timeout(engine: str) -> None:
    circuit = _SEARCH_CIRCUIT[engine]
    circuit["timeouts"] += 1
    if circuit["timeouts"] >= SEARCH_TIMEOUT_TRIP:
        circuit["disabled"] = True
        LOG.warning("Search engine %s disabled for run after %s timeouts", engine, circuit["timeouts"])

try:
    import phonenumbers
except ImportError:
    phonenumbers = None
try:
    from bs4 import BeautifulSoup
except ImportError:
    BeautifulSoup = None

# ───────────────────── configuration & constants ─────────────────────
# Google Custom Search credentials. Prefer CS_* names but fall back to
# GOOGLE_API_KEY / GOOGLE_CX if provided. Comma-separated pools allow
# rotation when a key/engine pair is throttled.
def _env_default(*names: str) -> str:
    for name in names:
        val = os.getenv(name)
        if val:
            return val
    # Fall back to raising on the last name to preserve the previous behavior
    return os.environ[names[-1]]


def _parse_pool(env_name: str, fallback: str) -> List[str]:
    raw = os.getenv(env_name, "")
    vals = [v.strip() for v in raw.split(",") if v.strip()]
    return vals or ([fallback] if fallback else [])


CS_API_KEY     = _env_default("CS_API_KEY", "GOOGLE_API_KEY")
CS_CX          = _env_default("CS_CX", "GOOGLE_CX")
_CSE_KEY_POOL  = _parse_pool("CS_API_KEYS", CS_API_KEY)
_CSE_CX_POOL   = _parse_pool("CS_CXS", CS_CX)
if len(_CSE_CX_POOL) == 1 and len(_CSE_KEY_POOL) > 1:
    _CSE_CX_POOL = _CSE_CX_POOL * len(_CSE_KEY_POOL)
_CSE_CRED_POOL: List[Tuple[str, str]] = list(zip(_CSE_KEY_POOL, _CSE_CX_POOL))
_CSE_CRED_INDEX = 0
GSHEET_ID      = os.environ["GSHEET_ID"]
GSHEET_TAB     = os.getenv("GSHEET_TAB", "Sheet1")
GSHEET_RANGE   = os.getenv("GSHEET_RANGE", f"{GSHEET_TAB}!A1")
GSHEET_NEXT_ROW_HINT = int(os.getenv("GSHEET_NEXT_ROW_HINT", "2566"))
GSHEET_ROW_SCAN_WINDOW = int(os.getenv("GSHEET_ROW_SCAN_WINDOW", "200"))
SC_JSON        = json.loads(os.environ["GCP_SERVICE_ACCOUNT_JSON"])
SCOPES         = ["https://www.googleapis.com/auth/spreadsheets"]

RAPID_KEY      = os.getenv("RAPID_KEY", "").strip()
RAPID_HOST     = os.getenv("RAPID_HOST", "us-housing-market-data1.p.rapidapi.com").strip()
# Guard against the deprecated RapidAPI host; auto-upgrade to the renamed endpoint.
if RAPID_HOST in {"zillow-com1.p.rapidapi.com", "zillow-com.p.rapidapi.com"}:
    logging.warning(
        "Deprecated RAPID_HOST %s detected; switching to us-housing-market-data1.p.rapidapi.com",
        RAPID_HOST,
    )
    RAPID_HOST = "us-housing-market-data1.p.rapidapi.com"

if not RAPID_KEY:
    logging.warning("RAPID_KEY is empty; RapidAPI listing enrichment will be skipped")
GOOD_STATUS    = {"FOR_SALE", "ACTIVE", "COMING_SOON", "PENDING", "NEW_CONSTRUCTION"}

TZ             = pytz.timezone(os.getenv("BOT_TIMEZONE", "US/Eastern"))
FU_HOURS       = float(os.getenv("FOLLOW_UP_HOURS", "6"))
FU_LOOKBACK_ROWS = int(os.getenv("FU_LOOKBACK_ROWS", "50"))
WORK_START     = int(os.getenv("WORK_START_HOUR", "8"))   # inclusive (8 am)
WORK_END       = int(os.getenv("WORK_END_HOUR", "21"))    # exclusive (final run starts at 8 pm)
FOLLOWUP_INCLUDE_WEEKENDS = os.getenv("FOLLOWUP_INCLUDE_WEEKENDS", "true").lower() == "true"

_sms_enable_env = os.getenv("SMS_ENABLE")
if _sms_enable_env is None:
    # Enable SMS by default when any SMS API key is present
    SMS_ENABLE = bool(os.getenv("SMS_GATEWAY_API_KEY") or os.getenv("SMS_API_KEY"))
else:
    SMS_ENABLE = _sms_enable_env.lower() == "true"
SMS_TEST_MODE     = os.getenv("SMS_TEST_MODE", "true").lower() == "true"
SMS_TEST_NUMBER   = os.getenv("SMS_TEST_NUMBER", "")
SMS_PROVIDER      = os.getenv("SMS_PROVIDER", "android_gateway")
SMS_SENDER        = get_sender(SMS_PROVIDER)
SMS_TEMPLATE      = (
    "Hey {first}, this is Yoni Kutler—I saw your short sale listing at "
    "{address} and wanted to introduce myself. I specialize in helping "
    "agents get faster bank approvals and ensure these deals close. I "
    "know you likely handle short sales yourself, but I work behind the "
    "scenes to take on lender negotiations so you can focus on selling. "
    "No cost to you or your client—I’m only paid by the buyer at closing. "
    "Would you be open to a quick call to see if this could help?"
)
SMS_FU_TEMPLATE   = (
    "Hey, just wanted to follow up on my message from earlier. "
    "Let me know if I can help with anything—happy to connect whenever works for you!"
)
SMS_RETRY_ATTEMPTS = int(os.getenv("SMS_RETRY_ATTEMPTS", "2"))
CLOUDMERSIVE_KEY = os.getenv("CLOUDMERSIVE_KEY", "").strip()

CSE_MIN_INTERVAL = float(os.getenv("CSE_MIN_INTERVAL", "4.0"))
_cse_jitter_low = float(os.getenv("CSE_JITTER_LOW", "1.0"))
_cse_jitter_high = float(os.getenv("CSE_JITTER_HIGH", "2.6"))
_cse_window_seconds = float(os.getenv("CSE_WINDOW_SECONDS", "60"))
_cse_max_in_window = int(os.getenv("CSE_MAX_IN_WINDOW", "12"))
if _cse_jitter_high < _cse_jitter_low:
    _cse_jitter_low, _cse_jitter_high = _cse_jitter_high, _cse_jitter_low

# How long to back off from a domain after a block (403/429). Default 15 minutes.
BLOCK_SECONDS = float(os.getenv("BLOCK_SECONDS", "900"))
CSE_BLOCK_SECONDS = float(os.getenv("CSE_BLOCK_SECONDS", str(BLOCK_SECONDS)))

CONTACT_DOMAIN_MIN_GAP = float(os.getenv("CONTACT_DOMAIN_MIN_GAP", "4.0"))
CONTACT_DOMAIN_GAP_JITTER = float(os.getenv("CONTACT_DOMAIN_GAP_JITTER", "1.5"))

CONTACT_EMAIL_MIN_SCORE = float(os.getenv("CONTACT_EMAIL_MIN_SCORE", "0.75"))
CONTACT_EMAIL_FALLBACK_SCORE = float(os.getenv("CONTACT_EMAIL_FALLBACK_SCORE", "0.45"))
CONTACT_PHONE_MIN_SCORE = float(os.getenv("CONTACT_PHONE_MIN_SCORE", "2.25"))
CONTACT_PHONE_LOW_CONF  = float(os.getenv("CONTACT_PHONE_LOW_CONF", "1.5"))
CONTACT_PHONE_OVERRIDE_MIN = float(os.getenv("CONTACT_PHONE_OVERRIDE_MIN", "1.0"))
CONTACT_PHONE_OVERRIDE_DELTA = float(os.getenv("CONTACT_PHONE_OVERRIDE_DELTA", "1.0"))
CLOUDMERSIVE_MOBILE_BOOST = float(os.getenv("CLOUDMERSIVE_MOBILE_BOOST", "0.8"))
CONTACT_OVERRIDE_JSON = os.getenv("CONTACT_OVERRIDE_JSON", "")

_USER_AGENT_POOL = [
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    ),
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) "
        "Gecko/20100101 Firefox/126.0"
    ),
    (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6_6) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15"
    ),
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0"
    ),
]

_BASE_BROWSER_HEADERS = {
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,image/apng,*/*;q=0.8"
    ),
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": _ACCEPT_LANGUAGE_POOL[0],
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}

def _random_cookie_header() -> Dict[str, str]:
    if not _SCRAPER_COOKIE_POOL:
        return {}
    return {"Cookie": random.choice(_SCRAPER_COOKIE_POOL)}

def _browser_headers(domain: Optional[str] = None) -> Dict[str, str]:
    headers = dict(_BASE_BROWSER_HEADERS)
    headers["User-Agent"] = random.choice(_USER_AGENT_POOL)
    headers["Accept-Language"] = random.choice(_ACCEPT_LANGUAGE_POOL)
    headers.update(_random_cookie_header())
    if domain:
        headers.setdefault("Referer", f"https://{domain}")
    return headers

# Backwards-compatible alias for callers expecting a module-level mapping.
BROWSER_HEADERS = _browser_headers()

_generic_domains_env = os.getenv(
    "CONTACT_GENERIC_EMAIL_DOMAINS",
    "homelight.com,example.org,example.com,yoursite.com,yourdomain.com",
)
GENERIC_EMAIL_DOMAINS = {
    d.strip().lower()
    for d in _generic_domains_env.split(",")
    if d.strip()
}
GENERIC_EMAIL_PREFIXES = {
    "info",
    "contact",
    "support",
    "press",
    "pr",
    "office",
    "hello",
    "team",
    "frontdesk",
    "marketing",
    "admin",
    "name",
    "yourname",
    "email",
    "firstnamelastname",
}
ENABLE_SYNTH_EMAIL_FALLBACK = os.getenv("ENABLE_SYNTH_EMAIL_FALLBACK", "false").lower() == "true"

STATE_ABBR_TO_NAME = {
    "AL": "Alabama",
    "AK": "Alaska",
    "AZ": "Arizona",
    "AR": "Arkansas",
    "CA": "California",
    "CO": "Colorado",
    "CT": "Connecticut",
    "DE": "Delaware",
    "DC": "District of Columbia",
    "FL": "Florida",
    "GA": "Georgia",
    "HI": "Hawaii",
    "ID": "Idaho",
    "IL": "Illinois",
    "IN": "Indiana",
    "IA": "Iowa",
    "KS": "Kansas",
    "KY": "Kentucky",
    "LA": "Louisiana",
    "ME": "Maine",
    "MD": "Maryland",
    "MA": "Massachusetts",
    "MI": "Michigan",
    "MN": "Minnesota",
    "MS": "Mississippi",
    "MO": "Missouri",
    "MT": "Montana",
    "NE": "Nebraska",
    "NV": "Nevada",
    "NH": "New Hampshire",
    "NJ": "New Jersey",
    "NM": "New Mexico",
    "NY": "New York",
    "NC": "North Carolina",
    "ND": "North Dakota",
    "OH": "Ohio",
    "OK": "Oklahoma",
    "OR": "Oregon",
    "PA": "Pennsylvania",
    "RI": "Rhode Island",
    "SC": "South Carolina",
    "SD": "South Dakota",
    "TN": "Tennessee",
    "TX": "Texas",
    "UT": "Utah",
    "VT": "Vermont",
    "VA": "Virginia",
    "WA": "Washington",
    "WV": "West Virginia",
    "WI": "Wisconsin",
    "WY": "Wyoming",
}

# column indices (0‑based)
COL_FIRST       = 0   # A
COL_LAST        = 1   # B
COL_PHONE       = 2   # C
COL_EMAIL       = 3   # D
COL_STREET      = 4   # E
COL_CITY        = 5   # F
COL_STATE       = 6   # G
COL_SENT_FLAG   = 7   # H
COL_REPLY_FLAG  = 8   # I  ← check before follow‑up
COL_MANUAL_NOTE = 9   # J  ← check before follow‑up
COL_REPLY_TS    = 10  # K
COL_MSG_ID      = 11  # L
COL_INIT_TS     = 22  # W
COL_FU_TS       = 23  # X
COL_PHONE_CONF  = 24  # Y
COL_CONTACT_REASON = 25  # Z
COL_EMAIL_CONF  = 26  # AA
MIN_COLS        = 27

MAX_ZILLOW_403      = 3
MAX_RATE_429        = 3
BACKOFF_FACTOR      = 1.7
MAX_BACKOFF_SECONDS = 12
GOOGLE_CONCURRENCY  = 1
METRICS: Counter    = Counter()

logging.basicConfig(
    level=os.getenv("LOGLEVEL", "DEBUG"),
    format="%(asctime)s %(levelname)-8s: %(message)s",
    force=True,
)
LOG = logging.getLogger("bot_min")

# ───────────────────── regexes & misc helpers ─────────────────────
SHORT_RE = re.compile(r"\bshort\s+sale\b", re.I)
BAD_RE   = re.compile(
    r"\b(?:approved short sale|short sale approved|not a\s+short\s+sale)\b",
    re.I,
)
TEAM_RE  = re.compile(r"^\s*the\b|\bteam\b", re.I)
IMG_EXT  = (".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp")
PHONE_RE = re.compile(
    r"(?<!\d)(?:\+?1[\s\-\.]*)?\(?\d{3}\)?[\s\-\.]*\d{3}[\s\-\.]*\d{4}(?!\d)"
)
EMAIL_RE = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.I)

LABEL_TABLE = {
    "mobile": 4, "cell": 4, "direct": 4, "text": 4,
    "c:": 4, "m:": 4,
    "phone": 2, "tel": 2, "p:": 2,
    "office": 1, "main": 1, "customer": 1, "footer": 1,
}
LABEL_RE      = re.compile("(" + "|".join(map(re.escape, LABEL_TABLE)) + ")", re.I)
US_AREA_CODES = {str(i) for i in range(201, 990)}
OFFICE_HINTS  = {"office", "main", "fax", "team", "brokerage", "corporate"}
BAD_AREA      = {
    "800",
    "888",
    "877",
    "866",
    "855",
    "844",
    "833",
}
CONTACT_PAGE_HINTS = ("contact", "office", "team", "company")
PHONE_OFFICE_TERMS = {
    "office",
    "front desk",
    "main office",
    "main line",
    "switchboard",
    "reception",
    "brokerage",
    "team",
    "corporate",
    "assistant",
}

ALT_PHONE_SITES: Tuple[str, ...] = (
    "kw.com",
    "coldwellbankerhomes.com",
    "remax.com",
    "century21.com",
    "bhhs.com",
    "exprealty.com",
    "compass.com",
    "realtyonegroup.com",
    "zillow.com",
    "realtor.com",
)

CONTACT_SITE_PRIORITY: Tuple[str, ...] = (
    "realtor.com",
    "zillow.com",
    "remax.com",
    "kw.com",
    "kellerwilliams.com",
    "bhhs.com",
    "compass.com",
    "coldwellbankerhomes.com",
    "facebook.com",
    "linkedin.com",
    "instagram.com",
)

GOOGLE_PORTAL_DENYLIST: Set[str] = {
    "zillow.com",
    "www.zillow.com",
    "realtor.com",
    "www.realtor.com",
    "redfin.com",
    "homes.com",
    "www.homes.com",
    "trulia.com",
    "www.trulia.com",
    "yelp.com",
    "www.yelp.com",
}

# ───────────────────── Google / Sheets setup ─────────────────────
creds           = Credentials.from_service_account_info(SC_JSON, scopes=SCOPES)
sheets_service  = gapi_build("sheets", "v4", credentials=creds, cache_discovery=False)
gc              = gspread.authorize(creds)
ws              = gc.open_by_key(GSHEET_ID).worksheet(GSHEET_TAB)

try:
    _preloaded = ws.col_values(COL_PHONE + 1)
except Exception:
    _preloaded = []
seen_phones: Set[str] = set(_preloaded)

SCRAPE_SITES:  List[str] = []
DYNAMIC_SITES: Set[str]  = set()
PORTAL_DOMAINS: Set[str] = {
    "zillow.com",
    "realtor.com",
    "redfin.com",
    "homes.com",
    "trulia.com",
    "apartments.com",
    "homesnap.com",
    "har.com",
    "mlspin.com",
}
# Keep portals in sync with any scrape-site hints that get loaded later.
PORTAL_DOMAINS.update(SCRAPE_SITES)

BAN_KEYWORDS = {
    "zillow.com", "realtor.com", "redfin.com", "homes.com",
    "linkedin.com", "twitter.com", "instagram.com", "pinterest.com", "facebook.com", "legacy.com",
    "obituary", "obituaries", "funeral",
    ".gov", ".edu", ".mil",
}

SEARCH_BACKOFF_RANGE = (
    float(os.getenv("SEARCH_BACKOFF_MIN", "0.4")),
    float(os.getenv("SEARCH_BACKOFF_MAX", "1.2")),
)
SEARCH_TIMEOUT_TRIP = int(os.getenv("SEARCH_TIMEOUT_TRIP", "2"))
_SEARCH_CIRCUIT: Dict[str, Dict[str, Any]] = defaultdict(lambda: {"timeouts": 0, "disabled": False})

_executor = concurrent.futures.ThreadPoolExecutor(max_workers=GOOGLE_CONCURRENCY)
def pmap(fn, iterable): return list(_executor.map(fn, iterable))

# ───────────────────── phone / email formatting helpers ─────────────────────
def _is_bad_area(area: str) -> bool:
    """Return True when *area* is clearly not a valid US area code.

    Reject toll-free prefixes, anything starting with ``1``, and any prefix not
    present in ``US_AREA_CODES`` (e.g., "040"), so obviously malformed numbers
    never pass initial formatting/validation.
    """

    if not area:
        return True
    if area.startswith("1"):
        return True
    if area in BAD_AREA:
        return True
    return area not in US_AREA_CODES

def fmt_phone(r: str) -> str:
    d = re.sub(r"\D", "", r)
    if len(d) == 11 and d.startswith("1"):
        d = d[1:]
    if len(d) == 10 and not _is_bad_area(d[:3]):
        return f"{d[:3]}-{d[3:6]}-{d[6:]}"
    return ""

def valid_phone(p: str) -> bool:
    if not p:
        return False
    if phonenumbers:
        try:
            ok = phonenumbers.is_possible_number(phonenumbers.parse(p, "US"))
            return ok and not _is_bad_area(p[:3])
        except Exception:
            return False
    return bool(re.fullmatch(r"\d{3}-\d{3}-\d{4}", p) and not _is_bad_area(p[:3]))

def clean_email(e: str) -> str:
    return e.split("?")[0].strip()

def ok_email(e: str) -> bool:
    e = clean_email(e)
    return (
        e and "@" in e
        and not e.lower().endswith(IMG_EXT)
        and not re.search(r"\.(gov|edu|mil)$", e, re.I)
    )


def decode_cfemail(data: str) -> str:
    """Decode Cloudflare obfuscated email strings."""
    if not data:
        return ""
    try:
        key = int(data[:2], 16)
    except ValueError:
        return ""
    chars: List[str] = []
    for i in range(2, len(data), 2):
        chunk = data[i : i + 2]
        if len(chunk) < 2:
            return ""
        try:
            decoded = int(chunk, 16) ^ key
        except ValueError:
            return ""
        chars.append(chr(decoded))
    return "".join(chars)

def is_short_sale(text: str) -> bool:
    return SHORT_RE.search(text) and not BAD_RE.search(text)


def _is_weekend(d: datetime) -> bool:
    """Return True if ``d`` falls on a weekend (Saturday/Sunday)."""
    return d.weekday() >= 5

# ───────────────────── working‑hour elapsed helper (UPDATED) ─────────────────────
def business_hours_elapsed(start_ts: datetime, now: datetime) -> float:
    """Return number of *working* hours elapsed between ``start_ts`` and ``now``.

    Only time falling between ``WORK_START`` and ``WORK_END`` on weekdays is
    counted.  Both datetimes are converted to the bot's timezone to avoid
    mismatches.  The calculation walks in 15‑minute increments for reasonable
    accuracy without being too expensive.
    """

    if start_ts.tzinfo is None:
        start_ts = start_ts.replace(tzinfo=TZ)
    else:
        start_ts = start_ts.astimezone(TZ)

    if now.tzinfo is None:
        now = now.replace(tzinfo=TZ)
    else:
        now = now.astimezone(TZ)

    if start_ts >= now:
        return 0.0

    total = 0.0
    cur = start_ts
    step = timedelta(minutes=15)
    while cur < now:
        end_of_work = cur.replace(hour=WORK_END, minute=0, second=0, microsecond=0)
        nxt = min(now, cur + step, end_of_work)
        if nxt == cur:
            cur = (cur + timedelta(days=1)).replace(
                hour=WORK_START, minute=0, second=0, microsecond=0
            )
            LOG.debug("business_hours_elapsed skipped to next workday")
            continue
        if not _is_weekend(cur) and WORK_START <= cur.hour < WORK_END:
            total += (nxt - cur).total_seconds() / 3600.0
        cur = nxt

    return total

# ───────────────────── scraping / lookup helpers (UNCHANGED) ─────────────────────
_RAPID_CACHE: Dict[str, Tuple[float, Dict[str, Any]]] = {}
RAPID_TTL_SEC = 15 * 60

def _phone_obj_to_str(obj: Dict[str, str]) -> str:
    if not obj:
        return ""
    key_order = [
        "areacode", "area_code", "areaCode", "prefix",
        "centralofficecode", "central_office_code", "centralOfficeCode",
        "number", "line", "line_number", "lineNumber",
    ]
    parts = []
    for k in key_order:
        if obj.get(k):
            parts.append(re.sub(r"\D", "", str(obj[k])))
    for v in obj.values():
        chunk = re.sub(r"\D", "", str(v))
        if 2 <= len(chunk) <= 4:
            parts.append(chunk)
    digits = "".join(parts)[:10]
    return fmt_phone(digits)

def rapid_property(zpid: str) -> Dict[str, Any]:
    now = time.time()
    entry = _RAPID_CACHE.get(zpid)
    if entry and now - entry[0] < RAPID_TTL_SEC:
        return entry[1]
    if not RAPID_KEY:
        _RAPID_CACHE[zpid] = (now, {})
        return {}
    try:
        headers = {"X-RapidAPI-Key": RAPID_KEY, "X-RapidAPI-Host": RAPID_HOST}
        r = _http_get(
            f"https://{RAPID_HOST}/property",
            params={"zpid": zpid},
            extra_headers=headers,
            timeout=15,
        )
        if r.status_code == 429:
            LOG.error("Rapid‑API quota exhausted (HTTP 429)")
            data = {}
        else:
            r.raise_for_status()
            data = r.json().get("data") or r.json()
    except Exception as exc:
        LOG.debug("Rapid‑API fetch error %s for zpid=%s", exc, zpid)
        data = {}
    _RAPID_CACHE[zpid] = (now, data)
    return data

def _phones_from_block(blk: Dict[str, Any]) -> List[str]:
    out = []
    if not blk:
        return out
    if blk.get("phone"):
        out.append(_phone_obj_to_str(blk["phone"]))
    for ph in blk.get("phones", []):
        out.append(_phone_obj_to_str(ph))
    return [p for p in out if p]

def _emails_from_block(blk: Dict[str, Any]) -> List[str]:
    if not blk:
        return []
    out = []
    for k in ("email", "emailAddress"):
        if blk.get(k):
            out.append(clean_email(blk[k]))
    for e in blk.get("emails", []):
        out.append(clean_email(e))
    return [e for e in out if ok_email(e)]


def _rapid_profile_urls(data: Dict[str, Any]) -> List[str]:
    if not data:
        return []
    urls: Set[str] = set()
    blocks: List[Dict[str, Any]] = []
    lb = data.get("listed_by") or {}
    if isinstance(lb, dict) and lb:
        blocks.append(lb)
    contacts = data.get("contact_recipients") or []
    if isinstance(contacts, list):
        blocks.extend([blk for blk in contacts if isinstance(blk, dict)])

    url_keys = {
        "profile_url",
        "profileUrl",
        "profile",
        "agent_url",
        "agentUrl",
        "url",
        "website",
        "web_url",
        "webUrl",
        "link",
    }

    def _add_url(val: Any) -> None:
        if isinstance(val, str) and val.startswith(("http://", "https://")):
            urls.add(val.strip())

    for blk in blocks:
        for key in url_keys:
            _add_url(blk.get(key))
        for bucket_key in ("urls", "links"):
            bucket = blk.get(bucket_key)
            if isinstance(bucket, list):
                for item in bucket:
                    _add_url(item)

    return list(urls)

def _names_match(a: str, b: str) -> bool:
    if not a or not b:
        return False
    if not isinstance(a, str):
        a = str(a or "")
    if not isinstance(b, str):
        b = str(b or "")
    ta = {t.lower().strip(".") for t in a.split() if len(t) > 1}
    tb = {t.lower().strip(".") for t in b.split() if len(t) > 1}
    return bool(ta & tb)

def rapid_phone(zpid: str, agent_name: str) -> Tuple[str, str]:
    data = rapid_property(zpid)
    if not data:
        return "", ""
    cand, allp = [], set()
    for blk in data.get("contact_recipients", []):
        for pn in _phones_from_block(blk):
            allp.add(pn)
            if _names_match(agent_name, blk.get("display_name", "")):
                cand.append(("rapid:contact_recipients", pn))
    lb = data.get("listed_by", {})
    for pn in _phones_from_block(lb):
        allp.add(pn)
        if _names_match(agent_name, lb.get("display_name", "")):
            cand.append(("rapid:listed_by", pn))
    if cand:
        return cand[0][1], cand[0][0]
    if len(allp) == 1:
        return next(iter(allp)), "rapid:fallback_single"
    return "", ""

def _jitter() -> None:
    time.sleep(random.uniform(0.8, 1.5))

def _mark_block(dom: str, *, seconds: float = BLOCK_SECONDS) -> None:
    _blocked_until[dom] = time.time() + seconds


def _cse_blocked() -> bool:
    return _blocked("www.googleapis.com") or _cse_blocked_until > time.time()

def _blocked(dom: str) -> bool:
    return _blocked_until.get(dom, 0.0) > time.time()

def _try_textise(dom: str, url: str) -> str:
    try:
        mirror_url = f"https://r.jina.ai/http://{urlparse(url).netloc}{urlparse(url).path}"
        r = _http_get(
            mirror_url,
            timeout=10,
            headers=_browser_headers(dom),
            rotate_user_agent=True,
            respect_block=False,
        )
        if r.status_code == 200 and r.text.strip():
            return r.text
    except Exception:
        pass
    return ""

def _domain(host_or_url: str) -> str:
    host = urlparse(host_or_url).netloc or host_or_url
    parts = host.lower().split(".")
    return ".".join(parts[-2:]) if len(parts) >= 2 else host.lower()

def _proxy_for_domain(domain: str) -> str:
    dom = _domain(domain)
    if not _PROXY_POOL or not dom:
        return ""
    if dom in _PROXY_TARGETS or any(dom.endswith(t) for t in _PROXY_TARGETS):
        if dom.endswith("kw.com") or dom.endswith("kellerwilliams.com"):
            pool = _MOBILE_PROXIES or _PROXY_POOL
        else:
            pool = _PROXY_POOL
        return random.choice(pool) if pool else ""
    return ""

def _redact_proxy(proxy_url: str) -> str:
    if not proxy_url:
        return ""
    if "@" in proxy_url:
        return proxy_url.split("@", 1)[-1]
    return proxy_url if proxy_url.startswith(_PROXY_REDACT) else _PROXY_REDACT

def _is_banned(dom: str) -> bool:
    return any(bad in dom for bad in BAN_KEYWORDS)

def _should_fetch(url: str, strict: bool = True) -> bool:
    dom = _domain(url)
    if _blocked(dom):
        return False
    return not (_is_banned(dom) and strict)

def fetch_simple(u: str, strict: bool = True):
    if not _should_fetch(u, strict):
        return None
    dom = _domain(u)
    try:
        try:
            r = _http_get(
                u,
                timeout=10,
                headers=_browser_headers(dom),
                rotate_user_agent=True,
            )
        except requests.HTTPError as exc:
            r = exc.response
            if r is None:
                raise
        if r.status_code == 200:
            return r.text
        if r.status_code in (403, 429):
            _mark_block(dom)
        if r.status_code in (403, 451):
            txt = _try_textise(dom, u)
            if txt:
                return txt
    except Exception as exc:
        LOG.debug("fetch_simple error %s on %s", exc, u)
    return None

def fetch(u: str, strict: bool = True):
    if not _should_fetch(u, strict):
        return None
    dom = _domain(u)
    proxy_url = _proxy_for_domain(dom)
    bare = re.sub(r"^https?://", "", u)
    variants = [
        u,
        f"https://r.jina.ai/http://{bare}",
        f"https://r.jina.ai/http://screenshot/{bare}",
    ]
    z403 = ratelimit = 0
    backoff = 1.0
    for url in variants:
        if _blocked(dom):
            return None
        for _ in range(3):
            try:
                try:
                    r = _http_get(
                        url,
                        timeout=10,
                        headers=_browser_headers(dom),
                        rotate_user_agent=True,
                        proxy=proxy_url,
                    )
                except requests.HTTPError as exc:
                    r = exc.response
                    if r is None:
                        raise
            except Exception as exc:
                METRICS["fetch_error"] += 1
                LOG.debug("fetch error %s on %s", exc, url)
                break
            if r is None:
                break
            if r.status_code == 200:
                if "unusual traffic" in r.text[:700].lower():
                    METRICS["fetch_unusual"] += 1
                    break
                return r.text
            if r.status_code == 403 and "zillow.com" in url:
                z403 += 1
                METRICS["fetch_403"] += 1
                if z403 >= MAX_ZILLOW_403:
                    return None
                _mark_block(dom)
                break
            elif r.status_code == 429:
                ratelimit += 1
                METRICS["fetch_429"] += 1
                if ratelimit >= MAX_RATE_429:
                    _mark_block(dom)
                    return None
                break
            elif r.status_code in (403, 451):
                _mark_block(dom)
                txt = _try_textise(dom, u)
                if txt:
                    return txt
                break
            else:
                METRICS[f"fetch_other_{r.status_code}"] += 1
            _jitter()
            time.sleep(min(backoff, MAX_BACKOFF_SECONDS))
            backoff *= BACKOFF_FACTOR
    return None

def fetch_simple_relaxed(u: str):
    return fetch_simple(u, strict=False)

def fetch_relaxed(u: str):
    return fetch(u, strict=False)

# ───────────────────── contact fetch helpers ─────────────────────
_CONTACT_FETCH_BACKOFFS = (0.0, 2.5, 6.0)

_CONTACT_DOMAIN_LAST_FETCH: Dict[str, float] = {}
_REALTOR_DOMAINS = {"realtor.com", "www.realtor.com"}
_REALTOR_MAX_RETRIES = int(os.getenv("REALTOR_MAX_RETRIES", "5"))
_REALTOR_BACKOFF_BASE = float(os.getenv("REALTOR_BACKOFF_BASE", "3.0"))
_REALTOR_BACKOFF_CAP = float(os.getenv("REALTOR_BACKOFF_CAP", "20.0"))
_REALTOR_BACKOFF_JITTER = float(os.getenv("REALTOR_BACKOFF_JITTER", "1.75"))


def _mirror_url(url: str) -> str:
    parsed = urlparse(url)
    if not parsed.scheme or not parsed.netloc:
        return ""
    base = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
    mirror = f"https://r.jina.ai/{base}"
    if parsed.query:
        mirror = f"{mirror}?{parsed.query}"
    return mirror


# ───────────────────── Jina Reader cache helpers ─────────────────────
_CACHE_DB_PATH = os.path.join(os.path.dirname(__file__), "jina_cache.sqlite")
_CACHE_LOCK = threading.Lock()
_CACHE_DOMAIN_LAST_FETCH: Dict[str, float] = {}
_CACHE_DEDUPE_RUN: Set[str] = set()
_TRACKING_QUERY_KEYS = {
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "utm_term",
    "utm_content",
    "fbclid",
    "gclid",
    "mc_cid",
    "mc_eid",
}


def normalize_url(url: str) -> str:
    parsed = urlparse(url)
    filtered_qs = [
        (k, v)
        for k, v in parse_qsl(parsed.query, keep_blank_values=True)
        if k.lower() not in _TRACKING_QUERY_KEYS and not k.lower().startswith("utm_")
    ]
    new_query = urlencode(filtered_qs)
    cleaned = parsed._replace(query=new_query, fragment="")
    return urlunparse(cleaned)


def _cache_conn() -> sqlite3.Connection:
    with _CACHE_LOCK:
        need_init = not os.path.exists(_CACHE_DB_PATH)
        conn = sqlite3.connect(_CACHE_DB_PATH, timeout=10)
        if need_init:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS jina_cache (
                    url TEXT PRIMARY KEY,
                    fetched_at REAL,
                    ttl_seconds REAL,
                    http_status INTEGER,
                    extracted_text TEXT,
                    final_url TEXT
                )
                """
            )
            conn.commit()
        return conn


def cache_get(url: str) -> Optional[Dict[str, Any]]:
    norm = normalize_url(url)
    conn = _cache_conn()
    cur = conn.execute(
        "SELECT url, fetched_at, ttl_seconds, http_status, extracted_text, final_url FROM jina_cache WHERE url=?",
        (norm,),
    )
    row = cur.fetchone()
    cur.close()
    if not row:
        return None
    fetched_at, ttl_seconds = row[1], row[2]
    if fetched_at is None or ttl_seconds is None:
        return None
    if (fetched_at + ttl_seconds) < time.time():
        return None
    return {
        "url": row[0],
        "fetched_at": fetched_at,
        "ttl_seconds": ttl_seconds,
        "http_status": row[3],
        "extracted_text": row[4] or "",
        "final_url": row[5] or norm,
    }


def cache_set(
    url: str,
    extracted_text: str,
    http_status: int,
    final_url: str,
    ttl_seconds: int,
) -> None:
    norm = normalize_url(url)
    conn = _cache_conn()
    conn.execute(
        "REPLACE INTO jina_cache (url, fetched_at, ttl_seconds, http_status, extracted_text, final_url) VALUES (?, ?, ?, ?, ?, ?)",
        (norm, time.time(), ttl_seconds, http_status, extracted_text, final_url or norm),
    )
    conn.commit()


def _respect_domain_delay(url: str) -> None:
    dom = _domain(url)
    if not dom:
        return
    last = _CACHE_DOMAIN_LAST_FETCH.get(dom, 0.0)
    delay = random.uniform(2.0, 4.0)
    now = time.time()
    if last and now - last < delay:
        time.sleep(delay - (now - last))
    _CACHE_DOMAIN_LAST_FETCH[dom] = time.time()


def fetch_text_cached(url: str, ttl_days: int = 14) -> Dict[str, Any]:
    norm = normalize_url(url)
    if norm in _CACHE_DEDUPE_RUN:
        cached = cache_get(norm)
        if cached:
            return cached
    cached = cache_get(norm)
    if cached:
        return cached

    _respect_domain_delay(norm)
    mirror = _mirror_url(norm) or f"https://r.jina.ai/{norm}"
    try:
        resp = _http_get(
            mirror,
            timeout=12,
            headers=_browser_headers(_domain(mirror)),
            rotate_user_agent=True,
            respect_block=False,
        )
        text = resp.text if resp and resp.text else ""
        status = resp.status_code if resp else 0
        final_url = getattr(resp, "url", norm) if resp else norm
    except Exception:
        text = ""
        status = 0
        final_url = norm
    ttl_seconds = int(ttl_days * 86400)
    cache_set(norm, text, status, final_url, ttl_seconds)
    _CACHE_DEDUPE_RUN.add(norm)
    return {
        "url": norm,
        "fetched_at": time.time(),
        "ttl_seconds": ttl_seconds,
        "http_status": status,
        "extracted_text": text,
        "final_url": final_url,
    }


def _decode_duckduckgo_link(raw: str) -> str:
    parsed = urlparse(raw)
    qs = dict(parse_qsl(parsed.query))
    target = qs.get("uddg") or ""
    return unquote(target) if target else raw


def jina_cached_search(query: str, *, max_results: int = 18, ttl_days: int = 14) -> List[str]:
    if not query:
        return []

    def _extract_hits(body: str, seen: Set[str]) -> List[str]:
        hits: List[str] = []
        for m in re.finditer(r"https?://duckduckgo\.com/l/\?[^\s\"]+", body):
            decoded = _decode_duckduckgo_link(html.unescape(m.group()))
            if decoded and decoded not in seen:
                seen.add(decoded)
                hits.append(decoded)
        for m in re.finditer(r"https?://[\w./?&%#=\-]+", body):
            candidate = html.unescape(m.group())
            if "duckduckgo.com" in candidate:
                continue
            if candidate not in seen:
                seen.add(candidate)
                hits.append(candidate)
        return hits

    hits: List[str] = []
    seen: Set[str] = set()
    offset = 0
    page = 0
    max_pages = max(1, min(5, (max_results + 9) // 10))

    while len(hits) < max_results and page < max_pages:
        try:
            params = {"q": query}
            if offset:
                params["s"] = str(offset)
            search_url = f"https://duckduckgo.com/html/?{urlencode(params)}"
            cached = fetch_text_cached(search_url, ttl_days=ttl_days)
            body = cached.get("extracted_text", "")
        except Exception:
            break

        if not body:
            break

        before = len(hits)
        hits.extend(_extract_hits(body, seen))
        if len(hits) >= max_results:
            break
        if len(hits) == before:
            break

        offset += 30
        page += 1

    return hits[:max_results]


# ───────────────────── contact candidate extraction & reranking ─────────────────────
_OPENAI_SPEC = importlib.util.find_spec("openai")
if _OPENAI_SPEC:
    import openai  # type: ignore
else:
    openai = None  # type: ignore


def _candidate_urls(agent: str, state: str, row_payload: Dict[str, Any]) -> List[str]:
    urls: List[str] = []
    zpid = str(row_payload.get("zpid", ""))
    rapid = rapid_property(zpid) if zpid else {}
    urls.extend(_rapid_profile_urls(rapid))
    hint_key = _normalize_override_key(agent, state)
    hint_urls = PROFILE_HINTS.get(hint_key) or PROFILE_HINTS.get(hint_key.lower(), [])
    urls.extend(hint_urls or [])
    city = row_payload.get("city", "")
    postal_code = row_payload.get("zip", "")
    brokerage = (row_payload.get("brokerageName") or row_payload.get("brokerage") or "").strip()
    domain_hint = (
        row_payload.get("domain_hint", "").strip()
        or _infer_domain_from_text(brokerage)
        or _infer_domain_from_text(agent)
    )

    site_targets: List[str] = []
    if domain_hint:
        site_targets.append(domain_hint)
    for site in CONTACT_SITE_PRIORITY:
        if site not in site_targets:
            site_targets.append(site)
    for site in ALT_PHONE_SITES:
        if site not in site_targets:
            site_targets.append(site)

    queries = _dedupe_queries(
        [
            *build_q_phone(
                agent,
                state,
                city=city,
                postal_code=postal_code,
                brokerage=brokerage,
            ),
            *build_q_email(
                agent,
                state,
                brokerage=brokerage,
                domain_hint=domain_hint,
                city=city,
                postal_code=postal_code,
                include_realtor_probe=True,
            ),
            *(
                f'"{agent}" {state} site:{site} phone'
                for site in site_targets
            ),
            *(
                f'"{agent}" {state} site:{site} email'
                for site in site_targets
            ),
        ]
    )
    search_hits: List[str] = []
    for q in queries[:12]:
        for link in jina_cached_search(q):
            search_hits.append(link)
    urls.extend(search_hits)
    return list(dict.fromkeys(urls))


def _extract_candidates_from_text(text: str, source_url: str) -> List[Dict[str, Any]]:
    candidates: List[Dict[str, Any]] = []
    if not text:
        return candidates
    seen: Set[Tuple[str, str]] = set()
    for m in PHONE_RE.finditer(text):
        phone = fmt_phone(m.group())
        if not (phone and valid_phone(phone)):
            continue
        snippet = text[max(0, m.start() - 120): m.end() + 120]
        key = ("phone", phone)
        if key in seen:
            continue
        seen.add(key)
        candidates.append(
            {
                "source_url": source_url,
                "evidence_snippet": " ".join(snippet.split()),
                "phones": [phone],
                "emails": [],
            }
        )
    for m in EMAIL_RE.finditer(text):
        email = clean_email(m.group())
        if not (email and ok_email(email)):
            continue
        snippet = text[max(0, m.start() - 120): m.end() + 120]
        key = ("email", email)
        if key in seen:
            continue
        seen.add(key)
        candidates.append(
            {
                "source_url": source_url,
                "evidence_snippet": " ".join(snippet.split()),
                "phones": [],
                "emails": [email],
            }
        )
    return candidates


def _score_contact_candidate(snippet: str, value: str, kind: str) -> float:
    low = snippet.lower()
    score = 1.0
    for good in ("cell", "mobile", "direct", "text"):
        if good in low:
            score += 1.2
    for bad in ("office", "main", "fax", "ext", "switchboard", "toll free"):
        if bad in low:
            score -= 0.8
    if kind == "email" and _is_generic_email(value):
        score -= 0.5
    return score


def _heuristic_rerank(candidates: List[Dict[str, Any]]) -> Dict[str, Any]:
    best_phone = ("", 0.0, "", "")
    best_email = ("", 0.0, "", "")
    for cand in candidates:
        snippet = cand.get("evidence_snippet", "")
        url = cand.get("source_url", "")
        for phone in cand.get("phones", []):
            score = _score_contact_candidate(snippet, phone, "phone")
            if score > best_phone[1]:
                best_phone = (phone, score, url, snippet)
        for email in cand.get("emails", []):
            score = _score_contact_candidate(snippet, email, "email")
            if score > best_email[1]:
                best_email = (email, score, url, snippet)
    return {
        "best_phone": best_phone[0],
        "best_phone_confidence": max(0, min(100, int(best_phone[1] * 18))),
        "best_phone_source_url": best_phone[2],
        "best_phone_evidence": best_phone[3],
        "best_email": best_email[0],
        "best_email_confidence": max(0, min(100, int(best_email[1] * 18))),
        "best_email_source_url": best_email[2],
        "best_email_evidence": best_email[3],
    }


def _openai_rerank(candidates: List[Dict[str, Any]], agent: str) -> Optional[Dict[str, Any]]:
    if not openai or not os.getenv("OPENAI_API_KEY"):
        return None
    items = []
    for cand in candidates:
        items.append(
            {
                "source_url": cand.get("source_url", ""),
                "snippet": cand.get("evidence_snippet", "")[:600],
                "phones": cand.get("phones", []),
                "emails": cand.get("emails", []),
            }
        )
    prompt = (
        "You are Rina, a diligent contact info reranker. "
        "Given extracted snippets, choose the best direct mobile/cell phone and direct email for agent "
        f"{agent}. Prefer numbers/emails labeled cell, mobile, direct or text. Downrank office/main/fax/ext/switchboard/toll free. "
        "Prefer personal style emails (first.last or gmail) over generic info/office/hello. "
        "Respond with strict JSON with keys best_phone, best_phone_confidence (0-100), best_phone_source_url, best_phone_evidence, "
        "best_email, best_email_confidence (0-100), best_email_source_url, best_email_evidence."
    )
    try:
        resp = openai.ChatCompletion.create(
            model=os.getenv("OPENAI_RERANK_MODEL", "gpt-4o-mini"),
            temperature=0.0,
            messages=[
                {"role": "system", "content": prompt},
                {
                    "role": "user",
                    "content": json.dumps(items) if items else "[]",
                },
            ],
        )
        content = resp["choices"][0]["message"]["content"] if resp else ""
        data = json.loads(content or "{}") if content else {}
        required_keys = {
            "best_phone",
            "best_phone_confidence",
            "best_phone_source_url",
            "best_phone_evidence",
            "best_email",
            "best_email_confidence",
            "best_email_source_url",
            "best_email_evidence",
        }
        if not required_keys.issubset(data):
            return None
        return data
    except Exception:
        return None


def _rina_rerank(candidates: List[Dict[str, Any]], agent: str) -> Optional[Dict[str, Any]]:
    """Apply an LLM-based reranker ("Rina") when available.

    Falls back to the OpenAI helper when installed, otherwise returns ``None``
    so heuristic reranking can take over.
    """

    try:
        return _openai_rerank(candidates, agent)
    except Exception:
        return None


def rerank_contact_candidates(candidates: List[Dict[str, Any]], agent: str) -> Dict[str, Any]:
    if not candidates:
        return {
            "best_phone": "",
            "best_phone_confidence": 0,
            "best_phone_source_url": "",
            "best_phone_evidence": "",
            "best_email": "",
            "best_email_confidence": 0,
            "best_email_source_url": "",
            "best_email_evidence": "",
        }
    ai_choice = _rina_rerank(candidates, agent)
    if ai_choice:
        return ai_choice
    return _heuristic_rerank(candidates)


def _candidate_quality(candidates: List[Dict[str, Any]]) -> Dict[str, Any]:
    phone_snippets: List[str] = []
    phones: List[str] = []
    emails: List[str] = []
    office_terms = {"office", "main", "fax", "switchboard", "toll free"}
    mobile_terms = {"cell", "mobile", "text", "direct"}

    for cand in candidates:
        snippet = cand.get("evidence_snippet", "").lower()
        if snippet:
            phone_snippets.append(snippet)
        for phone in cand.get("phones", []):
            phones.append(phone)
        for email in cand.get("emails", []):
            emails.append(email)

    def _all_office(snips: List[str]) -> bool:
        if not snips:
            return False
        all_office = True
        for snip in snips:
            has_office = any(term in snip for term in office_terms)
            has_mobile = any(term in snip for term in mobile_terms)
            if not has_office or has_mobile:
                all_office = False
                break
        return all_office

    return {
        "phones_found": len(phones),
        "emails_found": len(emails),
        "all_office": _all_office(phone_snippets),
        "all_generic_email": bool(emails) and all(_is_generic_email(e) for e in emails),
    }


def _fallback_jina_queries(agent: str, state: str, row_payload: Dict[str, Any]) -> List[str]:
    brokerage = (row_payload.get("brokerageName") or row_payload.get("brokerage") or "").strip()
    city = str(row_payload.get("city", "")).strip()
    domain_hint = (
        row_payload.get("domain_hint", "").strip()
        or _infer_domain_from_text(brokerage)
        or _infer_domain_from_text(agent)
    )

    site_targets: List[str] = []
    if domain_hint:
        site_targets.append(domain_hint)
    for site in CONTACT_SITE_PRIORITY:
        if site not in site_targets:
            site_targets.append(site)
    for site in ALT_PHONE_SITES:
        if site not in site_targets:
            site_targets.append(site)

    variants = [
        f"{agent} realtor {state} phone email",
        f"{agent} Realtor {state} {brokerage} contact".strip(),
        f"{agent} {city} {state} realtor cell".strip(),
        f"{agent} {state} {brokerage} email".strip(),
        f"{agent} {state} contact email".strip(),
    ]

    for site in site_targets:
        variants.append(f'"{agent}" {state} site:{site} phone')
        variants.append(f'"{agent}" {state} site:{site} contact email')

    return [v for v in _dedupe_queries(variants) if v]


def enrich_contact(agent: str, state: str, row_payload: Dict[str, Any]) -> Dict[str, Any]:
    urls = _candidate_urls(agent, state, row_payload)
    candidates: List[Dict[str, Any]] = []

    def _collect_from_urls(urls_to_fetch: Iterable[str]) -> None:
        for url in urls_to_fetch:
            if not url:
                continue
            fetched = fetch_text_cached(url)
            text = fetched.get("extracted_text", "")
            candidates.extend(_extract_candidates_from_text(text, fetched.get("final_url") or url))

    # RapidAPI emails are trusted and accepted immediately.
    zpid = str(row_payload.get("zpid", ""))
    rapid = rapid_property(zpid) if zpid else {}
    rapid_emails: List[str] = []
    if rapid:
        lb = rapid.get("listed_by") or {}
        rapid_emails.extend(_emails_from_block(lb))
        for blk in rapid.get("contact_recipients", []) or []:
            rapid_emails.extend(_emails_from_block(blk))
    rapid_emails = [e for e in rapid_emails if e]
    if rapid_emails:
        best = rapid_emails[0]
        return {
            "best_phone": "",
            "best_phone_confidence": 0,
            "best_phone_source_url": "",
            "best_phone_evidence": "",
            "best_email": best,
            "best_email_confidence": 95,
            "best_email_source_url": "rapidapi",
            "best_email_evidence": "rapidapi listing contact",
        }

    _collect_from_urls(urls)
    quality = _candidate_quality(candidates)
    needs_fallback = (
        (quality["phones_found"] == 0 and quality["emails_found"] == 0)
        or (quality["phones_found"] > 0 and quality["all_office"])
        or (quality["emails_found"] > 0 and quality["all_generic_email"])
    )

    if needs_fallback:
        fallback_urls: List[str] = []
        for fq in _fallback_jina_queries(agent, state, row_payload):
            fallback_urls.extend(jina_cached_search(fq))
        _collect_from_urls(fallback_urls)

    return rerank_contact_candidates(candidates, agent)


def _contact_enrichment(agent: str, state: str, row_payload: Dict[str, Any]) -> Dict[str, Any]:
    cache_key = "_contact_enrichment"
    if cache_key not in row_payload:
        row_payload[cache_key] = enrich_contact(agent, state, row_payload)
    return row_payload.get(cache_key, {})


def fetch_contact_page(url: str) -> Tuple[str, bool]:
    if not _should_fetch(url, strict=False):
        return "", False
    dom = _domain(url)
    blocked = False
    proxy_url = _proxy_for_domain(dom)
    tries = len(_CONTACT_FETCH_BACKOFFS)
    if dom in _REALTOR_DOMAINS:
        tries = max(tries, _REALTOR_MAX_RETRIES)

    def _fallback(reason: str) -> Tuple[str, bool]:
        mirror = _mirror_url(url)
        if mirror:
            try:
                mirror_resp = _http_get(
                    mirror,
                    timeout=10,
                    headers=_browser_headers(_domain(mirror)),
                    rotate_user_agent=True,
                    respect_block=False,
                    proxy=proxy_url,
                )
                if mirror_resp.status_code == 200 and mirror_resp.text.strip():
                    LOG.info("MIRROR FALLBACK used for %s (%s)", dom, reason)
                    return mirror_resp.text, True
            except Exception as exc:
                LOG.debug("mirror fetch failed %s on %s", exc, mirror)
        if HEADLESS_ENABLED and sync_playwright:
            rendered = _headless_fetch(url, proxy_url=proxy_url, domain=dom)
            if rendered.strip():
                LOG.info(
                    "BROWSER FALLBACK used for %s (proxy=%s reason=%s)",
                    dom,
                    bool(proxy_url),
                    reason,
                )
                return rendered, True
        return "", False

    last_seen = _CONTACT_DOMAIN_LAST_FETCH.get(dom, 0.0)
    if CONTACT_DOMAIN_MIN_GAP > 0 and last_seen:
        gap = time.time() - last_seen
        min_gap = CONTACT_DOMAIN_MIN_GAP
        if gap < min_gap:
            sleep_for = min_gap - gap
            if CONTACT_DOMAIN_GAP_JITTER > 0:
                sleep_for += random.uniform(0.25, CONTACT_DOMAIN_GAP_JITTER)
            time.sleep(max(0.0, sleep_for))
    attempt = 0
    while attempt < tries:
        attempt += 1
        delay = _CONTACT_FETCH_BACKOFFS[min(attempt - 1, len(_CONTACT_FETCH_BACKOFFS) - 1)]
        if _blocked(dom):
            blocked = True
            LOG.warning("BLOCK cached -> abort %s/%s", attempt, tries)
            break
        if delay:
            time.sleep(delay)
        try:
            try:
                resp = _http_get(
                    url,
                    timeout=10,
                    headers=_browser_headers(dom),
                    rotate_user_agent=True,
                    proxy=proxy_url,
                )
            except requests.HTTPError as exc:
                resp = exc.response
                if resp is None:
                    raise
        except Exception as exc:
            LOG.debug("fetch_contact_page error %s on %s", exc, url)
            if isinstance(exc, _CONNECTION_ERRORS):
                blocked = True
                _mark_block(dom)
                LOG.warning("BLOCK connect -> abort %s/%s", attempt, tries)
                break
            break
        _CONTACT_DOMAIN_LAST_FETCH[dom] = time.time()
        status = resp.status_code
        body = (resp.text or "").strip() if resp is not None else ""
        if status == 200 and body:
            if proxy_url:
                LOG.info("CONTACT fetched via proxy %s (%s)", _redact_proxy(proxy_url), dom)
            return body, False
        if status == 403 or (status == 200 and not body):
            blocked = True
            _mark_block(dom)
            LOG.warning("BLOCK %s -> attempt headless for %s/%s", status, attempt, tries)
            html, used_fallback = _fallback("403")
            if html:
                return html, used_fallback
            break
        if status == 429:
            blocked = True
            if dom in _REALTOR_DOMAINS:
                delay = min(
                    _REALTOR_BACKOFF_BASE * (1.8 ** (attempt - 1)),
                    _REALTOR_BACKOFF_CAP,
                )
                delay += random.uniform(0, _REALTOR_BACKOFF_JITTER)
                LOG.warning(
                    "Realtor.com throttled (429) attempt %s/%s; backing off %.1fs",
                    attempt,
                    tries,
                    delay,
                )
                time.sleep(delay)
                continue
            _mark_block(dom)
            LOG.warning("BLOCK 429 -> abort %s/%s", attempt, tries)
            break
        if status in (301, 302) and resp.headers.get("Location"):
            url = resp.headers["Location"]
            continue
        if status in (403, 451):
            blocked = True
            _mark_block(dom)
            LOG.warning("BLOCK %s -> abort %s/%s", status, attempt, tries)
            break
        break

    if blocked:
        html, used_fallback = _fallback("blocked")
        if html:
            return html, used_fallback
    return "", False

def _headless_fetch(url: str, *, proxy_url: str = "", domain: str = "") -> str:
    if not HEADLESS_ENABLED or not sync_playwright:
        return ""
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=["--disable-blink-features=AutomationControlled"],
            )
            accept_language = random.choice(_ACCEPT_LANGUAGE_POOL)
            context = browser.new_context(
                user_agent=random.choice(_USER_AGENT_POOL),
                locale=accept_language.split(",")[0],
                extra_http_headers={
                    "Accept-Language": accept_language,
                    **_random_cookie_header(),
                },
                proxy={"server": proxy_url} if proxy_url else None,
            )
            page = context.new_page()
            page.goto(url, wait_until="domcontentloaded", timeout=HEADLESS_TIMEOUT_MS)
            page.wait_for_timeout(HEADLESS_WAIT_MS)
            content = page.content() or ""
            context.close()
            browser.close()
            if not content.strip():
                return ""
            LOG.debug(
                "Headless fetch ok for %s (proxy=%s)",
                domain or _domain(url),
                bool(proxy_url),
            )
            return content
    except Exception as exc:  # pragma: no cover - network/env specific
        LOG.warning("Headless fetch failed for %s (%s)", domain or _domain(url), exc)
        return ""

# ───────────────────── Google CSE helpers ─────────────────────

_cse_cache: Dict[str, List[Dict[str, Any]]] = {}
_last_cse_ts = 0.0
_cse_lock = threading.Lock()
_cse_recent: deque[float] = deque()


def _cse_key(q: str) -> str:
    return re.sub(r"\s+", " ", q or "").strip().lower()


def _pick_cse_cred(prev_idx: Optional[int], *, advance: bool = False) -> Tuple[str, str, int]:
    global _CSE_CRED_INDEX
    if not _CSE_CRED_POOL:
        raise RuntimeError("No Google CSE credentials configured")
    with _cse_lock:
        if prev_idx is None:
            idx = _CSE_CRED_INDEX
        elif advance:
            idx = (prev_idx + 1) % len(_CSE_CRED_POOL)
        else:
            idx = prev_idx
        _CSE_CRED_INDEX = idx
        key, cx = _CSE_CRED_POOL[idx]
    return key, cx, idx


def _dedupe_queries(queries: Iterable[str]) -> List[str]:
    seen: Set[str] = set()
    uniq: List[str] = []
    for q in queries:
        norm = _cse_key(q)
        if not norm or norm in seen:
            continue
        seen.add(norm)
        uniq.append(q)
    return uniq

def google_items(q: str, tries: int = 3) -> List[Dict[str, Any]]:
    links = jina_cached_search(q, max_results=10)
    return [{"link": link} for link in links if link]


# ───────────────────── alternate search helpers ─────────────────────

def duckduckgo_search(query: str, limit: int = 5) -> List[Dict[str, Any]]:
    links = jina_cached_search(query, max_results=limit)
    return [{"link": link} for link in links if link]


def bing_search(query: str, limit: int = 5) -> List[Dict[str, Any]]:
    links = jina_cached_search(query, max_results=limit)
    return [{"link": link} for link in links if link]


def search_round_robin(queries: Iterable[str], per_query_limit: int = 4) -> List[List[Tuple[str, List[Dict[str, Any]]]]]:
    engines: List[Tuple[str, Any]] = [
        ("jina", lambda q, limit: duckduckgo_search(q, limit=limit)),
    ]

    deduped = _dedupe_queries(queries)
    results: List[List[Tuple[str, List[Dict[str, Any]]]]] = []
    if not deduped:
        return results

    for idx, q in enumerate(deduped):
        start = idx % len(engines)
        ordered = engines[start:] + engines[:start]
        attempts: List[Tuple[str, List[Dict[str, Any]]]] = []
        for name, fn in ordered:
            hits = fn(q, per_query_limit)
            attempts.append((name, hits))
            if hits:
                break
        results.append(attempts)
    return results

# ───────────────────── page‑parsing helpers ─────────────────────
def extract_struct(td: str) -> Tuple[List[str], List[str], List[Dict[str, Any]], Dict[str, Any]]:
    phones, mails, meta = [], [], []
    info: Dict[str, Any] = {"title": "", "mailto": [], "tel": []}
    if not BeautifulSoup:
        return phones, mails, meta, info

    soup = BeautifulSoup(td, "html.parser")
    if soup.title and soup.title.string:
        info["title"] = soup.title.string.strip()

    def _as_list(value: Any) -> List[Any]:
        if value is None:
            return []
        if isinstance(value, list):
            return value
        return [value]

    def _contact_values(node: Dict[str, Any], key: str) -> List[Any]:
        values: List[Any] = []
        for cp in _as_list(node.get("contactPoint")):
            if isinstance(cp, dict):
                val = cp.get(key)
                if isinstance(val, list):
                    values.extend(val)
                elif val:
                    values.append(val)
        return values

    def _add_phone(raw_value: Any, collected: List[str]) -> None:
        formatted = fmt_phone(str(raw_value))
        if formatted:
            phones.append(formatted)
            collected.append(formatted)

    def _add_email(raw_value: Any, collected: List[str]) -> None:
        cleaned = clean_email(str(raw_value))
        if cleaned and ok_email(cleaned):
            mails.append(cleaned)
            collected.append(cleaned)

    for sc in soup.find_all("script", {"type": "application/ld+json"}):
        try:
            data = json.loads(sc.string or "")
        except Exception:
            continue
        if isinstance(data, list):
            if not data:
                continue
            # Zillow pages often wrap the Person node in an array; iterate all.
            nodes = [d for d in data if isinstance(d, dict)]
        else:
            nodes = [data] if isinstance(data, dict) else []
        for node in nodes:
            entry: Dict[str, Any] = {"raw": node}
            collected_phones: List[str] = []
            for tel_val in _as_list(node.get("telephone")) + _contact_values(node, "telephone"):
                _add_phone(tel_val, collected_phones)
            collected_emails: List[str] = []
            for mail_val in _as_list(node.get("email")) + _contact_values(node, "email"):
                _add_email(mail_val, collected_emails)
            if collected_phones:
                entry["phones"] = collected_phones
            if collected_emails:
                entry["emails"] = collected_emails
            node_type = node.get("@type")
            if node_type:
                entry["type"] = node_type
            if node.get("name"):
                entry["name"] = node.get("name")
            if entry.keys() - {"raw"}:
                meta.append(entry)

    def _context_for(node) -> str:
        parent = node.find_parent()
        target = parent or node
        snippet = target.get_text(" ", strip=True)
        return " ".join(snippet.split())[:280]

    for a in soup.select('a[href^="tel:"]'):
        tel_val = a.get("href", "").split("tel:")[-1]
        formatted = fmt_phone(tel_val)
        if formatted:
            phones.append(formatted)
            info["tel"].append({
                "phone": formatted,
                "context": _context_for(a).lower(),
            })

    seen_emails: Set[str] = set()

    for a in soup.select('a[href^="mailto:"]'):
        mail_val = a.get("href", "").split("mailto:")[-1]
        cleaned = clean_email(mail_val)
        if cleaned and ok_email(cleaned) and cleaned not in seen_emails:
            mails.append(cleaned)
            seen_emails.add(cleaned)
            info["mailto"].append({
                "email": cleaned,
                "context": _context_for(a).lower(),
            })

    for cf_node in soup.select("[data-cfemail]"):
        decoded = decode_cfemail(cf_node.get("data-cfemail", ""))
        cleaned = clean_email(decoded)
        if cleaned and ok_email(cleaned) and cleaned not in seen_emails:
            mails.append(cleaned)
            seen_emails.add(cleaned)
            info["mailto"].append({
                "email": cleaned,
                "context": _context_for(cf_node).lower(),
            })

    soup.decompose()
    return phones, mails, meta, info

def proximity_scan(t: str, first_name: str = "", last_name: str = ""):
    out: Dict[str, Dict[str, Any]] = {}
    for m in PHONE_RE.finditer(t):
        p = fmt_phone(m.group())
        if not valid_phone(p):
            continue
        snippet = t[max(m.start() - 120, 0): m.end() + 120]
        snippet_low = snippet.lower()
        has_first = bool(first_name and first_name in snippet_low)
        has_last = bool(last_name and last_name in snippet_low)
        lab_match = LABEL_RE.search(snippet_low)
        lab = lab_match.group().lower() if lab_match else ""
        w = LABEL_TABLE.get(lab, 0)
        if w < 1 and has_first and has_last:
            # Allow plain numbers that appear right next to the agent's
            # full name even if there is no explicit "Cell"/"Phone" label.
            w = 3
        if w < 1:
            continue
        if not has_last:
            if not has_first:
                continue
            if w < 3:
                continue
        entry = out.setdefault(
            p,
            {
                "weight": 0,
                "score": 0.0,
                "office": False,
                "snippets": [],
            },
        )
        entry["weight"] = max(entry["weight"], w)
        entry["score"] += 2 + w
        entry["office"] = entry["office"] or lab in ("office", "main")
        entry["snippets"].append(" ".join(snippet.split()))
    return out

def _compact_tokens(*parts: str) -> str:
    return " ".join(p.strip() for p in parts if p and p.strip()).strip()


def build_q_phone(
    name: str,
    state: str,
    *,
    city: str = "",
    postal_code: str = "",
    brokerage: str = "",
) -> List[str]:
    queries: List[str] = []

    def _add(q: str) -> None:
        if q and q not in queries:
            queries.append(q)

    localized_base = _compact_tokens(f'"{name}"', city, state, postal_code)
    state_base = _compact_tokens(f'"{name}"', state)
    name_only = f'"{name}"'.strip()

    for base in (localized_base, state_base, name_only):
        if not base:
            continue
        _add(f"{base} realtor phone")
        _add(f"{base} real estate cell")
        _add(f"{base} mobile")

    if brokerage:
        _add(f'"{name}" "{brokerage}" phone')
        _add(f'"{brokerage}" {state} "phone"')

    return queries


def build_alt_q_phone(
    name: str,
    state: str,
    *,
    brokerage: str = "",
    extras: Optional[List[str]] = None,
) -> List[str]:
    alt_queries: List[str] = []
    base = f'"{name}" {state}'.strip()

    def _add(q: str) -> None:
        if q and q not in alt_queries:
            alt_queries.append(q)

    for site in ALT_PHONE_SITES:
        _add(f"{base} site:{site} phone")
        _add(f"{base} site:{site} contact")
    if brokerage:
        _add(f'"{name}" "{brokerage}" phone')
        _add(f'"{brokerage}" "{state}" "phone"')
    for extra in extras or []:
        extra = extra.strip()
        if not extra:
            continue
        _add(f'"{name}" "{extra}" phone')
        _add(f'"{name}" "{extra}" contact')
    return alt_queries

def build_q_email(
    name: str,
    state: str,
    brokerage: str = "",
    domain_hint: str = "",
    mls_id: str = "",
    *,
    city: str = "",
    postal_code: str = "",
    include_realtor_probe: bool = False,
) -> List[str]:
    queries: List[str] = []

    def _add(q: str) -> None:
        if q and q not in queries:
            queries.append(q)

    localized_base = _compact_tokens(f'"{name}"', city, state, postal_code)
    state_base = _compact_tokens(f'"{name}"', state)

    for base in (localized_base, state_base):
        if not base:
            continue
        _add(f"{base} realtor email")
        _add(f"{base} real estate email")
        _add(f"{base} contact email")
        _add(f"{base} email address")

    parts = [p for p in name.split() if p]
    if len(parts) >= 2:
        first, last = parts[0], parts[-1]
        _add(f'"{first} {last}" "email" {state}'.strip())
        if brokerage:
            _add(f'"{last}" "{brokerage}" email')

    if brokerage:
        _add(f'"{name}" "{brokerage}" email')
        _add(f'"{name}" "{brokerage}" "contact"')
        if parts:
            _add(f'"{brokerage}" "{parts[-1]}" email')
        if state:
            _add(f'"{brokerage}" "{state}" "email"')

    if domain_hint:
        _add(f'site:{domain_hint} "{name}" email')
        _add(f'"{name}" "@{domain_hint}"')

    if mls_id and parts:
        _add(f'"{mls_id}" "{parts[-1]}" email')
        _add(f'"{name}" "{mls_id}" email')
        if brokerage:
            _add(f'"{mls_id}" "{brokerage}" email')

    if include_realtor_probe:
        _add(f'"{name}" realtor.com email')
        if state:
            _add(f'"{name}" realtor.com email {state}')

    return queries

# nickname mapping for email‑matching heuristic
_NICK_MAP = {
    "bob": "robert", "rob": "robert", "bobby": "robert",
    "bill": "william", "will": "william", "billy": "william", "liam": "william",
    "liz": "elizabeth", "beth": "elizabeth", "lisa": "elizabeth",
    "tom": "thomas", "tommy": "thomas",
    "dave": "david",
    "jim": "james", "jimmy": "james", "jamie": "james",
    "mike": "michael",
    "rick": "richard", "rich": "richard", "dick": "richard",
    "jen": "jennifer", "jenny": "jennifer", "jenn": "jennifer",
    "andy": "andrew", "drew": "andrew",
    "pepe": "jose", "chepe": "jose", "josé": "jose",
    "toni": "antonio", "tony": "antonio",
    "paco": "francisco", "pancho": "francisco", "fran": "francisco", "frank": "francisco",
    "chuy": "jesus",
    "lupe": "guadalupe", "lupita": "guadalupe",
    "alex": "alexander", "sandy": "alexandra", "sandra": "alexandra",
    "ricki": "ricardo", "ricky": "ricardo", "richie": "richard",
}


def _token_in_text(text: str, token: str) -> bool:
    if not token:
        return False
    return bool(re.search(rf"\b{re.escape(token)}\b", text))


def _normalize_location_token(token: str) -> str:
    return re.sub(r"[^a-z0-9]", "", token.lower())


def _collect_location_hints(
    row_payload: Dict[str, Any],
    state: str,
    *extra_texts: str,
) -> Tuple[Set[str], Set[str]]:
    tokens: Set[str] = set()
    digits: Set[str] = set()

    def add_token(value: str, *, keep_short: bool = False) -> None:
        norm = _normalize_location_token(value)
        if not norm:
            return
        if norm.isdigit():
            if len(norm) >= 4:
                digits.add(norm)
            return
        if len(norm) < 3 and not keep_short:
            return
        tokens.add(norm)

    def add_text(value: Any) -> None:
        if not value:
            return
        text = html.unescape(str(value))
        for part in re.split(r"[\s,;|/]+", text):
            if part:
                add_token(part)

    if state:
        add_token(state, keep_short=True)
        state_name = STATE_ABBR_TO_NAME.get(state.upper())
        if state_name:
            add_text(state_name)

    add_text(row_payload.get("city"))
    add_text(row_payload.get("stateFull") or row_payload.get("state_name"))
    add_text(row_payload.get("county") or row_payload.get("countyName"))

    for extra in extra_texts:
        add_text(extra)

    return tokens, digits


def _page_has_location(page_text: str, tokens: Set[str], digits: Set[str]) -> bool:
    if not (tokens or digits):
        return True
    low = page_text.lower()
    if any(_token_in_text(low, tok) for tok in tokens if tok):
        return True
    if any(d and d in low for d in digits):
        return True
    return False


def _first_last_tokens(name: str) -> Tuple[Set[str], str]:
    parts = [p for p in name.split() if p]
    if not parts:
        return set(), ""
    first_raw = re.sub(r"[^a-z]", "", parts[0].lower())
    last_part = parts[-1] if len(parts) > 1 else parts[0]
    last_raw = re.sub(r"[^a-z]", "", last_part.lower())
    first_variants = {_ for _ in _token_variants(first_raw) if _}
    return first_variants, last_raw


def _token_variants(tok: str) -> Set[str]:
    tok = tok.lower()
    out = {tok}
    if tok in _NICK_MAP:
        out.add(_NICK_MAP[tok])
    for k, v in _NICK_MAP.items():
        if tok == v:
            out.add(k)
    return out

def _email_matches_name(agent: str, email: str) -> bool:
    local, domain = email.split("@", 1)
    local = local.lower()
    domain_l = domain.lower()
    tks = [re.sub(r"[^a-z]", "", w.lower()) for w in agent.split() if w]
    if not tks:
        return False
    first, last = tks[0], tks[-1]
    segments = [seg for seg in re.split(r"[._\-]+", local) if seg]
    for tk in tks:
        if len(tk) >= 3 and tk in local:
            return True
        for var in _token_variants(tk):
            if len(var) >= 3 and var in local:
                return True
            for seg in segments:
                if len(seg) >= 2 and (seg.startswith(var) or var.startswith(seg)):
                    return True
    if first and last and (
        first[0] + last in local
        or first + last[0] in local
        or last + first[0] in local
    ):
        return True
    if last and last in domain_l and (not first or first in domain_l or first[0] in domain_l):
        return True
    return False

domain_patterns: Dict[str, str] = {}
_MX_CACHE: Dict[str, bool] = {}
_contact_override_cache: Dict[str, Any] = {"raw": None, "map": {}}


def _normalize_override_key(agent: str, state: str) -> str:
    agent_part = str(agent).strip().lower()
    state_part = str(state).strip().upper()
    return f"{agent_part}|{state_part}"


def _load_contact_overrides() -> Dict[str, Dict[str, str]]:
    global _contact_override_cache
    raw = os.getenv("CONTACT_OVERRIDE_JSON", CONTACT_OVERRIDE_JSON).strip()
    if raw == _contact_override_cache.get("raw"):
        return _contact_override_cache.get("map", {})

    overrides: Dict[str, Dict[str, str]] = {}
    if raw:
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            LOG.warning("Invalid CONTACT_OVERRIDE_JSON payload; ignoring")
        else:
            if isinstance(data, dict):
                for key, val in data.items():
                    if not isinstance(val, dict):
                        continue
                    if all(isinstance(v, dict) for v in val.values()):
                        for state, payload in val.items():
                            if not isinstance(payload, dict):
                                continue
                            cleaned = {
                                k: str(v) for k, v in payload.items() if k in {"phone", "email"} and v
                            }
                            if cleaned:
                                overrides[_normalize_override_key(str(key), str(state))] = cleaned
                    else:
                        cleaned = {k: str(v) for k, v in val.items() if k in {"phone", "email"} and v}
                        if cleaned:
                            if "|" in str(key):
                                agent_key, state_key = str(key).split("|", 1)
                            else:
                                agent_key, state_key = str(key), ""
                            norm_key = _normalize_override_key(agent_key, state_key)
                            overrides[norm_key] = cleaned

    _contact_override_cache = {"raw": raw, "map": overrides}
    return overrides


def _contact_override(agent: str, state: str) -> Dict[str, str]:
    overrides = _load_contact_overrides()
    key = _normalize_override_key(agent, state)
    return overrides.get(key, {})


def _agent_tokens(name: str) -> List[str]:
    return [re.sub(r"[^a-z]", "", part.lower()) for part in name.split() if len(part) > 1]


def _page_is_contactish(url: str, title: str = "") -> bool:
    low_url = url.lower()
    low_title = title.lower() if title else ""
    return any(h in low_url for h in CONTACT_PAGE_HINTS) or (
        low_title and any(h in low_title for h in CONTACT_PAGE_HINTS)
    )

def _pattern_from_example(addr: str, name: str) -> str:
    first, last = map(lambda s: re.sub(r"[^a-z]", "", s.lower()), (name.split()[0], name.split()[-1]))
    local, _ = addr.split("@", 1)
    if local == f"{first}{last}":
        return "{first}{last}"
    if local == f"{first}.{last}":
        return "{first}.{last}"
    if local == f"{first[0]}{last}":
        return "{fi}{last}"
    if local == f"{first}.{last[0]}":
        return "{first}{li}"
    return ""

def _synth_email(name: str, domain: str) -> str:
    patt = domain_patterns.get(domain)
    if not patt:
        return ""
    first, last = map(lambda s: re.sub(r"[^a-z]", "", s.lower()), (name.split()[0], name.split()[-1]))
    fi, li = first[0], last[0]
    local = patt.format(first=first, last=last, fi=fi, li=li)
    return f"{local}@{domain}"

def _has_mx(domain: str) -> bool:
    if domain in _MX_CACHE:
        return _MX_CACHE[domain]
    if not dns:
        _MX_CACHE[domain] = True
        return True
    try:
        answers = dns.resolve(domain, "MX")  # type: ignore[attr-defined]
        _MX_CACHE[domain] = bool(answers)
    except Exception:
        _MX_CACHE[domain] = False
    return _MX_CACHE[domain]

def _synth_from_tokens(name: str, domains: Set[str]) -> List[str]:
    parts = [re.sub(r"[^a-z]", "", p.lower()) for p in name.split() if p]
    if len(parts) < 2 or not domains:
        return []
    first, last = parts[0], parts[-1]
    combos = [
        f"{first}.{last}",
        f"{first}{last}",
        f"{first[0]}{last}",
        f"{first}{last[0]}",
    ]
    emails: List[str] = []
    for dom in domains:
        dom = dom.lower().strip()
        if not dom or not _has_mx(dom):
            continue
        for local in combos:
            addr = f"{local}@{dom}"
            if ok_email(addr):
                emails.append(addr)
    return list(dict.fromkeys(emails))

def _guess_domain_from_brokerage(brokerage: str) -> str:
    if not brokerage:
        return ""
    tokens = {
        re.sub(r"[^a-z]", "", part.lower())
        for part in brokerage.split()
        if len(part) > 2
    }
    tokens.discard("")
    if not tokens:
        return ""
    best_domain = ""
    best_hits = 0
    for dom in domain_patterns.keys():
        dom_tokens = {seg for seg in re.split(r"[^a-z]", dom.lower()) if seg}
        hits = len(tokens & dom_tokens)
        if hits > best_hits:
            best_domain = dom
            best_hits = hits
    return best_domain if best_hits else ""


def _infer_domain_from_text(value: str) -> str:
    """Derive a plausible custom domain from free-form text (e.g., brokerage name).

    Example: "The Campos Group" → "thecamposgroup.com".
    Returns an empty string when a confident slug cannot be built.
    """
    if not value:
        return ""
    tokens = [re.sub(r"[^a-z0-9]", "", part.lower()) for part in value.split()]
    tokens = [tok for tok in tokens if len(tok) >= 3]
    if len(tokens) < 2:
        return ""
    slug = "".join(tokens)
    if len(slug) < 5:
        return ""
    return f"{slug}.com"

def _split_portals(urls):
    portals, non = [], []
    for u in urls:
        (portals if any(d in u for d in SCRAPE_SITES) else non).append(u)
    return non, portals


def _extract_remax_emails(page: str) -> Set[str]:
    emails: Set[str] = set()
    if not page:
        return emails
    soup = None
    if BeautifulSoup:
        try:
            soup = BeautifulSoup(page, "html.parser")
        except Exception:
            soup = None
    if soup:
        for attr in ("data-email", "data-agent-email", "data-agentemail", "data-contact-email", "data-vcard-email"):
            for node in soup.select(f"[{attr}]"):
                raw = node.get(attr) or ""
                cleaned = clean_email(html.unescape(raw))
                if cleaned and ok_email(cleaned):
                    emails.add(cleaned)
    attr_pattern = re.compile(r"data-(?:agent-)?email\s*=\s*['\"]([^'\"]+@[^'\"]+)['\"]", re.I)
    for match in attr_pattern.finditer(page):
        cleaned = clean_email(html.unescape(match.group(1)))
        if cleaned and ok_email(cleaned):
            emails.add(cleaned)
    json_pattern = re.compile(r'"(?:agentEmail|email|contactEmail)"\s*:\s*"([^"@\s]+@[^"\s]+)"', re.I)
    for match in json_pattern.finditer(page):
        raw = match.group(1).replace("mailto:", "")
        cleaned = clean_email(html.unescape(raw))
        if cleaned and ok_email(cleaned):
            emails.add(cleaned)
    return emails


EMAIL_SOURCE_BASE = {
    "payload_contact": 1.0,
    "rapid_contact": 0.95,
    "rapid_listed_by": 0.95,
    "jsonld_person": 1.0,
    "jsonld_other": 0.7,
    "mailto": 0.8,
    "dom": 0.6,
    "pattern": 0.5,
    "cse_contact": 0.7,
    "remax_struct": 0.85,
}


BROKERAGE_EMAIL_DOMAINS = {"remax.com", "remax.net", "kw.com", "kellerwilliams.com"}


def _is_generic_email(email: str) -> bool:
    local, domain = email.split("@", 1)
    local_key = re.sub(r"[^a-z0-9]", "", local.lower())
    domain_l = domain.lower()
    domain_root = _domain(domain_l)
    if domain_l in BROKERAGE_EMAIL_DOMAINS or domain_root in BROKERAGE_EMAIL_DOMAINS:
        return False
    if any(local_key.startswith(prefix) for prefix in GENERIC_EMAIL_PREFIXES if prefix):
        LOG.info("EMAIL REJECT generic: %s", email)
        return True
    for gd in GENERIC_EMAIL_DOMAINS:
        if domain_l == gd or domain_l.endswith(f".{gd}"):
            LOG.info("EMAIL REJECT generic: %s", email)
            return True
    return False

def _looks_direct(phone: str, agent: str, state: str, tries: int = 2) -> Optional[bool]:
    if not phone:
        return None
    last = agent.split()[-1].lower()
    digits = re.sub(r"\D", "", phone)
    queries = [f'"{phone}" {state}', f'"{phone}" "{agent.split()[0]}"']
    saw_page = False
    for q in queries:
        for it in google_items(q, tries=tries):
            link = it.get("link", "")
            page = fetch_simple(link, strict=False)
            if not page:
                continue
            saw_page = True
            low_digits = re.sub(r"\D", "", page)
            if digits not in low_digits:
                continue
            pos = low_digits.find(digits)
            if pos == -1:
                continue
            if last in page.lower()[max(0, pos - 200): pos + 200]:
                return True
    return False if saw_page else None

PHONE_SOURCE_BASE = {
    "payload_contact": 2.6,
    "rapid_contact": 1.4,
    "rapid_listed_by": 1.4,
    "rapid_fallback": 1.0,
    "jsonld_person": 2.4,
    "jsonld_other": 1.6,
    "agent_card_dom": 2.2,
    "crawler_unverified": 0.9,
    "contact_us": 0.0,
    "cse_contact": 1.6,
}


def _build_trusted_domains(agent: str, urls: Iterable[str]) -> Set[str]:
    """Return domains that look like they belong to *agent*.

    We treat domains containing the agent's last name (or both first/last tokens)
    as trusted, which lets us loosen some office heuristics for official bio/
    contact pages.
    """

    parts = [p for p in agent.split() if p]
    if not parts:
        return set()
    first, last = parts[0].lower(), parts[-1].lower()
    tokens = {first, last, f"{first}{last}", f"{first}-{last}"}
    trusted = set()
    portal_domains = set(PORTAL_DOMAINS) | set(SCRAPE_SITES) | set(DYNAMIC_SITES)
    for url in urls:
        dom = _domain(url)
        if not dom or dom in portal_domains:
            continue
        low_dom = dom.lower()
        if any(tok and tok in low_dom for tok in tokens):
            trusted.add(dom)
    return trusted


def lookup_phone(agent: str, state: str, row_payload: Dict[str, Any]) -> Dict[str, Any]:
    override = _contact_override(agent, state)
    override_phone = override.get("phone") if override else ""
    if override_phone:
        formatted = fmt_phone(str(override_phone))
        if formatted and valid_phone(formatted):
            result = {
                "number": formatted,
                "confidence": "high",
                "score": max(CONTACT_PHONE_MIN_SCORE, CONTACT_PHONE_LOW_CONF + 0.5),
                "source": "override",
                "reason": "",
            }
            return result

    enrichment = _contact_enrichment(agent, state, row_payload)
    enriched_phone = enrichment.get("best_phone", "")
    if enriched_phone:
        confidence_score = enrichment.get("best_phone_confidence", 0)
        confidence = "high" if confidence_score >= 80 else "low"
        result = {
            "number": enriched_phone,
            "confidence": confidence,
            "score": max(CONTACT_PHONE_LOW_CONF, confidence_score / 25),
            "source": enrichment.get("best_phone_source_url", "enrichment"),
            "reason": "",
            "evidence": enrichment.get("best_phone_evidence", ""),
        }
        return result

    candidates: Dict[str, Dict[str, Any]] = {}
    had_candidates = False
    brokerage_hint = (row_payload.get("brokerageName") or row_payload.get("brokerage") or "").strip()
    location_extras: List[str] = [brokerage_hint] if brokerage_hint else []
    processed_urls: Set[str] = set()
    mirror_hits: Set[str] = set()
    trusted_domains: Set[str] = set()

    def _register(
        phone: Any,
        source: str,
        *,
        url: str = "",
        page_title: str = "",
        context: str = "",
        meta_name: str = "",
        name_match: bool = False,
        bonus: float = 0.0,
        office_flag: bool = False,
        trusted: bool = False,
    ) -> bool:
        nonlocal had_candidates
        formatted = fmt_phone(str(phone))
        if not (formatted and valid_phone(formatted)):
            return False
        had_candidates = True
        was_new = formatted not in candidates
        domain = _domain(url) if url else ""
        trusted_domain = trusted or (domain in trusted_domains if domain else False)
        info = candidates.setdefault(
            formatted,
            {
                "score": 0.0,
                "sources": set(),
                "applied": set(),
                "contexts": [],
                "page_titles": set(),
                "meta_names": set(),
                "urls": set(),
                "best_source": source,
                "best_base": -1.0,
                "office_demoted": False,
                "name_match": False,
                "direct_ok": None,
                "template_penalized": False,
                "office_flag_rapid": False,
            },
        )
        prev_score = info["score"]
        base = PHONE_SOURCE_BASE.get(source, PHONE_SOURCE_BASE["crawler_unverified"])
        if source and source not in info["applied"]:
            info["score"] += base
            info["applied"].add(source)
            if base >= info["best_base"]:
                info["best_base"] = base
                info["best_source"] = source
        elif not source:
            info["score"] += base
        if bonus:
            info["score"] += bonus
        if trusted_domain and not info.get("name_match"):
            info["score"] += 0.45
            info["name_match"] = True
        if name_match and not info["name_match"]:
            info["score"] += 0.6
            info["name_match"] = True
        info["sources"].add(source)
        if context:
            ctx = context.lower()
            info["contexts"].append(ctx)
            if not info["office_demoted"] and not trusted_domain and any(
                term in ctx for term in PHONE_OFFICE_TERMS
            ):
                info["score"] -= 0.6
                info["score"] -= 0.35
                info["office_demoted"] = True
                LOG.debug("PHONE DEMOTE office: %s", formatted)
        if office_flag and not info["office_demoted"]:
            info["score"] -= 1.0
            info["score"] -= 0.7
            info["office_demoted"] = True
            LOG.debug("PHONE DEMOTE office: %s", formatted)
            if source in {"rapid_contact", "rapid_listed_by"}:
                info["office_flag_rapid"] = True
        if trusted_domain and not office_flag:
            info["score"] += 0.25
        if office_flag and trusted_domain:
            info["contexts"].append("trusted-domain-office")
        if page_title:
            info["page_titles"].add(page_title.lower())
        if url:
            info["urls"].add(url.lower())
            DYNAMIC_SITES.add(_domain(url))
        if meta_name:
            info["meta_names"].add(meta_name.lower())
        triggered = False
        if info["score"] >= CONTACT_PHONE_LOW_CONF:
            if was_new or prev_score < CONTACT_PHONE_LOW_CONF:
                triggered = True
        return triggered
    for blk in (row_payload.get("contact_recipients") or []):
        ctx = " ".join(str(blk.get(k, "")) for k in ("title", "label", "role") if blk.get(k))
        meta_name = blk.get("display_name", "")
        match = _names_match(agent, meta_name)
        for p in _phones_from_block(blk):
            _register(p, "payload_contact", context=ctx, meta_name=meta_name, name_match=match)

    zpid = str(row_payload.get("zpid", ""))
    rapid = rapid_property(zpid) if zpid else {}
    lb: Dict[str, Any] = {}
    if rapid:
        lb = rapid.get("listed_by") or {}
        lb_name = lb.get("display_name", "")
        brokerage_from_lb = lb.get("brokerageName", "")
        if brokerage_from_lb:
            location_extras.append(brokerage_from_lb)
            if not brokerage_hint:
                brokerage_hint = brokerage_from_lb
        match = _names_match(agent, lb_name)
        for p in _phones_from_block(lb):
            _register(
                p,
                "rapid_listed_by",
                meta_name=lb_name,
                name_match=match,
                office_flag=not match and bool(lb_name),
            )
        for blk in rapid.get("contact_recipients", []) or []:
            blk_name = blk.get("display_name", "")
            match = _names_match(agent, blk_name)
            ctx = " ".join(str(blk.get(k, "")) for k in ("title", "label", "role") if blk.get(k))
            for p in _phones_from_block(blk):
                _register(
                    p,
                    "rapid_contact",
                    context=ctx,
                    meta_name=blk_name,
                    name_match=match,
                    office_flag=not match and bool(blk_name),
                )
        address_info = rapid.get("address") or {}
        location_extras.extend(
            [
                rapid.get("city", ""),
                rapid.get("state", ""),
                address_info.get("city", ""),
                address_info.get("state", ""),
            ]
        )

    def _rapid_mobile_shortcut() -> Optional[Dict[str, Any]]:
        rapid_candidates: List[Tuple[float, str, Dict[str, Any]]] = []
        for num, info in candidates.items():
            if not (info.get("sources", set()) & {"rapid_contact", "rapid_listed_by"}):
                continue
            rapid_candidates.append((info.get("score", 0.0), num, info))
        rapid_candidates.sort(reverse=True)
        for _, num, info in rapid_candidates:
            line_info = get_line_info(num)
            if not line_info.get("valid"):
                continue
            if line_info.get("mobile"):
                src = info.get("best_source") or next(iter(info.get("sources", [])), "rapid")
                base_score = max(
                    info.get("score", 0.0) + 0.25,
                    CONTACT_PHONE_LOW_CONF + 0.4,
                    CONTACT_PHONE_MIN_SCORE,
                )
                LOG.info("PHONE RAPID mobile shortcut: %s via %s", num, src)
                return {
                    "number": num,
                    "confidence": "high",
                    "score": base_score,
                    "source": f"{src}_cloudmersive_mobile",
                    "reason": "cloudmersive_mobile_rapid",
                }
        return None

    shortcut_result = _rapid_mobile_shortcut()
    if shortcut_result:
        return shortcut_result

    location_tokens, location_digits = _collect_location_hints(
        row_payload,
        state,
        *[hint for hint in location_extras if hint],
    )

    queries = _dedupe_queries(
        build_q_phone(
            agent,
            state,
            city=row_payload.get("city", ""),
            postal_code=row_payload.get("zip", ""),
            brokerage=brokerage_hint,
        )
    )
    rapid_urls = list(dict.fromkeys(_rapid_profile_urls(rapid)))
    hint_key = _normalize_override_key(agent, state)
    hint_urls = PROFILE_HINTS.get(hint_key) or PROFILE_HINTS.get(hint_key.lower(), [])
    hint_urls = [url for url in hint_urls if url]
    parts = [p for p in agent.split() if p]
    first_name = re.sub(r"[^a-z]", "", parts[0].lower()) if parts else ""
    last_name = re.sub(r"[^a-z]", "", (parts[-1] if len(parts) > 1 else parts[0]).lower()) if parts else ""
    first_variants, last_token = _first_last_tokens(agent)

    def _page_has_name(page_text: str) -> bool:
        if not (last_token or first_variants):
            return False
        low = page_text.lower()
        if last_token and not _token_in_text(low, last_token):
            return False
        if first_variants and not any(_token_in_text(low, tok) for tok in first_variants):
            return False
        if not _page_has_location(page_text, location_tokens, location_digits):
            return False
        return True

    def _process_page(url: str, page: str, trusted: bool = False) -> bool:
        if not page:
            return False
        if not trusted and not _page_has_name(page):
            return False
        page_viable = False
        ph, _, meta, info = extract_struct(page)
        page_title = info.get("title", "")
        domain = _domain(url)
        trusted_hit = trusted or (domain in trusted_domains if domain else False)
        for entry in meta:
            entry_type = entry.get("type")
            types = entry_type if isinstance(entry_type, list) else [entry_type]
            source = "jsonld_person" if any(
                t and isinstance(t, str) and ("Person" in t or "Agent" in t)
                for t in types
            ) else "jsonld_other"
            meta_name = str(entry.get("name", ""))
            match = _names_match(agent, meta_name)
            for num in entry.get("phones", []):
                if _register(
                    num,
                    source,
                    url=url,
                    page_title=page_title,
                    meta_name=meta_name,
                    name_match=match,
                    trusted=trusted_hit,
                ):
                    page_viable = True
        for anchor in info.get("tel", []):
            if _register(
                anchor.get("phone", ""),
                "agent_card_dom",
                url=url,
                page_title=page_title,
                context=anchor.get("context", ""),
                trusted=trusted_hit,
            ):
                page_viable = True
        low = html.unescape(page.lower())
        for num, details in proximity_scan(low, first_name, last_name).items():
            if _register(
                num,
                "agent_card_dom",
                url=url,
                page_title=page_title,
                context=" ".join(details.get("snippets", [])),
                bonus=min(1.0, details.get("score", 0.0) / 4.0),
                office_flag=details.get("office", False),
                trusted=trusted_hit,
            ):
                page_viable = True
        return page_viable

    def _has_viable_phone_candidate() -> bool:
        return any(
            info.get("score", 0.0) >= CONTACT_PHONE_LOW_CONF and not info.get("office_demoted")
            for info in candidates.values()
        )

    def _handle_url(url: str) -> bool:
        if not url:
            return False
        low = url.lower()
        if low in processed_urls:
            return False
        domain = _domain(url)
        trusted = domain in TRUSTED_CONTACT_DOMAINS
        page, mirrored = fetch_contact_page(url)
        processed_urls.add(low)
        if mirrored:
            mirror_hits.add(domain)
        if not page:
            return False
        return _process_page(url, page, trusted=trusted or domain in trusted_domains)

    priority_urls = list(dict.fromkeys(rapid_urls + hint_urls))
    trusted_domains.update(_build_trusted_domains(agent, priority_urls))
    priority_non_portal, priority_portal = _split_portals(priority_urls)

    processed = 0
    for url in priority_non_portal:
        if _handle_url(url) and _has_viable_phone_candidate():
            break
        if url:
            processed += 1
        if processed >= 3 and candidates:
            break

    if not candidates:
        processed = 0
        for url in priority_portal:
            if _handle_url(url) and _has_viable_phone_candidate():
                break
            if url:
                processed += 1
            if processed >= 3 and candidates:
                break

    urls: List[str] = list(priority_urls)
    if not _has_viable_phone_candidate():
        ddg_queries = _dedupe_queries(
            [
                f"{q} site:{site}"
                for q in queries
                for site in CONTACT_SITE_PRIORITY
            ]
        )
        search_hits = search_round_robin(ddg_queries, per_query_limit=3)
        urls.extend(
            [
                it.get("link", "")
                for attempts in search_hits
                for engine, items in attempts
                for it in items
                if it.get("link")
                and (engine != "google" or _domain(it.get("link", "")) not in GOOGLE_PORTAL_DENYLIST)
            ]
        )

        for attempts in search_hits:
            for engine, items in attempts:
                if engine == "google":
                    for it in items:
                        tel = (it.get("pagemap", {}).get("contactpoint", [{}])[0].get("telephone") or "")
                        if tel:
                            _register(tel, "cse_contact", url=it.get("link", ""))
                    trusted_domains.update(
                        _build_trusted_domains(
                            agent,
                            [it.get("link", "") for it in items],
                        )
                    )
        urls = urls[:20] if len(urls) > 20 else urls
    urls = list(dict.fromkeys(urls))
    non_portal, portal = _split_portals(urls)

    processed = 0
    for url in non_portal:
        if _handle_url(url) and _has_viable_phone_candidate():
            break
        if url:
            processed += 1
        if processed >= 3 and candidates:
            break

    if not candidates:
        processed = 0
        for url in portal:
            if _handle_url(url) and _has_viable_phone_candidate():
                break
            if url:
                processed += 1
            if processed >= 3 and candidates:
                break

    def _fallback_needed() -> bool:
        if _has_viable_phone_candidate():
            return False
        if mirror_hits:
            return True
        if candidates and any(not info.get("office_demoted") for info in candidates.values()):
            return False
        return True

    if _fallback_needed():
        extras = [e for e in location_extras if e]
        alt_queries = build_alt_q_phone(
            agent,
            state,
            brokerage=brokerage_hint,
            extras=extras,
        )
        if alt_queries:
            alt_results = list(pmap(google_items, _dedupe_queries(alt_queries)))
            for items in alt_results:
                for it in items:
                    tel = (it.get("pagemap", {}).get("contactpoint", [{}])[0].get("telephone") or "")
                    if tel:
                        _register(tel, "cse_contact", url=it.get("link", ""))
            alt_urls = []
            for items in alt_results:
                for it in items:
                    link = it.get("link", "")
                    if not link:
                        continue
                    low = link.lower()
                    if low in processed_urls:
                        continue
                    if _domain(link) in GOOGLE_PORTAL_DENYLIST:
                        continue
                    alt_urls.append(link)
            alt_urls = alt_urls[:15]
            alt_non_portal, alt_portal = _split_portals(alt_urls)
            for url in alt_non_portal:
                if _handle_url(url) and _has_viable_phone_candidate():
                    break
            if not _has_viable_phone_candidate():
                for url in alt_portal:
                    if _handle_url(url) and _has_viable_phone_candidate():
                        break

    tokens = _agent_tokens(agent)
    direct_cache: Dict[str, Optional[bool]] = {}
    best_number = ""
    best_score = float("-inf")
    best_source = ""
    best_is_mobile = False
    best_non_office_mobile_number = ""
    best_non_office_mobile_score = float("-inf")
    best_non_office_mobile_source = ""
    mobile_candidates: List[Tuple[str, Dict[str, Any]]] = []
    for number, info in candidates.items():
        if tokens and any(any(tok in ctx for tok in tokens) for ctx in info.get("contexts", [])):
            info["score"] += 0.5
        for meta_name in info.get("meta_names", []):
            if _names_match(agent, meta_name):
                info["score"] += 0.4
                break
        if info.get("page_titles") and agent.lower() in " ".join(info["page_titles"]):
            info["score"] += 0.3
        if any(
            _page_is_contactish(url, next(iter(info["page_titles"])) if info["page_titles"] else "")
            for url in info.get("urls", [])
        ):
            info["score"] -= 0.4
        preferred_source = info.get("best_source") or (
            next(iter(info["sources"])) if info["sources"] else ""
        )
        if preferred_source in {
            "agent_card_dom",
            "dom",
            "jsonld_other",
            "cse_contact",
            "crawler_unverified",
            "rapid_contact",
            "rapid_listed_by",
        }:
            if info.get("direct_ok") is None:
                if number not in direct_cache:
                    try:
                        direct_cache[number] = _looks_direct(number, agent, state)
                    except Exception as exc:
                        LOG.debug("PHONE direct check failed for %s: %s", number, exc)
                        direct_cache[number] = True
                info["direct_ok"] = direct_cache[number]
                if preferred_source == "cse_contact" and info["direct_ok"] is False:
                    info["direct_ok"] = None
            if info.get("direct_ok") is False and not info.get("template_penalized"):
                info["score"] -= 2.0
                info["template_penalized"] = True
        line_info = get_line_info(number)
        if not line_info.get("valid"):
            LOG.debug("PHONE DROP invalid number: %s", number)
            continue
        if line_info.get("country") and line_info.get("country") != "US":
            LOG.debug("PHONE DROP non-US number: %s (%s)", number, line_info.get("country"))
            continue
        mobile = bool(line_info.get("mobile"))
        info["is_mobile"] = mobile
        if mobile:
            mobile_candidates.append((number, info))
            mobile_bonus = 0.35
            if any(src in info.get("sources", set()) for src in {"rapid_contact", "rapid_listed_by"}):
                mobile_bonus += 0.25
            if info.get("office_demoted"):
                mobile_bonus += 0.4
            info["score"] += mobile_bonus
            if (
                info.get("office_demoted")
                and info.get("office_flag_rapid")
                and info["score"] < CONTACT_PHONE_LOW_CONF
            ):
                info["score"] = CONTACT_PHONE_LOW_CONF
                LOG.info(
                    "PHONE RECOVER rapid mobile after office demotion: %s -> %.2f",
                    number,
                    info["score"],
                )
        else:
            info["score"] -= 1.0
        info["final_score"] = info["score"]
        source = preferred_source
        if info["score"] > best_score:
            best_score = info["score"]
            best_number = number
            best_source = source
            best_is_mobile = mobile
        if mobile and not info.get("office_demoted") and info["score"] > best_non_office_mobile_score:
            best_non_office_mobile_score = info["score"]
            best_non_office_mobile_number = number
            best_non_office_mobile_source = source

    other_viable_candidates = [
        (num, info)
        for num, info in candidates.items()
        if num != best_number
        and info.get("final_score", info.get("score", 0.0)) >= CONTACT_PHONE_LOW_CONF
    ]

    override_threshold = max(CONTACT_PHONE_OVERRIDE_MIN, CONTACT_PHONE_LOW_CONF)
    if best_score != float("-inf"):
        override_threshold = max(
            CONTACT_PHONE_OVERRIDE_MIN,
            CONTACT_PHONE_LOW_CONF,
            best_score - CONTACT_PHONE_OVERRIDE_DELTA,
        )

    if (
        best_number
        and candidates.get(best_number, {}).get("office_demoted")
        and best_non_office_mobile_number
        and best_non_office_mobile_number != best_number
    ):
        LOG.info(
            "PHONE OVERRIDE prefer_non_office_mobile: %s (%.2f) -> %s (%.2f)",
            best_number,
            best_score,
            best_non_office_mobile_number,
            best_non_office_mobile_score,
        )
        best_number = best_non_office_mobile_number
        best_score = best_non_office_mobile_score
        best_source = best_non_office_mobile_source
        best_is_mobile = True

    result = {
        "number": "",
        "confidence": "",
        "score": best_score if best_score != float("-inf") else 0.0,
        "source": best_source,
        "reason": "",
    }

    if best_number:
        candidate_info = candidates.get(best_number, {})
        override_low_conf = False
        adjusted_score = best_score
        direct_ok = candidate_info.get("direct_ok")
        if best_score >= CONTACT_PHONE_MIN_SCORE:
            confidence = "high"
            LOG.debug("PHONE WIN %s via %s score=%.2f", best_number, best_source or "unknown", best_score)
        elif best_score >= CONTACT_PHONE_LOW_CONF:
            confidence = "low"
            LOG.info("PHONE WIN (low-confidence): %s %.2f %s", best_number, best_score, best_source or "unknown")
        elif best_is_mobile:
            allow_override = True
            if direct_ok is False:
                sole_mobile = len(mobile_candidates) == 1
                other_viable = bool(other_viable_candidates)
                if not sole_mobile and other_viable:
                    allow_override = False
            if allow_override and best_source in {"rapid_contact", "rapid_listed_by"}:
                if not candidate_info.get("name_match") and candidate_info.get("direct_ok") is not True:
                    allow_override = False
            if allow_override:
                override_low_conf = True
                confidence = "low"
                adjusted_score = max(best_score, CONTACT_PHONE_LOW_CONF)
                LOG.info(
                    "PHONE WIN (Cloudmersive override): %s %.2f %s raw=%.2f",
                    best_number,
                    adjusted_score,
                    best_source or "unknown",
                    best_score,
                )
            else:
                confidence = ""
                override_low_conf = False
                adjusted_score = best_score
        else:
            confidence = ""
        if confidence:
            best_score = adjusted_score
            result.update({
                "number": best_number,
                "confidence": confidence,
                "score": adjusted_score,
                "source": best_source,
            })
            return result

    if candidates:
        office_choice = max(
            (
                (number, info)
                for number, info in candidates.items()
                if info.get("office_demoted") and info.get("score", 0.0) >= CONTACT_PHONE_OVERRIDE_MIN
            ),
            key=lambda item: item[1].get("score", float("-inf")),
            default=("", {}),
        )
        office_number, office_info = office_choice
        if office_number:
            office_score = office_info.get("score", 0.0)
            office_source = next(iter(office_info.get("sources", [])), "")
            LOG.info(
                "PHONE WIN (office fallback): %s %.2f %s",
                office_number,
                office_score,
                office_source or "unknown",
            )
            result.update(
                {
                    "number": office_number,
                    "confidence": "low",
                    "score": office_score,
                    "source": office_source,
                }
            )
            return result

    if had_candidates or best_number:
        reason = "withheld_low_conf_mix"
    else:
        reason = "no_personal_mobile"

    if candidates:
        top_candidates = sorted(
            candidates.items(),
            key=lambda item: item[1].get("score", float("-inf")),
            reverse=True,
        )
        summary = []
        for number, info in top_candidates[:5]:
            summary.append(
                "{} score={:.2f} src={} office_demoted={}".format(
                    number,
                    info.get("score", 0.0),
                    ",".join(sorted(info.get("sources", []))) or "",
                    info.get("office_demoted", False),
                )
            )
        LOG.warning(
            "PHONE DROP candidates for %s %s (had_candidates=%s): %s",
            agent,
            state,
            had_candidates,
            " | ".join(summary) if summary else "<none>",
        )

    result.update({
        "number": "",
        "confidence": "",
        "reason": reason,
        "score": best_score if best_score != float("-inf") else 0.0,
        "source": best_source,
    })
    METRICS["phone_no_verified_mobile"] += 1
    LOG.warning(
        "PHONE DROP no verified mobile for %s %s zpid=%s reason=%s had_candidates=%s",
        agent,
        state,
        zpid or "",
        reason,
        had_candidates,
    )
    return result

def lookup_email(agent: str, state: str, row_payload: Dict[str, Any]) -> Dict[str, Any]:
    override = _contact_override(agent, state)
    override_email = override.get("email") if override else ""
    if override_email:
        cleaned = clean_email(str(override_email))
        if cleaned and ok_email(cleaned):
            result = {
                "email": cleaned,
                "confidence": "high",
                "score": max(CONTACT_EMAIL_MIN_SCORE, 1.0),
                "source": "override",
                "reason": "",
            }
            return result

    enrichment = _contact_enrichment(agent, state, row_payload)
    enriched_email = enrichment.get("best_email", "")
    if enriched_email:
        confidence_score = enrichment.get("best_email_confidence", 0)
        confidence = "high" if confidence_score >= 80 else "low"
        result = {
            "email": enriched_email,
            "confidence": confidence,
            "score": max(CONTACT_EMAIL_LOW_CONF, confidence_score / 25),
            "source": enrichment.get("best_email_source_url", "enrichment"),
            "reason": "",
            "evidence": enrichment.get("best_email_evidence", ""),
        }
        return result

    brokerage = (row_payload.get("brokerageName") or row_payload.get("brokerage") or "").strip()
    domain_hint = mls_id = ""
    zpid = str(row_payload.get("zpid", ""))
    rapid = rapid_property(zpid) if zpid else {}
    candidates: Dict[str, Dict[str, Any]] = {}
    generic_seen: Set[str] = set()
    had_candidates = False
    location_extras: List[str] = [brokerage] if brokerage else []
    blocked_domains: Set[str] = set()
    trusted_domains: Set[str] = set()
    cse_rate_limited = False

    inferred_domain_hint = _infer_domain_from_text(brokerage) or _infer_domain_from_text(agent)
    if inferred_domain_hint and not domain_hint:
        domain_hint = inferred_domain_hint
    if domain_hint:
        trusted_domains.add(domain_hint.lower())

    tokens = _agent_tokens(agent)
    IDENTITY_SOURCES = {
        "mailto",
        "dom",
        "jsonld_other",
        "jsonld_person",
        "cse_contact",
        "pattern",
    }

    def _authoritative_rapid_email() -> str:
        if not rapid:
            return ""
        blocks: List[Dict[str, Any]] = []
        lb = rapid.get("listed_by") or {}
        if lb:
            blocks.append(lb)
        blocks.extend(rapid.get("contact_recipients", []) or [])
        for blk in blocks:
            for em in _emails_from_block(blk):
                raw = str(em or "").strip()
                if not raw:
                    continue
                cleaned = clean_email(raw) or raw
                return cleaned
        return ""

    rapid_authoritative_email = _authoritative_rapid_email()
    if rapid_authoritative_email:
        score = max(CONTACT_EMAIL_MIN_SCORE, CONTACT_EMAIL_FALLBACK_SCORE + 0.2)
        return {
            "email": rapid_authoritative_email,
            "confidence": "high",
            "score": score,
            "source": "rapid_email_authoritative",
            "reason": "",
        }

    def _register(
        email: str,
        source: str,
        *,
        url: str = "",
        page_title: str = "",
        context: str = "",
        meta_name: str = "",
        penalty: float = 0.0,
        trusted: bool = False,
    ) -> None:
        nonlocal had_candidates
        cleaned = clean_email(email)
        if not cleaned or not ok_email(cleaned):
            return
        low = cleaned.lower()
        matches_agent = _email_matches_name(agent, cleaned)
        is_generic = _is_generic_email(cleaned)
        if is_generic and not matches_agent:
            if low in generic_seen:
                return
            generic_seen.add(low)
        elif is_generic:
            generic_seen.add(low)

        domain = _domain(url) if url else ""
        trusted_domain = trusted or (domain in trusted_domains if domain else False)

        if source in IDENTITY_SOURCES:
            identity_ok = False
            if matches_agent:
                identity_ok = True
            elif meta_name and _names_match(agent, meta_name):
                identity_ok = True
            else:
                haystacks: List[str] = []
                if context:
                    haystacks.append(context.lower())
                if page_title:
                    haystacks.append(page_title.lower())
                if meta_name:
                    haystacks.append(meta_name.lower())
                if tokens:
                    for tok in tokens:
                        if tok and any(tok in hay for hay in haystacks):
                            identity_ok = True
                            break
            if not identity_ok:
                return

        if trusted_domain:
            matches_agent = True
            had_candidates = True

        had_candidates = True
        info = candidates.setdefault(
            cleaned,
            {
                "score": 0.0,
                "sources": set(),
                "applied": set(),
                "contexts": [],
                "page_titles": set(),
                "meta_names": set(),
                "urls": set(),
                "best_source": source,
                "best_base": -1.0,
                "identity_hits": 0,
                "identity_sources": set(),
                "agent_match": False,
                "domain": cleaned.split("@", 1)[1].lower(),
                "generic": False,
                "generic_penalized": False,
                "penalty_applied": False,
            },
        )
        if is_generic:
            info["generic"] = True
        base = EMAIL_SOURCE_BASE.get(source, EMAIL_SOURCE_BASE["dom"])
        if source and source not in info["applied"]:
            info["score"] += base
            info["applied"].add(source)
            if base >= info["best_base"]:
                info["best_base"] = base
                info["best_source"] = source
        info["sources"].add(source)
        if penalty and not info["penalty_applied"]:
            info["score"] -= penalty
            info["penalty_applied"] = True
        if trusted_domain:
            info["score"] += 0.35
            info["identity_hits"] += 1
            info["identity_sources"].add("trusted_domain")
        if is_generic and not matches_agent and not info["generic_penalized"]:
            info["score"] -= 0.25
            info["generic_penalized"] = True
        if source in IDENTITY_SOURCES and source not in info["identity_sources"]:
            info["identity_sources"].add(source)
            info["identity_hits"] += 1
        if context:
            info["contexts"].append(context.lower())
        if page_title:
            info["page_titles"].add(page_title.lower())
        if url:
            info["urls"].add(url.lower())
            DYNAMIC_SITES.add(_domain(url))
        if meta_name:
            meta_low = meta_name.lower()
            if meta_low not in info["meta_names"]:
                info["meta_names"].add(meta_low)
                if _names_match(agent, meta_name):
                    info["identity_hits"] += 1
                    info["identity_sources"].add("meta_name")
            else:
                info["meta_names"].add(meta_low)
        if matches_agent and not info["agent_match"]:
            info["agent_match"] = True
            info["identity_hits"] += 1
            info["identity_sources"].add("email_match")

    for blk in (row_payload.get("contact_recipients") or []):
        ctx = " ".join(str(blk.get(k, "")) for k in ("title", "label", "role") if blk.get(k))
        for em in _emails_from_block(blk):
            if _email_matches_name(agent, em):
                _register(em, "payload_contact", context=ctx, meta_name=blk.get("display_name", ""))

    zpid = str(row_payload.get("zpid", ""))
    rapid: Dict[str, Any] = {}
    if rapid:
        lb = rapid.get("listed_by") or {}
        brokerage = (lb.get("brokerageName", "") or brokerage).strip()
        if brokerage and brokerage not in location_extras:
            location_extras.append(brokerage)
        mls_id = lb.get("listingAgentMlsId", "")
        lb_display = lb.get("display_name", "")
        lb_ctx = " ".join(str(lb.get(k, "")) for k in ("title", "label", "role") if lb.get(k))
        for em in _emails_from_block(lb):
            name_match = not lb_display or _names_match(agent, lb_display)
            _register(
                em,
                "rapid_listed_by",
                meta_name=lb_display,
                context=lb_ctx,
                penalty=0.4 if not name_match else 0.0,
            )
        for blk in rapid.get("contact_recipients", []) or []:
            ctx = " ".join(str(blk.get(k, "")) for k in ("title", "label", "role") if blk.get(k))
            for em in _emails_from_block(blk):
                display_name = blk.get("display_name", "")
                name_match = not display_name or _names_match(agent, display_name)
                _register(
                    em,
                    "rapid_contact",
                    context=ctx,
                    meta_name=display_name,
                    penalty=0.4 if not name_match else 0.0,
                )
        address_info = rapid.get("address") or {}
        location_extras.extend(
            [
                rapid.get("city", ""),
                rapid.get("state", ""),
                address_info.get("city", ""),
                address_info.get("state", ""),
            ]
        )

    location_tokens, location_digits = _collect_location_hints(
        row_payload,
        state,
        *[hint for hint in location_extras if hint],
    )

    queries = build_q_email(
        agent,
        state,
        brokerage,
        domain_hint,
        mls_id,
        city=row_payload.get("city", ""),
        postal_code=row_payload.get("zip", ""),
        include_realtor_probe=True,
    )
    rapid_urls = list(dict.fromkeys(_rapid_profile_urls(rapid) if zpid else []))
    hint_key = _normalize_override_key(agent, state)
    hint_urls = PROFILE_HINTS.get(hint_key) or PROFILE_HINTS.get(hint_key.lower(), [])
    hint_urls = [url for url in hint_urls if url]

    first_variants, last_token = _first_last_tokens(agent)

    def _page_has_name(page_text: str, *, domain_hint_hit: bool = False) -> bool:
        if domain_hint_hit:
            return True
        if not (last_token or first_variants):
            return False
        low = page_text.lower()
        if last_token and not _token_in_text(low, last_token):
            return False
        if first_variants and not any(_token_in_text(low, tok) for tok in first_variants):
            return False
        if not _page_has_location(page_text, location_tokens, location_digits):
            return False
        return True

    def _process_page(url: str, page: str) -> None:
        if not page:
            return
        dom = _domain(url)
        domain_hint_hit = bool(domain_hint and dom.endswith(domain_hint.lower()))
        if not _page_has_name(page, domain_hint_hit=domain_hint_hit):
            return
        _, ems, meta, info = extract_struct(page)
        page_title = info.get("title", "")
        seen: Set[str] = set()
        domain = dom
        trusted_hit = domain in trusted_domains or domain_hint_hit
        if domain in BROKERAGE_EMAIL_DOMAINS:
            for mail in _extract_remax_emails(page):
                if mail in seen:
                    continue
                seen.add(mail)
                _register(
                    mail,
                    "remax_struct",
                    url=url,
                    page_title=page_title,
                    meta_name=agent,
                    trusted=trusted_hit,
                )
        for entry in meta:
            entry_type = entry.get("type")
            types = entry_type if isinstance(entry_type, list) else [entry_type]
            source = "jsonld_person" if any(
                t and isinstance(t, str) and ("Person" in t or "Agent" in t)
                for t in types
            ) else "jsonld_other"
            meta_name = str(entry.get("name", ""))
            for mail in entry.get("emails", []):
                seen.add(mail)
                _register(
                    mail,
                    source,
                    url=url,
                    page_title=page_title,
                    meta_name=meta_name,
                    trusted=trusted_hit,
                )
                patt = _pattern_from_example(mail, agent)
                if patt:
                    domain_patterns.setdefault(_domain(mail), patt)
        for item in info.get("mailto", []):
            mail = item.get("email", "")
            if not mail:
                continue
            seen.add(mail)
            _register(
                mail,
                "mailto",
                url=url,
                page_title=page_title,
                context=item.get("context", ""),
                trusted=trusted_hit,
            )
        for mail in ems:
            if mail in seen:
                continue
            seen.add(mail)
            _register(mail, "dom", url=url, page_title=page_title, trusted=trusted_hit)
        lower_page = page.lower()
        for m in EMAIL_RE.finditer(lower_page):
            raw = page[m.start(): m.end()]
            cleaned = clean_email(raw)
            if cleaned in seen:
                continue
            snippet = lower_page[max(0, m.start() - 120): m.end() + 120]
            _register(
                cleaned,
                "dom",
                url=url,
                page_title=page_title,
                context=" ".join(snippet.split()),
                trusted=trusted_hit,
            )

    def _has_viable_email_candidate() -> bool:
        return any(info.get("score", 0.0) >= CONTACT_EMAIL_FALLBACK_SCORE for info in candidates.values())

    priority_urls = list(dict.fromkeys(rapid_urls + hint_urls))
    trusted_domains.update(_build_trusted_domains(agent, priority_urls))
    priority_non_portal, priority_portal = _split_portals(priority_urls)

    processed = 0
    for url in priority_non_portal:
        dom = _domain(url)
        page, _ = fetch_contact_page(url)
        if not page:
            if _blocked(dom):
                blocked_domains.add(dom)
            continue
        _process_page(url, page)
        if _has_viable_email_candidate():
            break
        processed += 1
        if processed >= 4 and candidates:
            break

    if not candidates:
        processed = 0
        for url in priority_portal:
            dom = _domain(url)
            page, _ = fetch_contact_page(url)
            if not page:
                if _blocked(dom):
                    blocked_domains.add(dom)
                continue
            _process_page(url, page)
            if _has_viable_email_candidate():
                break
            processed += 1
            if processed >= 4 and candidates:
                break

    urls: List[str] = list(priority_urls)
    if not _has_viable_email_candidate():
        ddg_queries = _dedupe_queries(
            [
                f"{q} site:{site}"
                for q in queries
                for site in CONTACT_SITE_PRIORITY
            ]
        )
        search_hits = search_round_robin(ddg_queries, per_query_limit=4)
        urls.extend(
            [
                it.get("link", "")
                for attempts in search_hits
                for _, items in attempts
                for it in items
                if it.get("link")
            ]
        )

        for attempts in search_hits:
            for _, items in attempts:
                trusted_domains.update(
                    _build_trusted_domains(
                        agent,
                        [it.get("link", "") for it in items],
                    )
                )
        if not any(items for attempts in search_hits for _, items in attempts):
            cse_rate_limited = True
        urls = urls[:20] if len(urls) > 20 else urls
    urls = list(dict.fromkeys(urls))
    non_portal, portal = _split_portals(urls)

    processed = 0
    for url in non_portal:
        dom = _domain(url)
        page, _ = fetch_contact_page(url)
        if not page:
            if _blocked(dom):
                blocked_domains.add(dom)
            continue
        _process_page(url, page)
        if _has_viable_email_candidate():
            break
        processed += 1
        if processed >= 4 and candidates:
            break

    if not candidates:
        processed = 0
        for url in portal:
            dom = _domain(url)
            page, _ = fetch_contact_page(url)
            if not page:
                if _blocked(dom):
                    blocked_domains.add(dom)
                continue
            _process_page(url, page)
            if _has_viable_email_candidate():
                break
            processed += 1
            if processed >= 4 and candidates:
                break

    if not candidates and blocked_domains:
        for dom in blocked_domains:
            guess = _synth_email(agent, dom)
            if guess:
                _register(guess, "pattern", url=f"https://{dom}")
        if not candidates and brokerage:
            broker_domain = _guess_domain_from_brokerage(brokerage)
            if broker_domain:
                if not domain_hint:
                    domain_hint = broker_domain
                guess = _synth_email(agent, broker_domain)
                if guess:
                    _register(guess, "pattern", url=f"https://{broker_domain}")

    if not candidates and domain_hint:
        guess = _synth_email(agent, domain_hint)
        if guess:
            _register(guess, "pattern")

    best_email = ""
    best_score = 0.0
    best_source = ""
    for email, info in candidates.items():
        local = email.split("@", 1)[0].lower()
        if _email_matches_name(agent, email):
            info["score"] += 0.35
        hits = sum(1 for tok in tokens if tok and tok in local)
        if hits:
            info["score"] += 0.4 if hits >= len(tokens) else 0.25
        if tokens and any(
            any(tok in ctx for tok in tokens)
            for ctx in info.get("contexts", [])
        ):
            info["score"] += 0.2
        if info.get("page_titles") and agent.lower() in " ".join(info["page_titles"]):
            info["score"] += 0.25
        else:
            for title in info.get("page_titles", []):
                if tokens and all(tok in title for tok in tokens):
                    info["score"] += 0.2
                    break
        for meta_name in info.get("meta_names", []):
            if _names_match(agent, meta_name):
                info["score"] += 0.3
                break
        if any(
            _page_is_contactish(url, next(iter(info["page_titles"])) if info["page_titles"] else "")
            for url in info.get("urls", [])
        ):
            info["score"] -= 0.3
        if any(
            any(term in ctx for term in PHONE_OFFICE_TERMS)
            for ctx in info.get("contexts", [])
        ):
            info["score"] -= 0.35
        final_score = info["score"]
        info["final_score"] = final_score
        source = info.get("best_source") or (next(iter(info["sources"])) if info["sources"] else "")
        if final_score > best_score:
            best_email = email
            best_score = final_score
            best_source = source

    reason = ""
    has_non_generic_candidate = any(
        not info.get("generic") for info in candidates.values()
    )
    result = {
        "email": best_email,
        "score": best_score,
        "source": best_source,
        "reason": reason,
        "confidence": "",
    }

    normalized_brokerage_tokens = [
        tok
        for tok in re.sub(r"[^a-z0-9]+", " ", brokerage.lower()).split()
        if len(tok) >= 4
    ] if brokerage else []

    if best_email and best_score >= CONTACT_EMAIL_MIN_SCORE:
        info = candidates.get(best_email, {})
        if info.get("generic"):
            result["confidence"] = "low"
            LOG.info(
                "EMAIL WIN (generic) %s via %s score=%.2f",
                best_email,
                best_source or "unknown",
                best_score,
            )
            return result
        result["confidence"] = "high"
        LOG.debug(
            "EMAIL WIN %s via %s score=%.2f",
            best_email,
            best_source or "unknown",
            best_score,
        )
        return result

    fallback_ok = False
    if best_email and best_score >= CONTACT_EMAIL_FALLBACK_SCORE:
        info = candidates.get(best_email, {})
        sources = info.get("sources", set())
        identity_hits = info.get("identity_hits", 0)
        agent_match = info.get("agent_match", False)
        identity_sources = info.get("identity_sources", set())
        strong_sources = sources & {
            "rapid_listed_by",
            "rapid_contact",
            "payload_contact",
            "jsonld_person",
        }
        domain = info.get("domain", best_email.split("@", 1)[1].lower()) if best_email else ""
        domain_l = domain.lower()
        domain_root = _domain(domain_l)
        domain_hint_hit = bool(domain_hint and domain_l.endswith(domain_hint.lower()))
        brokerage_domain_ok = any(
            candidate in BROKERAGE_EMAIL_DOMAINS
            for candidate in (domain_l, domain_root)
        )
        brokerage_hit = any(tok in domain_l or tok in domain_root for tok in normalized_brokerage_tokens)
        domain_contains_agent = any(
            tok and (tok in domain_l or tok in domain_root)
            for tok in tokens
            if len(tok) >= 3
        )
        context_match = any(
            tok and any(tok in ctx for ctx in info.get("contexts", []))
            for tok in tokens
        )
        generic_only = info.get("generic") and not has_non_generic_candidate
        fallback_ok = (
            agent_match
            or identity_hits >= 2
            or bool(strong_sources)
            or bool(identity_sources - {"dom"})
            or domain_hint_hit
            or brokerage_hit
            or brokerage_domain_ok
            or (
                generic_only
                and (
                    domain_contains_agent
                    or brokerage_hit
                    or domain_hint_hit
                    or brokerage_domain_ok
                    or context_match
                    or identity_hits >= 1
                )
            )
        )
        if fallback_ok:
            result["confidence"] = "low"
            LOG.info(
                "EMAIL WIN (fallback) %s via %s score=%.2f", best_email, best_source or "unknown", best_score
            )
            return result

    def _domain_signals(email: str, info: Dict[str, Any]) -> Tuple[bool, bool, bool, bool, str, str]:
        domain = info.get("domain", email.split("@", 1)[1].lower()) if email else ""
        domain_l = domain.lower()
        domain_root = _domain(domain_l)
        domain_hint_hit = bool(domain_hint and domain_l.endswith(domain_hint.lower()))
        brokerage_domain_ok = any(
            candidate in BROKERAGE_EMAIL_DOMAINS for candidate in (domain_l, domain_root)
        )
        brokerage_hit = any(tok in domain_l or tok in domain_root for tok in normalized_brokerage_tokens)
        agent_token_hit = any(
            tok and (tok in domain_l or tok in domain_root) for tok in tokens if len(tok) >= 3
        )
        return domain_hint_hit, brokerage_domain_ok, brokerage_hit, agent_token_hit, domain_l, domain_root

    if not result.get("confidence") and candidates:
        rapidish_sources = {"rapid_listed_by", "rapid_contact", "payload_contact", "cse_contact"}
        fallback_email = ""
        fallback_source = ""
        fallback_score = float("-inf")
        for email, info in candidates.items():
            if not (info.get("sources", set()) & rapidish_sources):
                continue
            domain_hint_hit, brokerage_domain_ok, brokerage_hit, agent_token_hit, domain_l, domain_root = _domain_signals(email, info)
            if not (
                domain_hint_hit
                or brokerage_domain_ok
                or brokerage_hit
                or agent_token_hit
                or domain_l in trusted_domains
                or domain_root in trusted_domains
            ):
                continue
            score = info.get("final_score", info.get("score", 0.0))
            if score > fallback_score:
                fallback_score = score
                fallback_email = email
                fallback_source = info.get("best_source") or fallback_source or next(iter(info.get("sources", [])), "")
        if fallback_email:
            adjusted_score = max(fallback_score, CONTACT_EMAIL_FALLBACK_SCORE)
            result.update(
                {
                    "email": fallback_email,
                    "confidence": "low",
                    "source": fallback_source,
                    "score": adjusted_score,
                }
            )
            LOG.info(
                "EMAIL WIN (rapid/portal fallback) %s via %s score=%.2f raw=%.2f",
                fallback_email,
                fallback_source or "unknown",
                adjusted_score,
                fallback_score,
            )
            return result

    if not had_candidates and ENABLE_SYNTH_EMAIL_FALLBACK:
        synth_domains: Set[str] = set()
        if domain_hint:
            synth_domains.add(domain_hint)
        synth_domains.update(trusted_domains)
        if brokerage:
            guessed_dom = _guess_domain_from_brokerage(brokerage)
            if guessed_dom:
                synth_domains.add(guessed_dom)
        synth_domains = {d for d in synth_domains if d}
        synthetic_candidates = _synth_from_tokens(agent, synth_domains)
        if synthetic_candidates:
            synthetic_email = synthetic_candidates[0]
            result.update(
                {
                    "email": synthetic_email,
                    "confidence": "low",
                    "source": "synthetic_pattern",
                    "score": max(best_score, CONTACT_EMAIL_FALLBACK_SCORE - 0.05),
                    "reason": "",
                }
            )
            LOG.info(
                "EMAIL WIN (synthetic pattern) %s via %s",
                synthetic_email,
                ",".join(sorted(synth_domains)) or "<unknown>",
            )
            return result

    if not had_candidates:
        reason = "cse_rate_limited" if cse_rate_limited else "no_personal_email"
    else:
        reason = "withheld_low_conf_mix"
    result.update({
        "email": "",
        "reason": reason,
        "score": best_score,
        "source": best_source,
        "confidence": "",
    })
    if cse_rate_limited:
        LOG.warning(
            "EMAIL DEFER for %s %s – CSE blocked/rate limited",
            agent,
            state,
        )
    LOG.debug(
        "EMAIL FAIL for %s %s – personalised e-mail not found (%s)",
        agent,
        state,
        reason,
    )
    return result


def is_active_listing(zpid):
    if not RAPID_KEY:
        return True
    try:
        status = rapid_property(zpid).get("homeStatus", "").upper()
        return (not status) or status in GOOD_STATUS
    except Exception as e:
        LOG.warning("Rapid status check failed for %s (%s) – keeping row", zpid, e)
        return True

def mark_sent(row_idx: int, msg_id: str):
    ts = datetime.now(tz=TZ).isoformat()
    data = [
        {"range": f"{GSHEET_TAB}!H{row_idx}", "values": [["x"]]},
        {"range": f"{GSHEET_TAB}!W{row_idx}", "values": [[ts]]},
        {"range": f"{GSHEET_TAB}!L{row_idx}", "values": [[msg_id]]},
    ]
    try:
        sheets_service.spreadsheets().values().batchUpdate(
            spreadsheetId=GSHEET_ID,
            body={"valueInputOption": "RAW", "data": data},
        ).execute()
        LOG.info("Marked row %s H:x W:init-ts L:msg-id (msg_id=%s)", row_idx, msg_id)
    except Exception as e:
        LOG.error("GSheet mark_sent error %s", e)

def mark_followup(row_idx: int):
    ts = datetime.now(tz=TZ).isoformat()
    data = [
        {"range": f"{GSHEET_TAB}!I{row_idx}", "values": [["x"]]},
        {"range": f"{GSHEET_TAB}!X{row_idx}", "values": [[ts]]},
    ]
    try:
        sheets_service.spreadsheets().values().batchUpdate(
            spreadsheetId=GSHEET_ID,
            body={"valueInputOption": "RAW", "data": data},
        ).execute()
        LOG.info("Marked row %s I:x X:follow-up done", row_idx)
    except Exception as e:
        LOG.error("GSheet mark_followup error %s", e)

def mark_reply(row_idx: int):
    ts = datetime.now(tz=TZ).isoformat()
    data = [
        {"range": f"{GSHEET_TAB}!I{row_idx}", "values": [["x"]]},
        {"range": f"{GSHEET_TAB}!K{row_idx}", "values": [[ts]]},
    ]
    try:
        sheets_service.spreadsheets().values().batchUpdate(
            spreadsheetId=GSHEET_ID,
            body={"valueInputOption": "RAW", "data": data},
        ).execute()
        LOG.info("Marked row %s I:x K:ts – reply detected", row_idx)
    except Exception as e:
        LOG.error("GSheet mark_reply error %s", e)

def _row_is_empty(row_vals: List[Any]) -> bool:
    return not any(str(v).strip() for v in row_vals)


def _find_next_open_row(start_row: Optional[int] = None) -> int:
    row = start_row or GSHEET_NEXT_ROW_HINT
    window = max(1, GSHEET_ROW_SCAN_WINDOW)
    while True:
        end_row = row + window - 1
        resp = sheets_service.spreadsheets().values().get(
            spreadsheetId=GSHEET_ID,
            range=f"{GSHEET_TAB}!A{row}:A{end_row}",
            majorDimension="ROWS",
        ).execute()
        values = resp.get("values", [])
        for offset, row_vals in enumerate(values):
            if _row_is_empty(row_vals):
                return row + offset
        if len(values) < window:
            return row + len(values)
        row += window


_next_row_hint = GSHEET_NEXT_ROW_HINT


def append_row(vals) -> int:
    global _next_row_hint
    row_idx = _find_next_open_row(_next_row_hint)
    resp = sheets_service.spreadsheets().values().update(
        spreadsheetId=GSHEET_ID,
        range=f"{GSHEET_TAB}!A{row_idx}",
        valueInputOption="RAW",
        body={"values": [vals]},
    ).execute()
    updated_range = resp.get("updatedRange") or resp.get("range") or f"{GSHEET_TAB}!A{row_idx}"
    row_idx = int(updated_range.split("!")[1].split(":")[0][1:])
    _next_row_hint = row_idx + 1
    LOG.info("Row appended to sheet (row %s); next hint %s", row_idx, _next_row_hint)
    return row_idx

def phone_exists(p):
    return p in seen_phones

def _digits_only(num: str) -> str:
    """Keep digits, prefix 1 if US local (10 digits)."""
    digits = re.sub(r"\D", "", num or "")
    if len(digits) == 10:
        digits = "1" + digits
    return digits


_line_type_cache: Dict[str, bool] = {}
_line_type_verified: Dict[str, bool] = {}
_line_info_cache: Dict[str, Dict[str, Any]] = {}
PROFILE_HINTS: Dict[str, List[str]] = {}
TRUSTED_CONTACT_DOMAINS: Set[str] = set()


def _is_explicit_mobile(value: Any) -> bool:
    if value is None:
        return False
    return str(value).strip().lower() == "mobile"


def get_line_info(phone: str) -> Dict[str, Any]:
    """Return Cloudmersive classification for *phone* with caching.

    The result dictionary contains ``valid`` (bool), ``mobile`` (bool), and
    ``country`` (upper-case ISO code or empty string). If Cloudmersive is
    unavailable, we fall back to lightweight validation and treat the number as
    mobile to avoid overly aggressive filtering.
    """

    if not phone:
        return {"valid": False, "mobile": False, "country": ""}
    if phone in _line_info_cache:
        return _line_info_cache[phone]

    info = {"valid": valid_phone(phone), "mobile": False, "country": "US"}
    if not CLOUDMERSIVE_KEY:
        info["mobile"] = True
        _line_info_cache[phone] = info
        return info

    digits = _digits_only(phone)
    data: Dict[str, Any] = {}
    status: Any = "n/a"
    try:
        resp = requests.post(
            "https://api.cloudmersive.com/validate/phonenumber/basic",
            json={"PhoneNumber": digits, "DefaultCountryCode": "US"},
            headers={"Apikey": CLOUDMERSIVE_KEY},
            timeout=6,
        )
        status = resp.status_code
        data = resp.json()
    except Exception as exc:
        LOG.warning("Cloudmersive lookup failed for %s (%s)", phone, exc)
        _line_info_cache[phone] = info
        return info

    LOG.debug("Cloudmersive response for %s: status=%s data=%s", digits, status, data)

    if status != 200:
        LOG.warning(
            "Cloudmersive lookup for %s failed with status %s; falling back to local validation",
            phone,
            status,
        )
        info["mobile"] = True
        _line_info_cache[phone] = info
        return info

    if not isinstance(data, dict) or "IsValid" not in data:
        LOG.warning(
            "Cloudmersive response for %s missing IsValid; falling back to local validation",
            phone,
        )
        info["mobile"] = True
        _line_info_cache[phone] = info
        return info

    info["valid"] = bool(data.get("IsValid"))
    info["country"] = str(data.get("CountryCode") or "US").upper()
    line_type = data.get("LineType")
    phone_type = data.get("PhoneNumberType")
    is_mobile = bool(data.get("IsMobile"))
    normalized_line = str(line_type or "").strip().lower()
    normalized_type = str(phone_type or "").strip().lower()
    if not is_mobile:
        if _is_explicit_mobile(line_type) or _is_explicit_mobile(phone_type):
            is_mobile = True
    if not is_mobile:
        if normalized_line == "fixedlineormobile" or normalized_type == "fixedlineormobile":
            is_mobile = True
    info["mobile"] = is_mobile

    _line_info_cache[phone] = info
    _line_type_cache[phone] = is_mobile
    _line_type_verified[phone] = is_mobile
    return info


def is_mobile_number(phone: str) -> bool:
    """Return True if *phone* is classified as a mobile line via Cloudmersive."""

    info = get_line_info(phone)
    return bool(info.get("mobile"))


def send_sms(
    phone: str,
    first: str,
    address: str,
    row_idx: int,
    follow_up: bool = False,
):
    if not SMS_ENABLE or not phone:
        LOG.debug("SMS disabled or missing phone; skipping send to %s", phone)
        return
    if SMS_TEST_MODE and SMS_TEST_NUMBER:
        phone = SMS_TEST_NUMBER
    msg_txt = SMS_FU_TEMPLATE if follow_up else SMS_TEMPLATE.format(first=first, address=address)
    digits = _digits_only(phone)
    for attempt in range(1, SMS_RETRY_ATTEMPTS + 1):
        try:
            msg_id = SMS_SENDER.send(digits, msg_txt) or ""
            if follow_up:
                mark_followup(row_idx)
                LOG.info(
                    "Follow‑up SMS sent to %s (row %s, attempt %s, msg_id=%s)",
                    digits, row_idx, attempt, msg_id
                )
            else:
                mark_sent(row_idx, msg_id)
                LOG.info(
                    "Initial SMS sent to %s (row %s, attempt %s, msg_id=%s)",
                    digits, row_idx, attempt, msg_id
                )
            return
        except Exception as e:
            LOG.debug("SMS attempt %s failed → retrying", attempt)
            time.sleep(5)
    LOG.error("SMS failed after %s attempts to %s", SMS_RETRY_ATTEMPTS, digits)

def check_reply(phone: str, since_iso: str) -> bool:
    """Return True if a reply from *phone* has been received since *since_iso*.

    SMS Gateway for Android does not expose an API for retrieving inbound
    messages in this script, so replies are not processed here and this always
    returns ``False``."""

    return False

# ───────────────────── scheduler helpers ─────────────────────
def _hour_floor(dt: datetime) -> datetime:
    dt = dt.astimezone(TZ)
    return dt.replace(minute=0, second=0, microsecond=0)


def _next_scheduler_run(now: datetime) -> datetime:
    """Return the next top-of-hour slot within work hours (7 days/week)."""

    now = now.astimezone(TZ)
    base = _hour_floor(now)

    if base.hour >= WORK_END:
        return (base + timedelta(days=1)).replace(
            hour=WORK_START, minute=0, second=0, microsecond=0
        )
    if base.hour < WORK_START:
        return base.replace(hour=WORK_START, minute=0, second=0, microsecond=0)

    next_slot = base if now == base else base + timedelta(hours=1)
    if next_slot.hour >= WORK_END:
        return (base + timedelta(days=1)).replace(
            hour=WORK_START, minute=0, second=0, microsecond=0
        )
    return next_slot


def run_hourly_scheduler(stop_event: Optional[threading.Event] = None) -> None:
    LOG.info(
        "Hourly scheduler loop starting (thread=%s)",
        threading.current_thread().name,
    )
    next_run = _next_scheduler_run(datetime.now(tz=TZ))
    while True:
        if stop_event and stop_event.is_set():
            LOG.info("Hourly scheduler stop requested; exiting loop")
            break
        try:
            now = datetime.now(tz=TZ)
            sleep_secs = max(0, (next_run - now).total_seconds())
            if sleep_secs > 0:
                LOG.debug(
                    "Sleeping %.0f seconds until next run at %s",
                    sleep_secs,
                    next_run.isoformat(),
                )
            if stop_event and stop_event.wait(timeout=sleep_secs):
                LOG.info("Hourly scheduler stop requested; exiting loop")
                break
            elif not stop_event:
                time.sleep(sleep_secs)

            run_time = _hour_floor(datetime.now(tz=TZ))
            hour = run_time.hour
            if hour >= WORK_END:
                LOG.info(
                    "Current hour %s outside work hours (%s–%s); skipping follow-up",
                    hour,
                    WORK_START,
                    WORK_END,
                )
            elif not FOLLOWUP_INCLUDE_WEEKENDS and _is_weekend(run_time):
                LOG.info("Weekend; skipping follow-up pass (FOLLOWUP_INCLUDE_WEEKENDS=false)")
            else:
                LOG.info("Starting follow-up pass at %s", run_time.isoformat())
                try:
                    _follow_up_pass()
                except Exception as exc:
                    LOG.exception("Error during follow-up pass: %s", exc)

            next_run = _next_scheduler_run(run_time + timedelta(seconds=1))
        except Exception as exc:
            LOG.exception("Hourly scheduler crashed; retrying in 30 seconds: %s", exc)
            if stop_event and stop_event.wait(timeout=30):
                LOG.info("Hourly scheduler stop requested during backoff; exiting loop")
                break
    LOG.info("Hourly scheduler loop terminated (thread=%s)", threading.current_thread().name)

# ───────────────────── follow‑up pass (UPDATED) ─────────────────────
def _follow_up_pass():
    resp = sheets_service.spreadsheets().values().get(
        spreadsheetId=GSHEET_ID,
        range=f"{GSHEET_TAB}!A:AA",
        majorDimension="ROWS",
        valueRenderOption="FORMATTED_VALUE",
    ).execute()
    all_rows = resp.get("values", [])
    if len(all_rows) <= 1:
        return

    last_row_idx = len(all_rows)
    # look back over recent rows for potential follow‑ups
    start_row_idx = max(2, last_row_idx - FU_LOOKBACK_ROWS)
    recent_rows = all_rows[start_row_idx - 1:]

    for sheet_row, row in enumerate(recent_rows, start=start_row_idx):
        row += [""] * (MIN_COLS - len(row))  # pad

        # skip if follow‑up already sent …
        if row[COL_FU_TS].strip():
            continue
        # … or if reply/manual note exists
        if row[COL_REPLY_FLAG].strip() or row[COL_MANUAL_NOTE].strip():
            continue

        # auto-check for replies since initial message
        if check_reply(row[COL_PHONE], row[COL_INIT_TS]):
            LOG.info(
                "Auto-detected reply for row %s (msg_id=%s)",
                sheet_row,
                row[COL_MSG_ID],
            )
            mark_reply(sheet_row)
            continue

        try:
            ts = datetime.fromisoformat(row[COL_INIT_TS])
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=TZ)
        except Exception:
            continue

        hrs = business_hours_elapsed(ts, now)
        if hrs < FU_HOURS:
            LOG.debug(
                "FU‑skip row %s – %.2f business hours elapsed", sheet_row, hrs
            )
            continue

        if now.hour < 9:
            LOG.debug(
                "FU‑skip row %s – before 9am", sheet_row
            )
            continue

        # debug line requested by Yoni
        LOG.debug(
            "FU‑check row %s → Col I='%s' Col J='%s' (both blank—sending FU)",
            sheet_row, row[COL_REPLY_FLAG], row[COL_MANUAL_NOTE]
        )

        send_sms(
            phone=row[COL_PHONE],
            first=row[COL_FIRST],
            address=row[COL_STREET],
            row_idx=sheet_row,
            follow_up=True,
        )

# ───────────────────── core row processor (UNCHANGED) ─────────────────────
def _expand_row(l: List[str], n: int = MIN_COLS) -> List[str]:
    return l + [""] * (n - len(l)) if len(l) < n else l

def process_rows(rows: List[Dict[str, Any]]):
    for r in rows:
        txt = (r.get("description", "") + " " + r.get("openai_summary", "")).strip()
        if not is_short_sale(txt):
            LOG.debug("SKIP non-short-sale %s (%s)", r.get("street"), r.get("zpid"))
            continue
        street = (r.get("street") or r.get("address") or "").strip()
        if street == "(Undisclosed Address)":
            LOG.debug("SKIP undisclosed address zpid %s", r.get("zpid"))
            continue
        zpid = str(r.get("zpid", ""))
        if zpid and not is_active_listing(zpid):
            LOG.info("Skip stale/off-market zpid %s", zpid)
            continue
        name = (r.get("agentName") or "").strip()
        if not name or TEAM_RE.search(name):
            LOG.debug("SKIP missing agent name for %s (%s)", r.get("street"), r.get("zpid"))
            continue
        state = r.get("state", "")
        phone_info = lookup_phone(name, state, r)
        phone = phone_info.get("number", "")
        email_info = lookup_email(name, state, r)
        email = email_info.get("email", "")
        if phone and phone_exists(phone):
            continue
        first, *last = name.split()
        now_iso = datetime.now(tz=TZ).isoformat()
        row_vals = [""] * MIN_COLS
        row_vals[COL_FIRST]   = first
        row_vals[COL_LAST]    = " ".join(last)
        row_vals[COL_PHONE]   = phone
        row_vals[COL_EMAIL]   = email
        row_vals[COL_PHONE_CONF] = phone_info.get("confidence", "")
        row_vals[COL_EMAIL_CONF] = email_info.get("confidence", "")
        reason = ""
        phone_reason = phone_info.get("reason", "")
        email_reason = email_info.get("reason", "")
        if "withheld_low_conf_mix" in {phone_reason, email_reason}:
            reason = "withheld_low_conf_mix"
        elif phone_reason == "no_personal_mobile":
            reason = "no_personal_mobile"
        elif email_reason == "no_personal_email":
            reason = "no_personal_email"
        row_vals[COL_CONTACT_REASON] = reason
        row_vals[COL_STREET]  = r.get("street", "")
        row_vals[COL_CITY]    = r.get("city", "")
        row_vals[COL_STATE]   = state
        row_vals[COL_INIT_TS] = now_iso
        row_idx = append_row(row_vals)
        if phone:
            seen_phones.add(phone)
            send_sms(phone, first, r.get("street", ""), row_idx)

# ───────────────────── main entry point & scheduler ─────────────────────
if __name__ == "__main__":
    try:
        stdin_txt = sys.stdin.read().strip()
        payload = json.loads(stdin_txt) if stdin_txt else None
    except json.JSONDecodeError:
        payload = None

    if payload and payload.get("listings"):
        LOG.info(
            "received %s listings directly in payload",
            len(payload["listings"])
        )
        process_rows(payload["listings"])
        LOG.info("Finished processing payload; exiting.")
    else:
        LOG.info("No JSON payload detected; entering hourly scheduler mode.")
        run_hourly_scheduler()
