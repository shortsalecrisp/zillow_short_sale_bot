from __future__ import annotations

import asyncio
import concurrent.futures
import html
import json
import logging
import os
import re
import sqlite3
import subprocess
import sys
import threading
import importlib.util
from pathlib import Path
from collections import Counter, defaultdict, deque
from datetime import datetime, timedelta
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode, unquote, urljoin

import time, random
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Tuple

import gspread
import pytz
import requests
from requests.adapters import HTTPAdapter, Retry
from requests import exceptions as req_exc
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build as gapi_build
from sms_providers import get_sender
try:
    from playwright.async_api import async_playwright
    from playwright.async_api import TimeoutError as PlaywrightTimeoutError
except ImportError:  # pragma: no cover - optional dependency
    async_playwright = None
    PlaywrightTimeoutError = None  # type: ignore
try:
    import dns.resolver  # type: ignore
except ImportError:  # pragma: no cover - optional dependency
    dns = None
else:
    dns = dns.resolver


class DomainBlockedError(RuntimeError):
    """Raised when a domain is blocked for the remainder of the run."""

_session = requests.Session()
_retries = Retry(
    total=0,
    connect=0,
    read=0,
    backoff_factor=0.0,
    status_forcelist=(),
    allowed_methods=None,
    raise_on_status=False,
)
_adapter = HTTPAdapter(max_retries=_retries)
_session.mount("http://", _adapter)
_session.mount("https://", _adapter)

_DEFAULT_TIMEOUT = 25
_PROXY_TARGETS = {
    "zillow.com",
    "www.zillow.com",
    "kw.com",
    "kellerwilliams.com",
    "facebook.com",
    "www.facebook.com",
    "instagram.com",
    "www.instagram.com",
    "linkedin.com",
    "www.linkedin.com",
    "remax.com",
    "coldwellbankerhomes.com",
    "compass.com",
}
ZILLOW_DOMAINS = ("zillow.com", "www.zillow.com")
BLOCKED_DOMAINS = ZILLOW_DOMAINS


def is_blocked_url(url: str) -> bool:
    return bool(url) and any(d in url.lower() for d in BLOCKED_DOMAINS)

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
HEADLESS_TIMEOUT_MS = int(os.getenv("HEADLESS_FETCH_TIMEOUT_MS", "20000"))
HEADLESS_NAV_TIMEOUT_MS = int(os.getenv("HEADLESS_NAV_TIMEOUT_MS", str(HEADLESS_TIMEOUT_MS)))
HEADLESS_WAIT_MS = int(os.getenv("HEADLESS_FETCH_WAIT_MS", "1200"))
HEADLESS_FACEBOOK_TIMEOUT_MS = int(os.getenv("HEADLESS_FACEBOOK_TIMEOUT_MS", str(HEADLESS_TIMEOUT_MS + 12000)))
HEADLESS_CONTACT_BUDGET = int(os.getenv("HEADLESS_CONTACT_BUDGET", "4"))
HEADLESS_OVERALL_TIMEOUT_S = int(os.getenv("HEADLESS_OVERALL_TIMEOUT_S", "50"))
PLAYWRIGHT_CONTACT_DOMAINS = {
    "facebook.com",
    "remax.com",
    "har.com",
    "century21.com",
}
CONTACT_JS_DOMAINS = {
    "boomtownroi.com",
    "kvcore.com",
    "realgeeks.com",
    "idxbroker.com",
    "placester.net",
}

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
_blocked_domains: Dict[str, str] = {}
_timeout_counts: Dict[str, int] = {}
_realtor_fetch_seen = False
cache_p: Dict[str, Any] = {}
cache_e: Dict[str, Any] = {}
CONTACT_CACHE_TTL_SECONDS = int(os.getenv("CONTACT_CACHE_TTL_SECONDS", str(24 * 3600)))
SEEN_ZPID_CACHE_SECONDS = int(os.getenv("SEEN_ZPID_CACHE_SECONDS", "300"))
_headless_loop: Optional[asyncio.AbstractEventLoop] = None
_headless_loop_thread: Optional[threading.Thread] = None
_headless_loop_lock = threading.Lock()


def _ensure_headless_loop() -> asyncio.AbstractEventLoop:
    global _headless_loop, _headless_loop_thread
    if _headless_loop and _headless_loop.is_running():
        return _headless_loop
    with _headless_loop_lock:
        if _headless_loop and _headless_loop.is_running():
            return _headless_loop
        loop = asyncio.new_event_loop()
        def _runner() -> None:
            asyncio.set_event_loop(loop)
            loop.run_forever()
        thread = threading.Thread(target=_runner, name="playwright-loop", daemon=True)
        thread.start()
        _headless_loop = loop
        _headless_loop_thread = thread
    return loop


def _contact_cache_key(agent: str, state: str, row_payload: Dict[str, Any]) -> str:
    brokerage_val = (
        row_payload.get("brokerageName")
        or row_payload.get("brokerage")
        or row_payload.get("broker")
        or ""
    )
    market = (
        row_payload.get("market")
        or row_payload.get("city")
        or state
        or ""
    )
    agent_norm = re.sub(r"\s+", " ", agent.strip().lower())
    brokerage_norm = re.sub(r"\s+", " ", str(brokerage_val).strip().lower())
    market_norm = re.sub(r"\s+", " ", str(market).strip().lower())
    return "|".join([agent_norm, brokerage_norm, market_norm])


def _contact_cache_get(cache_store: Dict[str, Any], key: str) -> Optional[Dict[str, Any]]:
    entry = cache_store.get(key)
    if not entry:
        return None
    expires_at = entry.get("expires_at", 0.0)
    if expires_at and expires_at < time.time():
        cache_store.pop(key, None)
        return None
    return entry.get("result")


def _contact_cache_set(cache_store: Dict[str, Any], key: str, result: Dict[str, Any]) -> None:
    cache_store[key] = {
        "result": result,
        "expires_at": time.time() + CONTACT_CACHE_TTL_SECONDS,
    }

# RapidAPI request coordination/state
_rapid_cache: Dict[str, Dict[str, Any]] = {}
_rapid_fetch_events: Dict[str, threading.Event] = {}
_rapid_cache_lock = threading.Lock()
_rapid_request_lock = threading.Lock()
_rapid_cooldown_until: float = 0.0
_rapid_logged: Set[str] = set()
_rapid_contact_cache: Dict[Tuple[str, str], Dict[str, Any]] = {}
RAPID_MIN_INTERVAL = float(os.getenv("RAPID_MIN_INTERVAL", "0.75"))
RAPID_COOLDOWN_SECONDS = float(os.getenv("RAPID_COOLDOWN_SECONDS", "18.0"))

def _http_get(
    url: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    extra_headers: Optional[Dict[str, str]] = None,
    timeout: int | float = _DEFAULT_TIMEOUT,
    rotate_user_agent: bool = False,
    respect_block: bool = True,
    block_on_status: bool = True,
    record_timeout: bool = True,
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
        if dom and dom in _blocked_domains:
            raise DomainBlockedError(f"blocked: {dom}")
        if respect_block and dom in _blocked_until and _blocked_until[dom] > time.time():
            raise DomainBlockedError(f"blocked: {dom}")
        try:
            resp = _session.get(
                url,
                params=params,
                headers=hdrs or None,
                timeout=timeout,
                proxies=proxy_cfg,
            )
        except req_exc.Timeout as exc:
            if record_timeout:
                _record_domain_timeout(dom)
            raise
        except _CONNECTION_ERRORS:
            _reset_timeout(dom)
            raise
        _reset_timeout(dom)
        status = resp.status_code
        if status in (403, 429) and dom and block_on_status:
            block_for = CSE_BLOCK_SECONDS if "googleapis.com" in dom else BLOCK_SECONDS
            reason = f"{status}"
            _mark_block(dom, seconds=block_for, reason=reason)
            raise DomainBlockedError(f"{status} received for {dom}")
        resp.raise_for_status()
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


def _env_flag(name: str, *, default: bool = False) -> bool:
    """Return a boolean flag from environment variables.

    Accepts common truthy values ("1", "true", "yes", "y", "on"). Any other
    value is treated as false, and the provided ``default`` is used when the
    variable is unset.
    """

    val = os.getenv(name)
    if val is None:
        return default
    return val.strip().lower() in {"1", "true", "yes", "y", "on"}


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
SEEN_ZPID_TAB  = "Seen Zpids"
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
    logging.warning("RAPID_KEY is empty; RapidAPI fallback enrichment will be skipped")
GOOD_STATUS    = {"FOR_SALE", "ACTIVE", "COMING_SOON", "PENDING", "NEW_CONSTRUCTION"}

TZ             = pytz.timezone(os.getenv("BOT_TIMEZONE", "US/Eastern"))
SCHEDULER_TZ   = pytz.timezone("America/New_York")
FU_HOURS       = float(os.getenv("FOLLOW_UP_HOURS", "6"))
FU_LOOKBACK_ROWS = int(os.getenv("FU_LOOKBACK_ROWS", "50"))
WORK_START     = int(os.getenv("WORK_START_HOUR", "8"))   # inclusive (8 am)
WORK_END       = int(os.getenv("WORK_END_HOUR", "20"))    # exclusive (8 pm cutoff)
FOLLOWUP_INCLUDE_WEEKENDS = _env_flag("FOLLOWUP_INCLUDE_WEEKENDS", default=False)
SCHEDULER_INCLUDE_WEEKENDS = _env_flag("SCHEDULER_INCLUDE_WEEKENDS", default=False)
APIFY_DECISION_LOCK_PATH = Path(os.getenv("APIFY_DECISION_LOCK_PATH", "/tmp/apify_hourly_decision.txt"))

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
CSE_PER_KEY_MIN_INTERVAL = float(os.getenv("CSE_PER_KEY_MIN_INTERVAL", "0.4"))
_cse_jitter_low = float(os.getenv("CSE_JITTER_LOW", "1.0"))
_cse_jitter_high = float(os.getenv("CSE_JITTER_HIGH", "2.6"))
_cse_window_seconds = float(os.getenv("CSE_WINDOW_SECONDS", "60"))
_cse_max_in_window = int(os.getenv("CSE_MAX_IN_WINDOW", "12"))
CSE_MAX_ATTEMPTS = int(os.getenv("CSE_MAX_ATTEMPTS", "3"))
if _cse_jitter_high < _cse_jitter_low:
    _cse_jitter_low, _cse_jitter_high = _cse_jitter_high, _cse_jitter_low

# How long to back off from a domain after a block (403/429). Default 15 minutes.
BLOCK_SECONDS = float(os.getenv("BLOCK_SECONDS", "900"))
CSE_BLOCK_SECONDS = float(os.getenv("CSE_BLOCK_SECONDS", str(BLOCK_SECONDS)))
JINA_BLOCK_SECONDS = float(os.getenv("JINA_BLOCK_SECONDS", str(BLOCK_SECONDS)))

CONTACT_DOMAIN_MIN_GAP = float(os.getenv("CONTACT_DOMAIN_MIN_GAP", "4.0"))
CONTACT_DOMAIN_GAP_JITTER = float(os.getenv("CONTACT_DOMAIN_GAP_JITTER", "1.5"))

CONTACT_EMAIL_MIN_SCORE = float(os.getenv("CONTACT_EMAIL_MIN_SCORE", "0.75"))
CONTACT_EMAIL_FALLBACK_SCORE = float(os.getenv("CONTACT_EMAIL_FALLBACK_SCORE", "0.45"))
CONTACT_EMAIL_LOW_CONF = float(os.getenv("CONTACT_EMAIL_LOW_CONF", str(CONTACT_EMAIL_FALLBACK_SCORE)))
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
ROLE_EMAIL_PREFIXES = {"info", "office", "admin"}
ALLOW_ROLE_EMAIL_FALLBACK = os.getenv("ALLOW_ROLE_EMAIL_FALLBACK", "false").lower() == "true"

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
COL_ZPID        = 27  # AB
COL_STATUS      = 28  # AC
COL_NOTES       = 29  # AD
MIN_COLS        = 30

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
_playwright_status_logged = False
_playwright_ready = False
_playwright_ready_lock = threading.Lock()
PLAYWRIGHT_BROWSER_CACHE = "/tmp/ms-playwright"


def _log_blocked_url(url: str) -> None:
    LOG.info("URL_BLOCKED url=%s", url)

async def _connect_playwright_browser(p) -> Tuple[Any, str]:
    browser = await p.chromium.launch(headless=True)
    LOG.info("PLAYWRIGHT_LOCAL_LAUNCH executable_path=%s", p.chromium.executable_path)
    return browser, "local"


async def _ensure_playwright_ready_async(logger: logging.Logger) -> bool:
    if async_playwright is None:
        logger.warning("PLAYWRIGHT_MISSING playwright not installed")
        return False
    if not os.getenv("PLAYWRIGHT_BROWSERS_PATH"):
        os.environ["PLAYWRIGHT_BROWSERS_PATH"] = PLAYWRIGHT_BROWSER_CACHE
    async with async_playwright() as p:
        executable_path = Path(p.chromium.executable_path)
    if not executable_path.exists():
        logger.warning(
            "PLAYWRIGHT_MISSING_EXECUTABLE path=%s; installing chromium",
            executable_path,
        )
        try:
            subprocess.run(
                [sys.executable, "-m", "playwright", "install", "chromium"],
                check=True,
            )
        except Exception as exc:
            logger.error("PLAYWRIGHT_INSTALL_FAILED err=%s", exc)
            return False
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            await browser.close()
        logger.info("PLAYWRIGHT_READY local chromium smoke test OK")
        return True
    except Exception as exc:
        logger.error("PLAYWRIGHT_BROWSER_ERROR smoke test failed err=%s", exc)
        return False


def ensure_playwright_ready(logger: Optional[logging.Logger] = None) -> bool:
    global _playwright_ready
    sink = logger or LOG
    if _playwright_ready:
        return True
    if not HEADLESS_ENABLED:
        sink.warning("PLAYWRIGHT_DISABLED HEADLESS_FALLBACK=false – set HEADLESS_FALLBACK=true to enable headless reviews")
        return False
    with _playwright_ready_lock:
        if _playwright_ready:
            return True
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None
        if loop and loop.is_running():
            future = asyncio.run_coroutine_threadsafe(_ensure_playwright_ready_async(sink), _ensure_headless_loop())
            _playwright_ready = bool(future.result())
        else:
            _playwright_ready = bool(asyncio.run(_ensure_playwright_ready_async(sink)))
        return _playwright_ready


def log_headless_status(logger: Optional[logging.Logger] = None) -> None:
    """Emit a one-time status line explaining whether Playwright will run."""
    global _playwright_status_logged
    if _playwright_status_logged:
        return
    _playwright_status_logged = True
    sink = logger or LOG
    if not HEADLESS_ENABLED:
        sink.warning(
            "PLAYWRIGHT_DISABLED HEADLESS_FALLBACK=false – set HEADLESS_FALLBACK=true to enable headless reviews"
        )
        return
    ensure_playwright_ready(sink)

# ───────────────────── regexes & misc helpers ─────────────────────
SHORT_RE = re.compile(r"\bshort\s+sale\b", re.I)
BAD_RE   = re.compile(
    r"\b(?:approved short sale|short sale approved|not a\s+short\s+sale)\b",
    re.I,
)
APPROVED_RE = re.compile(r"\bapproved\b", re.I)
NEGOTIATOR_RE = re.compile(r"\bnegotiator\b", re.I)
ATTORNEY_CONTEXT_RE = re.compile(
    r"(?:\battorney\b.{0,60}\b(?:negotiat|negotiation|negotiate|handling|handle|approval|short sale|third party)\b"
    r"|\b(?:negotiat|negotiation|negotiate|handling|handle|approval|short sale|third party)\b.{0,60}\battorney\b)",
    re.I,
)
TEAM_RE  = re.compile(r"^\s*the\b|\bteam\b", re.I)
IMG_EXT  = (".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp")
PHONE_RE = re.compile(
    r"(?<!\d)(?:\+?1[\s\-\.]*)?\(?\d{3}\)?[\s\-\.]*\d{3}[\s\-\.]*\d{4}(?!\d)"
)
EMAIL_RE = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.I)
OBFUSCATED_AT_RE = re.compile(r"(?:\[\s*at\s*\]|\(\s*at\s*\)|\{\s*at\s*\}|\bat\b)", re.I)
OBFUSCATED_DOT_RE = re.compile(r"(?:\[\s*dot\s*\]|\(\s*dot\s*\)|\{\s*dot\s*\}|\bdot\b)", re.I)

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
    "realtor.com",
)

CONTACT_SITE_PRIORITY: Tuple[str, ...] = (
    "realtor.com",
    "remax.com",
    "kw.com",
    "kellerwilliams.com",
    "bhhs.com",
    "compass.com",
    "coldwellbankerhomes.com",
    "facebook.com",
    "linkedin.com",
)

SOCIAL_DOMAINS: Set[str] = {"facebook.com", "linkedin.com", "instagram.com"}
CONTACT_ALLOWLIST_BASE: Set[str] = {
    "realtor.com",
    "nar.realtor",
    "facebook.com",
    "linkedin.com",
    "instagram.com",
    "kw.com",
    "kellerwilliams.com",
    "remax.com",
    "compass.com",
    "bhhs.com",
    "coldwellbankerhomes.com",
    "century21.com",
    "century21judgefite.com",
    "har.com",
    "exprealty.com",
    "realbroker.com",
    "realbrokerllc.com",
}
CONTACT_RESULT_DENYLIST: Set[str] = set(ZILLOW_DOMAINS) | {
    "realtor.com",
    "redfin.com",
    "homes.com",
    "trulia.com",
    "yelp.com",
}
CONTACT_MEDICAL_TERMS: Set[str] = {
    "clinic",
    "hospital",
    "health",
    "dental",
    "dentist",
    "orthopedic",
    "urgentcare",
    "cardio",
    "dermatology",
    "pediatric",
    "medical",
}
CONTACT_DIRECTORY_TERMS: Set[str] = {
    "mls",
    "realtor",
    "association",
    "board",
    "directory",
    "agent",
    "broker",
    "realestate",
    "realty",
}

# ───────────────────── Google / Sheets setup ─────────────────────
creds           = Credentials.from_service_account_info(SC_JSON, scopes=SCOPES)
sheets_service  = gapi_build("sheets", "v4", credentials=creds, cache_discovery=False)
gc              = gspread.authorize(creds)
ws              = gc.open_by_key(GSHEET_ID).worksheet(GSHEET_TAB)
_seen_zpid_ws: Optional[gspread.Worksheet] = None
_seen_zpid_ws_lock = threading.Lock()


def _get_seen_zpid_worksheet() -> gspread.Worksheet:
    global _seen_zpid_ws
    with _seen_zpid_ws_lock:
        if _seen_zpid_ws:
            return _seen_zpid_ws
        workbook = gc.open_by_key(GSHEET_ID)
        try:
            seen_ws = workbook.worksheet(SEEN_ZPID_TAB)
        except gspread.exceptions.WorksheetNotFound:
            seen_ws = workbook.add_worksheet(title=SEEN_ZPID_TAB, rows=1000, cols=1)
        try:
            header = seen_ws.acell("A1").value
            if not header or header.strip().lower() != "zpid":
                seen_ws.update(values=[["zpid"]], range_name="A1")
        except Exception as exc:
            LOG.warning("Unable to verify Seen Zpids header: %s", exc)
        _seen_zpid_ws = seen_ws
        return _seen_zpid_ws

def _normalize_phone_for_dedupe(phone: str) -> str:
    digits = re.sub(r"\D", "", phone or "")
    if len(digits) == 10:
        digits = "1" + digits
    return digits


def _normalize_agent_name(name: str) -> str:
    cleaned = re.sub(r"[^a-z0-9 ]+", " ", (name or "").lower())
    return re.sub(r"\s+", " ", cleaned).strip()


seen_phones: Set[str] = set()
seen_agents: Set[str] = set()
seen_zpids: Set[str] = set()
_seen_zpids_loaded_at = 0.0
_seen_contacts_loaded_at = 0.0
_seen_zpids_lock = threading.Lock()
_seen_contacts_lock = threading.Lock()


def load_seen_contacts(force: bool = False) -> Tuple[Set[str], Set[str]]:
    """Load agent names and phones present in the Google Sheet with caching."""

    global seen_phones, seen_agents, _seen_contacts_loaded_at
    with _seen_contacts_lock:
        now = time.time()
        if (seen_phones or seen_agents) and not force and (now - _seen_contacts_loaded_at) < SEEN_ZPID_CACHE_SECONDS:
            return set(seen_phones), set(seen_agents)
        try:
            resp = sheets_service.spreadsheets().values().get(
                spreadsheetId=GSHEET_ID,
                range=f"{GSHEET_TAB}!A:C",
                majorDimension="ROWS",
                valueRenderOption="FORMATTED_VALUE",
            ).execute()
        except Exception as exc:
            LOG.warning("Unable to refresh seen contacts from sheet: %s", exc)
            return set(seen_phones), set(seen_agents)
        rows = resp.get("values", [])
        phone_set: Set[str] = set()
        agent_set: Set[str] = set()
        for row in rows[1:]:
            row += [""] * 3
            first = str(row[COL_FIRST]).strip()
            last = str(row[COL_LAST]).strip()
            agent = _normalize_agent_name(f"{first} {last}".strip())
            if agent:
                agent_set.add(agent)
            phone = _normalize_phone_for_dedupe(str(row[COL_PHONE]))
            if phone:
                phone_set.add(phone)
        seen_phones = phone_set
        seen_agents = agent_set
        _seen_contacts_loaded_at = now
        LOG.info(
            "seen_contacts_loaded phones=%s agents=%s rows=%s",
            len(seen_phones),
            len(seen_agents),
            max(0, len(rows) - 1),
        )
        return set(seen_phones), set(seen_agents)


def load_seen_zpids(force: bool = False) -> Set[str]:
    """Load ZPIDs present in the Google Sheet with a short-lived cache."""

    global seen_zpids, _seen_zpids_loaded_at
    with _seen_zpids_lock:
        now = time.time()
        if seen_zpids and not force and (now - _seen_zpids_loaded_at) < SEEN_ZPID_CACHE_SECONDS:
            return set(seen_zpids)
        try:
            _get_seen_zpid_worksheet()
            resp = sheets_service.spreadsheets().values().get(
                spreadsheetId=GSHEET_ID,
                range=f"'{SEEN_ZPID_TAB}'!A:A",
                majorDimension="COLUMNS",
                valueRenderOption="UNFORMATTED_VALUE",
            ).execute()
        except Exception as exc:
            LOG.warning("Unable to refresh seen ZPIDs from sheet: %s", exc)
            return set(seen_zpids)
        col_vals = (resp.get("values") or [[]])[0]
        refreshed = {
            str(val).strip()
            for val in col_vals[1:]
            if str(val).strip()
        }
        refreshed = {z for z in refreshed if re.fullmatch(r"\d+", z)}
        seen_zpids = refreshed
        _seen_zpids_loaded_at = now
        LOG.info("seen_zpids_loaded=%s tab=%s", len(refreshed), SEEN_ZPID_TAB)
        if not refreshed:
            LOG.warning("seen_zpids_loaded=0 from tab=%s; dedupe may be ineffective", SEEN_ZPID_TAB)
        return set(seen_zpids)


def append_seen_zpids(zpids: Iterable[str]) -> None:
    cleaned: List[str] = []
    for zpid in zpids:
        val = str(zpid).strip()
        if not val:
            continue
        cleaned.append(val)
    if not cleaned:
        return
    with _seen_zpids_lock:
        new_vals = [val for val in cleaned if val not in seen_zpids]
        if not new_vals:
            return
        seen_zpids.update(new_vals)
    try:
        _get_seen_zpid_worksheet()
        data = [[val] for val in new_vals]
        sheets_service.spreadsheets().values().append(
            spreadsheetId=GSHEET_ID,
            range=f"'{SEEN_ZPID_TAB}'!A:A",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": data},
        ).execute()
        LOG.info("seen_zpids_appended=%s", len(new_vals))
    except Exception as exc:
        LOG.warning("Unable to append seen ZPIDs to sheet: %s", exc)


def record_seen_zpid(zpid: str) -> None:
    if not zpid:
        return
    cleaned = str(zpid).strip()
    if not cleaned:
        return
    with _seen_zpids_lock:
        seen_zpids.add(cleaned)


def dedupe_rows_by_zpid(
    rows: List[Dict[str, Any]],
    logger_obj: Optional[logging.Logger] = None,
    *,
    append_seen: bool = True,
) -> List[Dict[str, Any]]:
    """Filter *rows* against ZPIDs already present in the sheet cache."""

    cached = load_seen_zpids()
    fresh_rows: List[Dict[str, Any]] = []
    already_seen = 0
    fresh_zpids: List[str] = []
    for row in rows:
        zpid = str(row.get("zpid", "")).strip()
        if zpid and zpid in cached:
            already_seen += 1
            continue
        if zpid:
            cached.add(zpid)
            fresh_zpids.append(zpid)
        fresh_rows.append(row)
    if append_seen and fresh_zpids:
        append_seen_zpids(fresh_zpids)
    if logger_obj:
        logger_obj.info(
            "DEDUP received=%s already_seen=%s processing=%s",
            len(rows),
            already_seen,
            len(fresh_rows),
        )
    return fresh_rows

SCRAPE_SITES:  List[str] = []
DYNAMIC_SITES: Set[str]  = set()
PORTAL_DOMAINS: Set[str] = set(ZILLOW_DOMAINS) | {
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
EMAIL_ENRICH_DENYLIST: Set[str] = {
    "science.gov",
    "nih.gov",
    "ncbi.nlm.nih.gov",
    "pmc.ncbi.nlm.nih.gov",
}
EMAIL_ENRICH_DENY_TERMS = {
    "pubmed",
    "ncbi",
    "nih",
    "science",
    "journal",
    "research",
    "pmc",
    "clinical",
    "medical",
    "medicine",
    "health",
    "hospital",
    "obituary",
    "obituaries",
    "memorial",
}
_LICENSING_TERMS = {
    "realestate",
    "real-estate",
    "real_estate",
    "realtor",
    "realty",
    "license",
    "licensing",
    "dre",
    "dora",
    "mls",
    "broker",
    "division-of-real-estate",
    "department-of-real-estate",
    "real-estate-commission",
}
EMAIL_ALLOWED_PORTALS: Set[str] = set(PORTAL_DOMAINS) | {f"www.{dom}" for dom in PORTAL_DOMAINS}

SEARCH_BACKOFF_RANGE = (
    float(os.getenv("SEARCH_BACKOFF_MIN", "0.4")),
    float(os.getenv("SEARCH_BACKOFF_MAX", "1.2")),
)
SEARCH_TIMEOUT_TRIP = int(os.getenv("SEARCH_TIMEOUT_TRIP", "2"))
MAX_SEARCH_QUERIES = 3
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

def _phone_to_e164(phone: str) -> str:
    if not phone or not phonenumbers:
        return ""
    try:
        parsed = phonenumbers.parse(phone, "US")
        if not phonenumbers.is_possible_number(parsed):
            return ""
        return phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164)
    except Exception:
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
    if not e or "@" not in e:
        return False
    if e.startswith("@"):
        return False
    if e.lower().endswith(IMG_EXT):
        return False
    local, _, domain = e.rpartition("@")
    if not (local and domain and "." in domain):
        return False
    tld = domain.rsplit(".", 1)[-1]
    if not (2 <= len(tld) <= 8 and re.fullmatch(r"[A-Za-z]{2,8}", tld)):
        return False
    if re.search(r"\.(gov|edu|mil)$", domain, re.I):
        return False
    return True


def _normalize_obfuscated_email_text(text: str) -> str:
    if not text:
        return ""
    normalized = OBFUSCATED_AT_RE.sub("@", text)
    normalized = OBFUSCATED_DOT_RE.sub(".", normalized)
    normalized = re.sub(r"\s*@\s*", "@", normalized)
    normalized = re.sub(r"\s*\.\s*", ".", normalized)
    return normalized


def _extract_emails_with_obfuscation(text: str) -> List[Tuple[str, str]]:
    """Return emails (with snippets) after handling basic obfuscation tokens."""
    if not text:
        return []
    normalized = _normalize_obfuscated_email_text(text)
    results: List[Tuple[str, str]] = []
    seen: Set[str] = set()
    for match in EMAIL_RE.finditer(normalized):
        cleaned = clean_email(match.group())
        if not (cleaned and ok_email(cleaned)):
            continue
        if cleaned in seen:
            continue
        seen.add(cleaned)
        snippet = normalized[max(0, match.start() - 80): match.end() + 80]
        results.append((cleaned, " ".join(snippet.split())))
    return results


VCARD_EMAIL_RE = re.compile(r"email[^:]*[:;]\s*([^\s;]+)", re.I)
VCARD_TEL_RE = re.compile(r"tel[^:]*[:;]\s*([+0-9(). -]+)", re.I)

def _extract_vcard_contacts(text: str) -> Tuple[List[str], List[str]]:
    emails: List[str] = []
    phones: List[str] = []
    if not text or "vcard" not in text.lower():
        return emails, phones
    for mail_match in VCARD_EMAIL_RE.finditer(text):
        cleaned = clean_email(mail_match.group(1))
        if cleaned and ok_email(cleaned):
            emails.append(cleaned)
    for tel_match in VCARD_TEL_RE.finditer(text):
        formatted = fmt_phone(tel_match.group(1))
        if formatted and valid_phone(formatted):
            phones.append(formatted)
    return emails, phones


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


def _normalize_listing_text(text: str) -> str:
    if not text:
        return ""
    cleaned = html.unescape(str(text))
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def _extract_text_fragments(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, str):
        cleaned = _normalize_listing_text(value)
        return [cleaned] if cleaned else []
    if isinstance(value, (int, float)):
        cleaned = _normalize_listing_text(str(value))
        return [cleaned] if cleaned else []
    if isinstance(value, (list, tuple, set)):
        parts: List[str] = []
        for item in value:
            parts.extend(_extract_text_fragments(item))
        return parts
    if isinstance(value, dict):
        for key in ("text", "description", "remarks", "summary"):
            if key in value:
                return _extract_text_fragments(value.get(key))
    return []


def _nested_value(payload: Dict[str, Any], path: List[str]) -> Any:
    cur: Any = payload
    for key in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(key)
    return cur


_LISTING_TEXT_FIELDS = (
    "description",
    "listing_description",
    "openai_summary",
    "listingDescription",
    "homeDescription",
    "marketingDescription",
    "remarks",
    "publicRemarks",
    "brokerRemarks",
    "agentRemarks",
    "listingRemarks",
    "shortSaleDescription",
    "whatsSpecial",
    "whatsSpecialText",
)

_LISTING_TEXT_PATHS = (
    ("hdpData", "homeInfo", "description"),
    ("hdpData", "homeInfo", "homeDescription"),
    ("hdpData", "homeInfo", "listingDescription"),
    ("hdpData", "homeInfo", "whatsSpecial"),
    ("hdpData", "homeInfo", "whatsSpecialText"),
    ("property", "description"),
    ("property", "remarks"),
    ("listing", "description"),
    ("listing", "remarks"),
    ("listing", "listingRemarks"),
    ("property", "listingRemarks"),
)


def _collect_listing_text_fields(payload: Any) -> List[str]:
    parts: List[str] = []
    if isinstance(payload, dict):
        for key, value in payload.items():
            if key in _LISTING_TEXT_FIELDS:
                parts.extend(_extract_text_fragments(value))
            else:
                parts.extend(_collect_listing_text_fields(value))
    elif isinstance(payload, list):
        for item in payload:
            parts.extend(_collect_listing_text_fields(item))
    return parts


def _listing_text_from_payload(payload: Dict[str, Any]) -> str:
    parts: List[str] = []
    for key in _LISTING_TEXT_FIELDS:
        parts.extend(_extract_text_fragments(payload.get(key)))
    for path in _LISTING_TEXT_PATHS:
        parts.extend(_extract_text_fragments(_nested_value(payload, list(path))))
    if not parts:
        parts.extend(_collect_listing_text_fields(payload))
    if not parts:
        return ""
    seen: Set[str] = set()
    deduped: List[str] = []
    for part in parts:
        if part and part not in seen:
            seen.add(part)
            deduped.append(part)
    return " ".join(deduped).strip()


def _short_sale_text_from_payload(listing_text: str) -> str:
    if not listing_text:
        return ""
    return _normalize_listing_text(listing_text).strip().lower()


def _short_sale_exclusion_reason(text: str) -> Optional[str]:
    if not text:
        return None
    if BAD_RE.search(text):
        return "existing_rule"
    if APPROVED_RE.search(text):
        return "approved"
    if NEGOTIATOR_RE.search(text):
        return "negotiator"
    if ATTORNEY_CONTEXT_RE.search(text):
        return "attorney"
    return None


def _extract_address_fields(payload: Dict[str, Any]) -> Dict[str, str]:
    address = payload.get("address") or payload.get("property", {}).get("address") or {}
    if isinstance(address, str):
        return {"street": address.strip()} if address.strip() else {}
    if not isinstance(address, dict):
        address = {}
    street_candidates = [
        address.get("street"),
        address.get("streetAddress"),
        address.get("streetAddress1"),
        address.get("line1"),
        payload.get("streetAddress"),
    ]
    street = next((val for val in street_candidates if isinstance(val, str) and val.strip()), "")
    city = address.get("city") if isinstance(address.get("city"), str) else payload.get("city", "")
    state = address.get("state") if isinstance(address.get("state"), str) else payload.get("state", "")
    postal = address.get("zipcode") or address.get("zip") or address.get("postalCode") or payload.get("zipcode") or payload.get("zip")
    result: Dict[str, str] = {}
    if street:
        result["street"] = street.strip()
    if isinstance(city, str) and city.strip():
        result["city"] = city.strip()
    if isinstance(state, str) and state.strip():
        result["state"] = state.strip()
    if isinstance(postal, str) and postal.strip():
        result["zip"] = postal.strip()
    return result


def _merge_rapid_listing_data(row: Dict[str, Any], rapid_payload: Dict[str, Any]) -> None:
    if not row or not rapid_payload:
        return
    if not row.get("agentName"):
        for path in (
            ("listed_by", "name"),
            ("listingAgent", "name"),
            ("agent", "name"),
            ("listing_agent", "name"),
        ):
            agent_name = _nested_value(rapid_payload, list(path))
            if isinstance(agent_name, str) and agent_name.strip():
                row["agentName"] = agent_name.strip()
                break
    address_fields = _extract_address_fields(rapid_payload)
    if address_fields:
        if not row.get("street") and address_fields.get("street"):
            row["street"] = address_fields["street"]
        if not row.get("address") and address_fields.get("street"):
            row["address"] = address_fields["street"]
        if not row.get("city") and address_fields.get("city"):
            row["city"] = address_fields["city"]
        if not row.get("state") and address_fields.get("state"):
            row["state"] = address_fields["state"]
        if not row.get("zip") and address_fields.get("zip"):
            row["zip"] = address_fields["zip"]


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

def _rapid_status(zpid: str) -> Optional[int]:
    with _rapid_cache_lock:
        entry = _rapid_cache.get(zpid)
        if entry:
            return entry.get("status")
    return None


def rapid_property(zpid: str) -> Dict[str, Any]:
    if not (RAPID_KEY and zpid):
        return {}

    with _rapid_cache_lock:
        cached = _rapid_cache.get(zpid)
        if cached and cached.get("status") == 200:
            return cached.get("data", {}) or {}
        if cached and cached.get("status") is not None:
            return cached.get("data", {}) or {}
        waiter = _rapid_fetch_events.get(zpid)
        if waiter:
            event = waiter
        else:
            event = threading.Event()
            _rapid_fetch_events[zpid] = event
            waiter = None

    # Another caller is already fetching this ZPID; wait briefly for cache to fill.
    if waiter:
        event.wait(timeout=20)
        with _rapid_cache_lock:
            cached = _rapid_cache.get(zpid, {})
            return cached.get("data", {}) or {}

    status: Optional[int] = None
    data: Dict[str, Any] = {}
    now = time.time()
    global _rapid_cooldown_until

    if _rapid_cooldown_until and now < _rapid_cooldown_until:
        LOG.warning(
            "RapidAPI cooldown in effect for zpid=%s (%.1fs remaining)",
            zpid,
            _rapid_cooldown_until - now,
        )
        status = 429
    else:
        with _rapid_request_lock:
            delay = max(0.0, RAPID_MIN_INTERVAL - (time.time() - getattr(rapid_property, "_last_call", 0.0)))
            if delay > 0:
                time.sleep(delay)
            setattr(rapid_property, "_last_call", time.time())
            try:
                headers = {"X-RapidAPI-Key": RAPID_KEY, "X-RapidAPI-Host": RAPID_HOST}
                resp = _session.get(
                    f"https://{RAPID_HOST}/property",
                    params={"zpid": zpid},
                    headers=headers,
                    timeout=15,
                )
                status = resp.status_code
                if status == 200:
                    payload = resp.json()
                    data = payload.get("data") or payload
                elif status == 429:
                    _rapid_cooldown_until = time.time() + RAPID_COOLDOWN_SECONDS
                    LOG.warning("RapidAPI 429 for zpid=%s; entering cooldown", zpid)
                else:
                    LOG.debug("RapidAPI non-200 status=%s for zpid=%s", status, zpid)
            except Exception as exc:
                LOG.warning("RAPID_SOFT_FAIL fetch error for zpid=%s err=%s", zpid, exc)
                status = 520

    with _rapid_cache_lock:
        existing = _rapid_cache.get(zpid)
        if existing and existing.get("status") == 200:
            data = existing.get("data", {}) or data
            status = existing.get("status", status)
        final_status = status if status is not None else (existing.get("status") if existing else None)
        _rapid_cache[zpid] = {
            "data": data,
            "status": final_status,
            "timestamp": time.time(),
        }
        event = _rapid_fetch_events.pop(zpid, None)
        if event:
            event.set()
    return data


def _rapid_from_payload(row_payload: Dict[str, Any]) -> Dict[str, Any]:
    """Fetch Rapid API property details for this row without caching results."""

    zpid = str(row_payload.get("zpid", ""))
    return rapid_property(zpid) if zpid else {}


def _rapid_walk(payload: Any) -> Iterable[Tuple[str, str]]:
    stack: List[Tuple[str, Any]] = [("", payload)]
    while stack:
        path, value = stack.pop()
        if isinstance(value, dict):
            for key, val in value.items():
                new_path = f"{path}.{key}" if path else str(key)
                stack.append((new_path, val))
        elif isinstance(value, list):
            for idx, val in enumerate(value):
                new_path = f"{path}[{idx}]" if path else f"[{idx}]"
                stack.append((new_path, val))
        else:
            if value is None:
                continue
            text = str(value)
            if text.strip():
                yield path, text


_RAPID_PHONE_HINTS = [
    "agentphone",
    "agent_phone",
    "agentmobilephone",
    "agent_mobile_phone",
    "agentphonenumber",
    "mobilephone",
    "mobile_phone",
    "agentmobile",
    "contact_recipients",
    "listed_by",
]


def _rapid_path_has_phone_hint(path: str) -> bool:
    """Return True if any component of *path* implies it may contain a phone number."""
    lower_path = path.lower()
    return any(hint in lower_path for hint in _RAPID_PHONE_HINTS)


def _rapid_collect_contacts(payload: Any) -> Tuple[List[Dict[str, str]], List[Dict[str, str]], str]:
    phone_entries: List[Tuple[int, int, Dict[str, str]]] = []
    emails: List[Dict[str, str]] = []
    joined: List[str] = []
    seen_phone: Set[str] = set()
    seen_email: Set[str] = set()
    phone_counter = 0

    def _phone_rank(path: str) -> int:
        low = path.lower()
        if "mobile" in low:
            return 0
        if "agent" in low:
            return 1
        return 2

    def _block_phone_rank(label: str) -> int:
        low = label.lower()
        if any(term in low for term in ("cell", "mobile")):
            return 0
        if any(term in low for term in ("phone", "direct", "call")):
            return 1
        if any(term in low for term in ("office", "main", "brokerage")):
            return 3
        return 2

    def _add_phone_entry(number: Any, context: str, text: str, rank: int) -> None:
        nonlocal phone_counter
        formatted = fmt_phone(str(number))
        if not formatted:
            return
        e164 = _phone_to_e164(formatted)
        dedupe_key = e164 or formatted
        if dedupe_key in seen_phone:
            return
        seen_phone.add(dedupe_key)
        phone_entries.append(
            (rank, phone_counter, {"value": formatted, "context": context, "text": text, "e164": e164})
        )
        phone_counter += 1

    def _iter_contact_blocks() -> Iterable[Dict[str, Any]]:
        blocks: List[Dict[str, Any]] = []
        lb = payload.get("listed_by") or {}
        if isinstance(lb, dict) and lb:
            blocks.append(lb)
        contacts = payload.get("contact_recipients") or []
        if isinstance(contacts, list):
            blocks.extend([blk for blk in contacts if isinstance(blk, dict)])
        return blocks

    for blk in _iter_contact_blocks():
        label = str(blk.get("label", "") or blk.get("title", "") or "")
        context = " ".join([label, str(blk.get("display_name") or blk.get("full_name") or blk.get("name") or "")]).strip()
        rank = _block_phone_rank(label)
        phones_field = blk.get("phones") or blk.get("phone")
        if isinstance(phones_field, list):
            for ph in phones_field:
                if isinstance(ph, dict):
                    _add_phone_entry(ph.get("number") or ph.get("phone") or ph.get("value") or "", context or "contact_block", str(ph), rank)
                elif ph:
                    _add_phone_entry(ph, context or "contact_block", str(ph), rank)
        elif isinstance(phones_field, dict):
            _add_phone_entry(phones_field.get("number") or phones_field.get("phone") or "", context or "contact_block", str(phones_field), rank)
        elif phones_field:
            _add_phone_entry(phones_field, context or "contact_block", str(phones_field), rank)
        for key in ("agentPhone", "agentMobilePhone", "mobilePhone", "agent_phone", "agent_mobile_phone"):
            if blk.get(key):
                _add_phone_entry(blk[key], context or key, str(blk), 0)

    for path, text in _rapid_walk(payload):
        joined.append(text)
        for em in EMAIL_RE.finditer(text):
            cleaned = clean_email(em.group())
            if not cleaned or cleaned in seen_email:
                continue
            seen_email.add(cleaned)
            emails.append({"value": cleaned, "context": path, "text": text})
        if _rapid_path_has_phone_hint(path):
            rank = _phone_rank(path)
            for pm in PHONE_RE.finditer(text):
                _add_phone_entry(pm.group(), path, text, rank)
            digits_only = re.sub(r"\D", "", text)
            if digits_only and len(digits_only) >= 10:
                _add_phone_entry(digits_only[:10], path, text, rank)
    joined_text = " ".join(joined)
    ordered: List[Dict[str, str]] = []
    for _, _, entry in sorted(phone_entries, key=lambda t: (t[0], t[1])):
        ordered.append(entry)
        if len(ordered) >= 2:
            break
    return ordered, emails, joined_text


def _rapid_email_allowed(agent: str, email: str, *, context: str, payload_text: str) -> bool:
    if not email:
        return False
    domain = email.split("@", 1)[1].lower() if "@" in email else ""
    if domain.endswith(".gov") or ".gov." in domain or domain.endswith(".mil"):
        return False
    haystack = f"{context} {payload_text}".lower()
    for bad in ("obituary", "obituaries", "memorial", "medical", "clinic", "hospital", "pdf"):
        if bad in haystack:
            return False
    name_match = _email_matches_name(agent, email)
    page_name_hit = _page_mentions_agent(haystack, agent)
    return bool(name_match or page_name_hit)


def _rapid_select_email(agent: str, emails: List[Dict[str, str]], payload_text: str) -> Tuple[str, str]:
    for entry in emails:
        email = entry.get("value", "")
        context = " ".join([entry.get("context", ""), entry.get("text", "")]).strip()
        if not email or not ok_email(email):
            continue
        if _is_role_email(email) and not ALLOW_ROLE_EMAIL_FALLBACK:
            continue
        if not _rapid_email_allowed(agent, email, context=context, payload_text=payload_text):
            continue
        reason = "rapid_name_match" if _email_matches_name(agent, email) else "rapid_context_match"
        return email, reason
    return "", ""


def _rapid_plausible_phone(phone: str) -> bool:
    digits = _digits_only(phone)
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    if len(digits) != 10:
        return False
    if digits == "5555555555":
        return False
    if len(set(digits)) == 1:
        return False
    if _is_bad_area(digits[:3]):
        return False
    return True


def _rapid_rank_phones(agent: str, phones: List[Dict[str, str]]) -> Dict[str, Any]:
    ranked: List[Dict[str, Any]] = []
    for idx, entry in enumerate(phones):
        phone = entry.get("value", "")
        if not phone:
            continue
        info = get_line_info(phone)
        phone_type = str(info.get("type") or "unknown").strip()
        valid = bool(info.get("valid"))
        verified_mobile = bool(valid and info.get("mobile_verified"))
        digits = _digits_only(phone)
        is_us_10 = len(digits) == 10 or (len(digits) == 11 and digits.startswith("1"))
        if verified_mobile:
            score = 100
            reason = "rapid_score_verified_mobile"
        elif is_us_10:
            score = 60
            reason = "rapid_score_us_10_digit"
        elif _rapid_plausible_phone(phone):
            score = 40
            reason = "rapid_score_plausible_invalid"
        else:
            score = 10
            reason = "rapid_score_fallback"
        ranked.append(
            {
                "idx": idx,
                "phone": phone,
                "e164": entry.get("e164", ""),
                "score": score,
                "score_reason": reason,
                "verified_mobile": verified_mobile,
                "valid": valid,
                "type": phone_type or "unknown",
                "is_us_10": is_us_10,
            }
        )
    if not ranked:
        return {
            "selected_phone": "",
            "selected_reason": "",
            "selected_score": 0,
            "selected_verified_mobile": False,
            "candidates": [],
        }
    verified = next((item for item in ranked if item["verified_mobile"]), None)
    if verified:
        best = verified
    else:
        us_10 = next((item for item in ranked if item["is_us_10"]), None)
        best = us_10 or ranked[0]
    return {
        "selected_phone": best["phone"],
        "selected_reason": best["score_reason"],
        "selected_score": best["score"],
        "selected_verified_mobile": best["verified_mobile"],
        "candidates": ranked,
    }


def _rapid_select_phone(agent: str, phones: List[Dict[str, str]]) -> Tuple[str, str]:
    ranked = _rapid_rank_phones(agent, phones)
    return ranked.get("selected_phone", ""), ranked.get("selected_reason", "")


def _rapid_contact_snapshot(agent: str, row_payload: Dict[str, Any]) -> Dict[str, Any]:
    zpid = str(row_payload.get("zpid", "")).strip()
    if not zpid:
        return {
            "status": None,
            "data": {},
            "phones": [],
            "emails": [],
            "selected_phone": "",
            "phone_reason": "",
            "selected_email": "",
            "email_reason": "",
        }

    cache_key = (zpid, agent.strip().lower())
    with _rapid_cache_lock:
        cached = _rapid_contact_cache.get(cache_key)
        if cached:
            return cached

    data = rapid_property(zpid)
    status = _rapid_status(zpid)
    phones, emails, joined_text = _rapid_collect_contacts(data)
    ranked = _rapid_rank_phones(agent, phones)
    selected_phone = ranked.get("selected_phone", "")
    phone_reason = ranked.get("selected_reason", "")
    phone_score = ranked.get("selected_score", 0)
    phone_verified_mobile = bool(ranked.get("selected_verified_mobile"))
    selected_email, email_reason = _rapid_select_email(agent, emails, joined_text)
    cm_info = get_line_info(selected_phone) if selected_phone else {}
    phone_type = cm_info.get("type") or "unknown"
    rapid_fallback_phone = selected_phone if phones else ""
    rapid_primary_phone = selected_phone if phone_verified_mobile else ""

    snapshot = {
        "status": status,
        "data": data,
        "phones": phones,
        "emails": emails,
        "selected_phone": selected_phone,
        "phone_reason": phone_reason,
        "phone_score": phone_score,
        "rapid_fallback_phone": rapid_fallback_phone,
        "rapid_primary_phone": rapid_primary_phone,
        "rapid_candidates": ranked.get("candidates", []),
        "selected_email": selected_email,
        "email_reason": email_reason,
        "cloudmersive_type": phone_type,
        "phone_verified_mobile": phone_verified_mobile,
    }

    with _rapid_cache_lock:
        _rapid_contact_cache[cache_key] = snapshot
        if zpid not in _rapid_logged:
            LOG.info(
                "Rapid summary zpid=%s status=%s phones_found=%s emails_found=%s selected_phone=%s phone_reason=%s selected_email=%s email_reason=%s",
                zpid,
                status,
                len(phones),
                len(emails),
                selected_phone,
                phone_reason or "<none>",
                selected_email,
                email_reason or "<none>",
            )
            _rapid_logged.add(zpid)
            LOG.info(
                "phone_gate: rapid_verified_mobile=%s type=%s chosen=%s",
                phone_verified_mobile,
                phone_type or "unknown",
                selected_phone or "",
            )
    return snapshot


def _rapid_contact_normalized(agent: str, row_payload: Dict[str, Any]) -> Dict[str, Any]:
    zpid = str(row_payload.get("zpid", "")).strip()
    base = {
        "agent": agent,
        "zpid": zpid,
        "status": None,
        "data": {},
        "phones": [],
        "emails": [],
        "selected_phone": "",
        "phone_reason": "",
        "phone_score": 0,
        "rapid_fallback_phone": "",
        "rapid_primary_phone": "",
        "rapid_candidates": [],
        "selected_email": "",
        "email_reason": "",
        "cloudmersive_type": "",
        "phone_verified_mobile": False,
    }
    if not (RAPID_KEY and zpid):
        return base
    try:
        snapshot = _rapid_contact_snapshot(agent, row_payload)
    except Exception as exc:
        LOG.warning("RAPID_SOFT_FAIL contact snapshot agent=%s zpid=%s err=%s", agent, zpid, exc)
        return base
    if not isinstance(snapshot, dict):
        return base
    for key in base:
        if key in snapshot and snapshot.get(key) not in (None, ""):
            base[key] = snapshot.get(key)
    return base

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
            cleaned = val.strip()
            if is_blocked_url(cleaned):
                _log_blocked_url(cleaned)
                return
            urls.add(cleaned)

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
    snapshot = _rapid_contact_snapshot(agent_name, {"zpid": zpid})
    phone = snapshot.get("selected_phone", "") if snapshot else ""
    if phone:
        return phone, snapshot.get("phone_reason", "rapid")
    return "", ""

def _jitter() -> None:
    time.sleep(random.uniform(0.8, 1.5))

def _mark_block(dom: str, *, seconds: float = BLOCK_SECONDS, reason: str = "blocked") -> None:
    if not dom:
        return
    _blocked_until[dom] = time.time() + seconds
    _blocked_domains.setdefault(dom, reason)


def _reset_timeout(dom: str) -> None:
    if dom in _timeout_counts:
        _timeout_counts.pop(dom, None)


def _record_domain_timeout(dom: str) -> None:
    if not dom:
        return
    _timeout_counts[dom] = _timeout_counts.get(dom, 0) + 1
    if _timeout_counts[dom] >= 2:
        _mark_block(dom, reason="timeouts")
        raise DomainBlockedError(f"timeout threshold reached for {dom}")


def _cse_blocked() -> bool:
    return _blocked("www.googleapis.com") or _cse_blocked_until > time.time()

def _blocked(dom: str) -> bool:
    if not dom:
        return False
    return dom in _blocked_domains or _blocked_until.get(dom, 0.0) > time.time()

def _try_textise(dom: str, url: str) -> str:
    if is_blocked_url(url):
        _log_blocked_url(url)
        return ""
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


def _allowed_contact_domains(domain_hint: str) -> Optional[Set[str]]:
    allowed = set(CONTACT_ALLOWLIST_BASE) | set(SOCIAL_DOMAINS)
    dom = _domain(domain_hint)
    if dom:
        allowed.add(dom)
    return allowed or None

def _should_use_playwright_for_contact(dom: str, body: str = "", *, js_hint: bool = False) -> bool:
    domain = _domain(dom)
    if not domain:
        return False
    if domain in _ALWAYS_SKIP_DOMAINS:
        return False
    if domain in _CONTACT_DENYLIST or _blocked(domain):
        return False
    if domain in PLAYWRIGHT_CONTACT_DOMAINS:
        return True
    if js_hint or domain in CONTACT_JS_DOMAINS:
        return True
    if not (body or "").strip():
        return True
    return False

def _combine_playwright_snapshot(snapshot: Dict[str, Any]) -> str:
    if not snapshot:
        return ""
    meta = {
        "playwright_final_url": snapshot.get("final_url", ""),
        "playwright_visible_text": snapshot.get("visible_text", ""),
        "playwright_mailto_links": snapshot.get("mailto_links", []),
        "playwright_tel_links": snapshot.get("tel_links", []),
    }
    try:
        meta_json = json.dumps(meta, ensure_ascii=False)
    except Exception:
        meta_json = ""
    payload = f"<!--PLAYWRIGHT_SNAPSHOT {html.escape(meta_json)} -->" if meta_json else ""
    return f"{snapshot.get('html', '')}\n{payload}"

def _proxy_for_domain(domain: str) -> str:
    dom = _domain(domain)
    if not _PROXY_POOL or not dom:
        return ""
    proxy_targets = set(_PROXY_TARGETS) | set(CONTACT_SITE_PRIORITY) | set(ALT_PHONE_SITES) | SOCIAL_DOMAINS
    if dom in proxy_targets or any(dom.endswith(t) for t in proxy_targets):
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


def _is_publication_domain(host: str) -> bool:
    low = host.lower()
    if low in EMAIL_ENRICH_DENYLIST:
        return True
    return any(term in low for term in EMAIL_ENRICH_DENY_TERMS)


def _looks_real_estate_gov(host: str, path: str) -> bool:
    low_host = host.lower()
    low_path = path.lower()
    return any(term in low_host or term in low_path for term in _LICENSING_TERMS)


def _contact_source_allowed(
    url: str,
    brokerage_domain: str,
    trusted_domains: Set[str],
) -> bool:
    if not url:
        return True
    if is_blocked_url(url):
        _log_blocked_url(url)
        return False
    parsed = urlparse(url)
    host = (parsed.netloc or "").lower()
    dom = _domain(host)
    path = parsed.path or ""
    low_url = url.lower()

    if not host:
        return False
    if host in EMAIL_ENRICH_DENYLIST or dom in EMAIL_ENRICH_DENYLIST:
        return False
    if host.startswith("data.") and dom.endswith(".gov"):
        return False
    if dom.endswith(".gov") or host.endswith(".gov"):
        if not _looks_real_estate_gov(host, path):
            return False
    if _is_publication_domain(host):
        return False
    if any(term in low_url for term in EMAIL_ENRICH_DENY_TERMS):
        return False
    if "obituary" in low_url or "obituaries" in low_url:
        return False
    if path.lower().endswith(".pdf") or ".pdf" in path.lower():
        return False

    trusted_pool = set(trusted_domains) | set(TRUSTED_CONTACT_DOMAINS)
    if brokerage_domain:
        trusted_pool.add(brokerage_domain)
    allowed_pool = set(CONTACT_ALLOWLIST_BASE) | trusted_pool
    if host in allowed_pool or dom in allowed_pool or any(host.endswith(f".{td}") for td in allowed_pool):
        return True
    if host in SOCIAL_DOMAINS or dom in SOCIAL_DOMAINS:
        return True
    if host in EMAIL_ALLOWED_PORTALS or dom in EMAIL_ALLOWED_PORTALS:
        return True
    if _looks_real_estate_gov(host, path):
        return True
    if _is_real_estate_domain(host):
        return True
    if any(term in host for term in CONTACT_DIRECTORY_TERMS) or any(term in path.lower() for term in CONTACT_DIRECTORY_TERMS):
        return True
    return False

def _should_fetch(url: str, strict: bool = True) -> bool:
    if is_blocked_url(url):
        _log_blocked_url(url)
        return False
    dom = _domain(url)
    if dom in _ALWAYS_SKIP_DOMAINS:
        _mark_block(dom, reason="always_skip")
        return False
    if _blocked(dom):
        return False
    if strict and dom in _CONTACT_DENYLIST:
        _mark_block(dom, reason="denylist")
        return False
    return not (_is_banned(dom) and strict)

def fetch_simple(u: str, strict: bool = True):
    if not _should_fetch(u, strict):
        return None
    dom = _domain(u)
    try:
        r = _http_get(
            u,
            timeout=10,
            headers=_browser_headers(dom),
            rotate_user_agent=True,
        )
    except DomainBlockedError:
        return None
    except requests.HTTPError as exc:
        r = exc.response
        if r is None:
            raise
    except Exception as exc:
        LOG.debug("fetch_simple error %s on %s", exc, u)
        return None
    try:
        if r.status_code == 200:
            return r.text
        if r.status_code in (403, 429):
            _mark_block(dom)
        if r.status_code in (403, 451):
            txt = _try_textise(dom, u)
            if txt:
                return txt
    except DomainBlockedError:
        return None
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
    ]
    z403 = ratelimit = 0
    backoff = 1.0
    for url in variants:
        if _blocked(dom):
            return None
        try:
            r = _http_get(
                url,
                timeout=10,
                headers=_browser_headers(dom),
                rotate_user_agent=True,
                proxy=proxy_url,
            )
        except DomainBlockedError:
            return None
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
            _mark_block(dom, reason="429")
            return None
        elif r.status_code in (403, 451):
            _mark_block(dom, reason=str(r.status_code))
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
_CONTACT_DENYLIST = set(ZILLOW_DOMAINS) | {
    "forrent.com",
    "apartmenthomeliving.com",
    "rent.com",
    "apartments.com",
}
_ALWAYS_SKIP_DOMAINS = set(ZILLOW_DOMAINS) | _REALTOR_DOMAINS
_REALTOR_MAX_RETRIES = int(os.getenv("REALTOR_MAX_RETRIES", "5"))
_REALTOR_BACKOFF_BASE = float(os.getenv("REALTOR_BACKOFF_BASE", "3.0"))
_REALTOR_BACKOFF_CAP = float(os.getenv("REALTOR_BACKOFF_CAP", "20.0"))
_REALTOR_BACKOFF_JITTER = float(os.getenv("REALTOR_BACKOFF_JITTER", "1.75"))
_CONTACT_PAGE_CACHE: Dict[str, Tuple[str, bool, str]] = {}
_CONTACT_PAGE_CACHE_LOCK = threading.Lock()


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
    if not parsed.scheme or not parsed.netloc:
        return url
    scheme = parsed.scheme.lower()
    netloc = parsed.netloc.lower()
    if netloc.startswith("www."):
        netloc = netloc[4:]
    if netloc.endswith(":80"):
        netloc = netloc[:-3]
    elif netloc.endswith(":443"):
        netloc = netloc[:-4]
    path = parsed.path or "/"
    if path != "/" and path.endswith("/"):
        path = path.rstrip("/")
    filtered_qs = [
        (k, v)
        for k, v in parse_qsl(parsed.query, keep_blank_values=True)
        if k.lower() not in _TRACKING_QUERY_KEYS and not k.lower().startswith("utm_")
    ]
    new_query = urlencode(filtered_qs)
    cleaned = parsed._replace(
        scheme=scheme,
        netloc=netloc,
        path=path,
        query=new_query,
        fragment="",
    )
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


def fetch_text_cached(
    url: str,
    ttl_days: int = 14,
    *,
    respect_block: bool = True,
    allow_blocking: bool = True,
) -> Dict[str, Any]:
    norm = normalize_url(url)
    if is_blocked_url(norm):
        _log_blocked_url(norm)
        return {
            "url": norm,
            "fetched_at": time.time(),
            "ttl_seconds": 120,
            "http_status": 451,
            "extracted_text": "",
            "final_url": norm,
            "retry_needed": False,
        }
    dom = _domain(norm)
    if respect_block and dom and _blocked(dom):
        LOG.warning(
            "Skipping fetch for %s (blocked until %.0f)", dom, _blocked_until.get(dom, 0.0)
        )
        return {
            "url": norm,
            "fetched_at": time.time(),
            "ttl_seconds": 120,
            "http_status": 429,
            "extracted_text": "",
            "final_url": norm,
            "retry_needed": True,
        }
    if norm in _CACHE_DEDUPE_RUN:
        cached = cache_get(norm)
        if cached:
            return cached
    cached = cache_get(norm)
    if cached:
        return cached

    _respect_domain_delay(norm)

    def _screenshot_mirror(u: str) -> str:
        # Disabled because the upstream endpoint returns 400 and wastes time.
        return ""

    mirror = _mirror_url(norm) or f"https://r.jina.ai/{norm}"
    text = ""
    status = 0
    final_url = norm
    retry_needed = False

    try:
        resp = _http_get(
            mirror,
            timeout=12,
            headers=_browser_headers(_domain(mirror)),
            rotate_user_agent=True,
            respect_block=False,
            block_on_status=allow_blocking,
            record_timeout=allow_blocking,
        )
        text = resp.text if resp and resp.text else ""
        status = resp.status_code if resp else 0
        final_url = getattr(resp, "url", norm) if resp else norm
    except requests.HTTPError as exc:
        resp = exc.response
        text = resp.text if resp and resp.text else ""
        status = resp.status_code if resp else 0
        final_url = getattr(resp, "url", norm) if resp else norm
    except Exception:
        text = ""
        status = 0
        final_url = norm

    blocked_statuses = {403, 429, 451}
    if status in blocked_statuses and dom and allow_blocking:
        is_jina = dom in {"r.jina.ai", "duckduckgo.com", "www.duckduckgo.com"}
        base_block = JINA_BLOCK_SECONDS if is_jina else BLOCK_SECONDS
        block_for = max(base_block, 3600.0 if is_jina else base_block)
        LOG.info(
            "Jina fetch blocked for %s with %s; marking domain for retry", dom, status
        )
        _mark_block(dom, seconds=block_for)
    success = bool(text.strip()) and 200 <= status < 300

    if status == 200 and not text.strip() and "duckduckgo.com/html" in norm:
        try:
            ddg_resp = _http_get(
                norm,
                timeout=12,
                headers=_browser_headers(_domain(norm)),
                rotate_user_agent=True,
                respect_block=False,
                block_on_status=allow_blocking,
                record_timeout=allow_blocking,
            )
            if ddg_resp and ddg_resp.status_code == 200 and ddg_resp.text.strip():
                text = ddg_resp.text
                status = ddg_resp.status_code
                final_url = getattr(ddg_resp, "url", norm)
                success = True
                retry_needed = False
        except Exception:
            pass

    if not success:
        retry_needed = True

    if not success and dom:
        textise = _try_textise(dom, norm)
        if textise:
            text = textise
            status = status or 200
            success = True

    ttl_seconds = int(ttl_days * 86400 if success else 900)
    if success or text or status:
        cache_set(norm, text, status, final_url, ttl_seconds)
        _CACHE_DEDUPE_RUN.add(norm)

    return {
        "url": norm,
        "fetched_at": time.time(),
        "ttl_seconds": ttl_seconds,
        "http_status": status,
        "extracted_text": text,
        "final_url": final_url,
        "retry_needed": retry_needed,
    }


def _decode_duckduckgo_link(raw: str) -> str:
    parsed = urlparse(raw)
    qs = dict(parse_qsl(parsed.query))
    target = qs.get("uddg") or ""
    return unquote(target) if target else raw


def jina_cached_search(
    query: str,
    *,
    max_results: int = 18,
    ttl_days: int = 14,
    allowed_domains: Optional[Set[str]] = None,
) -> List[str]:
    if not query:
        return []
    if _blocked("r.jina.ai") or _blocked("duckduckgo.com") or _blocked("www.duckduckgo.com"):
        LOG.warning("Jina/DuckDuckGo blocked; skipping search for query %s", query)
        return []

    def _allowed(host: str) -> bool:
        if not allowed_domains:
            return True
        return any(host == dom or host.endswith(f".{dom}") for dom in allowed_domains)

    def _extract_hits(body: str, seen: Set[str]) -> List[str]:
        hits: List[str] = []
        for m in re.finditer(r"https?://duckduckgo\.com/l/\?[^\s\"]+", body):
            decoded = _decode_duckduckgo_link(html.unescape(m.group()))
            if is_blocked_url(decoded):
                _log_blocked_url(decoded)
                continue
            if decoded and decoded not in seen and _allowed(_domain(decoded)):
                seen.add(decoded)
                hits.append(decoded)
        for m in re.finditer(r"https?://[\w./?&%#=\-]+", body):
            candidate = html.unescape(m.group())
            if "duckduckgo.com" in candidate:
                continue
            if is_blocked_url(candidate):
                _log_blocked_url(candidate)
                continue
            if candidate not in seen and _allowed(_domain(candidate)):
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
        except Exception as exc:
            LOG.debug("jina_cached_search failed for %s: %s", query, exc)
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


def _targeted_contact_link_allowed(link: str, agent: str, brokerage: str = "", domain_hint: str = "") -> bool:
    if not link:
        return False
    if is_blocked_url(link):
        _log_blocked_url(link)
        return False
    dom = _domain(link)
    if not dom:
        return False
    allowed: Set[str] = set(CONTACT_ALLOWLIST_BASE) | set(SOCIAL_DOMAINS)
    broker_dom = _domain(domain_hint or brokerage) if (domain_hint or brokerage) else ""
    if broker_dom:
        allowed.add(broker_dom)
    if dom in allowed:
        return True
    if "mls" in dom or dom.endswith(".realtor"):
        return True
    return _plausible_agent_url(link, agent, brokerage, domain_hint)

def _focused_contact_link_allowed(link: str, agent: str, brokerage: str = "", domain_hint: str = "") -> bool:
    if not link:
        return False
    if is_blocked_url(link):
        _log_blocked_url(link)
        return False
    parsed = urlparse(link)
    host = parsed.netloc.lower()
    dom = _domain(link)
    if not dom:
        return False
    if dom.endswith(".gov") or host.endswith(".gov") or dom.endswith(".edu") or host.endswith(".edu"):
        return False
    if dom in CONTACT_RESULT_DENYLIST or host in CONTACT_RESULT_DENYLIST or dom in PORTAL_DOMAINS:
        return False
    if any(term in host for term in CONTACT_MEDICAL_TERMS):
        return False
    broker_dom = _domain(domain_hint or brokerage) if (domain_hint or brokerage) else ""
    if broker_dom and (dom == broker_dom or host.endswith(f".{broker_dom}")):
        return True
    if dom in SOCIAL_DOMAINS or host.endswith(tuple(SOCIAL_DOMAINS)):
        return True
    if _is_real_estate_domain(host):
        return True
    path = parsed.path.lower()
    directory_terms = (
        "agent",
        "realtor",
        "mls",
        "idx",
        "real-estate",
        "realestate",
        "properties",
        "homes",
        "listing",
        "listings",
        "brokerage",
        "team",
        "our-team",
        "people",
        "office",
        "contact",
    )
    if any(term in path for term in directory_terms):
        return True
    return _plausible_agent_url(link, agent, brokerage, domain_hint)


def _collect_targeted_candidates(
    urls: Iterable[str],
    agent: str,
    row_payload: Dict[str, Any],
) -> Tuple[List[Dict[str, Any]], bool]:
    candidates: List[Dict[str, Any]] = []
    blocked = False
    for url in urls:
        if not url:
            continue
        if is_blocked_url(url):
            _log_blocked_url(url)
            continue
        fetched = fetch_text_cached(url, ttl_days=14)
        status = int(fetched.get("http_status", 0) or 0)
        final_url = fetched.get("final_url") or url
        if status in {403, 429}:
            blocked = True
            _mark_block(_domain(final_url), seconds=max(BLOCK_SECONDS, 3600.0), reason="cse-block")
            break
        text = fetched.get("extracted_text", "")
        if not text:
            continue
        jsonld_cands, _, _, _ = _extract_jsonld_contacts_first(text, final_url, agent=agent, row_payload=row_payload)
        candidates.extend(jsonld_cands)
        candidates.extend(_extract_structured_candidates(text, final_url))
        candidates.extend(_extract_candidates_from_text(text, final_url))
    return candidates, blocked

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


def _score_contact_url(
    url: str, agent_tokens: List[str], brokerage: str, brokerage_domain: str
) -> float:
    if not url:
        return -float("inf")
    parsed = urlparse(url)
    host = parsed.netloc.lower()
    path = parsed.path.lower()
    score = 0.0

    if brokerage_domain and (host == brokerage_domain or host.endswith(f".{brokerage_domain}")):
        score += 4.5
    if any(host == dom or host.endswith(f".{dom}") for dom in SOCIAL_DOMAINS):
        score += 4.0
    if host in PORTAL_DOMAINS:
        score -= 2.5

    if any(tok and tok in host for tok in agent_tokens):
        score += 1.5
    if any(tok and tok in path for tok in agent_tokens):
        score += 2.5
    if "contact" in path:
        score += 2.0
    if any(key in path for key in ("agent", "realtor", "profile")):
        score += 1.5
    if any(key in path for key in ("cell", "mobile", "text")):
        score += 1.5
    if brokerage and brokerage.lower().replace(" ", "") in path:
        score += 0.5
    if any(bad in path for bad in ("/office", "contact-us", "about", "careers")):
        score -= 2.0
    if path.count("/") <= 1:
        score -= 0.25
    return score


def _rank_urls(
    urls: Iterable[str], agent: str, brokerage: str, *, domain_hint: str = "", limit: int = 10
) -> List[str]:
    agent_tokens = [tok.lower() for tok in agent.split() if tok]
    brokerage_domain = _domain(domain_hint or brokerage)
    scored: List[Tuple[float, str]] = []
    for url in urls:
        if not url:
            continue
        if is_blocked_url(url):
            _log_blocked_url(url)
            continue
        scored.append((_score_contact_url(url, agent_tokens, brokerage, brokerage_domain), url))
    scored.sort(key=lambda t: t[0], reverse=True)
    return [u for _, u in scored[:limit]]


def _slugify_agent(agent: str) -> str:
    tokens = [re.sub(r"[^a-z0-9]", "", part.lower()) for part in agent.split() if part.strip()]
    tokens = [tok for tok in tokens if tok]
    return "-".join(tokens)


def _brokerage_contact_urls(agent: str, brokerage: str, domain_hint: str = "") -> List[str]:
    if not (domain_hint or brokerage):
        return []
    domain = _domain(domain_hint or _infer_domain_from_text(brokerage) or _guess_domain_from_brokerage(brokerage))
    if not domain:
        return []
    if is_blocked_url(domain):
        _log_blocked_url(domain)
        return []
    base = f"https://{domain}".rstrip("/")
    slug = _slugify_agent(agent)
    slug_variants = [slug] if slug else []
    if slug and "-" in slug:
        slug_variants.append(slug.replace("-", ""))
    paths = [
        "/agents/",
        "/agents",
        "/our-team/",
        "/our-team",
        "/team/",
        "/team",
        "/people/",
        "/people",
        "/realtors/",
        "/realtors",
    ]
    for sv in slug_variants:
        paths.extend(
            [
                f"/agent/{sv}",
                f"/agents/{sv}",
                f"/our-team/{sv}",
            ]
        )
    urls = [f"{base}{p}" for p in paths]
    filtered: List[str] = []
    for url in urls:
        if is_blocked_url(url):
            _log_blocked_url(url)
            continue
        filtered.append(url)
    return list(dict.fromkeys(filtered))


def _extract_jsonld_contacts_first(
    html_text: str,
    source_url: str,
    *,
    agent: str = "",
    row_payload: Optional[Dict[str, Any]] = None,
) -> Tuple[List[Dict[str, Any]], bool, bool, Any]:
    candidates: List[Dict[str, Any]] = []
    agent_hit = False
    contact_found = False
    entries, soup = _jsonld_person_contacts(html_text)
    sameas_links: List[str] = []
    for entry in entries:
        meta_name = entry.get("name", "")
        if meta_name:
            agent_hit = agent_hit or _names_match(agent, meta_name)
        phones = entry.get("phones", [])
        emails = entry.get("emails", [])
        sameas_links.extend(entry.get("sameas", []))
        if not phones and not emails:
            continue
        contact_found = True
        candidates.append(
            {
                "source_url": source_url,
                "phones": [p for p in phones if p and valid_phone(p)],
                "emails": [e for e in emails if e and ok_email(e)],
                "evidence_snippet": "jsonld Person",
            }
        )
    if row_payload is not None:
        _record_sameas_links(row_payload, sameas_links)
    return candidates, agent_hit, contact_found, soup


def _extract_structured_candidates(html_text: str, source_url: str) -> List[Dict[str, Any]]:
    candidates: List[Dict[str, Any]] = []
    if not html_text:
        return candidates
    phones, mails, meta, info = extract_struct(html_text)
    seen: Set[Tuple[str, str]] = set()

    def _add(phone_list: List[str], email_list: List[str], evidence: str) -> None:
        nonlocal seen
        for ph in phone_list:
            if not (ph and valid_phone(ph)):
                continue
            key = ("phone", ph)
            if key in seen:
                continue
            seen.add(key)
            candidates.append(
                {
                    "source_url": source_url,
                    "phones": [ph],
                    "emails": [],
                    "evidence_snippet": evidence,
                }
            )
        for em in email_list:
            cleaned = clean_email(em)
            if not (cleaned and ok_email(cleaned)):
                continue
            key = ("email", cleaned)
            if key in seen:
                continue
            seen.add(key)
            candidates.append(
                {
                    "source_url": source_url,
                    "phones": [],
                    "emails": [cleaned],
                    "evidence_snippet": evidence,
                }
            )

    for anchor in info.get("tel", []):
        _add([anchor.get("phone", "")], [], anchor.get("context", ""))
    for anchor in info.get("mailto", []):
        _add([], [anchor.get("email", "")], anchor.get("context", ""))

    if phones or mails:
        _add(phones, mails, info.get("title") or "structured contact")

    for entry in meta:
        entry_phones = entry.get("phones", [])
        entry_emails = entry.get("emails", [])
        if not entry_phones and not entry_emails:
            continue
        context = entry.get("name") or entry.get("type") or "jsonld"
        _add(entry_phones, entry_emails, str(context))
    return candidates


# ───────────────────── contact candidate extraction & reranking ─────────────────────
_OPENAI_SPEC = importlib.util.find_spec("openai")
if _OPENAI_SPEC:
    import openai  # type: ignore
else:
    openai = None  # type: ignore


def _authority_contact_urls(
    agent: str,
    state: str,
    brokerage: str,
    domain_hint: str = "",
    limit: int = 4,
    *,
    max_queries: Optional[int] = None,
) -> Tuple[List[str], int]:
    queries = _dedupe_queries(
        [
            _compact_tokens(f'"{agent}"', state, "real estate license lookup"),
            _compact_tokens(f'"{agent}"', state, "real estate commission phone email"),
            _compact_tokens(f'"{agent}"', "REALTOR roster"),
            _compact_tokens(f'"{agent}"', brokerage, "agent roster"),
        ]
    )
    urls: List[str] = []
    used = 0
    allowed = {"realtor.com", "nar.realtor"}
    first_seen: str = ""
    for q in queries:
        if max_queries is not None and used >= max_queries:
            break
        used += 1
        for item in _safe_google_items(q, tries=2):
            link = item.get("link", "")
            if not link or link in urls:
                continue
            if is_blocked_url(link):
                _log_blocked_url(link)
                continue
            if not first_seen:
                first_seen = link
            dom = _domain(link)
            if dom.endswith(".gov") or dom in allowed:
                urls.append(link)
            if len(urls) >= limit:
                break
        if len(urls) >= limit:
            break
    if not urls and first_seen:
        urls.append(first_seen)
    return urls[:limit], used


def _candidate_urls(
    agent: str, state: str, row_payload: Dict[str, Any], *, domain_hint: str = ""
) -> Tuple[List[str], bool]:
    urls: List[str] = []
    hint_key = _normalize_override_key(agent, state)
    hint_urls = PROFILE_HINTS.get(hint_key) or PROFILE_HINTS.get(hint_key.lower(), [])
    for hint_url in hint_urls or []:
        if not hint_url:
            continue
        if is_blocked_url(hint_url):
            _log_blocked_url(hint_url)
            continue
        urls.append(hint_url)
    city = row_payload.get("city", "")
    postal_code = row_payload.get("zip", "")
    brokerage = (row_payload.get("brokerageName") or row_payload.get("brokerage") or "").strip()
    if urls:
        deduped = list(dict.fromkeys(urls))
        return deduped[:SEARCH_CONTACT_URL_LIMIT], False

    base_query_parts = [f'"{agent}"', "realtor", state, city, postal_code]
    base_query = " ".join(p for p in base_query_parts if p).strip()

    contact_query = _dedupe_queries([base_query])
    primary_query = contact_query[0] if contact_query else ""
    row_payload["_primary_contact_query"] = primary_query

    if urls or not primary_query:
        deduped = list(dict.fromkeys(urls))
        return deduped[:SEARCH_CONTACT_URL_LIMIT], False

    search_hits, _, _, search_exhausted = _contact_search_urls(
        agent,
        state,
        row_payload,
        domain_hint=domain_hint,
        brokerage=brokerage,
        limit=SEARCH_CONTACT_URL_LIMIT,
        include_exhausted=True,
        engine="google",
    )
    ranked_hits = _rank_urls(
        search_hits,
        agent,
        brokerage,
        domain_hint=domain_hint,
        limit=SEARCH_CONTACT_URL_LIMIT,
    )
    urls.extend(ranked_hits)
    deduped = list(dict.fromkeys(urls))
    return deduped[:SEARCH_CONTACT_URL_LIMIT], search_exhausted


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
    for mail, snippet in _extract_emails_with_obfuscation(text):
        key = ("email", mail)
        if key in seen:
            continue
        seen.add(key)
        candidates.append(
            {
                "source_url": source_url,
                "evidence_snippet": " ".join(snippet.split()),
                "phones": [],
                "emails": [mail],
            }
        )
    vcard_emails, vcard_phones = _extract_vcard_contacts(text)
    for phone in vcard_phones:
        key = ("phone", phone)
        if key in seen:
            continue
        seen.add(key)
        candidates.append(
            {
                "source_url": source_url,
                "evidence_snippet": "vcard",
                "phones": [phone],
                "emails": [],
            }
        )
    for mail in vcard_emails:
        key = ("email", mail)
        if key in seen:
            continue
        seen.add(key)
        candidates.append(
            {
                "source_url": source_url,
                "evidence_snippet": "vcard",
                "phones": [],
                "emails": [mail],
            }
        )
    return candidates


def _agent_contact_candidates_from_html(html_text: str, source_url: str, agent: str) -> List[Dict[str, Any]]:
    candidates: List[Dict[str, Any]] = []
    if not (html_text and agent.strip()):
        return candidates
    tokens = _agent_tokens(agent)
    parts = agent.split()
    first = parts[0] if parts else ""
    last = parts[-1] if parts else ""
    seen: Set[Tuple[str, str]] = set()

    def _add(phone_list: List[str], email_list: List[str], evidence: str) -> None:
        for ph in phone_list:
            if not (ph and valid_phone(ph)):
                continue
            key = ("phone", ph)
            if key in seen:
                continue
            seen.add(key)
            candidates.append(
                {
                    "source_url": source_url,
                    "phones": [ph],
                    "emails": [],
                    "evidence_snippet": evidence,
                }
            )
        for em in email_list:
            cleaned = clean_email(em)
            if not (cleaned and ok_email(cleaned) and _email_matches_name(agent, cleaned)):
                continue
            key = ("email", cleaned)
            if key in seen:
                continue
            seen.add(key)
            candidates.append(
                {
                    "source_url": source_url,
                    "phones": [],
                    "emails": [cleaned],
                    "evidence_snippet": evidence,
                }
            )

    def _context_hits_agent(ctx: str) -> bool:
        low = ctx.lower()
        has_label = any(term in low for term in ("mobile", "cell", "direct", "phone", "tel", "call", "text"))
        has_name = any(tok and tok in low for tok in tokens)
        return has_label or has_name

    jsonld_entries, soup = _jsonld_person_contacts(html_text)
    for entry in jsonld_entries:
        meta_name = entry.get("name", "")
        if meta_name and not _names_match(agent, meta_name):
            continue
        _add(entry.get("phones", []), entry.get("emails", []), meta_name or "jsonld person")

    phones, mails, meta, info = extract_struct(html_text)
    for anchor in info.get("tel", []):
        context = anchor.get("context", "")
        if not _context_hits_agent(context):
            continue
        _add([anchor.get("phone", "")], [], context)
    for anchor in info.get("mailto", []):
        _add([], [anchor.get("email", "")], anchor.get("context", "") or "mailto")
    for entry in meta:
        meta_name = entry.get("name", "")
        entry_type = entry.get("type", "")
        name_match = bool(meta_name and _names_match(agent, meta_name))
        personish = isinstance(entry_type, str) and ("person" in entry_type.lower() or "agent" in entry_type.lower())
        if not name_match and not personish:
            continue
        _add(entry.get("phones", []), entry.get("emails", []), meta_name or entry_type or "jsonld")

    low = html.unescape(html_text.lower())
    for num, details in proximity_scan(low, first_name=first.lower(), last_name=last.lower()).items():
        if details.get("weight", 0) < 2:
            continue
        snippet = " ".join(details.get("snippets", []))
        _add([num], [], snippet or "proximity")

    for m in EMAIL_RE.finditer(html_text):
        email = clean_email(m.group())
        if not (email and _email_matches_name(agent, email)):
            continue
        snippet = html_text[max(0, m.start() - 80): m.end() + 80]
        _add([], [email], " ".join(snippet.split()))
    for email, snippet in _extract_emails_with_obfuscation(html_text):
        _add([], [email], snippet)

    soup.decompose() if soup else None
    return candidates


def _score_contact_candidate(
    snippet: str,
    value: str,
    kind: str,
    *,
    source_url: str = "",
    brokerage_domain: str = "",
) -> float:
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
    host = _domain(source_url) if source_url else ""
    if any(host == dom or host.endswith(f".{dom}") for dom in SOCIAL_DOMAINS):
        score += 2.0
    if brokerage_domain and (host == brokerage_domain or host.endswith(f".{brokerage_domain}")):
        score += 1.5
    if host in PORTAL_DOMAINS:
        score -= 1.5
    return score


def _confidence_with_phone_type(score: float, phone: str) -> Tuple[int, str]:
    conf = max(0, min(100, int(score * 18)))
    if not phone:
        return conf, ""
    info = get_line_info(phone)
    phone_type = "mobile" if info.get("mobile") else "landline"
    if info.get("mobile"):
        conf = min(100, conf + 15)
    elif not info.get("valid"):
        conf = max(10, conf - 10)
    return conf, phone_type


def _heuristic_rerank(
    candidates: List[Dict[str, Any]], *, brokerage_domain: str = ""
) -> Dict[str, Any]:
    best_phone = ("", 0.0, "", "")
    best_email = ("", 0.0, "", "")
    for cand in candidates:
        snippet = cand.get("evidence_snippet", "")
        url = cand.get("source_url", "")
        for phone in cand.get("phones", []):
            score = _score_contact_candidate(
                snippet,
                phone,
                "phone",
                source_url=url,
                brokerage_domain=brokerage_domain,
            )
            if score > best_phone[1]:
                best_phone = (phone, score, url, snippet)
        for email in cand.get("emails", []):
            score = _score_contact_candidate(
                snippet,
                email,
                "email",
                source_url=url,
                brokerage_domain=brokerage_domain,
            )
            if score > best_email[1]:
                best_email = (email, score, url, snippet)
    phone_conf, phone_type = _confidence_with_phone_type(best_phone[1], best_phone[0])
    return {
        "best_phone": best_phone[0],
        "best_phone_confidence": phone_conf,
        "best_phone_type": phone_type,
        "best_phone_source_url": best_phone[2],
        "best_phone_evidence": best_phone[3],
        "best_email": best_email[0],
        "best_email_confidence": max(0, min(100, int(best_email[1] * 18))),
        "best_email_source_url": best_email[2],
        "best_email_evidence": best_email[3],
    }


def _apply_phone_type_boost(result: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(result, dict):
        return result
    phone = result.get("best_phone", "")
    if not phone:
        return result
    conf = int(result.get("best_phone_confidence", 0) or 0)
    info = get_line_info(phone)
    phone_type = "mobile" if info.get("mobile") else "landline"
    if info.get("mobile"):
        conf = min(100, conf + 15)
    elif not info.get("valid"):
        conf = max(10, conf - 10)
    result["best_phone_confidence"] = conf
    result["best_phone_type"] = phone_type
    return result


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


def rerank_contact_candidates(
    candidates: List[Dict[str, Any]], agent: str, *, brokerage_domain: str = ""
) -> Dict[str, Any]:
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
        return _apply_phone_type_boost(ai_choice)
    return _apply_phone_type_boost(_heuristic_rerank(candidates, brokerage_domain=brokerage_domain))


def _best_effort_contact(candidates: List[Dict[str, Any]]) -> Dict[str, Any]:
    best_phone = ("", 0, "", "")
    best_email = ("", 0, "", "")
    for cand in candidates:
        url = cand.get("source_url", "")
        snippet = cand.get("evidence_snippet", "")
        for phone in cand.get("phones", []):
            if not valid_phone(phone):
                continue
            info = get_line_info(phone)
            conf = 30
            if info.get("mobile"):
                conf += 15
            if not info.get("valid"):
                conf = max(10, conf - 8)
            if conf > best_phone[1]:
                best_phone = (phone, conf, url, snippet)
        for email in cand.get("emails", []):
            if not ok_email(email):
                continue
            conf = 35
            if _is_generic_email(email):
                conf = 20
            if conf > best_email[1]:
                best_email = (email, conf, url, snippet)
    return {
        "best_phone": best_phone[0],
        "best_phone_confidence": best_phone[1],
        "best_phone_source_url": best_phone[2],
        "best_phone_evidence": best_phone[3],
        "best_email": best_email[0],
        "best_email_confidence": best_email[1],
        "best_email_source_url": best_email[2],
        "best_email_evidence": best_email[3],
    }


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


def _plausible_agent_url(link: str, agent: str, brokerage: str = "", domain_hint: str = "") -> bool:
    if not link:
        return False
    parsed = urlparse(link)
    host = parsed.netloc.lower()
    path = parsed.path.lower()
    agent_tokens = [re.sub(r"[^a-z0-9]", "", part.lower()) for part in agent.split() if part.strip()]
    brokerage_slug = re.sub(r"[^a-z0-9]", "", brokerage.lower()) if brokerage else ""
    domain_hint_slug = re.sub(r"[^a-z0-9]", "", domain_hint.lower()) if domain_hint else ""
    brokerage_domain = _domain(domain_hint or brokerage)
    directory_terms = (
        "agent",
        "agents",
        "team",
        "our-team",
        "people",
        "real-estate-agents",
        "realtor",
        "staff",
        "directory",
    )
    real_estate_terms = (
        "realty",
        "realestate",
        "realtor",
        "homes",
        "properties",
        "estate",
        "broker",
        "mls",
        "kw.com",
        "kellerwilliams",
        "compass",
        "remax",
    )
    if brokerage_domain and (host == brokerage_domain or host.endswith(f".{brokerage_domain}")):
        return True
    if any(host == dom or host.endswith(f".{dom}") for dom in CONTACT_SITE_PRIORITY + ALT_PHONE_SITES):
        return True
    if any(tok and tok in host for tok in agent_tokens):
        return True
    if any(tok and tok in path for tok in agent_tokens):
        return True
    if host in PORTAL_DOMAINS and any(tok and tok in path for tok in agent_tokens):
        return True
    if brokerage_slug and (brokerage_slug in host or brokerage_slug in path):
        return True
    if domain_hint_slug and (domain_hint_slug in host or domain_hint_slug in path):
        return True
    if any(term in path for term in directory_terms):
        return True
    if any(term in host for term in real_estate_terms) and any(tok and tok in (host + path) for tok in agent_tokens):
        return True
    if path.count("/") <= 1 and (brokerage_slug or any(term in host for term in real_estate_terms)):
        return True
    return False


def _looks_directory_page(url: str) -> bool:
    parsed = urlparse(url)
    path = parsed.path.strip("/").lower()
    if not path or path in {"", "home", "index"}:
        return True
    return any(term in path for term in ("agents", "agent", "team", "our-team", "realtor", "people", "staff", "directory"))


def _page_mentions_agent(text: str, agent: str, soup: Any = None) -> bool:
    tokens = [part.lower() for part in agent.split() if len(part) > 1]
    if not tokens:
        return False
    low = text.lower()
    hits = sum(1 for tok in tokens if tok in low)
    if hits >= max(1, len(tokens) - 1):
        return True
    if soup and soup.title and soup.title.string:
        title_low = soup.title.string.lower()
        hits = sum(1 for tok in tokens if tok in title_low)
        if hits >= max(1, len(tokens) - 1):
            return True
    return False


def _agent_matches_context(agent: str, *, text: str = "", snippet: str = "", title: str = "") -> bool:
    if not agent.strip():
        return True
    context = " ".join(part for part in (title, snippet, text) if part).strip()
    if not context:
        return False
    return _page_mentions_agent(context, agent)


def _discover_agent_links(
    soup: Any,
    base_url: str,
    agent: str,
    *,
    brokerage: str = "",
    include_predictable: bool = False,
    limit: int = 10,
) -> Set[str]:
    discovered: Set[str] = set()
    parsed = urlparse(base_url)
    root = f"{parsed.scheme}://{parsed.netloc}" if parsed.scheme and parsed.netloc else ""
    if not soup or not BeautifulSoup:
        return set(list(discovered)[:limit])
    agent_tokens = [tok.lower() for tok in agent.split() if tok]
    brokerage_slug = re.sub(r"[^a-z0-9]", "", brokerage.lower()) if brokerage else ""
    for a in soup.find_all("a", href=True):
        href = a.get("href", "")
        if not href:
            continue
        full = urljoin(base_url, href)
        if is_blocked_url(full):
            _log_blocked_url(full)
            continue
        path = urlparse(full).path.lower()
        text = a.get_text(" ", strip=True).lower()
        if _plausible_agent_url(full, agent, brokerage, brokerage) or any(tok and (tok in text or tok in path) for tok in agent_tokens):
            discovered.add(full)
            continue
        if brokerage_slug and brokerage_slug in path:
            discovered.add(full)
            continue
        if any(term in path for term in ("agent", "agents", "team", "our-team", "realtor", "people", "staff")):
            discovered.add(full)
    if len(discovered) > limit:
        discovered = set(list(discovered)[:limit])
    return discovered


def _is_real_estate_domain(host: str) -> bool:
    if not host:
        return False
    low = host.lower()
    terms = (
        "realty",
        "realestate",
        "realtor",
        "homes",
        "properties",
        "estate",
        "broker",
        "kw.",
        "kellerwilliams",
        "compass",
        "remax",
        "exprealty",
        "bhhs",
        "sotheby",
        "century21",
        "c21",
        "coldwell",
    )
    return any(term in low for term in terms)

def _preferred_email_domains_for_text(*parts: str) -> Set[str]:
    haystack = " ".join(p.lower() for p in parts if p)
    matches: Set[str] = set()
    if not haystack:
        return matches
    for brand, domains in PREFERRED_BROKERAGE_EMAIL_DOMAINS.items():
        if brand in haystack:
            matches.update(domains)
    return matches


def _contact_cse_queries(
    agent: str,
    state: str,
    row_payload: Dict[str, Any],
    *,
    brokerage: str = "",
    domain_hint: str = "",
) -> Tuple[List[str], List[str]]:
    city = str(row_payload.get("city") or "").strip()
    name_term = f'"{agent}"'
    location_anchor = _compact_tokens(city, state)
    queries: List[str] = []

    def _add(q: str) -> None:
        if q and q not in queries:
            queries.append(q)

    _add(_compact_tokens(name_term, '"realtor"', location_anchor, "email"))
    _add(_compact_tokens(name_term, '"real estate agent"', location_anchor, "contact"))
    _add(_compact_tokens(name_term, '"realtor"', state, "contact", "profile"))
    if brokerage:
        _add(_compact_tokens(name_term, '"realtor"', brokerage, state, "email"))
        _add(_compact_tokens(name_term, '"real estate agent"', brokerage, "contact", state))
    if domain_hint:
        _add(_compact_tokens(name_term, f"site:{domain_hint}", "contact"))
        _add(_compact_tokens(name_term, f"site:{domain_hint}", "email"))
    _add(_compact_tokens(name_term, '"realtor"', state, "phone", "email"))
    return _dedupe_queries(queries), []


def _broaden_contact_query(agent: str, state: str, city: str, brokerage: str) -> str:
    return _compact_tokens(f'"{agent}"', state, city, "email", brokerage, "contact")


def _compact_url_log(urls: Iterable[str]) -> List[Dict[str, str]]:
    compact: List[Dict[str, str]] = []
    for url in urls:
        if not url:
            continue
        dom = _domain(url) or urlparse(url).netloc
        compact.append({"domain": dom, "url": url})
    return compact


def _contact_search_urls(
    agent: str,
    state: str,
    row_payload: Dict[str, Any],
    *,
    domain_hint: str = "",
    brokerage: str = "",
    limit: int = 10,
    include_exhausted: bool = False,
    engine: str = "google",
    allow_portals: bool = False,
) -> Tuple[List[str], bool, str] | Tuple[List[str], bool, str, bool]:
    """Issue enriched contact queries and return candidate URLs.

    Returns (urls, search_empty, cse_status) unless include_exhausted is True, in which
    case (urls, search_empty, cse_status, search_exhausted) is returned.
    """

    limit = max(1, min(limit, CONTACT_CSE_FETCH_LIMIT))
    target_count = min(limit, 5)
    city = str(row_payload.get("city") or "").strip()
    top5_log = row_payload.setdefault("_top5_log", {})
    strict_queries, _ = _contact_cse_queries(
        agent,
        state,
        row_payload,
        brokerage=brokerage,
        domain_hint=domain_hint,
    )
    urls: List[str] = []
    rejected_urls: List[Tuple[str, str]] = []
    search_empty = False
    search_exhausted = False
    cse_status = _cse_last_state
    search_cache = row_payload.setdefault("_contact_search_cache", {})

    def _select_until_target(
        raw_results: List[Dict[str, Any]],
        *,
        location_hint: str,
        existing: List[str],
        relaxed_mode: bool,
        fetch_ok: bool,
    ) -> Tuple[List[str], List[Tuple[str, str]]]:
        qualified: List[str] = []
        rejected: List[Tuple[str, str]] = []
        if not raw_results:
            return qualified, rejected
        page_size = 10
        for start in range(0, len(raw_results), page_size):
            batch = raw_results[start : start + page_size]
            filtered, rejected_batch = select_top_5_urls(
                batch,
                fetch_check=fetch_ok,
                relaxed=relaxed_mode,
                allow_portals=allow_portals,
                property_state=state,
                property_city=city,
                limit=target_count,
                existing=existing + qualified,
                location_hint=location_hint,
                agent=agent,
                brokerage=brokerage,
            )
            rejected.extend(rejected_batch)
            for url in filtered:
                if url not in qualified and url not in existing:
                    qualified.append(url)
            if len(qualified) >= target_count:
                break
        return qualified[:target_count], rejected
    try:
        cache_key = ""
        primary_query = strict_queries[0] if strict_queries else ""
        if not primary_query:
            search_empty = True
        else:
            cache_key = f"{engine}:{limit}:{primary_query}"
            if cache_key in search_cache:
                cached = search_cache[cache_key]
                return cached if include_exhausted else cached[:3]

            raw_results: List[Dict[str, Any]] = []
            fallback_results: List[Dict[str, Any]] = []
            selected_urls: List[str] = []
            broadened_used = False
            if engine == "google":
                used_queries: List[str] = []
                fetch_ok = _cse_last_state not in {"disabled"}
                relaxed_mode = _cse_last_state in {"disabled"}
                search_queries = strict_queries or [primary_query]
                for query in search_queries:
                    used_queries.append(query)
                    raw_results = google_cse_search(
                        query,
                        limit=CONTACT_CSE_FETCH_LIMIT,
                    )
                    if not raw_results and _cse_last_state == "disabled":
                        try:
                            raw_results = google_items(query)
                        except Exception:
                            raw_results = []
                    if not raw_results and _cse_last_state == "disabled":
                        rr = search_round_robin([query], per_query_limit=CONTACT_CSE_FETCH_LIMIT, engine_limit=1)
                        for attempt in rr:
                            for _, hits in attempt:
                                if hits:
                                    raw_results = hits
                                    break
                            if raw_results:
                                break
                    selected, rejected = _select_until_target(
                        raw_results,
                        location_hint=query,
                        existing=selected_urls,
                        relaxed_mode=relaxed_mode,
                        fetch_ok=fetch_ok,
                    )
                    LOG.info(
                        "EMAIL_SEARCH_RESULTS engine=%s query=%s returned=%s eligible=%s rejected=%s",
                        engine,
                        query,
                        len(raw_results),
                        len(selected),
                        len(rejected),
                    )
                    if selected:
                        LOG.info(
                            "EMAIL_SEARCH_SHORTLIST engine=%s urls=%s",
                            engine,
                            json.dumps(_compact_url_log(selected), separators=(",", ":")),
                        )
                    rejected_urls.extend(rejected)
                    for url in selected:
                        if url not in selected_urls:
                            selected_urls.append(url)
                    if len(selected_urls) >= target_count:
                        break
                if len(selected_urls) < target_count:
                    broadened_query = _broaden_contact_query(agent, state, city, brokerage)
                    if broadened_query and broadened_query not in used_queries:
                        used_queries.append(broadened_query)
                        broadened_results = google_cse_search(
                            broadened_query,
                            limit=CONTACT_CSE_FETCH_LIMIT,
                        )
                        broadened_selected, broadened_rejected = _select_until_target(
                            broadened_results,
                            location_hint=broadened_query,
                            existing=selected_urls,
                            relaxed_mode=relaxed_mode,
                            fetch_ok=fetch_ok,
                        )
                        LOG.info(
                            "EMAIL_SEARCH_RESULTS engine=%s query=%s returned=%s eligible=%s rejected=%s",
                            engine,
                            broadened_query,
                            len(broadened_results),
                            len(broadened_selected),
                            len(broadened_rejected),
                        )
                        if broadened_selected:
                            LOG.info(
                                "EMAIL_SEARCH_SHORTLIST engine=%s urls=%s",
                                engine,
                                json.dumps(_compact_url_log(broadened_selected), separators=(",", ":")),
                            )
                        broadened_used = bool(broadened_selected or broadened_results)
                        rejected_urls.extend(broadened_rejected)
                        for url in broadened_selected:
                            if url not in selected_urls:
                                selected_urls.append(url)
                if len(selected_urls) < target_count and _cse_last_state in {"blocked", "throttled", "disabled"}:
                    ddg_queries = used_queries or [primary_query]
                    for ddg_query in ddg_queries:
                        fallback_results, blocked = duckduckgo_search(
                            ddg_query,
                            limit=CONTACT_CSE_FETCH_LIMIT,
                            allowed_domains=None,
                            with_blocked=True,
                        )
                        if blocked:
                            _mark_block("duckduckgo.com", reason="blocked")
                        cse_status = "duckduckgo-blocked" if blocked else "duckduckgo"
                        fb_selected, fb_rejected = _select_until_target(
                            fallback_results,
                            location_hint=ddg_query,
                            existing=selected_urls,
                            relaxed_mode=relaxed_mode,
                            fetch_ok=fetch_ok,
                        )
                        LOG.info(
                            "EMAIL_SEARCH_RESULTS engine=duckduckgo query=%s returned=%s eligible=%s rejected=%s",
                            ddg_query,
                            len(fallback_results),
                            len(fb_selected),
                            len(fb_rejected),
                        )
                        if fb_selected:
                            LOG.info(
                                "EMAIL_SEARCH_SHORTLIST engine=duckduckgo urls=%s",
                                json.dumps(_compact_url_log(fb_selected), separators=(",", ":")),
                            )
                        rejected_urls.extend(fb_rejected)
                        for url in fb_selected:
                            if url not in selected_urls:
                                selected_urls.append(url)
                        if len(selected_urls) >= target_count:
                            break
            elif engine in {"duckduckgo", "jina"}:
                fetch_ok = _cse_last_state not in {"disabled"}
                relaxed_mode = _cse_last_state in {"disabled"}
                ddg_query = primary_query or (strict_queries[0] if strict_queries else "")
                fallback_results, blocked = duckduckgo_search(
                    ddg_query,
                    limit=CONTACT_CSE_FETCH_LIMIT,
                    allowed_domains=None,
                    with_blocked=True,
                )
                if blocked:
                    _mark_block("duckduckgo.com", reason="blocked")
                cse_status = "duckduckgo-blocked" if blocked else "duckduckgo"
                selected_urls, rejected_urls = _select_until_target(
                    fallback_results,
                    location_hint=ddg_query,
                    existing=[],
                    relaxed_mode=relaxed_mode,
                    fetch_ok=fetch_ok,
                )
                LOG.info(
                    "EMAIL_SEARCH_RESULTS engine=duckduckgo query=%s returned=%s eligible=%s rejected=%s",
                    ddg_query,
                    len(fallback_results),
                    len(selected_urls),
                    len(rejected_urls),
                )
                if selected_urls:
                    LOG.info(
                        "EMAIL_SEARCH_SHORTLIST engine=duckduckgo urls=%s",
                        json.dumps(_compact_url_log(selected_urls), separators=(",", ":")),
                    )
            if engine == "google":
                cse_status = _cse_last_state
                if broadened_used:
                    cse_status = f"{cse_status}-broadened"
            else:
                cse_status = cse_status or engine
            search_empty = not bool(selected_urls)
            urls = selected_urls
            if selected_urls or rejected_urls:
                top5_log[engine] = {
                    "urls": selected_urls[:target_count],
                    "rejected": rejected_urls,
                }
                if search_empty or len(selected_urls) >= target_count:
                    LOG.info(
                        "TOP5_SELECTED urls=%s rejected=%s",
                        selected_urls[:target_count],
                        rejected_urls,
                    )
        search_exhausted = True
    except Exception:
        LOG.exception("contact search failed for %s %s", agent, state)
        search_empty = True
        search_exhausted = True
        cse_status = _cse_last_state or "error"
    result = (urls[:limit], search_empty, cse_status, search_exhausted)
    if cache_key:
        search_cache[cache_key] = result
    return result if include_exhausted else result[:3]


def _normalize_contact_search_result(result: Tuple[Any, ...]) -> Tuple[List[str], bool, str, bool]:
    """Return a 4-tuple from _contact_search_urls results, tolerating monkeypatched shapes."""
    try:
        urls, search_empty, cse_status, search_exhausted = result
    except ValueError:
        urls, search_empty, cse_status = result
        search_exhausted = False
    return list(urls), bool(search_empty), str(cse_status), bool(search_exhausted)


def _phone_candidates_from_email_search(
    agent: str,
    state: str,
    email: str,
    row_payload: Dict[str, Any],
    *,
    brokerage: str = "",
) -> List[Dict[str, Any]]:
    city = str(row_payload.get("city") or "").strip()
    cleaned = clean_email(email)
    if not cleaned:
        return []
    cache = row_payload.setdefault("_phone_email_probe_cache", {})
    cache_key = cleaned.lower()
    if cache_key in cache:
        return cache.get(cache_key, [])

    queries = [
        _compact_tokens(f'"{cleaned}"', f'"{agent}"', "phone"),
        _compact_tokens(f'"{cleaned}"', state, "realtor", "phone"),
    ]
    if brokerage:
        queries.append(_compact_tokens(f'"{cleaned}"', f'"{brokerage}"', "phone"))

    candidates: List[Dict[str, Any]] = []
    for query in queries:
        if not query:
            continue
        try:
            results = google_cse_search(query, limit=CONTACT_CSE_FETCH_LIMIT)
            selected, _ = select_top_5_urls(
                results,
                fetch_check=True,
                relaxed=False,
                property_state=state,
                location_hint=query,
                property_city=city,
                agent=agent,
                brokerage=brokerage,
            )
            if not selected and _cse_last_state in {"blocked", "throttled", "disabled"}:
                fallback_results, _ = duckduckgo_search(
                    query,
                    limit=CONTACT_CSE_FETCH_LIMIT,
                    allowed_domains=None,
                    with_blocked=False,
                )
                selected, _ = select_top_5_urls(
                    fallback_results,
                    fetch_check=True,
                    relaxed=True,
                    property_state=state,
                    location_hint=query,
                    property_city=city,
                    agent=agent,
                    brokerage=brokerage,
                )
            for url in selected:
                page, _, _ = fetch_contact_page(url)
                if not page:
                    continue
                candidates.extend(_agent_contact_candidates_from_html(page, url, agent))
            if candidates:
                break
        except Exception:
            LOG.exception("email+phone CSE probe failed for %s", query)
            continue

    cache[cache_key] = candidates
    return candidates


def enrich_contact(agent: str, state: str, row_payload: Dict[str, Any]) -> Dict[str, Any]:
    brokerage = (row_payload.get("brokerageName") or row_payload.get("brokerage") or "").strip()
    domain_hint = row_payload.get("domain_hint", "").strip()
    brokerage_domain_hint = _domain(domain_hint or brokerage)
    brokerage_domain = brokerage_domain_hint
    urls, search_exhausted = _candidate_urls(agent, state, row_payload, domain_hint=domain_hint)
    urls = list(dict.fromkeys(urls))
    selected_domains = sorted(
        {d for d in (_domain(u) for u in urls) if d}
    )
    LOG.info(
        "search_sources_used: %s",
        ",".join(selected_domains) if selected_domains else "<none>",
    )
    search_disabled = bool(urls)
    candidates: List[Dict[str, Any]] = []
    headless_budget = HEADLESS_CONTACT_BUDGET
    contact_found = False
    brokerage_crawl_planned: Set[str] = set()

    prefetch_urls = list(urls)
    prefetch_pages: List[Tuple[str, str]] = []
    for url in prefetch_urls:
        try:
            page, _, _ = fetch_contact_page(url)
            if page:
                prefetch_pages.append((url, page))
        except Exception:
            pass
    prefetch_cache = {u: p for u, p in prefetch_pages}

    def _collect_from_urls(
        urls_to_fetch: Iterable[str], *, prefer_headless: bool = False
    ) -> Tuple[Set[str], bool]:
        nonlocal headless_budget, contact_found
        discovered: Set[str] = set()
        agent_seen = False
        for url in urls_to_fetch:
            if not url:
                continue
            dom = _domain(url)
            if dom in _CONTACT_DENYLIST:
                _mark_block(dom, reason="denylist")
                continue
            if _blocked(dom):
                continue
            is_portal = dom in PORTAL_DOMAINS
            fetched: Dict[str, Any] = {}
            cached_prefetch = prefetch_cache.get(url)
            use_headless = prefer_headless and headless_budget > 0 and cached_prefetch is None
            if cached_prefetch is not None:
                fetched = {
                    "url": url,
                    "extracted_text": cached_prefetch,
                    "final_url": url,
                }
            elif use_headless:
                page, _, _ = fetch_contact_page(url)
                if page:
                    fetched = {
                        "url": url,
                        "extracted_text": page,
                        "final_url": url,
                    }
                    headless_budget -= 1
                else:
                    fetched = fetch_text_cached(url)
            else:
                fetched = fetch_text_cached(url)
            text = fetched.get("extracted_text", "")
            final_url = fetched.get("final_url") or url
            if not text:
                continue
            jsonld_cands, jsonld_hit, jsonld_contact, soup = _extract_jsonld_contacts_first(
                text, final_url, agent=agent, row_payload=row_payload
            )
            if jsonld_cands:
                candidates.extend(jsonld_cands)
                if any(c.get("phones") or c.get("emails") for c in jsonld_cands):
                    contact_found = True
            agent_seen = agent_seen or jsonld_hit
            if jsonld_contact:
                discovered.update(
                    _discover_agent_links(
                        soup,
                        final_url,
                        agent,
                        brokerage=brokerage,
                        include_predictable=_looks_directory_page(final_url),
                    )
                )
                continue
            if not is_portal:
                structured_candidates = _extract_structured_candidates(text, final_url)
                if structured_candidates:
                    candidates.extend(structured_candidates)
                    if any(c.get("phones") or c.get("emails") for c in structured_candidates):
                        contact_found = True
                text_candidates = _extract_candidates_from_text(text, final_url)
                if text_candidates:
                    candidates.extend(text_candidates)
                    if any(c.get("phones") or c.get("emails") for c in text_candidates):
                        contact_found = True
            if not soup and BeautifulSoup:
                try:
                    soup = BeautifulSoup(text, "html.parser")
                except Exception:
                    soup = None
            discovered.update(
                _discover_agent_links(
                    soup,
                    final_url,
                    agent,
                    brokerage=brokerage,
                    include_predictable=_looks_directory_page(final_url),
                )
            )
            if _page_mentions_agent(text, agent, soup):
                agent_seen = True
        return discovered, agent_seen

    if prefetch_urls:
        _collect_from_urls(prefetch_urls, prefer_headless=True)
        if not candidates and prefetch_pages:
            best_email = ""
            best_phone = ""
            best_email_url = ""
            best_phone_url = ""
            for url, page in prefetch_pages:
                for match in EMAIL_RE.finditer(page):
                    mail = clean_email(match.group())
                    if mail and not best_email:
                        best_email = mail
                        best_email_url = url
                if not best_phone:
                    pm = PHONE_RE.search(page)
                    if pm:
                        phone = fmt_phone(pm.group())
                        if phone:
                            best_phone = phone
                            best_phone_url = url
            if best_email or best_phone:
                return {
                    "best_phone": best_phone,
                    "best_phone_confidence": 85 if best_phone else 0,
                    "best_phone_source_url": best_phone_url,
                    "best_phone_evidence": "prefetch_headless" if best_phone else "",
                    "best_email": best_email,
                    "best_email_confidence": 85 if best_email else 0,
                    "best_email_source_url": best_email_url,
                    "best_email_evidence": "prefetch_headless" if best_email else "",
                }

    pending_urls: deque[str] = deque(urls[:MAX_CONTACT_URLS])
    seen_urls: Set[str] = set()
    agent_page_seen = False

    def _enqueue(new_urls: Iterable[str]) -> None:
        for nu in new_urls:
            if not nu or nu in seen_urls or nu in pending_urls:
                continue
            if len(seen_urls) + len(pending_urls) >= MAX_CONTACT_URLS:
                break
            pending_urls.append(nu)

    def _process_queue() -> None:
        nonlocal agent_page_seen, contact_found
        while pending_urls and len(seen_urls) < MAX_CONTACT_URLS and not contact_found:
            batch: List[str] = []
            while pending_urls and len(batch) < max(1, HEADLESS_CONTACT_BUDGET):
                next_url = pending_urls.popleft()
                if next_url in seen_urls:
                    continue
                seen_urls.add(next_url)
                batch.append(next_url)
            if not batch:
                break
            discovered, seen_agent = _collect_from_urls(batch, prefer_headless=True)
            agent_page_seen = agent_page_seen or seen_agent
            ranked_new = _rank_urls(
                discovered,
                agent,
                brokerage,
                domain_hint=domain_hint,
                limit=MAX_CONTACT_URLS,
            )
            _enqueue(ranked_new)

    _process_queue()
    quality = _candidate_quality(candidates)
    needs_fallback = (
        (quality["phones_found"] == 0 and quality["emails_found"] == 0)
        or (quality["phones_found"] > 0 and quality["all_office"])
        or (quality["emails_found"] > 0 and quality["all_generic_email"])
    )
    if contact_found:
        needs_fallback = False

    search_empty = False
    cse_status = _cse_last_state
    if needs_fallback and not search_exhausted:
        fallback_urls: List[str] = []
        fallback_exhausted = False
        if not row_payload.get("_duckduckgo_search_done"):
            fallback_urls, search_empty, cse_status, fallback_exhausted = _normalize_contact_search_result(
                _contact_search_urls(
                    agent,
                    state,
                    row_payload,
                    domain_hint=domain_hint or brokerage,
                    brokerage=brokerage,
                    limit=SEARCH_CONTACT_URL_LIMIT,
                    include_exhausted=True,
                    engine="duckduckgo",
                )
            )
            row_payload["_duckduckgo_search_done"] = True
        search_exhausted = search_exhausted or fallback_exhausted
        ranked_fallback = _rank_urls(
            fallback_urls,
            agent,
            brokerage,
            domain_hint=domain_hint,
            limit=SEARCH_CONTACT_URL_LIMIT,
        )
        if ranked_fallback:
            search_disabled = True
            _enqueue(ranked_fallback)
            _process_queue()

    if agent_page_seen and not candidates:
        domain_pool = {
            _domain(u)
            for u in seen_urls
            if _domain(u) and _domain(u) not in PORTAL_DOMAINS and _domain(u) not in _CONTACT_DENYLIST
        }
        hint_domain = _domain(domain_hint or brokerage)
        if hint_domain:
            domain_pool.add(hint_domain)
        if not domain_pool:
            domain_pool = {_domain(u) for u in seen_urls if _domain(u)}
        synth_emails = _synth_from_tokens(agent, domain_pool)
        for email in synth_emails:
            candidates.append(
                {
                    "source_url": next(iter(seen_urls), ""),
                    "phones": [],
                    "emails": [email],
                    "evidence_snippet": "synthetic web low confidence",
                }
            )

    result = rerank_contact_candidates(
        candidates, agent, brokerage_domain=brokerage_domain
    )
    if (not result.get("best_email") and not result.get("best_phone")) and candidates:
        result = _apply_phone_type_boost(_best_effort_contact(candidates))
    if agent_page_seen and (not result.get("best_email") and not result.get("best_phone")) and candidates:
        result = _apply_phone_type_boost(_best_effort_contact(candidates))
    if not result.get("best_email") and not result.get("best_phone"):
        blocked_state = {
            _domain(u): _blocked_until.get(_domain(u), 0.0) - time.time()
            for u in urls
            if _blocked(_domain(u))
        }
        LOG.warning(
            "ENRICH CONTACT miss for %s %s – quality=%s search_empty=%s cse_state=%s blocked=%s candidates=%s",
            agent,
            state,
            quality,
            search_empty,
            cse_status,
            {k: round(v, 2) for k, v in blocked_state.items()},
            len(candidates),
        )
    return result


def _contact_enrichment(agent: str, state: str, row_payload: Dict[str, Any]) -> Dict[str, Any]:
    cache_key = "_contact_enrichment"
    if cache_key not in row_payload:
        try:
            row_payload[cache_key] = _two_stage_contact_search(agent, state, row_payload)
        except Exception:
            LOG.exception("two_stage_contact_search failed for %s %s", agent, state)
            row_payload[cache_key] = {
                "_two_stage_done": False,
                "_two_stage_candidates": 0,
                "_blocked_engines": ["error"],
            }
    return row_payload.get(cache_key, {})


def _two_stage_contact_search(agent: str, state: str, row_payload: Dict[str, Any]) -> Dict[str, Any]:
    """Run Google CSE + contact extraction and return ranked contacts."""

    brokerage = (row_payload.get("brokerageName") or row_payload.get("brokerage") or "").strip()
    domain_hint = (
        row_payload.get("domain_hint", "").strip()
        or _infer_domain_from_text(brokerage)
        or _infer_domain_from_text(agent)
    )
    candidates: List[Dict[str, Any]] = []
    email_candidates_info: List[Dict[str, str]] = []
    blocked_engines: Set[str] = set()
    brokerage_domain = _domain(domain_hint or brokerage)

    def _empty_result() -> Dict[str, Any]:
        return {
            "_two_stage_done": False,
            "_two_stage_candidates": len(candidates),
            "_blocked_engines": sorted(blocked_engines),
            "best_email": "",
            "best_email_confidence": 0,
            "best_email_source_url": "",
            "best_email_evidence": "",
            "_email_candidates": [],
            "_email_rejected": [],
        }

    def _has_verifiable_contacts() -> bool:
        quality = _candidate_quality(candidates)
        return bool(quality["phones_found"] or quality["emails_found"])

    def _collect_page(page: str, final_url: str) -> None:
        page_candidates = _agent_contact_candidates_from_html(page, final_url, agent)
        for cand in page_candidates:
            src = cand.get("source_url", final_url)
            for em in cand.get("emails", []):
                email_candidates_info.append({"email": em, "source": src})
        candidates.extend(page_candidates)

    def _direct_fetch(url: str) -> Tuple[str, str, int]:
        dom = _domain(url)
        proxy_url = _proxy_for_domain(dom)
        proxy_cfg = {"http": proxy_url, "https": proxy_url} if proxy_url else None
        tries = len(_CONTACT_FETCH_BACKOFFS)
        last_status = 0
        final_url = url
        for attempt in range(1, tries + 1):
            if attempt > 1:
                delay = _CONTACT_FETCH_BACKOFFS[min(attempt - 1, len(_CONTACT_FETCH_BACKOFFS) - 1)]
                if delay:
                    time.sleep(delay)
            try:
                resp = _session.get(
                    url,
                    timeout=10,
                    headers=_browser_headers(dom),
                    proxies=proxy_cfg,
                )
            except req_exc.Timeout:
                LOG.debug("CONTACT direct timeout url=%s attempt=%s", url, attempt)
                continue
            except _CONNECTION_ERRORS as exc:
                LOG.debug("CONTACT direct error %s on %s", exc, url)
                continue
            except Exception as exc:
                LOG.debug("CONTACT direct error %s on %s", exc, url)
                break
            final_url = getattr(resp, "url", url) or url
            last_status = int(resp.status_code or 0)
            body = (resp.text or "").strip()
            if last_status == 200 and body:
                return body, final_url, last_status
            if last_status in {403, 429, 451}:
                LOG.info("CONTACT direct blocked status=%s url=%s", last_status, final_url)
                return "", final_url, last_status
            break
        return "", final_url, last_status

    try:
        urls, search_empty, cse_status, _ = _normalize_contact_search_result(
            _contact_search_urls(
                agent,
                state,
                row_payload,
                domain_hint=domain_hint or brokerage,
                brokerage=brokerage,
                limit=CONTACT_CSE_FETCH_LIMIT,
                include_exhausted=True,
                engine="google",
            )
        )
        urls = list(dict.fromkeys(urls))[:5]
        if cse_status in {"blocked", "throttled"}:
            blocked_engines.add("google")
        if search_empty or not urls:
            return _empty_result()

        for url in urls:
            page, final_url, status = _direct_fetch(url)
            if status in {403, 429, 451}:
                continue
            if not page:
                continue
            _collect_page(page, final_url)

        if not _has_verifiable_contacts():
            for url in urls:
                fetched = fetch_text_cached(url, ttl_days=14, respect_block=False, allow_blocking=False)
                status = int(fetched.get("http_status", 0) or 0)
                if status in {403, 429, 451}:
                    continue
                page = fetched.get("extracted_text", "") or ""
                final_url = fetched.get("final_url") or url
                if not page.strip():
                    continue
                _collect_page(page, final_url)

        if not _has_verifiable_contacts():
            for url in urls:
                dom = _domain(url)
                if not _should_use_playwright_for_contact(dom, "", js_hint=dom in CONTACT_JS_DOMAINS):
                    continue
                proxy_url = _proxy_for_domain(dom)
                snapshot = _headless_fetch(url, proxy_url=proxy_url, domain=dom, reason="cse-top5")
                rendered = _combine_playwright_snapshot(snapshot)
                if not rendered.strip():
                    continue
                final_url = snapshot.get("final_url") or url
                _collect_page(rendered, final_url)

        ranked = rerank_contact_candidates(candidates, agent, brokerage_domain=brokerage_domain)
        ranked = _apply_phone_type_boost(ranked)

        email = ranked.get("best_email", "")
        rejected_email_reasons: List[Tuple[str, str, str]] = []
        seen_rejects: Set[Tuple[str, str, str]] = set()

        def _track_reject(val: str, reason: str, source: str = "") -> None:
            key = (val, reason, source or "")
            if key in seen_rejects:
                return
            seen_rejects.add(key)
            rejected_email_reasons.append(key)

        def _email_ok(val: str, source: str = "") -> bool:
            if not _email_matches_name(agent, val):
                _track_reject(val, "name_mismatch", source)
                return False
            if _is_generic_email(val) and not _email_matches_name(agent, val):
                _track_reject(val, "junk_domain", source)
                return False
            if _is_junk_email(val):
                _track_reject(val, "junk_domain", source)
                return False
            return True

        if email and not _email_ok(email, ranked.get("best_email_source_url", "")):
            ranked["best_email"] = ""
            ranked["best_email_confidence"] = 0
            ranked["best_email_source_url"] = ""
            ranked["best_email_evidence"] = ""

        ranked["_two_stage_done"] = bool(candidates)
        ranked["_two_stage_candidates"] = len(candidates)
        ranked["_blocked_engines"] = sorted(blocked_engines)
        for cand in email_candidates_info:
            em = cand.get("email", "")
            if not em:
                continue
            _email_ok(em, cand.get("source", ""))
        final_email = ranked.get("best_email", "")
        if final_email:
            _email_ok(final_email, ranked.get("best_email_source_url", ""))
        ranked["_email_candidates"] = email_candidates_info
        ranked["_email_rejected"] = rejected_email_reasons
        return ranked
    except Exception:
        LOG.exception("two_stage_contact_search failed during ranking for %s %s", agent, state)
        return _empty_result()


def fetch_contact_page(url: str) -> Tuple[str, bool, str]:
    if not url:
        return "", False, "empty"
    if _domain(url) == "r.jina.ai":
        unwrapped = _unwrap_jina_url(url)
        if unwrapped:
            LOG.debug("fetch_contact_page unwrap jina mirror -> %s", unwrapped)
            url = unwrapped
    if is_blocked_url(url):
        _log_blocked_url(url)
        return "", False, "blocked"
    if not _should_fetch(url, strict=False):
        return "", False, "skipped"
    cache_key = normalize_url(url)
    with _CONTACT_PAGE_CACHE_LOCK:
        if cache_key in _CONTACT_PAGE_CACHE:
            return _CONTACT_PAGE_CACHE[cache_key]

    def _cache_result(html: str, used_fallback: bool, method: str) -> Tuple[str, bool, str]:
        with _CONTACT_PAGE_CACHE_LOCK:
            _CONTACT_PAGE_CACHE[cache_key] = (html, used_fallback, method)
        return html, used_fallback, method

    dom = _domain(url)
    if dom in _REALTOR_DOMAINS:
        global _realtor_fetch_seen
        if _realtor_fetch_seen:
            _mark_block(dom, reason="realtor-once")
            return "", False, "blocked"
        _realtor_fetch_seen = True
    blocked = False
    proxy_url = _proxy_for_domain(dom)
    tries = len(_CONTACT_FETCH_BACKOFFS)
    if dom in _REALTOR_DOMAINS:
        tries = max(tries, _REALTOR_MAX_RETRIES)

    def _fallback(reason: str) -> Tuple[str, bool, str]:
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
                    return mirror_resp.text, True, "jina_cache"
            except Exception as exc:
                LOG.debug("mirror fetch failed %s on %s", exc, mirror)
        return "", False, "jina_cache"

    def _run_playwright_review(reason: str, body: str = "") -> Tuple[str, bool, str]:
        if not HEADLESS_ENABLED or not async_playwright:
            return "", False, ""
        if is_blocked_url(url):
            LOG.info("PLAYWRIGHT_SKIPPED_BLOCKED url=%s", url)
            return "", False, ""
        allow_playwright = _should_use_playwright_for_contact(dom, body, js_hint=dom in CONTACT_JS_DOMAINS)
        if not allow_playwright:
            return "", False, ""
        snapshot = _headless_fetch(url, proxy_url=proxy_url, domain=dom, reason=reason)
        rendered = _combine_playwright_snapshot(snapshot)
        if rendered.strip():
            LOG.info(
                "PLAYWRIGHT REVIEW used for %s (proxy=%s reason=%s)",
                dom,
                bool(proxy_url),
                reason,
            )
            return rendered, True, "playwright"
        LOG.info("PLAYWRIGHT REVIEW empty for %s (reason=%s)", dom, reason)
        return "", False, ""

    def _finalize(
        html: str,
        used_fallback: bool,
        reason: str,
        body: str = "",
        method: str = "direct",
    ) -> Tuple[str, bool, str]:
        headless_html, headless_used, headless_method = _run_playwright_review(reason, body)
        if headless_html:
            html = headless_html
            used_fallback = used_fallback or headless_used
            method = headless_method or method
        return _cache_result(html, used_fallback, method)

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
            resp = _http_get(
                url,
                timeout=10,
                headers=_browser_headers(dom),
                rotate_user_agent=True,
                proxy=proxy_url,
            )
        except DomainBlockedError:
            blocked = True
            _mark_block(dom, reason="blocked")
            break
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
            return _finalize(body, False, "post-fast-fetch", body, method="direct")
        if status == 403 or (status == 200 and not body):
            blocked = True
            _mark_block(dom)
            LOG.warning("BLOCK %s -> attempt headless for %s/%s", status, attempt, tries)
            html, used_fallback, method = _fallback("403")
            return _finalize(html, used_fallback, "post-mirror-403", html, method=method or "jina_cache")
        if status == 429:
            blocked = True
            _mark_block(dom)
            if dom in _REALTOR_DOMAINS:
                LOG.warning(
                    "Realtor.com throttled (429) attempt %s/%s; short-circuiting further attempts",
                    attempt,
                    tries,
                )
            else:
                LOG.warning(
                    "BLOCK 429 -> short-circuiting further attempts for %s/%s",
                    attempt,
                    tries,
                )
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

    if dom in _REALTOR_DOMAINS:
        _mark_block(dom, reason="realtor-once")

    if blocked:
        html, used_fallback, method = _fallback("blocked")
        return _finalize(html, used_fallback, "post-mirror-blocked", html, method=method or "jina_cache")
    return _finalize("", False, "post-fast-fetch-empty", "")

def _unwrap_jina_url(url: str) -> str:
    parsed = urlparse(url)
    if parsed.netloc != "r.jina.ai":
        return url
    inner = parsed.path.lstrip("/")
    if inner.startswith("http://") or inner.startswith("https://"):
        # Preserve query string if present on the outer URL.
        if parsed.query:
            return f"{inner}?{parsed.query}"
        return inner
    return url


async def _headless_fetch_async(
    url: str, *, proxy_url: str = "", domain: str = "", reason: str = ""
) -> Dict[str, Any]:
    if not HEADLESS_ENABLED or not async_playwright:
        return {}
    target_url = _unwrap_jina_url(url)
    if is_blocked_url(target_url):
        LOG.info("PLAYWRIGHT_SKIPPED_BLOCKED url=%s", target_url)
        return {}
    nav_timeout = HEADLESS_FACEBOOK_TIMEOUT_MS if "facebook.com" in target_url else HEADLESS_NAV_TIMEOUT_MS
    nav_timeout = max(10000, nav_timeout)
    default_timeout = max(10000, HEADLESS_TIMEOUT_MS)
    overall_timeout = HEADLESS_OVERALL_TIMEOUT_S

    async def _progressive_scroll(page) -> None:
        for _ in range(3):
            try:
                await page.mouse.wheel(0, 1800)
            except Exception:
                break
            await page.wait_for_timeout(400)

    async def _expand_facebook(page) -> None:
        try:
            await _progressive_scroll(page)
            labels = ("About", "Contact info", "Contact", "About info", "See more")
            for label in labels:
                try:
                    locator = page.get_by_role("button", name=re.compile(label, re.I))  # type: ignore[attr-defined]
                    await locator.first.click(timeout=1500)
                    await page.wait_for_timeout(450)
                    continue
                except Exception:
                    pass
                try:
                    locator = page.locator(f"text={label}")
                    await locator.first.click(timeout=1500)
                    await page.wait_for_timeout(450)
                except Exception:
                    continue
        except Exception:
            return
    async def _run() -> Dict[str, Any]:
        browser = None
        context = None
        page = None
        try:
            async with async_playwright() as p:
                browser, runtime_mode = await _connect_playwright_browser(p)
                accept_language = random.choice(_ACCEPT_LANGUAGE_POOL)
                context = await browser.new_context(
                    user_agent=random.choice(_USER_AGENT_POOL),
                    locale=accept_language.split(",")[0],
                    extra_http_headers={
                        "Accept-Language": accept_language,
                        **_random_cookie_header(),
                    },
                )
                page = await context.new_page()
                page.set_default_timeout(default_timeout)
                page.set_default_navigation_timeout(default_timeout)
                LOG.info(
                    "PLAYWRIGHT_START url=%s reason=%s remote=%s",
                    target_url,
                    reason or "fallback",
                    runtime_mode == "remote",
                )
                for attempt in range(2):
                    try:
                        await page.goto(target_url, wait_until="domcontentloaded", timeout=nav_timeout)
                        break
                    except Exception as exc:
                        if PlaywrightTimeoutError and isinstance(exc, PlaywrightTimeoutError):
                            LOG.warning(
                                "PLAYWRIGHT_TIMEOUT nav url=%s attempt=%s err=%s",
                                target_url,
                                attempt + 1,
                                exc,
                            )
                        else:
                            LOG.warning(
                                "PLAYWRIGHT_ERROR nav url=%s attempt=%s err=%s",
                                target_url,
                                attempt + 1,
                                exc,
                            )
                        if attempt == 0:
                            try:
                                await page.wait_for_timeout(800)
                            except Exception:
                                pass
                            continue
                        return {}
                try:
                    await page.wait_for_load_state("networkidle", timeout=3000)
                except Exception:
                    pass
                await page.wait_for_timeout(HEADLESS_WAIT_MS)
                await _progressive_scroll(page)
                if "facebook.com" in (domain or _domain(target_url) or ""):
                    await _expand_facebook(page)
                    await page.wait_for_timeout(600)

                content = await page.content() or ""
                visible_text = ""
                try:
                    visible_text = await page.inner_text("body", timeout=2000) or ""
                except Exception:
                    visible_text = ""
                mail_links: List[str] = []
                tel_links: List[str] = []
                try:
                    hrefs = await page.eval_on_selector_all(
                        "a[href]",
                        "els => els.map(el => el.getAttribute('href') || '')",
                    ) or []
                    mail_links = [h for h in hrefs if h and h.lower().startswith("mailto:")]
                    tel_links = [h for h in hrefs if h and h.lower().startswith("tel:")]
                except Exception:
                    pass
                if not content.strip():
                    LOG.info("PLAYWRIGHT_ERROR url=%s err=%s", target_url, "empty-content")
                    return {}
                LOG.info(
                    "PLAYWRIGHT_OK url=%s bytes=%s",
                    target_url,
                    len(content.encode("utf-8")),
                )
                LOG.debug(
                    "Headless fetch ok for %s (remote=%s)",
                    domain or _domain(target_url),
                    runtime_mode == "remote",
                )
                return {
                    "html": content,
                    "visible_text": visible_text,
                    "mailto_links": mail_links,
                    "tel_links": tel_links,
                    "final_url": page.url,
                }
        except Exception as exc:  # pragma: no cover - network/env specific
            LOG.warning("PLAYWRIGHT_ERROR url=%s err=%s", target_url, exc)
            return {}
        finally:
            try:
                if page:
                    await page.close()
            except Exception:
                pass
            try:
                if context:
                    await context.close()
            except Exception:
                pass
            try:
                if browser:
                    await browser.close()
            except Exception:
                pass

    try:
        return await asyncio.wait_for(_run(), timeout=overall_timeout)
    except asyncio.TimeoutError:
        LOG.warning("PLAYWRIGHT_TIMEOUT url=%s err=%s", target_url, "overall_timeout")
        return {}
    except Exception as exc:
        LOG.warning("PLAYWRIGHT_ERROR url=%s err=%s", target_url, exc)
        return {}


def _headless_fetch(url: str, *, proxy_url: str = "", domain: str = "", reason: str = "") -> Dict[str, Any]:
    if not HEADLESS_ENABLED or not async_playwright:
        return {}
    log_headless_status()
    if not ensure_playwright_ready(LOG):
        return {}

    loop = _ensure_headless_loop()
    coro = _headless_fetch_async(url, proxy_url=proxy_url, domain=domain, reason=reason)
    future = asyncio.run_coroutine_threadsafe(coro, loop)
    try:
        return future.result(timeout=HEADLESS_OVERALL_TIMEOUT_S + 5)
    except concurrent.futures.TimeoutError:
        future.cancel()
        LOG.warning("PLAYWRIGHT_TIMEOUT url=%s err=%s", url, "thread-timeout")
        return {}
    except Exception:
        LOG.exception("PLAYWRIGHT_FAIL url=%s err=%s", url, "thread-runner")
        return {}

# ───────────────────── Google CSE helpers ─────────────────────

_cse_cache: Dict[str, List[Dict[str, Any]]] = {}
_last_cse_ts = 0.0
_cse_lock = threading.Lock()
_cse_recent: deque[float] = deque()
_cse_last_state = "idle"
_cse_last_ts_per_key: Dict[Tuple[str, str], float] = {}
CONTACT_CSE_FETCH_LIMIT = int(os.getenv("CONTACT_CSE_FETCH_LIMIT", "30"))

TOP5_DENYLIST_DOMAINS = {
    "zillow.com",
    "realtor.com",
    "trulia.com",
    "redfin.com",
    "homes.com",
    "home.com",
    "loopnet.com",
    "crexi.com",
}
TOP5_LONGFORM_DOMAINS = {
    "archive.org",
    "books.google.com",
    "openlibrary.org",
    "gutenberg.org",
    "projectgutenberg.org",
    "scribd.com",
}
TOP5_JUNK_DOMAINS = {
    "wikipedia.org",
    "youtube.com",
    "twitter.com",
    "tiktok.com",
    "pinterest.com",
    "yelp.com",
}
TOP5_REALTOR_ALLOW_PATHS = (
    "/realestateagents",
    "/realestateagent",
    "/realestateandhomes-search",
    "/realestateagents/agency",
)
TOP5_SOCIAL_ALLOW = {"facebook.com", "linkedin.com", "instagram.com"}
TOP5_BROKERAGE_ALLOW = {
    "kw.com",
    "remax.com",
    "coldwellbanker.com",
    "bhhs.com",
    "century21.com",
    "century21judgefite.com",
    "compass.com",
    "exprealty.com",
    "sothebysrealty.com",
    "corcoran.com",
    "betterhomesandgardens.com",
    "weichert.com",
    "longandfoster.com",
    "era.com",
    "realtyonegroup.com",
    "homesmart.com",
    "allentate.com",
    "howardhanna.com",
    "windermere.com",
    "cbharper.com",
    "har.com",
}
TOP5_DOMAIN_HINT_TOKENS = {
    "realty",
    "realtor",
    "homes",
    "properties",
    "team",
    "group",
    "brokerage",
}
TOP5_PATH_HINT_TOKENS = {
    "agent",
    "agents",
    "realtor",
    "team",
    "our-team",
    "profile",
    "about",
    "contact",
}
TOP5_LISTING_HINTS = {"listing", "listings", "for-sale", "homedetails", "property", "newlisting", "mls", "idx"}
TOP5_GOOD_PATH_HINTS = (
    "/agent/",
    "/agents/",
    "/team/",
    "/teams/",
    "/realtor",
    "/realtors",
    "/about",
    "/bio",
    "/profile",
    "/contact",
    "/contact-us",
    "/our-team",
)
TOP5_GOOD_TEXT_HINTS = (
    "our agents",
    "meet the team",
    "our team",
    "find an agent",
    "find a realtor",
    "broker roster",
)
TOP5_BAD_SOCIAL_PATTERNS = ("/groups/", "/posts/", "/p/")
TOP5_HARD_PATH_BLOCKS = (
    "/search",
    "/results",
    "/listings",
    "/homes-for-sale",
    "/property/",
    "/mls/",
    "/idx/",
    "/books/",
    "/book/",
    "/ebook/",
    "/ebooks/",
    "/pdf/",
    "/download/",
    "/wp-content/uploads",
)
TOP5_OFF_TOPIC_HINTS = (
    "forum",
    "chamber",
    "school",
    "pta",
    "library",
    "blogspot",
)
TOP5_ASSOCIATION_HINTS = {"realtor", "mls", "association", "board-of-realtors"}
PREFERRED_BROKERAGE_EMAIL_DOMAINS: Dict[str, Set[str]] = {
    "keller williams": {"kw.com", "kellerwilliams.com"},
    "re/max": {"remax.com", "remax.net"},
    "remax": {"remax.com", "remax.net"},
    "compass": {"compass.com"},
    "coldwell banker": {"cbrealty.com", "coldwellbankerhomes.com", "coldwellbanker.com"},
    "century 21": {"century21.com"},
    "bhhs": {"bhhs.com", "berkshirehathawayhs.com"},
    "berkshire hathaway": {"bhhs.com", "berkshirehathawayhs.com"},
    "kw": {"kw.com"},
}
PREFERRED_SOCIAL_PATHS = ("/in/", "/profile.php", "/people/", "/business/")

DISPOSABLE_EMAIL_DOMAINS = {
    "mailinator.com",
    "tempmail.com",
    "tempmailo.com",
    "guerrillamail.com",
    "yopmail.com",
    "10minutemail.com",
    "sharklasers.com",
}
SPAMMY_TLDS = {"xyz", "icu", "top", "click"}


class TopUrlSelectionError(Exception):
    def __init__(
        self,
        message: str,
        partial: Optional[List[str]] = None,
        rejected: Optional[List[Tuple[str, str]]] = None,
    ):
        super().__init__(message)
        self.partial = partial or []
        self.rejected = rejected or []


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

def _cse_ready() -> bool:
    if not any(k and cx for k, cx in _CSE_CRED_POOL):
        return False
    if all((k or "").lower() in {"test", "dummy"} for k, _ in _CSE_CRED_POOL):
        return False
    if all((cx or "").lower() in {"test", "dummy"} for _, cx in _CSE_CRED_POOL):
        return False
    return not _cse_blocked()


def _next_cse_creds() -> Tuple[str, str]:
    global _CSE_CRED_INDEX
    if not _CSE_CRED_POOL:
        return "", ""
    key, cx = _CSE_CRED_POOL[_CSE_CRED_INDEX % len(_CSE_CRED_POOL)]
    _CSE_CRED_INDEX = (_CSE_CRED_INDEX + 1) % len(_CSE_CRED_POOL)
    return key, cx


def _mark_cse_block() -> None:
    global _cse_blocked_until
    cooldown = max(CSE_BLOCK_SECONDS, 3600.0)
    _cse_blocked_until = time.time() + cooldown
    LOG.warning("CSE blocked; cooling off for %.0f seconds", cooldown)


def _filter_allowed(link: str, allowed_domains: Optional[Set[str]]) -> bool:
    if not allowed_domains:
        return True
    dom = _domain(link)
    return any(dom == d or dom.endswith(f".{d}") for d in allowed_domains)

_LISTING_PATH_RE = re.compile(r"/(?:idx|mls|listing|listings|newlisting|homedetails|property)[^/]*", re.I)
_LISTING_BOILERPLATE_TERMS = (
    "copyright",
    "dmca",
    "mls",
    "multiple listing service",
    "listing provided by",
    "idx",
    "information deemed reliable",
)


def _listing_path_blocked(path: str) -> bool:
    if not path:
        return False
    return bool(_LISTING_PATH_RE.search(path) or any(hint in path for hint in TOP5_LISTING_HINTS))


def _looks_listing_boilerplate(text: str, path: str = "") -> bool:
    if _listing_path_blocked(path):
        return True
    if not text:
        return False
    low = text.lower()
    hits = sum(low.count(term) for term in _LISTING_BOILERPLATE_TERMS)
    if hits >= 3:
        return True
    words = len(low.split())
    return bool(words and hits >= 2 and (hits / max(words, 1)) > 0.01)


def _is_social_root(root: str) -> bool:
    return root in TOP5_SOCIAL_ALLOW or any(root.endswith(f".{dom}") for dom in TOP5_SOCIAL_ALLOW)

def _low_signal_social_path(root: str, path: str) -> bool:
    if not _is_social_root(root):
        return False
    low_terms = (
        "/reel",
        "/reels",
        "/stories",
        "/story",
        "/watch",
        "/explore",
        "/search",
    )
    if root.endswith("facebook.com") and any(term in path for term in ("/groups/", "/posts/")):
        return True
    if root.endswith("instagram.com") and "/p/" in path:
        return True
    return any(term in path for term in low_terms)


def _top_url_allowed(link: str, *, relaxed: bool = False, allow_portals: bool = False) -> Tuple[bool, str]:
    if not link:
        return False, "no_link"
    parsed = urlparse(link)
    dom = parsed.netloc.lower()
    path = parsed.path.lower()
    if not dom:
        return False, "no_domain"
    root = _domain(link) or dom
    if root in TOP5_LONGFORM_DOMAINS or any(root.endswith(f".{d}") for d in TOP5_LONGFORM_DOMAINS):
        return False, "longform_domain"
    if root in TOP5_JUNK_DOMAINS or any(root.endswith(f".{d}") for d in TOP5_JUNK_DOMAINS):
        return False, "junk_domain"
    realtor_allowed = root.endswith("realtor.com") and any(path.startswith(p) for p in TOP5_REALTOR_ALLOW_PATHS)
    if root.endswith("realtor.com") and not realtor_allowed:
        return False, "realtor_non_profile"
    allow = False
    allow_reason = ""
    if root in TOP5_DENYLIST_DOMAINS or any(root.endswith(f".{d}") for d in TOP5_DENYLIST_DOMAINS):
        if realtor_allowed:
            allow = True
            allow_reason = "realtor_profile"
        elif allow_portals and root in PORTAL_DOMAINS:
            allow = True
            allow_reason = "portal_allow"
        else:
            return False, "denylist_domain"
    if root.endswith(".gov") or root.endswith(".edu") or dom.endswith(".gov") or dom.endswith(".edu"):
        return False, "gov_edu"
    if path.endswith(".pdf") or ".pdf" in path:
        return False, "pdf"
    if any(block in path for block in TOP5_HARD_PATH_BLOCKS):
        return False, "hard_block_path"
    if _listing_path_blocked(path):
        return False, "listing_detail"
    if _low_signal_social_path(root, path) or (_is_social_root(root) and any(term in path for term in TOP5_BAD_SOCIAL_PATTERNS)):
        return False, "social_low_signal"
    if path in {"/search", "/results"} or "/search?" in link:
        return False, "search_page"
    if any(tok in root for tok in TOP5_OFF_TOPIC_HINTS) and not _is_real_estate_domain(root):
        return False, "off_topic_domain"
    if any(tok in path for tok in TOP5_OFF_TOPIC_HINTS) and not _is_real_estate_domain(root):
        return False, "off_topic_path"

    if _is_social_root(root):
        allow = True
        allow_reason = "social_allow"
    elif root in TOP5_BROKERAGE_ALLOW or any(root.endswith(f".{dom}") for dom in TOP5_BROKERAGE_ALLOW):
        allow = True
        allow_reason = "brokerage_allow"
    elif any(tok in root for tok in TOP5_ASSOCIATION_HINTS):
        allow = True
        allow_reason = "association_allow"
    elif any(tok in root for tok in TOP5_DOMAIN_HINT_TOKENS):
        allow = True
        allow_reason = "domain_hint"
    elif any(tok in path for tok in TOP5_PATH_HINT_TOKENS):
        allow = True
        allow_reason = "path_hint"

    if not allow:
        if relaxed:
            return True, "relaxed_allow"
        return False, "non_agent_domain"
    return True, allow_reason or "allowed"


US_STATE_ABBR = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY", "DC",
}
_STATE_NAME_MAP = {
    "alabama": "AL",
    "alaska": "AK",
    "arizona": "AZ",
    "arkansas": "AR",
    "california": "CA",
    "colorado": "CO",
    "connecticut": "CT",
    "delaware": "DE",
    "florida": "FL",
    "georgia": "GA",
    "hawaii": "HI",
    "idaho": "ID",
    "illinois": "IL",
    "indiana": "IN",
    "iowa": "IA",
    "kansas": "KS",
    "kentucky": "KY",
    "louisiana": "LA",
    "maine": "ME",
    "maryland": "MD",
    "massachusetts": "MA",
    "michigan": "MI",
    "minnesota": "MN",
    "mississippi": "MS",
    "missouri": "MO",
    "montana": "MT",
    "nebraska": "NE",
    "nevada": "NV",
    "new hampshire": "NH",
    "new jersey": "NJ",
    "new mexico": "NM",
    "new york": "NY",
    "north carolina": "NC",
    "north dakota": "ND",
    "ohio": "OH",
    "oklahoma": "OK",
    "oregon": "OR",
    "pennsylvania": "PA",
    "rhode island": "RI",
    "south carolina": "SC",
    "south dakota": "SD",
    "tennessee": "TN",
    "texas": "TX",
    "utah": "UT",
    "vermont": "VT",
    "virginia": "VA",
    "washington": "WA",
    "west virginia": "WV",
    "wisconsin": "WI",
    "wyoming": "WY",
    "district of columbia": "DC",
}
_STATE_PATTERN = re.compile(r"\b(" + "|".join(sorted(US_STATE_ABBR)) + r")\b")
_STATE_ZIP_PATTERN = re.compile(r"\b(" + "|".join(sorted(US_STATE_ABBR)) + r")[\s,]+\d{5}(?:-\d{4})?\b")
_STATE_NAME_PATTERN = re.compile(r"\b(" + "|".join(re.escape(k) for k in _STATE_NAME_MAP.keys()) + r")\b", re.IGNORECASE)


def _state_hints_from_url(link: str) -> Set[str]:
    hints: Set[str] = set()
    if not link:
        return hints
    parsed = urlparse(link)
    for segment in parsed.path.split("/"):
        seg_clean = segment.strip()
        if len(seg_clean) == 2 and seg_clean.isupper() and seg_clean in US_STATE_ABBR:
            hints.add(seg_clean)
        for part in seg_clean.split("-"):
            part_clean = part.strip().upper()
            if len(part_clean) == 2 and part_clean in US_STATE_ABBR:
                hints.add(part_clean)
    return hints


def _state_hints_from_text(text: str) -> Set[str]:
    hints: Set[str] = set()
    if not text:
        return hints
    for regex in (_STATE_ZIP_PATTERN, _STATE_PATTERN):
        for match in regex.finditer(text):
            token = match.group(1).upper()
            if token in US_STATE_ABBR:
                hints.add(token)
    for match in _STATE_NAME_PATTERN.finditer(text):
        abbr = _STATE_NAME_MAP.get(match.group(1).lower())
        if abbr:
            hints.add(abbr)
    return hints


def _state_mismatch(link: str, text: str, property_state: str) -> bool:
    state = property_state.strip().upper()
    if not state:
        return False
    hints = _state_hints_from_url(link)
    hints.update(_state_hints_from_text(text))
    if not hints:
        return False
    return state not in hints


def _location_matches(
    link: str,
    text: str,
    snippet: str,
    property_state: str,
    city: str = "",
    brokerage: str = "",
) -> bool:
    state = property_state.strip().upper()
    if not state:
        return True
    combined = " ".join(part for part in (text or "", snippet or "") if part)
    hints = _state_hints_from_url(link)
    hints.update(_state_hints_from_text(combined.upper()))
    if state in hints:
        return True
    city_clean = city.strip().lower()
    if city_clean:
        low = combined.lower()
        if city_clean in low and state.lower() in low:
            return True
    brokerage_clean = brokerage.strip().lower()
    if brokerage_clean:
        low = combined.lower()
        if brokerage_clean in low and (state in hints or state.lower() in low):
            return True
    return False


def select_top_5_urls(
    results: Iterable[Dict[str, Any] | str],
    *,
    fetch_check: bool = True,
    relaxed: bool = False,
    allow_portals: bool = False,
    property_state: str = "",
    property_city: str = "",
    brokerage: str = "",
    agent: str = "",
    limit: int = 10,
    existing: Optional[Iterable[str]] = None,
    location_hint: str = "",
) -> Tuple[List[str], List[Tuple[str, str]]]:
    items = list(results)
    limit = max(1, min(limit, CONTACT_CSE_FETCH_LIMIT))
    property_state = property_state.strip().upper()
    property_city = property_city.strip()
    target = min(limit, 5)
    existing_norms: Set[str] = {
        normalize_url(u) for u in (existing or []) if u
    }
    agent_tokens = [re.sub(r"[^a-z0-9]", "", part.lower()) for part in agent.split() if part.strip()]
    brokerage_token = re.sub(r"[^a-z0-9]", "", brokerage.lower()) if brokerage else ""
    city_token = property_city.lower()
    original_order: Dict[str, int] = {}

    def _good_candidate_score(url: str, *, snippet: str = "", text: str = "") -> int:
        parsed = urlparse(url)
        root = _domain(url) or parsed.netloc.lower()
        path = parsed.path.lower()
        context = " ".join(part for part in (snippet, text) if part).lower()
        score = 0
        contactish_terms = {"contact", "about", "team", "profile", "agent", "realtor", "bio"}
        if any(hint in path for hint in TOP5_GOOD_PATH_HINTS):
            score += 7
        if any(term in context for term in TOP5_GOOD_TEXT_HINTS):
            score += 5
        if any(term in path for term in contactish_terms):
            score += 3
        if any(term in context for term in contactish_terms):
            score += 2
        if brokerage_token and (brokerage_token in root or brokerage_token in path or brokerage_token in context):
            score += 3
        if city_token and city_token in context:
            score += 2
        if property_state and property_state in context.upper():
            score += 2
        if agent_tokens and any(tok and (tok in path or tok in context) for tok in agent_tokens):
            score += 3
        if agent_tokens and any(tok and (tok in root) for tok in agent_tokens):
            score += 4
        if _looks_directory_page(url):
            score += 2
        if root in TOP5_BROKERAGE_ALLOW or any(root.endswith(f".{dom}") for dom in TOP5_BROKERAGE_ALLOW):
            score += 5
        if _is_social_root(root):
            score -= 2
            if any(path.startswith(p) or p in path for p in PREFERRED_SOCIAL_PATHS):
                score += 2
        if allow_portals and (root in PORTAL_DOMAINS):
            score += 1
        if any(term in root for term in TOP5_ASSOCIATION_HINTS):
            score += 2
        if "contact" in path:
            score += 1
        if "email" in context or "phone" in context:
            score += 1
        score += max(0, 2 - path.count("/"))
        return score

    def _select(relax: bool) -> Tuple[List[str], List[Tuple[str, str]]]:
        rejected: List[Tuple[str, str]] = []
        seen_links: Set[str] = set(existing_norms)
        candidates: List[Tuple[str, bool, int]] = []
        overflow: List[Tuple[str, bool, int]] = []
        for idx, item in enumerate(items):
            link = ""
            snippet = ""
            title = ""
            if isinstance(item, str):
                link = item
            elif isinstance(item, dict):
                link = str(item.get("link") or item.get("url") or "")
                snippet = str(item.get("snippet") or item.get("htmlSnippet") or "")
                title = str(item.get("title") or "")
            if not link:
                continue
            if is_blocked_url(link):
                _log_blocked_url(link)
                rejected.append((link, "blocked_domain"))
                continue
            norm = normalize_url(link)
            if norm in seen_links:
                continue
            original_order.setdefault(norm, idx)
            allowed, reason = _top_url_allowed(norm, relaxed=relax, allow_portals=allow_portals)
            if not allowed:
                rejected.append((norm, reason))
                continue
            seen_links.add(norm)
            final_url = norm
            text = ""
            snippet_text = " ".join(part for part in (title, snippet) if part)
            location_context = " ".join(part for part in (snippet_text, location_hint) if part)
            if fetch_check:
                fetched = fetch_text_cached(norm, ttl_days=7, respect_block=False, allow_blocking=False)
                final_url = fetched.get("final_url") or norm
                if is_blocked_url(final_url):
                    _log_blocked_url(final_url)
                    rejected.append((final_url, "blocked_domain"))
                    continue
                status = int(fetched.get("http_status", 0) or 0)
                text = fetched.get("extracted_text", "")
                if fetched.get("retry_needed") or status == 0 or not text.strip():
                    rejected.append((final_url, "fetch_failed"))
                    continue
                final_path = urlparse(final_url).path.lower()
                if _looks_listing_boilerplate(text, final_path):
                    rejected.append((final_url, "listing_boilerplate"))
                    continue
                if property_state and not _location_matches(final_url, text, location_context, property_state, property_city, brokerage):
                    rejected.append((final_url, "state_mismatch"))
                    continue
                if agent and not _agent_matches_context(agent, text=text, snippet=location_context, title=title):
                    rejected.append((final_url, "agent_mismatch"))
                    continue
            else:
                if property_state and not _location_matches(final_url, "", location_context, property_state, property_city, brokerage):
                    rejected.append((final_url, "state_mismatch"))
                    continue
                if agent and not _agent_matches_context(agent, snippet=location_context, title=title):
                    rejected.append((final_url, "agent_mismatch"))
                    continue
            original_order.setdefault(normalize_url(final_url), original_order.get(norm, idx))
            root = _domain(final_url) or _domain(norm) or ""
            score = _good_candidate_score(final_url, snippet=snippet_text, text=text)
            entry = (final_url, _is_social_root(root), score)
            if len(candidates) < target:
                candidates.append(entry)
            else:
                overflow.append(entry)

        def _merge_with_preference() -> List[Tuple[str, bool]]:
            preferred: List[Tuple[str, bool]] = []
            all_candidates = candidates + overflow
            all_candidates.sort(
                key=lambda c: (
                    -c[2],
                    c[1],
                    original_order.get(normalize_url(c[0]), original_order.get(c[0], 0)),
                )
            )
            non_social = [c for c in all_candidates if not c[1]]
            social = [c for c in all_candidates if c[1]]
            preferred.extend(non_social[:target])
            if len(preferred) < target:
                preferred.extend(social[: target - len(preferred)])
            return [(url, is_social) for url, is_social, _ in preferred[:target]]

        filtered_candidates = _merge_with_preference()
        filtered = [url for url, _ in filtered_candidates]
        return filtered, rejected

    filtered, rejected = _select(relaxed)
    if (len(filtered) < target) and not relaxed:
        filtered_relaxed, rejected_relaxed = _select(True)
        if filtered_relaxed:
            filtered = filtered_relaxed
            rejected.extend(rejected_relaxed)
    return filtered[:target], rejected


def google_cse_search(
    query: str,
    limit: int = 10,
    *,
    allowed_domains: Optional[Set[str]] = None,
) -> List[Dict[str, Any]]:
    global _cse_last_state
    _cse_last_state = "idle"
    if not query or not _cse_ready():
        _cse_last_state = "disabled"
        return []
    if _cse_blocked():
        _cse_last_state = "blocked"
        LOG.warning("Skipping Google CSE for %s due to active block", query)
        return []

    limit = max(1, min(limit, CONTACT_CSE_FETCH_LIMIT))
    results: List[Dict[str, Any]] = []
    seen_results: Set[str] = set()
    attempts = 0
    seen_throttled = False
    seen_http_error = False
    max_attempts = max(1, min(len(_CSE_CRED_POOL), CSE_MAX_ATTEMPTS))
    fallback_results: List[Dict[str, Any]] = []

    def _fetch_cse_page(
        start: int,
        num: int,
        key: str,
        cx: str,
    ) -> Dict[str, Any]:
        params = {
            "q": query,
            "key": key,
            "cx": cx,
            "num": max(1, min(num, 10)),
        }
        if start > 1:
            params["start"] = start
        try:
            resp = _http_get(
                "https://www.googleapis.com/customsearch/v1",
                params=params,
                timeout=12,
                rotate_user_agent=True,
            )
        except requests.HTTPError as exc:
            status = getattr(exc.response, "status_code", 0)
            LOG.warning(
                "CSE HTTP error status=%s query=%r start=%d num=%d",
                status or "unknown",
                query,
                start,
                params["num"],
            )
            if params["num"] != 10:
                params["num"] = 10
                resp = _http_get(
                    "https://www.googleapis.com/customsearch/v1",
                    params=params,
                    timeout=12,
                    rotate_user_agent=True,
                )
            else:
                raise
        return resp.json() if resp is not None else {}

    for _ in range(max_attempts):
        key, cx = _next_cse_creds()
        if not key or not cx:
            continue
        attempts += 1
        # Light per-key spacing to avoid rapid-fire throttling.
        last_ts = _cse_last_ts_per_key.get((key, cx), 0.0)
        if last_ts:
            gap = time.time() - last_ts
            if gap < CSE_PER_KEY_MIN_INTERVAL:
                time.sleep((CSE_PER_KEY_MIN_INTERVAL - gap) + random.uniform(0.05, 0.2))
        try:
            pages: List[Tuple[int, int]] = []
            remaining = limit
            start_idx = 1
            while remaining > 0:
                batch = min(remaining, 10)
                pages.append((start_idx, batch))
                remaining -= batch
                start_idx += 10
            raw_links: List[Dict[str, Any] | str] = []
            seen_raw_links: Set[str] = set()
            for start, num_param in pages:
                payload = _fetch_cse_page(start, num_param, key, cx)
                _cse_last_ts_per_key[(key, cx)] = time.time()
                items = payload.get("items", []) or []
                for item in items:
                    link = item.get("link")
                    if not link:
                        continue
                    if is_blocked_url(link):
                        _log_blocked_url(link)
                        continue
                    norm = normalize_url(link)
                    if norm in seen_raw_links:
                        continue
                    seen_raw_links.add(norm)
                    raw_links.append({
                        "link": link,
                        "title": item.get("title"),
                        "snippet": item.get("snippet") or item.get("htmlSnippet"),
                    })
            for link in raw_links:
                link_url = link if isinstance(link, str) else link.get("link", "")
                if not link_url or not _filter_allowed(link_url, allowed_domains):
                    continue
                norm = normalize_url(link_url)
                if norm in seen_results:
                    continue
                seen_results.add(norm)
                results.append(
                    {
                        "link": link_url,
                        "title": link.get("title") if isinstance(link, dict) else "",
                        "snippet": link.get("snippet") if isinstance(link, dict) else "",
                    }
                )
                if len(results) >= limit:
                    break
            if len(raw_links) < limit:
                LOG.info("CSE fallback triggered cse_items=%d limit=%d", len(raw_links), limit)
                fallback_results, blocked = duckduckgo_search(
                    query,
                    limit=limit,
                    allowed_domains=allowed_domains,
                    with_blocked=True,
                )
                if blocked:
                    _mark_block("duckduckgo.com", reason="blocked")
            if results or fallback_results:
                _cse_last_state = "ok"
                break
            # Empty result set is treated as a normal miss, not a throttle.
            if payload.get("items") is not None:
                LOG.info(
                    "CSE empty for query=%r allowed=%s",
                    query,
                    sorted(allowed_domains) if allowed_domains else "any",
                )
                _cse_last_state = "no_results"
                break
        except req_exc.RetryError:
            _mark_cse_block()
            _cse_last_state = "throttled"
            break
        except DomainBlockedError:
            _mark_cse_block()
            _cse_last_state = "blocked"
            break
        except requests.HTTPError as exc:
            status = getattr(exc.response, "status_code", 0)
            seen_http_error = True
            if status in (403, 429):
                seen_throttled = True
                retry_after = exc.response.headers.get("Retry-After") if exc.response else None
                if retry_after and retry_after.isdigit():
                    time.sleep(min(float(retry_after), CSE_BLOCK_SECONDS))
                _mark_cse_block()
                break
            if attempts >= max_attempts:
                _record_timeout("google_cse")
        except Exception:
            if attempts >= max_attempts:
                _record_timeout("google_cse")
        _search_sleep()
    if fallback_results and len(results) < limit:
        for item in fallback_results:
            link = item.get("link", "")
            if not link or not _filter_allowed(link, allowed_domains):
                continue
            if is_blocked_url(link):
                _log_blocked_url(link)
                continue
            norm = normalize_url(link)
            if norm in seen_results:
                continue
            seen_results.add(norm)
            results.append({"link": link})
            if len(results) >= limit:
                break
    if not results and _cse_last_state == "idle":
        if seen_http_error:
            _cse_last_state = "error"
        else:
            _cse_last_state = "throttled" if seen_throttled else "no_results"
    return results[:limit]


def google_items(q: str, tries: int = 3) -> List[Dict[str, Any]]:
    if _cse_ready():
        hits = google_cse_search(q, limit=10)
        if hits:
            return hits
    links = jina_cached_search(q, max_results=10)
    return [{"link": link} for link in links if link]


def _safe_google_items(q: str, *, tries: int = 3) -> List[Dict[str, Any]]:
    """Wrapper around google_items that tolerates monkeypatched signatures."""
    try:
        return google_items(q, tries=tries)
    except TypeError:
        return google_items(q)


# ───────────────────── alternate search helpers ─────────────────────

def duckduckgo_search(
    query: str,
    limit: int = 10,
    *,
    allowed_domains: Optional[Set[str]] = None,
    with_blocked: bool = False,
) -> List[Dict[str, Any]] | Tuple[List[Dict[str, Any]], bool]:
    blocked = False
    links: List[str] = []
    try:
        links = jina_cached_search(query, max_results=limit, allowed_domains=allowed_domains)
    except DomainBlockedError:
        blocked = True
    except Exception:
        blocked = True
        LOG.exception("duckduckgo_search failed for %s", query)
    hits = [{"link": link} for link in links if link]
    return (hits, blocked) if with_blocked else hits


def bing_search(query: str, limit: int = 10) -> List[Dict[str, Any]]:
    links = jina_cached_search(query, max_results=limit)
    return [{"link": link} for link in links if link]


def search_round_robin(
    queries: Iterable[str],
    per_query_limit: int = 4,
    *,
    allowed_domains: Optional[Set[str]] = None,
    engine_limit: Optional[int] = None,
) -> List[List[Tuple[str, List[Dict[str, Any]]]]]:
    engines: List[Tuple[str, Any]] = []

    if _cse_ready() and not _search_disabled("google_cse"):
        engines.append(
            (
                "google_cse",
                lambda q, limit: google_cse_search(
                    q,
                    limit=limit,
                    allowed_domains=allowed_domains,
                ),
            )
        )

    engines.append(
        (
            "jina",
            lambda q, limit: duckduckgo_search(
                q,
                limit=limit,
                allowed_domains=allowed_domains,
            ),
        ),
    )

    if engine_limit is not None:
        engines = engines[: max(1, min(len(engines), engine_limit))]

    deduped = _dedupe_queries(queries)
    results: List[List[Tuple[str, List[Dict[str, Any]]]]] = []
    if not deduped:
        return results

    for idx, q in enumerate(deduped):
        start = idx % len(engines)
        ordered = engines[start:] + engines[:start]
        attempts: List[Tuple[str, List[Dict[str, Any]]]] = []
        for name, fn in ordered:
            hits = fn(q, engine_limit or per_query_limit)
            attempts.append((name, hits))
            if hits:
                break
        results.append(attempts)
    return results

# ───────────────────── page‑parsing helpers ─────────────────────
def _record_sameas_links(row_payload: Dict[str, Any], links: Iterable[str]) -> None:
    if not links:
        return
    existing: List[str] = []
    if "_sameas_links" in row_payload and isinstance(row_payload["_sameas_links"], list):
        existing = row_payload["_sameas_links"]
    seen = set(existing)
    for link in links:
        if not link:
            continue
        cleaned = str(link).strip()
        if is_blocked_url(cleaned):
            _log_blocked_url(cleaned)
            continue
        if not cleaned or cleaned in seen:
            continue
        existing.append(cleaned)
        seen.add(cleaned)
    if existing:
        row_payload["_sameas_links"] = existing


def _iter_jsonld_nodes(data: Any) -> Iterable[Dict[str, Any]]:
    """Yield all dict nodes from a JSON-LD payload, including @graph entries."""
    stack: List[Any] = [data]
    while stack:
        node = stack.pop()
        if isinstance(node, list):
            stack.extend(node)
            continue
        if not isinstance(node, dict):
            continue
        yield node
        graph = node.get("@graph")
        if isinstance(graph, list):
            stack.extend(graph)


def _extract_emails_from_scripts(soup: Any) -> List[Tuple[str, str]]:
    results: List[Tuple[str, str]] = []
    if not soup:
        return results
    for sc in soup.find_all("script"):
        try:
            content = sc.string or sc.get_text() or ""
        except Exception:
            continue
        if not content:
            continue
        for email, snippet in _extract_emails_with_obfuscation(content):
            results.append((email, snippet))
    return results


def _jsonld_person_contacts(html_text: str, soup: Any = None) -> Tuple[List[Dict[str, Any]], Any]:
    entries: List[Dict[str, Any]] = []
    if not BeautifulSoup:
        return entries, None
    if soup is None:
        try:
            soup = BeautifulSoup(html_text, "html.parser")
        except Exception:
            soup = None
    if not soup:
        return entries, None

    def _as_list(value: Any) -> List[Any]:
        if value is None:
            return []
        if isinstance(value, list):
            return value
        return [value]

    for sc in soup.find_all("script", {"type": "application/ld+json"}):
        try:
            raw_json = sc.string or sc.get_text()
            data = json.loads(raw_json or "")
        except Exception:
            continue
        for node in _iter_jsonld_nodes(data):
            node_type = node.get("@type")
            types = node_type if isinstance(node_type, list) else [node_type]
            if not any(t and isinstance(t, str) and "Person" in t for t in types):
                continue
            entry: Dict[str, Any] = {}
            name_val = node.get("name", "")
            if name_val:
                entry["name"] = str(name_val)
            phones: List[str] = []
            emails: List[str] = []
            same_as: List[str] = []
            for tel_val in _as_list(node.get("telephone")):
                formatted = fmt_phone(str(tel_val))
                if formatted:
                    phones.append(formatted)
            for cp in _as_list(node.get("contactPoint")):
                if not isinstance(cp, dict):
                    continue
                for tel_val in _as_list(cp.get("telephone")):
                    formatted = fmt_phone(str(tel_val))
                    if formatted:
                        phones.append(formatted)
                for mail_val in _as_list(cp.get("email")):
                    cleaned = clean_email(str(mail_val))
                    if cleaned and ok_email(cleaned):
                        emails.append(cleaned)
            for mail_val in _as_list(node.get("email")):
                cleaned = clean_email(str(mail_val))
                if cleaned and ok_email(cleaned):
                    emails.append(cleaned)
            for same_val in _as_list(node.get("sameAs")):
                if not same_val:
                    continue
                cleaned = str(same_val).strip()
                if cleaned:
                    same_as.append(cleaned)
            if phones:
                entry["phones"] = list(dict.fromkeys(phones))
            if emails:
                entry["emails"] = list(dict.fromkeys(emails))
            if same_as:
                entry["sameas"] = list(dict.fromkeys(same_as))
            if entry:
                entries.append(entry)

    return entries, soup


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
            data = json.loads(sc.string or sc.get_text() or "")
        except Exception:
            continue
        for node in _iter_jsonld_nodes(data):
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

    for node in soup.select('[itemprop="email"], [itemprop="telephone"]'):
        prop = node.get("itemprop", "")
        content_val = node.get("content", "")
        value = content_val or node.get_text(" ", strip=True)
        if not value:
            continue
        if "email" in prop:
            cleaned = clean_email(value)
            if cleaned and ok_email(cleaned) and cleaned not in seen_emails:
                mails.append(cleaned)
                seen_emails.add(cleaned)
                info["mailto"].append({
                    "email": cleaned,
                    "context": "microdata",
                })
        if "telephone" in prop:
            formatted = fmt_phone(value)
            if formatted:
                phones.append(formatted)
                info["tel"].append({
                    "phone": formatted,
                    "context": "microdata",
                })

    for email, snippet in _extract_emails_from_scripts(soup):
        if email in seen_emails:
            continue
        mails.append(email)
        seen_emails.add(email)
        info.setdefault("script_emails", []).append({
            "email": email,
            "context": snippet.lower(),
        })

    text_blob = soup.get_text(" ", strip=True)
    vcard_emails, vcard_phones = _extract_vcard_contacts(text_blob)
    for phone in vcard_phones:
        if phone:
            phones.append(phone)
            info["tel"].append({
                "phone": phone,
                "context": "vcard",
            })
    for mail in vcard_emails:
        if mail and mail not in seen_emails:
            mails.append(mail)
            seen_emails.add(mail)
            info["mailto"].append({
                "email": mail,
                "context": "vcard",
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


def _normalize_agent_for_query(name: str) -> str:
    """Trim noisy middle parts of an agent name for search queries."""
    parts = [p.strip() for p in name.split() if p.strip()]
    if len(parts) >= 3:
        parts = [parts[0], parts[-1]]
    return " ".join(parts)


def build_q_phone(
    name: str,
    state: str,
    *,
    city: str = "",
    postal_code: str = "",
    brokerage: str = "",
    domain_hint: str = "",
) -> List[str]:
    queries: List[str] = []

    def _add(q: str) -> None:
        if q and q not in queries:
            queries.append(q)

    broker_domain = _domain(domain_hint) if domain_hint else ""
    high_signal = _compact_tokens(f'"{name}"', '"Real Estate Agent"', '"Mobile"', state)
    _add(high_signal)
    if brokerage:
        _add(_compact_tokens(f'"{name}"', '"Direct"', f'"{brokerage}"'))
    if broker_domain:
        _add(_compact_tokens(f'"{name}"', f"site:{broker_domain}", "contact"))

    localized_base = _compact_tokens(f'"{name}"', city, state, postal_code)
    state_base = _compact_tokens(f'"{name}"', state)
    name_only = f'"{name}"'.strip()

    for base in (localized_base, state_base, name_only):
        if not base:
            continue
        _add(f"{base} contact phone")
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

    broker_domain = _domain(domain_hint) if domain_hint else ""
    _add(_compact_tokens(f'"{name}"', '"Real Estate Agent"', "email"))
    if brokerage:
        _add(_compact_tokens(f'"{name}"', '"Direct"', f'"{brokerage}"', "email"))
    if broker_domain:
        _add(_compact_tokens(f'"{name}"', f"site:{broker_domain}", "email"))

    localized_base = _compact_tokens(f'"{name}"', city, state, postal_code)
    state_base = _compact_tokens(f'"{name}"', state)

    for base in (localized_base, state_base):
        if not base:
            continue
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

    return queries

def _fallback_contact_query(agent: str, state: str, row_payload: Dict[str, Any]) -> str:
    city = str(row_payload.get("city", "")).strip()
    brokerage = str(row_payload.get("brokerage") or row_payload.get("brokerageName") or "").strip()
    domain_hint = _infer_domain_from_text(brokerage) or _guess_domain_from_brokerage(brokerage)
    tokens = [
        f'"{agent}"',
        state,
        city,
        brokerage,
        domain_hint,
        "mobile",
        "contact",
        "phone",
        "email",
    ]
    return " ".join(t for t in tokens if t)

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
_BROKERAGE_DOMAIN_CACHE: Dict[str, str] = {}
_DOMAIN_INFER_CACHE: Dict[str, str] = {}
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
        _MX_CACHE[domain] = True
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
    cleaned = re.sub(r"[^a-z0-9]", "", brokerage.lower())
    domain = f"{cleaned}.com" if cleaned else ""
    _BROKERAGE_DOMAIN_CACHE[brokerage.strip().lower()] = domain
    return domain


def _infer_domain_from_text(value: str) -> str:
    cache_key = value.strip().lower()
    _DOMAIN_INFER_CACHE[cache_key] = ""
    return ""

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


PORTAL_PROFILE_DOMAINS: Set[str] = {
    "realtor.com",
    "kw.com",
    "kellerwilliams.com",
    "exprealty.com",
    "realbroker.com",
    "realbrokerllc.com",
    "compass.com",
}


def _portal_contact_details(
    url: str,
    page: str,
    *,
    agent: str = "",
    location_tokens: Optional[Set[str]] = None,
    location_digits: Optional[Set[str]] = None,
    soup: Any = None,
) -> Dict[str, List[Dict[str, Any]]]:
    results: Dict[str, List[Dict[str, Any]]] = {"phones": [], "emails": []}
    dom = _domain(url)
    if dom not in PORTAL_PROFILE_DOMAINS:
        return results
    if not page:
        return results
    location_tokens = location_tokens or set()
    location_digits = location_digits or set()

    def add_phone(num: Any, *, label: str = "", context: str = "", name: str = "", source: str = "portal_struct") -> None:
        formatted = fmt_phone(str(num))
        if not formatted:
            return
        results["phones"].append(
            {
                "phone": formatted,
                "label": label.lower(),
                "context": context,
                "name": name,
                "source": source,
            }
        )

    def add_email(addr: Any, *, label: str = "", context: str = "", name: str = "", source: str = "portal_struct") -> None:
        cleaned = clean_email(str(addr))
        if not (cleaned and ok_email(cleaned)):
            return
        results["emails"].append(
            {
                "email": cleaned,
                "label": label.lower(),
                "context": context,
                "name": name,
                "source": source,
            }
        )

    def name_ok(name_val: str) -> bool:
        if not agent:
            return True
        return _names_match(agent, name_val)

    # JSON-LD is the most reliable signal on portals.
    try:
        soup = soup or (BeautifulSoup(page, "html.parser") if BeautifulSoup else None)
    except Exception:
        soup = None

    if soup:
        for sc in soup.find_all("script", {"type": "application/ld+json"}):
            try:
                data = json.loads(sc.string or "")
            except Exception:
                continue
            nodes = data if isinstance(data, list) else [data]
            for node in nodes:
                if not isinstance(node, dict):
                    continue
                node_type = node.get("@type")
                types = node_type if isinstance(node_type, list) else [node_type]
                is_person = any(
                    t and isinstance(t, str) and ("Person" in t or "Agent" in t)
                    for t in types
                )
                meta_name = str(node.get("name", ""))
                if is_person and meta_name and not name_ok(meta_name):
                    continue
                tel_vals = node.get("telephone") or []
                if isinstance(tel_vals, str):
                    tel_vals = [tel_vals]
                for tel in tel_vals:
                    add_phone(tel, label="jsonld", name=meta_name, source="jsonld_person")
                email_vals = node.get("email") or []
                if isinstance(email_vals, str):
                    email_vals = [email_vals]
                for em in email_vals:
                    add_email(em, label="jsonld", name=meta_name, source="jsonld_person")
                for cp in node.get("contactPoint") or []:
                    if not isinstance(cp, dict):
                        continue
                    tel = cp.get("telephone")
                    if tel:
                        add_phone(tel, label=str(cp.get("contactType") or ""), name=meta_name, source="jsonld_person")
                    email = cp.get("email")
                    if email:
                        add_email(email, label=str(cp.get("contactType") or ""), name=meta_name, source="jsonld_person")

        contact_nodes = soup.find_all(
            lambda tag: tag.name in {"a", "div", "span", "p"}
            and any(cls in (tag.get("class") or []) for cls in ["contact", "agent-contact", "agent-phone", "agent-info"])
        )
        for node in contact_nodes:
            text = node.get_text(" ", strip=True)
            low = text.lower()
            phone_match = re.search(r"(\+?[\d\-\.\(\)\s]{7,})", text)
            if phone_match:
                label = ""
                for term in ("cell", "mobile", "direct", "text"):
                    if term in low:
                        label = term
                        break
                add_phone(phone_match.group(1), label=label, context=text)
            for mail_match in EMAIL_RE.finditer(text):
                add_email(mail_match.group(0), context=text)

    if location_tokens or location_digits:
        if not _page_has_location(page, location_tokens, location_digits):
            return {"phones": [], "emails": []}

    labeled_phone_re = re.compile(r"(cell|mobile|direct|text)[^\d]{0,12}(\+?[\d\s\-().]{7,})", re.I)
    for match in labeled_phone_re.finditer(page):
        add_phone(match.group(2), label=match.group(1), context=match.group(0))

    return results


EMAIL_SOURCE_BASE = {
    "payload_contact": 1.0,
    "rapid_contact": 0.95,
    "rapid_listed_by": 0.95,
    "jsonld_person": 1.0,
    "jsonld_other": 0.7,
    "mailto": 0.8,
    "script_blob": 0.8,
    "obfuscated": 0.75,
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


def _is_junk_email(email: str) -> bool:
    if "@" not in email:
        return True
    local, domain = email.split("@", 1)
    domain_l = domain.lower()
    root = _domain(domain_l) or domain_l
    if domain_l in DISPOSABLE_EMAIL_DOMAINS or root in DISPOSABLE_EMAIL_DOMAINS:
        return True
    if any(root.endswith(f".{tld}") for tld in SPAMMY_TLDS):
        return True
    local_key = re.sub(r"[^a-z0-9]", "", local.lower())
    if len(local_key) <= 2:
        return True
    if re.fullmatch(r"[a-z]*\d{4,}", local_key):
        return True
    core = root.split(".", 1)[0]
    vowels = sum(1 for ch in core if ch in "aeiou")
    consonants = sum(1 for ch in core if ch.isalpha() and ch not in "aeiou")
    if consonants >= 6 and vowels == 0:
        return True
    return False

def _is_role_email(email: str) -> bool:
    local = email.split("@", 1)[0]
    local_key = re.sub(r"[^a-z0-9]", "", local.lower())
    return any(local_key.startswith(prefix) for prefix in ROLE_EMAIL_PREFIXES)

def _looks_direct(phone: str, agent: str, state: str, tries: int = 2) -> Optional[bool]:
    if not phone:
        return None
    # Reverse phone lookups via web search are disabled; rely on Cloudmersive only.
    return None

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
    "email_probe": 1.8,
}


MAX_CONTACT_URLS = 15
SEARCH_CONTACT_URL_LIMIT = 10


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
    cache_key = _contact_cache_key(agent, state, row_payload)

    def _redact_phone_value(phone: str) -> str:
        digits = _digits_only(phone)
        if not digits:
            return "<blank>"
        return f"...{digits[-4:]}"

    def _log_final_phone(res: Dict[str, Any]) -> None:
        label = "web_unverified"
        if res.get("source", "") == "rapid_fallback" or (
            res.get("source", "").startswith("rapid") and not res.get("verified_mobile")
        ):
            label = "rapid_fallback"
        elif res.get("verified_mobile"):
            label = "web_verified"
        LOG.info(
            "FINAL_PHONE source=%s phone=%s",
            label,
            res.get("number", "") or "<blank>",
        )

    def _log_final_decision(phone: str, source: str, reason: str) -> None:
        LOG.info(
            "FINAL_PHONE_DECISION final_phone=%s source=%s reason=%s",
            phone or "<blank>",
            source or "none",
            reason or "<none>",
        )

    def _log_search_phone(res: Dict[str, Any]) -> None:
        LOG.info(
            "SEARCH_PHONE_RESULT chosen=%s confidence=%s verified_mobile=%s source=%s",
            res.get("number", "") or "<blank>",
            res.get("confidence", "") or "<blank>",
            res.get("verified_mobile", False),
            res.get("source", "") or "<blank>",
        )

    def _finalize(res: Dict[str, Any]) -> Dict[str, Any]:
        res.setdefault("verified_mobile", False)
        _contact_cache_set(cache_p, cache_key, res)
        _log_final_phone(res)
        return res

    if not cache_p:
        with _rapid_cache_lock:
            _rapid_contact_cache.clear()
            _rapid_logged.clear()

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
            return _finalize(result)

    cached = _contact_cache_get(cache_p, cache_key)
    if cached is not None:
        _log_final_phone(cached)
        return cached

    zpid = str(row_payload.get("zpid", ""))
    rapid_snapshot = _rapid_contact_normalized(agent, row_payload)
    rapid_candidates = rapid_snapshot.get("rapid_candidates", []) if rapid_snapshot else []
    rapid_primary_phone = rapid_snapshot.get("rapid_primary_phone", "") if rapid_snapshot else ""
    rapid_phone_reason = rapid_snapshot.get("phone_reason", "") if rapid_snapshot else ""
    rapid_phone_score = rapid_snapshot.get("phone_score", 0) if rapid_snapshot else 0
    rapid_best_phone = rapid_snapshot.get("selected_phone", "") if rapid_snapshot else ""
    redacted_rapid = [_redact_phone_value(item.get("phone", "")) for item in rapid_candidates]
    LOG.info(
        "RAPID_PHONE_CANDIDATES count=%s phones=%s selected=%s score_reason=%s score=%s",
        len(rapid_candidates),
        redacted_rapid,
        rapid_best_phone or "<blank>",
        rapid_phone_reason or "<none>",
        rapid_phone_score,
    )

    if rapid_primary_phone:
        rapid_result = {
            "number": rapid_primary_phone,
            "confidence": "high",
            "score": max(CONTACT_PHONE_MIN_SCORE, CONTACT_PHONE_LOW_CONF + 0.5),
            "source": "rapid_contact_cloudmersive_mobile",
            "reason": rapid_phone_reason or "rapid_verified_mobile",
            "verified_mobile": True,
        }
        _log_final_decision(rapid_primary_phone, "rapid_verified_mobile", rapid_phone_reason)
        return _finalize(rapid_result)

    def _apply_final_decision(search_res: Dict[str, Any]) -> Dict[str, Any]:
        _log_search_phone(search_res)

        final_result = search_res
        decision_source = "none"
        decision_reason = ""
        if search_res.get("number") and search_res.get("verified_mobile"):
            decision_source = "search_verified_mobile"
            decision_reason = "search_verified_mobile"
        elif rapid_best_phone:
            final_result = {
                "number": rapid_best_phone,
                "confidence": "low",
                "score": max(search_res.get("score", 0.0), CONTACT_PHONE_OVERRIDE_MIN),
                "source": "rapid_fallback",
                "reason": rapid_phone_reason or "rapid_fallback",
                "verified_mobile": False,
            }
            decision_source = "rapid_fallback"
            decision_reason = rapid_phone_reason or "rapid_fallback"
        else:
            if search_res.get("number"):
                decision_source = "search_unverified"
                decision_reason = "search_unverified"
            else:
                decision_source = "none"
                decision_reason = "no_phone_candidates"

        _log_final_decision(final_result.get("number", ""), decision_source, decision_reason)
        return _finalize(final_result)

    candidates: Dict[str, Dict[str, Any]] = {}
    had_candidates = False
    brokerage_hint = (row_payload.get("brokerageName") or row_payload.get("brokerage") or "").strip()
    location_extras: List[str] = [brokerage_hint] if brokerage_hint else []
    processed_urls: Set[str] = set()
    mirror_hits: Set[str] = set()
    trusted_domains: Set[str] = set()
    blocked_domains: Set[str] = set()
    search_empty = False
    cse_status = _cse_last_state
    domain_hint = (
        row_payload.get("domain_hint", "").strip()
        or _infer_domain_from_text(brokerage_hint)
        or _infer_domain_from_text(agent)
    )

    mobile_terms = {"cell", "mobile", "text", "direct", "sms"}

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
        mobile_hint: bool = False,
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
            mobile_hint = mobile_hint or any(term in ctx for term in mobile_terms)
            info["contexts"].append(ctx)
            if not info["office_demoted"] and not trusted_domain:
                if any(term in ctx for term in PHONE_OFFICE_TERMS):
                    info["score"] -= 0.6
                    info["score"] -= 0.35
                    if "fax" in ctx:
                        info["score"] -= 0.4
                    info["office_demoted"] = True
                    LOG.debug("PHONE DEMOTE office: %s", formatted)
            if mobile_hint and not info["office_demoted"]:
                info["score"] += 0.55
                info["direct_ok"] = True
        elif mobile_hint and not info["office_demoted"]:
            info["score"] += 0.55
            info["direct_ok"] = True
        if office_flag and not info["office_demoted"]:
            info["score"] -= 1.2
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

    search_result: Optional[Dict[str, Any]] = None
    enrichment = _contact_enrichment(agent, state, row_payload)
    enrichment_done = bool(enrichment.get("_two_stage_done") and enrichment.get("_two_stage_candidates", 0) > 0)
    enriched_phone = enrichment.get("best_phone", "")
    enriched_conf = int(enrichment.get("best_phone_confidence", 0) or 0)
    if enriched_phone:
        line_info = get_line_info(enriched_phone)
        verified_mobile = bool(line_info.get("mobile_verified"))
        if enriched_conf >= CONTACT_PHONE_LOW_CONF or verified_mobile:
            confidence = "high" if enriched_conf >= 80 or verified_mobile else "low"
            search_result = {
                "number": enriched_phone,
                "confidence": confidence,
                "score": max(CONTACT_PHONE_LOW_CONF, enriched_conf / 25),
                "source": enrichment.get("best_phone_source_url", "two_stage_cse"),
                "reason": "two_stage_cse",
                "evidence": enrichment.get("best_phone_evidence", ""),
                "verified_mobile": verified_mobile,
            }
    # If two-stage search produced no phone, continue to other sources (including RapidAPI).
    if search_result is not None:
        return _apply_final_decision(search_result)

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
            domain_hint=domain_hint or brokerage_hint,
        )
    )
    hint_key = _normalize_override_key(agent, state)
    hint_urls = PROFILE_HINTS.get(hint_key) or PROFILE_HINTS.get(hint_key.lower(), [])
    hint_urls = [url for url in hint_urls if url]
    parts = [p for p in agent.split() if p]
    first_name = re.sub(r"[^a-z]", "", parts[0].lower()) if parts else ""
    last_name = re.sub(r"[^a-z]", "", (parts[-1] if len(parts) > 1 else parts[0]).lower()) if parts else ""
    first_variants, last_token = _first_last_tokens(agent)
    location_hint = " ".join(
        part for part in (str(row_payload.get("city") or "").strip(), state) if part
    )

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
        domain = _domain(url)
        trusted_hit = trusted or (domain in trusted_domains if domain else False)
        page_title = ""
        jsonld_entries, soup = _jsonld_person_contacts(page)
        if soup and soup.title and soup.title.string:
            page_title = soup.title.string.strip()
        if jsonld_entries:
            sameas_links: List[str] = []
            jsonld_phone_found = False
            for entry in jsonld_entries:
                meta_name = entry.get("name", "")
                name_match = _names_match(agent, meta_name) if meta_name else False
                for tel in entry.get("phones", []):
                    if _register(
                        tel,
                        "jsonld_person",
                        url=url,
                        page_title=page_title,
                        meta_name=meta_name,
                        name_match=name_match,
                        trusted=trusted_hit,
                        bonus=0.25,
                    ):
                        jsonld_phone_found = True
                sameas_links.extend(entry.get("sameas", []))
            _record_sameas_links(row_payload, sameas_links)
            if jsonld_phone_found:
                return True

        portal_contacts = _portal_contact_details(
            url,
            page,
            agent=agent,
            location_tokens=location_tokens,
            location_digits=location_digits,
            soup=soup,
        )
        portal_hit = bool(portal_contacts.get("phones"))
        if not trusted and not _page_has_name(page) and not portal_hit:
            return False
        page_viable = False
        ph, _, meta, info = extract_struct(page)
        page_title = page_title or info.get("title", "")
        for entry in portal_contacts.get("phones", []):
            label = entry.get("label", "")
            mobile_hint = any(term in label for term in mobile_terms)
            meta_name = entry.get("name", "")
            name_match = _names_match(agent, meta_name) if meta_name else False
            if _register(
                entry.get("phone", ""),
                entry.get("source", "portal_struct"),
                url=url,
                page_title=page_title,
                context=entry.get("context", ""),
                meta_name=meta_name,
                name_match=name_match,
                trusted=trusted_hit,
                mobile_hint=mobile_hint,
            ):
                page_viable = True
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
            context_val = anchor.get("context", "")
            if _register(
                anchor.get("phone", ""),
                "agent_card_dom",
                url=url,
                page_title=page_title,
                context=context_val,
                trusted=trusted_hit,
                mobile_hint=any(term in str(context_val).lower() for term in mobile_terms),
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
        page, mirrored, _ = fetch_contact_page(url)
        processed_urls.add(low)
        if mirrored:
            mirror_hits.add(domain)
        if not page:
            return False
        return _process_page(url, page, trusted=trusted or domain in trusted_domains)

    priority_urls = list(dict.fromkeys(hint_urls))
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

    urls: List[str] = list(priority_urls)
    if not _has_viable_phone_candidate():
        fallback_urls, search_empty, cse_status, _ = _normalize_contact_search_result(
            _contact_search_urls(
                agent,
                state,
                row_payload,
                domain_hint=domain_hint or brokerage_hint,
                brokerage=brokerage_hint,
                limit=SEARCH_CONTACT_URL_LIMIT,
                include_exhausted=True,
                allow_portals=False,
            )
        )
        urls.extend(fallback_urls)
        if fallback_urls:
            trusted_domains.update(_build_trusted_domains(agent, fallback_urls))
    urls = list(dict.fromkeys(urls))
    if len(urls) > MAX_CONTACT_URLS:
        urls = urls[:MAX_CONTACT_URLS]
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
        LOG.info("PHONE LOOKUP fallback: skipping alt search to honor single-search budget")

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
        if info.get("direct_ok") is False and info.get("sources", set()) <= {"rapid_contact", "rapid_listed_by"}:
            LOG.debug("PHONE DROP rapid candidate after direct check: %s", number)
            continue
        if (
            info.get("office_flag_rapid")
            and info.get("direct_ok") is False
            and info.get("sources", set()) & {"rapid_contact", "rapid_listed_by"}
        ):
            continue
        line_info = get_line_info(number)
        if not line_info.get("valid"):
            LOG.debug("PHONE DROP invalid number: %s", number)
            continue
        if line_info.get("country") and line_info.get("country") != "US":
            LOG.debug("PHONE DROP non-US number: %s (%s)", number, line_info.get("country"))
            continue
        if info.get("sources", set()) <= {"rapid_contact", "rapid_listed_by"}:
            direct_flag = info.get("direct_ok")
            if direct_flag is None:
                direct_flag = _looks_direct(number, agent, state)
            if direct_flag is False:
                LOG.debug("PHONE DROP rapid after final direct check: %s", number)
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

    if best_number:
        candidate_info = candidates.get(best_number, {})
        if (
            candidate_info.get("office_flag_rapid")
            and candidate_info.get("direct_ok") is False
            and candidate_info.get("sources", set()) & {"rapid_contact", "rapid_listed_by"}
        ):
            best_number = ""
            best_score = float("-inf")
            best_source = ""
            best_is_mobile = False
        elif candidate_info.get("sources", set()) <= {"rapid_contact", "rapid_listed_by"}:
            direct_flag = candidate_info.get("direct_ok")
            if direct_flag is None:
                direct_flag = _looks_direct(best_number, agent, state)
            if direct_flag is False:
                LOG.debug("PHONE DROP rapid best_number after direct check: %s", best_number)
                best_number = ""
                best_score = float("-inf")
                best_source = ""
                best_is_mobile = False

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
        "verified_mobile": False,
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
            verified_mobile = bool(get_line_info(best_number).get("mobile_verified"))
            search_result = {
                "number": best_number,
                "confidence": confidence,
                "score": adjusted_score,
                "source": best_source,
                "verified_mobile": verified_mobile,
            }

    if search_result is None and candidates:
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
                    "verified_mobile": False,
                }
            )
            search_result = result

    if search_result is None:
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
        blocked_state = {dom: _blocked_until.get(dom, 0.0) - time.time() for dom in blocked_domains}
        candidate_quality = {
            "phones_found": len(candidates),
            "emails_found": 0,
            "all_office": all(info.get("office_demoted") for info in candidates.values()) if candidates else False,
            "all_generic_email": False,
        }
        LOG.warning(
            "PHONE DROP no verified mobile for %s %s zpid=%s reason=%s had_candidates=%s cse_state=%s search_empty=%s blocked_domains=%s candidates=%s quality=%s",
            agent,
            state,
            zpid or "",
            reason,
            had_candidates,
            cse_status,
            search_empty,
            {k: round(v, 2) for k, v in blocked_state.items()},
            len(candidates),
            candidate_quality,
        )
        search_result = result

    return _apply_final_decision(search_result)

def lookup_email(agent: str, state: str, row_payload: Dict[str, Any]) -> Dict[str, Any]:
    cache_key = _contact_cache_key(agent, state, row_payload)
    email_rejections: List[Tuple[str, str]] = []
    reviewed_urls: Set[str] = set()
    shortlisted_urls: Set[str] = set()
    pipeline_summary = {
        "shortlist": 0,
        "reviewed": 0,
        "emails_found": 0,
    }

    def _log_pipeline_summary(res: Dict[str, Any]) -> None:
        pipeline_summary["shortlist"] = len(shortlisted_urls)
        pipeline_summary["reviewed"] = len(reviewed_urls)
        LOG.info(
            "EMAIL_PIPELINE_SUMMARY agent=%s state=%s shortlist=%s reviewed=%s emails_found=%s chosen=%s reason=%s",
            agent,
            state,
            pipeline_summary["shortlist"],
            pipeline_summary["reviewed"],
            pipeline_summary["emails_found"],
            res.get("email", "") or "<blank>",
            res.get("reason", "") or "",
        )

    def _log_final_email(res: Dict[str, Any]) -> None:
        LOG.info(
            "FINAL_EMAIL source=%s email=%s rejected_candidates=%s",
            res.get("source", "") or "",
            res.get("email", "") or "<blank>",
            email_rejections,
        )

    def _finalize(res: Dict[str, Any]) -> Dict[str, Any]:
        _contact_cache_set(cache_e, cache_key, res)
        LOG.info(
            "EMAIL_DECISION chosen=%s reason=%s source=%s score=%.2f",
            res.get("email", "") or "<blank>",
            res.get("reason", "") or "",
            res.get("source", "") or "",
            float(res.get("score", 0.0) or 0.0),
        )
        _log_final_email(res)
        _log_pipeline_summary(res)
        return res

    if not cache_p:
        with _rapid_cache_lock:
            _rapid_contact_cache.clear()
            _rapid_logged.clear()

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
            return _finalize(result)

    cached = _contact_cache_get(cache_e, cache_key)
    if cached is not None:
        _log_final_email(cached)
        return cached

    hint_key = _normalize_override_key(agent, state)
    hint_urls = PROFILE_HINTS.get(hint_key) or PROFILE_HINTS.get(hint_key.lower(), [])
    for url in hint_urls or []:
        try:
            page, _, _ = fetch_contact_page(url)
        except Exception:
            continue
        if not page:
            continue
        candidates = _agent_contact_candidates_from_html(page, url, agent)
        page_low = page.lower()
        for cand in candidates:
            for em in cand.get("emails", []):
                cleaned = clean_email(em)
                if not cleaned:
                    continue
                if not _email_matches_name(agent, cleaned) or _is_junk_email(cleaned):
                    continue
                evidence = str(cand.get("evidence_snippet", "") or "")
                if f"mailto:{cleaned.lower()}" in page_low:
                    source_label = "mailto"
                elif "application/ld+json" in page_low:
                    source_label = "jsonld_person"
                elif evidence:
                    source_label = evidence.replace(" ", "_")
                else:
                    source_label = "hint_url"
                result = {
                    "email": cleaned,
                    "confidence": "low",
                    "score": max(CONTACT_EMAIL_LOW_CONF, CONTACT_EMAIL_FALLBACK_SCORE),
                    "source": source_label,
                    "source_url": url,
                    "reason": "hint_url",
                    "evidence": cand.get("evidence_snippet", ""),
                }
                return _finalize(result)

    enrichment = _contact_enrichment(agent, state, row_payload)
    email_rejections = enrichment.get("_email_rejected", []) or []
    enrichment_done = bool(enrichment.get("_two_stage_done") and enrichment.get("_two_stage_candidates", 0) > 0)
    enriched_email = enrichment.get("best_email", "")
    if enriched_email:
        confidence_score = enrichment.get("best_email_confidence", 0)
        confidence = "high" if confidence_score >= 80 else "low"
        source_label = (
            enrichment.get("best_email_evidence")
            or enrichment.get("best_email_source_url", "two_stage_cse")
        )
        source_label = str(source_label or "")
        if "://" in source_label:
            if str(enrichment.get("best_email_source_url", "")).startswith("mailto:"):
                source_label = "mailto"
            else:
                source_label = "web_page"
        source_label = source_label.replace(" ", "_")
        result = {
            "email": enriched_email,
            "confidence": confidence,
            "score": max(CONTACT_EMAIL_LOW_CONF, confidence_score / 25),
            "source": source_label,
            "source_url": enrichment.get("best_email_source_url", ""),
            "reason": "two_stage_cse",
            "evidence": enrichment.get("best_email_evidence", ""),
        }
        return _finalize(result)
    if enrichment_done:
        return _finalize(
            {
                "email": "",
                "confidence": "",
                "score": 0.0,
                "source": "two_stage_cse",
                "reason": "two_stage_no_email",
            }
        )

    brokerage = (row_payload.get("brokerageName") or row_payload.get("brokerage") or "").strip()
    domain_hint = mls_id = ""
    zpid = str(row_payload.get("zpid", ""))
    candidates: Dict[str, Dict[str, Any]] = {}
    generic_seen: Set[str] = set()
    had_candidates = False
    location_extras: List[str] = [brokerage] if brokerage else []
    blocked_domains: Set[str] = set()
    trusted_domains: Set[str] = set(TRUSTED_CONTACT_DOMAINS)
    brokerage_domain_guess = _guess_domain_from_brokerage(brokerage)
    brokerage_domain = _domain(domain_hint or brokerage_domain_guess or brokerage)
    cse_rate_limited = False
    cse_status: str = "idle"
    search_empty = False

    inferred_domain_hint = _infer_domain_from_text(brokerage) or _infer_domain_from_text(agent)
    if inferred_domain_hint and not domain_hint:
        domain_hint = inferred_domain_hint
    if domain_hint:
        hint_domain = _domain(domain_hint) or domain_hint.lower()
        trusted_domains.add(hint_domain)
    if brokerage_domain:
        trusted_domains.add(brokerage_domain)
    if brokerage_domain_guess and brokerage_domain_guess != brokerage_domain:
        trusted_domains.add(brokerage_domain_guess)
    preferred_email_domains: Set[str] = set()
    preferred_email_domains.update(_preferred_email_domains_for_text(brokerage))
    preferred_email_domains.update(
        _preferred_email_domains_for_text(
            row_payload.get("brokerageName", ""),
            row_payload.get("company", ""),
        )
    )
    if brokerage_domain:
        preferred_email_domains.add(brokerage_domain)
    if domain_hint:
        preferred_email_domains.add(_domain(domain_hint) or domain_hint.lower())

    tokens = _agent_tokens(agent)
    IDENTITY_SOURCES = {
        "mailto",
        "dom",
        "jsonld_other",
        "jsonld_person",
        "cse_contact",
        "pattern",
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
        role_email = _is_role_email(cleaned)
        if role_email and not ALLOW_ROLE_EMAIL_FALLBACK:
            return
        if url and not _contact_source_allowed(url, brokerage_domain, trusted_domains):
            return
        low = cleaned.lower()
        matches_agent = _email_matches_name(agent, cleaned)
        is_generic = _is_generic_email(cleaned)
        email_domain = cleaned.split("@", 1)[1].lower() if "@" in cleaned else ""
        preferred_hit = email_domain in preferred_email_domains
        if is_generic and not matches_agent:
            if low in generic_seen:
                return
            generic_seen.add(low)
        elif is_generic:
            generic_seen.add(low)

        domain = _domain(url) if url else ""
        trusted_domain = trusted or (domain in trusted_domains if domain else False)
        if not trusted_domain and brokerage_domain:
            trusted_domain = bool(domain) and (
                domain == brokerage_domain or domain.endswith(f".{brokerage_domain}")
            )
        haystack_components = [context, page_title, meta_name]
        page_agent_hit = _page_mentions_agent(" ".join(haystack_components), agent) if any(haystack_components) else False

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
                "trusted_hit": False,
                "agent_on_page": False,
                "role_email": role_email,
                "preferred_domains": set(),
                "preferred_bonus": False,
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
            info["trusted_hit"] = True
        if is_generic and not matches_agent and not trusted_domain and not info["generic_penalized"]:
            info["score"] -= 0.25
            info["generic_penalized"] = True
        if preferred_hit and not info.get("preferred_bonus"):
            info["preferred_bonus"] = True
            info["preferred_domains"].add(email_domain)
            info["score"] += 0.4
        if source in IDENTITY_SOURCES and source not in info["identity_sources"]:
            info["identity_sources"].add(source)
            info["identity_hits"] += 1
        if source == "jsonld_person" and (matches_agent or (meta_name and _names_match(agent, meta_name))):
            info["score"] += 0.2
        elif source == "mailto" and (matches_agent or (meta_name and _names_match(agent, meta_name))):
            info["score"] += 0.1
        if role_email:
            info["role_email"] = True
            if not ALLOW_ROLE_EMAIL_FALLBACK:
                info["penalty_applied"] = True
                info["score"] -= 0.5
            else:
                info["score"] -= 0.35
        if page_agent_hit and not info.get("agent_on_page"):
            info["agent_on_page"] = True
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
    location_tokens, location_digits = _collect_location_hints(
        row_payload,
        state,
        *[hint for hint in location_extras if hint],
    )
    location_hint = " ".join(
        part for part in (str(row_payload.get("city") or "").strip(), state) if part
    )

    queries = build_q_email(
        agent,
        state,
        brokerage,
        domain_hint,
        mls_id,
        city=row_payload.get("city", ""),
        postal_code=row_payload.get("zip", ""),
    )
    hint_key = _normalize_override_key(agent, state)
    hint_urls = PROFILE_HINTS.get(hint_key) or PROFILE_HINTS.get(hint_key.lower(), [])
    hint_urls = [url for url in hint_urls if url]
    brokerage_urls: List[str] = []
    authority_urls: List[str] = []

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

    def _shortlist_urls(
        urls: Iterable[str],
        *,
        stage: str,
        relaxed: bool = False,
        allow_portals: bool = False,
    ) -> List[str]:
        selected, rejected = select_top_5_urls(
            urls,
            fetch_check=False,
            relaxed=relaxed,
            allow_portals=allow_portals,
            property_state=state,
            property_city=str(row_payload.get("city") or ""),
            brokerage=brokerage,
            agent=agent,
            limit=5,
            existing=list(reviewed_urls),
            location_hint=location_hint,
        )
        for url in selected:
            shortlisted_urls.add(normalize_url(url))
        LOG.info(
            "EMAIL_SHORTLIST stage=%s eligible=%s rejected=%s urls=%s",
            stage,
            len(selected),
            len(rejected),
            json.dumps(_compact_url_log(selected), separators=(",", ":")),
        )
        if rejected:
            LOG.info("EMAIL_SHORTLIST_REJECTED stage=%s rejected=%s", stage, rejected)
        return selected

    def _review_url(url: str, *, stage: str) -> int:
        dom = _domain(url)
        page, _, method = fetch_contact_page(url)
        reviewed_urls.add(normalize_url(url))
        if not page:
            if _blocked(dom):
                blocked_domains.add(dom)
            LOG.info(
                "EMAIL_REVIEW url=%s domain=%s method=%s candidates=0 stage=%s",
                url,
                dom,
                method or "unknown",
                stage,
            )
            return 0
        new_count = _process_page(url, page)
        LOG.info(
            "EMAIL_REVIEW url=%s domain=%s method=%s candidates=%s stage=%s",
            url,
            dom,
            method or "unknown",
            new_count,
            stage,
        )
        return new_count

    def _process_page(url: str, page: str) -> int:
        if not page:
            return 0
        before = set(candidates.keys())
        dom = _domain(url)
        domain_hint_hit = bool(domain_hint and dom.endswith(domain_hint.lower()))
        page_title = ""
        seen: Set[str] = set()
        jsonld_entries, soup = _jsonld_person_contacts(page)
        if soup and soup.title and soup.title.string:
            page_title = soup.title.string.strip()
        preferred_email_domains.update(_preferred_email_domains_for_text(page[:4000], brokerage, page_title))
        trusted_hit = dom in trusted_domains or domain_hint_hit
        if jsonld_entries:
            sameas_links: List[str] = []
            jsonld_email_found = False
            for entry in jsonld_entries:
                meta_name = entry.get("name", "")
                sameas_links.extend(entry.get("sameas", []))
                for mail in entry.get("emails", []):
                    seen.add(mail)
                    _register(
                        mail,
                        "jsonld_person",
                        url=url,
                        page_title=page_title,
                        meta_name=meta_name,
                        trusted=trusted_hit,
                    )
                    patt = _pattern_from_example(mail, agent)
                    if patt:
                        domain_patterns.setdefault(_domain(mail), patt)
                    jsonld_email_found = True
            _record_sameas_links(row_payload, sameas_links)
            if jsonld_email_found:
                return len(set(candidates.keys()) - before)

        portal_contacts = _portal_contact_details(
            url,
            page,
            agent=agent,
            location_tokens=location_tokens,
            location_digits=location_digits,
            soup=soup,
        )
        portal_hit = bool(portal_contacts.get("emails"))
        if not _page_has_name(page, domain_hint_hit=domain_hint_hit) and not portal_hit:
            return len(set(candidates.keys()) - before)
        _, ems, meta, info = extract_struct(page)
        page_title = page_title or info.get("title", "")
        domain = dom
        for entry in portal_contacts.get("emails", []):
            meta_name = entry.get("name", "")
            _register(
                entry.get("email", ""),
                entry.get("source", "portal_struct"),
                url=url,
                page_title=page_title,
                context=entry.get("context", ""),
                meta_name=meta_name,
                trusted=trusted_hit,
            )
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
        for item in info.get("script_emails", []):
            mail = item.get("email", "")
            if not mail:
                continue
            if mail in seen:
                continue
            seen.add(mail)
            _register(
                mail,
                "script_blob",
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
        for mail, snippet in _extract_emails_with_obfuscation(page):
            if mail in seen:
                continue
            seen.add(mail)
            _register(
                mail,
                "obfuscated",
                url=url,
                page_title=page_title,
                context=snippet,
                trusted=trusted_hit,
            )
        return len(set(candidates.keys()) - before)

    def _has_viable_email_candidate() -> bool:
        return any(info.get("score", 0.0) >= CONTACT_EMAIL_FALLBACK_SCORE for info in candidates.values())

    priority_urls = list(dict.fromkeys(brokerage_urls + authority_urls + hint_urls))
    trusted_domains.update(_build_trusted_domains(agent, priority_urls))
    priority_review_urls = _shortlist_urls(priority_urls, stage="priority")
    if len(priority_review_urls) < 2 and priority_urls:
        priority_review_urls = _shortlist_urls(
            priority_urls,
            stage="priority-broadened",
            relaxed=True,
            allow_portals=True,
        )
    priority_non_portal, priority_portal = _split_portals(priority_review_urls)

    processed = 0
    for url in priority_non_portal:
        _review_url(url, stage="priority")
        if _has_viable_email_candidate():
            break
        processed += 1
        if processed >= 4 and candidates:
            break

    urls: List[str] = list(priority_urls)
    if not _has_viable_email_candidate():
        fallback_urls, search_empty, cse_status, _ = _normalize_contact_search_result(
            _contact_search_urls(
                agent,
                state,
                row_payload,
                domain_hint=domain_hint or brokerage,
                brokerage=brokerage,
                limit=SEARCH_CONTACT_URL_LIMIT,
                include_exhausted=True,
            )
        )
        urls.extend(fallback_urls)
        if fallback_urls:
            trusted_domains.update(_build_trusted_domains(agent, fallback_urls))
        if search_empty:
            cse_status = _cse_last_state
            cse_rate_limited = cse_status in {"throttled", "blocked"}
            if ENABLE_SYNTH_EMAIL_FALLBACK:
                synth_domains: List[str] = []
                if domain_hint:
                    synth_domains.append(domain_hint)
                guessed_dom = _guess_domain_from_brokerage(brokerage)
                if guessed_dom:
                    synth_domains.append(guessed_dom)
                for dom in synth_domains:
                    synthetic_candidates = _synth_from_tokens(agent, {dom})
                    if synthetic_candidates:
                        guess = synthetic_candidates[0]
                        guess_dom = guess.split("@", 1)[1] if "@" in guess else dom
                        _register(
                            guess,
                            "synthetic_pattern",
                            url=f"https://{guess_dom}" if guess_dom else "",
                            trusted=True,
                        )
                        break
                else:
                    synthetic_candidates = _synth_from_tokens(agent, set(synth_domains))
                    if synthetic_candidates:
                        guess = synthetic_candidates[0]
                        dom = guess.split("@", 1)[1] if "@" in guess else ""
                        _register(
                            guess,
                            "synthetic_pattern",
                            url=f"https://{dom}" if dom else "",
                            trusted=True,
                        )
    urls = list(dict.fromkeys(urls))
    if len(urls) > MAX_CONTACT_URLS:
        urls = urls[:MAX_CONTACT_URLS]
    review_urls = _shortlist_urls(urls, stage="search")
    if len(review_urls) < 2 and urls:
        review_urls = _shortlist_urls(
            urls,
            stage="search-broadened",
            relaxed=True,
            allow_portals=True,
        )
    non_portal, portal = _split_portals(review_urls)

    processed = 0
    for url in non_portal:
        _review_url(url, stage="search")
        if _has_viable_email_candidate():
            break
        processed += 1
        if processed >= 4 and candidates:
            break

    if not candidates:
        processed = 0
        for url in portal:
            _review_url(url, stage="portal")
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

    normalized_brokerage_tokens = [
        tok
        for tok in re.sub(r"[^a-z0-9]+", " ", brokerage.lower()).split()
        if len(tok) >= 4
    ] if brokerage else []

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

    filtered_candidates: Dict[str, Dict[str, Any]] = {}
    domain_match_required = bool(domain_hint or brokerage_domain or trusted_domains)
    for email, info in candidates.items():
        domain_hint_hit, brokerage_domain_ok, brokerage_hit, agent_token_hit, domain_l, domain_root = _domain_signals(email, info)
        haystacks: List[str] = []
        haystacks.extend(info.get("contexts", []))
        haystacks.extend(info.get("page_titles", []))
        haystacks.extend(info.get("meta_names", []))
        agent_on_page = info.get("agent_on_page") or _page_mentions_agent(" ".join(haystacks), agent)
        trusted_hit = info.get("trusted_hit") or domain_l in trusted_domains or domain_root in trusted_domains
        role_email = info.get("role_email")
        if role_email and not ALLOW_ROLE_EMAIL_FALLBACK:
            continue
        sources = info.get("sources", set())
        rapid_source = any(src.startswith("rapid") for src in sources)
        name_match = _email_matches_name(agent, email)
        if not name_match and not rapid_source:
            continue
        domain_matches_known = brokerage_domain_ok or domain_hint_hit or brokerage_hit or trusted_hit or agent_token_hit
        if domain_match_required and not domain_matches_known:
            continue
        if not domain_match_required and not (domain_matches_known or agent_on_page):
            continue
        filtered_candidates[email] = info
    agent_match_filtered = bool(candidates) and not filtered_candidates
    if filtered_candidates:
        candidates = filtered_candidates
    elif agent_match_filtered:
        candidates = {}

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

    pipeline_summary["emails_found"] = len(candidates)
    if candidates:
        candidate_log = []
        for email, info in sorted(
            candidates.items(),
            key=lambda item: item[1].get("final_score", item[1].get("score", 0.0)),
            reverse=True,
        )[:8]:
            candidate_log.append(
                {
                    "email": email,
                    "score": round(info.get("final_score", info.get("score", 0.0)), 3),
                    "source": info.get("best_source") or next(iter(info.get("sources", [])), ""),
                    "domain": info.get("domain", ""),
                }
            )
        LOG.info(
            "EMAIL_FINAL_CANDIDATES count=%s candidates=%s",
            len(candidates),
            json.dumps(candidate_log, separators=(",", ":")),
        )
    else:
        LOG.info("EMAIL_FINAL_CANDIDATES count=0 candidates=[]")

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
            return _finalize(result)
        result["confidence"] = "high"
        LOG.debug(
            "EMAIL WIN %s via %s score=%.2f",
            best_email,
            best_source or "unknown",
            best_score,
        )
        return _finalize(result)

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
        strong_identity = (
            identity_hits >= 1
            and info.get("best_source") == "jsonld_person"
            and (
                domain_contains_agent
                or domain_hint_hit
                or brokerage_domain_ok
                or brokerage_hit
                or context_match
            )
        )
        fallback_ok = (
            agent_match
            or identity_hits >= 2
            or bool(strong_sources)
            or bool(identity_sources - {"dom"})
            or domain_hint_hit
            or brokerage_hit
            or brokerage_domain_ok
            or strong_identity
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
            return _finalize(result)

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
            return _finalize(result)

    if not result.get("confidence"):
        rapid_snapshot = _rapid_contact_normalized(agent, row_payload)
        rapid_email = rapid_snapshot.get("selected_email", "") if rapid_snapshot else ""
        rapid_email_reason = rapid_snapshot.get("email_reason", "") if rapid_snapshot else ""
        if rapid_email:
            data = rapid_snapshot.get("data", {}) if rapid_snapshot else {}
            source = "rapid_fallback"
            if data:
                listed_by = data.get("listed_by") or {}
                if listed_by.get("emails") or listed_by.get("email"):
                    source = "rapid_listed_by"
                elif data.get("contact_recipients"):
                    source = "rapid_contact"
            LOG.info("RAPID_FALLBACK_EMAIL email=%s source=%s", rapid_email, source)
            return _finalize(
                {
                    "email": rapid_email,
                    "confidence": "low",
                    "score": max(CONTACT_EMAIL_FALLBACK_SCORE, CONTACT_EMAIL_MIN_SCORE),
                    "source": source,
                    "reason": rapid_email_reason or "rapid_fallback",
                }
            )

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
            return _finalize(result)

    if not had_candidates:
        reason = "cse_rate_limited" if cse_rate_limited else "no_personal_email"
    elif not candidates or agent_match_filtered:
        reason = "no_agent_match"
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
            "EMAIL DEFER for %s %s – CSE blocked/rate limited (cse_state=%s search_empty=%s blocked_domains=%s candidates=%s)",
            agent,
            state,
            cse_status,
            search_empty,
            {dom: _blocked_until.get(dom, 0.0) - time.time() for dom in blocked_domains},
            len(candidates),
        )
    candidate_quality = {
        "phones_found": 0,
        "emails_found": len(candidates),
        "all_office": False,
        "all_generic_email": all(info.get("generic") for info in candidates.values()) if candidates else False,
    }
    LOG.warning(
        "EMAIL FAIL – no agent-matching email for %s %s reason=%s cse_state=%s search_empty=%s blocked_domains=%s candidates=%s quality=%s",
        agent,
        state,
        reason,
        cse_status,
        search_empty,
        {dom: _blocked_until.get(dom, 0.0) - time.time() for dom in blocked_domains},
        len(candidates),
        candidate_quality,
    )
    return _finalize(result)


def is_active_listing(row_payload: Dict[str, Any]) -> bool:
    if not row_payload:
        return True
    status = ""
    for key in ("homeStatus", "status", "listingStatus", "home_status"):
        value = row_payload.get(key)
        if isinstance(value, str) and value.strip():
            status = value.strip().upper()
            break
    if not status:
        for path in (
            ("hdpData", "homeInfo", "homeStatus"),
            ("property", "homeStatus"),
            ("listing", "status"),
        ):
            value = _nested_value(row_payload, list(path))
            if isinstance(value, str) and value.strip():
                status = value.strip().upper()
                break
    return (not status) or status in GOOD_STATUS

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
    if len(vals) > COL_ZPID and vals[COL_ZPID]:
        record_seen_zpid(str(vals[COL_ZPID]))
    LOG.info("Row appended to sheet (row %s); next hint %s", row_idx, _next_row_hint)
    return row_idx

def phone_exists(p):
    normalized = _normalize_phone_for_dedupe(p or "")
    return bool(normalized) and normalized in seen_phones

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
    val = str(value).strip().lower()
    return any(tok in val for tok in ("mobile", "wireless", "cellular"))


def get_line_info(phone: str) -> Dict[str, Any]:
    """Return Cloudmersive classification for *phone* with caching.

    The result dictionary contains ``valid`` (bool), ``mobile`` (bool),
    ``mobile_verified`` (bool when explicitly Mobile/Wireless), ``country``
    (upper-case ISO code or empty string) and ``type`` (Cloudmersive line
    classification). When Cloudmersive is unavailable we still return basic
    validation but do not treat the number as verified mobile.
    """

    if not phone:
        return {"valid": False, "mobile": False, "mobile_verified": False, "country": "", "type": ""}
    if phone in _line_info_cache:
        return _line_info_cache[phone]

    info = {
        "valid": valid_phone(phone),
        "mobile": False,
        "mobile_verified": False,
        "country": "US",
        "type": "unknown",
    }
    if not CLOUDMERSIVE_KEY:
        info["mobile"] = info["valid"]
        info["mobile_verified"] = info["mobile"]
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
        info["mobile"] = info["valid"]
        _line_info_cache[phone] = info
        return info

    LOG.debug("Cloudmersive response for %s: status=%s data=%s", digits, status, data)

    if status != 200:
        LOG.warning(
            "Cloudmersive lookup for %s failed with status %s; falling back to local validation",
            phone,
            status,
        )
        info["mobile"] = info["valid"]
        _line_info_cache[phone] = info
        return info

    if not isinstance(data, dict) or "IsValid" not in data:
        LOG.warning(
            "Cloudmersive response for %s missing IsValid; falling back to local validation",
            phone,
        )
        info["mobile"] = info["valid"]
        _line_info_cache[phone] = info
        return info

    info["valid"] = bool(data.get("IsValid"))
    info["country"] = str(data.get("CountryCode") or "US").upper()
    line_type_label = str(data.get("LineType") or "").strip()
    phone_number_type_label = str(data.get("PhoneNumberType") or "").strip()
    type_label = line_type_label or phone_number_type_label
    normalized_type = type_label.lower()
    normalized_phone_type = phone_number_type_label.lower()
    ambiguous_mobile = normalized_type in {"fixedlineormobile", "fixed line or mobile"} or normalized_phone_type in {
        "fixedlineormobile",
        "fixed line or mobile",
    }
    explicit_mobile = (
        (_is_explicit_mobile(line_type_label) or _is_explicit_mobile(phone_number_type_label))
        and not ambiguous_mobile
    )
    info["type"] = type_label or phone_number_type_label or ("Unknown" if data else "")
    if not info["valid"]:
        _line_info_cache[phone] = info
        _line_type_cache[phone] = False
        _line_type_verified[phone] = False
        return info

    is_mobile = bool(data.get("IsMobile")) or explicit_mobile
    info["mobile_verified"] = explicit_mobile
    info["mobile"] = bool(is_mobile)
    info["ambiguous_mobile"] = ambiguous_mobile

    _line_info_cache[phone] = info
    _line_type_cache[phone] = bool(is_mobile)
    _line_type_verified[phone] = bool(explicit_mobile)
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
_APIFY_DECISION_LOCK = threading.Lock()


def apify_hour_key(slot: datetime) -> str:
    slot = slot.astimezone(SCHEDULER_TZ)
    return slot.strftime("%Y-%m-%d-%H")


def apify_work_hours_status(run_time: datetime) -> Tuple[int, bool]:
    local_dt = run_time.astimezone(SCHEDULER_TZ)
    local_hour = local_dt.hour
    within_work_hours = WORK_START <= local_hour < WORK_END
    if not SCHEDULER_INCLUDE_WEEKENDS and _is_weekend(local_dt):
        within_work_hours = False
    return local_hour, within_work_hours


def apify_acquire_decision_slot(slot: datetime) -> bool:
    hour_key = apify_hour_key(slot)
    with _APIFY_DECISION_LOCK:
        try:
            lock_file = APIFY_DECISION_LOCK_PATH.with_name(
                f"{APIFY_DECISION_LOCK_PATH.name}.{hour_key}"
            )
            lock_file.parent.mkdir(parents=True, exist_ok=True)
        except Exception:
            LOG.debug("Unable to prepare Apify decision guard path", exc_info=True)
            return False
        try:
            fd = os.open(lock_file, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            return False
        except Exception:
            LOG.debug("Unable to acquire Apify decision guard", exc_info=True)
            return False
        try:
            os.close(fd)
        except Exception:
            LOG.debug("Unable to close Apify decision guard fd", exc_info=True)
            return False
        return True


def _hour_floor(dt: datetime) -> datetime:
    dt = dt.astimezone(SCHEDULER_TZ)
    return dt.replace(minute=0, second=0, microsecond=0)


def _next_scheduler_run(now: datetime) -> datetime:
    """Return the next top-of-hour slot in the scheduler timezone."""

    now = now.astimezone(SCHEDULER_TZ)
    base = _hour_floor(now)

    if now < base + timedelta(seconds=1):
        candidate = base
    else:
        candidate = base + timedelta(hours=1)
    if _within_scheduler_hours(candidate):
        return candidate
    return _next_work_start(candidate, include_weekends=SCHEDULER_INCLUDE_WEEKENDS)


def _within_work_hours(slot: datetime) -> bool:
    """Return True when ``slot`` falls inside working hours for follow-ups."""

    slot = slot.astimezone(SCHEDULER_TZ)
    if slot.hour < WORK_START or slot.hour >= WORK_END:
        return False
    if not FOLLOWUP_INCLUDE_WEEKENDS and _is_weekend(slot):
        return False
    return True


def _within_initial_hours(slot: datetime) -> bool:
    """Return True when ``slot`` falls inside working hours for initial SMS."""

    slot = slot.astimezone(SCHEDULER_TZ)
    return WORK_START <= slot.hour < WORK_END


def _within_scheduler_hours(slot: datetime) -> bool:
    """Return True when ``slot`` falls inside hourly scheduler run windows."""

    slot = slot.astimezone(SCHEDULER_TZ)
    if not SCHEDULER_INCLUDE_WEEKENDS and _is_weekend(slot):
        return False
    return WORK_START <= slot.hour < WORK_END


def _next_work_start(slot: datetime, *, include_weekends: bool) -> datetime:
    slot = slot.astimezone(SCHEDULER_TZ)
    candidate = slot

    while True:
        if not include_weekends and _is_weekend(candidate):
            candidate = (candidate + timedelta(days=1)).replace(
                hour=WORK_START, minute=0, second=0, microsecond=0
            )
            continue
        if candidate.hour < WORK_START:
            return candidate.replace(
                hour=WORK_START, minute=0, second=0, microsecond=0
            )
        if candidate.hour >= WORK_END:
            candidate = (candidate + timedelta(days=1)).replace(
                hour=WORK_START, minute=0, second=0, microsecond=0
            )
            continue
        return candidate


def _sleep_until_initial_window(*, row_idx: Optional[int], phone: str) -> None:
    now = datetime.now(tz=SCHEDULER_TZ)
    if _within_initial_hours(now):
        return
    next_start = _next_work_start(now, include_weekends=True)
    sleep_secs = max(0, (next_start - now).total_seconds())
    row_label = f"row {row_idx}" if row_idx else "row <unknown>"
    LOG.info(
        "Initial SMS outside work hours; sleeping %.2fs until %s (now=%s, %s, phone=%s)",
        sleep_secs,
        next_start.isoformat(),
        now.isoformat(),
        row_label,
        phone,
    )
    time.sleep(sleep_secs)


def _run_hourly_cycle(
    run_time: datetime,
    hourly_callbacks: Optional[List[Callable[[datetime], None]]] = None,
    *,
    skip_callbacks: bool = False,
) -> None:
    """Execute one hourly cycle: follow-ups + callbacks."""

    if _within_work_hours(run_time):
        LOG.info("Starting follow-up pass at %s", run_time.isoformat())
        try:
            _follow_up_pass()
        except Exception as exc:
            LOG.exception("Error during follow-up pass: %s", exc)
    else:
        LOG.info(
            "Current hour %s outside work hours (%s–%s); skipping follow-up",
            run_time.hour,
            WORK_START,
            WORK_END,
        )
        next_followup = _next_work_start(
            run_time + timedelta(seconds=1),
            include_weekends=FOLLOWUP_INCLUDE_WEEKENDS,
        )
        LOG.info(
            "Follow-up scheduler idle; sleeping until next work window at %s",
            next_followup.isoformat(),
        )

    if skip_callbacks:
        return

    callbacks = hourly_callbacks or []
    LOG.info("Executing %s hourly callbacks", len(callbacks))
    for cb in callbacks:
        try:
            cb(run_time)
        except Exception as exc:
            LOG.exception(
                "Error during hourly callback %s: %s",
                getattr(cb, "__name__", cb),
                exc,
            )


def run_hourly_scheduler(
    stop_event: Optional[threading.Event] = None,
    hourly_callbacks: Optional[List[Callable[[datetime], None]]] = None,
    *,
    run_immediately: bool = False,
    initial_callbacks: bool = True,
) -> None:
    LOG.info(
        "Hourly scheduler loop starting (thread=%s)",
        threading.current_thread().name,
    )
    LOG.info(
        "Follow-up hours configured: %02d:00-%02d:00 %s (include_weekends=%s)",
        WORK_START,
        WORK_END,
        SCHEDULER_TZ,
        FOLLOWUP_INCLUDE_WEEKENDS,
    )
    LOG.info(
        "Scheduler hours configured: %02d:00-%02d:00 %s (include_weekends=%s)",
        WORK_START,
        WORK_END,
        SCHEDULER_TZ,
        SCHEDULER_INCLUDE_WEEKENDS,
    )
    next_run = _next_scheduler_run(datetime.now(tz=SCHEDULER_TZ))

    LOG.info(
        "Hourly scheduler initialized; first scheduled wake at %s (run_immediately=%s)",
        next_run.isoformat(),
        run_immediately,
    )

    if run_immediately:
        initial_run = _hour_floor(datetime.now(tz=SCHEDULER_TZ))
        if _within_scheduler_hours(initial_run):
            _run_hourly_cycle(
                initial_run,
                hourly_callbacks,
                skip_callbacks=not initial_callbacks,
            )
            next_run = _next_scheduler_run(initial_run + timedelta(seconds=1))
        else:
            LOG.info(
                "Skipping immediate run outside work hours; next scheduled run at %s",
                next_run.isoformat(),
            )

    while True:
        try:
            if stop_event and stop_event.is_set():
                LOG.info("Hourly scheduler stop requested; exiting loop")
                break

            now = datetime.now(tz=SCHEDULER_TZ)
            sleep_secs = max(0, (next_run - now).total_seconds())
            LOG.info(
                "Scheduler sleeping %.2fs until next wake at %s (now=%s)",
                sleep_secs,
                next_run.isoformat(),
                now.isoformat(),
            )
            if stop_event and stop_event.wait(timeout=sleep_secs):
                LOG.info("Hourly scheduler stop requested; exiting loop")
                break
            elif not stop_event:
                time.sleep(sleep_secs)

            run_time = next_run
            next_run = _next_scheduler_run(run_time + timedelta(seconds=1))
            LOG.info(
                "Scheduler wake at %s; next wake at %s",
                run_time.isoformat(),
                next_run.isoformat(),
            )
            _run_hourly_cycle(run_time, hourly_callbacks)
        except Exception:
            LOG.exception("Hourly scheduler crashed; continuing")
            if stop_event and stop_event.is_set():
                break
            time.sleep(30)
            next_run = _next_scheduler_run(datetime.now(tz=SCHEDULER_TZ))
            continue
    LOG.info("Hourly scheduler loop terminated (thread=%s)", threading.current_thread().name)

# ───────────────────── follow‑up pass (UPDATED) ─────────────────────
def _follow_up_pass():
    now = datetime.now(tz=TZ)
    resp = sheets_service.spreadsheets().values().get(
        spreadsheetId=GSHEET_ID,
        range=f"{GSHEET_TAB}!A:AB",
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
        except Exception:
            continue
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=TZ)
        else:
            ts = ts.astimezone(TZ)

        elapsed_hours = (now - ts).total_seconds() / 3600.0
        if elapsed_hours < FU_HOURS:
            LOG.debug(
                "FU‑skip row %s – %.2f hours elapsed", sheet_row, elapsed_hours
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

def process_rows(rows: List[Dict[str, Any]], *, skip_dedupe: bool = False):
    if not skip_dedupe:
        rows = dedupe_rows_by_zpid(rows, LOG)
        if not rows:
            LOG.info("No fresh rows after de-duplication; skipping enrichment run")
            return
    load_seen_contacts()
    for r in rows:
        zpid = str(r.get("zpid", ""))
        listing_text = _listing_text_from_payload(r)
        short_sale_text = _short_sale_text_from_payload(listing_text)
        exclusion_reason = _short_sale_exclusion_reason(short_sale_text)
        if exclusion_reason:
            LOG.info(
                "SHORT_SALE_EXCLUDE zpid=%s reason=%s",
                zpid,
                exclusion_reason,
            )
            continue
        if not listing_text:
            LOG.debug(
                "SHORT_SALE_TEXT_EMPTY zpid=%s street=%s",
                zpid,
                r.get("street"),
            )
        description_match = is_short_sale(listing_text or "")
        if not description_match:
            LOG.debug(
                "SKIP non-short-sale %s (%s)",
                r.get("street"),
                r.get("zpid"),
            )
            continue
        LOG.info(
            "SHORT_SALE_MATCH zpid=%s source=%s",
            zpid,
            "description",
        )
        street = (r.get("street") or r.get("address") or "").strip()
        if street == "(Undisclosed Address)":
            LOG.debug("SKIP undisclosed address zpid %s", r.get("zpid"))
            continue
        if zpid and not is_active_listing(r):
            LOG.info("Skip stale/off-market zpid %s", zpid)
            continue
        name = (r.get("agentName") or "").strip()
        if not name or TEAM_RE.search(name):
            LOG.debug("SKIP missing agent name for %s (%s)", r.get("street"), r.get("zpid"))
            continue
        state = r.get("state", "")
        normalized_agent = _normalize_agent_name(name)
        if normalized_agent and normalized_agent in seen_agents:
            LOG.info("SKIP already-contacted agent %s (%s)", name, r.get("zpid"))
            continue
        selected_phone = ""
        selected_email = ""
        if selected_phone and phone_exists(selected_phone):
            LOG.info(
                "SKIP already-contacted phone %s for agent %s (%s)",
                selected_phone,
                name,
                r.get("zpid"),
            )
            continue

        first, *last = name.split()
        now_iso = datetime.now(tz=TZ).isoformat()
        pulled_at = str(r.get("pulled_at") or now_iso)
        row_vals = [""] * MIN_COLS
        row_vals[COL_FIRST]   = first
        row_vals[COL_LAST]    = " ".join(last)
        row_vals[COL_PHONE]   = selected_phone
        row_vals[COL_EMAIL]   = selected_email
        row_vals[COL_PHONE_CONF] = "low" if selected_phone else ""
        row_vals[COL_EMAIL_CONF] = ""
        row_vals[COL_CONTACT_REASON] = ""
        row_vals[COL_STREET]  = r.get("street", "")
        row_vals[COL_CITY]    = r.get("city", "")
        row_vals[COL_STATE]   = state
        row_vals[COL_INIT_TS] = pulled_at
        row_vals[COL_ZPID]    = zpid
        row_idx = append_row(row_vals)
        LOG.info(
            "SHEET_APPEND_OK zpid=%s agent=%s phone=%s email=%s",
            zpid,
            name,
            selected_phone or "",
            selected_email or "",
        )
        if normalized_agent:
            seen_agents.add(normalized_agent)
        if selected_phone and not phone_exists(selected_phone):
            seen_phones.add(_normalize_phone_for_dedupe(selected_phone))
            _sleep_until_initial_window(row_idx=row_idx, phone=selected_phone)
            send_sms(selected_phone, first, r.get("street", ""), row_idx)

        phone_info = {"number": "", "confidence": "", "reason": ""}
        email_info = {"email": "", "confidence": "", "reason": ""}
        try:
            phone_info = lookup_phone(name, state, r)
        except Exception as exc:
            LOG.error("ENRICHMENT_FAILED zpid=%s agent=%s err=%s", zpid, name, exc)
        try:
            email_info = lookup_email(name, state, r)
        except Exception as exc:
            LOG.error("ENRICHMENT_FAILED zpid=%s agent=%s err=%s", zpid, name, exc)

        enriched_phone = phone_info.get("number", "") if phone_info else ""
        enriched_email = email_info.get("email", "") if email_info else ""
        data_updates: List[Dict[str, Any]] = []
        if enriched_phone or enriched_email:
            update_phone = enriched_phone and enriched_phone != selected_phone
            update_email = enriched_email and enriched_email != selected_email
            if update_phone or update_email:
                if update_phone:
                    data_updates.append({"range": f"{GSHEET_TAB}!C{row_idx}", "values": [[enriched_phone]]})
                    data_updates.append({"range": f"{GSHEET_TAB}!Y{row_idx}", "values": [[phone_info.get("confidence", "")]]})
                if update_email:
                    data_updates.append({"range": f"{GSHEET_TAB}!D{row_idx}", "values": [[enriched_email]]})
                    data_updates.append({"range": f"{GSHEET_TAB}!AA{row_idx}", "values": [[email_info.get("confidence", "")]]})
                reason = ""
                phone_reason = phone_info.get("reason", "")
                email_reason = email_info.get("reason", "")
                if "withheld_low_conf_mix" in {phone_reason, email_reason}:
                    reason = "withheld_low_conf_mix"
                elif phone_reason == "no_personal_mobile":
                    reason = "no_personal_mobile"
                elif email_reason == "no_personal_email":
                    reason = "no_personal_email"
                if reason:
                    data_updates.append({"range": f"{GSHEET_TAB}!Z{row_idx}", "values": [[reason]]})
            if data_updates:
                try:
                    sheets_service.spreadsheets().values().batchUpdate(
                        spreadsheetId=GSHEET_ID,
                        body={"valueInputOption": "RAW", "data": data_updates},
                    ).execute()
                except Exception as exc:
                    LOG.warning("Sheet update failed for row %s: %s", row_idx, exc)
            if enriched_phone and not phone_exists(enriched_phone):
                seen_phones.add(_normalize_phone_for_dedupe(enriched_phone))
                _sleep_until_initial_window(row_idx=row_idx, phone=enriched_phone)
                send_sms(enriched_phone, first, r.get("street", ""), row_idx)

# ───────────────────── main entry point & scheduler ─────────────────────
if __name__ == "__main__":
    log_headless_status()
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
        enable_scheduler = (
            os.getenv("ENABLE_STANDALONE_SCHEDULER", "false").lower() == "true"
        )
        if not enable_scheduler:
            LOG.info(
                "ENABLE_STANDALONE_SCHEDULER=false; skipping standalone scheduler startup"
            )
            sys.exit(0)

        LOG.info("No JSON payload detected; entering hourly scheduler mode.")
        run_hourly_scheduler(run_immediately=True)
