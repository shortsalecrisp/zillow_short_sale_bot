from __future__ import annotations

import concurrent.futures
import html
import json
import logging
import os
import re
import sys
import threading
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from urllib.parse import urlparse

import time, random
from typing import Any, Dict, List, Optional, Set, Tuple

import gspread
import pytz
import requests
from requests.adapters import HTTPAdapter, Retry
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build as gapi_build
from sms_providers import get_sender

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

def _http_get(
    url: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    extra_headers: Optional[Dict[str, str]] = None,
    timeout: int | float = _DEFAULT_TIMEOUT,
) -> requests.Response:
    hdrs = {}
    if headers:
        hdrs.update(headers)
    if extra_headers:
        hdrs.update(extra_headers)

    attempts = 0
    while True:
        attempts += 1
        resp = _session.get(url, params=params, headers=hdrs or None, timeout=timeout)
        if resp.status_code == 429 and attempts <= 5:
            ra = resp.headers.get("Retry-After")
            sleep_s = int(ra) if ra and ra.isdigit() else min(30, 2 ** attempts) + random.uniform(0, 0.5)
            time.sleep(sleep_s)
            continue
        try:
            resp.raise_for_status()
        except requests.HTTPError:
            if attempts <= 1 and resp.status_code in (500, 502, 503, 504):
                time.sleep(0.5 + random.uniform(0, 0.25))
                continue
            raise
        return resp

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
# GOOGLE_API_KEY / GOOGLE_CX if provided.
CS_API_KEY     = os.getenv("CS_API_KEY") or os.environ["GOOGLE_API_KEY"]
CS_CX          = os.getenv("CS_CX") or os.environ["GOOGLE_CX"]
GSHEET_ID      = os.environ["GSHEET_ID"]
SC_JSON        = json.loads(os.environ["GCP_SERVICE_ACCOUNT_JSON"])
SCOPES         = ["https://www.googleapis.com/auth/spreadsheets"]

RAPID_KEY      = os.getenv("RAPID_KEY", "").strip()
RAPID_HOST     = os.getenv("RAPID_HOST", "zillow-com1.p.rapidapi.com").strip()
GOOD_STATUS    = {"FOR_SALE", "ACTIVE", "COMING_SOON", "PENDING", "NEW_CONSTRUCTION"}

TZ             = pytz.timezone(os.getenv("BOT_TIMEZONE", "US/Eastern"))
FU_HOURS       = float(os.getenv("FOLLOW_UP_HOURS", "6"))
FU_LOOKBACK_ROWS = int(os.getenv("FU_LOOKBACK_ROWS", "50"))
WORK_START     = int(os.getenv("WORK_START_HOUR", "8"))   # inclusive (8 am)
WORK_END       = int(os.getenv("WORK_END_HOUR", "21"))    # exclusive (pauses at 9 pm)

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

CONTACT_EMAIL_MIN_SCORE = float(os.getenv("CONTACT_EMAIL_MIN_SCORE", "0.75"))
CONTACT_PHONE_MIN_SCORE = float(os.getenv("CONTACT_PHONE_MIN_SCORE", "2.25"))
CONTACT_PHONE_LOW_CONF  = float(os.getenv("CONTACT_PHONE_LOW_CONF", "1.5"))

BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,image/apng,*/*;q=0.8"
    ),
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}

_generic_domains_env = os.getenv("CONTACT_GENERIC_EMAIL_DOMAINS", "homelight.com,example.org")
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
MIN_COLS        = 26

MAX_ZILLOW_403      = 3
MAX_RATE_429        = 3
BACKOFF_FACTOR      = 1.7
MAX_BACKOFF_SECONDS = 12
GOOGLE_CONCURRENCY  = 2
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
PHONE_OFFICE_TERMS = {"office", "front desk", "main", "team", "switchboard"}

# ───────────────────── Google / Sheets setup ─────────────────────
creds           = Credentials.from_service_account_info(SC_JSON, scopes=SCOPES)
sheets_service  = gapi_build("sheets", "v4", credentials=creds, cache_discovery=False)
gc              = gspread.authorize(creds)
ws              = gc.open_by_key(GSHEET_ID).sheet1

try:
    _preloaded = ws.col_values(COL_PHONE + 1)
except Exception:
    _preloaded = []
seen_phones: Set[str] = set(_preloaded)

SCRAPE_SITES:  List[str] = []
DYNAMIC_SITES: Set[str]  = set()
BAN_KEYWORDS = {
    "zillow.com", "realtor.com", "redfin.com", "homes.com",
    "linkedin.com", "twitter.com", "instagram.com", "pinterest.com", "facebook.com", "legacy.com",
    "obituary", "obituaries", "funeral",
    ".gov", ".edu", ".mil",
}

_executor = concurrent.futures.ThreadPoolExecutor(max_workers=GOOGLE_CONCURRENCY)
def pmap(fn, iterable): return list(_executor.map(fn, iterable))

# ───────────────────── phone / email formatting helpers ─────────────────────
def _is_bad_area(area: str) -> bool:
    return area in BAD_AREA or area.startswith("1")

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

def _names_match(a: str, b: str) -> bool:
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

_blocked_until: Dict[str, float] = {}
def _mark_block(dom: str) -> None:
    _blocked_until[dom] = time.time() + 600

def _try_textise(dom: str, url: str) -> str:
    try:
        r = _http_get(
            f"https://r.jina.ai/http://{urlparse(url).netloc}{urlparse(url).path}",
            timeout=10,
            headers=BROWSER_HEADERS,
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

def _is_banned(dom: str) -> bool:
    return any(bad in dom for bad in BAN_KEYWORDS)

def _should_fetch(url: str, strict: bool = True) -> bool:
    dom = _domain(url)
    if dom in _blocked_until and _blocked_until[dom] > time.time():
        return False
    return not (_is_banned(dom) and strict)

def fetch_simple(u: str, strict: bool = True):
    if not _should_fetch(u, strict):
        return None
    dom = _domain(u)
    try:
        try:
            r = _http_get(u, timeout=10, headers=BROWSER_HEADERS)
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
    bare = re.sub(r"^https?://", "", u)
    variants = [
        u,
        f"https://r.jina.ai/http://{bare}",
        f"https://r.jina.ai/http://screenshot/{bare}",
    ]
    z403 = ratelimit = 0
    backoff = 1.0
    for url in variants:
        for _ in range(3):
            try:
                try:
                    r = _http_get(url, timeout=10, headers=BROWSER_HEADERS)
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
            elif r.status_code == 429:
                ratelimit += 1
                METRICS["fetch_429"] += 1
                if ratelimit >= MAX_RATE_429:
                    _mark_block(dom)
                    return None
            elif r.status_code in (403, 451):
                _mark_block(dom)
                txt = _try_textise(dom, u)
                if txt:
                    return txt
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
_CONTACT_FETCH_BACKOFFS = (0.0, 0.7, 2.0)


def _mirror_url(url: str) -> str:
    parsed = urlparse(url)
    if not parsed.scheme or not parsed.netloc:
        return ""
    base = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
    mirror = f"https://r.jina.ai/{base}"
    if parsed.query:
        mirror = f"{mirror}?{parsed.query}"
    return mirror


def fetch_contact_page(url: str) -> Tuple[str, bool]:
    if not _should_fetch(url, strict=False):
        return "", False
    dom = _domain(url)
    blocked = False
    tries = len(_CONTACT_FETCH_BACKOFFS)
    for attempt, delay in enumerate(_CONTACT_FETCH_BACKOFFS, start=1):
        if delay:
            time.sleep(delay)
        try:
            try:
                resp = _http_get(url, timeout=10, headers=BROWSER_HEADERS)
            except requests.HTTPError as exc:
                resp = exc.response
                if resp is None:
                    raise
        except Exception as exc:
            LOG.debug("fetch_contact_page error %s on %s", exc, url)
            break
        status = resp.status_code
        if status == 200:
            return resp.text, False
        if status in (403, 429):
            blocked = True
            _mark_block(dom)
            LOG.warning("BLOCK %s -> retry %s/%s", status, attempt, tries)
            continue
        if status in (301, 302) and resp.headers.get("Location"):
            url = resp.headers["Location"]
            continue
        if status in (403, 451):
            blocked = True
            _mark_block(dom)
            LOG.warning("BLOCK %s -> retry %s/%s", status, attempt, tries)
            continue
        break

    if blocked:
        mirror = _mirror_url(url)
        if mirror:
            try:
                mirror_resp = _http_get(mirror, timeout=10, headers=BROWSER_HEADERS)
                if mirror_resp.status_code == 200 and mirror_resp.text.strip():
                    LOG.info("MIRROR FALLBACK used")
                    return mirror_resp.text, True
            except Exception as exc:
                LOG.debug("mirror fetch failed %s on %s", exc, mirror)
    return "", False

# ───────────────────── Google CSE helpers ─────────────────────
_cse_cache: Dict[str, List[Dict[str, Any]]] = {}
_last_cse_ts = 0.0
_cse_lock = threading.Lock()

def google_items(q: str, tries: int = 3) -> List[Dict[str, Any]]:
    global _last_cse_ts
    with _cse_lock:
        if q in _cse_cache:
            return _cse_cache[q]
        delta = time.time() - _last_cse_ts
        if delta < 1.5:
            time.sleep(1.5 - delta)
        _last_cse_ts = time.time()
    backoff = 1.0
    for _ in range(tries):
        try:
            j = _http_get(
                "https://www.googleapis.com/customsearch/v1",
                params={"key": CS_API_KEY, "cx": CS_CX, "q": q, "num": 10},
                timeout=10,
            ).json()
            items = j.get("items", [])
            with _cse_lock:
                _cse_cache[q] = items
            return items
        except Exception:
            time.sleep(min(backoff, MAX_BACKOFF_SECONDS))
            backoff *= BACKOFF_FACTOR
    with _cse_lock:
        _cse_cache[q] = []
    return []

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

    for a in soup.select('a[href^="mailto:"]'):
        mail_val = a.get("href", "").split("mailto:")[-1]
        cleaned = clean_email(mail_val)
        if cleaned and ok_email(cleaned):
            mails.append(cleaned)
            info["mailto"].append({
                "email": cleaned,
                "context": _context_for(a).lower(),
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
        if first_name and first_name.lower() not in snippet:
            continue
        if last_name and last_name.lower() not in snippet:
            continue
        lab_match = LABEL_RE.search(snippet)
        lab = lab_match.group().lower() if lab_match else ""
        w = LABEL_TABLE.get(lab, 0)
        if w < 1:
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

def build_q_phone(name: str, state: str) -> List[str]:
    base = f'"{name}" {state}'
    return [
        f"{base} realtor phone",
        f"{base} real estate cell",
        f"{base} mobile",
    ]

def build_q_email(
    name: str, state: str, brokerage: str = "", domain_hint: str = "", mls_id: str = ""
) -> List[str]:
    queries: List[str] = []

    def _add(q: str) -> None:
        if q and q not in queries:
            queries.append(q)

    base = f'"{name}" {state}'.strip()
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

    if domain_hint:
        _add(f'site:{domain_hint} "{name}" email')
        _add(f'"{name}" "@{domain_hint}"')

    if mls_id and parts:
        _add(f'"{mls_id}" "{parts[-1]}" email')

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

cache_p: Dict[str, Dict[str, Any]] = {}
cache_e: Dict[str, Dict[str, Any]] = {}
domain_patterns: Dict[str, str] = {}


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

def _split_portals(urls):
    portals, non = [], []
    for u in urls:
        (portals if any(d in u for d in SCRAPE_SITES) else non).append(u)
    return non, portals


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
}


def _is_generic_email(email: str) -> bool:
    local, domain = email.split("@", 1)
    local_key = re.sub(r"[^a-z0-9]", "", local.lower())
    domain_l = domain.lower()
    if any(local_key.startswith(prefix) for prefix in GENERIC_EMAIL_PREFIXES if prefix):
        LOG.info("EMAIL REJECT generic: %s", email)
        return True
    for gd in GENERIC_EMAIL_DOMAINS:
        if domain_l == gd or domain_l.endswith(f".{gd}"):
            LOG.info("EMAIL REJECT generic: %s", email)
            return True
    return False

def _looks_direct(phone: str, agent: str, state: str, tries: int = 2) -> bool:
    if not phone:
        return False
    last = agent.split()[-1].lower()
    digits = re.sub(r"\D", "", phone)
    queries = [f'"{phone}" {state}', f'"{phone}" "{agent.split()[0]}"']
    for q in queries:
        for it in google_items(q, tries=tries):
            link = it.get("link", "")
            page = fetch_simple(link, strict=False)
            if not page:
                continue
            low_digits = re.sub(r"\D", "", page)
            if digits not in low_digits:
                continue
            pos = low_digits.find(digits)
            if pos == -1:
                continue
            if last in page.lower()[max(0, pos - 200): pos + 200]:
                return True
    return False

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
    "cse_contact": 1.4,
}


def lookup_phone(agent: str, state: str, row_payload: Dict[str, Any]) -> Dict[str, Any]:
    key = f"{agent}|{state}"
    if key in cache_p:
        return cache_p[key]

    candidates: Dict[str, Dict[str, Any]] = {}
    had_candidates = False

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
    ) -> None:
        nonlocal had_candidates
        formatted = fmt_phone(str(phone))
        if not (formatted and valid_phone(formatted)):
            return
        had_candidates = True
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
            },
        )
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
        if name_match and not info["name_match"]:
            info["score"] += 0.6
            info["name_match"] = True
        info["sources"].add(source)
        if context:
            ctx = context.lower()
            info["contexts"].append(ctx)
            if any(term in ctx for term in PHONE_OFFICE_TERMS) and not info["office_demoted"]:
                info["score"] -= 1.0
                info["office_demoted"] = True
                LOG.debug("PHONE DEMOTE office: %s", formatted)
        if office_flag and not info["office_demoted"]:
            info["score"] -= 1.0
            info["office_demoted"] = True
            LOG.debug("PHONE DEMOTE office: %s", formatted)
        if page_title:
            info["page_titles"].add(page_title.lower())
        if url:
            info["urls"].add(url.lower())
            DYNAMIC_SITES.add(_domain(url))
        if meta_name:
            info["meta_names"].add(meta_name.lower())

    for blk in (row_payload.get("contact_recipients") or []):
        ctx = " ".join(str(blk.get(k, "")) for k in ("title", "label", "role") if blk.get(k))
        meta_name = blk.get("display_name", "")
        match = _names_match(agent, meta_name)
        for p in _phones_from_block(blk):
            _register(p, "payload_contact", context=ctx, meta_name=meta_name, name_match=match)

    zpid = str(row_payload.get("zpid", ""))
    rapid = rapid_property(zpid) if zpid else {}
    if rapid:
        lb = rapid.get("listed_by") or {}
        lb_name = lb.get("display_name", "")
        match = _names_match(agent, lb_name)
        for p in _phones_from_block(lb):
            _register(p, "rapid_listed_by", meta_name=lb_name, name_match=match)
        for blk in rapid.get("contact_recipients", []) or []:
            blk_name = blk.get("display_name", "")
            match = _names_match(agent, blk_name)
            ctx = " ".join(str(blk.get(k, "")) for k in ("title", "label", "role") if blk.get(k))
            for p in _phones_from_block(blk):
                _register(p, "rapid_contact", context=ctx, meta_name=blk_name, name_match=match)

    queries = build_q_phone(agent, state)
    for items in pmap(google_items, queries):
        for it in items:
            tel = (it.get("pagemap", {}).get("contactpoint", [{}])[0].get("telephone") or "")
            if tel:
                _register(tel, "cse_contact", url=it.get("link", ""))

    urls = [
        it.get("link", "")
        for items in pmap(google_items, queries)
        for it in items
    ][:20]
    non_portal, portal = _split_portals(urls)
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
        return True

    def _process_page(url: str, page: str) -> None:
        if not page or not _page_has_name(page):
            return
        ph, _, meta, info = extract_struct(page)
        page_title = info.get("title", "")
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
                _register(
                    num,
                    source,
                    url=url,
                    page_title=page_title,
                    meta_name=meta_name,
                    name_match=match,
                )
        for anchor in info.get("tel", []):
            _register(
                anchor.get("phone", ""),
                "agent_card_dom",
                url=url,
                page_title=page_title,
                context=anchor.get("context", ""),
            )
        low = html.unescape(page.lower())
        for num, details in proximity_scan(low, first_name, last_name).items():
            _register(
                num,
                "agent_card_dom",
                url=url,
                page_title=page_title,
                context=" ".join(details.get("snippets", [])),
                bonus=min(1.0, details.get("score", 0.0) / 4.0),
                office_flag=details.get("office", False),
            )

    for url in non_portal:
        page, _ = fetch_contact_page(url)
        if not page:
            continue
        _process_page(url, page)
        if candidates:
            break

    if not candidates:
        for url in portal:
            page, _ = fetch_contact_page(url)
            if not page:
                continue
            _process_page(url, page)
            if candidates:
                break

    tokens = _agent_tokens(agent)
    best_number = ""
    best_score = float("-inf")
    best_source = ""
    best_is_mobile = False
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
        mobile = is_mobile_number(number)
        info["is_mobile"] = mobile
        if not mobile:
            info["score"] -= 1.0
        info["final_score"] = info["score"]
        source = info.get("best_source") or (next(iter(info["sources"])) if info["sources"] else "")
        if info["score"] > best_score:
            best_score = info["score"]
            best_number = number
            best_source = source
            best_is_mobile = mobile

    result = {
        "number": "",
        "confidence": "",
        "score": best_score if best_score != float("-inf") else 0.0,
        "source": best_source,
        "reason": "",
    }

    if best_number:
        override_low_conf = False
        adjusted_score = best_score
        if best_score >= CONTACT_PHONE_MIN_SCORE:
            confidence = "high"
            LOG.debug("PHONE WIN %s via %s score=%.2f", best_number, best_source or "unknown", best_score)
        elif best_score >= CONTACT_PHONE_LOW_CONF:
            confidence = "low"
            LOG.info("PHONE WIN (low-confidence): %s %.2f %s", best_number, best_score, best_source or "unknown")
        elif best_is_mobile:
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
        if confidence:
            best_score = adjusted_score
            result.update({
                "number": best_number,
                "confidence": confidence,
                "score": adjusted_score,
                "source": best_source,
            })
            cache_p[key] = result
            return result

    if had_candidates or best_number:
        reason = "withheld_low_conf_mix"
    else:
        reason = "no_personal_mobile"
    result.update({
        "number": "",
        "confidence": "",
        "reason": reason,
        "score": best_score if best_score != float("-inf") else 0.0,
        "source": best_source,
    })
    cache_p[key] = result
    METRICS["phone_no_verified_mobile"] += 1
    LOG.warning(
        "PHONE DROP no verified mobile for %s %s zpid=%s reason=%s",
        agent,
        state,
        zpid or "",
        reason,
    )
    return result

def lookup_email(agent: str, state: str, row_payload: Dict[str, Any]) -> Dict[str, Any]:
    key = f"{agent}|{state}"
    if key in cache_e:
        return cache_e[key]

    brokerage = domain_hint = mls_id = ""
    candidates: Dict[str, Dict[str, Any]] = {}
    generic_seen: Set[str] = set()
    had_candidates = False

    tokens = _agent_tokens(agent)
    IDENTITY_SOURCES = {
        "mailto",
        "dom",
        "jsonld_other",
        "jsonld_person",
        "cse_contact",
        "pattern",
    }

    def _register(email: str, source: str, *, url: str = "", page_title: str = "", context: str = "", meta_name: str = "") -> None:
        nonlocal had_candidates
        cleaned = clean_email(email)
        if not cleaned or not ok_email(cleaned):
            return
        low = cleaned.lower()
        if low in generic_seen:
            return
        if _is_generic_email(cleaned):
            generic_seen.add(low)
            return

        if source in IDENTITY_SOURCES:
            identity_ok = False
            if _email_matches_name(agent, cleaned):
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
            },
        )
        base = EMAIL_SOURCE_BASE.get(source, EMAIL_SOURCE_BASE["dom"])
        if source and source not in info["applied"]:
            info["score"] += base
            info["applied"].add(source)
            if base >= info["best_base"]:
                info["best_base"] = base
                info["best_source"] = source
        info["sources"].add(source)
        if context:
            info["contexts"].append(context.lower())
        if page_title:
            info["page_titles"].add(page_title.lower())
        if url:
            info["urls"].add(url.lower())
            DYNAMIC_SITES.add(_domain(url))
        if meta_name:
            info["meta_names"].add(meta_name.lower())

    for blk in (row_payload.get("contact_recipients") or []):
        ctx = " ".join(str(blk.get(k, "")) for k in ("title", "label", "role") if blk.get(k))
        for em in _emails_from_block(blk):
            if _email_matches_name(agent, em):
                _register(em, "payload_contact", context=ctx, meta_name=blk.get("display_name", ""))

    zpid = str(row_payload.get("zpid", ""))
    if zpid:
        rapid = rapid_property(zpid)
        if rapid:
            lb = rapid.get("listed_by") or {}
            brokerage = lb.get("brokerageName", "")
            mls_id = lb.get("listingAgentMlsId", "")
            lb_display = lb.get("display_name", "")
            lb_ctx = " ".join(str(lb.get(k, "")) for k in ("title", "label", "role") if lb.get(k))
            for em in _emails_from_block(lb):
                if lb_display and not _names_match(agent, lb_display):
                    continue
                _register(
                    em,
                    "rapid_listed_by",
                    meta_name=lb_display,
                    context=lb_ctx,
                )
            for blk in rapid.get("contact_recipients", []) or []:
                ctx = " ".join(str(blk.get(k, "")) for k in ("title", "label", "role") if blk.get(k))
                for em in _emails_from_block(blk):
                    display_name = blk.get("display_name", "")
                    if display_name and not _names_match(agent, display_name):
                        continue
                    _register(
                        em,
                        "rapid_contact",
                        context=ctx,
                        meta_name=display_name,
                    )

    queries = build_q_email(agent, state, brokerage, domain_hint, mls_id)
    for items in pmap(google_items, queries):
        for it in items:
            mail = (it.get("pagemap", {}).get("contactpoint", [{}])[0].get("email") or "")
            if mail:
                _register(mail, "cse_contact", url=it.get("link", ""))

    urls = [
        it.get("link", "")
        for items in pmap(google_items, queries)
        for it in items
    ][:20]
    non_portal, portal = _split_portals(urls)

    first_variants, last_token = _first_last_tokens(agent)

    def _page_has_name(page_text: str) -> bool:
        if not (last_token or first_variants):
            return False
        low = page_text.lower()
        if last_token and not _token_in_text(low, last_token):
            return False
        if first_variants and not any(_token_in_text(low, tok) for tok in first_variants):
            return False
        return True

    def _process_page(url: str, page: str) -> None:
        if not page or not _page_has_name(page):
            return
        _, ems, meta, info = extract_struct(page)
        page_title = info.get("title", "")
        seen = set()
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
            )
        for mail in ems:
            if mail in seen:
                continue
            seen.add(mail)
            _register(mail, "dom", url=url, page_title=page_title)
        lower_page = page.lower()
        for m in EMAIL_RE.finditer(lower_page):
            raw = page[m.start(): m.end()]
            cleaned = clean_email(raw)
            if cleaned in seen:
                continue
            snippet = lower_page[max(0, m.start() - 120): m.end() + 120]
            _register(cleaned, "dom", url=url, page_title=page_title, context=" ".join(snippet.split()))

    for url in non_portal:
        page, _ = fetch_contact_page(url)
        if not page:
            continue
        _process_page(url, page)
        if candidates:
            break

    if not candidates:
        for url in portal:
            page, _ = fetch_contact_page(url)
            if not page:
                continue
            _process_page(url, page)
            if candidates:
                break

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
    result = {
        "email": best_email,
        "score": best_score,
        "source": best_source,
        "reason": reason,
    }

    if best_email and best_score >= CONTACT_EMAIL_MIN_SCORE:
        cache_e[key] = result
        LOG.debug("EMAIL WIN %s via %s score=%.2f", best_email, best_source or "unknown", best_score)
        return result

    if not had_candidates:
        reason = "no_personal_email"
    else:
        reason = "withheld_low_conf_mix"
    result.update({"email": "", "reason": reason, "score": best_score, "source": best_source})
    cache_e[key] = result
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
        {"range": f"Sheet1!H{row_idx}", "values": [["x"]]},
        {"range": f"Sheet1!W{row_idx}", "values": [[ts]]},
        {"range": f"Sheet1!L{row_idx}", "values": [[msg_id]]},
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
        {"range": f"Sheet1!I{row_idx}", "values": [["x"]]},
        {"range": f"Sheet1!X{row_idx}", "values": [[ts]]},
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
        {"range": f"Sheet1!I{row_idx}", "values": [["x"]]},
        {"range": f"Sheet1!K{row_idx}", "values": [[ts]]},
    ]
    try:
        sheets_service.spreadsheets().values().batchUpdate(
            spreadsheetId=GSHEET_ID,
            body={"valueInputOption": "RAW", "data": data},
        ).execute()
        LOG.info("Marked row %s I:x K:ts – reply detected", row_idx)
    except Exception as e:
        LOG.error("GSheet mark_reply error %s", e)

def append_row(vals) -> int:
    resp = sheets_service.spreadsheets().values().append(
        spreadsheetId=GSHEET_ID,
        range="Sheet1!A1",
        valueInputOption="RAW",
        body={"values": [vals]},
    ).execute()
    row_idx = int(resp["updates"]["updatedRange"].split("!")[1].split(":")[0][1:])
    LOG.info("Row appended to sheet (row %s)", row_idx)
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


def _is_explicit_mobile(value: Any) -> bool:
    if value is None:
        return False
    return str(value).strip().lower() == "mobile"


def is_mobile_number(phone: str) -> bool:
    """Return True if *phone* is classified as a mobile line via Cloudmersive."""
    if not phone:
        return False
    if phone in _line_type_cache:
        return _line_type_cache[phone]
    if not CLOUDMERSIVE_KEY:
        return True
    digits = _digits_only(phone)
    try:
        resp = requests.post(
            "https://api.cloudmersive.com/validate/phonenumber/basic",
            json={"PhoneNumber": digits, "DefaultCountryCode": "US"},
            headers={"Apikey": CLOUDMERSIVE_KEY},
            timeout=6,
        )
        data = resp.json()
    except Exception as exc:
        LOG.warning("Cloudmersive lookup failed for %s (%s)", phone, exc)
        _line_type_cache[phone] = False
        return False
    LOG.debug(
        "Cloudmersive response for %s: status=%s data=%s",
        digits,
        resp.status_code,
        data,
    )
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
            # Cloudmersive returns "FixedLineOrMobile" when it cannot definitively
            # classify the line type. Treat it as usable so we do not drop real
            # mobile numbers that happen to be marked ambiguous.
            is_mobile = True
    _line_type_cache[phone] = is_mobile
    LOG.debug("Cloudmersive classified %s as mobile=%s", digits, is_mobile)
    return is_mobile


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

# ───────────────────── follow‑up pass (UPDATED) ─────────────────────
def _follow_up_pass():
    resp = sheets_service.spreadsheets().values().get(
        spreadsheetId=GSHEET_ID,
        range="Sheet1!A:Z",
        majorDimension="ROWS",
        valueRenderOption="FORMATTED_VALUE",
    ).execute()
    all_rows = resp.get("values", [])
    if len(all_rows) <= 1:
        return

    now = datetime.now(tz=TZ)
    if _is_weekend(now):
        LOG.debug("Weekend – skipping follow-up pass")
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
        while True:
            start = datetime.now(tz=TZ)
            hour = start.hour
            if WORK_START <= hour < WORK_END:
                if _is_weekend(start):
                    LOG.info("Weekend; skipping follow-up pass")
                else:
                    LOG.info("Starting follow‑up pass at %s", start.isoformat())
                    try:
                        _follow_up_pass()
                    except Exception as e:
                        LOG.error("Error during follow-up pass: %s", e)
            else:
                LOG.info(
                    "Current hour %s outside work hours (%s–%s); skipping follow‑up",
                    hour, WORK_START, WORK_END
                )

            now = datetime.now(tz=TZ)
            if now.hour >= WORK_END - 1:
                next_run = (now + timedelta(days=1)).replace(
                    hour=WORK_START, minute=0, second=0, microsecond=0
                )
            elif now.hour < WORK_START:
                next_run = now.replace(
                    hour=WORK_START, minute=0, second=0, microsecond=0
                )
            else:
                next_run = (now + timedelta(hours=1)).replace(
                    minute=0, second=0, microsecond=0
                )
            sleep_secs = max(0, (next_run - datetime.now(tz=TZ)).total_seconds())
            LOG.debug(
                "Sleeping %.0f seconds until next run at %s",
                sleep_secs, next_run.isoformat()
            )
            time.sleep(sleep_secs)

