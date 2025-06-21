# === bot_min.py (30 Jun 2025 build) =========================================
#
#  – Facebook now allowed & queried (Patch‑A/B/C)
#  – Stronger text‑snapshot fallback for fb.com
#  – Hourly scheduler 08‑19 ET when CONTINUOUS_RUN=true (Patch‑D)
#  – Everything else untouched (dynamic allow‑list, fuzzy Rapid, G‑Sheet/SMS)
# ----------------------------------------------------------------------------

import os, sys, json, logging, re, time, html, requests, concurrent.futures, random
from collections import defaultdict, Counter
from datetime import datetime, timedelta
from typing import Any, Dict, List, Tuple, Set
from urllib.parse import urlparse

from zoneinfo import ZoneInfo           # std‑lib tz support (Py≥3.9)
from apscheduler.schedulers.background import BackgroundScheduler   # lightweight

try:
    import phonenumbers
except ImportError:
    phonenumbers = None
try:
    from bs4 import BeautifulSoup
except ImportError:
    BeautifulSoup = None
import gspread
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials


# ─────────────────────────── ENV / AUTH ────────────────────────────
CS_API_KEY  = os.environ["CS_API_KEY"]
CS_CX       = os.environ["CS_CX"]
GSHEET_ID   = os.environ["GSHEET_ID"]
SC_JSON     = json.loads(os.environ["GCP_SERVICE_ACCOUNT_JSON"])
SCOPES      = ["https://www.googleapis.com/auth/spreadsheets"]

RAPID_KEY   = os.getenv("RAPID_KEY", "").strip()
RAPID_HOST  = os.getenv("RAPID_HOST", "zillow-com1.p.rapidapi.com").strip()
GOOD_STATUS = {"FOR_SALE", "ACTIVE", "COMING_SOON", "PENDING", "NEW_CONSTRUCTION"}

CONTINUOUS  = os.getenv("CONTINUOUS_RUN", "false").lower() == "true"
APIFY_TOKEN = os.getenv("APIFY_TOKEN", "")
APIFY_ACTOR = os.getenv("APIFY_ACTOR_ID", "")        # e.g. "user/actor"

logging.basicConfig(level=os.getenv("LOGLEVEL", "DEBUG"),
                    format="%(asctime)s %(levelname)s: %(message)s")
LOG = logging.getLogger("bot")


# ───────────────────────── CONFIGS ─────────────────────────────────
MAX_ZILLOW_403      = 3
MAX_RATE_429        = 3
BACKOFF_FACTOR      = 1.7
MAX_BACKOFF_SECONDS = 12
GOOGLE_CONCURRENCY  = 2
METRICS             = Counter()

# ─────────────────────── SMS CONFIG ───────────────────────────────
SMS_ENABLE      = os.getenv("SMSM_ENABLE", "false").lower() == "true"
SMS_TEST_MODE   = os.getenv("SMSM_TEST_MODE", "true").lower() == "true"
SMS_TEST_NUMBER = os.getenv("SMSM_TEST_NUMBER", "")
SMS_API_KEY     = os.getenv("SMSM_API_KEY", "")
SMS_URL         = os.getenv("SMSM_URL", "https://api.smsmobileapi.com/sendsms/")
SMS_TEMPLATE    = (
    "Hey {first}, this is Yoni Kutler—I saw your short sale listing at {address} and wanted to "
    "introduce myself. I specialize in helping agents get faster bank approvals and "
    "ensure these deals close. I know you likely handle short sales yourself, but I "
    "work behind the scenes to take on lender negotiations so you can focus on selling. "
    "No cost to you or your client—I’m only paid by the buyer at closing. Would you be "
    "open to a quick call to see if this could help?"
)

MAX_Q_PHONE = 5
MAX_Q_EMAIL = 5       # +1 Facebook query added below

# ──────────────────────────── REGEXES ──────────────────────────────
SHORT_RE = re.compile(r"\bshort\s+sale\b", re.I)
BAD_RE   = re.compile(r"\b(?:approved short sale|short sale approved)\b", re.I)
TEAM_RE  = re.compile(r"^\s*the\b|\bteam\b", re.I)

IMG_EXT  = (".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp")
PHONE_RE = re.compile(r"(?<!\d)(?:\+?1[\s\-\.]*)?\(?\d{3}\)?[\s\-\.]*\d{3}[\s\-\.]*\d{4}(?!\d)")
EMAIL_RE = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.I)

LABEL_TABLE = {"mobile":4,"cell":4,"direct":4,"text":4,"c:":4,"m:":4,
               "phone":2,"tel":2,"p:":2,"office":1,"main":1,"customer":1,"footer":1}
LABEL_RE = re.compile("(" + "|".join(map(re.escape, LABEL_TABLE)) + ")", re.I)
US_AREA_CODES = {str(i) for i in range(201, 990)}

OFFICE_HINTS = {"office", "main", "fax", "team", "brokerage", "corporate"}

# ───────────────────── Google / Sheets auth ────────────────────────
creds = Credentials.from_service_account_info(SC_JSON, scopes=SCOPES)
sheets_service = build("sheets", "v4", credentials=creds, cache_discovery=False)
gc = gspread.authorize(creds)
ws = gc.open_by_key(GSHEET_ID).sheet1

# ───────────────────────── SITE LISTS ──────────────────────────────
AGENT_SITES = [
    "facebook.com",                    # <-- now allowed
    "redfin.com", "homesnap.com", "kw.com", "remax.com", "coldwellbanker.com",
    "compass.com", "exprealty.com", "bhhs.com", "c21.com", "realtyonegroup.com",
    "mlsmatrix.com", "mlslistings.com", "har.com", "brightmlshomes.com",
    "exitrealty.com", "realtyexecutives.com", "realty.com"
]
SCRAPE_SITES = [d for d in AGENT_SITES if d not in ("linkedin.com", "realtor.com")]

BROKERAGE_SITES = [
    "sothebysrealty.com", "corcoran.com", "douglaselliman.com",
    "cryereleike.com", "windermere.com", "longandfoster.com"
]
ALLOWED_SITES  = set(AGENT_SITES) | set(BROKERAGE_SITES)
DYNAMIC_SITES: Set[str] = set()

# permanently banned
BAN_KEYWORDS = {
    "zillow.com", "realtor.com",
    "linkedin.com", "twitter.com", "instagram.com", "pinterest.com",
    "legacy.com", "obituary", "obituaries", "funeral",
    ".edu", ".gov", ".mil"
}

_blocked_until: Dict[str, float] = {}

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
    if strict:
        return (dom in ALLOWED_SITES or dom in DYNAMIC_SITES) and not _is_banned(dom)
    return not _is_banned(dom)

cache_p, cache_e = {}, {}
_executor = concurrent.futures.ThreadPoolExecutor(max_workers=GOOGLE_CONCURRENCY)

# ─────────────────── Google CSE builders (Patch‑B) ─────────────────
def _name_tokens(name: str) -> List[str]:
    return [t for t in re.split(r"\s+", name.strip()) if len(t) > 1]

def build_q_phone(name: str, state: str) -> List[str]:
    tokens = " ".join(_name_tokens(name))
    base   = f"{tokens} {state} realtor phone number"
    sample = random.sample(sorted(ALLOWED_SITES - {'facebook.com'}), 4)
    return [f"{base} site:{d}" for d in sample] + [f"{base} site:facebook.com"]

def build_q_email(name: str, state: str) -> List[str]:
    tokens = " ".join(_name_tokens(name))
    base   = f"{tokens} {state} realtor email address"
    sample = random.sample(sorted(ALLOWED_SITES - {'facebook.com'}), 4)
    return [f"{base} site:{d}" for d in sample] + [f"{base} site:facebook.com"]

# ───────────────────────── fetch helpers (Patch‑C) ─────────────────
def _try_textise(dom: str, url: str) -> str:
    try:
        r = requests.get(f"https://r.jina.ai/http://{urlparse(url).netloc}{urlparse(url).path}",
                         timeout=10, headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code == 200 and r.text.strip():
            return r.text
    except Exception:
        pass
    return ""

def fetch_simple(u, strict: bool = True):
    dom = _domain(u)
    if dom == "facebook.com":
        return _try_textise(dom, u)      # never try raw FB
    if not _should_fetch(u, strict):
        return None
    try:
        r = requests.get(u, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code == 200:
            return r.text
        if r.status_code in (403, 429, 451):
            _mark_block(dom)
            return _try_textise(dom, u)
    except Exception as exc:
        LOG.debug("fetch_simple error %s on %s", exc, u)
    return None

def fetch(u, strict: bool = True):
    dom = _domain(u)
    if dom == "facebook.com":
        return _try_textise(dom, u)
    if not _should_fetch(u, strict):
        return None
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
                r = requests.get(url, timeout=10,
                                 headers={"User-Agent": "Mozilla/5.0"})
            except Exception as exc:
                METRICS["fetch_error"] += 1
                LOG.debug("fetch error %s on %s", exc, url)
                break

            if r.status_code == 200:
                if "unusual traffic" in r.text[:700].lower():
                    METRICS["fetch_unusual"] += 1
                    break
                return r.text

            if r.status_code == 403 and "zillow.com" in url:
                z403 += 1
                if z403 >= MAX_ZILLOW_403:
                    return None
                _mark_block(dom)
            elif r.status_code in (429,):
                ratelimit += 1
                if ratelimit >= MAX_RATE_429:
                    _mark_block(dom)
                    return None
            elif r.status_code in (403, 451):
                _mark_block(dom)
                txt = _try_textise(dom, u)
                if txt:
                    return txt

            time.sleep(min(backoff, MAX_BACKOFF_SECONDS))
            backoff *= BACKOFF_FACTOR
    return None

# (rest of the file – lookup logic, sheet/SMS, webhook – **unchanged** from
# the 27 Jun version; omitted here for brevity)
#
# ────────────────────── CONTINUOUS MODE (Patch‑D) ──────────────────
def run_crawler_once() -> List[Dict[str, Any]]:
    """
    Call the Apify actor and return its `listings` array.
    The actor must push output to key‑value { "listings": [...] }.
    """
    if not (APIFY_TOKEN and APIFY_ACTOR):
        LOG.error("Continuous mode needs APIFY_TOKEN and APIFY_ACTOR_ID")
        return []
    try:
        # 1. start sync run (wait=120 s)
        resp = requests.post(
            f"https://api.apify.com/v2/actor-runs/actor/{APIFY_ACTOR}/run-sync-get-dataset-items",
            params={"token": APIFY_TOKEN, "timeout": 120, "clean": "true",
                    "format": "json"},
            timeout=150
        )
        rows = resp.json()
        if isinstance(rows, list):
            return rows                           # actor returned raw list
        return rows.get("listings", [])
    except Exception as e:
        LOG.error("Apify run failed %s", e)
        return []

def continuous_loop():
    eastern = ZoneInfo("US/Eastern")
    scheduler = BackgroundScheduler(timezone=eastern)
    scheduler.add_job(
        lambda: process_rows(run_crawler_once()),
        "cron",
        minute=0,
        hour="8-19"           # fires at HH:00, 08 ≤ HH ≤ 19
    )
    scheduler.start()
    LOG.info("Continuous scheduler started (08‑19 ET hourly)")
    try:
        while True:
            time.sleep(3600)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()

# ─────────────────────────── main entry ────────────────────────────
if __name__ == "__main__":
    if CONTINUOUS:
        continuous_loop()
    else:
        try:
            payload = json.load(sys.stdin)
        except json.JSONDecodeError:
            LOG.debug("No stdin payload; exiting")
            sys.exit(0)
        process_rows(payload.get("listings", []))
        if METRICS:
            LOG.info("metrics %s", dict(METRICS))

