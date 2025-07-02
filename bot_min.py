#  ╭────────────────── bot_min.py ─────────────────────────╮
#  │ build: 02 Jul 2025 – “FU singleton + inbound poller”  │
#  │   • Scheduler singleton guard (P1)                    │
#  │   • Per‑domain exponential back‑off (P2)              │
#  │   • Inbound‑SMS poller + optional FastAPI webhook     │
#  │   • Richer decision‑path logging & observability      │
#  ╰───────────────────────────────────────────────────────╯
#
#  SHEET LAYOUT  (1‑based columns) ───────────────────────────────────────────
#      A First          H (8)  Sent‑flag  “x”  – initial SMS queued
#      B Last           I (9)  Reply‑flag “x”  – SMSMobile auto reply detected
#      C Phone          J (10) Manual‑note    – human‑entered agent response
#      D Email          K (11) Reply‑TS       – ISO time auto reply logged
#      E Street         L (12) Msg‑ID (outbound provider reference)
#      F City           … …                   – free / reserved
#      G State          W (23) Init‑SMS‑TS    – ISO time initial SMS sent
#                      X (24) Follow‑up‑TS   – ISO time follow‑up SMS sent
#  ───────────────────────────────────────────────────────────────────────────

# ───────────────────────── imports ─────────────────────────
import concurrent.futures
import html
import json
import logging
import os
import random
import re
import sys
import threading
import time
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from typing import Any, Dict, List, Set, Tuple
from urllib.parse import urlparse

import gspread
import pytz
import requests
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build as gapi_build

# optional deps
try:
    import phonenumbers
except ImportError:
    phonenumbers = None
try:
    from bs4 import BeautifulSoup
except ImportError:
    BeautifulSoup = None
try:
    from fastapi import FastAPI, Request
except ImportError:
    FastAPI = None  # webhook mode will be disabled automatically

# ─────────────────────────── ENV / AUTH ────────────────────────────
CS_API_KEY = os.environ["CS_API_KEY"]
CS_CX      = os.environ["CS_CX"]
GSHEET_ID  = os.environ["GSHEET_ID"]
SC_JSON    = json.loads(os.environ["GCP_SERVICE_ACCOUNT_JSON"])
SCOPES     = ["https://www.googleapis.com/auth/spreadsheets"]

RAPID_KEY  = os.getenv("RAPID_KEY", "").strip()
RAPID_HOST = os.getenv("RAPID_HOST", "zillow-com1.p.rapidapi.com").strip()
GOOD_STATUS = {
    "FOR_SALE", "ACTIVE", "COMING_SOON", "PENDING", "NEW_CONSTRUCTION",
}

TZ         = pytz.timezone(os.getenv("BOT_TIMEZONE", "US/Eastern"))
FU_HOURS   = float(os.getenv("FOLLOW_UP_HOURS", "6"))
WORK_START = int(os.getenv("WORK_START_HOUR", "8"))
WORK_END   = int(os.getenv("WORK_END_HOUR", "21"))  # inc. 20 : 00–20 : 59

# ─────────────────────── SMS CONFIG ────────────────────────────────
SMS_ENABLE      = os.getenv("SMSM_ENABLE", "false").lower() == "true"
SMS_TEST_MODE   = os.getenv("SMSM_TEST_MODE", "true").lower() == "true"
SMS_TEST_NUMBER = os.getenv("SMSM_TEST_NUMBER", "")
SMS_API_KEY     = os.getenv("SMSM_API_KEY", "")
SMS_FROM        = os.getenv("SMSM_FROM", "")
SMS_URL         = os.getenv("SMSM_URL", "https://api.smsmobileapi.com/sendsms/")
SMS_TEMPLATE = (
    "Hey {first}, this is Yoni Kutler—I saw your short sale listing at "
    "{address} and wanted to introduce myself. I specialize in helping "
    "agents get faster bank approvals and ensure these deals close. I "
    "know you likely handle short sales yourself, but I work behind the "
    "scenes to take on lender negotiations so you can focus on selling. "
    "No cost to you or your client—I’m only paid by the buyer at closing. "
    "Would you be open to a quick call to see if this could help?"
)
SMS_FU_TEMPLATE = (
    "Hey, just wanted to follow up on my message from earlier. "
    "Let me know if I can help with anything—happy to connect whenever works for you!"
)
SMS_RETRY_ATTEMPTS = int(os.getenv("SMSM_RETRY_ATTEMPTS", "2"))

# inbound API endpoints
RECEIVE_URL = os.getenv("SMSM_INBOUND_URL", "https://api.smsmobileapi.com/getsms/")
READ_URL    = os.getenv("SMSM_READ_URL",    "https://api.smsmobileapi.com/readsms/")

# webhook mode
USE_WEBHOOK = os.getenv("SMSM_USE_WEBHOOK", "false").lower() == "true"

# ───────────────────────── SHEET COLUMN CONSTANTS ─────────────────────────
COL_FIRST        = 0   # A
COL_LAST         = 1   # B
COL_PHONE        = 2   # C
COL_EMAIL        = 3   # D
COL_STREET       = 4   # E
COL_CITY         = 5   # F
COL_STATE        = 6   # G
COL_SENT_FLAG    = 7   # H
COL_REPLY_FLAG   = 8   # I
COL_MANUAL_NOTE  = 9   # J
COL_REPLY_TS     = 10  # K
COL_MSG_ID       = 11  # L
COL_INIT_TS      = 22  # W
COL_FU_TS        = 23  # X
MIN_COLS         = 24

# ───────────────────────── CONFIGS / LOGGING ──────────────────────────────
MAX_ZILLOW_403       = 3
MAX_RATE_429         = 3
BACKOFF_FACTOR       = 1.7
MAX_BACKOFF_SECONDS  = 12
GOOGLE_CONCURRENCY   = 2
INBOUND_WINDOW_HOURS = 48     # how far back to look when polling
METRICS: Counter     = Counter()

logging.basicConfig(
    level=os.getenv("LOGLEVEL", "DEBUG"),
    format="%(asctime)s %(levelname)-8s: %(message)s",
    force=True,
)
LOG = logging.getLogger("bot_min")

# ───────────────────────── Rapid‑API in‑memory cache ──────────────────────
_RAPID_CACHE: Dict[str, Tuple[float, Dict[str, Any]]] = {}
RAPID_TTL_SEC = 15 * 60  # 15 minutes

# ───── per‑domain 429 back‑off table (NEW – P2) ─────
_domain_backoff: Dict[str, float] = {}

# ──────────────────────────── REGEXES ─────────────────────────────────────
SHORT_RE = re.compile(r"\bshort\s+sale\b", re.I)
BAD_RE   = re.compile(r"\b(?:approved short sale|short sale approved)\b", re.I)
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
LABEL_RE = re.compile("(" + "|".join(map(re.escape, LABEL_TABLE)) + ")", re.I)

US_AREA_CODES = {str(i) for i in range(201, 990)}
OFFICE_HINTS  = {"office","main","fax","team","brokerage","corporate"}
BAD_AREA      = {"800","866"}
_blocked_until: Dict[str, float] = {}

# ───────────────────── Google / Sheets setup ───────────────────────
creds          = Credentials.from_service_account_info(SC_JSON, scopes=SCOPES)
sheets_service = gapi_build("sheets", "v4", credentials=creds, cache_discovery=False)
gc             = gspread.authorize(creds)
ws             = gc.open_by_key(GSHEET_ID).sheet1

# preload phone column to avoid duplicates
try:
    _preloaded = ws.col_values(COL_PHONE + 1)  # gspread is 1‑based
except Exception:
    _preloaded = []
seen_phones: Set[str] = set(_preloaded)

# ───────────────────────── SITE LISTS ──────────────────────────────
SCRAPE_SITES:  List[str] = []
DYNAMIC_SITES: Set[str]  = set()
BAN_KEYWORDS = {
    "zillow.com","realtor.com","redfin.com","homes.com",
    "linkedin.com","twitter.com","instagram.com","pinterest.com",
    "legacy.com","obituary","obituaries","funeral",
    ".gov",".edu",".mil",
}

# ───────────────────── threading pool for Google I/O ───────────────
_executor = concurrent.futures.ThreadPoolExecutor(max_workers=GOOGLE_CONCURRENCY)
def pmap(fn, iterable): return list(_executor.map(fn, iterable))

# ───────────────────── utilities: phone / email / fmt  ─────────────
def _is_bad_area(area: str) -> bool:
    return area in BAD_AREA or area.startswith("1")
def fmt_phone(r: str) -> str:
    d = re.sub(r"\D","",r)
    if len(d)==11 and d.startswith("1"):
        d = d[1:]
    if len(d)==10 and not _is_bad_area(d[:3]):
        return f"{d[:3]}-{d[3:6]}-{d[6:]}"
    return ""
def valid_phone(p: str) -> bool:
    if not p:
        return False
    if phonenumbers:
        try:
            ok = phonenumbers.is_possible_number(phonenumbers.parse(p,"US"))
            return ok and not _is_bad_area(p[:3])
        except Exception: return False
    return bool(re.fullmatch(r"\d{3}-\d{3}-\d{4}",p) and not _is_bad_area(p[:3]))
def clean_email(e: str) -> str:
    return e.split("?")[0].strip()
def ok_email(e: str) -> bool:
    e = clean_email(e)
    return (
        e and "@" in e and
        not e.lower().endswith(IMG_EXT) and
        not re.search(r"\.(gov|edu|mil)$",e,re.I)
    )
def is_short_sale(text: str) -> bool:
    return SHORT_RE.search(text) and not BAD_RE.search(text)

# ───────────────────── Rapid‑API helpers with cache ─────────────────────────
def _phone_obj_to_str(obj: Dict[str,str]) -> str:
    if not obj: return ""
    key_order = [
        "areacode","area_code","areaCode","prefix",
        "centralofficecode","central_office_code","centralOfficeCode",
        "number","line","line_number","lineNumber",
    ]
    parts=[]
    for k in key_order:
        if obj.get(k):
            parts.append(re.sub(r"\D","",str(obj[k])))
    for v in obj.values():
        chunk=re.sub(r"\D","",str(v))
        if 2<=len(chunk)<=4: parts.append(chunk)
    digits="".join(parts)[:10]
    return fmt_phone(digits)

def rapid_property(zpid:str)->Dict[str,Any]:
    """
    Cached wrapper around RapidAPI Zillow /property endpoint.
    Entry TTL = 15 minutes.
    """
    now = time.time()
    entry = _RAPID_CACHE.get(zpid)
    if entry and now - entry[0] < RAPID_TTL_SEC:
        return entry[1]                     # hit!

    if not RAPID_KEY:
        _RAPID_CACHE[zpid] = (now, {})
        return {}

    try:
        headers={"X-RapidAPI-Key":RAPID_KEY,"X-RapidAPI-Host":RAPID_HOST}
        r=requests.get(
            f"https://{RAPID_HOST}/property",
            params={"zpid":zpid},
            headers=headers,timeout=15)
        if r.status_code==429:
            LOG.error("Rapid-API quota exhausted (HTTP 429)")
            data={}
        else:
            r.raise_for_status()
            data=r.json().get("data") or r.json()
    except Exception as exc:
        LOG.debug("Rapid-API fetch error %s for zpid=%s",exc,zpid)
        data={}
    _RAPID_CACHE[zpid]=(now,data)
    return data

def _phones_from_block(blk:Dict[str,Any])->List[str]:
    out=[]
    if not blk: return out
    if blk.get("phone"): out.append(_phone_obj_to_str(blk["phone"]))
    for ph in blk.get("phones",[]): out.append(_phone_obj_to_str(ph))
    return [p for p in out if p]

def _emails_from_block(blk:Dict[str,Any])->List[str]:
    if not blk: return []
    out=[]
    for k in("email","emailAddress"):
        if blk.get(k): out.append(clean_email(blk[k]))
    for e in blk.get("emails",[]): out.append(clean_email(e))
    return [e for e in out if ok_email(e)]

def _names_match(a:str,b:str)->bool:
    ta={t.lower().strip(".") for t in a.split() if len(t)>1}
    tb={t.lower().strip(".") for t in b.split() if len(t)>1}
    return bool(ta & tb)

def rapid_phone(zpid:str,agent_name:str)->Tuple[str,str]:
    data=rapid_property(zpid)
    if not data: return "",""
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

# ───────────────────── HTML fetch helpers (↑ adds per-domain back‑off) ────
def _jitter() -> None:
    time.sleep(random.uniform(0.8, 1.5))
def _mark_block(dom: str, dur: int = 600) -> None:
    _blocked_until[dom] = time.time() + dur
def _try_textise(dom: str, url: str) -> str:
    try:
        r = requests.get(
            f"https://r.jina.ai/http://{urlparse(url).netloc}{urlparse(url).path}",
            timeout=10,
            headers={"User-Agent": "Mozilla/5.0"},
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
    if dom in _domain_backoff and _domain_backoff[dom] > time.time():
        return False
    return not (_is_banned(dom) and strict)

def fetch_simple(u: str, strict: bool = True):
    if not _should_fetch(u, strict):
        return None
    dom = _domain(u)
    try:
        r = requests.get(u, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code == 200:
            return r.text
        if r.status_code in (403, 429):
            _mark_block(dom)
            if r.status_code == 429:
                _domain_backoff[dom] = time.time() + BACKOFF_FACTOR * 60
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
                r = requests.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
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
                METRICS["fetch_403"] += 1
                if z403 >= MAX_ZILLOW_403:
                    return None
                _mark_block(dom)
            elif r.status_code == 429:
                ratelimit += 1
                METRICS["fetch_429"] += 1
                _domain_backoff[dom] = time.time() + backoff * 60
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

# ───────────────────── Google CSE helper (unchanged) ──────────────────────
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
            j = requests.get(
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

# ───────────────────── structured & proximity scan (unchanged) ────────────
def extract_struct(td: str) -> Tuple[List[str], List[str]]:
    phones, mails = [], []
    if not BeautifulSoup:
        return phones, mails
    soup = BeautifulSoup(td, "html.parser")
    for sc in soup.find_all("script", {"type": "application/ld+json"}):
        try:
            data = json.loads(sc.string or "")
        except Exception:
            continue
        if isinstance(data, list):
            data = data[0]
        if not isinstance(data, dict):
            continue
        tel = data.get("telephone") or (data.get("contactPoint") or {}).get("telephone")
        if isinstance(tel, list):
            for t in tel:
                phones.append(fmt_phone(t))
        elif tel:
            phones.append(fmt_phone(tel))
        mail = data.get("email") or (data.get("contactPoint") or {}).get("email")
        if isinstance(mail, list):
            for m in mail:
                mails.append(clean_email(m))
        elif mail:
            mails.append(clean_email(mail))
    for a in soup.select('a[href^="tel:"]'):
        phones.append(fmt_phone(a["href"].split("tel:")[-1]))
    for a in soup.select('a[href^="mailto:"]'):
        mails.append(clean_email(a["href"].split("mailto:")[-1]))
    return phones, mails
def proximity_scan(t: str, last_name: str | None = None):
    out = {}
    for m in PHONE_RE.finditer(t):
        p = fmt_phone(m.group())
        if not valid_phone(p):
            continue
        sn_start = max(m.start() - 120, 0)
        snippet = t[sn_start : m.end() + 120]
        if last_name and last_name.lower() not in snippet:
            continue
        lab_match = LABEL_RE.search(snippet)
        lab = lab_match.group().lower() if lab_match else ""
        w = LABEL_TABLE.get(lab, 0)
        if w < 1:
            continue
        bw, ts, off = out.get(p, (0, 0, False))
        out[p] = (max(bw, w), ts + 2 + w, lab in ("office", "main"))
    return out

# ───────────────────── Google query builders (unchanged) ───────────────────
def build_q_phone(name: str, state: str) -> List[str]:
    return [f'"{name}" {state} realtor phone']
def build_q_email(
    name: str, state: str, brokerage: str = "", domain_hint: str = "", mls_id: str = ""
) -> List[str]:
    out = [f'"{name}" {state} realtor email']
    if brokerage:
        out.append(f'"{name}" "{brokerage}" email')
    if domain_hint:
        out.append(f'site:{domain_hint} "{name}" email')
    if mls_id:
        out.append(f'"{mls_id}" "{name.split()[-1]}" email')
    return out

# ───────────────────── nickname helpers / email heuristics (unchanged) ─────
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
    local = email.split("@", 1)[0].lower()
    tks = [re.sub(r"[^a-z]", "", w.lower()) for w in agent.split() if w]
    if not tks:
        return False
    first, last = tks[0], tks[-1]
    for tk in tks:
        if len(tk) >= 3 and tk in local:
            return True
        for var in _token_variants(tk):
            if len(var) >= 3 and var in local:
                return True
    if first and last and (
        first[0] + last in local or first + last[0] in local or last + first[0] in local
    ):
        return True
    return False

# ───────────────────── caches & pattern synth (unchanged) ──────────────────
cache_p: Dict[str, str] = {}
cache_e: Dict[str, str] = {}
domain_patterns: Dict[str, str] = {}
def _pattern_from_example(addr: str, name: str) -> str:
    first, last = map(
        lambda s: re.sub(r"[^a-z]", "", s.lower()),
        (name.split()[0], name.split()[-1]),
    )
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
    first, last = map(
        lambda s: re.sub(r"[^a-z]", "", s.lower()),
        (name.split()[0], name.split()[-1]),
    )
    fi, li = first[0], last[0]
    local = patt.format(first=first, last=last, fi=fi, li=li)
    return f"{local}@{domain}"

# ───────────────────── phone-number lookup (unchanged) ────────────────────
def _split_portals(urls):
    portals, non = [], []
    for u in urls:
        (portals if any(d in u for d in SCRAPE_SITES) else non).append(u)
    return non, portals
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
            if last in page.lower()[max(0, pos - 200) : pos + 200]:
                return True
    return False
def lookup_phone(agent: str, state: str, row_payload: Dict[str, Any]) -> str:
    key = f"{agent}|{state}"
    if key in cache_p:
        return cache_p[key]
    for blk in (row_payload.get("contact_recipients") or []):
        for p in _phones_from_block(blk):
            d = fmt_phone(p)
            if d and valid_phone(d):
                cache_p[key] = d
                LOG.debug("PHONE hit directly from contact_recipients")
                return d
    zpid = str(row_payload.get("zpid", ""))
    undirect_phone = ""
    if zpid:
        phone, src = rapid_phone(zpid, agent)
        if phone and _looks_direct(phone, agent, state):
            cache_p[key] = phone
            LOG.debug("PHONE WIN %s via %s (surname proximity)", phone, src)
            return phone
        undirect_phone = phone
    cand_good, cand_office, src_good = {}, {}, {}
    def add(p, score, office_flag, src=""):
        d = fmt_phone(p)
        if not valid_phone(d):
            return
        (cand_office if office_flag else cand_good)[d] = (
            (cand_office if office_flag else cand_good).get(d, 0) + score
        )
        if not office_flag and src:
            src_good[d] = src
            DYNAMIC_SITES.add(_domain(src))
    for items in pmap(google_items, build_q_phone(agent, state)):
        for it in items:
            tel = (
                it.get("pagemap", {})
                .get("contactpoint", [{}])[0]
                .get("telephone")
                or ""
            )
            if tel:
                add(tel, 4, False, f"CSE:{it.get('link','')}")
    urls = [
        it.get("link", "")
        for items in pmap(google_items, build_q_phone(agent, state))
        for it in items
    ][:20]
    non_portal, portal = _split_portals(urls)
    last_name = (agent.split()[-1] if len(agent.split()) > 1 else agent).lower()
    for url, page in zip(non_portal, pmap(fetch_simple, non_portal)):
        if not page or agent.lower() not in page.lower():
            continue
        ph, _ = extract_struct(page)
        for p in ph:
            add(p, 6, False, url)
        low = html.unescape(page.lower())
        for p, (_, sc, off) in proximity_scan(low, last_name).items():
            add(p, sc, off, url)
        if cand_good or cand_office:
            break
    if not cand_good:
        for url, page in zip(portal, pmap(fetch, portal)):
            if not page or agent.lower() not in page.lower():
                continue
            ph, _ = extract_struct(page)
            for p in ph:
                add(p, 4, False, url)
            low = html.unescape(page.lower())
            for p, (_, sc, off) in proximity_scan(low, last_name).items():
                add(p, sc, off, url)
            if cand_good:
                break
    phone = cand_good and max(cand_good, key=cand_good.get) or undirect_phone
    cache_p[key] = phone or ""
    if phone:
        LOG.debug("PHONE WIN %s via %s", phone, src_good.get(phone, "crawler/unverified"))
    else:
        LOG.debug(
            "PHONE FAIL for %s %s  cand_good=%s cand_office=%s",
            agent, state, cand_good, cand_office,
        )
    return phone

# ───────────────────── e‑mail lookup (unchanged) ───────────────────────────
def lookup_email(agent: str, state: str, row_payload: Dict[str, Any]) -> str:
    key = f"{agent}|{state}"
    if key in cache_e:
        return cache_e[key]
    brokerage = domain_hint = mls_id = ""
    for blk in (row_payload.get("contact_recipients") or []):
        for em in _emails_from_block(blk):
            if _email_matches_name(agent, em):
                cache_e[key] = em
                LOG.debug("EMAIL direct-payload match")
                return em
    zpid = str(row_payload.get("zpid", ""))
    if zpid:
        rapid = rapid_property(zpid)
        if rapid:
            lb = rapid.get("listed_by") or {}
            brokerage = lb.get("brokerageName", "")
            mls_id = lb.get("listingAgentMlsId", "")
            for em in _emails_from_block(lb):
                if _email_matches_name(agent, em):
                    cache_e[key] = em
                    LOG.debug("EMAIL via rapid:listed_by")
                    return em
    cand, src_e = defaultdict(int), {}
    def add_e(m, score, src=""):
        m = clean_email(m)
        if not ok_email(m) or not _email_matches_name(agent, m):
            return
        if re.search(r"\b(info|office|admin|support|advertising|noreply|hello)\b", m, re.I):
            score -= 2
        tokens = {re.sub(r"[^a-z]", "", w.lower()) for w in agent.split()}
        if tokens and all(tok and tok in m.lower() for tok in tokens):
            score += 3
        if brokerage and brokerage.lower() in m.lower():
            score += 1
        cand[m] += score
        if src:
            src_e.setdefault(m, src)
            DYNAMIC_SITES.add(_domain(src))
        patt = _pattern_from_example(m, agent)
        if patt:
            domain_patterns.setdefault(_domain(m), patt)
    for items in pmap(google_items, build_q_email(agent, state, brokerage, domain_hint, mls_id)):
        for it in items:
            mail = (
                it.get("pagemap", {})
                .get("contactpoint", [{}])[0]
                .get("email", "")
            )
            add_e(mail, 3, f"CSE:{it.get('link','')}")
    urls = [
        it.get("link", "")
        for items in pmap(google_items, build_q_email(agent, state, brokerage, domain_hint, mls_id))
        for it in items
    ][:20]
    non_portal, portal = _split_portals(urls)
    for url, page in zip(non_portal, pmap(fetch_simple, non_portal)):
        if not page or agent.lower() not in page.lower():
            continue
        _, ems = extract_struct(page)
        for m in ems:
            add_e(m, 3, url)
        for m in EMAIL_RE.findall(page):
            add_e(m, 1, url)
        if cand:
            break
    if not cand:
        for url, page in zip(portal, pmap(fetch, portal)):
            if not page or agent.lower() not in page.lower():
                continue
            _, ems = extract_struct(page)
            for m in ems:
                add_e(m, 2, url)
            for m in EMAIL_RE.findall(page):
                add_e(m, 1, url)
            if cand:
                break
    if not cand and domain_hint:
        guess = _synth_email(agent, domain_hint)
        if guess:
            add_e(guess, 2, "pattern-synth")
    email = ""
    if cand:
        max_score = max(cand.values())
        winners = [m for m, s in cand.items() if s == max_score]
        if len(winners) == 1:
            email = winners[0]
        else:
            last_tok = re.sub(r"[^a-z]", "", agent.split()[-1].lower())
            good = [m for m in winners if last_tok and last_tok in m.split("@")[0].lower()]
            email = good[0] if good else ""
            if not email:
                LOG.debug("EMAIL tie %s – dropped (last name absent)", winners)
    cache_e[key] = email or ""
    if email:
        LOG.debug("EMAIL WIN %s via %s", email, src_e.get(email, "crawler/pattern"))
    else:
        LOG.debug("EMAIL FAIL for %s %s – personalised e-mail not found", agent, state)
    return email

# ───────────────────── Google-Sheet helpers (UPDATED logging) ──────────────
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
        LOG.debug("Row %s marked SENT (H, W, L filled) – msg_id=%s", row_idx, msg_id)
    except Exception as e:
        LOG.error("GSheet mark_sent error %s", e)

def mark_followup(row_idx: int):
    ts = datetime.now(tz=TZ).isoformat()
    try:
        sheets_service.spreadsheets().values().update(
            spreadsheetId=GSHEET_ID,
            range=f"Sheet1!X{row_idx}:X{row_idx}",
            valueInputOption="RAW",
            body={"values": [[ts]]},
        ).execute()
        LOG.debug("Row %s marked FOLLOW‑UP (X filled)", row_idx)
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
        LOG.info("Row %s marked REPLY (I, K) – agent responded", row_idx)
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
    LOG.info("Row appended (row %s)", row_idx)
    return row_idx

def phone_exists(p):
    return p in seen_phones

# ───────────────────── inbound‑SMS processing (NEW) ────────────────────────
def _normalize_e164(p: str) -> str:
    d = re.sub(r"\D", "", p)
    if len(d) == 10:
        d = "1" + d
    if not d.startswith("+"):
        d = "+" + d
    return d

def _find_row(phone: str = "", ref: str = "") -> int | None:
    """
    Try to locate the sheet row by outbound msg_id OR by phone.
    Search only the newest 250 rows for speed.
    """
    try:
        vals = ws.get_all_values()
    except Exception as e:
        LOG.error("Sheet read error in _find_row: %s", e)
        return None
    base = max(1, len(vals) - 250)
    for idx in range(len(vals)-1, base-1, -1):
        row = vals[idx]
        if len(row) < MIN_COLS:
            row += [""] * (MIN_COLS - len(row))
        row_phone = row[COL_PHONE].strip()
        row_ref   = row[COL_MSG_ID].strip()
        if ref and ref == row_ref:
            return idx + 1       # convert 0‑based list index to 1‑based sheet row
        if phone and phone == row_phone:
            return idx + 1
    return None

def _handle_inbound(from_num: str, reference: str, ts_str: str | None = None):
    phone_std = fmt_phone(from_num)
    row = _find_row(phone_std, reference)
    if not row:
        LOG.debug("Inbound SMS from %s (ref=%s) – no matching sheet row", from_num, reference)
        return
    mark_reply(row)

# ───────── inbound‑SMS poller thread  (polling strategy) ─────────
def _inbound_poll_loop():
    LOG.info("Inbound‑SMS poller thread started")
    since = datetime.now(tz=TZ) - timedelta(hours=INBOUND_WINDOW_HOURS)
    while True:
        try:
            params = {
                "apikey": SMS_API_KEY,
                "direction": "inbound",
                "after": since.isoformat(timespec="seconds"),
            }
            r = requests.get(RECEIVE_URL, params=params, timeout=12)
            if r.status_code != 200:
                LOG.debug("Inbound poll HTTP %s – %s", r.status_code, (r.text or "")[:120])
            else:
                data = r.json()
                if str(data.get("error")) == "0":
                    msgs = data.get("messages", [])
                    for m in msgs:
                        frm = m.get("from", "")
                        ref = m.get("reference", "") or ""
                        ts  = m.get("timestamp") or m.get("date") or ""
                        LOG.info("Inbound SMS poll hit – from=%s ref=%s ts=%s", frm, ref, ts)
                        _handle_inbound(frm, ref, ts)
                        # mark as read so it does not repeat
                        mid = m.get("id", "")
                        if mid:
                            try:
                                requests.post(READ_URL, timeout=6,
                                              data={"apikey": SMS_API_KEY, "id": mid, "read": 1})
                            except Exception:
                                pass
                else:
                    LOG.debug("Inbound poll API error field=%s", data.get("error"))
        except Exception as e:
            LOG.error("Inbound poll exception: %s", e)
        since = datetime.now(tz=TZ) - timedelta(hours=INBOUND_WINDOW_HOURS)
        time.sleep(120)   # every 2 min

# ───────────────────── SMS sender (UPDATED logging) ───────────────────────
def _send_once(phone: str, message: str) -> Tuple[bool, str]:
    payload = {
        "apikey": SMS_API_KEY,
        "recipients": phone,
        "message": message,
        "sendsms": "1",
    }
    try:
        resp = requests.post(SMS_URL, timeout=10, data=payload)
        result = {}
        try:
            result = resp.json().get("result", {})
        except Exception:
            pass
        ok = resp.status_code == 200 and str(result.get("error")) == "0"
        msg_id = result.get("message_id") or ""
        if not ok:
            LOG.error("SMS API error %s – %s", resp.status_code, (resp.text or "")[:240])
        return ok, msg_id
    except Exception as e:
        LOG.error("SMS send exception %s", e)
        return False, ""

def send_sms(
    phone: str,
    first: str,
    address: str,
    row_idx: int,
    follow_up: bool = False,
):
    if not SMS_ENABLE or not phone:
        return
    dest_phone = phone
    if SMS_TEST_MODE and SMS_TEST_NUMBER:
        dest_phone = SMS_TEST_NUMBER
    msg_txt = SMS_FU_TEMPLATE if follow_up else SMS_TEMPLATE.format(first=first, address=address)
    for attempt in range(1, SMS_RETRY_ATTEMPTS + 1):
        ok, msg_id = _send_once(dest_phone, msg_txt)
        if ok:
            if follow_up:
                mark_followup(row_idx)
                LOG.info("FOLLOW‑UP SMS sent to %s (row %s) msg_id=%s", dest_phone, row_idx, msg_id)
            else:
                mark_sent(row_idx, msg_id)
                LOG.info("INITIAL SMS sent to %s (row %s) msg_id=%s", dest_phone, row_idx, msg_id)
            return
        LOG.debug("SMS attempt %s failed → retrying", attempt)
        time.sleep(5)
    LOG.error("SMS failed after %s attempts to %s", SMS_RETRY_ATTEMPTS, dest_phone)

# ───────────────────── follow-up scheduler (UPDATED) ──────────────────────
def _business_elapsed(start: datetime, end: datetime) -> float:
    if start >= end:
        return 0.0
    cur = start
    elapsed = 0.0
    while cur < end:
        nxt = min(end, cur + timedelta(hours=1))
        if WORK_START <= cur.hour < WORK_END:
            elapsed += (nxt - cur).total_seconds() / 3600.0
        cur = nxt
    return elapsed

def _schedule_loop():
    LOG.info("FU‑scheduler thread started")
    while True:
        try:
            vals = ws.get_all_values()
            if len(vals) <= 2:
                LOG.debug("Sheet empty – skipping scheduler pass")
                time.sleep(600)
                continue
            all_rows = vals[1:]          # drop header
            rows = all_rows[-100:]       # newest 100 for a wider window
            base_idx = len(vals) - len(rows) + 1
            for rel_i, row in enumerate(rows, start=0):
                idx = base_idx + rel_i
                # pad to full length
                if len(row) < MIN_COLS:
                    row = row + [""] * (MIN_COLS - len(row))
                sent_flag   = row[COL_SENT_FLAG].strip().lower()
                reply_flag  = row[COL_REPLY_FLAG].strip()
                manual_note = row[COL_MANUAL_NOTE].strip()
                init_ts     = row[COL_INIT_TS].strip()
                fu_ts       = row[COL_FU_TS].strip()
                msg_id      = row[COL_MSG_ID].strip()
                phone       = row[COL_PHONE].strip()
                first       = row[COL_FIRST].strip()
                address     = row[COL_STREET].strip()
                # decision path logging
                if LOG.isEnabledFor(logging.DEBUG):
                    LOG.debug("Row %s – flags: sent=%s fu=%s reply=%s note=%s", idx,
                              bool(sent_flag), bool(fu_ts), bool(reply_flag), bool(manual_note))
                # skip cases
                if not sent_flag or not init_ts:
                    continue
                if fu_ts:
                    continue
                if reply_flag or manual_note:
                    continue
                # elapsed time check
                try:
                    t0 = datetime.fromisoformat(init_ts)
                except Exception:
                    LOG.debug("Row %s – bad init_ts", idx)
                    continue
                now = datetime.now(tz=TZ)
                worked = _business_elapsed(t0.astimezone(TZ), now)
                if worked < FU_HOURS:
                    continue
                # real‑time double‑check for reply via API
                if check_reply(phone, msg_id, init_ts):
                    mark_reply(idx)
                    LOG.info("Row %s – reply detected during FU check, follow‑up aborted", idx)
                    continue
                # send FU
                LOG.info("Row %s – sending follow‑up (elapsed %.2fh)", idx, worked)
                send_sms(phone, first, address, idx, follow_up=True)
        except Exception as e:
            LOG.error("FU scheduler error: %s", e)
        time.sleep(600)  # 10 min

# ─────────── ensure scheduler singleton (P1) ────────────
if not getattr(sys.modules[__name__], "_SCHEDULER_STARTED", False):
    _scheduler_thread = threading.Thread(target=_schedule_loop, daemon=True)
    _scheduler_thread.start()
    sys.modules[__name__]._SCHEDULER_STARTED = True

# start inbound poller once
if SMS_ENABLE and not getattr(sys.modules[__name__], "_INBOUND_STARTED", False):
    _poll_thread = threading.Thread(target=_inbound_poll_loop, daemon=True)
    _poll_thread.start()
    sys.modules[__name__]._INBOUND_STARTED = True

# ───────── optional FastAPI webhook (requires USE_WEBHOOK & FastAPI) ──────
app: FastAPI | None = None
if USE_WEBHOOK and FastAPI:
    app = FastAPI()
    @app.post("/sms-webhook")
    async def sms_webhook(req: Request):
        data = await req.json()
        LOG.info("Inbound SMS webhook payload: %s", data)
        _handle_inbound(data.get("from",""), data.get("reference",""), data.get("timestamp"))
        return {"status": "ok"}

# ───────────────────── core row processor (unchanged) ────────────
def _expand_row(l: List[str], n: int = MIN_COLS) -> List[str]:
    return l + [""] * (n - len(l)) if len(l) < n else l

def process_rows(rows: List[Dict[str, Any]]):
    for r in rows:
        txt = (r.get("description", "") + " " + r.get("openai_summary", "")).strip()
        if not is_short_sale(txt):
            LOG.debug("SKIP non-short-sale %s (%s)", r.get("street"), r.get("zpid"))
            continue
        zpid = str(r.get("zpid", ""))
        if zpid and not is_active_listing(zpid):
            LOG.info("Skip stale/off-market zpid %s", zpid)
            continue
        name = r.get("agentName", "").strip() or extract_name(txt)
        if not name or TEAM_RE.search(name):
            continue
        state = r.get("state", "")
        phone = fmt_phone(lookup_phone(name, state, r))
        email = lookup_email(name, state, r)
        if phone and phone_exists(phone):
            continue
        first, *last = name.split()
        now_iso = datetime.now(tz=TZ).isoformat()
        row_vals = [""] * MIN_COLS
        row_vals[COL_FIRST]       = first
        row_vals[COL_LAST]        = " ".join(last)
        row_vals[COL_PHONE]       = phone
        row_vals[COL_EMAIL]       = email
        row_vals[COL_STREET]      = r.get("street", "")
        row_vals[COL_CITY]        = r.get("city", "")
        row_vals[COL_STATE]       = state
        row_vals[COL_INIT_TS]     = now_iso
        row_idx = append_row(row_vals)
        if phone:
            seen_phones.add(phone)
            send_sms(phone, first, r.get("street", ""), row_idx)

# ───────────────────── main entry point ────────────────────────────
if __name__ == "__main__":
    # When running as a script (e.g. cron or Cloud Run job) we still want to
    # allow piping a JSON payload directly to stdin for ad‑hoc processing.
    try:
        stdin_txt = sys.stdin.read().strip()
        payload = json.loads(stdin_txt) if stdin_txt else None
    except json.JSONDecodeError:
        payload = None
    if payload and payload.get("listings"):
        LOG.debug("Sample fields on first fresh row: %s", list(payload["listings"][0].keys()))
        process_rows(payload["listings"])
    else:
        LOG.info("No JSON payload detected; scheduler & poller threads remain alive.")

