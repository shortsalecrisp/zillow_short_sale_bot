# === bot_min.py (June 2025 → hardened + direct‑phone check + filters 20‑Jun patch + thread updates) ===
#
#  – Uses Rapid‑API property endpoint FIRST and logs any quota errors
#  – Skips LinkedIn / Realtor scrapes (too aggressive blocking)
#  – Lower Google concurrency + jitter to avoid burst bans
#  – Captures phones already in the Apify payload (contact_recipients / listed_by)
#  – Loosened proximity‑scanner label test to catch bare numbers
#  – Adds early‑exit if RAPID_KEY is missing
#  – **17 Jun**  Zillow / Rapid‑API phone is accepted only if a quick Google check
#                shows it is a direct/mobile line (not office)
#
#  – **19 Jun quality patch**
#        • Tight ALLOWED_SITES list and domain gate
#        • Google query builders restricted to ALLOWED_SITES (no wildcards)
#        • Fetch hard‑blocks any domain outside ALLOWED_SITES and applies a
#          10‑min TTL cache after 403/429 to stop hammering portals
#        • Rapid‑API phone fallback when there’s a single phone even if
#          display‑name mismatch, and support for a “phones” array
#
#  – **20 Jun fixes**
#        • _phone_obj_to_str rebuilt: correctly assembles numbers when Zillow
#          returns the area code, prefix, and line as separate keys
#
#  – **Thread updates (21 Jun)**
#        • `zillow.com` removed from all search/allow lists
#        • `redfin.com` **added back** plus the brokerage list promoted to
#          ALLOWED_SITES
#        • Rapid‑API **e‑mail** extraction helper added and wired into
#          `lookup_email`
#        • `lookup_email` now receives the row payload
# --------------------------------------------------------------------------------------

import os, sys, json, logging, re, time, html, requests, concurrent.futures, random
from collections import defaultdict, Counter
from datetime import datetime
from typing import Any, Dict, List, Tuple
from urllib.parse import urlparse

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
CS_API_KEY = os.environ["CS_API_KEY"]
CS_CX      = os.environ["CS_CX"]
GSHEET_ID  = os.environ["GSHEET_ID"]
SC_JSON    = json.loads(os.environ["GCP_SERVICE_ACCOUNT_JSON"])
SCOPES     = ["https://www.googleapis.com/auth/spreadsheets"]

RAPID_KEY  = os.getenv("RAPID_KEY", "").strip()
RAPID_HOST = os.getenv("RAPID_HOST", "zillow-com1.p.rapidapi.com").strip()
GOOD_STATUS = {"FOR_SALE", "ACTIVE", "COMING_SOON", "PENDING", "NEW_CONSTRUCTION"}

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
SMS_API_KEY     = os.getenv("SMSM_API_KEY", "")            # ← uses SMSM_API_KEY only
SMS_FROM        = os.getenv("SMSM_FROM", "")
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
MAX_Q_EMAIL = 5

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

# ───────────────────── extra globals ───────────────────────────────
OFFICE_HINTS = {"office", "main", "fax", "team", "brokerage", "corporate"}

creds = Credentials.from_service_account_info(SC_JSON, scopes=SCOPES)
sheets_service = build("sheets", "v4", credentials=creds, cache_discovery=False)
gc = gspread.authorize(creds)
ws = gc.open_by_key(GSHEET_ID).sheet1

# ───────────────────────── SITE LISTS ──────────────────────────────
AGENT_SITES = [                       # zillow.com removed, redfin.com added
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

DOMAIN_CLAUSE    = " OR ".join(f"site:{d}" for d in SCRAPE_SITES)
BROKERAGE_CLAUSE = " OR ".join(f"site:{d}" for d in BROKERAGE_SITES)
SOCIAL_CLAUSE    = "site:facebook.com OR site:linkedin.com"

# ────────────────────── allowed domains & cache ────────────────────
ALLOWED_SITES = {                     # union of agent + brokerage
    "redfin.com", "homesnap.com", "kw.com", "remax.com", "coldwellbanker.com",
    "compass.com", "exprealty.com", "bhhs.com", "c21.com", "realtyonegroup.com",
    "mlsmatrix.com", "mlslistings.com", "har.com", "brightmlshomes.com",
    "exitrealty.com", "realtyexecutives.com", "realty.com",
    "sothebysrealty.com", "corcoran.com", "douglaselliman.com",
    "cryereleike.com", "windermere.com", "longandfoster.com"
}

_blocked_until: Dict[str, float] = {}  # domain → epoch seconds

def _domain(host_or_url: str) -> str:
    host = urlparse(host_or_url).netloc or host_or_url
    parts = host.lower().split(".")
    return ".".join(parts[-2:]) if len(parts) >= 2 else host.lower()

def _should_fetch(url: str) -> bool:
    dom = _domain(url)
    if dom in _blocked_until and _blocked_until[dom] > time.time():
        return False
    return dom in ALLOWED_SITES

cache_p, cache_e = {}, {}
_executor = concurrent.futures.ThreadPoolExecutor(max_workers=GOOGLE_CONCURRENCY)

# ─────────────────── Rapid‑API helpers ─────────────────────────────
def _phone_obj_to_str(obj: Dict[str, str]) -> str:
    if not obj:
        return ""
    key_order = [
        "areacode", "area_code", "areaCode",
        "prefix", "centralofficecode", "central_office_code", "centralOfficeCode",
        "number", "line", "line_number", "lineNumber"
    ]
    parts: List[str] = []
    for k in key_order:
        if k in obj and obj[k]:
            parts.append(re.sub(r"\D", "", str(obj[k])))
    if not parts:
        for v in obj.values():
            chunk = re.sub(r"\D", "", str(v))
            if 2 <= len(chunk) <= 4:
                parts.append(chunk)
    digits = "".join(parts)[:10]
    return fmt_phone(digits)

def rapid_property(zpid: str) -> Dict[str, Any]:
    if not RAPID_KEY:
        return {}
    try:
        headers = {"X-RapidAPI-Key": RAPID_KEY, "X-RapidAPI-Host": RAPID_HOST}
        r = requests.get(f"https://{RAPID_HOST}/property",
                         params={"zpid": zpid}, headers=headers, timeout=15)
        if r.status_code == 429:
            LOG.error("Rapid‑API quota exhausted (HTTP 429)")
            return {}
        r.raise_for_status()
        return r.json().get("data") or r.json()
    except Exception as exc:
        LOG.debug("Rapid‑API fetch error %s for zpid=%s", exc, zpid)
        return {}

def _phones_from_block(blk: Dict[str, Any]) -> List[str]:
    out = []
    if not blk:
        return out
    if blk.get("phone"):
        out.append(_phone_obj_to_str(blk["phone"]))
    for ph in blk.get("phones", []):
        out.append(_phone_obj_to_str(ph))
    return [p for p in out if p]

# NEW – e‑mail counterparts
def _emails_from_block(blk: Dict[str, Any]) -> List[str]:
    if not blk:
        return []
    out = []
    for key in ("email", "emailAddress"):
        if blk.get(key):
            out.append(clean_email(blk[key]))
    for e in blk.get("emails", []):
        out.append(clean_email(e))
    return [e for e in out if ok_email(e)]

def rapid_phone(zpid: str, agent_name: str) -> Tuple[str, str]:
    data = rapid_property(zpid)
    if not data:
        return "", ""

    cand, all_phones = [], set()

    for blk in data.get("contact_recipients", []):
        for pn in _phones_from_block(blk):
            all_phones.add(pn)
            if pn and agent_name.lower() in blk.get("display_name", "").lower():
                cand.append(("rapid:contact_recipients", pn))

    lb = data.get("listed_by", {})
    for pn in _phones_from_block(lb):
        all_phones.add(pn)
        if pn and agent_name.lower() in lb.get("display_name", "").lower():
            cand.append(("rapid:listed_by", pn))

    if cand:
        return cand[0][1], cand[0][0]
    if len(all_phones) == 1:
        return next(iter(all_phones)), "rapid:fallback_single"
    return "", ""

def rapid_email(zpid: str, agent_name: str) -> Tuple[str, str]:
    data = rapid_property(zpid)
    if not data:
        return "", ""
    cand, all_em = [], set()

    for blk in data.get("contact_recipients", []):
        for em in _emails_from_block(blk):
            all_em.add(em)
            if agent_name.lower() in blk.get("display_name", "").lower():
                cand.append(("rapid:contact_recipients", em))

    lb = data.get("listed_by", {})
    for em in _emails_from_block(lb):
        all_em.add(em)
        if agent_name.lower() in lb.get("display_name", "").lower():
            cand.append(("rapid:listed_by", em))

    if cand:
        return cand[0][1], cand[0][0]
    if len(all_em) == 1:
        return next(iter(all_em)), "rapid:fallback_single"
    return "", ""

# ─────────────────── Google helper for directness ──────────────────
def _looks_direct(phone: str, agent: str, state: str, tries: int = 2) -> bool:
    if not phone:
        return False
    last = agent.split()[-1].lower()
    q = f'"{phone}" {state}'
    for _ in range(tries):
        for it in google_items(q, tries=1):
            snip = (it.get("snippet") or "").lower()
            if (last in snip or agent.lower() in snip) and not any(k in snip for k in OFFICE_HINTS):
                return True
        q = f'"{phone}" "{agent.split()[0]}"'
    return False

# ─────────────────── query builders (Google) ───────────────────────
def _name_tokens(name: str) -> List[str]:
    return [t for t in re.split(r"\s+", name.strip()) if len(t) > 1]

def build_q_phone(name: str, state: str) -> List[str]:
    tokens = " ".join(_name_tokens(name))
    base   = f"{tokens} {state} realtor phone number"
    return [f"{base} site:{d}" for d in random.sample(sorted(ALLOWED_SITES), 4)]

def build_q_email(name: str, state: str) -> List[str]:
    tokens = " ".join(_name_tokens(name))
    base   = f"{tokens} {state} realtor email address"
    return [f"{base} site:{d}" for d in random.sample(sorted(ALLOWED_SITES), 4)]

# ────────────────────────── UTILITIES ──────────────────────────────
def is_short_sale(t):     return SHORT_RE.search(t) and not BAD_RE.search(t)

def fmt_phone(r):
    d = re.sub(r"\D", "", r)
    if len(d) == 11 and d.startswith("1"):
        d = d[1:]
    return f"{d[:3]}-{d[3:6]}-{d[6:]}" if len(d) == 10 else ""

def valid_phone(p):
    if phonenumbers:
        try:
            return phonenumbers.is_possible_number(phonenumbers.parse(p, "US"))
        except Exception:
            return False
    return re.fullmatch(r"\d{3}-\d{3}-\d{4}", p) and p[:3] in US_AREA_CODES

def clean_email(e):   return e.split("?")[0].strip()

def ok_email(e):
    e = clean_email(e)
    return e and "@" in e and not e.lower().endswith(IMG_EXT) and not re.search(r"\.(gov|edu|mil)$", e, re.I)

# ────────────────────────── fetch helpers ──────────────────────────
def _jitter():          time.sleep(random.uniform(0.8, 1.5))
def _mark_block(dom):   _blocked_until[dom] = time.time() + 600   # 10 min

def fetch_simple(u):
    if not _should_fetch(u):
        return None
    dom = _domain(u)
    try:
        r = requests.get(u, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code == 200:
            return r.text
        if r.status_code in (403, 429):
            _mark_block(dom)
    except Exception as exc:
        LOG.debug("fetch_simple error %s on %s", exc, u)
    return None

def fetch(u):
    if not _should_fetch(u):
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
                METRICS["fetch_403"] += 1
                if z403 >= MAX_ZILLOW_403:
                    LOG.debug("bailing after %s consecutive Zillow 403s", z403)
                    return None
                _mark_block(dom)
            elif r.status_code == 429:
                ratelimit += 1
                METRICS["fetch_429"] += 1
                if ratelimit >= MAX_RATE_429:
                    LOG.debug("rate‑limit ceiling hit, giving up (%s)", url)
                    _mark_block(dom)
                    return None
            else:
                METRICS["fetch_other_%s" % r.status_code] += 1

            _jitter()
            time.sleep(min(backoff, MAX_BACKOFF_SECONDS))
            backoff *= BACKOFF_FACTOR
    return None

# ─────────────────── Google CSE helper ─────────────────────────────
def google_items(q, tries=3):
    backoff = 1.0
    for _ in range(tries):
        try:
            j = requests.get(
                "https://www.googleapis.com/customsearch/v1",
                params={"key": CS_API_KEY, "cx": CS_CX, "q": q, "num": 10},
                timeout=10
            ).json()
            return j.get("items", [])
        except Exception:
            time.sleep(min(backoff, MAX_BACKOFF_SECONDS))
            backoff *= BACKOFF_FACTOR
    return []

# ─────────────────── proximity scan & structured ───────────────────
def proximity_scan(t, last_name=None):
    out = {}
    for m in PHONE_RE.finditer(t):
        p = fmt_phone(m.group())
        if not valid_phone(p):
            continue
        sn_start = max(m.start() - 50, 0)
        sn_end   = min(m.end() + 50, len(t))
        snippet  = t[sn_start:sn_end]
        if last_name and last_name.lower() not in snippet:
            continue
        lab_match = LABEL_RE.search(snippet)
        lab = lab_match.group().lower() if lab_match else ""
        w = LABEL_TABLE.get(lab, 0)
        if w < 1:
            continue
        bw, ts, _ = out.get(p, (0, 0, False))
        out[p] = (max(bw, w), ts + 2 + w, lab in ("office", "main"))
    return out

def extract_struct(td):
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
        if isinstance(data, dict):
            tel  = data.get("telephone") or (data.get("contactPoint") or {}).get("telephone")
            mail = data.get("email")     or (data.get("contactPoint") or {}).get("email")
            if tel:
                phones.append(fmt_phone(tel))
            if mail:
                mails.append(clean_email(mail))
    for a in soup.select('a[href^="tel:"]'):
        phones.append(fmt_phone(a["href"].split("tel:")[-1]))
    for a in soup.select('a[href^="mailto:"]'):
        mails.append(clean_email(a["href"].split("mailto:")[-1]))
    return phones, mails

# ───────────────────── parallel map helper ─────────────────────────
def pmap(fn, iterable): return list(_executor.map(fn, iterable))

# ───────────────────── lookup functions ────────────────────────────
def _split_portals(urls):
    portals, non = [], []
    for u in urls:
        if any(d in u for d in SCRAPE_SITES):
            portals.append(u)
        else:
            non.append(u)
    return non, portals

def lookup_phone(agent: str, state: str, row_payload: Dict[str, Any]) -> str:
    key = f"{agent}|{state}"
    if key in cache_p:
        return cache_p[key]

    # 0️⃣  Phones already in Apify payload
    for blk in (row_payload.get("contact_recipients") or []):
        for p in _phones_from_block(blk):
            d = fmt_phone(p)
            if d and valid_phone(d):
                cache_p[key] = d
                LOG.debug("PHONE hit directly from contact_recipients")
                return d

    # 1️⃣  Rapid‑API
    phone = ""
    zpid = str(row_payload.get("zpid", ""))
    if zpid:
        phone, src_tag = rapid_phone(zpid, agent)
        if phone and _looks_direct(phone, agent, state):
            cache_p[key] = phone
            LOG.debug("PHONE WIN %s via %s (verified direct)", phone, src_tag)
            return phone
        phone = ""   # discard unverified / office

    cand_good, cand_office, src_good = {}, {}, {}

    def add(p, score, office_flag, src=""):
        d = fmt_phone(p)
        if not valid_phone(d):
            return
        target = cand_office if office_flag else cand_good
        target[d] = target.get(d, 0) + score
        if not office_flag and src:
            src_good[d] = src

    # 2️⃣  Google metadata
    queries = build_q_phone(agent, state)[:MAX_Q_PHONE]
    google_batches = pmap(google_items, queries)
    for items in google_batches:
        for it in items:
            tel = it.get("pagemap", {}).get("contactpoint", [{}])[0].get("telephone")
            if tel:
                add(tel, 4, False, f"CSE:{it.get('link', '')}")

    if cand_good:
        phone = max(cand_good, key=cand_good.get)
        cache_p[key] = phone
        LOG.debug("PHONE WIN %s via %s", phone, src_good.get(phone, "CSE‑json"))
        return phone

    # 3️⃣  Crawl (same logic as before) …
    urls_all = [it.get("link", "") for items in google_batches for it in items][:30]
    non_portal_urls, portal_urls = _split_portals(urls_all)
    last_name = (agent.split()[-1] if len(agent.split()) > 1 else agent).lower()

    # 3a  direct pages fast
    pages = pmap(fetch_simple, non_portal_urls)
    for url, page in zip(non_portal_urls, pages):
        if not page or agent.lower() not in page.lower():
            continue
        ph, _ = extract_struct(page)
        for p in ph:
            add(p, 6, False, url)
        low = html.unescape(page.lower())
        for p, (bw, sc, office_flag) in proximity_scan(low, last_name).items():
            add(p, sc, office_flag, url)
        if cand_good or cand_office:
            break

    # 3b  jina.ai fallback
    if not cand_good and not cand_office:
        pages = pmap(fetch, non_portal_urls)
        for url, page in zip(non_portal_urls, pages):
            if not page or agent.lower() not in page.lower():
                continue
            ph, _ = extract_struct(page)
            for p in ph:
                add(p, 5, False, url)
            low = html.unescape(page.lower())
            for p, (bw, sc, office_flag) in proximity_scan(low, last_name).items():
                add(p, sc, office_flag, url)
            if cand_good or cand_office:
                break

    # 3c  portals
    if not cand_good:
        pages = pmap(fetch, portal_urls)
        for url, page in zip(portal_urls, pages):
            if not page or agent.lower() not in page.lower():
                continue
            ph, _ = extract_struct(page)
            for p in ph:
                add(p, 4, False, url)
            low = html.unescape(page.lower())
            for p, (bw, sc, office_flag) in proximity_scan(low, last_name).items():
                add(p, sc, office_flag, url)
            if cand_good:
                break

    phone = ""
    if cand_good:
        phone = max(cand_good, key=cand_good.get)
    elif cand_office:
        phone = ""

    if phone:
        LOG.debug("PHONE WIN %s via %s", phone, src_good.get(phone, "crawler"))
    else:
        LOG.debug("PHONE FAIL for %s %s  cand_good=%s cand_office=%s",
                  agent, state, cand_good, cand_office)

    cache_p[key] = phone
    return phone


def lookup_email(agent: str, state: str, row_payload: Dict[str, Any]) -> str:
    key = f"{agent}|{state}"
    if key in cache_e:
        return cache_e[key]

    # 0️⃣  already in payload?
    for blk in (row_payload.get("contact_recipients") or []):
        for em in _emails_from_block(blk):
            cache_e[key] = em
            LOG.debug("EMAIL hit directly from contact_recipients")
            return em

    # 1️⃣  Rapid‑API
    zpid = str(row_payload.get("zpid", ""))
    if zpid:
        em, src = rapid_email(zpid, agent)
        if em:
            cache_e[key] = em
            LOG.debug("EMAIL WIN %s via %s", em, src)
            return em

    # 2️⃣  Google / crawl  (original logic ‑ unchanged except for arg order)
    cand, src_e = defaultdict(int), {}

    queries = build_q_email(agent, state)[:MAX_Q_EMAIL]
    google_batches = pmap(google_items, queries)

    for items in google_batches:
        for it in items:
            mail = clean_email(it.get("pagemap", {}).get("contactpoint", [{}])[0].get("email", ""))
            if ok_email(mail):
                cand[mail] += 3
                src_e.setdefault(mail, f"CSE:{it.get('link', '')}")

    if cand:
        email = max(cand, key=cand.get)
        cache_e[key] = email
        LOG.debug("EMAIL WIN %s via %s", email, src_e.get(email, "CSE‑json"))
        return email

    urls_all = [it.get("link", "") for items in google_batches for it in items][:30]
    non_portal_urls, portal_urls = _split_portals(urls_all)

    # crawl non‑portal
    for url, page in zip(non_portal_urls, pmap(fetch_simple, non_portal_urls)):
        if not page or agent.lower() not in page.lower():
            continue
        _, ems = extract_struct(page)
        for m in ems:
            m = clean_email(m)
            if ok_email(m):
                cand[m] += 3
                src_e.setdefault(m, url)
        for m in EMAIL_RE.findall(page):
            m = clean_email(m)
            if ok_email(m):
                cand[m] += 1
                src_e.setdefault(m, url)
        if cand:
            break

    if not cand:
        for url, page in zip(non_portal_urls, pmap(fetch, non_portal_urls)):
            if not page or agent.lower() not in page.lower():
                continue
            _, ems = extract_struct(page)
            for m in ems:
                m = clean_email(m)
                if ok_email(m):
                    cand[m] += 2
                    src_e.setdefault(m, url)
            for m in EMAIL_RE.findall(page):
                m = clean_email(m)
                if ok_email(m):
                    cand[m] += 1
                    src_e.setdefault(m, url)
            if cand:
                break

    if not cand:
        for url, page in zip(portal_urls, pmap(fetch, portal_urls)):
            if not page or agent.lower() not in page.lower():
                continue
            _, ems = extract_struct(page)
            for m in ems:
                m = clean_email(m)
                if ok_email(m):
                    cand[m] += 2
                    src_e.setdefault(m, url)
            if cand:
                break

    tokens = {re.sub(r"[^a-z]", "", w.lower()) for w in agent.split()}
    good = {m: sc for m, sc in cand.items()
            if any(tok and tok in m.lower() for tok in tokens)}

    email = max(good, key=good.get) if good else (max(cand, key=cand.get) if cand else "")

    if email:
        LOG.debug("EMAIL WIN %s via %s", email, src_e.get(email, "crawler"))
    else:
        LOG.debug("EMAIL FAIL for %s %s  tokens=%s  candidates=%s",
                  agent, state, tokens, dict(list(cand.items())[:8]))

    cache_e[key] = email
    return email

# ───────────────────── Google Sheet helpers ────────────────────────
def mark_sent(row_idx: int):
    try:
        sheets_service.spreadsheets().values().update(
            spreadsheetId=GSHEET_ID,
            range=f"Sheet1!H{row_idx}:H{row_idx}",
            valueInputOption="RAW",
            body={"values": [["x"]]}
        ).execute()
        LOG.debug("Marked row %s column H as sent", row_idx)
    except Exception as e:
        LOG.error("GSheet mark_sent error %s", e)

def append_row(values):
    resp = sheets_service.spreadsheets().values().append(
        spreadsheetId=GSHEET_ID,
        range="Sheet1!A1",
        valueInputOption="RAW",
        body={"values": [values]}
    ).execute()
    row_idx = int(resp["updates"]["updatedRange"].split("!")[1].split(":")[0][1:])
    LOG.info("Row appended to sheet (row %s)", row_idx)
    return row_idx

def phone_exists(p):
    try:
        return p in ws.col_values(3)
    except Exception:
        return False

# ─────────────────── Zillow status helper ──────────────────────────
def is_active_listing(zpid):
    if not RAPID_KEY:
        return True
    try:
        r = requests.get(
            f"https://{RAPID_HOST}/property",
            params={"zpid": zpid},
            headers={"X-RapidAPI-Key": RAPID_KEY, "X-RapidAPI-Host": RAPID_HOST},
            timeout=15
        )
        r.raise_for_status()
        data = r.json().get("data") or r.json()
        status = (data.get("homeStatus") or "").upper()
        return (not status) or status in GOOD_STATUS
    except Exception as e:
        LOG.warning("Rapid‑API status check failed for %s (%s) – keeping row", zpid, e)
        return True

# ───────────────────── scrape helpers ──────────────────────────────
def extract_name(t):
    m = re.search(r"listing agent[:\s\-]*([A-Za-z \.'’-]{3,})", t, re.I)
    if m:
        n = m.group(1).strip()
        if not TEAM_RE.search(n):
            return n
    return None

# ───────────────────── main row processor ──────────────────────────
def send_sms(phone, first, address, row_idx):
    if not SMS_ENABLE or not phone:
        return
    if SMS_TEST_MODE and SMS_TEST_NUMBER:
        phone = SMS_TEST_NUMBER
    try:
        resp = requests.post(SMS_URL, timeout=10, data={
            "api_key": SMS_API_KEY,
            "to": phone,
            "from": SMS_FROM,
            "text": SMS_TEMPLATE.format(first=first, address=address)
        })
        if resp.status_code == 200 and "OK" in resp.text.upper():
            mark_sent(row_idx)
            LOG.info("SMS sent to %s", phone)
        else:
            LOG.error("SMS API error %s %s", resp.status_code, resp.text[:200])
    except Exception as e:
        LOG.error("SMS send error %s", e)

def process_rows(rows):
    for r in rows:
        txt = (r.get("description", "") + " " + r.get("openai_summary", "")).strip()
        if not is_short_sale(txt):
            LOG.debug("SKIP non‑short‑sale %s (%s)", r.get("street"), r.get("zpid"))
            continue

        zpid = str(r.get("zpid", ""))
        if zpid and not is_active_listing(zpid):
            LOG.info("Skip stale/off‑market zpid %s", zpid)
            continue

        name = r.get("agentName", "").strip()
        if not name:
            name = extract_name(txt)
            if not name:
                continue
        if TEAM_RE.search(name):
            alt = extract_name(txt)
            if alt:
                name = alt
            else:
                continue
        if TEAM_RE.search(name):
            continue

        state = r.get("state", "")

        phone = fmt_phone(lookup_phone(name, state, r))
        email = lookup_email(name, state, r)

        if phone and phone_exists(phone):
            continue

        first, *last = name.split()
        row_idx = append_row([first, " ".join(last), phone, email,
                              r.get("street", ""), r.get("city", ""), state, ""])

        if phone:
            send_sms(phone, first, r.get("street", ""), row_idx)

# ——— main webhook entry ———
if __name__ == "__main__":
    try:
        payload = json.load(sys.stdin)
    except json.JSONDecodeError:
        LOG.debug("No stdin payload; exiting")
        sys.exit(0)

    fresh_rows = payload.get("listings", [])
    if not fresh_rows:
        LOG.info("No listings in payload")
        sys.exit(0)

    LOG.debug("Sample fields on first fresh row: %s", list(fresh_rows[0].keys()))
    process_rows(fresh_rows)

    if METRICS:
        LOG.info("metrics %s", dict(METRICS))

