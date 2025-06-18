# === bot_min.py  (June 2025 → direct-phone fix • skip “not bank” • block Realtor/Lnkd • SMS restored) ===

import os, sys, json, logging, re, time, html, random, requests, concurrent.futures
from collections import defaultdict, Counter
from typing import Any, Dict, List, Tuple

# ───────────────────────── optional libs ───────────────────────────
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


# ─────────────────────────── ENV / AUTH 
CS_API_KEY = os.environ["CS_API_KEY"]
CS_CX      = os.environ["CS_CX"]
GSHEET_ID  = os.environ["GSHEET_ID"]
SC_JSON    = json.loads(os.environ["GCP_SERVICE_ACCOUNT_JSON"])
SCOPES     = ["https://www.googleapis.com/auth/spreadsheets"]

# Rapid-API (optional)
RAPID_KEY  = os.getenv("RAPID_KEY", "").strip()
RAPID_HOST = os.getenv("RAPID_HOST", "zillow-com1.p.rapidapi.com").strip()
GOOD_STATUS = {"FOR_SALE", "ACTIVE", "COMING_SOON", "PENDING", "NEW_CONSTRUCTION"}

logging.basicConfig(level=os.getenv("LOGLEVEL", "DEBUG"),
                    format="%(asctime)s %(levelname)s: %(message)s")
LOG = logging.getLogger("bot")

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
SMS_FROM        = os.getenv("SMSM_FROM", "")
SMS_URL         = os.getenv("SMSM_URL", "https://api.smsmobileapi.com/sendsms/")
SMS_TEMPLATE    = (
    "Hey {first}, this is Yoni Kutler—I saw your short sale listing at {address} and "
    "wanted to introduce myself. I specialise in lender negotiations so you can focus "
    "on selling. No cost to you or your client—I’m only paid by the buyer at closing. "
    "Would you be open to a quick call to see if this could help?"
)

MAX_Q_PHONE = 5
MAX_Q_EMAIL = 5

# ──────────────────────────── REGEXES 
SHORT_RE = re.compile(r"\bshort\s+sale\b", re.I)
BAD_RE   = re.compile(
    r"\b(?:approved short sale|short sale approved|"
    r"not\s+a?\s*bank\s+short\s+sale|not\s+bank\s+short\s+sale)\b", re.I)

TEAM_RE  = re.compile(r"^\s*the\b|\bteam\b", re.I)

IMG_EXT  = (".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp")
PHONE_RE = re.compile(r"(?<!\d)(?:\+?1[\s\-\.]*)?\(?\d{3}\)?[\s\-\.]*\d{3}[\s\-\.]*\d{4}(?!\d)")
EMAIL_RE = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.I)

LABEL_TABLE = {"mobile":4,"cell":4,"direct":4,"text":4,"c:":4,"m:":4,
               "phone":2,"tel":2,"p:":2,"office":1,"main":1,"customer":1,"footer":1}
LABEL_RE = re.compile("(" + "|".join(map(re.escape, LABEL_TABLE)) + ")", re.I)
US_AREA_CODES = {str(i) for i in range(201, 990)}

# ─────────────────────────── SHEETS 
creds = Credentials.from_service_account_info(SC_JSON, scopes=SCOPES)
sheets_service = build("sheets", "v4", credentials=creds, cache_discovery=False)
gc = gspread.authorize(creds)
ws = gc.open_by_key(GSHEET_ID).sheet1

# ───────────────────────── SITE LISTS 
AGENT_SITES = [
    "realtor.com", "zillow.com", "redfin.com", "homesnap.com", "kw.com",
    "remax.com", "coldwellbanker.com", "compass.com", "exprealty.com",
    "bhhs.com", "c21.com", "realtyonegroup.com", "mlsmatrix.com",
    "mlslistings.com", "har.com", "brightmlshomes.com",
    "exitrealty.com", "realtyexecutives.com", "realty.com"
]
SCRAPE_SITES = [d for d in AGENT_SITES if d not in ("linkedin.com", "realtor.com")]

BROKERAGE_SITES = [
    "sothebysrealty.com", "corcoran.com", "douglaselliman.com",
    "cryereleike.com", "windermere.com", "longandfoster.com"
]
DOMAIN_CLAUSE    = " OR ".join(f"site:{d}" for d in SCRAPE_SITES)
BROKERAGE_CLAUSE = " OR ".join(f"site:{d}" for d in BROKERAGE_SITES)

OFFICE_HINTS = {"office", "main", "fax", "team", "brokerage", "corporate"}

cache_p, cache_e = {}, {}
_executor = concurrent.futures.ThreadPoolExecutor(max_workers=GOOGLE_CONCURRENCY)

# ─────────────────── Rapid-API helpers ─────────────────────────────
def fmt_phone(r: str) -> str:
    d = re.sub(r"\D", "", r)
    d = d[1:] if len(d) == 11 and d.startswith("1") else d
    return f"{d[:3]}-{d[3:6]}-{d[6:]}" if len(d) == 10 else ""

def _phone_obj_to_str(obj: Dict[str, str]) -> str:
    return fmt_phone("".join(obj.get(k, "") for k in ("areacode", "prefix", "number")))

def rapid_property(zpid: str) -> Dict[str, Any]:
    if not RAPID_KEY:
        return {}
    try:
        headers = {"X-RapidAPI-Key": RAPID_KEY, "X-RapidAPI-Host": RAPID_HOST}
        r = requests.get(f"https://{RAPID_HOST}/property",
                         params={"zpid": zpid}, headers=headers, timeout=15)
        if r.status_code == 429:
            LOG.error("Rapid-API quota exhausted (HTTP 429).")
            return {}
        r.raise_for_status()
        return r.json().get("data") or r.json()
    except Exception as exc:
        LOG.debug("Rapid-API fetch error %s for zpid=%s", exc, zpid)
        return {}

def rapid_phone(zpid: str, agent_name: str) -> Tuple[str, str]:
    data = rapid_property(zpid)
    if not data:
        return "",""

    agent_low = agent_name.lower()
    last_low  = agent_name.split()[-1].lower()
    _match = lambda n: agent_low in (n or "").lower() or last_low in (n or "").lower()

    cand: List[Tuple[str,str]] = []
    for blk in data.get("contact_recipients", []):
        pn = _phone_obj_to_str(blk.get("phone", {}))
        if pn and _match(blk.get("display_name", "")):
            cand.append(("rapid:contact_recipients", pn))
    lb = data.get("listed_by", {})
    pn = _phone_obj_to_str(lb.get("phone", {}))
    if pn and _match(lb.get("display_name", "")):
        cand.append(("rapid:listed_by", pn))

    return (cand[0][1], cand[0][0]) if cand else ("","")

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

# ─────────────────── Google helpers ────────────────────────────────
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

def _name_tokens(name: str) -> List[str]:
    return [t for t in re.split(r"\s+", name.strip()) if len(t) > 1]

def build_q_phone(name: str, state: str) -> List[str]:
    tokens = _name_tokens(name)
    base   = " ".join(tokens) + f" {state} realtor phone number"
    portals = ["zillow.com", "kw.com", "redfin.com", "homesnap.com"]
    random.shuffle(portals)
    return [f"{base} site:{p}" for p in portals[:4]] + [base]

def build_q_email(name: str, state: str) -> List[str]:
    tokens = _name_tokens(name)
    base   = " ".join(tokens) + f" {state} realtor email address"
    portals = ["kw.com", "facebook.com"]
    return [f"{base} site:{p}" for p in portals[:4]] + [base]

# ─────────────────────────── MISC UTILS 
def valid_phone(p):
    if phonenumbers:
        try:
            return phonenumbers.is_possible_number(phonenumbers.parse(p, "US"))
        except Exception:
            return False
    return re.fullmatch(r"\d{3}-\d{3}-\d{4}", p) and p[:3] in US_AREA_CODES

def clean_email(e): return e.split("?")[0].strip()

def ok_email(e):
    e = clean_email(e)
    return e and "@" in e and not e.lower().endswith(IMG_EXT) and not re.search(r"\.(gov|edu|mil)$", e, re.I)

def is_short_sale(t): return SHORT_RE.search(t) and not BAD_RE.search(t)

# ────────────────────────── fetch helpers ──────────────────────────
def fetch(u: str) -> str | None:
    if "linkedin.com" in u or "realtor.com" in u:
        METRICS["fetch_skip_blocked"] += 1
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
                    LOG.debug("bailing after %s consecutive Zillow 403s", z403)
                    return None
            elif r.status_code == 429:
                ratelimit += 1
                METRICS["fetch_429"] += 1
                if ratelimit >= MAX_RATE_429:
                    LOG.debug("rate-limit ceiling hit, giving up (%s)", url)
                    return None
            else:
                METRICS["fetch_other_%s" % r.status_code] += 1

            time.sleep(min(backoff, MAX_BACKOFF_SECONDS))
            backoff *= BACKOFF_FACTOR
    return None

def pmap(fn, iterable):
    return list(_executor.map(fn, iterable))

# ───────────────── proximity scan & struct extract ─────────────────
def proximity_scan(t):
    out = {}
    for m in PHONE_RE.finditer(t):
        p = fmt_phone(m.group())
        if not valid_phone(p):
            continue
        sn = t[max(m.start()-80, 0):min(m.end()+80, len(t))]
        lab_match = LABEL_RE.search(sn)
        lab = lab_match.group().lower() if lab_match else ""
        w = LABEL_TABLE.get(lab, 0)
        if w < 2:
            continue
        bw, ts, office = out.get(p, (0, 0, False))
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

# ───────────────────── Google Sheet helpers ────────────────────────
def mark_sent(row_idx: int):
    try:
        sheets_service.spreadsheets().values().update(
            spreadsheetId=GSHEET_ID,
            range=f"Sheet1!H{row_idx}:H{row_idx}",
            valueInputOption="RAW",
            body={"values": [["x"]]}
        ).execute()
        LOG.debug("Marked row %s column H as sent", row_idx)
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
    LOG.info("Row appended to sheet (row %s)", row_idx)
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
        LOG.warning("RapidAPI status check failed for %s (%s) – keeping row", zpid, e)
        return True

# ───────────────────── lookup functions ────────────────────────────
def lookup_phone(agent: str, state: str, row_payload: Dict[str,Any]) -> str:
    key = f"{agent}|{state}"
    if key in cache_p:
        return cache_p[key]

    # 0️⃣ phone inside incoming payload?
    for blk in (row_payload.get("contact_recipients") or []):
        p = _phone_obj_to_str((blk or {}).get("phone", {}))
        if p and valid_phone(p):
            cache_p[key] = p
            return p

    # 1️⃣ Rapid-API
    phone = ""
    zpid = str(row_payload.get("zpid",""))
    if zpid:
        phone, src = rapid_phone(zpid, agent)
        if phone and _looks_direct(phone, agent, state):
            LOG.debug("PHONE WIN %s via %s (verified direct)", phone, src)
            cache_p[key] = phone
            return phone
        phone = ""

    cand_good, cand_office = {}, {}
    src_good = {}

    def add(p, score, office_flag, src=""):
        d = fmt_phone(p)
        if not valid_phone(d):
            return
        (cand_office if office_flag else cand_good)[d] = score + (cand_good.get(d,0) if not office_flag else 0)
        if not office_flag and src:
            src_good[d] = src

    # 2️⃣ Google fast queries
    queries = build_q_phone(agent, state)[:MAX_Q_PHONE]
    google_batches = pmap(google_items, queries)
    for items in google_batches:
        for it in items:
            tel = it.get("pagemap", {}).get("contactpoint", [{}])[0].get("telephone")
            if tel:
                add(tel, 4, False, f"CSE:{it.get('link','')}")

    if cand_good:
        phone = max(cand_good, key=cand_good.get)
        cache_p[key] = phone
        LOG.debug("PHONE WIN %s via %s", phone, src_good.get(phone, "CSE-json"))
        return phone

    # 3️⃣ Crawl result links
    urls_all = [it.get("link", "") for items in google_batches for it in items][:30]
    pages = pmap(fetch, urls_all)
    for url, page in zip(urls_all, pages):
        if not page or agent.lower() not in page.lower():
            continue
        ph, _ = extract_struct(page)
        for p in ph:
            add(p, 6, False, url)
        low = html.unescape(page.lower())
        for p, (bw, sc, office_flag) in proximity_scan(low).items():
            add(p, sc, office_flag, url)

    phone = max(cand_good, key=cand_good.get) if cand_good else ""
    if phone:
        LOG.debug("PHONE WIN %s via %s", phone, src_good.get(phone, "crawler"))
    else:
        LOG.debug("PHONE FAIL for %s %s", agent, state)
    cache_p[key] = phone
    return phone

def lookup_email(agent, state):
    key = f"{agent}|{state}"
    if key in cache_e:
        return cache_e[key]
    cand, src_e = defaultdict(int), {}

    queries = build_q_email(agent, state)[:MAX_Q_EMAIL]
    google_batches = pmap(google_items, queries)
    for items in google_batches:
        for it in items:
            mail = clean_email(it.get("pagemap", {}).get("contactpoint", [{}])[0].get("email", ""))
            if ok_email(mail):
                cand[mail] += 3
                src_e.setdefault(mail, f"CSE:{it.get('link','')}")

    urls = [it.get("link", "") for items in google_batches for it in items][:30]
    pages = pmap(fetch, urls)
    for url, page in zip(urls, pages):
        if not page or agent.lower() not in page.lower():
            continue
        _, em = extract_struct(page)
        for m in em + EMAIL_RE.findall(page):
            m = clean_email(m)
            if ok_email(m):
                cand[m] += 2
                src_e.setdefault(m, url)

    tokens = {re.sub(r"[^a-z]", "", w.lower()) for w in agent.split()}
    good = {m: sc for m, sc in cand.items()
            if any(tok and tok in m.lower() for tok in tokens)}
    email = max(good, key=good.get) if good else (max(cand, key=cand.get) if cand else "")

    if email:
        LOG.debug("EMAIL WIN %s via %s", email, src_e.get(email, "crawler"))
    else:
        LOG.debug("EMAIL FAIL for %s %s", agent, state)
    cache_e[key] = email
    return email

# ─────────────────────── SMS (working, row-marking) ────────────────
def send_sms(phone: str, first: str, address: str, row_idx: int) -> None:
    if not SMS_ENABLE or not SMS_API_KEY:
        return
    digits = re.sub(r"\D", "", phone)
    if len(digits) == 10:
        to_e164 = "+1" + digits
    elif len(digits) == 11 and digits.startswith("1"):
        to_e164 = "+" + digits
    else:
        LOG.debug("send_sms: invalid digits %s", phone)
        return

    if SMS_TEST_MODE and SMS_TEST_NUMBER:
        td = re.sub(r"\D", "", SMS_TEST_NUMBER)
        to_e164 = "+1" + td if len(td) == 10 else "+" + td

    payload = {
        "recipients": to_e164,
        "message": SMS_TEMPLATE.format(first=first, address=address),
        "apikey":   SMS_API_KEY,
        "sendsms":  "1"
    }
    if SMS_FROM:
        payload["from"] = SMS_FROM

    try:
        r = requests.post(SMS_URL, data=payload, timeout=15)
        if r.status_code == 200 and '"error"' not in (r.text or "").lower():
            LOG.info("SMS sent to %s", to_e164)
            mark_sent(row_idx)
        else:
            LOG.error("SMS failed %s %s", r.status_code, r.text[:200])
    except Exception as e:
        LOG.error("SMS error %s", e)

# ───────────────────── scrape helpers & main loop ──────────────────
def extract_name(t):
    m = re.search(r"listing agent[:\s\-]*([A-Za-z \.'’-]{3,})", t, re.I)
    if m:
        n = m.group(1).strip()
        if not TEAM_RE.search(n):
            return n
    return None

def process_rows(rows):
    for r in rows:
        txt = (r.get("description","") + " " + r.get("openai_summary","")).strip()
        if not is_short_sale(txt):
            continue

        zpid = str(r.get("zpid",""))
        if zpid and not is_active_listing(zpid):
            LOG.info("Skip stale/off-market zpid %s", zpid)
            continue

        name = r.get("agentName","").strip() or extract_name(txt) or ""
        if not name or TEAM_RE.search(name):
            continue

        state = r.get("state","")
        phone = fmt_phone(lookup_phone(name, state, r))
        email = lookup_email(name, state)

        if phone and phone_exists(phone):
            continue

        first, *last = name.split()
        row_idx = append_row([
            first, " ".join(last), phone, email,
            r.get("street",""), r.get("city",""), state, ""
        ])

        if phone:
            send_sms(phone, first, r.get("street",""), row_idx)

# ——— webhook entry ———
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

