# === bot_min.py  (Jun 2025 → patch 3: strict-name window + Google confirm + repeat-bonus) ===
#
#  • fixes false-positive SMS “ERROR” log
#  • returns row-index from append_row() and writes “x” in column H when SMS succeeds
#  • small email-coverage boost (extra catch-all query + more brokerage domains)
#
#  ─────────────────────────────  NEW  2025-06-18  ─────────────────────────────
#  1. Name window filter      – a phone is accepted from a page only when the
#                               agent’s first or last name appears inside ±80
#                               characters of the number  (proximity_scan()).
#  2. Google-snippet confirm  – every candidate phone must appear in at least
#                               one of the first 5 Google CSE results *together*
#                               with the agent’s full name (google_confirms()).
#  3. Repeat-page bonus       – numbers that occur on ≥ 2 different pages get a
#                               +2 score bump before the winner is chosen.
#  ─────────────────────────────────────────────────────────────────────────────

import os, sys, json, logging, re, time, html, random, requests, asyncio, concurrent.futures
from collections import defaultdict, Counter
from datetime import datetime

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


# ----------------------------- ENV / AUTH -----------------------------
CS_API_KEY = os.environ["CS_API_KEY"]
CS_CX      = os.environ["CS_CX"]
GSHEET_ID  = os.environ["GSHEET_ID"]
SC_JSON    = json.loads(os.environ["GCP_SERVICE_ACCOUNT_JSON"])
SCOPES     = ["https://www.googleapis.com/auth/spreadsheets"]

# ► RapidAPI key/host for status check (optional)
RAPID_KEY  = os.getenv("RAPID_KEY", "").strip()
RAPID_HOST = os.getenv("RAPID_HOST", "zillow-com1.p.rapidapi.com").strip()
GOOD_STATUS = {
    "FOR_SALE", "ACTIVE", "COMING_SOON", "PENDING", "NEW_CONSTRUCTION"
}

logging.basicConfig(level=os.getenv("LOGLEVEL", "DEBUG"),
                    format="%(asctime)s %(levelname)s: %(message)s")
LOG = logging.getLogger("bot")

# --------------------------- NEW   CONFIGS ----------------------------
MAX_ZILLOW_403      = 3
MAX_RATE_429        = 3
BACKOFF_FACTOR      = 1.7
MAX_BACKOFF_SECONDS = 12
GOOGLE_CONCURRENCY  = 4
METRICS             = Counter()

# --------------------------- SMS CONFIG -------------------------------
SMS_ENABLE      = os.getenv("SMSM_ENABLE", "false").lower() == "true"
SMS_TEST_MODE   = os.getenv("SMSM_TEST_MODE", "true").lower() == "true"
SMS_TEST_NUMBER = os.getenv("SMSM_TEST_NUMBER", "")
SMS_API_KEY     = os.getenv("SMSM_API_KEY", "")
SMS_FROM        = os.getenv("SMSM_FROM", "")
SMS_URL         = os.getenv("SMSM_URL", "https://api.smsmobileapi.com/sendsms/")
SMS_TEMPLATE    = (
    "Hey {first}, this is Yoni Kutler—I saw your listing at {address} and wanted to "
    "introduce myself. I specialize in helping agents get faster bank approvals and "
    "ensure these deals close. I know you likely handle short sales yourself, but I "
    "work behind the scenes to take on lender negotiations so you can focus on selling. "
    "No cost to you or your client—I’m only paid by the buyer at closing. Would you be "
    "open to a quick call to see if this could help?"
)

MAX_Q_PHONE = 5
MAX_Q_EMAIL = 5

# ------------------------------ REGEXES -------------------------------
SHORT_RE = re.compile(r"\bshort\s+sale\b", re.I)
BAD_RE   = re.compile(r"\b(?:approved short sale|short sale approved)\b", re.I)

TEAM_RE = re.compile(r"^\s*the\b|\bteam\b", re.I)

IMG_EXT  = (".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp")
PHONE_RE = re.compile(r"(?<!\d)(?:\+?1[\s\-\.]*)?\(?\d{3}\)?[\s\-\.]*\d{3}[\s\-\.]*\d{4}(?!\d)")
EMAIL_RE = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.I)

LABEL_TABLE = {"mobile":4,"cell":4,"direct":4,"text":4,"c:":4,"m:":4,
               "phone":2,"tel":2,"p:":2,"office":1,"main":1,"customer":1,"footer":1}
LABEL_RE = re.compile("(" + "|".join(map(re.escape, LABEL_TABLE)) + ")", re.I)
US_AREA_CODES = {str(i) for i in range(201, 990)}

creds = Credentials.from_service_account_info(SC_JSON, scopes=SCOPES)
sheets_service = build("sheets", "v4", credentials=creds, cache_discovery=False)
gc = gspread.authorize(creds)
ws = gc.open_by_key(GSHEET_ID).sheet1

# ----------------------------- SITE LISTS -----------------------------
AGENT_SITES = [
    "realtor.com", "zillow.com", "redfin.com", "homesnap.com", "kw.com",
    "remax.com", "coldwellbanker.com", "compass.com", "exprealty.com",
    "bhhs.com", "c21.com", "realtyonegroup.com", "mlsmatrix.com",
    "mlslistings.com", "har.com", "brightmlshomes.com",
    "exitrealty.com", "realtyexecutives.com", "realty.com"
]

BROKERAGE_SITES = [
    "sothebysrealty.com", "corcoran.com", "douglaselliman.com",
    "cryereleike.com", "windermere.com", "longandfoster.com",
    # extras spotted in the wild
    "lifestyleintl.com", "lifestyleinternationalrealty.com"
]

DOMAIN_CLAUSE    = " OR ".join(f"site:{d}" for d in AGENT_SITES)
BROKERAGE_CLAUSE = " OR ".join(f"site:{d}" for d in BROKERAGE_SITES)
SOCIAL_CLAUSE    = "site:facebook.com OR site:linkedin.com"

cache_p, cache_e = {}, {}

# ----------------------------- UTILITIES ------------------------------
def is_short_sale(t):
    return SHORT_RE.search(t) and not BAD_RE.search(t)

def fmt_phone(r):
    d = re.sub(r"\D", "", r)
    d = d[1:] if len(d) == 11 and d.startswith("1") else d
    return f"{d[:3]}-{d[3:6]}-{d[6:]}" if len(d) == 10 else ""

def valid_phone(p):
    if phonenumbers:
        try:
            return phonenumbers.is_possible_number(phonenumbers.parse(p, "US"))
        except Exception:
            return False
    return re.fullmatch(r"\d{3}-\d{3}-\d{4}", p) and p[:3] in US_AREA_CODES

def clean_email(e):
    return e.split("?")[0].strip()

def ok_email(e):
    e = clean_email(e)
    return e and "@" in e and not e.lower().endswith(IMG_EXT) and not re.search(r"\.(gov|edu|mil)$", e, re.I)

# ► RapidAPI helper to decide if listing is active
def is_active_listing(zpid):
    if not RAPID_KEY:
        return True
    try:
        r = requests.get(
            f"https://{RAPID_HOST}/property",
            params={"zpid": zpid},
            headers={
                "X-RapidAPI-Key": RAPID_KEY,
                "X-RapidAPI-Host": RAPID_HOST
            },
            timeout=15
        )
        r.raise_for_status()
        data = r.json().get("data") or r.json()
        status = (data.get("homeStatus") or "").upper()
        return (not status) or status in GOOD_STATUS
    except Exception as e:
        LOG.warning("RapidAPI status check failed for %s (%s) – keeping row", zpid, e)
        return True

# ---------------- fetch() with JS-render fallback --------------------
def fetch(u):
    bare = u[8:] if u.startswith("https://") else u[7:] if u.startswith("http://") else u
    variants = [
        u,
        f"https://r.jina.ai/http://{u}",
        f"https://r.jina.ai/http://{bare}",
        f"https://r.jina.ai/http://screenshot/{bare}",
        f"https://r.jina.ai/http://screenshot/{u}"
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
            elif r.status_code == 429:
                ratelimit += 1
                METRICS["fetch_429"] += 1
                if ratelimit >= MAX_RATE_429:
                    LOG.debug("rate-limit ceiling hit, giving up (%s)", url)
                    return None
            else:
                METRICS[f"fetch_other_{r.status_code}"] += 1

            time.sleep(min(backoff, MAX_BACKOFF_SECONDS))
            backoff *= BACKOFF_FACTOR
    return None

# ---------------- Google API helpers (parallel) ----------------------
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

# -- NEW helper: confirm phone appears in Google snippet with agent name
def google_confirms(phone, agent_name):
    q = f'"{phone}" "{agent_name}"'
    for it in google_items(q, tries=1)[:5]:
        snippet = (it.get("snippet") or "").lower()
        if all(tok in snippet for tok in agent_name.lower().split()):
            return True
    return False

# ---------------- query builders ------------------------------------
def build_q_phone(a, s):
    return [
        f'"{a}" realtor {s} phone number',
        f'"{a}" {s} ("mobile" OR "cell" OR "direct") phone ({DOMAIN_CLAUSE})',
        f'"{a}" {s} phone ({DOMAIN_CLAUSE})',
        f'"{a}" {s} contact ({DOMAIN_CLAUSE})',
        f'"{a}" {s} phone {SOCIAL_CLAUSE}',
        f'"{a}" {s} phone ({BROKERAGE_CLAUSE})',
    ]

def build_q_email(a, s):
    return [
        f'"{a}" {s} email ({DOMAIN_CLAUSE})',
        f'"{a}" {s} contact email ({DOMAIN_CLAUSE})',
        f'"{a}" {s} real estate email ({DOMAIN_CLAUSE} OR {SOCIAL_CLAUSE})',
        f'"{a}" {s} realty email',
        f'"{a}" {s} gmail.com',
        f'"{a}" {s} email ({BROKERAGE_CLAUSE})',
        # catch-all pass (no domain clause) – new
        f'"{a}" {s} email'
    ]

# ---------------- proximity scan & structured -----------------------
def proximity_scan(t, name_tokens):
    """
    Return {phone: (best_weight, score, office_flag)} for phones that:
      • have a label weight ≥ 2
      • appear with at least one agent name token inside ±80 chars
    """
    out = {}
    for m in PHONE_RE.finditer(t):
        p = fmt_phone(m.group())
        if not valid_phone(p):
            continue
        sn = t[max(m.start()-80, 0):min(m.end()+80, len(t))]
        if not any(tok in sn for tok in name_tokens):
            continue           # NEW: must reference agent name close-by
        lab_match = LABEL_RE.search(sn)
        lab = lab_match.group().lower() if lab_match else ""
        w = LABEL_TABLE.get(lab, 0)
        if w < 2:
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

# ---------------- Sheet helpers -------------------------------------
def phone_exists(p):
    try:
        return p in ws.col_values(3)
    except Exception:
        return False

def append_row(v):
    resp = sheets_service.spreadsheets().values().append(
        spreadsheetId=GSHEET_ID,
        range="Sheet1!A1",
        valueInputOption="RAW",
        body={"values": [v]}
    ).execute()
    # updatedRange looks like 'Sheet1!A881:D881' → we need the row number
    try:
        row_idx = int(resp["updates"]["updatedRange"].split("!A")[1].split(":")[0])
    except Exception:
        row_idx = None
    LOG.info("Row appended to sheet%s", f" (row {row_idx})" if row_idx else "")
    return row_idx

# ---------------- parallel helper -----------------------------------
_executor = concurrent.futures.ThreadPoolExecutor(max_workers=GOOGLE_CONCURRENCY)
def pmap(fn, iterable):
    return list(_executor.map(fn, iterable))

# ---------------- lookup functions ----------------------------------
def lookup_phone(a, s):
    k = f"{a}|{s}"
    if k in cache_p:
        return cache_p[k]

    cand_good  = {}          # phone → score
    cand_office = {}
    seen_pages = Counter()   # phone → #distinct pages

    def add(p, score, office_flag, page_tag=None):
        d = fmt_phone(p)
        if not valid_phone(d):
            return
        tgt = cand_office if office_flag else cand_good
        tgt[d] = tgt.get(d, 0) + score
        if page_tag is not None:
            seen_pages[d] += page_tag  # page_tag is 0/1 unique indicator

    queries = build_q_phone(a, s)[:MAX_Q_PHONE]
    google_batches = pmap(google_items, queries)

    # quick wins from structured data in result JSON
    for items in google_batches:
        for it in items:
            tel = it.get("pagemap", {}).get("contactpoint", [{}])[0].get("telephone")
            if tel:
                add(tel, 4, False)

    urls = [it.get("link", "") for items in google_batches for it in items][:30]
    html_pages = pmap(fetch, urls)

    name_tokens = [w.lower() for w in a.split() if len(w) > 2]

    # parse each fetched page
    for idx, t in enumerate(html_pages):
        if not t:
            continue
        t_low = html.unescape(t.lower())
        if a.lower() not in t_low:
            continue
        page_phones = set()

        ph, _ = extract_struct(t)
        for p in ph:
            add(p, 6, False, 0)        # structured phones get good weight
            page_phones.add(fmt_phone(p))

        for p, (bw, sc, office_flag) in proximity_scan(t_low, name_tokens).items():
            add(p, sc, office_flag, 0)
            page_phones.add(p)

        # mark unique presence per page
        for p in page_phones:
            seen_pages[p] += 1

        if cand_good or cand_office:
            pass  # keep gathering; later repeat-bonus uses seen_pages

    # ③ repeat-page bonus
    for p, n in seen_pages.items():
        if n >= 2 and p in cand_good:
            cand_good[p] += 2

    # Google-snippet confirmation filter
    for p in list(cand_good):
        if not google_confirms(p, a):
            LOG.debug("discard %s – snippet lacks agent name", p)
            cand_good.pop(p, None)

    phone = ""
    if cand_good:
        phone = max(cand_good, key=cand_good.get)
    elif cand_office:
        phone = ""       # blank rather than office/main only
    cache_p[k] = phone
    return phone

def lookup_email(a, s):
    k = f"{a}|{s}"
    if k in cache_e:
        return cache_e[k]
    cand = defaultdict(int)

    queries = build_q_email(a, s)[:MAX_Q_EMAIL]
    google_batches = pmap(google_items, queries)

    for items in google_batches:
        for it in items:
            mail = clean_email(it.get("pagemap", {}).get("contactpoint", [{}])[0].get("email", ""))
            if ok_email(mail):
                cand[mail] += 3

    urls = [it.get("link", "") for items in google_batches for it in items][:30]
    html_pages = pmap(fetch, urls)

    for t in html_pages:
        if not t or a.lower() not in (t or "").lower():
            continue
        _, em = extract_struct(t)
        for m in em:
            m = clean_email(m)
            if ok_email(m):
                cand[m] += 3
        for m in EMAIL_RE.findall(t):
            m = clean_email(m)
            if ok_email(m):
                cand[m] += 1
        if cand:
            break

    tokens = {re.sub(r"[^a-z]", "", w.lower()) for w in a.split()}
    good = {m: sc for m, sc in cand.items()
            if any(tok and tok in m.lower() for tok in tokens)}
    email = max(good, key=good.get) if good else ""
    cache_e[k] = email
    return email

# ---------------- SMS helper ----------------------------------------
def send_sms(num, first, address):
    if not SMS_ENABLE:
        return False
    d = re.sub(r"\D", "", num)
    if len(d) == 10:
        to_e164 = "+1" + d
    elif len(d) == 11 and d.startswith("1"):
        to_e164 = "+" + d
    else:
        return False
    if SMS_TEST_MODE and SMS_TEST_NUMBER:
        td = re.sub(r"\D", "", SMS_TEST_NUMBER)
        to_e164 = "+1" + td if len(td) == 10 else "+" + td

    payload = {
        "recipients": to_e164,
        "message": SMS_TEMPLATE.format(first=first, address=address),
        "apikey": SMS_API_KEY,
        "sendsms": "1"
    }
    if SMS_FROM:
        payload["from"] = SMS_FROM
    try:
        r = requests.post(SMS_URL, data=payload, timeout=15)
        if r.status_code == 200:
            try:
                body = r.json()
            except ValueError:
                body = {}
            if body.get("result", {}).get("error") == 0:
                LOG.info("SMS sent to %s", to_e164)
                return True
        LOG.error("SMS failed %s %s", r.status_code, r.text[:200])
    except Exception as e:
        LOG.error("SMS error %s", e)
    return False

# ---------------- scrape helpers ------------------------------------
def extract_name(t):
    m = re.search(r"listing agent[:\s\-]*([A-Za-z \.'’-]{3,})", t, re.I)
    if m:
        n = m.group(1).strip()
        if not TEAM_RE.search(n):
            return n
    return None

# ---------------- main processing -----------------------------------
def process_rows(rows):
    for r in rows:
        if not is_short_sale(r.get("description", "")):
            continue

        zpid = str(r.get("zpid", ""))
        if zpid and not is_active_listing(zpid):
            LOG.info("Skip stale/off-market zpid %s", zpid)
            continue

        name = r.get("agentName", "").strip()
        if not name:
            name = extract_name(r.get("openai_summary", "") + "\n" + r.get("description", ""))
            if not name:
                continue

        if TEAM_RE.search(name):
            alt = extract_name(r.get("openai_summary", "") + "\n" + r.get("description", ""))
            if alt:
                name = alt
            else:
                continue
        if TEAM_RE.search(name):
            continue

        state = r.get("state", "")

        phone = fmt_phone(lookup_phone(name, state))
        email = lookup_email(name, state)

        if phone and phone_exists(phone):
            continue

        first, *last = name.split()
        row_idx = append_row([
            first, " ".join(last), phone, email,
            r.get("street", ""), r.get("city", ""), state
        ])

        if phone and send_sms(phone, first, r.get("street", "")) and row_idx:
            try:
                ws.update_cell(row_idx, 8, "x")   # Column H
            except Exception as e:
                LOG.warning("Could not mark SMS flag: %s", e)

# ---------------- CLI entrypoint ------------------------------------
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

    LOG.info("apify-hook: received %s listing%s directly in payload",
             len(fresh_rows), "" if len(fresh_rows)==1 else "s")
    LOG.debug("Sample fields on first fresh row: %s", list(fresh_rows[0].keys()))
    process_rows(fresh_rows)

    if METRICS:
        LOG.info("metrics %s", dict(METRICS))

