import os, json, logging, re, time, html, requests, hashlib
from collections import defaultdict
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

# --- ENV / CONSTANTS --------------------------------------------------------
CS_API_KEY  = os.environ["CS_API_KEY"]
CS_CX       = os.environ["CS_CX"]
GSHEET_ID   = os.environ["GSHEET_ID"]
SC_JSON     = json.loads(os.environ["GCP_SERVICE_ACCOUNT_JSON"])
SCOPES      = ["https://www.googleapis.com/auth/spreadsheets"]

# extra Google CSE keys for rotation (JSON array in env)
SEARCH_KEYS = [CS_API_KEY] + json.loads(os.getenv("CS_EXTRA_KEYS", "[]"))
_key_idx    = 0

# RapidAPI Zillow agent endpoints
RAPID_HOST = os.getenv("RAPID_HOST", "zillow-com1.p.rapidapi.com").strip()
RAPID_KEY  = os.getenv("RAPID_KEY", "").strip()

logging.basicConfig(level=os.getenv("LOGLEVEL", "DEBUG"),
                    format="%(asctime)s %(levelname)s: %(message)s")
LOG = logging.getLogger("bot")

SMS_ENABLE      = os.getenv("SMSM_ENABLE", "false").lower() == "true"
SMS_TEST_MODE   = os.getenv("SMSM_TEST_MODE", "true").lower() == "true"
SMS_TEST_NUMBER = os.getenv("SMSM_TEST_NUMBER", "")
SMS_API_KEY     = os.getenv("SMSM_API_KEY", "")
SMS_FROM        = os.getenv("SMSM_FROM", "")
SMS_URL         = os.getenv("SMSM_URL", "https://api.smsmobileapi.com/sendsms/")
SMS_TEMPLATE    = os.getenv("SMSM_TEMPLATE") or (
    "Hey {first}, this is Yoni Kutler—I saw your short-sale listing at {address} "
    "and wanted to introduce myself. I specialize in helping agents get faster bank "
    "approvals and ensure these deals close. No cost to you—I’m paid by the buyer "
    "at closing. Open to a quick call to see if this could help?"
)

MAX_Q_PHONE = 3          # still used but we now slice to 2
MAX_Q_EMAIL = 3

SHORT_RE = re.compile(r"\bshort\s+sale\b", re.I)
BAD_RE   = re.compile(r"approved|negotiator|settlement fee|fee at closing", re.I)
TEAM_RE  = re.compile(r"^\s*the\b|\bteam\b", re.I)

IMG_EXT  = (".png",".jpg",".jpeg",".gif",".svg",".webp")
PHONE_RE = re.compile(r"(?<!\d)(?:\+?1[\s\-\.]*)?\(?\d{3}\)?[\s\-\.]*\d{3}[\s\-\.]*\d{4}(?!\d)")
EMAIL_RE = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.I)

LABEL_TABLE = {"mobile":4,"cell":4,"direct":4,"text":4,"c:":4,"m:":4,
               "phone":2,"tel":2,"p:":2,"office":1,"main":1,"customer":1,"footer":1}
LABEL_RE = re.compile("("+"|".join(map(re.escape, LABEL_TABLE))+")", re.I)
US_AREA_CODES = {str(i) for i in range(201,990)}

# --- Google Sheets hooks ----------------------------------------------------
creds = Credentials.from_service_account_info(SC_JSON, scopes=SCOPES)
sheets_service = build("sheets","v4",credentials=creds,cache_discovery=False)
gc = gspread.authorize(creds)
ws = gc.open_by_key(GSHEET_ID).sheet1

# --- static lists for search ------------------------------------------------
AGENT_SITES = ["realtor.com","zillow.com","redfin.com","homesnap.com","kw.com","remax.com",
               "coldwellbanker.com","compass.com","exprealty.com","bhhs.com","c21.com",
               "realtyonegroup.com","mlsmatrix.com","mlslistings.com","har.com","brightmlshomes.com"]
DOMAIN_CLAUSE = " OR ".join(f"site:{d}" for d in AGENT_SITES)
SOCIAL_CLAUSE = "site:facebook.com OR site:linkedin.com"

# in-memory caches (persisted during run)
cache_p, cache_e = {}, {}

# --- helpers ---------------------------------------------------------------
def is_short_sale(t): return SHORT_RE.search(t) and not BAD_RE.search(t)

def fmt_phone(r):
    d = re.sub(r"\D", "", r)
    d = d[1:] if len(d) == 11 and d.startswith("1") else d
    return f"{d[:3]}-{d[3:6]}-{d[6:]}" if len(d) == 10 else ""

def valid_phone(p):
    if phonenumbers:
        try:
            return phonenumbers.is_possible_number(phonenumbers.parse(p, "US"))
        except: 
            return False
    return re.fullmatch(r"\d{3}-\d{3}-\d{4}", p) and p[:3] in US_AREA_CODES

def clean_email(e): return e.split("?")[0].strip()

def ok_email(e):
    e = clean_email(e)
    return e and "@" in e and not e.lower().endswith(IMG_EXT) and not re.search(r"\.(gov|edu|mil)$", e, re.I)

def proximity_scan(t):
    out = {}
    for m in PHONE_RE.finditer(t):
        p = fmt_phone(m.group())
        if not valid_phone(p):
            continue
        sn  = t[max(m.start()-80,0):min(m.end()+80,len(t))]
        lab = LABEL_RE.search(sn)
        w   = LABEL_TABLE.get(lab.group().lower(),0) if lab else 0
        if w < 2:
            continue
        bw,ts = out.get(p,(0,0))
        out[p] = (max(bw,w),ts+2+w)
    return out

def extract_struct(td):
    phones, mails = [], []
    if not BeautifulSoup:
        return phones, mails
    soup = BeautifulSoup(td, "html.parser")
    for sc in soup.find_all("script", {"type":"application/ld+json"}):
        try:
            data = json.loads(sc.string or "")
        except:
            continue
        if isinstance(data, list): data = data[0]
        if isinstance(data, dict):
            tel  = data.get("telephone") or (data.get("contactPoint") or {}).get("telephone")
            mail = data.get("email")     or (data.get("contactPoint") or {}).get("email")
            if tel:  phones.append(fmt_phone(tel))
            if mail: mails.append(clean_email(mail))
    for a in soup.select('a[href^="tel:"]'):    phones.append(fmt_phone(a["href"].split("tel:")[-1]))
    for a in soup.select('a[href^="mailto:"]'): mails.append(clean_email(a["href"].split("mailto:")[-1]))
    return phones, mails

# --- Google CSE with key rotation & back-off -------------------------------
def _next_key():
    global _key_idx
    key = SEARCH_KEYS[_key_idx % len(SEARCH_KEYS)]
    _key_idx += 1
    return key

def google_items(q, tries=3):
    delay = 2
    for _ in range(tries):
        key = _next_key()
        try:
            r = requests.get(
                "https://www.googleapis.com/customsearch/v1",
                params={"key": key, "cx": CS_CX, "q": q, "num": 10},
                timeout=10,
            )
            if r.status_code == 200:
                return r.json().get("items", [])
            if r.status_code == 429:
                LOG.warning("429 quota hit on key …%s; sleeping %ds", key[-5:], delay)
                time.sleep(delay)
                delay *= 2
                continue
            LOG.error("CSE %s %s", r.status_code, r.text[:200])
            break
        except Exception as e:
            LOG.error("CSE error %s", e)
            time.sleep(delay)
            delay *= 2
    return []

# --- RapidAPI helper -------------------------------------------------------
def rapid_agent_details(name):
    if not RAPID_KEY:
        return {}
    try:
        url = f"https://{RAPID_HOST}/agentDetails?name={requests.utils.quote(name)}"
        r   = requests.get(url,
                           headers={"X-RapidAPI-Key": RAPID_KEY,
                                    "X-RapidAPI-Host": RAPID_HOST},
                           timeout=10)
        if r.status_code == 200:
            return (r.json() or {}).get("data", {}) or {}
    except Exception as e:
        LOG.debug("RapidAPI agentDetails error %s", e)
    return {}

# --- query builders --------------------------------------------------------
def build_q_phone(a,s):
    return [
        f'"{a}" {s} ("mobile" OR "cell" OR "direct") phone ({DOMAIN_CLAUSE})',
        f'"{a}" {s} phone ({DOMAIN_CLAUSE})',
        f'"{a}" {s} contact ({DOMAIN_CLAUSE})',
    ]

def build_q_email(a,s):
    return [
        f'"{a}" {s} email ({DOMAIN_CLAUSE})',
        f'"{a}" {s} contact email ({DOMAIN_CLAUSE})',
        f'"{a}" {s} real estate email ({DOMAIN_CLAUSE} OR {SOCIAL_CLAUSE})',
    ]

# --- contact look-ups ------------------------------------------------------
def lookup_phone(a, s):
    k = f"{a}|{s}"
    if k in cache_p:
        return cache_p[k]

    # RapidAPI first
    rd = rapid_agent_details(a)
    tel = rd.get("phone") or rd.get("phoneNumber") or ""
    if tel:
        p = fmt_phone(tel)
        if valid_phone(p):
            cache_p[k] = p
            LOG.debug("RapidAPI phone hit for %s -> %s", a, p)
            return p

    cand = {}
    for q in build_q_phone(a, s)[:2]:  # only two highest-yield templates
        for it in google_items(q):
            tel = it.get("pagemap", {}).get("contactpoint", [{}])[0].get("telephone")
            if tel:
                p = fmt_phone(tel)
                if valid_phone(p):
                    cand[p] = (4, 8)
        if cand:
            break

    phone = max(cand, key=lambda k: cand[k]) if cand else ""
    cache_p[k] = phone
    return phone

def lookup_email(a, s):
    k = f"{a}|{s}"
    if k in cache_e:
        return cache_e[k]

    # RapidAPI first
    rd = rapid_agent_details(a)
    em = rd.get("email") or ""
    if ok_email(em):
        cache_e[k] = em
        LOG.debug("RapidAPI email hit for %s -> %s", a, em)
        return em

    cand = defaultdict(int)
    for q in build_q_email(a, s)[:2]:
        for it in google_items(q):
            mail = clean_email(it.get("pagemap", {}).get("contactpoint", [{}])[0].get("email", ""))
            if ok_email(mail):
                cand[mail] += 3
        if cand:
            break

    email = max(cand, key=cand.get) if cand else ""
    cache_e[k] = email
    return email

# --- SMS -------------------------------------------------------------------
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
        "sendsms": "1",
    }
    if SMS_FROM:
        payload["from"] = SMS_FROM
    try:
        r = requests.post(SMS_URL, data=payload, timeout=15)
        if r.status_code == 200:
            LOG.info("SMS sent to %s", to_e164)
            return True
        LOG.error("SMS failed %s %s", r.status_code, r.text[:200])
    except Exception as e:
        LOG.error("SMS error %s", e)
    return False

# --- sheet helpers / zpid vault -------------------------------------------
VAULT_RANGE = "Vault!A:A"   # column storing hashed zpids

def _hash_zpid(zpid: str) -> str:
    return hashlib.sha256(zpid.encode()).hexdigest()[:40]

def load_vault():
    try:
        vals = ws.spreadsheet.values_get(VAULT_RANGE).get("values", [])
        return {row[0] for row in vals if row}
    except Exception as e:
        LOG.error("Vault load error %s", e)
        return set()

vault = load_vault()

def vault_contains(zpid: str) -> bool:
    return _hash_zpid(zpid) in vault

def vault_add(zpid: str):
    h = _hash_zpid(zpid)
    if h in vault:
        return
    try:
        sheets_service.spreadsheets().values().append(
            spreadsheetId=GSHEET_ID,
            range=VAULT_RANGE,
            valueInputOption="RAW",
            body={"values": [[h]]},
        ).execute()
        vault.add(h)
        LOG.info("Added zpid %s to vault", zpid)
    except Exception as e:
        LOG.error("Vault append error %s", e)

def phone_exists(p):
    try:
        return p in ws.col_values(3)
    except:
        return False

def append_row(v):
    sheets_service.spreadsheets().values().append(
        spreadsheetId=GSHEET_ID,
        range="Sheet1!A1",
        valueInputOption="RAW",
        body={"values": [v]},
    ).execute()

# --- fetch helpers ---------------------------------------------------------
def fetch(u):
    bare = u[8:] if u.startswith("https://") else u[7:] if u.startswith("http://") else u
    for url in (u, f"https://r.jina.ai/http://{u}", f"https://r.jina.ai/http://{bare}"):
        for _ in range(2):
            try:
                r = requests.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
                if r.status_code == 200 and "unusual traffic" not in r.text[:600].lower():
                    return r.text
            except:
                continue
    return None

# --- main row processor ----------------------------------------------------
def extract_name(t):
    m = re.search(r"listing agent[:\s-]*([A-Za-z \.'’-]{3,})", t, re.I)
    if m:
        n = m.group(1).strip()
        if not TEAM_RE.search(n):
            return n
    return None

def process_rows(rows):
    if not rows:
        return
    LOG.debug("Sample fields on first fresh row: %s", list(rows[0].keys())[:8])
    for r in rows:
        zpid = str(r.get("zpid", "")).strip()
        if not zpid or vault_contains(zpid):
            LOG.debug("Skip already-seen zpid %s", zpid)
            continue
        desc = r.get("description", "")
        if not is_short_sale(desc):
            continue

        name = r.get("agentName", "").strip()
        if not name:
            name = extract_name(r.get("openai_summary", "") + "\n" + desc)
            if not name:
                continue
        if TEAM_RE.search(name):
            alt = extract_name(r.get("openai_summary", "") + "\n" + desc)
            if alt:
                name = alt
            else:
                continue
        if TEAM_RE.search(name):
            continue

        state = r.get("state", "")
        phone = fmt_phone(lookup_phone(name, state))
        email = lookup_email(name, state)

        if not phone and not email:
            LOG.warning("No contact found for %s, %s", name, zpid)
            vault_add(zpid)
            continue

        if phone and phone_exists(phone):
            LOG.debug("Phone %s already in sheet", phone)
            vault_add(zpid)
            continue

        first, *last = name.split()
        append_row([first, " ".join(last), phone, email,
                    r.get("street", ""), r.get("city", ""), state])
        LOG.info("Added zpid %s", zpid)
        vault_add(zpid)

        if phone:
            send_sms(phone, first, r.get("street", ""))

# --------------------------------------------------------------------------
if __name__ == "__main__":
    import sys, json
    if not sys.stdin.isatty():
        payload = json.load(sys.stdin)
        rows = payload.get("listings") or payload.get("rows") or []
        process_rows(rows)

