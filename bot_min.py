import os, sys, json, logging, re, time, html, random, requests
from collections import defaultdict

# ---------- optional deps ----------
try:
    import phonenumbers
except ImportError:
    phonenumbers = None
try:
    from bs4 import BeautifulSoup
except ImportError:
    BeautifulSoup = None

# ---------- G-Sheets + Google CSE ----------
import gspread
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials

CS_API_KEY = os.environ["CS_API_KEY"]
CS_CX      = os.environ["CS_CX"]
GSHEET_ID  = os.environ["GSHEET_ID"]
SC_JSON    = json.loads(os.environ["GCP_SERVICE_ACCOUNT_JSON"])
SCOPES     = ["https://www.googleapis.com/auth/spreadsheets"]

creds = Credentials.from_service_account_info(SC_JSON, scopes=SCOPES)
sheets_service = build("sheets", "v4", credentials=creds, cache_discovery=False)
gc = gspread.authorize(creds)
ws_main  = gc.open_by_key(GSHEET_ID).sheet1
ws_vault = gc.open_by_key(GSHEET_ID).worksheet("Vault")

# ---------- logging ----------
logging.basicConfig(
    level=os.getenv("LOGLEVEL", "DEBUG"),
    format="%(asctime)s %(levelname)s: %(message)s",
)
LOG = logging.getLogger("bot")

# ---------- SMS (optional) ----------
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

# ---------- regex rules ----------
SHORT_RE = re.compile(r"\bshort\s+sale\b", re.I)

# *** updated pattern: still blocks “bank approved” etc. but ALLOWS “unapproved” / “not approved” ***
BAD_RE   = re.compile(r"(?<!not )\bapproved\b|negotiator|settlement fee|fee at closing", re.I)

TEAM_RE  = re.compile(r"^\s*the\b|\bteam\b", re.I)
IMG_EXT  = (".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp")

PHONE_RE = re.compile(
    r"(?<!\d)(?:\+?1[\s\-\.]*)?\(?\d{3}\)?[\s\-\.]*\d{3}[\s\-\.]*\d{4}(?!\d)"
)
EMAIL_RE = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.I)

LABEL_TABLE = {
    "mobile": 4, "cell": 4, "direct": 4, "text": 4, "c:": 4, "m:": 4,
    "phone": 2,  "tel": 2,  "p:": 2,
    "office": 1, "main": 1, "customer": 1, "footer": 1,
}
LABEL_RE      = re.compile("(" + "|".join(map(re.escape, LABEL_TABLE)) + ")", re.I)
US_AREA_CODES = {str(i) for i in range(201, 990)}

# ---------- misc constants ----------
MAX_Q_PHONE = 5
MAX_Q_EMAIL = 5

AGENT_SITES = [
    "realtor.com", "zillow.com", "redfin.com", "homesnap.com", "kw.com",
    "remax.com", "coldwellbanker.com", "compass.com", "exprealty.com", "bhhs.com",
    "c21.com", "realtyonegroup.com", "mlsmatrix.com", "mlslistings.com", "har.com",
    "brightmlshomes.com", "realty.com", "realtyexecutives.com"
]
DOMAIN_CLAUSE  = " OR ".join(f"site:{d}" for d in AGENT_SITES)
SOCIAL_CLAUSE  = "site:facebook.com OR site:linkedin.com"
IMG_LABEL_TOKS = ("data:image", "base64,")

cache_p, cache_e = {}, {}

# ---------- helper funcs ----------
def is_short_sale(desc: str) -> bool:
    return bool(SHORT_RE.search(desc)) and not BAD_RE.search(desc)

def fmt_phone(raw: str) -> str:
    d = re.sub(r"\D", "", raw)
    if len(d) == 11 and d.startswith("1"):
        d = d[1:]
    return f"{d[:3]}-{d[3:6]}-{d[6:]}" if len(d) == 10 else ""

def valid_phone(p: str) -> bool:
    if not p:
        return False
    if phonenumbers:
        try:
            return phonenumbers.is_possible_number(phonenumbers.parse(p, "US"))
        except Exception:
            return False
    return bool(re.fullmatch(r"\d{3}-\d{3}-\d{4}", p)) and p[:3] in US_AREA_CODES

def clean_email(e: str) -> str:
    return e.split("?")[0].strip()

def ok_email(e: str) -> bool:
    e = clean_email(e)
    return (
        e
        and "@" in e
        and not e.lower().endswith(IMG_EXT)
        and not re.search(r"\.(gov|edu|mil)$", e, re.I)
    )

def proximity_scan(text: str):
    hits = {}
    for m in PHONE_RE.finditer(text):
        phone = fmt_phone(m.group())
        if not valid_phone(phone):
            continue
        span = text[max(m.start() - 80, 0): m.end() + 80]
        lab  = LABEL_RE.search(span)
        w    = LABEL_TABLE.get(lab.group().lower(), 0) if lab else 0
        if w < 2:
            continue
        bw, ts = hits.get(phone, (0, 0))
        hits[phone] = (max(bw, w), ts + 2 + w)
    return hits

def extract_struct(html_txt: str):
    phones, mails = [], []
    if not BeautifulSoup:
        return phones, mails
    soup = BeautifulSoup(html_txt, "html.parser")

    # -- ld+json blocks
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

    # tel/mailto links
    for a in soup.select('a[href^="tel:"]'):
        phones.append(fmt_phone(a["href"].split("tel:")[-1]))
    for a in soup.select('a[href^="mailto:"]'):
        mails.append(clean_email(a["href"].split("mailto:")[-1]))
    return phones, mails

def phone_exists(p: str) -> bool:
    try:
        return p in ws_main.col_values(3)
    except Exception:
        return False

def append_row(vals):
    sheets_service.spreadsheets().values().append(
        spreadsheetId=GSHEET_ID,
        range="Sheet1!A1",
        valueInputOption="RAW",
        body={"values": [vals]},
    ).execute()
    LOG.info("Row appended to sheet")

def fetch(url: str):
    bare = url[8:] if url.startswith("https://") else url[7:] if url.startswith("http://") else url
    for u in (url, f"https://r.jina.ai/http://{url}", f"https://r.jina.ai/http://{bare}"):
        for _ in range(2):
            try:
                r = requests.get(
                    u,
                    timeout=10,
                    headers={"User-Agent": "Mozilla/5.0"},
                )
                if r.status_code == 200 and "unusual traffic" not in r.text[:600].lower():
                    return r.text
            except Exception:
                continue
    return None

def google_items(q):
    backoff = 1
    for _ in range(7):
        try:
            resp = requests.get(
                "https://www.googleapis.com/customsearch/v1",
                params={"key": CS_API_KEY, "cx": CS_CX, "q": q, "num": 10},
                timeout=10,
            )
            if resp.status_code == 429:
                time.sleep(backoff)
                backoff = min(backoff * 2, 16)
                continue
            return resp.json().get("items", [])
        except Exception:
            time.sleep(backoff)
            backoff = min(backoff * 2, 16)
    return []

def build_q_phone(name, state):
    return [
        f'"{name}" {state} ("mobile" OR "cell" OR "direct") phone ({DOMAIN_CLAUSE})',
        f'"{name}" {state} phone ({DOMAIN_CLAUSE})',
        f'"{name}" {state} contact ({DOMAIN_CLAUSE})',
        f'"{name}" {state} realty phone',
        f'"{name}" {state} ("homesnap" OR "zillow" OR "realtor")',
    ][:MAX_Q_PHONE]

def build_q_email(name, state):
    return [
        f'"{name}" {state} email ({DOMAIN_CLAUSE})',
        f'"{name}" {state} contact email ({DOMAIN_CLAUSE})',
        f'"{name}" {state} real estate email ({DOMAIN_CLAUSE} OR {SOCIAL_CLAUSE})',
        f'"{name}" {state} realty email',
        f'"{name}" {state} gmail.com',
    ][:MAX_Q_EMAIL]

def lookup_phone(name, state):
    k = f"{name}|{state}"
    if k in cache_p:
        return cache_p[k]
    cand = {}
    for q in build_q_phone(name, state):
        time.sleep(0.25)
        for it in google_items(q):
            tel = it.get("pagemap", {}).get("contactpoint", [{}])[0].get("telephone")
            if tel:
                p = fmt_phone(tel)
                if valid_phone(p):
                    cand[p] = (4, 8)
        for it in google_items(q):
            u = it.get("link", "")
            t = fetch(u)
            if not t or name.lower() not in t.lower():
                continue
            ph, _ = extract_struct(t)
            for p in ph:
                pf = fmt_phone(p)
                if valid_phone(pf):
                    bw, ts = cand.get(pf, (0, 0))
                    cand[pf] = (4, ts + 6)
            low = html.unescape(t.lower())
            for p, (bw, sc) in proximity_scan(low).items():
                b, ts = cand.get(p, (0, 0))
                cand[p] = (max(bw, b), ts + sc)
        if cand:
            break
    if not cand:
        cache_p[k] = ""
        return ""
    phone = max(cand, key=lambda k_: cand[k_])  # choose best score
    cache_p[k] = phone
    return phone

def lookup_email(name, state):
    k = f"{name}|{state}"
    if k in cache_e:
        return cache_e[k]
    cand = defaultdict(int)
    for q in build_q_email(name, state):
        time.sleep(0.25)
        for it in google_items(q):
            mail = clean_email(it.get("pagemap", {}).get("contactpoint", [{}])[0].get("email", ""))
            if ok_email(mail):
                cand[mail] += 3
        for it in google_items(q):
            u = it.get("link", "")
            t = fetch(u)
            if not t or name.lower() not in t.lower():
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
    # rank: must contain part of name
    tokens = {re.sub(r"[^a-z]", "", w.lower()) for w in name.split()}
    filt   = {m: sc for m, sc in cand.items() if any(tok and tok in m.lower() for tok in tokens)}
    email  = max(filt, key=filt.get) if filt else ""
    cache_e[k] = email
    return email

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
        td      = re.sub(r"\D", "", SMS_TEST_NUMBER)
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

def extract_name(text: str):
    m = re.search(r"listing agent[:\s-]*([A-Za-z \.'’-]{3,})", text, re.I)
    if m:
        n = m.group(1).strip()
        if not TEAM_RE.search(n):
            return n
    return None

# ---------- vault helpers ----------
def load_vault():
    try:
        col = ws_vault.col_values(1)
        LOG.debug("Loaded %d zpids from Vault sheet", len(col))
        return set(col)
    except Exception as e:
        LOG.error("Vault load error %s", e)
        return set()

def add_to_vault(zpid):
    try:
        ws_vault.append_row([str(zpid)])
    except Exception as e:
        LOG.error("Vault append error %s", e)

vault_set = load_vault()

# ---------- main row processor ----------
def process_rows(rows):
    for r in rows:
        zpid = str(r.get("zpid", ""))
        if zpid in vault_set:
            continue

        if not is_short_sale(r.get("description", "")):
            add_to_vault(zpid)
            vault_set.add(zpid)
            continue

        name = r.get("agentName", "").strip()
        if not name:
            name = extract_name((r.get("openai_summary", "") + "\n" + r.get("description", "")))
            if not name:
                add_to_vault(zpid)
                vault_set.add(zpid)
                continue

        if TEAM_RE.search(name):
            alt = extract_name((r.get("openai_summary", "") + "\n" + r.get("description", "")))
            if alt:
                name = alt
            else:
                add_to_vault(zpid)
                vault_set.add(zpid)
                continue
        if TEAM_RE.search(name):
            add_to_vault(zpid)
            vault_set.add(zpid)
            continue

        state = r.get("state", "")
        phone = fmt_phone(lookup_phone(name, state))
        email = lookup_email(name, state)

        # enforce phone rank tie-break: mobile/direct preferred
        if phone and phone_exists(phone):
            add_to_vault(zpid)
            vault_set.add(zpid)
            continue

        first, *last = name.split()
        try:
            append_row(
                [
                    first,
                    " ".join(last),
                    phone,
                    email,
                    r.get("street", ""),
                    r.get("city", ""),
                    state,
                ]
            )
        except Exception as e:
            LOG.error("Row append failed %s", e)

        if phone:
            send_sms(phone, first, r.get("street", ""))

        add_to_vault(zpid)
        vault_set.add(zpid)

# ---------- entry ----------
if __name__ == "__main__":
    try:
        payload = json.load(sys.stdin)
    except Exception:
        LOG.debug("No stdin payload; exiting")
        sys.exit(0)

    fresh_rows = payload.get("listings") or payload.get("rows") or []
    if not fresh_rows:
        LOG.debug("Empty payload")
        sys.exit(0)

    LOG.debug("Sample fields on first fresh row: %s", list(fresh_rows[0].keys()))
    process_rows(fresh_rows)

