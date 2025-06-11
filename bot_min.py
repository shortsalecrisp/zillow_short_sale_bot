import os, json, logging, re, time, html, requests, datetime
from collections import defaultdict

try:
    import phonenumbers
except ImportError:
    phonenumbers = None
try:
    from bs4 import BeautifulSoup
except ImportError:
    BeautifulSoup = None

from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
import gspread

CS_API_KEY = os.environ["CS_API_KEY"]
CS_CX = os.environ["CS_CX"]
GSHEET_ID = os.environ["GSHEET_ID"]
SC_JSON = json.loads(os.environ["GCP_SERVICE_ACCOUNT_JSON"])
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

logging.basicConfig(level=os.getenv("LOGLEVEL", "DEBUG"),
                    format="%(asctime)s %(levelname)s: %(message)s")
LOG = logging.getLogger("bot")

SMS_ENABLE = os.getenv("SMSM_ENABLE", "false").lower() == "true"
SMS_TEST_MODE = os.getenv("SMSM_TEST_MODE", "true").lower() == "true"
SMS_TEST_NUMBER = os.getenv("SMSM_TEST_NUMBER", "")
SMS_API_KEY = os.getenv("SMSM_API_KEY", "")
SMS_FROM = os.getenv("SMSM_FROM", "")
SMS_URL = os.getenv("SMSM_URL", "https://api.smsmobileapi.com/sendsms/")
SMS_TEMPLATE = os.getenv("SMSM_TEMPLATE") or (
    "Hey {first}, this is Yoni Kutler—I saw your short-sale listing at {address} "
    "and wanted to introduce myself. I specialize in helping agents get faster bank "
    "approvals and ensure these deals close. No cost to you—I’m paid by the buyer "
    "at closing. Open to a quick call to see if this could help?"
)

MAX_Q_PHONE = 3
MAX_Q_EMAIL = 3

SHORT_RE = re.compile(r"\bshort\s+sale\b", re.I)
BAD_RE = re.compile(r"approved|negotiator|settlement fee|fee at closing", re.I)
TEAM_RE = re.compile(r"^\s*the\b|\bteam\b", re.I)

IMG_EXT = (".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp")
PHONE_RE = re.compile(r"(?<!\d)(?:\+?1[\s\-\.]*)?\(?\d{3}\)?[\s\-\.]*\d{3}[\s\-\.]*\d{4}(?!\d)")
EMAIL_RE = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.I)

LABEL_TABLE = {"mobile": 4, "cell": 4, "direct": 4, "text": 4, "c:": 4, "m:": 4,
               "phone": 2, "tel": 2, "p:": 2, "office": 1, "main": 1, "customer": 1, "footer": 1}
LABEL_RE = re.compile("(" + "|".join(map(re.escape, LABEL_TABLE)) + ")", re.I)
US_AREA_CODES = {str(i) for i in range(201, 990)}

creds = Credentials.from_service_account_info(SC_JSON, scopes=SCOPES)
sheets_service = build("sheets", "v4", credentials=creds, cache_discovery=False)
gc = gspread.authorize(creds)
ws = gc.open_by_key(GSHEET_ID).sheet1

AGENT_SITES = ["realtor.com", "zillow.com", "redfin.com", "homesnap.com", "kw.com", "remax.com",
               "coldwellbanker.com", "compass.com", "exprealty.com", "bhhs.com", "c21.com",
               "realtyonegroup.com", "mlsmatrix.com", "mlslistings.com", "har.com", "brightmlshomes.com"]
DOMAIN_CLAUSE = " OR ".join(f"site:{d}" for d in AGENT_SITES)
SOCIAL_CLAUSE = "site:facebook.com OR site:linkedin.com"

cache_p, cache_e, cache_fail = {}, {}, {}

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
        sn = t[max(m.start() - 80, 0):min(m.end() + 80, len(t))]
        lab = LABEL_RE.search(sn)
        w = LABEL_TABLE.get(lab.group().lower(), 0) if lab else 0
        if w < 2:
            continue
        bw, ts = out.get(p, (0, 0))
        out[p] = (max(bw, w), ts + 2 + w)
    return out

def extract_struct(td):
    phones, mails = [], []
    if not BeautifulSoup:
        return phones, mails
    soup = BeautifulSoup(td, "html.parser")
    for sc in soup.find_all("script", {"type": "application/ld+json"}):
        try:
            data = json.loads(sc.string or "")
        except:
            continue
        if isinstance(data, list):
            data = data[0]
        if isinstance(data, dict):
            tel = data.get("telephone") or (data.get("contactPoint") or {}).get("telephone")
            mail = data.get("email") or (data.get("contactPoint") or {}).get("email")
            if tel:
                phones.append(fmt_phone(tel))
            if mail:
                mails.append(clean_email(mail))
    for a in soup.select('a[href^="tel:"]'):
        phones.append(fmt_phone(a["href"].split("tel:")[-1]))
    for a in soup.select('a[href^="mailto:"]'):
        mails.append(clean_email(a["href"].split("mailto:")[-1]))
    return phones, mails

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
        body={"values": [v]}
    ).execute()

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

def google_items(q):
    for attempt in range(4):
        try:
            r = requests.get("https://www.googleapis.com/customsearch/v1",
                             params={"key": CS_API_KEY, "cx": CS_CX, "q": q, "num": 10},
                             timeout=10)
        except:
            time.sleep(1)
            continue
        if r.status_code == 200:
            return r.json().get("items", [])
        if r.status_code == 429:
            time.sleep(1.5 * (attempt + 1))
            continue
        break
    return []

def build_q_phone(a, s):
    return [
        f'"{a}" {s} ("mobile" OR "cell" OR "direct") phone ({DOMAIN_CLAUSE})',
        f'"{a}" {s} phone ({DOMAIN_CLAUSE})',
        f'"{a}" {s} contact ({DOMAIN_CLAUSE})'
    ]

def build_q_email(a, s):
    return [
        f'"{a}" {s} email ({DOMAIN_CLAUSE})',
        f'"{a}" {s} contact email ({DOMAIN_CLAUSE})',
        f'"{a}" {s} real estate email ({DOMAIN_CLAUSE} OR {SOCIAL_CLAUSE})'
    ]

def lookup_phone(a, s, listing_url=""):
    k = f"{a}|{s}"
    if k in cache_p:
        return cache_p[k]
    if k in cache_fail and (datetime.datetime.utcnow() - cache_fail[k]).seconds < 3600:
        return ""
    cand = {}
    for q in build_q_phone(a, s)[:MAX_Q_PHONE]:
        time.sleep(0.25)
        for it in google_items(q):
            tel = it.get("pagemap", {}).get("contactpoint", [{}])[0].get("telephone")
            if tel:
                p = fmt_phone(tel)
                if valid_phone(p):
                    cand[p] = (4, 8)
        if cand:
            break
    if not cand and listing_url:
        t = fetch(listing_url)
        if t:
            ph, _ = extract_struct(t)
            for p in ph:
                pf = fmt_phone(p)
                if valid_phone(pf):
                    cand[pf] = (4, 6)
            low = html.unescape(t.lower())
            for p, (bw, sc) in proximity_scan(low).items():
                b, ts = cand.get(p, (0, 0))
                cand[p] = (max(bw, b), ts + sc)
    if not cand:
        cache_fail[k] = datetime.datetime.utcnow()
    phone = max(cand, key=lambda k: cand[k]) if cand else ""
    cache_p[k] = phone
    return phone

def lookup_email(a, s, listing_url=""):
    k = f"{a}|{s}"
    if k in cache_e:
        return cache_e[k]
    if k in cache_fail and (datetime.datetime.utcnow() - cache_fail[k]).seconds < 3600:
        return ""
    cand = defaultdict(int)
    for q in build_q_email(a, s)[:MAX_Q_EMAIL]:
        time.sleep(0.25)
        for it in google_items(q):
            mail = clean_email(it.get("pagemap", {}).get("contactpoint", [{}])[0].get("email", ""))
            if ok_email(mail):
                cand[mail] += 3
        if cand:
            break
    if not cand and listing_url:
        t = fetch(listing_url)
        if t:
            _, em = extract_struct(t)
            for m in em:
                m = clean_email(m)
                if ok_email(m):
                    cand[m] += 3
            for m in EMAIL_RE.findall(t):
                m = clean_email(m)
                if ok_email(m):
                    cand[m] += 1
    if not cand:
        cache_fail[k] = datetime.datetime.utcnow()
    tokens = {re.sub(r"[^a-z]", "", w.lower()) for w in a.split()}
    good = {m: sc for m, sc in cand.items() if any(tok and tok in m.lower() for tok in tokens)}
    email = max(good, key=good.get) if good else ""
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
        td = re.sub(r"\D", "", SMS_TEST_NUMBER)
        to_e164 = "+1" + td if len(td) == 10 else "+" + td
    payload = {"recipients": to_e164,
               "message": SMS_TEMPLATE.format(first=first, address=address),
               "apikey": SMS_API_KEY,
               "sendsms": "1"}
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

def extract_name(t):
    m = re.search(r"listing agent[:\s-]*([A-Za-z \.'’-]{3,})", t, re.I)
    if m:
        n = m.group(1).strip()
        if not TEAM_RE.search(n):
            return n
    return None

def process_rows(rows):
    for r in rows:
        if not is_short_sale(r.get("description", "")):
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
        listing_url = r.get("detailUrl", "") or r.get("url", "")
        phone = fmt_phone(lookup_phone(name, state, listing_url))
        email = lookup_email(name, state, listing_url)
        if phone and phone_exists(phone):
            continue
        if not phone and not email:
            LOG.warning("No contact found for %s, %s", name, r.get("zpid", ""))
            continue
        first, *last = name.split()
        append_row([first, " ".join(last), phone, email,
                    r.get("street", ""), r.get("city", ""), state])
        if phone:
            send_sms(phone, first, r.get("street", ""))

