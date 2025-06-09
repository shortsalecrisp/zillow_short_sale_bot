import os, json, logging, re, time, html, requests
from collections import defaultdict
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

CS_API_KEY = os.environ["CS_API_KEY"]
CS_CX      = os.environ["CS_CX"]
GSHEET_ID  = os.environ["GSHEET_ID"]
SC_JSON    = json.loads(os.environ["GCP_SERVICE_ACCOUNT_JSON"])
SCOPES     = ["https://www.googleapis.com/auth/spreadsheets"]

logging.basicConfig(level=os.getenv("LOGLEVEL", "DEBUG"),
                    format="%(asctime)s %(levelname)s: %(message)s")
LOG = logging.getLogger("bot")

# ---------- SMSMobile settings ----------
SMS_ENABLE      = os.getenv("SMSM_ENABLE", "false").lower() == "true"
SMS_TEST_MODE   = os.getenv("SMSM_TEST_MODE", "true").lower() == "true"
SMS_TEST_NUMBER = os.getenv("SMSM_TEST_NUMBER", "")
SMS_API_KEY     = os.getenv("SMSM_API_KEY", "")
SMS_FROM        = os.getenv("SMSM_FROM", "")
SMS_TEMPLATE    = os.getenv("SMSM_TEMPLATE") or (
    "Hey {first}, this is Yoni Kutler—I saw your short-sale listing at {address} and "
    "wanted to introduce myself. I specialize in helping agents get faster bank approvals "
    "and ensure these deals close. No cost to you—I’m paid by the buyer at closing. "
    "Open to a quick call to see if this could help?"
)
# ----------------------------------------

SHORT_RE = re.compile(r"\bshort\s+sale\b", re.I)
BAD_RE   = re.compile(r"approved|negotiator|settlement fee|fee at closing", re.I)
TEAM_RE  = re.compile(r"^\s*the\b|\bteam\b", re.I)

IMG_EXT  = (".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp")
PHONE_RE = re.compile(r"(?<!\d)(?:\+?1[\s\-\.]*)?\(?\d{3}\)?[\s\-\.]*\d{3}[\s\-\.]*\d{4}(?!\d)")
EMAIL_RE = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.I)

def is_short_sale(txt: str) -> bool:
    return bool(SHORT_RE.search(txt)) and not BAD_RE.search(txt)

def fmt_phone(raw: str) -> str:
    d = re.sub(r"\D", "", raw)
    if len(d) == 11 and d.startswith("1"):
        d = d[1:]
    return f"{d[:3]}-{d[3:6]}-{d[6:]}" if len(d) == 10 else ""

US_AREA_CODES = set(str(i) for i in range(201, 990))

def valid_phone(p: str) -> bool:
    if phonenumbers:
        try:
            return phonenumbers.is_possible_number(phonenumbers.parse(p, "US"))
        except Exception:
            return False
    return bool(re.fullmatch(r"\d{3}-\d{3}-\d{4}", p)) and p[:3] in US_AREA_CODES

def ok_email(a: str) -> bool:
    return not a.lower().endswith(IMG_EXT)

LABEL_TABLE = {"mobile":4,"cell":4,"direct":4,"text":4,"c:":4,"m:":4,
               "phone":2,"tel":2,"p:":2,
               "office":1,"main":1,"customer":1,"footer":1}
LABEL_RE = re.compile(r"(" + "|".join(map(re.escape, LABEL_TABLE)) + r")", re.I)

def proximity_scan(t: str) -> dict[str, tuple[int,int]]:
    out = {}
    for m in PHONE_RE.finditer(t):
        p = fmt_phone(m.group())
        if not valid_phone(p): continue
        sn = t[max(m.start()-80,0):min(m.end()+80,len(t))]
        lab = LABEL_RE.search(sn)
        w   = LABEL_TABLE.get(lab.group().lower(),0) if lab else 0
        if w < 2: continue
        s = 2 + w
        bw, ts = out.get(p, (0,0))
        out[p] = (max(bw, w), ts + s)
    return out

def extract_struct(td: str) -> tuple[list[str], list[str]]:
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
            if tel:  phones.append(fmt_phone(tel))
            if mail: mails.append(mail)
    for a in soup.select('a[href^="tel:"]'):
        phones.append(fmt_phone(a["href"].split("tel:")[-1]))
    for a in soup.select('a[href^="mailto:"]'):
        mails.append(a["href"].split("mailto:")[-1])
    return phones, mails

creds           = Credentials.from_service_account_info(SC_JSON, scopes=SCOPES)
sheets_service  = build("sheets", "v4", credentials=creds, cache_discovery=False)
gc              = gspread.authorize(creds)
ws              = gc.open_by_key(GSHEET_ID).sheet1

def phone_exists(p: str) -> bool:
    try:
        return p in ws.col_values(3)   # column C
    except Exception:
        return False

def append(values: list[str]):
    sheets_service.spreadsheets().values().append(
        spreadsheetId=GSHEET_ID, range="Sheet1!A1",
        valueInputOption="RAW", body={"values": [values]}
    ).execute()

def fetch(url: str) -> str | None:
    for u in (url, f"https://r.jina.ai/http://{url}"):
        try:
            r = requests.get(u, timeout=10, headers={"User-Agent":"Mozilla/5.0"})
            if r.status_code != 200 and r.status_code not in (403,429,999):
                continue
            t = r.text
            if "unusual traffic" in t[:600].lower():
                continue
            return t
        except Exception:
            continue
    return None

AGENT_SITES = [
    "realtor.com","zillow.com","redfin.com","homesnap.com","kw.com","remax.com",
    "coldwellbanker.com","compass.com","exprealty.com","bhhs.com","c21.com",
    "realtyonegroup.com","mlsmatrix.com","mlslistings.com","har.com","brightmlshomes.com"
]
DOMAIN_CLAUSE = " OR ".join(f"site:{d}" for d in AGENT_SITES)

def build_phone_q(a: str, s: str) -> str:
    return f'"{a}" {s} ("mobile" OR "cell" OR "direct") phone email ({DOMAIN_CLAUSE})'

def build_email_q(a: str, s: str) -> str:
    return f'realtor {a} email address in {s} ({DOMAIN_CLAUSE})'

cache: dict[str, tuple[str,str]] = {}

def google_cse(q: str):
    try:
        return requests.get(
            "https://www.googleapis.com/customsearch/v1",
            params={"key":CS_API_KEY,"cx":CS_CX,"q":q,"num":10},
            timeout=10
        ).json().get("items", [])
    except Exception:
        return []

def lookup_phone(a: str, s: str) -> str:
    q = build_phone_q(a, s)
    items = google_cse(q)
    cand: dict[str, tuple[int,int]] = {}
    for it in items:
        u = it.get("link","")
        t = fetch(u)
        if not t or a.lower() not in t.lower(): continue
        ph,_ = extract_struct(t)
        for p in ph:
            pf = fmt_phone(p)
            if valid_phone(pf):
                bw,ts = cand.get(pf,(0,0))
                cand[pf] = (4, ts+6)
        low = html.unescape(t.lower())
        for p,(bw,sc) in proximity_scan(low).items():
            b,tos = cand.get(p,(0,0))
            cand[p] = (max(bw,b), tos+sc)
    if not cand:
        return ""
    phone = max(cand.items(), key=lambda kv:(kv[1][0],kv[1][1]))[0]
    if phone[:3] not in US_AREA_CODES:
        return ""
    return phone

def lookup_email(a: str, s: str) -> str:
    q = build_email_q(a, s)
    items = google_cse(q)
    cand: dict[str,int] = defaultdict(int)
    last = a.split()[-1].lower()
    for it in items:
        u = it.get("link","")
        t = fetch(u)
        if not t or a.lower() not in t.lower(): continue
        _,em = extract_struct(t)
        for m in em:
            if ok_email(m): cand[m]+=3
        for m in EMAIL_RE.findall(t):
            if ok_email(m) and last in m.lower():
                cand[m]+=1
    if not cand:
        return ""
    return max(cand, key=cand.get)

def send_sms(to_number: str, first: str, address: str) -> bool:
    if not SMS_ENABLE:
        LOG.debug("SMS disabled")
        return False
    digits = re.sub(r"\D", "", to_number)
    if len(digits) == 10:
        to_e164 = "+1" + digits
    elif len(digits) == 11 and digits.startswith("1"):
        to_e164 = "+" + digits
    else:
        LOG.error("Bad phone %s", to_number)
        return False
    if SMS_TEST_MODE and SMS_TEST_NUMBER:
        test_digits = re.sub(r"\D", "", SMS_TEST_NUMBER)
        to_e164 = "+1" + test_digits if len(test_digits) == 10 else "+" + test_digits
    text = SMS_TEMPLATE.format(first=first, address=address)
    payload = {"key": SMS_API_KEY, "to": to_e164, "from": SMS_FROM, "text": text}
    try:
        resp = requests.post("https://smsmobileapi.com/api/v1/messages",
                             json=payload, timeout=15)
    except Exception as exc:
        LOG.error("SMS request exception: %s", exc)
        return False
    if resp.status_code != 200:
        LOG.error("SMS failed (%s) %s", resp.status_code, resp.text[:400])
        return False
    LOG.info("SMS sent to %s", to_e164)
    return True

def extract_name(txt: str) -> str | None:
    m = re.search(r"listing agent[:\s-]*([A-Za-z \.\'’-]{3,})", txt, re.I)
    if m:
        nm = m.group(1).strip()
        if not TEAM_RE.search(nm):
            return nm
    return None

def process_rows(rows: list[dict]):
    for r in rows:
        if not is_short_sale(r.get("description","")):
            continue
        name = r.get("agentName","").strip()
        if not name:
            name = extract_name(r.get("openai_summary","") + "\n" +
                                r.get("description",""))
            if not name:
                continue
        if TEAM_RE.search(name):
            alt = extract_name(r.get("openai_summary","") + "\n" +
                               r.get("description",""))
            if alt:
                name = alt
            else:
                continue
        if TEAM_RE.search(name):
            continue

        state  = r.get("state","")
        phone  = lookup_phone(name, state)
        email  = lookup_email(name, state)

        phone  = fmt_phone(phone)
        if phone and phone_exists(phone):
            continue

        first,*last = name.split()
        append([first, " ".join(last), phone, email,
                r.get("street",""), r.get("city",""), state])

        if phone:
            send_sms(phone, first, r.get("street",""))

