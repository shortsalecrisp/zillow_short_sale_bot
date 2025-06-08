import os, json, logging, re, requests, time, html
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
CS_CX = os.environ["CS_CX"]
GSHEET_ID = os.environ["GSHEET_ID"]
SC_JSON = json.loads(os.environ["GCP_SERVICE_ACCOUNT_JSON"])
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

logging.basicConfig(level=os.getenv("LOGLEVEL", "DEBUG"), format="%(asctime)s %(levelname)s: %(message)s")
LOG = logging.getLogger("bot")

SHORT_RE = re.compile(r"\bshort\s+sale\b", re.I)
BAD_RE = re.compile(r"approved|negotiator|settlement fee|fee at closing", re.I)
TEAM_RE = re.compile(r"^\s*the\b|\bteam\b", re.I)

def is_short_sale(txt: str) -> bool:
    return bool(SHORT_RE.search(txt)) and not BAD_RE.search(txt)

IMG_EXT = (".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp")
PHONE_RE = re.compile(r"(?<!\d)(?:\+?1[\s\-\.]*)?\(?\d{3}\)?[\s\-\.]*\d{3}[\s\-\.]*\d{4}(?!\d)")
EMAIL_RE = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.I)

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
    if a.lower().endswith(IMG_EXT):
        return False
    if re.search(r"\.(gov|edu|mil)$", a, re.I):
        return False
    return True

LABEL_TABLE = {"mobile": 4, "cell": 4, "direct": 4, "text": 4, "c:": 4, "m:": 4,
               "phone": 2, "tel": 2, "p:": 2,
               "office": 1, "main": 1, "customer": 1, "footer": 1}
LABEL_RE = re.compile(r"(" + "|".join(map(re.escape, LABEL_TABLE)) + r")", re.I)

def proximity_scan(t: str) -> dict[str, tuple[int, int]]:
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
        s = 2 + w
        bw, ts = out.get(p, (0, 0))
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
            tel = data.get("telephone") or (data.get("contactPoint") or {}).get("telephone")
            mail = data.get("email") or (data.get("contactPoint") or {}).get("email")
            if tel:
                phones.append(fmt_phone(tel))
            if mail:
                mails.append(mail)
    for a in soup.select('a[href^="tel:"]'):
        phones.append(fmt_phone(a["href"].split("tel:")[-1]))
    for a in soup.select('a[href^="mailto:"]'):
        mails.append(a["href"].split("mailto:")[-1])
    return phones, mails

creds = Credentials.from_service_account_info(SC_JSON, scopes=SCOPES)
sheets_service = build("sheets", "v4", credentials=creds, cache_discovery=False)
gc = gspread.authorize(creds)
ws = gc.open_by_key(GSHEET_ID).sheet1

def phone_exists(p: str) -> bool:
    try:
        return p in ws.col_values(3)
    except Exception:
        return False

def append(r: list[str]):
    sheets_service.spreadsheets().values().append(
        spreadsheetId=GSHEET_ID, range="Sheet1!A1",
        valueInputOption="RAW", body={"values": [r]}).execute()

def fetch(url: str) -> str | None:
    for u in (url, f"https://r.jina.ai/http://{url}"):
        try:
            r = requests.get(u, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
            if r.status_code != 200 and r.status_code not in (403, 429, 999):
                continue
            t = r.text
            if "unusual traffic" in t[:600].lower():
                continue
            return t
        except Exception:
            continue
    return None

AGENT_SITES = ["realtor.com", "zillow.com", "redfin.com", "homesnap.com", "kw.com", "remax.com",
               "coldwellbanker.com", "compass.com", "exprealty.com", "bhhs.com", "c21.com",
               "realtyonegroup.com", "mlsmatrix.com", "mlslistings.com", "har.com", "brightmlshomes.com"]
DOMAIN_CLAUSE = " OR ".join(f"site:{d}" for d in AGENT_SITES)
SOCIAL_CLAUSE = "site:facebook.com OR site:linkedin.com"

def build_q(a: str, s: str) -> list[str]:
    return [
        f'"{a}" {s} ("mobile" OR "cell" OR "direct") phone email ({DOMAIN_CLAUSE})',
        f'"{a}" {s} phone email ({DOMAIN_CLAUSE})',
        f'"{a}" {s} contact ({DOMAIN_CLAUSE})'
    ]

AREA_BY_STATE = {"FL": ["305", "321", "352", "386", "407", "561", "727", "813", "850", "863", "904", "941", "954", "772", "786"],
                 "GA": ["404", "470", "478", "678", "706", "770", "912"],
                 "CA": ["209", "213", "310", "323", "408", "415", "424", "510", "559", "562", "619", "626", "650", "657",
                        "661", "707", "714", "747", "760", "805", "818", "831", "858", "909", "916", "925", "949", "951"],
                 "TX": ["210", "214", "254", "281", "325", "346", "361", "409", "430", "432", "469", "512", "682", "713", "737",
                        "806", "817", "830", "832", "903", "915", "936", "940", "956", "972", "979"],
                 "IL": ["217", "224", "309", "312", "331", "618", "630", "708", "773", "779", "815", "847", "872"],
                 "OK": ["405", "539", "580", "918"]}

cache: dict[str, tuple[str, str]] = {}

def realtor_fb(a: str, s: str) -> tuple[str, str]:
    f, *l = a.split()
    if not l:
        return "", ""
    url = f"https://www.realtor.com/realestateagents/{'-'.join([f.lower()] + l).lower()}_{s.lower()}"
    t = fetch(url)
    if not t:
        return "", ""
    ph, em = extract_struct(t)
    return next((p for p in ph if valid_phone(p)), ""), (em[0] if em else "")

def extra_email_search(a: str, s: str) -> dict[str, int]:
    out: dict[str, int] = {}
    queries = [
        f'realtor {a} email address in {s} ({DOMAIN_CLAUSE} OR {SOCIAL_CLAUSE})',
        f'"{a}" {s} real estate email ({DOMAIN_CLAUSE} OR {SOCIAL_CLAUSE})'
    ]
    last = a.split()[-1].lower()
    for q in queries:
        time.sleep(0.25)
        try:
            items = requests.get("https://www.googleapis.com/customsearch/v1",
                                 params={"key": CS_API_KEY, "cx": CS_CX, "q": q, "num": 10},
                                 timeout=10).json().get("items", [])
        except Exception:
            continue
        for it in items:
            pm = it.get("pagemap", {})
            mail = pm.get("contactpoint", [{}])[0].get("email")
            if mail and ok_email(mail) and last in mail.lower():
                out[mail] += 3
            u = it.get("link", "")
            t = fetch(u)
            if not t:
                continue
            for m in EMAIL_RE.findall(t):
                if ok_email(m) and last in m.lower():
                    out[m] += 2
        if out:
            break
    return out

def lookup(a: str, s: str) -> tuple[str, str]:
    if not a.strip():
        return "", ""
    k = f"{a}|{s}"
    if k in cache:
        return cache[k]
    cand_p: dict[str, tuple[int, int]] = {}
    cand_e: dict[str, int] = defaultdict(int)
    last = a.split()[-1].lower()
    for q in build_q(a, s):
        time.sleep(0.25)
        try:
            items = requests.get("https://www.googleapis.com/customsearch/v1",
                                 params={"key": CS_API_KEY, "cx": CS_CX, "q": q, "num": 10},
                                 timeout=10).json().get("items", [])
        except Exception:
            continue
        for it in items:
            m = it.get("pagemap", {})
            tel = m.get("contactpoint", [{}])[0].get("telephone")
            mail = m.get("contactpoint", [{}])[0].get("email")
            if tel:
                p = fmt_phone(tel)
                if valid_phone(p):
                    cand_p[p] = (4, 8)
            if mail and ok_email(mail):
                cand_e[mail] += 3
        for it in items:
            u = it.get("link", "")
            t = fetch(u)
            if not t or a.lower() not in t.lower():
                continue
            ph, em = extract_struct(t)
            for p in ph:
                pf = fmt_phone(p)
                if valid_phone(pf):
                    bw, ts = cand_p.get(pf, (0, 0))
                    cand_p[pf] = (4, ts + 6)
            for m in em:
                if ok_email(m):
                    cand_e[m] += 3
            low = html.unescape(t.lower())
            for p, (bw, sc) in proximity_scan(low).items():
                b, tos = cand_p.get(p, (0, 0))
                cand_p[p] = (max(bw, b), tos + sc)
            for m in EMAIL_RE.findall(low):
                if ok_email(m) and last in m.lower():
                    cand_e[m] += 1
        if cand_p and cand_e:
            break
    if not cand_p:
        fp, fe = realtor_fb(a, s)
        if fp:
            cand_p[fp] = (3, 3)
        if fe and ok_email(fe):
            cand_e[fe] = 2
    if not cand_e:
        cand_e.update(extra_email_search(a, s))
    phone = ""
    if cand_p:
        phone = max(cand_p.items(), key=lambda kv: (kv[1][0], kv[1][1]))[0]
    if phone and phone[:3] not in AREA_BY_STATE.get(s.upper(), []):
        phone = ""
    if not phone and cand_p:
        phone = list(cand_p.keys())[0]
    email = max(cand_e, key=cand_e.get) if cand_e else ""
    cache[k] = (phone, email)
    return phone, email

def extract_name(txt: str) -> str | None:
    m = re.search(r"listing agent[:\s-]*([A-Za-z \.\'’-]{3,})", txt, re.I)
    if m:
        nm = m.group(1).strip()
        if not TEAM_RE.search(nm):
            return nm
    return None

def process_rows(rows: list[dict]):
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
        phone, email = lookup(name, r.get("state", ""))
        phone = fmt_phone(phone)
        if phone and phone_exists(phone):
            continue
        first, *last = name.split()
        append([first, " ".join(last), phone, email,
                r.get("street", ""), r.get("city", ""), r.get("state", "")])

