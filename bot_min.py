import os, json, logging, re, time, html, requests
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

CS_API_KEY = os.environ["CS_API_KEY"]
CS_CX      = os.environ["CS_CX"]
GSHEET_ID  = os.environ["GSHEET_ID"]
SC_JSON    = json.loads(os.environ["GCP_SERVICE_ACCOUNT_JSON"])
SCOPES     = ["https://www.googleapis.com/auth/spreadsheets"]

logging.basicConfig(level=os.getenv("LOGLEVEL", "DEBUG"),
                    format="%(asctime)s %(levelname)s: %(message)s")
LOG = logging.getLogger("bot")

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

SHORT_RE = re.compile(r"\bshort\s+sale\b", re.I)
BAD_RE   = re.compile(r"approved|negotiator|settlement fee|fee at closing", re.I)
TEAM_RE  = re.compile(r"^\s*the\b|\bteam\b", re.I)

IMG_EXT  = (".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp")
PHONE_RE = re.compile(r"(?<!\d)(?:\+?1[\s\-\.]*)?\(?\d{3}\)?[\s\-\.]*\d{3}[\s\-\.]*\d{4}(?!\d)")
EMAIL_RE = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.I)

def is_short_sale(t: str) -> bool:
    return bool(SHORT_RE.search(t)) and not BAD_RE.search(t)

def fmt_phone(r: str) -> str:
    d = re.sub(r"\D", "", r)
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

def clean_email(e: str) -> str:
    return e.split("?")[0].strip()

def ok_email(e: str) -> bool:
    e = clean_email(e)
    if not e or "@" not in e:
        return False
    if e.lower().endswith(IMG_EXT):
        return False
    if re.search(r"\.(gov|edu|mil)$", e, re.I):
        return False
    return True

LABEL_TABLE = {"mobile":4,"cell":4,"direct":4,"text":4,"c:":4,"m:":4,
               "phone":2,"tel":2,"p:":2,
               "office":1,"main":1,"customer":1,"footer":1}
LABEL_RE = re.compile(r"(" + "|".join(map(re.escape, LABEL_TABLE)) + r")", re.I)

def proximity_scan(t: str) -> dict[str, tuple[int, int]]:
    out = {}
    for m in PHONE_RE.finditer(t):
        p = fmt_phone(m.group())
        if not valid_phone(p):
            continue
        sn = t[max(m.start()-80,0):min(m.end()+80,len(t))]
        lab = LABEL_RE.search(sn)
        w   = LABEL_TABLE.get(lab.group().lower(),0) if lab else 0
        if w < 2:
            continue
        bw, ts = out.get(p, (0,0))
        out[p] = (max(bw, w), ts + 2 + w)
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
            if tel:
                phones.append(fmt_phone(tel))
            if mail:
                mails.append(clean_email(mail))
    for a in soup.select('a[href^="tel:"]'):
        phones.append(fmt_phone(a["href"].split("tel:")[-1]))
    for a in soup.select('a[href^="mailto:"]'):
        mails.append(clean_email(a["href"].split("mailto:")[-1]))
    return phones, mails

creds          = Credentials.from_service_account_info(SC_JSON, scopes=SCOPES)
sheets_service = build("sheets", "v4", credentials=creds, cache_discovery=False)
gc             = gspread.authorize(creds)
ws             = gc.open_by_key(GSHEET_ID).sheet1

def phone_exists(p: str) -> bool:
    try:
        return p in ws.col_values(3)
    except Exception:
        return False

def append(v: list[str]):
    sheets_service.spreadsheets().values().append(
        spreadsheetId=GSHEET_ID, range="Sheet1!A1",
        valueInputOption="RAW", body={"values": [v]}
    ).execute()

def fetch(u: str) -> str | None:
    for x in (u, f"https://r.jina.ai/http://{u}"):
        try:
            r = requests.get(x, timeout=10, headers={"User-Agent":"Mozilla/5.0"})
            if r.status_code not in (200,403,429,999):
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
SOCIAL_CLAUSE = "site:facebook.com OR site:linkedin.com"

AREA_BY_STATE = {"FL":["305","321","352","386","407","561","727","813","850","863","904","941","954","772","786"],
                 "GA":["404","470","478","678","706","770","912"],
                 "CA":["209","213","310","323","408","415","424","510","559","562","619","626","650","657",
                        "661","707","714","747","760","805","818","831","858","909","916","925","949","951"],
                 "TX":["210","214","254","281","325","346","361","409","430","432","469","512","682","713","737",
                        "806","817","830","832","903","915","936","940","956","972","979"],
                 "IL":["217","224","309","312","331","618","630","708","773","779","815","847","872"],
                 "OK":["405","539","580","918"]}

def google_items(q: str) -> list[dict]:
    try:
        return requests.get("https://www.googleapis.com/customsearch/v1",
                            params={"key":CS_API_KEY,"cx":CS_CX,"q":q,"num":10},
                            timeout=10).json().get("items",[])
    except Exception:
        return []

def build_q_phone(a: str, s: str) -> list[str]:
    return [
        f'"{a}" {s} ("mobile" OR "cell" OR "direct") phone ({DOMAIN_CLAUSE})',
        f'"{a}" {s} phone ({DOMAIN_CLAUSE})',
        f'"{a}" {s} contact ({DOMAIN_CLAUSE})'
    ]

def build_q_email(a: str, s: str) -> list[str]:
    return [
        f'"{a}" {s} email ({DOMAIN_CLAUSE})',
        f'"{a}" {s} contact email ({DOMAIN_CLAUSE})'
    ]

cache_p: dict[str,str] = {}
cache_e: dict[str,str] = {}

def lookup_phone(a: str, s: str) -> str:
    k = f"{a}|{s}"
    if k in cache_p:
        return cache_p[k]
    cand: dict[str, tuple[int,int]] = {}
    for q in build_q_phone(a, s):
        time.sleep(0.25)
        for it in google_items(q):
            pm = it.get("pagemap", {})
            tel = pm.get("contactpoint", [{}])[0].get("telephone")
            if tel:
                p = fmt_phone(tel)
                if valid_phone(p):
                    cand[p] = (4, 8)
        for it in google_items(q):
            u = it.get("link", "")
            t = fetch(u)
            if not t or a.lower() not in t.lower():
                continue
            ph, _ = extract_struct(t)
            for p in ph:
                pf = fmt_phone(p)
                if valid_phone(pf):
                    bw, ts = cand.get(pf, (0,0))
                    cand[pf] = (4, ts+6)
            low = html.unescape(t.lower())
            for p,(bw,sc) in proximity_scan(low).items():
                b, ts = cand.get(p,(0,0))
                cand[p] = (max(bw,b), ts+sc)
        if cand:
            break
    phone = ""
    if cand:
        phone = max(cand.items(), key=lambda kv:(kv[1][0], kv[1][1]))[0]
    if phone and phone[:3] not in AREA_BY_STATE.get(s.upper(), []):
        phone = ""
    if not phone and cand:
        phone = list(cand.keys())[0]
    cache_p[k] = phone
    return phone

def extra_email_search(a: str, s: str) -> dict[str,int]:
    out: defaultdict[str,int] = defaultdict(int)
    last = a.split()[-1].lower()
    qs = [
        f'realtor {a} email address in {s} ({DOMAIN_CLAUSE} OR {SOCIAL_CLAUSE})',
        f'"{a}" {s} real estate email ({DOMAIN_CLAUSE} OR {SOCIAL_CLAUSE})'
    ]
    for q in qs:
        time.sleep(0.25)
        for it in google_items(q):
            pm   = it.get("pagemap", {})
            mail = clean_email(pm.get("contactpoint", [{}])[0].get("email",""))
            if ok_email(mail) and last in mail.lower():
                out[mail] += 3
            u = it.get("link","")
            t = fetch(u)
            if not t:
                continue
            for m in EMAIL_RE.findall(t):
                m = clean_email(m)
                if ok_email(m) and last in m.lower():
                    out[m] += 2
        if out:
            break
    return out

def lookup_email(a: str, s: str) -> str:
    k = f"{a}|{s}"
    if k in cache_e:
        return cache_e[k]
    cand: defaultdict[str,int] = defaultdict(int)
    last = a.split()[-1].lower()
    for q in build_q_email(a, s):
        time.sleep(0.25)
        for it in google_items(q):
            pm   = it.get("pagemap", {})
            mail = clean_email(pm.get("contactpoint", [{}])[0].get("email",""))
            if ok_email(mail):
                cand[mail] += 3
        for it in google_items(q):
            u = it.get("link","")
            t = fetch(u)
            if not t or a.lower() not in t.lower():
                continue
            _,em = extract_struct(t)
            for m in em:
                m = clean_email(m)
                if ok_email(m):
                    cand[m] += 3
            for m in EMAIL_RE.findall(t):
                m = clean_email(m)
                if ok_email(m) and last in m.lower():
                    cand[m] += 1
        if cand:
            break
    if not cand:
        cand.update(extra_email_search(a, s))
    email = max(cand, key=cand.get) if cand else ""
    cache_e[k] = email
    return email

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
        td = re.sub(r"\D", "", SMS_TEST_NUMBER)
        to_e164 = "+1" + td if len(td) == 10 else "+" + td
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

def extract_name(t: str) -> str | None:
    m = re.search(r"listing agent[:\s-]*([A-Za-z \.\'’-]{3,})", t, re.I)
    if m:
        n = m.group(1).strip()
        if not TEAM_RE.search(n):
            return n
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
        state = r.get("state","")
        phone = fmt_phone(lookup_phone(name, state))
        email = lookup_email(name, state)
        if phone and phone_exists(phone):
            continue
        first,*last = name.split()
        append([first, " ".join(last), phone, email,
                r.get("street",""), r.get("city",""), state])
        if phone:
            send_sms(phone, first, r.get("street",""))

