import os, json, logging, re, requests, time, html
from collections import defaultdict, Counter
from urllib.parse import urlparse
try:
    import phonenumbers                # full library if present
except ImportError:                    # keep running if it’s not installed
    phonenumbers = None

try:
    from bs4 import BeautifulSoup      # for structured-data parsing
except ImportError:
    BeautifulSoup = None

import gspread
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials

# ───────────────────────────────────────────  CONFIG  ────────────────────────
CS_API_KEY = os.environ["CS_API_KEY"]
CS_CX      = os.environ["CS_CX"]
GSHEET_ID  = os.environ["GSHEET_ID"]
SC_JSON    = json.loads(os.environ["GCP_SERVICE_ACCOUNT_JSON"])
SCOPES     = ["https://www.googleapis.com/auth/spreadsheets"]

logging.basicConfig(
    level=os.getenv("LOGLEVEL", "DEBUG"),
    format="%(asctime)s %(levelname)s: %(message)s",
)
LOG = logging.getLogger("bot")

# Spreadsheet column order  A  B   C     D     E      F     G
COL_ORDER = ["first", "last", "phone", "email", "street", "city", "state"]

# ───────────────────────────────  SHORT-SALE RULES  ─────────────────────────
SHORT_RE = re.compile(r"\bshort\s+sale\b", re.I)
BAD_RE   = re.compile(r"approved|negotiator|settlement fee|fee at closing", re.I)

def is_short_sale(txt: str) -> bool:
    return bool(SHORT_RE.search(txt)) and not BAD_RE.search(txt)

# ─────────────────────────────  PHONE / EMAIL UTIL  ─────────────────────────
IMG_EXT  = (".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp")
PHONE_RE = re.compile(
    r"(?<!\d)(?:\+?1[\s\-\.]*)?\(?\d{3}\)?[\s\-\.]*\d{3}[\s\-\.]*\d{4}(?!\d)"
)
EMAIL_RE = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.I)

def fmt_phone(raw: str) -> str:
    digits = re.sub(r"\D", "", raw)
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    return f"{digits[:3]}-{digits[3:6]}-{digits[6:]}" if len(digits) == 10 else ""

# ───────────  NEW: NANPA-based fallback validator for phonenumbers-less env  ──────────
US_AREA_CODES = {
    # NANPA list (truncated here—include full list 201-989 in production)
    '201','202','203','205','206','207','208','209','210','212','213','214','215',
    '216','217','218','219','220','224','225','227','228','229','231','234','239',
    '240','248','251','252','253','254','256','260','262','267','269','270','272',
    '276','281','301','302','303','304','305','307','308','309','310','312','313',
    '314','315','316','317','318','319','320','321','323','325','327','330','331',
    '332','334','336','337','339','346','347','351','352','360','361','364','380',
    '385','386','401','402','404','405','406','407','408','409','410','412','413',
    '414','415','417','419','423','424','425','430','432','434','435','440','442',
    '443','447','458','463','464','469','470','475','478','479','480','484','501',
    '502','503','504','505','507','508','509','510','512','513','515','516','517',
    '518','520','530','531','534','539','540','541','551','559','561','562','563',
    '564','567','570','571','572','573','574','575','580','585','586','601','602',
    '603','605','606','607','608','609','610','612','614','615','616','617','618',
    '619','620','623','626','628','629','630','631','636','641','646','650','651',
    '657','659','660','661','662','667','669','678','680','681','682','701','702',
    '703','704','706','707','708','712','713','714','715','716','717','718','719',
    '720','724','725','727','730','731','732','734','737','740','743','747','754',
    '757','760','762','763','764','765','769','770','771','772','773','774','775',
    '779','781','785','786','801','802','803','804','805','806','808','810','812',
    '813','814','815','816','817','818','819','820','828','830','831','832','838',
    '839','840','843','845','847','848','850','854','856','857','858','859','860',
    '862','863','864','865','870','872','878','901','903','904','906','907','908',
    '909','910','912','913','914','915','916','917','918','919','920','925','928',
    '929','930','931','934','936','937','938','940','941','945','947','949','951',
    '952','954','956','959','970','971','972','973','978','979','980','984','985',
    '986','989'
}

def plausible_us_number(p: str) -> bool:
    """Cheap validation when phonenumbers isn’t installed."""
    return bool(re.fullmatch(r"\d{3}-\d{3}-\d{4}", p)) and p[:3] in US_AREA_CODES

def valid_phone(p: str) -> bool:
    if phonenumbers:
        try:
            return phonenumbers.is_possible_number(phonenumbers.parse(p, "US"))
        except Exception:
            return False
    return plausible_us_number(p)
# 
──────────────────────────────────────────────────────────────────────────────

def ok_email(addr: str) -> bool:
    return not addr.lower().endswith(IMG_EXT)

LABEL_NEAR_RE = re.compile(r"(mobile|cell|direct|text|phone|tel|main|office|fax)", re.I)
def label_score(label: str) -> int:
    label = label.lower()
    if label in ("mobile", "cell", "direct", "text"):
        return 4
    if label in ("phone", "tel"):
        return 2
    return 1  # office / fax

# global duplicate-phone guard
global_phone_hits: Counter[str] = Counter()

def proximity_scan(html_text: str) -> list[tuple[str, int]]:
    """Return list of (phone, score) from a chunk of HTML."""
    out: list[tuple[str, int]] = []
    for m in PHONE_RE.finditer(html_text):
        phone = fmt_phone(m.group())
        if not valid_phone(phone):
            continue
        global_phone_hits[phone] += 1
        if global_phone_hits[phone] > 2:      # likely office PBX
            continue
        start = max(m.start() - 80, 0)
        end   = min(m.end() + 80, len(html_text))
        snippet = html_text[start:end]
        label_match = LABEL_NEAR_RE.search(snippet)
        score = label_score(label_match.group()) if label_match else 2
        if score < 2:                         # require some contextual label
            continue
        out.append((phone, score))
        LOG.debug("SNIPPET %s → %s (score %d)", phone, snippet[:120], score)
    return out

# ───────────── structured-data & mailto:/tel: extractor ─────────────
def extract_structured_contacts(html_text: str) -> tuple[list[str], list[str]]:
    phones, mails = [], []
    if not BeautifulSoup:
        return phones, mails
    soup = BeautifulSoup(html_text, "html.parser")
    for tag in soup.find_all("script", {"type": "application/ld+json"}):
        try:
            data = json.loads(tag.string or "")
        except Exception:
            continue
        if isinstance(data, list):
            data = data[0]
        if not isinstance(data, dict):
            continue
        tel  = data.get("telephone") or (data.get("contactPoint") or {}).get("telephone")
        mail = data.get("email")     or (data.get("contactPoint") or {}).get("email")
        if tel:
            phones.append(fmt_phone(tel))
        if mail:
            mails.append(mail)
    for a in soup.select('a[href^="tel:"]'):
        phones.append(fmt_phone(a["href"].split("tel:")[-1]))
    for a in soup.select('a[href^="mailto:"]'):
        mails.append(a["href"].split("mailto:")[-1])
    return phones, mails

# ─────────────────────────────  GOOGLE / CSE  ────────────────────────────────
creds = Credentials.from_service_account_info(SC_JSON, scopes=SCOPES)
sheets_service = build("sheets", "v4", credentials=creds, cache_discovery=False)
gc   = gspread.authorize(creds)
ws   = gc.open_by_key(GSHEET_ID).sheet1

def phone_exists(phone: str) -> bool:
    try:
        return phone in ws.col_values(3)
    except Exception as e:
        LOG.error("Sheet read err: %s", e)
        return False

def append_row(row: list[str]):
    sheets_service.spreadsheets().values().append(
        spreadsheetId=GSHEET_ID,
        range="Sheet1!A1",
        valueInputOption="RAW",
        body={"values":[row]}
    ).execute()
    LOG.info("Appended row: %s", row)

def fetch_text(url: str) -> str | None:
    """Fetch URL; fall back through jina.ai proxy if blocked."""
    for target in (url, f"https://r.jina.ai/http://{url}"):
        try:
            r = requests.get(target, timeout=10, headers={"User-Agent":"Mozilla/5.0"})
            if r.status_code != 200:
                if r.status_code in (403, 429, 999):
                    continue
            txt = r.text
            if "unusual traffic" in txt[:600].lower():
                continue
            return txt
        except Exception:
            continue
    return None

# domain pool & dynamic queries
AGENT_SITES = [
    "realtor.com","zillow.com","redfin.com","homesnap.com",
    "kw.com","remax.com","coldwellbanker.com","compass.com",
    "realtyonegroup.com","century21.com","bhhs.com",
    "linkedin.com","facebook.com","instagram.com"
]
DOMAIN_CLAUSE = " OR ".join(f"site:{d}" for d in AGENT_SITES)
BASE_QUERIES = [
    '"{agent}" {state} ("mobile" OR "cell" OR "direct") phone email',
    '"{agent}" {state} phone email',
    '"{agent}" {state} realtor contact'
]

agent_cache: dict[str, tuple[str, str]] = {}

def realtor_fallback(agent: str, state: str) -> tuple[str, str]:
    """Try Realtor.com profile JSON if CSE fails."""
    first, *last = agent.split()
    if not last:
        return "", ""
    url = f"https://www.realtor.com/realestateagents/{'-'.join([first.lower()]+last).lower()}_{state.lower()}"
    html_txt = fetch_text(url)
    if not html_txt:
        return "", ""
    phones, mails = extract_structured_contacts(html_txt)
    phone = next((p for p in phones if valid_phone(p)), "")
    email = mails[0] if mails else ""
    return phone, email

def google_lookup(agent: str, state: str) -> tuple[str, str]:
    cache_key = f"{agent}|{state}"
    if cache_key in agent_cache:
        return agent_cache[cache_key]

    queries = []
    for q in BASE_QUERIES:
        queries.append(f"{q} ({DOMAIN_CLAUSE}) -office".format(agent=agent, state=state))
        queries.append(f"{q} -office".format(agent=agent, state=state))

    candidate_scores: Counter[str] = Counter()
    candidate_email: dict[str, int] = defaultdict(int)

    for q in queries:
        time.sleep(0.25)  # keep CSE under burst limit
        LOG.info("CSE run_query: %r", q)
        try:
            resp = requests.get(
                "https://www.googleapis.com/customsearch/v1",
                params={"key":CS_API_KEY,"cx":CS_CX,"q":q,"num":10},
                timeout=10
            ).json()
            items = resp.get("items", [])
        except Exception as e:
            LOG.warning("CSE error: %s", e)
            continue

        # snippet/pagemap quick hits
        for it in items:
            meta = it.get("pagemap", {})
            tel  = meta.get("contactpoint",[{}])[0].get("telephone")
            if tel:
                phone = fmt_phone(tel)
                if valid_phone(phone):
                    candidate_scores[phone] += 4
            mail = meta.get("contactpoint",[{}])[0].get("email")
            if mail and ok_email(mail):
                candidate_email[mail] += 3

        # deep-dive HTML
        for it in items:
            url = it.get("link", "")
            domain = urlparse(url).netloc.lower()
            # skip obvious non-agent resources in domain-less query
            if domain.endswith(".gov") or domain.endswith(".edu") or domain.startswith("pmc."):
                continue
            html_txt = fetch_text(url)
            if not html_txt:
                continue

            phones, mails = extract_structured_contacts(html_txt)
            for p in phones:
                p_fmt = fmt_phone(p)
                if valid_phone(p_fmt):
                    candidate_scores[p_fmt] += 4
            for m in mails:
                if ok_email(m):
                    candidate_email[m] += 3

            html_low = html.unescape(html_txt.lower())
            for phone, score in proximity_scan(html_low):
                candidate_scores[phone] += score
            for em in EMAIL_RE.findall(html_low):
                if ok_email(em):
                    candidate_email[em] += 1

            # bail early if we already have a strong phone
            if candidate_scores and candidate_scores.most_common(1)[0][1] >= 4:
                break

        if candidate_scores:
            break

    # fallback
    if not candidate_scores:
        fb_phone, fb_email = realtor_fallback(agent, state)
        if fb_phone:
            candidate_scores[fb_phone] = 3
        if fb_email:
            candidate_email[fb_email] = 2

    phone = candidate_scores.most_common(1)[0][0] if candidate_scores else ""
    email = max(candidate_email, key=candidate_email.get) if candidate_email else ""

    LOG.info("Lookup %s → phone=%r email=%r", agent, phone, email)
    agent_cache[cache_key] = (phone, email)
    return phone, email

# ──────────────────────────────  MAIN LOOP  ─────────────────────────────────
def process_rows(rows: list[dict]):
    LOG.info("Processing %d rows", len(rows))
    for r in rows:
        if not is_short_sale(r.get("description","")):
            continue
        agent = r.get("agentName","").strip()
        if not agent:
            continue

        phone, email = google_lookup(agent, r.get("state",""))
        phone = fmt_phone(phone)

        if phone and phone_exists(phone):
            continue

        first, *last = agent.split()
        row = [
            first,
            " ".join(last),
            phone,
            email,
            r.get("street",""),
            r.get("city",""),
            r.get("state","")
        ]
        append_row(row)
    LOG.info("Done")

