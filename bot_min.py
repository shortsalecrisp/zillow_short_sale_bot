"""
bot_min.py  –  Zillow → Google Sheets bot with smarter Google-only contact finder
───────────────────────────────────────────────────────────────────────────────
Key upgrades in this build
• Wider but targeted domain list (AGENT_SITES / DOMAIN_CLAUSE)
• Dynamic BASE_QUERIES loop – every query tried with & without domain filter
• Structured-data scrape (JSON-LD) + mailto:/tel: anchors  ➜ higher-quality hits
• Weight tweaks + duplicate-phone guard (skip “main office” lines)
• Captcha page reject, gentle 0.25-s throttle on CSE calls
• Realtor.com fallback if nothing found
Everything else (short-sale filter, Sheets append, SMS hooks, logging) unchanged
"""
from __future__ import annotations

import os, json, logging, re, requests, time, html
from collections import defaultdict, Counter
from urllib.parse import urlparse
try:
    import phonenumbers
except ImportError:  # keep running even if library missing
    phonenumbers = None

# NEW: soup for structured-data extraction
try:
    from bs4 import BeautifulSoup              # type: ignore
except ImportError:
    BeautifulSoup = None

import gspread
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials

# ───────────────────────────────────────────  CONFIG  ────────────────────────
CS_API_KEY   = os.environ["CS_API_KEY"]
CS_CX        = os.environ["CS_CX"]
GSHEET_ID    = os.environ["GSHEET_ID"]
SC_JSON      = json.loads(os.environ["GCP_SERVICE_ACCOUNT_JSON"])
SCOPES       = ["https://www.googleapis.com/auth/spreadsheets"]

logging.basicConfig(
    level=os.getenv("LOGLEVEL", "DEBUG"),
    format="%(asctime)s %(levelname)s: %(message)s",
)
LOG = logging.getLogger("bot")

# sheet column mapping  A  B   C     D     E      F     G
COL_ORDER = ["first", "last", "phone", "email", "street", "city", "state"]

# ───────────────────────────────  SHORT-SALE RULES  ─────────────────────────
SHORT_RE = re.compile(r"\bshort\s+sale\b", re.I)
BAD_RE   = re.compile(r"approved|negotiator|settlement fee|fee at closing", re.I)

def is_short_sale(txt: str) -> bool:
    return bool(SHORT_RE.search(txt)) and not BAD_RE.search(txt)

# ─────────────────────────────  PHONE / EMAIL UTIL  ─────────────────────────
IMG_EXT  = (".png",".jpg",".jpeg",".gif",".svg",".webp")
PHONE_RE = re.compile(
    r"(?<!\d)(?:\+?1[\s\-\.]*)?\(?\d{3}\)?[\s\-\.]*\d{3}[\s\-\.]*\d{4}(?!\d)"
)
EMAIL_RE = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.I)

def fmt_phone(raw: str) -> str:
    digits = re.sub(r"\D", "", raw)
    if len(digits) == 11 and digits.startswith("1"): digits = digits[1:]
    return f"{digits[:3]}-{digits[3:6]}-{digits[6:]}" if len(digits)==10 else ""

def valid_phone(p: str) -> bool:
    if not phonenumbers:  # library missing – basic length check
        return bool(re.fullmatch(r"\d{3}-\d{3}-\d{4}", p))
    try:
        num = phonenumbers.parse(p, "US")
        return phonenumbers.is_possible_number(num)
    except Exception:
        return False

def ok_email(addr: str) -> bool:
    return not addr.lower().endswith(IMG_EXT)

# WEIGHT tweaks
LABEL_NEAR_RE = re.compile(r"(mobile|cell|direct|text|phone|tel|main|office|fax)", re.I)
def label_score(label: str) -> int:
    label = label.lower()
    if label in ("mobile","cell","direct","text"): return 4   # ↑ weight
    if label in ("phone","tel"):                   return 2
    return 1  # office / fax

# ───────────────────── NEW: duplicate-phone guard counter ────────────────────
global_phone_hits: Counter[str] = Counter()

def proximity_scan(html_text: str) -> list[tuple[str,int]]:
    """Return list of (phone,score) extracted from html_text, skipping
       numbers that appear on 3+ domains (likely office lines)."""
    out: list[tuple[str,int]] = []
    for m in PHONE_RE.finditer(html_text):
        phone = fmt_phone(m.group())
        if not valid_phone(phone): continue

        # skip shared numbers
        global_phone_hits[phone] += 1
        if global_phone_hits[phone] > 2:
            continue

        start = max(m.start()-80,0)
        end   = min(m.end()+80, len(html_text))
        snippet = html_text[start:end]
        label_match = LABEL_NEAR_RE.search(snippet)
        score = label_score(label_match.group()) if label_match else 2
        out.append((phone,score))
        LOG.debug("SNIPPET %s → %s (score %d)", phone, snippet[:120], score)
    return out

# ───────────────────── NEW: structured-data / anchor scrape ──────────────────
def extract_structured_contacts(html_text: str) -> tuple[list[str], list[str]]:
    """Pull tel/email from JSON-LD Person/Organization blocks and tel/mailto anchors."""
    phones, mails = [], []
    if not BeautifulSoup:  # bs4 missing
        return phones, mails

    soup = BeautifulSoup(html_text, "html.parser")

    # JSON-LD blocks
    for tag in soup.find_all("script", {"type": "application/ld+json"}):
        try:
            data = json.loads(tag.string or "")
        except Exception:
            continue
        if isinstance(data, list):
            data = data[0]
        if not isinstance(data, dict):
            continue
        tel  = data.get("telephone") or \
               (data.get("contactPoint") or {}).get("telephone")
        mail = data.get("email") or \
               (data.get("contactPoint") or {}).get("email")
        if tel:
            phones.append(fmt_phone(tel))
        if mail:
            mails.append(mail)

    # tel: / mailto:
    for a in soup.select('a[href^="tel:"]'):
        phone = fmt_phone(a["href"].split("tel:")[-1])
        phones.append(phone)
    for a in soup.select('a[href^="mailto:"]'):
        mails.append(a["href"].split("mailto:")[-1])

    return phones, mails

# ─────────────────────────────────  GOOGLE / CSE  ───────────────────────────
creds = Credentials.from_service_account_info(SC_JSON, scopes=SCOPES)
sheets_service = build("sheets","v4",credentials=creds,cache_discovery=False)
gc   = gspread.authorize(creds)
ws   = gc.open_by_key(GSHEET_ID).sheet1

def phone_exists(phone: str)->bool:
    try:
        return phone in ws.col_values(3)
    except Exception as e:
        LOG.error("Sheet read err: %s",e); return False

def append_row(row: list[str]):
    sheets_service.spreadsheets().values().append(
        spreadsheetId=GSHEET_ID,
        range="Sheet1!A1",
        valueInputOption="RAW",
        body={"values":[row]}
    ).execute()
    LOG.info("Appended row: %s", row)

# fallback proxy for bot-blocked pages
def fetch_text(url: str) -> str|None:
    for attempt, target in enumerate((url, f"https://r.jina.ai/http://{url}"), 1):
        try:
            r = requests.get(target, timeout=10, headers={"User-Agent":"Mozilla/5.0"})
            if r.status_code!=200:
                if r.status_code in (403,429,999): continue
            html_txt = r.text
            # captcha / unusual-traffic guard
            if "unusual traffic" in html_txt[:600].lower():
                continue
            return html_txt
        except Exception:
            continue
    return None

# ─────────────────────── NEW: wider domain pool / query loop ─────────────────
AGENT_SITES = [
    # national brands & IDX providers
    "realtor.com","zillow.com","redfin.com","homesnap.com",
    "kw.com","remax.com","coldwellbanker.com","compass.com",
    "realtyonegroup.com","century21.com","bhhs.com",
    # social pages – often list cell #
    "linkedin.com","facebook.com","instagram.com"
]
DOMAIN_CLAUSE = " OR ".join(f"site:{d}" for d in AGENT_SITES)

BASE_QUERIES = [
    '"{agent}" {state} ("mobile" OR "cell" OR "direct") phone email',
    '"{agent}" {state} phone email',
    '"{agent}" {state} realtor contact'
]

agent_cache: dict[str, tuple[str,str]] = {}

def realtor_fallback(agent: str, state: str) -> tuple[str,str]:
    """Last-ditch pull from Realtor.com profile JSON."""
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

def google_lookup(agent: str, state: str) -> tuple[str,str]:
    cache_key = f"{agent}|{state}"
    if cache_key in agent_cache:
        return agent_cache[cache_key]

    # build query list
    queries: list[str] = []
    for q in BASE_QUERIES:
        queries.append(f"{q} ({DOMAIN_CLAUSE}) -office".format(agent=agent, state=state))
        queries.append(f"{q} -office".format(agent=agent, state=state))

    candidate_scores: Counter[str] = Counter()
    candidate_email: dict[str,int] = defaultdict(int)

    for q in queries:
        time.sleep(0.25)  # throttle to stay under CSE burst cap
        LOG.info("CSE run_query: %r", q)
        try:
            resp = requests.get(
                "https://www.googleapis.com/customsearch/v1",
                params={"key":CS_API_KEY,"cx":CS_CX,"q":q,"num":10},
                timeout=10
            ).json()
            items = resp.get("items", [])
        except Exception as e:
            LOG.warning("CSE error: %s", e); continue

        # 1) quick wins from snippet / pagemap
        for it in items:
            meta = it.get("pagemap",{})
            tel  = meta.get("contactpoint",[{}])[0].get("telephone")
            if tel:
                phone = fmt_phone(tel)
                if valid_phone(phone):
                    candidate_scores[phone]+=4
            mail = meta.get("contactpoint",[{}])[0].get("email")
            if mail and ok_email(mail):
                candidate_email[mail]+=3

        # 2) fetch pages until we have good phone
        for it in items:
            url = it.get("link","")
            html_txt = fetch_text(url)
            if not html_txt: continue

            # structured data & anchors
            phones,mails = extract_structured_contacts(html_txt)
            for p in phones:
                p_fmt = fmt_phone(p)
                if valid_phone(p_fmt):
                    candidate_scores[p_fmt]+=4
            for m in mails:
                if ok_email(m):
                    candidate_email[m]+=3

            html_txt_low = html.unescape(html_txt.lower())
            for phone,score in proximity_scan(html_txt_low):
                candidate_scores[phone]+=score
            for em in EMAIL_RE.findall(html_txt_low):
                if ok_email(em):
                    candidate_email[em]+=1

            # stop early if high-confidence phone
            if candidate_scores and candidate_scores.most_common(1)[0][1] >= 4:
                break

        if candidate_scores:  # we found something in this query batch
            break

    if not candidate_scores:
        # fallback to Realtor.com
        fb_phone, fb_email = realtor_fallback(agent, state)
        if fb_phone:
            candidate_scores[fb_phone] = 3
        if fb_email:
            candidate_email[fb_email] = 2

    phone = candidate_scores.most_common(1)[0][0] if candidate_scores else ""
    email = candidate_email and max(candidate_email, key=candidate_email.get) or ""

    LOG.info("Lookup %s → phone=%r email=%r", agent, phone, email)
    agent_cache[cache_key]=(phone,email)
    return phone,email

# ──────────────────────────  MAIN PROCESSING LOOP  ──────────────────────────
def process_rows(rows: list[dict]):
    LOG.info("Processing %d rows", len(rows))
    for r in rows:
        if not is_short_sale(r.get("description","")):          continue
        agent = r.get("agentName","").strip()
        if not agent:                                           continue

        phone,email = google_lookup(agent, r.get("state",""))
        phone = fmt_phone(phone)

        if phone and phone_exists(phone):                       continue

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

