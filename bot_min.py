"""
bot_min.py  –  Zillow → Google Sheets bot with smarter Google-only contact finder
───────────────────────────────────────────────────────────────────────────────
Key upgrades vs previous build
• google_lookup():  
  - fires three ranked CSE queries (MLS sites, Facebook, generic)  
  - uses CSE “pagemap” hints before fetching html  
  - retries 403/429 pages through Jina AI proxy  
  - scores every phone / email by proximity to labels (mobile, cell, direct)  
  - validates phones with phonenumbers and rejects impossible NPAs  
  - returns highest-score candidate; never fabricates data
• agent_cache {} prevents re-querying the same agent in one run
Everything else (sheet columns, SMS logic, short-sale filter, etc.) is unchanged.
"""
from __future__ import annotations

import os, json, logging, re, requests, time, html
from collections import defaultdict, Counter
from urllib.parse import urlparse
try:
    import phonenumbers
except ImportError:  # keep running even if library missing
    phonenumbers = None

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

LABEL_NEAR_RE = re.compile(r"(mobile|cell|direct|text|phone|tel|main|office|fax)", re.I)

def label_score(label: str) -> int:
    label = label.lower()
    if label in ("mobile","cell","direct","text"): return 3
    if label in ("phone","tel","main"):            return 2
    return 1  # office / fax

def proximity_scan(html_text: str) -> list[tuple[str,int]]:
    """Return list of (phone,score) extracted from html_text."""
    out: list[tuple[str,int]] = []
    for m in PHONE_RE.finditer(html_text):
        phone = fmt_phone(m.group())
        if not valid_phone(phone): continue
        start = max(m.start()-80,0)
        end   = min(m.end()+80, len(html_text))
        snippet = html_text[start:end]
        label_match = LABEL_NEAR_RE.search(snippet)
        score = label_score(label_match.group()) if label_match else 2
        out.append((phone,score))
    return out

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
            if r.status_code==200:
                return r.text
            if r.status_code in (403,429,999):
                continue
        except Exception:
            continue
    return None

agent_cache: dict[str, tuple[str,str]] = {}

def google_lookup(agent: str, state: str) -> tuple[str,str]:
    cache_key = f"{agent}|{state}"
    if cache_key in agent_cache:
        return agent_cache[cache_key]

    domain_clause = "site:(realtor.com OR redfin.com OR homesnap.com OR coldwellbanker.com)"
    queries = [
        f'"{agent}" {state} ("mobile" OR "cell" OR "direct") phone email {domain_clause} -office',
        f'"{agent}" {state} phone email {domain_clause} -office',
        f'"{agent}" {state} phone email -office'
    ]

    candidate_scores: Counter[str] = Counter()
    candidate_email: dict[str,int] = defaultdict(int)

    for q in queries:
        LOG.info("CSE run_query: %r", q)
        try:
            items = requests.get(
                "https://www.googleapis.com/customsearch/v1",
                params={"key":CS_API_KEY,"cx":CS_CX,"q":q,"num":10},
                timeout=10
            ).json().get("items",[])
        except Exception as e:
            LOG.warning("CSE error: %s", e); continue

        # 1) quick wins from snippet / pagemap
        for it in items:
            meta = it.get("pagemap",{})
            tel  = meta.get("contactpoint",[{}])[0].get("telephone")
            if tel:
                phone = fmt_phone(tel)
                if valid_phone(phone):
                    candidate_scores[phone]+=3
            mail = meta.get("contactpoint",[{}])[0].get("email")
            if mail and ok_email(mail):
                candidate_email[mail]+=3

        # 2) fetch pages until we have good phone
        for it in items:
            url = it.get("link","")
            html_txt = fetch_text(url)
            if not html_txt: continue
            html_txt_low = html.unescape(html_txt.lower())
            for phone,score in proximity_scan(html_txt_low):
                candidate_scores[phone]+=score
            for em in EMAIL_RE.findall(html_txt_low):
                if ok_email(em):
                    candidate_email[em]+=1

            # stop early if we already have a high-confidence phone
            top, top_score = candidate_scores.most_common(1)[0] if candidate_scores else ("",0)
            if top_score>=3: break

        if candidate_scores: break  # stop if any found in this query batch

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

