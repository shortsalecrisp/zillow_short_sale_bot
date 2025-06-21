#!/usr/bin/env python3
#  ╭────────────────── bot_min.py ──────────────────╮
#  │ build: 22 Jun 2025 – “B‑list” patch            │
#  │   • Skip rows that are not short‑sales         │
#  │   • Phone: surname‑proximity relevance test    │
#  │   • E‑mail: blacklist model + brokerage boost  │
#  │   • Google‑CSE: 1 s guard‑rail + in‑run cache  │
#  ╰────────────────────────────────────────────────╯

import json, logging, os, random, re, sys, time, html, concurrent.futures, threading
from collections import Counter, defaultdict
from datetime import datetime
from typing import Any, Dict, List, Set, Tuple
from urllib.parse import urlparse

import requests
import gspread
from googleapiclient.discovery import build as gapi_build
from google.oauth2.service_account import Credentials

try:
    import phonenumbers
except ImportError:
    phonenumbers = None
try:
    from bs4 import BeautifulSoup
except ImportError:
    BeautifulSoup = None

# ─────────────────────────── ENV / AUTH ────────────────────────────
CS_API_KEY = os.environ["CS_API_KEY"]
CS_CX      = os.environ["CS_CX"]
GSHEET_ID  = os.environ["GSHEET_ID"]
SC_JSON    = json.loads(os.environ["GCP_SERVICE_ACCOUNT_JSON"])
SCOPES     = ["https://www.googleapis.com/auth/spreadsheets"]

RAPID_KEY  = os.getenv("RAPID_KEY", "").strip()
RAPID_HOST = os.getenv("RAPID_HOST", "zillow-com1.p.rapidapi.com").strip()
GOOD_STATUS = {"FOR_SALE", "ACTIVE", "COMING_SOON", "PENDING", "NEW_CONSTRUCTION"}

# ─────────────────────── SMS CONFIG (unchanged) ────────────────────
SMS_ENABLE      = os.getenv("SMSM_ENABLE", "false").lower() == "true"
SMS_TEST_MODE   = os.getenv("SMSM_TEST_MODE", "true").lower() == "true"
SMS_TEST_NUMBER = os.getenv("SMSM_TEST_NUMBER", "")
SMS_API_KEY     = os.getenv("SMSM_API_KEY", "")
SMS_FROM        = os.getenv("SMSM_FROM", "")
SMS_URL         = os.getenv("SMSM_URL", "https://api.smsmobileapi.com/sendsms/")
SMS_TEMPLATE    = (
    "Hey {first}, this is Yoni Kutler—I saw your short sale listing at {address} and wanted to "
    "introduce myself. I specialize in helping agents get faster bank approvals and "
    "ensure these deals close. I know you likely handle short sales yourself, but I "
    "work behind the scenes to take on lender negotiations so you can focus on selling. "
    "No cost to you or your client—I’m only paid by the buyer at closing. Would you be "
    "open to a quick call to see if this could help?"
)

# ───────────────────────── CONFIGS ─────────────────────────────────
MAX_ZILLOW_403      = 3
MAX_RATE_429        = 3
BACKOFF_FACTOR      = 1.7
MAX_BACKOFF_SECONDS = 12
GOOGLE_CONCURRENCY  = 2
METRICS             = Counter()

logging.basicConfig(level=os.getenv("LOGLEVEL", "DEBUG"),
                    format="%(asctime)s %(levelname)-8s: %(message)s")
LOG = logging.getLogger("bot_min")

# ──────────────────────────── REGEXES ──────────────────────────────
SHORT_RE  = re.compile(r"\bshort\s+sale\b", re.I)
BAD_RE    = re.compile(r"\b(?:approved short sale|short sale approved)\b", re.I)
TEAM_RE   = re.compile(r"^\s*the\b|\bteam\b", re.I)
IMG_EXT   = (".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp")
PHONE_RE  = re.compile(r"(?<!\d)(?:\+?1[\s\-\.]*)?\(?\d{3}\)?[\s\-\.]*\d{3}[\s\-\.]*\d{4}(?!\d)")
EMAIL_RE  = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.I)

LABEL_TABLE = {"mobile":4,"cell":4,"direct":4,"text":4,"c:":4,"m:":4,
               "phone":2,"tel":2,"p:":2,"office":1,"main":1,"customer":1,"footer":1}
LABEL_RE = re.compile("(" + "|".join(map(re.escape, LABEL_TABLE)) + ")", re.I)
US_AREA_CODES = {str(i) for i in range(201, 990)}

OFFICE_HINTS = {"office", "main", "fax", "team", "brokerage", "corporate"}

# ─────────────────────── Google / Sheets setup ─────────────────────
creds  = Credentials.from_service_account_info(SC_JSON, scopes=SCOPES)
sheets_service = gapi_build("sheets", "v4", credentials=creds, cache_discovery=False)
gc     = gspread.authorize(creds)
ws     = gc.open_by_key(GSHEET_ID).sheet1

# ───────────────────────── SITE LISTS  (whitelist removed) ─────────
# We now allow **any** domain except those matching the blacklist below.
SCRAPE_SITES: List[str] = []   # kept for function signatures
DYNAMIC_SITES: Set[str] = set()

BAN_KEYWORDS = {
    "zillow.com","realtor.com",
    "linkedin.com","twitter.com","instagram.com","pinterest.com",
    "legacy.com","obituary","obituaries","funeral",
    ".gov",".edu",".mil"
}

_blocked_until: Dict[str,float] = {}

# ───────────────────── threading pool for Google I/O ───────────────
_executor = concurrent.futures.ThreadPoolExecutor(max_workers=GOOGLE_CONCURRENCY)
def pmap(fn, iterable): return list(_executor.map(fn, iterable))

# ───────────────────── utilities: phone / email / fmt  ─────────────
def fmt_phone(r):
    d = re.sub(r"\D", "", r)
    if len(d) == 11 and d.startswith("1"):
        d = d[1:]
    return f"{d[:3]}-{d[3:6]}-{d[6:]}" if len(d) == 10 else ""

def valid_phone(p):
    if phonenumbers:
        try:
            return phonenumbers.is_possible_number(phonenumbers.parse(p, "US"))
        except Exception:
            return False
    return re.fullmatch(r"\d{3}-\d{3}-\d{4}", p)

def clean_email(e):   return e.split("?")[0].strip()
def ok_email(e):
    e = clean_email(e)
    return e and "@" in e and not e.lower().endswith(IMG_EXT) and not re.search(r"\.(gov|edu|mil)$", e, re.I)

def is_short_sale(text): return SHORT_RE.search(text) and not BAD_RE.search(text)

# ───────────────────── Rapid‑API helpers (unchanged) ───────────────
def _phone_obj_to_str(obj: Dict[str,str]) -> str:
    if not obj: return ""
    key_order = [
        "areacode","area_code","areaCode",
        "prefix","centralofficecode","central_office_code","centralOfficeCode",
        "number","line","line_number","lineNumber"
    ]
    parts = []
    for k in key_order:
        if obj.get(k):
            parts.append(re.sub(r"\D","",str(obj[k])))
    for v in obj.values():
        chunk = re.sub(r"\D","",str(v))
        if 2 <= len(chunk) <= 4: parts.append(chunk)
    digits = "".join(parts)[:10]
    return fmt_phone(digits)

def rapid_property(zpid: str) -> Dict[str,Any]:
    if not RAPID_KEY:
        return {}
    try:
        headers = {"X-RapidAPI-Key": RAPID_KEY, "X-RapidAPI-Host": RAPID_HOST}
        r = requests.get(f"https://{RAPID_HOST}/property",
                         params={"zpid": zpid}, headers=headers, timeout=15)
        if r.status_code == 429:
            LOG.error("Rapid-API quota exhausted (HTTP 429)")
            return {}
        r.raise_for_status()
        return r.json().get("data") or r.json()
    except Exception as exc:
        LOG.debug("Rapid-API fetch error %s for zpid=%s", exc, zpid)
        return {}

def _phones_from_block(blk: Dict[str,Any]) -> List[str]:
    out=[]
    if not blk: return out
    if blk.get("phone"): out.append(_phone_obj_to_str(blk["phone"]))
    for ph in blk.get("phones",[]): out.append(_phone_obj_to_str(ph))
    return [p for p in out if p]

def _emails_from_block(blk: Dict[str,Any]) -> List[str]:
    if not blk: return []
    out=[]
    for k in ("email","emailAddress"):
        if blk.get(k): out.append(clean_email(blk[k]))
    for e in blk.get("emails",[]): out.append(clean_email(e))
    return [e for e in out if ok_email(e)]

def _names_match(a: str, b: str) -> bool:
    ta={t.lower().strip(".") for t in a.split() if len(t)>1}
    tb={t.lower().strip(".") for t in b.split() if len(t)>1}
    return bool(ta & tb)

def rapid_phone(zpid: str, agent_name: str) -> Tuple[str,str]:
    data = rapid_property(zpid)
    if not data: return "",""
    cand, allp = [], set()
    for blk in data.get("contact_recipients",[]):
        for pn in _phones_from_block(blk):
            allp.add(pn)
            if _names_match(agent_name, blk.get("display_name","")):
                cand.append(("rapid:contact_recipients", pn))
    lb = data.get("listed_by",{})
    for pn in _phones_from_block(lb):
        allp.add(pn)
        if _names_match(agent_name, lb.get("display_name","")):
            cand.append(("rapid:listed_by", pn))
    if cand: return cand[0][1], cand[0][0]
    if len(allp)==1: return next(iter(allp)), "rapid:fallback_single"
    return "",""

def rapid_email(zpid: str, agent_name: str) -> Tuple[str,str]:
    data = rapid_property(zpid)
    if not data: return "",""
    cand, allem = [], set()
    for blk in data.get("contact_recipients",[]):
        for em in _emails_from_block(blk):
            allem.add(em)
            if _names_match(agent_name, blk.get("display_name","")):
                cand.append(("rapid:contact_recipients", em))
    lb=data.get("listed_by",{})
    for em in _emails_from_block(lb):
        allem.add(em)
        if _names_match(agent_name, lb.get("display_name","")):
            cand.append(("rapid:listed_by", em))
    if cand: return cand[0][1], cand[0][0]
    if len(allem)==1: return next(iter(allem)), "rapid:fallback_single"
    return "",""

# ───────────────────── HTML fetch helpers ──────────────────────────
def _jitter():          time.sleep(random.uniform(0.8,1.5))
def _mark_block(dom):   _blocked_until[dom] = time.time() + 600

def _try_textise(dom: str, url: str) -> str:
    try:
        r = requests.get(f"https://r.jina.ai/http://{urlparse(url).netloc}{urlparse(url).path}",
                         timeout=10, headers={"User-Agent":"Mozilla/5.0"})
        if r.status_code==200 and r.text.strip():
            return r.text
    except Exception: pass
    return ""

def _domain(host_or_url: str) -> str:
    host = urlparse(host_or_url).netloc or host_or_url
    parts = host.lower().split(".")
    return ".".join(parts[-2:]) if len(parts) >= 2 else host.lower()

def _is_banned(dom: str) -> bool:
    return any(bad in dom for bad in BAN_KEYWORDS)

def _should_fetch(url: str, strict: bool = True) -> bool:
    """Fetch policy: allow everything unless black‑listed or temporarily blocked."""
    dom = _domain(url)
    if dom in _blocked_until and _blocked_until[dom] > time.time():
        return False
    return not _is_banned(dom)

def fetch_simple(u, strict=True):
    if not _should_fetch(u, strict): return None
    dom=_domain(u)
    try:
        r=requests.get(u,timeout=10,headers={"User-Agent":"Mozilla/5.0"})
        if r.status_code==200: return r.text
        if r.status_code in (403,429): _mark_block(dom)
        if r.status_code in (403,451): return _try_textise(dom,u)
    except Exception as exc:
        LOG.debug("fetch_simple error %s on %s", exc,u)
    return None

def fetch(u, strict=True):
    if not _should_fetch(u, strict): return None
    dom=_domain(u)
    bare=re.sub(r"^https?://","",u)
    variants=[u,
              f"https://r.jina.ai/http://{bare}",
              f"https://r.jina.ai/http://screenshot/{bare}"]
    z403=ratelimit=0
    backoff=1.0
    for url in variants:
        for _ in range(3):
            try:
                r=requests.get(url,timeout=10,headers={"User-Agent":"Mozilla/5.0"})
            except Exception as exc:
                METRICS["fetch_error"]+=1
                LOG.debug("fetch error %s on %s", exc,url)
                break
            if r.status_code==200:
                if "unusual traffic" in r.text[:700].lower():
                    METRICS["fetch_unusual"]+=1; break
                return r.text
            if r.status_code==403 and "zillow.com" in url:
                z403+=1; METRICS["fetch_403"]+=1
                if z403>=MAX_ZILLOW_403: return None
                _mark_block(dom)
            elif r.status_code==429:
                ratelimit+=1; METRICS["fetch_429"]+=1
                if ratelimit>=MAX_RATE_429:
                    _mark_block(dom); return None
            elif r.status_code in (403,451):
                _mark_block(dom)
                txt=_try_textise(dom,u)
                if txt: return txt
            else:
                METRICS[f"fetch_other_{r.status_code}"]+=1
            _jitter(); time.sleep(min(backoff,MAX_BACKOFF_SECONDS)); backoff*=BACKOFF_FACTOR
    return None

def fetch_simple_relaxed(u): return fetch_simple(u, strict=False)
def fetch_relaxed(u):        return fetch(u, strict=False)

# ───────────────────── Google CSE helper (with cache + guard) ──────
_cse_cache: Dict[str,List[Dict[str,Any]]] = {}
_last_cse_ts = 0.0
_cse_lock    = threading.Lock()

def google_items(q, tries=3):
    global _last_cse_ts
    with _cse_lock:
        if q in _cse_cache:
            return _cse_cache[q]
        delta = time.time() - _last_cse_ts
        if delta < 1.0:               # 3.6 “guardian” delay
            time.sleep(1.0 - delta)
        _last_cse_ts = time.time()

    backoff=1.0
    for _ in range(tries):
        try:
            j=requests.get(
                "https://www.googleapis.com/customsearch/v1",
                params={"key":CS_API_KEY,"cx":CS_CX,"q":q,"num":10},timeout=10
            ).json()
            items=j.get("items",[])
            with _cse_lock:
                _cse_cache[q]=items
            return items
        except Exception:
            time.sleep(min(backoff,MAX_BACKOFF_SECONDS)); backoff*=BACKOFF_FACTOR
    return []

# ───────────────────── structured & proximity scan (unchanged) ────
def extract_struct(td):
    phones,mails=[],[]
    if not BeautifulSoup: return phones,mails
    soup=BeautifulSoup(td,"html.parser")
    for sc in soup.find_all("script",{"type":"application/ld+json"}):
        try:
            data=json.loads(sc.string or "")
        except Exception: continue
        if isinstance(data,list): data=data[0]
        if isinstance(data,dict):
            tel=data.get("telephone") or (data.get("contactPoint") or {}).get("telephone")
            mail=data.get("email") or (data.get("contactPoint") or {}).get("email")
            if tel:  phones.append(fmt_phone(tel))
            if mail: mails.append(clean_email(mail))
    for a in soup.select('a[href^="tel:"]'):
        phones.append(fmt_phone(a["href"].split("tel:")[-1]))
    for a in soup.select('a[href^="mailto:"]'):
        mails.append(clean_email(a["href"].split("mailto:")[-1]))
    return phones,mails

def proximity_scan(t,last_name=None):
    out={}
    for m in PHONE_RE.finditer(t):
        p=fmt_phone(m.group())
        if not valid_phone(p): continue
        sn_start=max(m.start()-120,0); sn_end=min(m.end()+120,len(t))   # ±120 chars
        snippet=t[sn_start:sn_end]
        if last_name and last_name.lower() not in snippet: continue
        lab_match=LABEL_RE.search(snippet); lab=lab_match.group().lower() if lab_match else ""
        w=LABEL_TABLE.get(lab,0)
        if w<1: continue
        bw,ts,off=out.get(p,(0,0,False))
        out[p]=(max(bw,w), ts+2+w, lab in ("office","main"))
    return out

# ───────────────────── Google query builders (whitelist → broad) ──
def _name_tokens(name:str)->List[str]:
    return [t for t in re.split(r"\s+",name.strip()) if len(t)>1]

def build_q_phone(name:str,state:str)->List[str]:
    tokens=" ".join(_name_tokens(name))
    return [f'"{name}" {state} realtor phone']

def build_q_email(name:str,state:str)->List[str]:
    tokens=" ".join(_name_tokens(name))
    return [f'"{name}" {state} realtor email address']

# ───────────────────── caches ────────────────────────────────
cache_p, cache_e = {}, {}

# ───────────────────── lookup phone / email  (phone test patched)──
def _split_portals(urls):
    portals, non = [], []
    for u in urls:
        (portals if any(d in u for d in SCRAPE_SITES) else non).append(u)
    return non, portals

def _looks_direct(phone:str, agent:str, state:str, tries:int=2)->bool:
    """True if *phone* is found on a page where agent’s surname appears
       within ±120 chars of the number."""
    if not phone: return False
    last=agent.split()[-1].lower()
    queries=[f'"{phone}" {state}', f'"{phone}" "{agent.split()[0]}"']
    digits=re.sub(r"\D","",phone)
    for q in queries:
        for it in google_items(q, tries=1):
            link=it.get("link","")
            page=fetch_simple(link, strict=False)
            if not page: continue
            low=page.lower()
            # strip formats for search
            low_digits=re.sub(r"\D","",page)
            if digits in low_digits:
                # locate first occurrence of digits in raw page text
                pos=low_digits.find(digits)
                if pos==-1: continue
                # map back to char‑level window via regex (approx.)
                if last in low[max(0,pos-200):pos+200]:
                    return True
    return False

def lookup_phone(agent:str,state:str,row_payload:Dict[str,Any])->str:
    key=f"{agent}|{state}"
    if key in cache_p: return cache_p[key]

    # direct in‑payload
    for blk in (row_payload.get("contact_recipients") or []):
        for p in _phones_from_block(blk):
            d=fmt_phone(p)
            if d and valid_phone(d):
                cache_p[key]=d
                LOG.debug("PHONE hit directly from contact_recipients")
                return d

    zpid=str(row_payload.get("zpid",""))
    undirect_phone=""
    if zpid:
        phone,src=rapid_phone(zpid,agent)
        if phone:
            if _looks_direct(phone,agent,state):
                cache_p[key]=phone
                LOG.debug("PHONE WIN %s via %s (surname proximity)",phone,src); return phone
            undirect_phone=phone

    cand_good,cand_office,src_good={}, {}, {}

    def add(p,score,office_flag,src=""):
        d=fmt_phone(p)
        if not valid_phone(d): return
        (cand_office if office_flag else cand_good)[d]= \
            (cand_office if office_flag else cand_good).get(d,0)+score
        if not office_flag and src: src_good[d]=src; DYNAMIC_SITES.add(_domain(src))

    # Google metadata (unchanged logic, but wider queries)
    queries=build_q_phone(agent,state)
    for items in pmap(google_items,queries):
        for it in items:
            tel=it.get("pagemap",{}).get("contactpoint",[{}])[0].get("telephone")
            if tel: add(tel,4,False,f"CSE:{it.get('link','')}")

    if cand_good:
        phone=max(cand_good,key=cand_good.get); cache_p[key]=phone
        LOG.debug("PHONE WIN %s via %s",phone,src_good.get(phone,"CSE-json")); return phone

    urls=[it.get("link","") for items in pmap(google_items,queries) for it in items][:20]
    non_portal,portal=_split_portals(urls)
    last_name=(agent.split()[-1] if len(agent.split())>1 else agent).lower()

    for url,page in zip(non_portal,pmap(fetch_simple,non_portal)):
        if not page or agent.lower() not in page.lower(): continue
        ph,_=extract_struct(page)
        for p in ph: add(p,6,False,url)
        low=html.unescape(page.lower())
        for p,(_,sc,off) in proximity_scan(low,last_name).items():
            add(p,sc,off,url)
        if cand_good or cand_office: break

    if not cand_good and not cand_office:
        for url,page in zip(portal,pmap(fetch,portal)):
            if not page or agent.lower() not in page.lower(): continue
            ph,_=extract_struct(page)
            for p in ph: add(p,4,False,url)
            low=html.unescape(page.lower())
            for p,(_,sc,off) in proximity_scan(low,last_name).items():
                add(p,sc,off,url)
            if cand_good: break

    phone=""
    if cand_good: phone=max(cand_good,key=cand_good.get)
    elif undirect_phone: phone=undirect_phone
    cache_p[key]=phone
    if phone:
        LOG.debug("PHONE WIN %s via %s",phone,src_good.get(phone,"crawler/unverified"))
    else:
        LOG.debug("PHONE FAIL for %s %s  cand_good=%s cand_office=%s",agent,state,cand_good,cand_office)
    return phone

def lookup_email(agent:str,state:str,row_payload:Dict[str,Any])->str:
    key=f"{agent}|{state}"
    if key in cache_e: return cache_e[key]

    for blk in (row_payload.get("contact_recipients") or []):
        for em in _emails_from_block(blk):
            cache_e[key]=em
            LOG.debug("EMAIL hit directly from contact_recipients"); return em

    zpid=str(row_payload.get("zpid",""))
    brokerage=""
    if zpid:
        em,src=rapid_email(zpid,agent)
        if em:
            cache_e[key]=em; LOG.debug("EMAIL WIN %s via %s",em,src); return em
        # pull brokerage for boost
        rapid = rapid_property(zpid)
        brokerage = (rapid.get("listed_by") or {}).get("brokerageName","") if rapid else ""

    cand,src_e=defaultdict(int),{}

    def add_e(m,score,src=""):
        m=clean_email(m)
        if not ok_email(m): return
        if brokerage and brokerage.lower() in m.lower():
            score += 1                                   # optional boost
        cand[m]+=score
        if src: src_e.setdefault(m,src); DYNAMIC_SITES.add(_domain(src))

    for items in pmap(google_items,build_q_email(agent,state)):
        for it in items:
            mail=it.get("pagemap",{}).get("contactpoint",[{}])[0].get("email","")
            add_e(mail,3,f"CSE:{it.get('link','')}")

    urls=[it.get("link","") for items in pmap(google_items,build_q_email(agent,state)) for it in items][:20]
    non_portal,portal=_split_portals(urls)

    for url,page in zip(non_portal,pmap(fetch_simple,non_portal)):
        if not page or agent.lower() not in page.lower(): continue
        _,ems=extract_struct(page)
        for m in ems: add_e(m,3,url)
        for m in EMAIL_RE.findall(page): add_e(m,1,url)
        if cand: break

    if not cand:
        for url,page in zip(portal,pmap(fetch,portal)):
            if not page or agent.lower() not in page.lower(): continue
            _,ems=extract_struct(page)
            for m in ems: add_e(m,2,url)
            for m in EMAIL_RE.findall(page): add_e(m,1,url)
            if cand: break

    tokens={re.sub(r"[^a-z]","",w.lower()) for w in agent.split()}
    good={m:sc for m,sc in cand.items() if any(tok and tok in m.lower() for tok in tokens)}
    email=max(good,key=good.get) if good else (max(cand,key=cand.get) if cand else "")
    if email:
        LOG.debug("EMAIL WIN %s via %s",email,src_e.get(email,"crawler"))
    else:
        LOG.debug("EMAIL FAIL for %s %s  tokens=%s  candidates=%s",agent,state,tokens,dict(list(cand.items())[:8]))
    cache_e[key]=email
    return email

# ───────────────────── Google Sheet helpers (unchanged) ────────────
def mark_sent(row_idx:int):
    try:
        sheets_service.spreadsheets().values().update(
            spreadsheetId=GSHEET_ID,
            range=f"Sheet1!H{row_idx}:H{row_idx}",
            valueInputOption="RAW",
            body={"values":[["x"]]}
        ).execute()
        LOG.debug("Marked row %s column H as sent",row_idx)
    except Exception as e:
        LOG.error("GSheet mark_sent error %s",e)

def append_row(values)->int:
    resp=sheets_service.spreadsheets().values().append(
        spreadsheetId=GSHEET_ID,range="Sheet1!A1",
        valueInputOption="RAW",body={"values":[values]}
    ).execute()
    row_idx=int(resp["updates"]["updatedRange"].split("!")[1].split(":")[0][1:])
    LOG.info("Row appended to sheet (row %s)",row_idx)
    return row_idx

def phone_exists(p):
    try:
        return p in ws.col_values(3)
    except Exception:
        return False

# ───────────────────── misc helpers (unchanged) ────────────────────
def extract_name(t):
    m=re.search(r"listing agent[:\s\-]*([A-Za-z \.'’-]{3,})",t,re.I)
    if m:
        n=m.group(1).strip()
        if not TEAM_RE.search(n): return n
    return None

def is_active_listing(zpid):
    if not RAPID_KEY: return True
    try:
        r=requests.get(
            f"https://{RAPID_HOST}/property",
            params={"zpid":zpid},
            headers={"X-RapidAPI-Key":RAPID_KEY,"X-RapidAPI-Host":RAPID_HOST},
            timeout=15
        )
        r.raise_for_status()
        status=(r.json().get("data") or r.json()).get("homeStatus","").upper()
        return (not status) or status in GOOD_STATUS
    except Exception as e:
        LOG.warning("Rapid status check failed for %s (%s) – keeping row",zpid,e)
        return True

# ───────────────────── SMS sender (unchanged) ─────────────────────
def send_sms(phone,first,address,row_idx):
    if not SMS_ENABLE or not phone: return
    if SMS_TEST_MODE and SMS_TEST_NUMBER: phone=SMS_TEST_NUMBER
    try:
        resp=requests.post(
            SMS_URL,timeout=10,
            data={
                "apikey":SMS_API_KEY,
                "recipients":phone,
                "message":SMS_TEMPLATE.format(first=first,address=address),
                "sendsms":"1"
            }
        )
        try:
            result=resp.json().get("result",{})
        except Exception:
            result={}
        if resp.status_code==200 and str(result.get("error"))=="0":
            mark_sent(row_idx)
            LOG.info("SMS sent to %s",phone)
        else:
            LOG.error("SMS API error %s – payload %s",resp.status_code,(resp.text or "")[:200])
    except Exception as e:
        LOG.error("SMS send error %s",e)

# ───────────────────── core row processor (skip ensured) ───────────
def process_rows(rows:List[Dict[str,Any]]):
    for r in rows:
        txt=(r.get("description","")+" "+r.get("openai_summary","")).strip()
        if not is_short_sale(txt):
            LOG.debug("SKIP non-short-sale %s (%s)",r.get("street"),r.get("zpid")); continue   # ← already skipping
        zpid=str(r.get("zpid",""))
        if zpid and not is_active_listing(zpid):
            LOG.info("Skip stale/off-market zpid %s",zpid); continue
        name=r.get("agentName","").strip() or extract_name(txt)
        if not name or TEAM_RE.search(name): continue
        state=r.get("state","")
        phone=fmt_phone(lookup_phone(name,state,r))
        email=lookup_email(name,state,r)
        if phone and phone_exists(phone): continue
        first,*last=name.split()
        row_idx=append_row([first," ".join(last),phone,email,
                            r.get("street",""),r.get("city",""),state,""])
        if phone: send_sms(phone,first,r.get("street",""),row_idx)

# ───────────────────── main entry point (unchanged) ────────────────
if __name__ == "__main__":
    try:
        stdin_txt=sys.stdin.read().strip()
        payload=json.loads(stdin_txt) if stdin_txt else None
    except json.JSONDecodeError:
        payload=None

    if payload and payload.get("listings"):
        LOG.debug("Sample fields on first fresh row: %s",list(payload['listings'][0].keys()))
        process_rows(payload["listings"])
    else:
        LOG.info("No JSON payload detected; exiting (scheduler removed).")

