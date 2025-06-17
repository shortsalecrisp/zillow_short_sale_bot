# === bot_min.py  (June 2025 → SMS “x” only after real confirmation) ================

import os, sys, json, logging, re, time, html, requests, concurrent.futures
from collections import defaultdict, Counter
from datetime import datetime

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


# ─────────────────────────── ENV / AUTH ────────────────────────────
CS_API_KEY = os.environ["CS_API_KEY"]
CS_CX      = os.environ["CS_CX"]
GSHEET_ID  = os.environ["GSHEET_ID"]
SC_JSON    = json.loads(os.environ["GCP_SERVICE_ACCOUNT_JSON"])
SCOPES     = ["https://www.googleapis.com/auth/spreadsheets"]

RAPID_KEY  = os.getenv("RAPID_KEY", "").strip()
RAPID_HOST = os.getenv("RAPID_HOST", "zillow-com1.p.rapidapi.com").strip()
GOOD_STATUS = {"FOR_SALE", "ACTIVE", "COMING_SOON", "PENDING", "NEW_CONSTRUCTION"}

logging.basicConfig(level=os.getenv("LOGLEVEL", "DEBUG"),
                    format="%(asctime)s %(levelname)s: %(message)s")
LOG = logging.getLogger("bot")

# ───────────────────────── CONFIGS ─────────────────────────────────
MAX_ZILLOW_403      = 3
MAX_RATE_429        = 3
BACKOFF_FACTOR      = 1.7
MAX_BACKOFF_SECONDS = 12
GOOGLE_CONCURRENCY  = 4
METRICS             = Counter()

# ─────────────────────── SMS CONFIG ───────────────────────────────
SMS_ENABLE      = os.getenv("SMSM_ENABLE", "false").lower() == "true"
SMS_TEST_MODE   = os.getenv("SMSM_TEST_MODE", "true").lower() == "true"
SMS_TEST_NUMBER = os.getenv("SMSM_TEST_NUMBER", "")
SMS_API_KEY     = os.getenv("SMSM_API_KEY", "")
SMS_FROM        = os.getenv("SMSM_FROM", "")
SMS_URL         = os.getenv("SMSM_URL", "https://api.smsmobileapi.com/sendsms/")
SMS_TEMPLATE    = (
    "Hey {first}, this is Yoni Kutler—I saw your listing at {address} and wanted to "
    "introduce myself. I specialize in helping agents get faster bank approvals and "
    "ensure these deals close. I know you likely handle short sales yourself, but I "
    "work behind the scenes to take on lender negotiations so you can focus on selling. "
    "No cost to you or your client—I’m only paid by the buyer at closing. Would you be "
    "open to a quick call to see if this could help?"
)

MAX_Q_PHONE = 5
MAX_Q_EMAIL = 5

# ──────────────────────────── REGEXES ──────────────────────────────
SHORT_RE = re.compile(r"\bshort\s+sale\b", re.I)
BAD_RE   = re.compile(r"\b(?:approved short sale|short sale approved)\b", re.I)
TEAM_RE  = re.compile(r"^\s*the\b|\bteam\b", re.I)

IMG_EXT  = (".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp")
PHONE_RE = re.compile(r"(?<!\d)(?:\+?1[\s\-\.]*)?\(?\d{3}\)?[\s\-\.]*\d{3}[\s\-\.]*\d{4}(?!\d)")
EMAIL_RE = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.I)

LABEL_TABLE = {"mobile":4,"cell":4,"direct":4,"text":4,"c:":4,"m:":4,
               "phone":2,"tel":2,"p:":2,"office":1,"main":1,"customer":1,"footer":1}
LABEL_RE = re.compile("(" + "|".join(map(re.escape, LABEL_TABLE)) + ")", re.I)
US_AREA_CODES = {str(i) for i in range(201, 990)}

creds = Credentials.from_service_account_info(SC_JSON, scopes=SCOPES)
sheets_service = build("sheets", "v4", credentials=creds, cache_discovery=False)
gc = gspread.authorize(creds)
ws = gc.open_by_key(GSHEET_ID).sheet1

# ───────────────────────── SITE LISTS ──────────────────────────────
AGENT_SITES = [
    "realtor.com", "zillow.com", "redfin.com", "homesnap.com", "kw.com",
    "remax.com", "coldwellbanker.com", "compass.com", "exprealty.com",
    "bhhs.com", "c21.com", "realtyonegroup.com", "mlsmatrix.com",
    "mlslistings.com", "har.com", "brightmlshomes.com",
    "exitrealty.com", "realtyexecutives.com", "realty.com"
]

BROKERAGE_SITES = [
    "sothebysrealty.com", "corcoran.com", "douglaselliman.com",
    "cryereleike.com", "windermere.com", "longandfoster.com"
]
DOMAIN_CLAUSE    = " OR ".join(f"site:{d}" for d in AGENT_SITES)
BROKERAGE_CLAUSE = " OR ".join(f"site:{d}" for d in BROKERAGE_SITES)
SOCIAL_CLAUSE    = "site:facebook.com OR site:linkedin.com"

cache_p, cache_e = {}, {}
_executor = concurrent.futures.ThreadPoolExecutor(max_workers=GOOGLE_CONCURRENCY)

# ────────────────────────── UTILITIES ──────────────────────────────
def is_short_sale(t):
    return SHORT_RE.search(t) and not BAD_RE.search(t)

def fmt_phone(r):
    d = re.sub(r"\D", "", r)
    d = d[1:] if len(d) == 11 and d.startswith("1") else d
    return f"{d[:3]}-{d[3:6]}-{d[6:]}" if len(d) == 10 else ""

def valid_phone(p):
    if phonenumbers:
        try:
            return phonenumbers.is_possible_number(phonenumbers.parse(p, "US"))
        except Exception:
            return False
    return re.fullmatch(r"\d{3}-\d{3}-\d{4}", p) and p[:3] in US_AREA_CODES

def clean_email(e):
    return e.split("?")[0].strip()

def ok_email(e):
    e = clean_email(e)
    return e and "@" in e and not e.lower().endswith(IMG_EXT) and not re.search(r"\.(gov|edu|mil)$", e, re.I)

# ────────────────────────── fetch helpers ──────────────────────────
def fetch_simple(u):
    """Straight GET with no jina.ai tricks – used in the 2nd-pass crawl."""
    try:
        r = requests.get(u, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code == 200:
            return r.text
    except Exception as exc:
        LOG.debug("fetch_simple error %s on %s", exc, u)
    return None


def fetch(u):
    """
    Robust fetch with jina.ai fall-back.  Variant list order changed so we never
    generate the bad “…http://https://…” URL that jina.ai rejects.
    """
    bare = re.sub(r"^https?://", "", u)
    variants = [
        u,                                                     # direct GET
        f"https://r.jina.ai/http://{bare}",
        f"https://r.jina.ai/http://screenshot/{bare}",
    ]

    z403 = ratelimit = 0
    backoff = 1.0
    for url in variants:
        for _ in range(3):
            try:
                r = requests.get(url, timeout=10,
                                 headers={"User-Agent": "Mozilla/5.0"})
            except Exception as exc:
                METRICS["fetch_error"] += 1
                LOG.debug("fetch error %s on %s", exc, url)
                break

            if r.status_code == 200:
                # jina.ai sometimes returns HTML that says we’re blocked
                if "unusual traffic" in r.text[:700].lower():
                    METRICS["fetch_unusual"] += 1
                    break
                return r.text

            if r.status_code == 403 and "zillow.com" in url:
                z403 += 1
                METRICS["fetch_403"] += 1
                if z403 >= MAX_ZILLOW_403:
                    LOG.debug("bailing after %s consecutive Zillow 403s", z403)
                    return None
            elif r.status_code == 429:
                ratelimit += 1
                METRICS["fetch_429"] += 1
                if ratelimit >= MAX_RATE_429:
                    LOG.debug("rate-limit ceiling hit, giving up (%s)", url)
                    return None
            else:
                METRICS["fetch_other_%s" % r.status_code] += 1

            time.sleep(min(backoff, MAX_BACKOFF_SECONDS))
            backoff *= BACKOFF_FACTOR
    return None


# ─────────────────── Google CSE helper ─────────────────────────────
def google_items(q, tries=3):
    backoff = 1.0
    for _ in range(tries):
        try:
            j = requests.get(
                "https://www.googleapis.com/customsearch/v1",
                params={"key": CS_API_KEY, "cx": CS_CX, "q": q, "num": 10},
                timeout=10
            ).json()
            return j.get("items", [])
        except Exception:
            time.sleep(min(backoff, MAX_BACKOFF_SECONDS))
            backoff *= BACKOFF_FACTOR
    return []


# ─────────────────── proximity scan & structured ───────────────────
def proximity_scan(t, last_name=None):
    """
    Returns {phone: (best_label_weight, score, office_flag)}.
    If *last_name* is given, require that name to appear within ±50 chars of the phone –
    this tightens up hits on massive agent rosters.
    """
    out = {}
    for m in PHONE_RE.finditer(t):
        p = fmt_phone(m.group())
        if not valid_phone(p):
            continue

        sn_start = max(m.start() - 50, 0)
        sn_end   = min(m.end() + 50, len(t))
        snippet  = t[sn_start:sn_end]

        if last_name and last_name.lower() not in snippet:
            continue   # roster row that isn’t for our agent

        lab_match = LABEL_RE.search(snippet)
        lab = lab_match.group().lower() if lab_match else ""
        w = LABEL_TABLE.get(lab, 0)
        if w < 2:
            continue

        bw, ts, _ = out.get(p, (0, 0, False))
        out[p] = (max(bw, w), ts + 2 + w, lab in ("office", "main"))
    return out


def extract_struct(td):
    """Pulls phones/emails from JSON-LD, tel: & mailto: links."""
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


# ───────────────────── parallel map helper ─────────────────────────
def pmap(fn, iterable):
    return list(_executor.map(fn, iterable))


# ───────────────────── lookup functions ────────────────────────────
def _split_portals(urls):
    """Partition CSE URLs into (non_portal, portal)"""
    portals, non = [], []
    for u in urls:
        if any(d in u for d in AGENT_SITES):
            portals.append(u)
        else:
            non.append(u)
    return non, portals


def lookup_phone(agent, state):
    key = f"{agent}|{state}"
    if key in cache_p:
        return cache_p[key]

    cand_good, cand_office, src_good = {}, {}, {}

    def add(p, score, office_flag, src=""):
        d = fmt_phone(p)
        if not valid_phone(d):
            return
        target = cand_office if office_flag else cand_good
        target[d] = target.get(d, 0) + score
        if not office_flag and src:
            src_good[d] = src

    # ── 1️⃣  Google-CSE JSON only (fast, no page fetch) ────────────
    queries = build_q_phone(agent, state)[:MAX_Q_PHONE]
    google_batches = pmap(google_items, queries)

    for items in google_batches:
        for it in items:
            tel = it.get("pagemap", {}).get("contactpoint", [{}])[0].get("telephone")
            if tel:
                add(tel, 4, False, f"CSE:{it.get('link','')}")

    # if found already we can short-circuit
    if cand_good:
        phone = max(cand_good, key=cand_good.get)
        cache_p[key] = phone
        LOG.debug("PHONE WIN %s via %s", phone, src_good.get(phone, "CSE"))
        return phone

    # collect URLs from all CSE results
    urls_all = [it.get("link", "") for items in google_batches for it in items][:30]
    non_portal_urls, portal_urls = _split_portals(urls_all)
    last_name = (agent.split()[-1] if len(agent.split()) > 1 else agent).lower()

    # ── 2️⃣  Crawl non-portal URLs directly (simple GET) ───────────
    pages = pmap(fetch_simple, non_portal_urls)
    for url, page in zip(non_portal_urls, pages):
        if not page or agent.lower() not in page.lower():
            continue
        ph, _ = extract_struct(page)
        for p in ph:
            add(p, 6, False, url)
        low = html.unescape(page.lower())
        for p, (bw, sc, office_flag) in proximity_scan(low, last_name).items():
            add(p, sc, office_flag, url)
        if cand_good or cand_office:
            break

    # ── 3️⃣  Same non-portal URLs but via jina.ai screenshot ───────
    if not cand_good and not cand_office:
        pages = pmap(fetch, non_portal_urls)
        for url, page in zip(non_portal_urls, pages):
            if not page or agent.lower() not in page.lower():
                continue
            ph, _ = extract_struct(page)
            for p in ph:
                add(p, 5, False, url)
            low = html.unescape(page.lower())
            for p, (bw, sc, office_flag) in proximity_scan(low, last_name).items():
                add(p, sc, office_flag, url)
            if cand_good or cand_office:
                break

    # ── 4️⃣  Finally hit big-portal pages (Zillow, Redfin, etc) ────
    if not cand_good:
        pages = pmap(fetch, portal_urls)
        for url, page in zip(portal_urls, pages):
            if not page or agent.lower() not in page.lower():
                continue
            ph, _ = extract_struct(page)
            for p in ph:
                add(p, 4, False, url)
            low = html.unescape(page.lower())
            for p, (bw, sc, office_flag) in proximity_scan(low, last_name).items():
                add(p, sc, office_flag, url)
            if cand_good:
                break

    phone = ""
    if cand_good:
        phone = max(cand_good, key=cand_good.get)
    elif cand_office:
        phone = ""   # prefer none over office lines

    if phone:
        LOG.debug("PHONE WIN %s via %s", phone, src_good.get(phone, "crawler"))
    else:
        LOG.debug("PHONE FAIL for %s %s  cand_good=%s cand_office=%s",
                  agent, state, cand_good, cand_office)

    cache_p[key] = phone
    return phone


def lookup_email(agent, state):
    key = f"{agent}|{state}"
    if key in cache_e:
        return cache_e[key]

    cand, src_e = defaultdict(int), {}

    # 1️⃣  Google-CSE JSON only
    queries = build_q_email(agent, state)[:MAX_Q_EMAIL]
    google_batches = pmap(google_items, queries)

    for items in google_batches:
        for it in items:
            mail = clean_email(it.get("pagemap", {}).get("contactpoint", [{}])[0].get("email", ""))
            if ok_email(mail):
                cand[mail] += 3
                src_e.setdefault(mail, f"CSE:{it.get('link','')}")

    if cand:
        email = max(cand, key=cand.get)
        cache_e[key] = email
        LOG.debug("EMAIL WIN %s via %s", email, src_e.get(email, "CSE"))
        return email

    urls_all = [it.get("link", "") for items in google_batches for it in items][:30]
    non_portal_urls, portal_urls = _split_portals(urls_all)

    # 2️⃣  crawl non-portal direct
    for url, page in zip(non_portal_urls, pmap(fetch_simple, non_portal_urls)):
        if not page or agent.lower() not in page.lower():
            continue
        _, em = extract_struct(page)
        for m in em:
            m = clean_email(m)
            if ok_email(m):
                cand[m] += 3
                src_e.setdefault(m, url)
        for m in EMAIL_RE.findall(page):
            m = clean_email(m)
            if ok_email(m):
                cand[m] += 1
                src_e.setdefault(m, url)
        if cand:
            break

    # 3️⃣  non-portal via jina.ai
    if not cand:
        for url, page in zip(non_portal_urls, pmap(fetch, non_portal_urls)):
            if not page or agent.lower() not in page.lower():
                continue
            _, em = extract_struct(page)
            for m in em:
                m = clean_email(m)
                if ok_email(m):
                    cand[m] += 2
                    src_e.setdefault(m, url)
            for m in EMAIL_RE.findall(page):
                m = clean_email(m)
                if ok_email(m):
                    cand[m] += 1
                    src_e.setdefault(m, url)
            if cand:
                break

    # 4️⃣  portals last
    if not cand:
        for url, page in zip(portal_urls, pmap(fetch, portal_urls)):
            if not page or agent.lower() not in page.lower():
                continue
            _, em = extract_struct(page)
            for m in em:
                m = clean_email(m)
                if ok_email(m):
                    cand[m] += 2
                    src_e.setdefault(m, url)
            if cand:
                break

    # Prefer addresses that contain agent tokens
    tokens = {re.sub(r"[^a-z]", "", w.lower()) for w in agent.split()}
    good = {m: sc for m, sc in cand.items()
            if any(tok and tok in m.lower() for tok in tokens)}

    email = max(good, key=good.get) if good else (max(cand, key=cand.get) if cand else "")

    if email:
        LOG.debug("EMAIL WIN %s via %s", email, src_e.get(email, "crawler"))
    else:
        LOG.debug("EMAIL FAIL for %s %s  tokens=%s  candidates=%s",
                  agent, state, tokens, dict(list(cand.items())[:8]))

    cache_e[key] = email
    return email


# ───────────────────── Google Sheet helpers ────────────────────────
def mark_sent(row_idx: int):
    try:
        sheets_service.spreadsheets().values().update(
            spreadsheetId=GSHEET_ID,
            range=f"Sheet1!H{row_idx}:H{row_idx}",
            valueInputOption="RAW",
            body={"values": [["x"]]}
        ).execute()
        LOG.debug("Marked row %s column H as sent", row_idx)
    except Exception as e:
        LOG.error("GSheet mark_sent error %s", e)


def append_row(values):
    resp = sheets_service.spreadsheets().values().append(
        spreadsheetId=GSHEET_ID,
        range="Sheet1!A1",
        valueInputOption="RAW",
        body={"values": [values]}
    ).execute()
    row_idx = int(resp["updates"]["updatedRange"].split("!")[1].split(":")[0][1:])
    LOG.info("Row appended to sheet (row %s)", row_idx)
    return row_idx


def phone_exists(p):
    try:
        return p in ws.col_values(3)
    except Exception:
        return False


# ─────────────────────── SMS w/ delivery confirm ───────────────────
def send_sms(num, first, address, row_idx):
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
        "sendsms": "1"
    }
    if SMS_FROM:
        payload["from"] = SMS_FROM

    try:
        r = requests.post(SMS_URL, data=payload, timeout=15)
        if r.status_code != 200:
            LOG.error("SMS failed %s %s", r.status_code, r.text[:200])
            return False

        data = {}
        try:
            data = r.json()
        except Exception:
            pass

        msg_id = data.get("message_id")
        LOG.info("SMS queued to %s id=%s", to_e164, msg_id)

        # In test mode we mark immediately; otherwise we require confirmed delivery
        if SMS_TEST_MODE:
            mark_sent(row_idx)
            return True

        if not msg_id:
            LOG.warning("No message_id returned; not marking sent")
            return False

        good = {"SENT", "DELIVERED", "DELIVERED_TO_HANDSET"}
        for _ in range(6):                      # up to ≈3 min
            time.sleep(30)
            try:
                s = requests.get(f"{SMS_URL}{msg_id}/status", timeout=10)
                if s.status_code == 200:
                    status = s.json().get("status", "").upper()
                    LOG.debug("SMS id=%s poll status=%s", msg_id, status)
                    if status in good:
                        mark_sent(row_idx)
                        return True
            except Exception:
                pass
        LOG.warning("SMS never confirmed delivered for id=%s", msg_id)
    except Exception as e:
        LOG.error("SMS error %s", e)
    return False


# ─────────────────── Zillow status helper ──────────────────────────
def is_active_listing(zpid):
    if not RAPID_KEY:
        return True
    try:
        r = requests.get(
            f"https://{RAPID_HOST}/property",
            params={"zpid": zpid},
            headers={"X-RapidAPI-Key": RAPID_KEY,
                     "X-RapidAPI-Host": RAPID_HOST},
            timeout=15
        )
        r.raise_for_status()
        data = r.json().get("data") or r.json()
        status = (data.get("homeStatus") or "").upper()
        return (not status) or status in GOOD_STATUS
    except Exception as e:
        LOG.warning("RapidAPI status check failed for %s (%s) – keeping row", zpid, e)
        return True


# ───────────────────── scrape helpers ──────────────────────────────
def extract_name(t):
    m = re.search(r"listing agent[:\s\-]*([A-Za-z \.'’-]{3,})", t, re.I)
    if m:
        n = m.group(1).strip()
        if not TEAM_RE.search(n):
            return n
    return None


# ───────────────────── main row processor ──────────────────────────
def process_rows(rows):
    for r in rows:
        txt = (r.get("description", "") + " " + r.get("openai_summary", "")).strip()
        if not is_short_sale(txt):
            LOG.debug("SKIP non-short-sale %s (%s)", r.get("street"), r.get("zpid"))
            continue

        zpid = str(r.get("zpid", ""))
        if zpid and not is_active_listing(zpid):
            LOG.info("Skip stale/off-market zpid %s", zpid)
            continue

        name = r.get("agentName", "").strip()
        if not name:
            name = extract_name(txt)
            if not name:
                continue
        if TEAM_RE.search(name):
            alt = extract_name(txt)
            if alt:
                name = alt
            else:
                continue
        if TEAM_RE.search(name):
            continue

        state = r.get("state", "")

        phone = fmt_phone(lookup_phone(name, state))
        email = lookup_email(name, state)

        if phone and phone_exists(phone):
            continue

        first, *last = name.split()
        row_idx = append_row([first, " ".join(last), phone, email,
                              r.get("street", ""), r.get("city", ""), state, ""])

        if phone:
            send_sms(phone, first, r.get("street", ""), row_idx)


# ——— main webhook entry ———
if __name__ == "__main__":
    try:
        payload = json.load(sys.stdin)
    except json.JSONDecodeError:
        LOG.debug("No stdin payload; exiting")
        sys.exit(0)

    fresh_rows = payload.get("listings", [])
    if not fresh_rows:
        LOG.info("No listings in payload")
        sys.exit(0)

    LOG.debug("Sample fields on first fresh row: %s", list(fresh_rows[0].keys()))
    process_rows(fresh_rows)

    if METRICS:
        LOG.info("metrics %s", dict(METRICS))

