#!/usr/bin/env python3
"""
Zillow Short-Sale Scraper + SMS Bot
Runs hourly between 8 AM and 8 PM Eastern, seven days a week.
"""

import json, re, sqlite3, time, random, requests, pytz, os, threading
from datetime import datetime, timedelta
from fake_useragent import UserAgent
from bs4 import BeautifulSoup
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import openai
from sms_providers import get_sender
from urllib.parse import urlparse
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------- 0. CONFIG ----------
with open("config.json") as f:
    CFG = json.load(f)

openai.api_key = CFG["openai_api_key"]
ua = UserAgent()
GOOGLE_API_KEY = CFG.get("google_api_key")
GOOGLE_CX = CFG.get("google_cx")
CLOUDMERSIVE_KEY = os.getenv("CLOUDMERSIVE_KEY", "").strip()


def _cfg_float(key: str, default: float) -> float:
    try:
        return float(CFG.get(key, default))
    except (TypeError, ValueError):
        return float(default)


CSE_MIN_INTERVAL = _cfg_float("google_cse_min_interval", 2.2)
CSE_JITTER_LOW = _cfg_float("google_cse_jitter_low", 0.35)
CSE_JITTER_HIGH = _cfg_float("google_cse_jitter_high", 0.85)
if CSE_JITTER_HIGH < CSE_JITTER_LOW:
    CSE_JITTER_LOW, CSE_JITTER_HIGH = CSE_JITTER_HIGH, CSE_JITTER_LOW

POST_CSE_DELAY_LOW = _cfg_float("google_cse_post_delay_low", 0.9)
POST_CSE_DELAY_HIGH = _cfg_float("google_cse_post_delay_high", 1.6)
if POST_CSE_DELAY_HIGH < POST_CSE_DELAY_LOW:
    POST_CSE_DELAY_LOW, POST_CSE_DELAY_HIGH = POST_CSE_DELAY_HIGH, POST_CSE_DELAY_LOW

CONTACT_DOMAIN_MIN_DELAY = _cfg_float("contact_domain_min_delay", 4.0)
CONTACT_DOMAIN_DELAY_JITTER = _cfg_float("contact_domain_delay_jitter", 1.4)

# expose SMS Gateway credentials
os.environ.setdefault("SMS_GATEWAY_API_KEY", CFG.get("sms_gateway_api_key", ""))

ACCEPT_LANGUAGES = [
    "en-US,en;q=0.9",
    "en-US,en;q=0.8,es;q=0.5",
    "en-CA,en;q=0.8",
    "en-GB,en;q=0.9",
]

BLOCK_SIGNATURES = (
    "captcha",
    "access denied",
    "unusual traffic",
    "verify you are human",
    "temporarily blocked",
    "service unavailable",
)

BLOCKED_DOMAINS = {
    "facebook.com",
    "www.facebook.com",
    "m.facebook.com",
    "linkedin.com",
    "www.linkedin.com",
    "instagram.com",
    "www.instagram.com",
}

_LAST_CSE_AT = 0.0
_CSE_BACKOFF_UNTIL = 0.0
_CSE_LOCK = threading.Lock()

_CONTACT_DOMAIN_LAST_FETCH = {}
_LINE_TYPE_CACHE: dict[str, bool] = {}

def random_headers(extra=None) -> dict:
    headers = {
        "User-Agent": ua.random,
        "Accept-Language": random.choice(ACCEPT_LANGUAGES),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Connection": "close",
    }
    if extra:
        headers.update(extra)
    return headers


def new_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=3,
        connect=2,
        read=2,
        backoff_factor=1.3,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "HEAD"),
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def looks_like_block(response: requests.Response, body: str = None) -> bool:
    # Treat common block status codes immediately, before inspecting the body.
    if response.status_code in (401, 403, 429, 503, 520):
        return True
    if body is None:
        try:
            body = response.text
        except Exception:
            body = ""
    lower_body = body.lower()
    return any(sig in lower_body for sig in BLOCK_SIGNATURES)


def domain_from_url(url: str) -> str:
    try:
        return urlparse(url).netloc.lower()
    except Exception:
        return ""


def decode_cfemail(data: str) -> str:
    """Decode Cloudflare's data-cfemail obfuscation."""
    if not data:
        return ""
    try:
        key = int(data[:2], 16)
    except ValueError:
        return ""
    decoded_chars = []
    for idx in range(2, len(data), 2):
        chunk = data[idx : idx + 2]
        if len(chunk) < 2:
            return ""
        try:
            decoded_chars.append(chr(int(chunk, 16) ^ key))
        except ValueError:
            return ""
    return "".join(decoded_chars)

# ---------- 1. ZILLOW HELPERS ----------
def z_get(url: str) -> requests.Response:
    session = new_session()
    return session.get(url, headers=random_headers(), timeout=20)

def parse_state_json(html: str) -> list:
    """
    Extract search results either from Zillow’s modern
    <script id="__NEXT_DATA__"> JSON or, if that fails, from the
    legacy HTML-comment block.  Returns searchResults.mapResults.
    """
    # ── 1️⃣ Modern location ───────────────
    m = re.search(
        r'<script[^>]*id="__NEXT_DATA__"[^>]*>(\{.*?\})</script>',
        html,
        re.DOTALL,
    )
    if m:
        data = json.loads(m.group(1))          # strict JSON (no </script>)
        try:
            state = data["props"]["pageProps"]["searchPageState"]
            cat1 = state.get("cat1", {})
            sr   = cat1.get("searchResults") or state.get("searchResults", {})
            if sr:
                if "mapResults" in sr:
                    return sr["mapResults"]
                if "listResults" in sr:        # mobile view sometimes
                    return sr["listResults"]
        except (KeyError, TypeError):
            pass  # fall through to legacy

    # ── 2️⃣ Legacy location (HTML comment) ─
    m = re.search(r'<!--\s*({.*?})\s*-->', html, re.DOTALL)
    if not m:
        raise ValueError("Zillow JSON blob not found")
    data = json.loads(m.group(1))
    first_key = next(iter(data["apiCache"]))
    results = data["apiCache"][first_key]["propertySearchSearchResultsV3"]
    return results["searchResults"]["mapResults"]
    results = data["apiCache"][first_key]["propertySearchSearchResultsV3"]
    return results["searchResults"]["mapResults"]
# ---------- 1A. PARSE ZILLOW HTML → LIST OF HOMES ----------
def parse_state_json(html: str) -> list:
    """
    Return Zillow search results (mapResults) from a Search Results Page.

    1. Preferred: modern  <script id="__NEXT_DATA__"> … JSON.
       • Handles both the new "cat1" layout and the earlier "searchResults".
    2. Fallback: legacy  <!-- { … } -->  comment block.
    Raises ValueError only if neither location exists.
    """
    import json, re

    # ── 1️⃣  Modern location ─────────────────────────────────────────
    m = re.search(
        r'<script[^>]*id="__NEXT_DATA__"[^>]*>(\{.*?\})\s*</script>',
        html,
        re.DOTALL,
    )
    if m:
        data = json.loads(m.group(1))
        # Newest layout: props → pageProps → searchPageState → cat1 …
        try:
            return (
                data["props"]["pageProps"]["searchPageState"]
                    ["cat1"]["searchResults"]["mapResults"]
            )
        except (KeyError, TypeError):
            pass
        # Older (but still modern) layout
        try:
            return (
                data["props"]["pageProps"]["searchPageState"]
                    ["searchResults"]["mapResults"]
            )
        except (KeyError, TypeError):
            pass  # fall through to legacy

    # ── 2️⃣  Legacy HTML-comment block ───────────────────────────────
    m = re.search(r'<!--\s*({.*?})\s*-->', html, re.DOTALL)
    if not m:
        raise ValueError("Zillow JSON blob not found")

    data = json.loads(m.group(1))
    first_key = next(iter(data["apiCache"]))
    results = data["apiCache"][first_key]["propertySearchSearchResultsV3"]
    return results["searchResults"]["mapResults"]

def qualifies(home: dict) -> bool:
    desc = (home.get("description") or "").lower()
    if CFG["must_include"] not in desc:
        return False
    if any(p in desc for p in CFG["disallowed_phrases"]):
        return False
    htype = home.get("hdpData", {}).get("homeInfo", {}).get("homeType", "")
    if htype not in CFG["allowed_types"]:
        return False
    if home.get("state") in CFG["disallowed_states"]:
        return False
    return True

def agent_name(home: dict) -> str:
    return home.get("agentName", "Agent")

# ---------- 2. PROFILE PAGE LOOKUP ----------
BROKER_PATTERNS = [
    "remax",
    "compass",
    "kw",
    "kellerwilliams",
    "century21",
    "coldwellbanker",
    "exp",
    "realty",
    "sothebys",
    "bhhs",
]

def google_search_items(query: str) -> tuple[list[dict], bool]:
    """Return raw result items from Google Custom Search API.

    The second element of the return tuple indicates whether we observed a
    rate-limit response (429 or related CSE error reasons). Callers can use
    this to short-circuit additional CSE traffic.
    """

    if not GOOGLE_API_KEY or not GOOGLE_CX:
        return [], False

    global _CSE_BACKOFF_UNTIL
    now = time.time()
    if now < _CSE_BACKOFF_UNTIL:
        wait_for = int(_CSE_BACKOFF_UNTIL - now)
        print(
            f"Skipping CSE query due to active backoff window ({wait_for}s remaining)"
        )
        return [], True

    global _LAST_CSE_AT
    params = {"key": GOOGLE_API_KEY, "cx": GOOGLE_CX, "q": query, "num": 10}

    # Build a lightweight session with no automatic 429 retries to avoid
    # stacking retries with our own backoff logic.
    session = requests.Session()
    retry = Retry(
        total=2,
        connect=1,
        read=1,
        backoff_factor=1.0,
        status_forcelist=(500, 502, 503, 504),
        allowed_methods=("GET", "HEAD"),
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    with _CSE_LOCK:
        delta = time.time() - _LAST_CSE_AT
        jitter = random.uniform(CSE_JITTER_LOW, CSE_JITTER_HIGH) if CSE_JITTER_HIGH > 0 else 0.0
        min_gap = max(0.0, CSE_MIN_INTERVAL + jitter)
        if delta < min_gap:
            time.sleep(min_gap - delta)
        _LAST_CSE_AT = time.time()

    rate_limited = False
    for attempt in range(3):
        delay = 1.5 * (attempt + 1)
        headers = random_headers({"Accept": "application/json"})
        try:
            resp = session.get(
                "https://www.googleapis.com/customsearch/v1",
                params=params,
                headers=headers,
                timeout=15,
            )
        except requests.RequestException as exc:
            print("Google search error:", exc)
            time.sleep(delay)
            continue

        try:
            payload = resp.json()
        except ValueError:
            print("Google search returned non-JSON payload", resp.status_code)
            time.sleep(delay)
            continue

        if resp.status_code != 200 or "error" in payload:
            error = payload.get("error", {})
            reason = ""
            if error:
                reason = error.get("errors", [{}])[0].get("reason", "")
            print(
                f"Google search API issue (status {resp.status_code} reason {reason or 'unknown'})"
            )

            if resp.status_code == 429 or reason in {
                "rateLimitExceeded",
                "userRateLimitExceeded",
                "dailyLimitExceeded",
            }:
                rate_limited = True
                retry_after = resp.headers.get("Retry-After")
                backoff_for = delay * 2
                if retry_after:
                    try:
                        backoff_for = float(retry_after)
                        time.sleep(backoff_for)
                    except ValueError:
                        time.sleep(delay * 2)
                _CSE_BACKOFF_UNTIL = max(_CSE_BACKOFF_UNTIL, time.time() + backoff_for)
                retry_at = datetime.fromtimestamp(_CSE_BACKOFF_UNTIL)
                print(
                    f"CSE rate-limit detected; deferring further lookups until {retry_at}"
                )
                break

            if resp.status_code in (503,):
                time.sleep(delay + random.uniform(0.5, 1.5))
                continue

            return [], rate_limited

        items = payload.get("items", [])
        if items:
            return items, rate_limited
        # Empty responses are often transient; small pause before retrying
        time.sleep(delay)

    return [], rate_limited


def _compact_tokens(*parts: str) -> str:
    return " ".join(p.strip() for p in parts if p and p.strip())


def build_phone_queries(
    name: str,
    state: str,
    brokerage: str = "",
    *,
    city: str = "",
    postal_code: str = "",
    address: str = "",
) -> list[str]:
    """Generate targeted phone search strings with location context."""

    queries: list[str] = []

    def _add(q: str) -> None:
        if q and q not in queries:
            queries.append(q)

    address_base = _compact_tokens(f'"{name}"', address)
    localized_base = _compact_tokens(f'"{name}"', city, state, postal_code)
    state_base = _compact_tokens(f'"{name}"', state)
    name_only = f'"{name}"'.strip()

    for base in (address_base, localized_base, state_base, name_only):
        if not base:
            continue
        _add(f"{base} realtor phone")
        _add(f"{base} real estate cell")
        _add(f"{base} mobile")

    if brokerage:
        _add(f'"{name}" "{brokerage}" phone')
        _add(f'"{brokerage}" {state} "phone"')

    return queries


def build_email_queries(
    name: str,
    state: str,
    brokerage: str = "",
    domain_hint: str = "",
    *,
    city: str = "",
    postal_code: str = "",
    address: str = "",
) -> list[str]:
    """Generate targeted email search strings with location context."""

    queries: list[str] = []

    def _add(q: str) -> None:
        if q and q not in queries:
            queries.append(q)

    address_base = _compact_tokens(f'"{name}"', address)
    localized_base = _compact_tokens(f'"{name}"', city, state, postal_code)
    state_base = _compact_tokens(f'"{name}"', state)
    name_only = f'"{name}"'.strip()

    for base in (address_base, localized_base, state_base, name_only):
        if not base:
            continue
        _add(f"{base} email")
        _add(f"{base} contact email")
        _add(f"{base} real estate email")

    if brokerage:
        _add(f'"{name}" "{brokerage}" email')
    if domain_hint:
        _add(f'site:{domain_hint} "{name}" email')

    return queries

def extract_contact(html: str):
    """Return lists of phone tuples and emails from HTML."""
    soup = BeautifulSoup(html, "html.parser")
    phones = []
    emails = []
    seen_emails = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        text = a.get_text(" ", strip=True).lower()
        if href.startswith("tel:"):
            phones.append((href[4:], text))
        elif href.startswith("mailto:"):
            addr = href[7:]
            if addr not in seen_emails:
                emails.append(addr)
                seen_emails.add(addr)

    for cf_node in soup.select("[data-cfemail]"):
        decoded = decode_cfemail(cf_node.get("data-cfemail", ""))
        if decoded and decoded not in seen_emails:
            emails.append(decoded)
            seen_emails.add(decoded)
    # Fallback regex search if anchors not found
    if not phones or not emails:
        text_blob = soup.get_text(" ", strip=True)
        if not phones:
            for m in re.findall(r"\+?\d[\d\-\.\(\)\s]{7,}\d", text_blob):
                phones.append((m, ""))
        if not emails:
            for match in re.findall(r"[\w\.-]+@[\w\.-]+", text_blob):
                if match not in seen_emails:
                    emails.append(match)
                    seen_emails.add(match)
    return phones, emails

def _digits_only(num: str) -> str:
    digits = re.sub(r"\D", "", num or "")
    if len(digits) == 10:
        digits = "1" + digits
    return digits


def is_mobile_number(phone: str) -> bool:
    """Return True if *phone* is classified as a mobile line via Cloudmersive."""

    if not phone:
        return False
    cached = _LINE_TYPE_CACHE.get(phone)
    if cached is not None:
        return cached
    if not CLOUDMERSIVE_KEY:
        return True
    digits = _digits_only(phone)
    try:
        resp = requests.post(
            "https://api.cloudmersive.com/validate/phonenumber/basic",
            json={"PhoneNumber": digits, "DefaultCountryCode": "US"},
            headers={"Apikey": CLOUDMERSIVE_KEY},
            timeout=6,
        )
        data = resp.json()
    except Exception as exc:
        print(f"Cloudmersive lookup failed for {phone}: {exc}")
        _LINE_TYPE_CACHE[phone] = False
        return False

    phone_type = str(data.get("PhoneNumberType", "")).strip().lower()
    line_type = str(data.get("LineType", "")).strip().lower()
    is_mobile = bool(data.get("IsMobile"))
    if not is_mobile:
        if phone_type in {"mobile", "fixedlineormobile"}:
            is_mobile = True
        elif line_type == "fixedlineormobile":
            is_mobile = True

    print(f"Cloudmersive classified {digits} as mobile={is_mobile}")
    _LINE_TYPE_CACHE[phone] = is_mobile
    return is_mobile


def select_best_phone(phones):
    """Prefer numbers labeled mobile/cell/direct and validated as mobile."""

    if not phones:
        return ""

    prioritized = []
    fallback = []
    for num, ctx in phones:
        if not num:
            continue
        target = prioritized if any(k in ctx for k in ("mobile", "cell", "direct")) else fallback
        target.append((num.strip(), ctx))

    ordered = prioritized + fallback
    for num, _ in ordered:
        if is_mobile_number(num):
            return num
    return ordered[0][0] if ordered else ""


def brokerage_domain_url(brokerage: str) -> str:
    """Return a best-guess brokerage homepage derived from the name."""

    if not brokerage:
        return ""
    slug = re.sub(r"[^a-z0-9]", "", brokerage.lower())
    if not slug:
        return ""
    return f"https://{slug}.com"

def search_agent_profile(
    name: str,
    state: str,
    brokerage: str = "",
    prop_addr: str = "",
    *,
    city: str = "",
    postal_code: str = "",
) -> tuple[str, str, bool]:
    """Search Google for agent contact info using multiple targeted queries.

    Returns (phone, email, blocked_flag).  The blocked flag indicates whether we
    detected a probable access block while crawling supporting pages.  In that
    case callers should skip marking the listing as processed so it can be
    retried later.  When blocked, a lightweight brokerage scrape and ChatGPT
    lookup are attempted as fallbacks before giving up.
    """

    session = new_session()

    if time.time() < _CSE_BACKOFF_UNTIL:
        retry_at = datetime.fromtimestamp(_CSE_BACKOFF_UNTIL)
        print(
            f"Skipping profile search for {name} due to active CSE backoff until {retry_at}"
        )
        return "", "", True
    queries = build_phone_queries(
        name,
        state,
        brokerage,
        city=city,
        postal_code=postal_code,
        address=prop_addr,
    ) + build_email_queries(
        name,
        state,
        brokerage,
        city=city,
        postal_code=postal_code,
        address=prop_addr,
    )
    items = []
    cse_blocked = False
    for q in queries:
        batch, rate_limited = google_search_items(q)
        items.extend(batch)
        if rate_limited:
            cse_blocked = True
            break
        if POST_CSE_DELAY_HIGH > 0:
            time.sleep(random.uniform(POST_CSE_DELAY_LOW, POST_CSE_DELAY_HIGH))

    dedup: dict[str, dict] = {}
    for it in items:
        link = it.get("link", "")
        if not link:
            continue
        domain = domain_from_url(link)
        if domain in BLOCKED_DOMAINS:
            continue
        if link not in dedup:
            dedup[link] = it
    items = list(dedup.values())

    def _score(it: dict) -> int:
        url = it.get("link", "").lower()
        sc = 0
        if all(tok.lower() in url for tok in name.split() if tok):
            sc += 2
        if brokerage and brokerage.lower().replace(" ", "") in url:
            sc += 2
        if any(pat in url for pat in BROKER_PATTERNS):
            sc += 1
        return sc

    items.sort(key=_score, reverse=True)
    if len(items) > 12:
        items = items[:12]

    phone = ""
    email = ""
    blocked = cse_blocked

    for it in items:
        cp = (it.get("pagemap", {}).get("contactpoint") or [{}])[0]
        phone = phone or cp.get("telephone", "")
        email = email or cp.get("email", "")
        if phone and email:
            return phone, email, False

    domain_last_hit: dict[str, float] = {}
    max_fetches = 6
    fetches = 0
    for it in items:
        if phone and email:
            break
        if fetches >= max_fetches:
            break
        link = it.get("link", "")
        if not link:
            continue
        domain = domain_from_url(link)
        if domain in BLOCKED_DOMAINS:
            continue
        last = domain_last_hit.get(domain) or _CONTACT_DOMAIN_LAST_FETCH.get(domain)
        if last:
            elapsed = time.time() - last
            min_gap = CONTACT_DOMAIN_MIN_DELAY
            if elapsed < min_gap:
                wait = min_gap - elapsed
                if CONTACT_DOMAIN_DELAY_JITTER > 0:
                    wait += random.uniform(0.2, CONTACT_DOMAIN_DELAY_JITTER)
                time.sleep(max(0.0, wait))
        now = time.time()
        domain_last_hit[domain] = now
        _CONTACT_DOMAIN_LAST_FETCH[domain] = now

        try:
            resp = session.get(link, headers=random_headers(), timeout=15)
        except requests.RequestException as exc:
            print(f"Error fetching {link}: {exc}")
            continue

        if looks_like_block(resp):
            blocked = True
            print(
                f"Block detected while fetching {link} (status {resp.status_code})."
            )
            time.sleep(random.uniform(6, 10))
            break

        html = resp.text
        phones, emails = extract_contact(html)
        if not phone:
            phone = select_best_phone(phones)
        if not email and emails:
            email = emails[0]
        fetches += 1
        time.sleep(random.uniform(0.5, 1.0))

    if cse_blocked and not email:
        fallback_url = brokerage_domain_url(brokerage)
        if fallback_url:
            try:
                resp = session.get(fallback_url, headers=random_headers(), timeout=10)
                if looks_like_block(resp):
                    blocked = True
                    print(
                        f"Block detected while fetching brokerage fallback {fallback_url}"
                    )
                else:
                    phones, emails = extract_contact(resp.text)
                    if not phone:
                        phone = select_best_phone(phones)
                    if not email and emails:
                        email = emails[0]
            except requests.RequestException as exc:
                print(f"Error fetching brokerage fallback {fallback_url}: {exc}")

        if not email and prop_addr:
            chat_phone, chat_email = get_contact_info(name, prop_addr)
            if not phone:
                phone = chat_phone
            if chat_email:
                email = chat_email

    return phone, email, blocked or cse_blocked

# ---------- 3. CHATGPT LOOKUP ----------
def get_contact_info(name: str, prop_addr: str) -> tuple[str, str]:
    prompt = (
        "Find the MOBILE phone number and EMAIL address for real-estate agent "
        + name +
        " who has a listing at " + prop_addr +
        ". Return exactly:\nMobile: <number>\nEmail: <email>\n"
    )
    try:
        resp = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "You are a helpful real-estate assistant."},
                {"role": "user",   "content": prompt}
            ],
            max_tokens=150,
            temperature=0.4
        )
        text = resp.choices[0].message.content
        mob = re.search(r"Mobile:\s*([\+\d\-\(\)\s]+)", text)
        eml = re.search(r"Email:\s*([\w\.-]+@[\w\.-]+)", text)
        return (
            mob.group(1).strip() if mob else "",
            eml.group(1).strip() if eml else ""
        )
    except Exception as e:
        print("ChatGPT lookup failed:", e)
        return "", ""

# ---------- 4. GOOGLE SHEETS ----------
scope = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]
creds = ServiceAccountCredentials.from_json_keyfile_name("service_account.json", scope)
sheet = gspread.authorize(creds).open(CFG["google_sheet_name"]).sheet1

# Cache existing phone numbers once on startup to avoid repeated fetches
try:
    KNOWN_PHONES = {p.strip() for p in sheet.col_values(3)}  # column 3 = "Phone"
except Exception as e:
    print("Phone preload error:", e)
    KNOWN_PHONES = set()

def is_duplicate(phone: str) -> bool:
    return phone.strip() in KNOWN_PHONES

def add_row(first, last, phone, email, street, city, state) -> None:
    if is_duplicate(phone):
        print("Duplicate phone, skipping", phone)
        return
    try:
        sheet.append_row(
            [first, last, phone, email, street, city, state],
            value_input_option="USER_ENTERED"
        )
        KNOWN_PHONES.add(phone.strip())
        print("Added row for", first, last)
    except Exception as e:
        print("Add-row error:", e)

# ---------- 5. SMS ----------
sms = get_sender(CFG.get("sms_provider"))

# ---------- 6. LOCAL SQLITE (dedupe by zpid) ----------
DB_PATH = "seen.db"

def _ensure_table(conn: sqlite3.Connection) -> None:
    """Create the processed table if it doesn't already exist."""
    conn.execute("CREATE TABLE IF NOT EXISTS processed (zpid TEXT PRIMARY KEY)")

def already_sent(zpid: str) -> bool:
    """Check if we've already processed this zpid."""
    with sqlite3.connect(DB_PATH) as conn:
        _ensure_table(conn)
        return conn.execute(
            "SELECT 1 FROM processed WHERE zpid = ?", (zpid,)
        ).fetchone() is not None

def mark_sent(zpid: str) -> None:
    """Record that we've processed this zpid."""
    with sqlite3.connect(DB_PATH) as conn:
        _ensure_table(conn)
        conn.execute("INSERT OR IGNORE INTO processed VALUES (?)", (zpid,))
        conn.commit()

# ---------- 7. MAIN CYCLE ----------
def run_cycle() -> None:
    print("Checking Zillow …")
    try:
        html = z_get(CFG["zillow_search_url"]).text
        with open("/tmp/last_zillow.html", "w", encoding="utf-8") as _fp:
            _fp.write(html)
        homes = parse_state_json(html)
        print(f"Pulled {len(homes)} homes from Zillow")
        print("Example statusText / description of first 3:")
        for h in homes[:3]:
            print(" •", h.get("statusText"), "|", (h.get("description") or "")[:60])


    except Exception as e:
        print("Zillow fetch error:", e)
        return

    for home in homes:
        zpid = str(home["zpid"])
        if already_sent(zpid):
            continue
        if not qualifies(home):
            continue

        address = home["address"]
        name = agent_name(home)

        addr_parts = [p.strip() for p in address.split(",")]
        street = addr_parts[0] if len(addr_parts) >= 1 else ""
        city = addr_parts[1] if len(addr_parts) >= 2 else ""
        state_zip = addr_parts[2].split() if len(addr_parts) >= 3 else []
        state = state_zip[0] if state_zip else ""
        postal_code = state_zip[1] if len(state_zip) >= 2 else ""
        brokerage = home.get("brokerageName", "") or home.get("brokerName", "")

        phone = ""
        email = ""
        blocked = False
        try:
            phone, email, blocked = search_agent_profile(
                name,
                state,
                brokerage,
                address,
                city=city,
                postal_code=postal_code,
            )
        except Exception as exc:
            print("Agent profile search error:", exc)

        if blocked:
            print(f"Blocked while searching for {name}; will retry later.")
            time.sleep(random.uniform(4, 7))
            continue

        try:
            if not phone or not email:
                chat_phone, chat_email = get_contact_info(name, address)
                if not phone:
                    phone = chat_phone
                if not email:
                    email = chat_email
            if not phone:
                print("No mobile for", name, "|", address)
                continue

            parts = name.split()
            first = parts[0]
            last  = " ".join(parts[1:]) if len(parts) > 1 else ""

            add_row(first, last, phone, email, street, city, state)

            sms_text = CFG["sms_template"].format(first=first, address=street)
            try:
                sms.send(phone, sms_text)
                print("SMS sent to", first, phone)
            except Exception as e:
                print("SMS error:", e)
        finally:
            mark_sent(zpid)

# ---------- 8. SCHEDULER ----------
ET = pytz.timezone("US/Eastern")


def _next_run_time(now: datetime) -> datetime:
    """Return the next hourly run time between 8 AM and 8 PM Eastern."""

    if now.hour < 8:
        return now.replace(hour=8, minute=0, second=0, microsecond=0)
    if now.hour >= 20:
        return (now + timedelta(days=1)).replace(
            hour=8, minute=0, second=0, microsecond=0
        )
    return (now + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)


while True:
    now = datetime.now(ET)
    if 8 <= now.hour <= 20:
        run_cycle()
    else:
        print(
            "Outside run window; Zillow scrape resumes at",
            _next_run_time(now).strftime("%I:%M %p %Z"),
        )

    wake = _next_run_time(datetime.now(ET))
    sleep_for = max(0, (wake - datetime.now(ET)).total_seconds())
    print(
        f"Sleeping {int(sleep_for/60)} min — next run {wake.strftime('%I:%M %p %Z')}"
    )
    time.sleep(sleep_for)
