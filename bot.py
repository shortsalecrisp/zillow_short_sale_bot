#!/usr/bin/env python3
"""
Zillow Short-Sale Scraper + SMS Bot
Runs 8 AM – 8 PM Eastern at random 51–72-minute intervals.
"""

import json, re, sqlite3, time, random, requests, pytz, os
from datetime import datetime, timedelta
from fake_useragent import UserAgent
from bs4 import BeautifulSoup
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import openai
from sms_providers import get_sender

# ---------- 0. CONFIG ----------
with open("config.json") as f:
    CFG = json.load(f)

openai.api_key = CFG["openai_api_key"]
ua = UserAgent()
GOOGLE_API_KEY = CFG.get("google_api_key")
GOOGLE_CX = CFG.get("google_cx")

# expose SMS credentials for provider abstraction
os.environ.setdefault("SMS_GATEWAY_API_KEY", CFG.get("sms_gateway_api_key", ""))
os.environ.setdefault("SMSMOBILE_API_KEY", CFG.get("smsmobile_api_key", ""))
os.environ.setdefault("SMSMOBILE_FROM", CFG.get("smsmobile_from", ""))

# ---------- 1. ZILLOW HELPERS ----------
def z_get(url: str) -> requests.Response:
    headers = {
        "User-Agent": ua.random,
        "Accept-Language": "en-US,en;q=0.9"
    }
    return requests.get(url, headers=headers, timeout=20)

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

def google_search_items(query: str) -> list[dict]:
    """Return raw result items from Google Custom Search API."""
    if not GOOGLE_API_KEY or not GOOGLE_CX:
        return []
    params = {"key": GOOGLE_API_KEY, "cx": GOOGLE_CX, "q": query, "num": 10}
    try:
        r = requests.get("https://www.googleapis.com/customsearch/v1", params=params, timeout=15)
        return r.json().get("items", [])
    except Exception as e:
        print("Google search error:", e)
        return []


def build_phone_queries(name: str, state: str, brokerage: str = "") -> list[str]:
    """Generate targeted phone search strings."""
    base = f'"{name}" {state}' if state else f'"{name}"'
    out = [
        f"{base} mobile",
        f"{base} cell",
        f"{base} phone",
    ]
    if brokerage:
        out.append(f'"{name}" "{brokerage}" phone')
    return out


def build_email_queries(name: str, state: str, brokerage: str = "", domain_hint: str = "") -> list[str]:
    """Generate targeted email search strings."""
    base = f'"{name}" {state}' if state else f'"{name}"'
    out = [
        f"{base} email",
        f"{base} contact email",
        f"{base} real estate email",
    ]
    if brokerage:
        out.append(f'"{name}" "{brokerage}" email')
    if domain_hint:
        out.append(f'site:{domain_hint} "{name}" email')
    return out

def extract_contact(html: str):
    """Return lists of phone tuples and emails from HTML."""
    soup = BeautifulSoup(html, "html.parser")
    phones = []
    emails = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        text = a.get_text(" ", strip=True).lower()
        if href.startswith("tel:"):
            phones.append((href[4:], text))
        elif href.startswith("mailto:"):
            emails.append(href[7:])
    # Fallback regex search if anchors not found
    if not phones or not emails:
        text_blob = soup.get_text(" ", strip=True)
        if not phones:
            for m in re.findall(r"\+?\d[\d\-\.\(\)\s]{7,}\d", text_blob):
                phones.append((m, ""))
        if not emails:
            emails.extend(re.findall(r"[\w\.-]+@[\w\.-]+", text_blob))
    return phones, emails

def select_best_phone(phones):
    """Prefer numbers labeled mobile/cell/direct."""
    for num, ctx in phones:
        if any(k in ctx for k in ("mobile", "cell", "direct")):
            return num
    return phones[0][0] if phones else ""

def search_agent_profile(name: str, state: str, brokerage: str = "") -> tuple[str, str]:
    """Search Google for agent contact info using multiple targeted queries."""
    headers = {"User-Agent": ua.random}
    queries = build_phone_queries(name, state, brokerage) + build_email_queries(
        name, state, brokerage
    )
    items = []
    for q in queries:
        items.extend(google_search_items(q))
    dedup: dict[str, dict] = {}
    for it in items:
        link = it.get("link", "")
        if link and link not in dedup:
            dedup[link] = it
    items = list(dedup.values())

    def _score(it: dict) -> int:
        url = it.get("link", "").lower()
        sc = 0
        if all(tok.lower() in url for tok in name.split()):
            sc += 2
        if brokerage and brokerage.lower().replace(" ", "") in url:
            sc += 1
        return sc

    items.sort(key=_score, reverse=True)

    phone = email = ""
    for it in items:
        cp = (it.get("pagemap", {}).get("contactpoint") or [{}])[0]
        phone = phone or cp.get("telephone", "")
        email = email or cp.get("email", "")
        if phone and email:
            return phone, email

    for it in items:
        link = it.get("link", "")
        if not link:
            continue
        try:
            resp = requests.get(link, headers=headers, timeout=15)
        except Exception:
            continue
        phones, emails = extract_contact(resp.text)
        if not phone:
            phone = select_best_phone(phones)
        if not email and emails:
            email = emails[0]
        if phone and email:
            break
    return phone, email

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
        state = addr_parts[2].split()[0] if len(addr_parts) >= 3 else ""
        brokerage = home.get("brokerageName", "") or home.get("brokerName", "")

        # Mark first so we never re-process this zpid
        mark_sent(zpid)

        phone, email = search_agent_profile(name, state, brokerage)
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
            sms.send_sms(phone, sms_text)
            print("SMS sent to", first, phone)
        except Exception as e:
            print("SMS error:", e)

# ---------- 8. SCHEDULER ----------
ET = pytz.timezone("US/Eastern")

def snooze() -> None:
    now = datetime.now(ET)
    if now.hour < 8:
        wake = now.replace(hour=8, minute=0, second=0, microsecond=0)
    elif now.hour >= 20:
        wake = (now + timedelta(days=1)).replace(hour=8, minute=0, second=0, microsecond=0)
    else:
        # random 51–72 minutes
        wake = now + timedelta(seconds=random.randint(3060, 4320))

    sleep_for = (wake - now).total_seconds()
    nxt = datetime.now(ET) + timedelta(seconds=sleep_for)
    print(
        f"Sleeping {int(sleep_for/60)} min — next run {nxt.strftime('%I:%M %p %Z')}"
    )
    time.sleep(sleep_for)

while True:
    if 8 <= datetime.now(ET).hour < 20:
        run_cycle()
    snooze()
