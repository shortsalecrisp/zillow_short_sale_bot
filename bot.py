#!/usr/bin/env python3
"""
Zillow Short-Sale Scraper + SMS Bot
Runs 8 AM – 8 PM Eastern at random 51–72-minute intervals.
"""

import json, re, sqlite3, time, random, requests, pytz
from datetime import datetime, timedelta
from fake_useragent import UserAgent
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import openai
from smsmobileapi import SMSSender

# ---------- 0. CONFIG ----------
with open("config.json") as f:
    CFG = json.load(f)

openai.api_key = CFG["openai_api_key"]
ua = UserAgent()

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

# ---------- 2. CHATGPT LOOKUP ----------
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

# ---------- 3. GOOGLE SHEETS ----------
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

# ---------- 4. SMS ----------
sms = SMSSender(api_key=CFG["smsmobile_api_key"])

# ---------- 5. LOCAL SQLITE (dedupe by zpid) ----------
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

# ---------- 6. MAIN CYCLE ----------
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

        # Mark first so we never re-process this zpid
        mark_sent(zpid)

        phone, email = get_contact_info(name, address)
        if not phone:
            print("No mobile for", name, "|", address)
            continue

        parts = name.split()
        first = parts[0]
        last  = " ".join(parts[1:]) if len(parts) > 1 else ""

        street = city = state = ""
        addr_parts = [p.strip() for p in address.split(",")]
        if len(addr_parts) >= 1: street = addr_parts[0]
        if len(addr_parts) >= 2: city   = addr_parts[1]
        if len(addr_parts) >= 3: state  = addr_parts[2].split()[0]

        add_row(first, last, phone, email, street, city, state)

        sms_text = CFG["sms_template"].format(first=first, address=street)
        try:
            sms.send_sms(phone, sms_text)
            print("SMS sent to", first, phone)
        except Exception as e:
            print("SMS error:", e)

# ---------- 7. SCHEDULER ----------
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
