import os, re, json, time, sqlite3, logging, requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from openai import OpenAI

load_dotenv()

OPENAI_MODEL          = "gpt-3.5-turbo-0125"
GOOGLE_API_KEY        = os.getenv("GOOGLE_SEARCH_API_KEY")
GOOGLE_CSE_ID         = os.getenv("GOOGLE_CSE_ID")
SHEET_URL             = os.getenv("SHEET_URL")

client = OpenAI()
UA     = "Mozilla/5.0 (compatible; ShortSaleBot/1.0)"

GSCOPE = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]
creds_dict = json.loads(os.getenv("GOOGLE_CREDENTIALS_JSON"))
CREDS  = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, GSCOPE)
GC     = gspread.authorize(CREDS)
SHEET  = GC.open_by_url(SHEET_URL).sheet1

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("bot_min")

PHONE_RE = re.compile(r"\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}")
EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
STRIP_TRAIL = re.compile(r"\b(TREC|DRE|Lic\.?|License)\b.*$", re.I)

def fetch_zillow_description(detail_url: str) -> str:
    try:
        html = requests.get(detail_url, timeout=10, headers={"User-Agent": UA}).text
    except Exception:
        return ""

    soup = BeautifulSoup(html, "html.parser")

    for s in soup.find_all("script", type="application/json"):
        m = re.search(r'"(?:homeDescription|descriptionPlainText)"\s*:\s*"([^"]+)"', s.string or "")
        if m:
            return bytes(m.group(1), "utf-8").decode("unicode_escape")

    return soup.get_text(" ", strip=True)

def fetch_zillow_agent(detail_url: str) -> str:
    try:
        html = requests.get(detail_url, timeout=10, headers={"User-Agent": UA}).text
    except Exception:
        return ""
    m = re.search(r'"listingProvider".+?"name"\s*:\s*"([^"]+)"', html)
    if m:
        return m.group(1)
    m = re.search(r"Listed by:\s*([A-Za-z][A-Za-z\s.\'-]+)", html)
    return m.group(1) if m else ""

def google_contact_lookup(agent: str, state: str) -> tuple[str, str]:
    if not GOOGLE_API_KEY or not GOOGLE_CSE_ID:
        return "", ""
    params = {
        "key": GOOGLE_API_KEY,
        "cx": GOOGLE_CSE_ID,
        "q": f'"{agent}" realtor {state} phone email',
        "num": 5,
    }
    try:
        res = requests.get("https://www.googleapis.com/customsearch/v1", params=params, timeout=10).json()
    except Exception:
        return "", ""
    for item in res.get("items", []):
        snippet = item.get("snippet", "")
        phone_match = PHONE_RE.search(snippet)
        email_match = EMAIL_RE.search(snippet)
        phone = phone_match.group() if phone_match else ""
        email = email_match.group() if email_match else ""
        if phone or email:
            return phone, email
        try:
            page = requests.get(item["link"], timeout=8, headers={"User-Agent": UA}).text
            phone_match = PHONE_RE.search(page)
            email_match = EMAIL_RE.search(page)
            phone = phone_match.group() if phone_match else ""
            email = email_match.group() if email_match else ""
            if phone or email:
                return phone, email
        except Exception:
            continue
    return "", ""

def process_rows(rows):
    logger.info("fetched %d rows at %s", len(rows), time.strftime("%X"))
    conn = sqlite3.connect("seen.db")
    conn.execute("CREATE TABLE IF NOT EXISTS listings (zpid TEXT PRIMARY KEY)")
    conn.commit()

    for row in rows:
        zpid = str(row.get("zpid", ""))
        if conn.execute("SELECT 1 FROM listings WHERE zpid=?", (zpid,)).fetchone():
            continue

        listing_text = (
            row.get("homeDescription")
            or row.get("description")
            or row.get("hdpData.homeInfo.homeDescription")
            or row.get("hdpData.homeInfo.description")
            or ""
        )
        detail_url = (
            row.get("detailUrl")
            or row.get("url")
            or row.get("hdpData.homeInfo.detailUrl")
            or ""
        )
        if not listing_text and detail_url:
            listing_text = fetch_zillow_description(detail_url)
        if not listing_text:
            logger.warning("skip %s – no description", zpid)
            continue

        prompt = (
            "Return YES if the following text contains the phrase 'short sale' "
            "(case-insensitive) and does NOT contain any of: approved, negotiator, "
            "settlement fee, fee at closing. Otherwise return NO.\n\n"
            f"{listing_text}"
        )
        resp = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.0,
        )
        if not resp.choices[0].message.content.strip().upper().startswith("YES"):
            continue

        agent = (
            row.get("listingProvider", {}).get("agents", [{}])[0].get("name") or
            row.get("listingAgentName") or
            row.get("listingAgent", {}).get("name") or
            row.get("agentName") or
            ""
        ).strip()
        if not agent and detail_url:
            agent = fetch_zillow_agent(detail_url)
        if not agent:
            m = re.search(r"Listed by:\s*([A-Za-z][A-Za-z\s.\'-]+)", listing_text)
            agent = m.group(1) if m else ""
        agent = STRIP_TRAIL.sub("", agent).strip()
        if not agent:
            logger.warning("skip %s – no agent name", zpid)
            continue

        state = row.get("addressState") or row.get("state", "")
        phone, email = google_contact_lookup(agent, state)
        if not phone and not email:
            logger.warning("skip %s – contact not found for %s", zpid, agent)
            continue

        parts = agent.split()
        first = parts[0]
        last  = " ".join(parts[1:]) if len(parts) > 1 else ""
        address = row.get("address") or row.get("addressStreet") or ""
        city    = row.get("addressCity") or ""
        st      = row.get("addressState") or row.get("state") or ""

        SHEET.append_row([first, last, phone, email, address, city, st, "", "", ""])
        conn.execute("INSERT OR IGNORE INTO listings(zpid) VALUES(?)", (zpid,))
        conn.commit()

    conn.close()

