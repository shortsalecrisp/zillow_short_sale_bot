import os, re, json, time, sqlite3, logging, requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from openai import OpenAI

load_dotenv()
OPENAI_MODEL = "gpt-3.5-turbo-0125"
GOOGLE_API_KEY = os.getenv("GOOGLE_SEARCH_API_KEY")
GOOGLE_CSE_ID = os.getenv("GOOGLE_CSE_ID")
APIFY_TOKEN = os.getenv("APIFY_TOKEN", "")
SHEET_URL = os.getenv("SHEET_URL")

client = OpenAI()
UA = "Mozilla/5.0 (compatible; ShortSaleBot/1.0)"

GSCOPE = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
CREDS = ServiceAccountCredentials.from_json_keyfile_dict(
    json.loads(os.getenv("GOOGLE_CREDENTIALS_JSON")), GSCOPE
)
SHEET = gspread.authorize(CREDS).open_by_url(SHEET_URL).sheet1

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("bot_min")

PHONE_RE = re.compile(r"\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}")
EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
STRIP_TRAIL = re.compile(r"\b(TREC|DRE|Lic\.?|License)\b.*$", re.I)


def fetch_page(url: str) -> str:
    try:
        return requests.get(url, timeout=10, headers={"User-Agent": UA}).text
    except Exception:
        return ""


def zillow_description(url: str) -> str:
    html = fetch_page(url)
    soup = BeautifulSoup(html, "html.parser")
    for s in soup.find_all("script", type="application/json"):
        m = re.search(r'"(?:homeDescription|descriptionPlainText)"\s*:\s*"([^"]+)"', s.string or "")
        if m:
            return bytes(m.group(1), "utf-8").decode("unicode_escape")
    return soup.get_text(" ", strip=True)


def zillow_agent(url: str) -> str:
    html = fetch_page(url)
    m = re.search(r'"listingProvider".+?"name"\s*:\s*"([^"]+)"', html)
    if m:
        return m.group(1)
    m = re.search(r"Listed by:\s*([A-Za-z][A-Za-z\s.\'-]+)", html)
    return m.group(1) if m else ""


def google_lookup(agent: str, state: str, broker: str) -> tuple[str, str]:
    if not GOOGLE_API_KEY or not GOOGLE_CSE_ID:
        return "", ""
    queries = [f'"{agent}" {state} phone email', f'"{agent}" "{broker}" phone email' if broker else ""]
    for q in queries:
        if not q:
            continue
        params = {"key": GOOGLE_API_KEY, "cx": GOOGLE_CSE_ID, "q": q, "num": 10}
        for it in requests.get("https://www.googleapis.com/customsearch/v1", params=params, timeout=10).json().get("items", []):
            html = fetch_page(it.get("link", ""))
            phone = PHONE_RE.search(html)
            email = EMAIL_RE.search(html)
            if phone or email:
                return phone.group() if phone else "", email.group() if email else ""
    if APIFY_TOKEN:
        try:
            resp = requests.post(
                "https://api.apify.com/v2/acts/drobnikj~realtor-agent-scraper/run-sync-get-dataset-items",
                params={"token": APIFY_TOKEN},
                json={"search": agent, "state": state},
                timeout=15,
            ).json()
            if isinstance(resp, list) and resp:
                rec = resp[0]
                return rec.get("mobilePhone") or rec.get("officePhone") or "", rec.get("email") or ""
        except Exception:
            pass
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

        detail_url = row.get("detailUrl") or row.get("url") or row.get("hdpData.homeInfo.detailUrl") or ""
        listing_text = (
            row.get("homeDescription")
            or row.get("description")
            or row.get("hdpData.homeInfo.homeDescription")
            or row.get("hdpData.homeInfo.description")
            or zillow_description(detail_url)
        )
        if not listing_text:
            listing_text = " ".join(str(v) for v in row.values() if isinstance(v, str))

        prompt = (
            "Return YES if the following text contains the phrase 'short sale' "
            "(case-insensitive) and does NOT contain any of: approved, negotiator, "
            "settlement fee, fee at closing. Otherwise return NO.\n\n"
            f"{listing_text}"
        )
        if not client.chat.completions.create(
            model=OPENAI_MODEL, messages=[{"role": "user", "content": prompt}], temperature=0.0
        ).choices[0].message.content.strip().upper().startswith("YES"):
            continue

        agent = (
            row.get("listingProvider", {}).get("agents", [{}])[0].get("name")
            or row.get("listingAgentName")
            or row.get("listingAgent", {}).get("name")
            or row.get("agentName")
            or zillow_agent(detail_url)
        ).strip()
        if not agent:
            m = re.search(r"Listed by:\s*([A-Za-z][A-Za-z\s.\'-]+)", listing_text)
            agent = m.group(1).strip() if m else ""
        agent = STRIP_TRAIL.sub("", agent).strip()
        if not agent:
            logger.warning("skip %s – no agent name", zpid)
            continue

        state = row.get("addressState") or row.get("state", "")
        broker = row.get("brokerName", "")
        phone, email = google_lookup(agent, state, broker)

        parts = agent.split()
        first, last = parts[0], " ".join(parts[1:]) if len(parts) > 1 else ""
        address = row.get("address") or row.get("addressStreet") or ""
        city = row.get("addressCity") or ""
        st = row.get("addressState") or row.get("state") or ""
        SHEET.append_row([first, last, phone, email, address, city, st, "", "", ""])

        conn.execute("INSERT OR IGNORE INTO listings(zpid) VALUES(?)", (zpid,))
        conn.commit()
    conn.close()

