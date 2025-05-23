import re, os, json, time, sqlite3, requests, logging
from bs4 import BeautifulSoup

from openai import OpenAI
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv

load_dotenv()
client = OpenAI()

SMSM_KEY  = os.getenv("SMSM_KEY")
SHEET_URL = os.getenv("SHEET_URL")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

GSCOPE = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]
creds_dict = json.loads(os.getenv("GOOGLE_CREDENTIALS_JSON"))
CREDS  = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, GSCOPE)
GC     = gspread.authorize(CREDS)
SHEET  = GC.open_by_url(SHEET_URL).sheet1

UA = "Mozilla/5.0 (compatible; ShortSaleBot/1.0)"

def fetch_zillow_description(detail_url: str) -> str:
    try:
        resp = requests.get(detail_url, timeout=10, headers={"User-Agent": UA})
        resp.raise_for_status()
    except Exception:
        return ""

    soup = BeautifulSoup(resp.text, "html.parser")

    for script in soup.find_all("script", type="application/json"):
        txt = script.string or ""
        m = re.search(r'"(?:homeDescription|descriptionPlainText)"\s*:\s*"([^"]+)"', txt)
        if m:
            return bytes(m.group(1), "utf-8").decode("unicode_escape")

    trig = soup.find(string=re.compile(r"(?i)what.?s.+special"))
    sec  = trig.find_parent("section") if trig else None
    if sec:
        return " ".join(sec.stripped_strings)

    return ""

def fetch_zillow_agent(detail_url: str) -> str:
    try:
        resp = requests.get(detail_url, timeout=10, headers={"User-Agent": UA})
        resp.raise_for_status()
    except Exception:
        return ""

    soup = BeautifulSoup(resp.text, "html.parser")

    for script in soup.find_all("script", type="application/json"):
        txt = script.string or ""
        m = re.search(r'"listingProvider".+?"name"\s*:\s*"([^"]+)"', txt)
        if m:
            return m.group(1)

    label = soup.find(string=re.compile(r"Listing agent", re.I))
    if label:
        name_el = label.find_next("a") or label.find_next("span")
        if name_el:
            return name_el.get_text(strip=True)

    listed_match = re.search(r"Listed by:\s*([A-Za-z][A-Za-z\s.\'-]+)", soup.get_text(" ", strip=True))
    if listed_match:
        return listed_match.group(1).strip()

    return ""

def parse_agent_from_text(text: str) -> str:
    patterns = [
        r"Listed by:\s*([A-Za-z][A-Za-z\s.\'-]+)",
        r"Listing agent[:\s]*([A-Za-z][A-Za-z\s.\'-]+)",
    ]
    for pat in patterns:
        m = re.search(pat, text, re.I)
        if m:
            return m.group(1).strip()
    return ""

def extract_agent_name(row) -> str:
    return (
        row.get("listingProvider", {}).get("agents", [{}])[0].get("name") or
        row.get("listingAgentName") or
        row.get("listingAgent", {}).get("name") or
        row.get("agentName") or
        ""
    ).strip()

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
            or row.get("hdpData", {}).get("homeInfo", {}).get("homeDescription")
            or ""
        )

        detail_url = row.get("detailUrl") or row.get("url") or ""

        if not listing_text and detail_url:
            listing_text = fetch_zillow_description(detail_url)

        if not listing_text:
            logger.warning("skip %s – no description", zpid)
            continue

        filter_prompt = (
            "Return YES if the following text contains the phrase 'short sale' "
            "(case-insensitive) and does NOT contain any of: approved, negotiator, "
            "settlement fee, fee at closing. Otherwise return NO.\n\n"
            f"{listing_text}"
        )
        resp = client.chat.completions.create(
            model="gpt-3.5-turbo-0125",
            messages=[{"role": "user", "content": filter_prompt}],
            temperature=0.0,
        )
        if not resp.choices[0].message.content.strip().upper().startswith("YES"):
            continue

        agent_name = extract_agent_name(row)
        if not agent_name:
            agent_name = parse_agent_from_text(listing_text)
        if not agent_name and detail_url:
            agent_name = fetch_zillow_agent(detail_url)
        agent_name = agent_name.strip()

        if not agent_name:
            logger.warning("skip %s – no agent name", zpid)
            continue

        state = row.get("addressState") or row.get("state", "")
        contact_prompt = (
            f"Find the MOBILE phone number and email for real-estate agent "
            f"{agent_name} in {state}. Respond in JSON with keys 'phone' and 'email'."
        )
        cont_resp = client.chat.completions.create(
            model="gpt-3.5-turbo-0125",
            messages=[{"role": "user", "content": contact_prompt}],
            temperature=0.2,
        )
        try:
            contact = json.loads(cont_resp.choices[0].message.content)
            phone   = contact.get("phone", "").strip()
            email   = contact.get("email", "").strip()
        except Exception:
            phone = email = ""

        if not phone:
            logger.warning("skip %s – no phone returned for %s", zpid, agent_name)
            continue

        parts = agent_name.split()
        first = parts[0]
        last  = " ".join(parts[1:]) if len(parts) > 1 else ""

        address = row.get("address") or row.get("addressStreet") or ""
        city    = row.get("addressCity") or ""
        st      = row.get("addressState") or row.get("state") or ""

        SHEET.append_row([first, last, phone, email, address, city, st, "", "", ""])

        conn.execute("INSERT OR IGNORE INTO listings(zpid) VALUES(?)", (zpid,))
        conn.commit()

    conn.close()

