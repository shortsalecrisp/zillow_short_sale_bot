import re, os, json, time, sqlite3, requests
from bs4 import BeautifulSoup

from openai import OpenAI
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv

load_dotenv()                       # reads .env in Render
client = OpenAI()                   # uses OPENAI_API_KEY from env

SMSM_KEY  = os.getenv("SMSM_KEY")   # kept for future SMS logic
SHEET_URL = os.getenv("SHEET_URL")

GSCOPE = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]
creds_dict = json.loads(os.getenv("GOOGLE_CREDENTIALS_JSON"))
CREDS  = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, GSCOPE)
GC     = gspread.authorize(CREDS)
SHEET  = GC.open_by_url(SHEET_URL).sheet1

def fetch_zillow_description(detail_url: str) -> str:
    try:
        resp = requests.get(
            detail_url,
            timeout=10,
            headers={"User-Agent": "Mozilla/5.0 (compatible; ShortSaleBot/1.0)"},
        )
        resp.raise_for_status()
    except Exception:
        return ""

    soup = BeautifulSoup(resp.text, "html.parser")

    # 1) JSON blobs embedded in <script type="application/json">
    for script in soup.find_all("script", type="application/json"):
        txt = script.string or ""
        m = re.search(
            r'"(?:homeDescription|descriptionPlainText)"\s*:\s*"([^"]+)"', txt
        )
        if m:
            return bytes(m.group(1), "utf-8").decode("unicode_escape")

    # 2) visible “What’s special …” section
    trig = soup.find(string=re.compile(r"(?i)what.?s.+special"))
    sec  = trig.find_parent("section") if trig else None
    if sec:
        return " ".join(sec.stripped_strings)

    return ""

def process_rows(rows):
    print(f"► fetched {len(rows)} rows at {time.strftime('%X')}", flush=True)

    conn = sqlite3.connect("seen.db")
    conn.execute("CREATE TABLE IF NOT EXISTS listings (zpid TEXT PRIMARY KEY)")
    conn.commit()

    for row in rows:
        zpid = str(row.get("zpid", ""))
        if conn.execute("SELECT 1 FROM listings WHERE zpid=?", (zpid,)).fetchone():
            continue

        # description from API fields
        listing_text = (
            row.get("homeDescription")
            or row.get("description")
            or row.get("hdpData", {}).get("homeInfo", {}).get("homeDescription")
            or ""
        )

        # fallback: scrape HTML
        if not listing_text:
            detail_url = row.get("detailUrl") or row.get("url") or ""
            listing_text = fetch_zillow_description(detail_url) if detail_url else ""

        if not listing_text:
            print(f"⤷ skip {zpid}: still no description")
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

        agent_name = row.get("listingAgent", {}).get("name", "")
        state      = row.get("addressState") or row.get("state", "")
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
            continue  # must have a phone number

        first, *rest = agent_name.split()
        last    = " ".join(rest)
        address = row.get("address") or row.get("addressStreet") or ""
        city    = row.get("addressCity") or ""
        st      = row.get("addressState") or row.get("state") or ""

        SHEET.append_row([first, last, phone, email, address, city, st, "", "", ""])

        # mark as processed
        conn.execute("INSERT OR IGNORE INTO listings(zpid) VALUES(?)", (zpid,))
        conn.commit()

    conn.close()

