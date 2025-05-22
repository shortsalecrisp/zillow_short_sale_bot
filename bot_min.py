import os, json, sqlite3, time, re, html
import requests
from bs4 import BeautifulSoup

from openai import OpenAI
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv

load_dotenv()
client      = OpenAI()
SMSM_KEY    = os.getenv("SMSM_KEY")
SHEET_URL   = os.getenv("SHEET_URL")

GSCOPE = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]
CREDS  = ServiceAccountCredentials.from_json_keyfile_dict(
    json.loads(os.getenv("GOOGLE_CREDENTIALS_JSON")), GSCOPE
)
GC     = gspread.authorize(CREDS)
SHEET  = GC.open_by_url(SHEET_URL).sheet1

SMSM_URL = "https://api.smsmobile.com/v1/messages"

def fetch_listing_description(url: str) -> str:
    try:
        r = requests.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
        if not r.ok:
            return ""
        txt = r.text

        # 1) quick JSON-blob scrape
        m = re.search(r'"description"\s*:\s*"([^"]+)"', txt)
        if m:
            return html.unescape(m.group(1))

        # 2) fallback to DOM lookup
        soup = BeautifulSoup(txt, "html.parser")
        node = soup.select_one("[data-testid='home-description-text']")
        if node:
            return node.get_text(" ", strip=True)
    except Exception as exc:
        print(f"✖ description fetch failed for {url}: {exc}", flush=True)
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

        listing_text = (
            row.get("homeDescription")
            or row.get("description")
            or row.get("hdpData", {}).get("homeInfo", {}).get("homeDescription", "")
            or ""
        )
        if not listing_text and row.get("detailUrl"):
            listing_text = fetch_listing_description(row["detailUrl"])

        if not listing_text:
            print(f"⤷ skip {zpid}: no description", flush=True)
            continue

        lt = listing_text.lower()
        if "short sale" not in lt:
            print(f"⤷ skip {zpid}: missing 'short sale'", flush=True)
            continue
        if any(t in lt for t in ("approved", "negotiator", "settlement fee", "fee at closing")):
            print(f"⤷ skip {zpid}: has excluded term", flush=True)
            continue

        agent_name = (row.get("listingAgent") or {}).get("name", "").strip()
        if not agent_name:
            print(f"⤷ skip {zpid}: no agent name", flush=True)
            continue
        try:
            first, last = (agent_name.split(maxsplit=1) + [""])[:2]
        except ValueError:
            first, last = agent_name, ""

        state = row.get("addressState") or row.get("state", "")
        contact_prompt = (
            f"Find the mobile phone number and email for real-estate agent "
            f"{agent_name} in {state}. Respond in JSON with keys 'phone' and 'email'."
        )
        resp = client.chat.completions.create(
            model="gpt-3.5-turbo-0125",
            messages=[{"role": "user", "content": contact_prompt}],
            temperature=0.2,
        )
        try:
            cdata = json.loads(resp.choices[0].message.content)
            phone = cdata.get("phone")
            email = cdata.get("email", "")
        except json.JSONDecodeError:
            print(f"⤷ skip {zpid}: contact JSON parse fail", flush=True)
            continue
        if not phone:
            print(f"⤷ skip {zpid}: no phone found", flush=True)
            continue

        all_rows = SHEET.get_all_records(expected_headers=["first", "last", "phone", "email", "listing_address", "city", "state"])
        if any(r.get("phone") == phone for r in all_rows):
            print(f"⤷ skip {zpid}: phone already in sheet", flush=True)
            continue

        addr   = row.get("address") or ""
        city   = row.get("addressCity") or ""
        state  = state or row.get("state", "")

        sms_body = (
            f"Hey {first}, this is Yoni Kutler—I saw your short-sale listing at {addr} "
            "and wanted to introduce myself. I specialize in faster bank approvals so these deals close. "
            "No cost to you or your client—I’m only paid by the buyer at closing. "
            "Would you be open to a quick call to see if this could help?"
        )
        requests.post(
            SMSM_URL,
            json={"to": phone, "message": sms_body},
            headers={"Authorization": f"Bearer {SMSM_KEY}"},
            timeout=10,
        )

        SHEET.append_row([first, last, phone, email, addr, city, state])
        conn.execute("INSERT OR IGNORE INTO listings(zpid) VALUES(?)", (zpid,))
        conn.commit()
        print(f"✓ added {zpid}", flush=True)

    conn.close()

