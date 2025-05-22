import os, json, sqlite3, requests, time
from openai import OpenAI
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv
import re
from bs4 import BeautifulSoup

load_dotenv()
client = OpenAI()
SMSM_KEY = os.getenv("SMSM_KEY")
SHEET_URL = os.getenv("SHEET_URL")

GSCOPE = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]
creds_dict = json.loads(os.getenv("GOOGLE_CREDENTIALS_JSON"))
CREDS = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, GSCOPE)
GC = gspread.authorize(CREDS)
SHEET = GC.open_by_url(SHEET_URL).sheet1

SMSM_URL = "https://api.smsmobile.com/v1/messages"

def process_rows(rows):
    print(f"► fetched {len(rows)} rows at {time.strftime('%X')}", flush=True)

    conn = sqlite3.connect("seen.db")
    conn.execute("CREATE TABLE IF NOT EXISTS listings (zpid TEXT PRIMARY KEY)")
    conn.commit()

    processed = 0
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
            try:
                html = requests.get(
                    row["detailUrl"],
                    timeout=10,
                    headers={"User-Agent": "Mozilla/5.0 (+https://example.com/bot)"}
                ).text
                soup   = BeautifulSoup(html, "html.parser")
                marker = soup.find(string=re.compile(r"what['’]s\s+(special|happening)", re.I))
                if marker:
                    listing_text = marker.find_next().get_text(" ", strip=True)
            except Exception:
                listing_text = ""

        if not listing_text.strip():
            continue  # nothing to evaluate

        filter_prompt = (
            "Return YES if the following listing text indicates a qualifying short sale "
            "with none of our excluded terms; otherwise return NO.\n\n"
            f"{listing_text}"
        )
        filt_resp = client.chat.completions.create(
            model="gpt-3.5-turbo-0125",
            messages=[{"role": "user", "content": filter_prompt}],
            temperature=0.2,
        )
        if not filt_resp.choices[0].message.content.strip().upper().startswith("YES"):
            continue

        agent_name = row.get("listingAgent", {}).get("name", "").strip()
        if not agent_name:
            continue  # skip rows without an agent name

        first, *rest = agent_name.split()
        last = " ".join(rest)
        state = row.get("addressState") or row.get("state", "")
        contact_prompt = (
            f"Find the mobile phone number and email for real estate agent "
            f"{agent_name} in {state}. Respond in JSON with keys 'phone' and 'email'."
        )
        cont_resp = client.chat.completions.create(
            model="gpt-3.5-turbo-0125",
            messages=[{"role": "user", "content": contact_prompt}],
            temperature=0.2,
        )
        try:
            contact = json.loads(cont_resp.choices[0].message.content)
            phone   = contact.get("phone")
            email   = contact.get("email", "")
        except Exception:
            continue
        if not phone:
            continue

        if not any(r.get("phone") == phone for r in SHEET.get_all_records()):
            address = row.get("listing_address") or row.get("address", "")
            city    = row.get("city")            or row.get("addressCity", "")
            state   = row.get("state")           or row.get("addressState", "")

            SHEET.append_row([
                first,              # name
                last,               # last name
                phone,
                email,
                address,
                city,
                state,
                "", "", "", ""      # Column 1, Initial Text, list, response_status
            ])

            sms_body = (
                f"Hey {first}, this is Yoni Kutler—I saw your short sale listing at "
                f"{address} and wanted to introduce myself. I specialize in helping "
                f"agents get faster bank approvals and ensure these deals close. "
                "No cost to you or your client—I’m only paid by the buyer at closing. "
                "Would you be open to a quick call to see if this could help?"
            )
            requests.post(
                SMSM_URL,
                json={"to": phone, "message": sms_body},
                headers={"Authorization": f"Bearer {SMSM_KEY}"},
                timeout=10,
            )

        conn.execute("INSERT OR IGNORE INTO listings(zpid) VALUES(?)", (zpid,))
        conn.commit()
        processed += 1

    print(f"► processed {processed} new listings", flush=True)
    conn.close()

