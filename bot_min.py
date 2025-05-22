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

    BAD_PHRASES = (
        "approved",
        "negotiator",
        "settlement fee",
        "fee at closing",
    )

    conn = sqlite3.connect("seen.db")
    conn.execute("CREATE TABLE IF NOT EXISTS listings (zpid TEXT PRIMARY KEY)")
    conn.commit()

    processed = 0
    for row in rows:
        zpid = str(row.get("zpid", ""))
        if conn.execute("SELECT 1 FROM listings WHERE zpid=?", (zpid,)).fetchone():
            print(f"⤷ skip {zpid}: already seen")
            continue

        listing_text = (
            row.get("homeDescription")
            or row.get("description")
            or row.get("hdpData", {}).get("homeInfo", {}).get("homeDescription", "")
            or ""
        )
        if not listing_text:
            print(f"⤷ skip {zpid}: no description")
            continue

        lt = listing_text.lower()
        if "short sale" not in lt:
            print(f"⤷ skip {zpid}: no 'short sale' phrase")
            continue
        if any(bad in lt for bad in BAD_PHRASES):
            print(f"⤷ skip {zpid}: contains disqualifier")
            continue

        agent_name = row.get("listingAgent", {}).get("name", "").strip()
        if not agent_name:
            print(f"⤷ skip {zpid}: no agent name")
            continue

        first, *rest = agent_name.split()
        last = " ".join(rest)
        state = row.get("addressState") or row.get("state", "")
        contact_prompt = (
            f"Find the mobile phone number and email for real-estate agent "
            f"{agent_name} in {state}. Respond in JSON "
            f'with keys "phone" and "email".'
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
            print(f"⤷ skip {zpid}: bad contact JSON")
            continue
        if not phone:
            print(f"⤷ skip {zpid}: no phone returned")
            continue

        existing = SHEET.get_all_records()
        if any(r.get("phone") == phone for r in existing):
            print(f"⤷ skip {zpid}: phone already in sheet")
        else:
            addr  = row.get("listing_address") or row.get("address", "")
            city  = row.get("city")            or row.get("addressCity", "")
            state = row.get("state")           or row.get("addressState", "")

            SHEET.append_row([
                first,                    # name
                last,                     # last name
                phone,
                email,
                addr,
                city,
                state,
                "", "", "", ""            # leave Column 1, Initial Text, list, response_status blank
            ])
            print(f"✔ wrote {zpid} to sheet")

        # mark as seen
        conn.execute("INSERT OR IGNORE INTO listings(zpid) VALUES(?)", (zpid,))
        conn.commit()
        processed += 1

    print(f"► processed {processed} new listings", flush=True)
    conn.close()

