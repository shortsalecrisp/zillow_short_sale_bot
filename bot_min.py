import os
import json
import sqlite3
import requests
import openai
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
SMSM_KEY       = os.getenv("SMSM_KEY")
SHEET_URL      = os.getenv("SHEET_URL")

if not OPENAI_API_KEY or not SMSM_KEY or not SHEET_URL:
    raise RuntimeError("Missing one of OPENAI_API_KEY, SMSM_KEY or SHEET_URL")

# OpenAI setup
openai.api_key = OPENAI_API_KEY

# Google Sheets setup
GSCOPE = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]
CREDS = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", GSCOPE)
GC    = gspread.authorize(CREDS)
SHEET = GC.open_by_url(SHEET_URL).sheet1

# SMS endpoint
SMSM_URL = "https://api.smsmobile.com/v1/messages"

def process_rows(rows):
    """
    1) dedupe
    2) filter
    3) lookup
    4) send & record
    """
    conn = sqlite3.connect("seen.db")
    # <— this entire string must be on one line below:
    conn.execute("CREATE TABLE IF NOT EXISTS listings (zpid TEXT PRIMARY KEY)")
    conn.commit()

    # ─── Block 2: per‐row dedupe check ────────────────────────────────────
    for row in rows:
        zpid = str(row.get("zpid", ""))
        # skip any zpid we’ve already seen
        if conn.execute(
            "SELECT 1 FROM listings WHERE zpid=?", (zpid,)
        ).fetchone():
            continue

        # ─── Block 3: filter qualifying short-sales via OpenAI ───────────────
        listing_text = row.get("description", "")
        filter_prompt = (
            "Return YES if the following listing text indicates a qualifying short sale "
            "with none of our excluded terms; otherwise return NO.\n\n"
            f"{listing_text}"
        )
        filt_resp = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "user", "content": filter_prompt}],
        )
        decision = filt_resp.choices[0].message.content.strip().upper()
        if not decision.startswith("YES"):
            continue

        # ─── Block 4: lookup agent contact via OpenAI ───────────────
        agent_name = row.get("listingAgent", {}).get("name", "")
        state      = row.get("state", "")
        contact_prompt = (
            f"Find the mobile phone number and email for real estate agent "
            f"{agent_name} in {state}. Respond in JSON with keys 'phone' and 'email'."
        )
        cont_resp = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "user", "content": contact_prompt}],
        )
        cont_text = cont_resp.choices[0].message.content.strip()
        try:
            contact = json.loads(cont_text)
            phone   = contact.get("phone")
            email   = contact.get("email", "")
        except json.JSONDecodeError:
            continue
        if not phone:
            continue

        # ─── Block 5: Append to sheet & send SMS ───────────────
        all_records = SHEET.get_all_records()
        if not any(r.get("phone") == phone for r in all_records):
            first   = agent_name.split()[0] if agent_name else ""
            address = row.get("address", "")
            sms_body = (
                "Hey {first}, this is Yoni Kutler—I saw your short sale listing at {address} "
                "and wanted to introduce myself. I specialize in helping agents get faster bank "
                "approvals and ensure these deals close. I know you likely handle short sales yourself, "
                "but I work behind the scenes to take on lender negotiations so you can focus on selling. "
                "No cost to you or your client—I’m only paid by the buyer at closing. "
                "Would you be open to a quick call to see if this could help?"
            ).format(first=first, address=address)

            requests.post(
                SMSM_URL,
                json={"to": phone, "message": sms_body},
                headers={"Authorization": f"Bearer {SMSM_KEY}"},
            )
            SHEET.append_row([zpid, agent_name, phone, email, address, "SMS sent"])

        conn.execute(
            "INSERT OR IGNORE INTO listings(zpid) VALUES(?)",
            (zpid,),
        )
        conn.commit()

    # ─── Block 6: close DB connection ───────────────
    conn.close()

