import os
import json
import sqlite3
import requests
from openai import OpenAI
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv

# ── 1  Load environment variables 
──────────────────────────────────────────
load_dotenv()

# OpenAI: the v1 client reads OPENAI_API_KEY from the env automatically
client = OpenAI()

SMSM_KEY  = os.getenv("SMSM_KEY")
SHEET_URL = os.getenv("SHEET_URL")

# ── 2  Google Sheets setup 
────────────────────────────────────────────────
GSCOPE = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]

cred_json = os.getenv("GOOGLE_CREDENTIALS_JSON")
if not cred_json:
    raise RuntimeError("GOOGLE_CREDENTIALS_JSON env var is missing")

try:
    creds_dict = json.loads(cred_json)
except json.JSONDecodeError as exc:
    raise RuntimeError("GOOGLE_CREDENTIALS_JSON is not valid JSON") from exc

CREDS = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, GSCOPE)
GC    = gspread.authorize(CREDS)
SHEET = GC.open_by_url(SHEET_URL).sheet1

# ── 3  SMS endpoint 
───────────────────────────────────────────────────────
SMSM_URL = "https://api.smsmobile.com/v1/messages"

# ── 4  Core pipeline 
──────────────────────────────────────────────────────
def process_rows(rows: list[dict]) -> None:
    """
    1) dedupe
    2) filter      – qualify short-sale listings via GPT
    3) lookup      – fetch agent contact info via GPT
    4) send/record – SMS + append to Google Sheet
    """
    conn = sqlite3.connect("seen.db")
    conn.execute("CREATE TABLE IF NOT EXISTS listings (zpid TEXT PRIMARY KEY)")
    conn.commit()

    for row in rows:
        zpid = str(row.get("zpid", ""))
        # ── 4a  Skip if we've already handled this listing ────────────────
        if conn.execute("SELECT 1 FROM listings WHERE zpid=?", (zpid,)).fetchone():
            continue

        # ── 4b  Filter: is this a qualifying short sale? ──────────────────
        listing_text  = row.get("description", "")
        filter_prompt = (
            "Return YES if the following listing text indicates a *qualifying short sale* "
            "with none of our excluded terms; otherwise return NO.\n\n"
            f"{listing_text}"
        )

        filt_resp = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "user", "content": filter_prompt}],
            temperature=0.2,
        )
        decision = filt_resp.choices[0].message.content.strip().upper()
        if not decision.startswith("YES"):
            continue

        # ── 4c  Lookup: phone & email for the agent ───────────────────────
        agent_name = row.get("listingAgent", {}).get("name", "")
        state      = row.get("state", "")
        contact_prompt = (
            f"Find the *mobile phone number* and *email* for real-estate agent "
            f"{agent_name} in {state}. Respond in JSON with keys 'phone' and 'email'."
        )

        cont_resp = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "user", "content": contact_prompt}],
            temperature=0,
        )
        try:
            contact = json.loads(cont_resp.choices[0].message.content)
            phone   = contact.get("phone")
            email   = contact.get("email", "")
        except (json.JSONDecodeError, AttributeError):
            continue
        if not phone:
            continue  # need a phone number to proceed

        # ── 4d  Send SMS + record to Sheet (skip if phone already present) ─
        if not any(r.get("phone") == phone for r in SHEET.get_all_records()):
            first    = agent_name.split()[0] if agent_name else ""
            address  = row.get("address", "")
            sms_body = (
                "Hey {first}, this is Yoni Kutler—I saw your short sale listing at {address} "
                "and wanted to introduce myself. I specialize in helping agents get faster bank "
                "approvals and ensure these deals close. I work behind the scenes to handle lender "
                "negotiations so you can focus on selling. No cost to you or your client—I’m only "
                "paid by the buyer at closing. Would you be open to a quick call to see if this "
                "could help?"
            ).format(first=first, address=address)

            requests.post(
                SMSM_URL,
                json={"to": phone, "message": sms_body},
                headers={"Authorization": f"Bearer {SMSM_KEY}"},
                timeout=10,
            )
            SHEET.append_row([zpid, agent_name, phone, email, address, "SMS sent"])

        # ── 4e  Persist zpid so we don't process it again ──────────────────
        conn.execute("INSERT OR IGNORE INTO listings(zpid) VALUES(?)", (zpid,))
        conn.commit()

    conn.close()

