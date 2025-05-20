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
OPENAI_API_KEY   = os.getenv("OPENAI_API_KEY")
SMSM_KEY         = os.getenv("SMSM_KEY")
GOOGLE_CREDENTIALS = os.getenv("GOOGLE_CREDENTIALS")
SHEET_URL        = os.getenv("SHEET_URL")

# Ensure all required secrets are present
if not OPENAI_API_KEY or not SMSM_KEY or not GOOGLE_CREDENTIALS or not 
SHEET_URL:
    raise RuntimeError(
        "Missing one of OPENAI_API_KEY, SMSM_KEY, GOOGLE_CREDENTIALS or 
SHEET_URL"
    )

# Configure OpenAI
openai.api_key = OPENAI_API_KEY

# Set up Google Sheets client
GSCOPE = ["https://spreadsheets.google.com/feeds", 
"https://www.googleapis.com/auth/drive"]
creds_dict = json.loads(GOOGLE_CREDENTIALS)
CREDS = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, 
GSCOPE)
GC    = gspread.authorize(CREDS)
SHEET = GC.open_by_url(SHEET_URL).sheet1

# SMS endpoint
SMSM_URL = "https://api.smsmobile.com/v1/messages"

def process_rows(rows):
    """
    1) Dedupe in SQLite.
    2) Filter qualifying short-sales via OpenAI.
    3) Lookup agent contact via OpenAI.
    4) Append to Google Sheet & send SMS.
    """
    conn = sqlite3.connect("seen.db")
    conn.execute("CREATE TABLE IF NOT EXISTS listings (zpid TEXT PRIMARY 
KEY)")
    conn.commit()

    for row in rows:
        zpid = str(row.get("zpid", ""))
        # Skip if already seen
        if conn.execute("SELECT 1 FROM listings WHERE zpid = ?", 
(zpid,)).fetchone():
            continue

        # 2) Filter via OpenAI
        listing_text = row.get("description", "")
        filter_prompt = (
            "Return YES if the following listing text indicates a 
qualifying short sale "
            "with none of our excluded terms; otherwise return NO.\n\n"
            + listing_text
        )
        filt_resp = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "user", "content": filter_prompt}],
        )
        decision = filt_resp.choices[0].message.content.strip().upper()
        if not decision.startswith("YES"):
            continue

        # 3) Lookup agent contact via OpenAI
        agent_name = row.get("listingAgent", {}).get("name", "")
        state      = row.get("state", "")
        contact_prompt = (
            f"Find the mobile phone number and email for real estate agent 
"
            f"{agent_name} in {state}. Respond in JSON with keys 'phone' 
and 'email'."
        )
        cont_resp = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "user", "content": contact_prompt}],
        )
        cont_text = cont_resp.choices[0].message.content.strip()
        try:
            contact = json.loads(cont_text)
        except json.JSONDecodeError:
            continue
        phone = contact.get("phone")
        email = contact.get("email", "")
        if not phone:
            continue

        # 4) Append to Google Sheet & send SMS if not already in sheet
        existing = SHEET.get_all_records()
        if not any(rec.get("phone") == phone for rec in existing):
            first   = agent_name.split()[0] if agent_name else ""
            address = row.get("address", "")

            sms_body = (
                f"Hey {first}, this is Yoni Kutler - I saw your short sale 
listing at {address} "
                "and wanted to introduce myself. I specialize in helping 
agents get faster bank "
                "approvals and ensure these deals close. I know you likely 
handle short sales yourself, "
                "but I work behind the scenes to take on lender 
negotiations so you can focus on selling. "
                "No cost to you or your client - I'm only paid by the 
buyer at closing. "
                "Would you be open to a quick call to see if this could 
help?"
            )

            requests.post(
                SMSM_URL,
                json={"to": phone, "message": sms_body},
                headers={"Authorization": f"Bearer {SMSM_KEY}"},
            )

            SHEET.append_row([zpid, agent_name, phone, email, address, 
"SMS sent"])

        # Mark as seen
        conn.execute("INSERT OR IGNORE INTO listings(zpid) VALUES (?)", 
(zpid,))
        conn.commit()

    conn.close()

