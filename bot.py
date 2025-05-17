import os
import json
import time
import sqlite3
import requests
import openai
import gspread
from google.oauth2.service_account import Credentials
from oauth2client.service_account import ServiceAccountCredentials  # 

# ───────── CONFIG ─────────

# These must be set in your Render environment
OPENAI_API_KEY        = os.getenv("OPENAI_API_KEY")
SMSMOBILE_API_KEY     = os.getenv("SMSMOBILE_API_KEY")
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON")

# Your sheet ID from:
# 
https://docs.google.com/spreadsheets/d/12UzsoQCo4W0WB_lNl3BjKpQ_wXNhEH7xegkFRVu2M70/edit
GOOGLE_SHEET_ID       = "12UzsoQCo4W0WB_lNl3BjKpQ_wXNhEH7xegkFRVu2M70"

openai.api_key = OPENAI_API_KEY

# ───────── SQLITE DEDUPE ─────────

conn = sqlite3.connect("seen.db")
conn.execute("CREATE TABLE IF NOT EXISTS listings (zpid TEXT PRIMARY 
KEY)")
conn.commit()

def has_seen(zpid: str) -> bool:
    cur = conn.execute("SELECT 1 FROM listings WHERE zpid = ?", (zpid,))
    return cur.fetchone() is not None

def mark_seen(zpid: str):
    conn.execute("INSERT OR IGNORE INTO listings(zpid) VALUES (?)", 
(zpid,))
    conn.commit()

# ───────── GOOGLE SHEETS ─────────

# scopes needed for Sheets & Drive
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
creds_dict = json.loads(GOOGLE_CREDENTIALS_JSON)
creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
gc = gspread.authorize(creds)
sheet = gc.open_by_key(GOOGLE_SHEET_ID).sheet1

# ───────── AI LOGIC ─────────

def qualifies_listing(listing_text: str) -> bool:
    prompt = (
        "You are a real estate expert. Return YES if the following listing 
text "
        "describes a short sale property; otherwise return NO.\n\n"
        f"{listing_text}\n\nAnswer ONLY YES or NO."
    )
    resp = openai.ChatCompletion.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        temperature=0,
    )
    answer = resp.choices[0].message.content.strip().upper()
    return answer == "YES"

def find_contact(agent_name: str, state: str) -> dict:
    prompt = (
        f"Provide the email and mobile phone number for real estate agent 
"
        f"{agent_name} in {state}. Return ONLY JSON with keys "
        f"'email' and 'phone'. If you can’t find it, use null values."
    )
    resp = openai.ChatCompletion.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        temperature=0,
    )
    try:
        return json.loads(resp.choices[0].message.content)
    except json.JSONDecodeError:
        return {"email": None, "phone": None}

# ───────── SMS SENDING ─────────

SMS_TEMPLATE = (
    "Hey {first}, this is Yoni Kutler—I saw your short sale listing at 
{address} "
    "and wanted to introduce myself. I specialize in helping agents get 
faster "
    "bank approvals and ensure these deals close. I know you likely handle 
short "
    "sales yourself, but I work behind the scenes to take on lender 
negotiations "
    "so you can focus on selling. No cost to you or your client—I’m only 
paid by "
    "the buyer at closing. Would you be open to a quick call to see if 
this could help?"
)

def send_sms(phone: str, first: str, address: str):
    body = SMS_TEMPLATE.format(first=first, address=address)
    resp = requests.post(
        "https://api.smsmobile.com/send",  # <-- replace if your endpoint 
differs
        headers={
            "Authorization": f"Bearer {SMSMOBILE_API_KEY}",
            "Content-Type": "application/json",
        },
        json={"to": phone, "message": body},
    )
    resp.raise_for_status()

# ───────── MAIN PROCESS ─────────

def process_rows(rows: list):
    """
    rows: list of dicts from Apify dataset, each with at least:
      - zpid
      - description
      - agentName
      - state
      - address
    """
    for row in rows:
        zpid        = row.get("zpid")
        if not zpid or has_seen(zpid):
            continue

        desc        = row.get("description", "")
        if not qualifies_listing(desc):
            mark_seen(zpid)
            continue

        agent_name  = row.get("agentName", "")
        state       = row.get("state", "")
        contact     = find_contact(agent_name, state)
        phone       = contact.get("phone")
        email       = contact.get("email")

        if phone:
            first   = agent_name.split()[0]
            address = row.get("address", "")
            send_sms(phone, first, address)

        # Append to Google Sheet:
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
        sheet.append_row([
            zpid, row.get("address"), agent_name,
            phone, email, timestamp
        ])

        mark_seen(zpid)
        time.sleep(1)  # throttle so we don’t hit rate limits

