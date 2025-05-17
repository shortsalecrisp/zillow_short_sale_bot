"""
bot.py
End-to-end pipeline:

1.  Qualify each Zillow row with GPT-4o        (short-sale? no banned 
phrases?)
2.  Deduplicate with SQLite + Google Sheets    (skip if zpid or phone 
seen)
3.  Use GPT web search to find agent contact   (mobile + email)
4.  Append to Google Sheet
5.  Send SMS through SMSMobile.io
"""

from __future__ import annotations

import json
import os
import sqlite3
import time
from pathlib import Path
from typing import Dict, List

import gspread
import openai
import requests
from google.oauth2.service_account import Credentials
from oauth2client.service_account import ServiceAccountCredentials  # 
gspread 6.x legacy

# ────────────────────────────── ENV / 
Secrets ──────────────────────────────
OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]
SMSMOBILE_KEY = os.environ["SMSMOBILE_KEY"]
SMSMOBILE_FROM = os.getenv("SMSMOBILE_FROM", "15551234567")

GOOGLE_CREDS_JSON = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]
SPREADSHEET_URL = (
    
"https://docs.google.com/spreadsheets/d/12UzsoQCo4W0WB_lNl3BjKpQ_wXNhEH7xegkFRVu2M70/edit#gid=0"
)

openai.api_key = OPENAI_API_KEY

# ────────────────────────────── GPT Prompts 
────────────────────────────────
QUALIFY_PROMPT_TEMPLATE = """
Return "YES" if the listing description below clearly shows the property 
is a
SHORT SALE **and** does NOT contain ANY of these phrases 
(case-insensitive):

    cash only, auction, reo, foreclosure, trustee sale, sheriff sale,
    tax sale, bankruptcy, court approval, third-party approval

Otherwise return "NO".

Respond with exactly YES or NO.

Listing description:
--------------------
{desc}
"""

CONTACT_PROMPT_TEMPLATE = """
You are a real-estate assistant. Using public web sources (MLS, brokerage
websites, state licensing DBs, etc.) find the **mobile phone number** and
**direct email address** for the listing agent below.

Return JSON with keys "phone" and "email".  Leave a value empty if not 
found.

Agent name: {agent_name}
Agent state: {state}
Brokerage (if known): {brokerage}
"""

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

# ────────────────────────────── SQLite 
dedupe ──────────────────────────────
DB_PATH = Path("seen.db")
conn = sqlite3.connect(DB_PATH)
conn.execute(
    """
    CREATE TABLE IF NOT EXISTS listings (
        zpid  TEXT PRIMARY KEY,
        phone TEXT
    )
"""
)
conn.commit()

# ────────────────────────────── Google 
Sheets ─────────────────────────────
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
gcreds = 
Credentials.from_service_account_info(json.loads(GOOGLE_CREDS_JSON), 
scopes=SCOPES)
gc = gspread.authorize(gcreds)
sheet = gc.open_by_url(SPREADSHEET_URL).sheet1  # first tab


def phone_exists_in_sheet(phone: str) -> bool:
    if not phone:
        return False
    # Column D (4) assumed to hold phone numbers
    return phone in sheet.col_values(4)


# ────────────────────────────── GPT helper 
wrappers ───────────────────────
def gpt_yes(prompt: str) -> bool:
    """Return True if GPT answers YES."""
    resp = openai.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        max_tokens=1,
        temperature=0,
    )
    return resp.choices[0].message.content.strip().upper() == "YES"


def gpt_json(prompt: str) -> Dict[str, str]:
    resp = openai.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.3,
    )
    try:
        return json.loads(resp.choices[0].message.content)
    except Exception:
        return {"phone": "", "email": ""}


# ────────────────────────────── SMSMobile 
─────────────────────────────────
def send_sms(to_number: str, body: str) -> None:
    if not to_number:
        return
    payload = {
        "api_key": SMSMOBILE_KEY,
        "from": SMSMOBILE_FROM,
        "to": to_number,
        "text": body,
    }
    r = requests.post("https://smsmobile.io/api/v1/send", json=payload, 
timeout=15)
    r.raise_for_status()


# ────────────────────────────── Main 
entrypoint ───────────────────────────
def process_rows(rows: List[Dict]) -> None:
    """
    Called from webhook_server.py.
    `rows` is a list of Apify items (dicts) for one Zillow scrape.
    """
    for row in rows:
        zpid = str(row.get("zpid") or row.get("detailUrl", ""))
        if not zpid:
            continue

        # ---- local dedupe by zpid 
-----------------------------------------
        if conn.execute("SELECT 1 FROM listings WHERE zpid = ?", 
(zpid,)).fetchone():
            continue

        desc = row.get("description") or row.get("homeDescription") or ""
        if not desc:
            continue

        # ---- GPT qualification 
-------------------------------------------
        if not gpt_yes(QUALIFY_PROMPT_TEMPLATE.format(desc=desc[:4000])):
            continue

        # ---- contact lookup 
----------------------------------------------
        agent_name = (
            row.get("listingAgentName")
            or row.get("brokerName")
            or row.get("agentName")
            or ""
        )
        state = row.get("state") or row.get("stateCode") or ""
        brokerage = row.get("brokerageName") or ""
        contact = gpt_json(
            CONTACT_PROMPT_TEMPLATE.format(
                agent_name=agent_name, state=state, brokerage=brokerage
            )
        )
        phone = contact.get("phone", "").strip()
        email = contact.get("email", "").strip()

        # ---- dedupe by phone in Google Sheet 
------------------------------
        if phone_exists_in_sheet(phone):
            continue

        # ---- append to sheet 
---------------------------------------------
        address = row.get("address") or row.get("streetAddress") or ""
        sheet.append_row(
            [
                time.strftime("%Y-%m-%d %H:%M"),
                zpid,
                agent_name,
                phone,
                email,
                address,
                row.get("detailUrl", ""),
            ],
            value_input_option="USER_ENTERED",
        )

        # ---- send SMS 
-----------------------------------------------------
        first = agent_name.split()[0] if agent_name else ""
        sms_body = SMS_TEMPLATE.format(first=first, address=address)
        send_sms(phone, sms_body)

        # ---- mark as seen 
-------------------------------------------------
        conn.execute("INSERT OR IGNORE INTO listings (zpid, phone) VALUES 
(?, ?)", (zpid, phone))
        conn.commit()

