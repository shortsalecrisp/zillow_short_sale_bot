"""
bot.py
Full pipeline:
1.  Qualify each Zillow row with GPT       (is it a short-sale & passes 
filters?)
2.  Deduplicate via SQLite + Google Sheet  (skip if zpid or phone already 
seen)
3.  Look up agent contact (GPT web search) (mobile + email)
4.  Append to Google Sheet
5.  Send SMS using SMSMobile.io
"""

from __future__ import annotations

import json
import os
import sqlite3
import time
from pathlib import Path
from typing import List, Dict, Tuple

import gspread
import openai
import requests
from google.oauth2.service_account import Credentials
from oauth2client.service_account import ServiceAccountCredentials  # 
gspread v6 still expects it

# 
-----------------------------------------------------------------------------
# ENV  – all secrets come from Render environment variables
# 
-----------------------------------------------------------------------------
OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]
SMSMOBILE_KEY = os.environ["SMSMOBILE_KEY"]
SMSMOBILE_FROM = os.getenv("SMSMOBILE_FROM", "15551234567")  # default 
sender

# Google
GOOGLE_CREDS_JSON = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]
SPREADSHEET_URL = (
    
"https://docs.google.com/spreadsheets/d/12UzsoQCo4W0WB_lNl3BjKpQ_wXNhEH7xegkFRVu2M70/edit#gid=0"
)

# 
-----------------------------------------------------------------------------
# GPT Prompt templates
# 
-----------------------------------------------------------------------------
QUALIFY_PROMPT_TEMPLATE = """
Return "YES" if the following Zillow listing description clearly shows the
property is being offered as a SHORT SALE and it does NOT contain any of 
the
following disqualifying phrases (case-insensitive):

    cash only, auction, reo, foreclosure, trustee sale, sheriff sale,
    tax sale, bankruptcy, court approval, third-party approval

Otherwise return "NO".

Respond with *exactly* YES or NO (no punctuation, no extra words).

Listing description:
--------------------
{desc}
"""

CONTACT_PROMPT_TEMPLATE = """
You are a real-estate assistant. Using public web sources (MLS, brokerage
sites, agent websites, state licensing DBs, etc.) find the **mobile phone
number** and **direct email address** for the listing agent below.

Return your answer as JSON with keys `phone` and `email`.  If you cannot 
find
either field leave it blank but still supply the key.

Agent name: {agent_name}
Agent state: {state}
Brokerage (if known): {brokerage}
"""

openai.api_key = OPENAI_API_KEY

# 
-----------------------------------------------------------------------------
# Local SQLite – keeps track of zpids & phones we’ve already processed
# 
-----------------------------------------------------------------------------
DB_PATH = Path("seen.db")
conn = sqlite3.connect(DB_PATH)
conn.execute(
    """
    CREATE TABLE IF NOT EXISTS listings (
        zpid   TEXT PRIMARY KEY,
        phone  TEXT
    )
"""
)
conn.commit()

# 
-----------------------------------------------------------------------------
# Google Sheet helpers
# 
-----------------------------------------------------------------------------
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
gcreds = 
Credentials.from_service_account_info(json.loads(GOOGLE_CREDS_JSON), 
scopes=SCOPES)
gc = gspread.authorize(gcreds)
sheet = gc.open_by_url(SPREADSHEET_URL).sheet1  # first worksheet


def sheet_phone_exists(phone: str) -> bool:
    if not phone:
        return False
    phones = sheet.col_values(4)  # assuming phone is column D (1-indexed)
    return phone in phones


# 
-----------------------------------------------------------------------------
# GPT wrappers
# 
-----------------------------------------------------------------------------
def gpt_yes_no(system_prompt: str) -> bool:
    """Return True if GPT says YES."""
    resp = openai.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": system_prompt}],
        max_tokens=1,
        temperature=0,
    )
    answer = resp.choices[0].message.content.strip().upper()
    return answer == "YES"


def gpt_json(prompt: str) -> Dict[str, str]:
    resp = openai.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.2,
    )
    try:
        return json.loads(resp.choices[0].message.content)
    except Exception:
        return {"phone": "", "email": ""}


# 
-----------------------------------------------------------------------------
# SMSMobile
# 
-----------------------------------------------------------------------------
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


# 
-----------------------------------------------------------------------------
# Main processing
# 
-----------------------------------------------------------------------------
def process_rows(rows: List[Dict]) -> None:
    """Called from webhook_server after each Apify dataset arrives."""
    for row in rows:
        zpid = str(row.get("zpid") or row.get("detailUrl", ""))

        # ---------------- skip if already seen ----------------
        cur = conn.execute("SELECT 1 FROM listings WHERE zpid = ?", 
(zpid,))
        if cur.fetchone():
            continue  # already imported earlier

        desc = row.get("description", "") or row.get("homeDescription", 
"")
        if not desc:
            continue

        # ---------------- GPT qualification ------------------
        prompt = QUALIFY_PROMPT_TEMPLATE.format(desc=desc[:4000])
        if not gpt_yes_no(prompt):
            continue  # listing doesn’t qualify

        # ---------------- contact lookup ---------------------
        agent_name = row.get("brokerName") or row.get("listingAgentName") 
or ""
        state = row.get("state") or row.get("stateCode") or ""
        brokerage = row.get("brokerageName") or ""
        contact_json = gpt_json(
            CONTACT_PROMPT_TEMPLATE.format(
                agent_name=agent_name, state=state, brokerage=brokerage
            )
        )
        phone = contact_json.get("phone", "").strip()
        email = contact_json.get("email", "").strip()

        # dedupe by phone as well
        if sheet_phone_exists(phone):
            continue

        # ---------------- append to sheet --------------------
        sheet.append_row(
            [
                time.strftime("%Y-%m-%d %H:%M"),
                zpid,
                agent_name,
                phone,
                email,
                row.get("detailUrl", ""),
                desc[:200],  # snippet
            ],
            value_input_option="USER_ENTERED",
        )

        # ---------------- send SMS ---------------------------
        sms_body = f"Hi {agent_name}, I have a buyer for your short-sale 
listing. Please call me back. – {SMSMOBILE_FROM}"
        send_sms(phone, sms_body)

        # ---------------- mark as seen -----------------------
        conn.execute("INSERT OR IGNORE INTO listings (zpid, phone) VALUES 
(?,?)", (zpid, phone))
        conn.commit()

