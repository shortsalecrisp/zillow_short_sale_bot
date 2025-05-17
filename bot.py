"""
Zillow short-sale pipeline.

Receives rows from Apify webhook, filters with GPT, looks up the listing
agent’s mobile & email via Google Search + GPT extraction, writes Google
Sheets, sends SMS through SMSmobile, and stores every zpid in SQLite so
we never process a listing twice.

Environment variables expected (Render → Environment tab):

  OPENAI_API_KEY        OpenAI secret key
  APIFY_API_TOKEN       Apify token
  SMSMOBILE_API_KEY     SMSmobile key
  SMSMOBILE_FROM        Sender ID / phone registered with SMSmobile
  GOOGLE_SVC_JSON       (optional) entire JSON for the service account
"""

import os, json, html, textwrap, datetime, sqlite3, requests
from pathlib import Path

import openai
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ---------- secrets ----------
openai.api_key = os.environ["OPENAI_API_KEY"]
APIFY_TOKEN    = os.environ["APIFY_API_TOKEN"]
SMS_KEY        = os.environ["SMSMOBILE_API_KEY"]
SMS_FROM       = os.environ["SMSMOBILE_FROM"]

# if service-account JSON is in an env-var, write it once
if "GOOGLE_SVC_JSON" in os.environ and not 
Path("service_account.json").exists():
    Path("service_account.json").write_text(os.environ["GOOGLE_SVC_JSON"], 
encoding="utf-8")

# ---------- Google Sheets ----------
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
creds  = 
ServiceAccountCredentials.from_json_keyfile_name("service_account.json", 
SCOPES)
gsc    = gspread.authorize(creds)
SHEET  = 
gsc.open_by_key("12UzsoQCo4W0WB_lNl3BjKpQ_wXNhEH7xegkFRVu2M70").sheet1   # 
<-- your sheet

# ---------- local dedupe ----------
conn = sqlite3.connect("seen.db")
conn.execute(
    """
    CREATE TABLE IF NOT EXISTS listings (
        zpid    TEXT PRIMARY KEY,
        address TEXT,
        agent   TEXT,
        phone   TEXT,
        email   TEXT,
        status  TEXT
    );
    """
)
conn.commit()

# ---------- helpers ----------
def send_sms(to: str, body: str) -> None:
    """Send SMS via SMSmobile (adjust URL/field names if your account 
differs)."""
    url = "https://rest.smsmobile.com/v1/messages"        # update if docs 
show a different path
    r = requests.post(
        url,
        json={
            "apiKey":  SMS_KEY,
            "from":    SMS_FROM,
            "to":      to,
            "message": body,
        },
        timeout=15,
    )
    r.raise_for_status()

def gpt_is_short_sale(text: str) -> bool:
    prompt = (
        "Return YES if the following listing text indicates the property 
is a short sale "
        "and NOT already approved or labelled 'not a short sale'. 
Otherwise return NO.\n\n"
        f"{text[:3500]}"
    )
    resp = openai.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[{"role":"user","content":prompt}],
        max_tokens=3,
        temperature=0,
    )
    return "YES" in resp.choices[0].message.content.upper()

# ---------- contact lookup via Google Search + GPT ----------
SEARCH_ACTOR = "apify/google-search-scraper"

def _google_search(query: str, max_links: int = 5) -> list[str]:
    run = requests.post(
        f"https://api.apify.com/v2/acts/{SEARCH_ACTOR}/runs",
        params={"token": APIFY_TOKEN, "waitForFinish": 1},
        json={
            "queries": [query],
            "resultsPerPage": max_links,
            "maxPagesPerQuery": 1,
        },
        timeout=90,
    ).json()["data"]

    dataset = run["defaultDatasetId"]
    items = requests.get(
        f"https://api.apify.com/v2/datasets/{dataset}/items",
        params={"token": APIFY_TOKEN, "clean": 1, "format": "json"},
        timeout=30,
    ).json()
    return [it["url"] for it in items][:max_links]

def _gpt_extract(url: str, html_text: str):
    prompt = textwrap.dedent(f"""
        Extract a phone number and an email from the HTML below.
        Return strictly JSON like {{"phone":"...","email":"..."}} – use 
null if missing.
        Do NOT guess.

        URL: {url}

        HTML snippet:
        {html.escape(html_text[:3500])}
    """)
    resp = openai.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[{"role":"user","content":prompt}],
        max_tokens=64,
        temperature=0,
    )
    try:
        data = json.loads(resp.choices[0].message.content)
        return data.get("phone"), data.get("email")
    except Exception:
        return None, None

def find_contact(row: dict):
    agent  = row.get("agentName", "")
    address = row.get("address","")
    state  = address.split(",")[-2].strip().split()[0] if "," in address 
else ""
    query  = f"\"{agent}\" real estate {state} phone"

    for link in _google_search(query):
        try:
            html_txt = requests.get(link, timeout=12, 
headers={"User-Agent":"Mozilla/5.0"}).text
        except Exception:
            continue
        phone, email = _gpt_extract(link, html_txt)
        if phone or email:
            return phone, email
    return None, None   # nothing found

# ---------- main entry called from webhook ----------
def process_rows(rows: list[dict]):
    imported = 0
    for row in rows:
        zpid = str(row["zpid"])

        # skip duplicates
        if conn.execute("SELECT 1 FROM listings WHERE zpid=?", 
(zpid,)).fetchone():
            continue

        # GPT short-sale filter
        if not gpt_is_short_sale(row.get("description","")):
            continue

        phone, email = find_contact(row)
        if not phone:
            continue    # require a phone to send SMS

        # write Google Sheet
        SHEET.append_row([
            datetime.datetime.now().isoformat(timespec="seconds"),
            row.get("address"),
            phone,
            email or "",
            row.get("agentName",""),
            row.get("detailUrl"),
        ])

        # send SMS
        sms_text = f"Hi {row.get('agentName','there')}, I saw your 
short-sale at {row.get('address')}. Are you open to discussing?"
        try:
            send_sms(phone, sms_text)
            print("Contacted", phone, row.get("address"))
        except Exception as e:
            print("SMS failed", phone, e)

        # mark as processed
        conn.execute("INSERT OR IGNORE INTO listings (zpid) VALUES (?)", 
(zpid,))
        conn.commit()
        imported += 1

    print("process_rows finished – imported", imported)

