import os, json, sqlite3, requests, time
from openai import OpenAI
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv

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
    new_rows = 0
    for row in rows:
        zpid = str(row.get("zpid", ""))
        if conn.execute("SELECT 1 FROM listings WHERE zpid=?", (zpid,)).fetchone():
            continue
        listing_text = row.get("description", "")
        filt_resp = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "user", "content": "Return YES if the following listing text indicates a qualifying short sale with none of our excluded terms; otherwise return NO.\n\n" + listing_text}],
            temperature=0.2,
        )
        if not filt_resp.choices[0].message.content.strip().upper().startswith("YES"):
            continue
        agent_name = row.get("listingAgent", {}).get("name", "")
        state = row.get("state", "")
        cont_resp = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "user", "content": f"Find the mobile phone number and email for real estate agent {agent_name} in {state}. Respond in JSON with keys 'phone' and 'email'."}],
            temperature=0.2,
        )
        try:
            contact = json.loads(cont_resp.choices[0].message.content.strip())
        except json.JSONDecodeError:
            continue
        phone = contact.get("phone")
        email = contact.get("email", "")
        if not phone:
            continue
        if any(r.get("phone") == phone for r in SHEET.get_all_records()):
            continue
        sms_body = (
            f"Hey {agent_name.split()[0] if agent_name else ''}, this is Yoni Kutler—I saw your short sale listing at {row.get('address','')} and wanted to introduce myself. "
            "I specialize in helping agents get faster bank approvals and ensure these deals close. "
            "No cost to you or your client—I’m only paid by the buyer at closing. "
            "Would you be open to a quick call to see if this could help?"
        )
        requests.post(
            SMSM_URL,
            json={"to": phone, "message": sms_body},
            headers={"Authorization": f"Bearer {SMSM_KEY}"},
        )
        SHEET.append_row([zpid, agent_name, phone, email, row.get("address", ""), "SMS sent"])
        conn.execute("INSERT OR IGNORE INTO listings(zpid) VALUES(?)", (zpid,))
        conn.commit()
        new_rows += 1
    conn.close()
    print(f"► processed {new_rows} new listings", flush=True)

