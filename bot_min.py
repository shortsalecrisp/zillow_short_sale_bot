import os, json, sqlite3, requests
from openai import OpenAI
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv

load_dotenv()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
client = OpenAI()

SMSM_KEY = os.getenv("SMSM_KEY")
SHEET_URL = os.getenv("SHEET_URL")

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
GC = gspread.authorize(CREDS)
SHEET = GC.open_by_url(SHEET_URL).sheet1

SMSM_URL = "https://api.smsmobile.com/v1/messages"

def process_rows(rows):
    conn = sqlite3.connect("seen.db")
    conn.execute("CREATE TABLE IF NOT EXISTS listings (zpid TEXT PRIMARY KEY)")
    conn.commit()

    for row in rows:
        zpid = str(row.get("zpid", ""))
        if conn.execute("SELECT 1 FROM listings WHERE zpid=?", (zpid,)).fetchone():
            continue

        listing_text = row.get("description", "")
        filter_prompt = (
            "Return YES if the following listing text indicates a qualifying short sale "
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

        agent_name = row.get("listingAgent", {}).get("name", "")
        state = row.get("state", "")
        contact_prompt = (
            f"Find the mobile phone number and email for real estate agent "
            f"{agent_name} in {state}. Respond in JSON with keys 'phone' and 'email'."
        )
        cont_resp = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "user", "content": contact_prompt}],
            temperature=0.2,
        )
        cont_text = cont_resp.choices[0].message.content.strip()
        try:
            contact = json.loads(cont_text)
            phone = contact.get("phone")
            email = contact.get("email", "")
        except json.JSONDecodeError:
            continue
        if not phone:
            continue

        all_records = SHEET.get_all_records()
        if not any(r.get("phone") == phone for r in all_records):
            first = agent_name.split()[0] if agent_name else ""
            address = row.get("address", "")
            sms_body = (
                f"Hey {first}, this is Yoni Kutler—I saw your short sale listing at {address} "
                "and wanted to introduce myself. I specialize in helping agents get faster bank "
                "approvals and ensure these deals close. No cost to you or your client—I’m only "
                "paid by the buyer at closing. Would you be open to a quick call to see if this could help?"
            )
            requests.post(
                SMSM_URL,
                json={"to": phone, "message": sms_body},
                headers={"Authorization": f"Bearer {SMSM_KEY}"},
            )
            SHEET.append_row([zpid, agent_name, phone, email, address, "SMS sent"])

        conn.execute("INSERT OR IGNORE INTO listings(zpid) VALUES(?)", (zpid,))
        conn.commit()

    conn.close()

