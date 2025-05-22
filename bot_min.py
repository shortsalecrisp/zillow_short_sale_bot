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

    EXPECTED_HEADERS = [
        "name", "last name", "phone", "email",
        "listing_address", "city", "state",
    ]
    all_records = SHEET.get_all_records(expected_headers=EXPECTED_HEADERS)

    new_rows = 0
    for row in rows:
        zpid = str(row.get("zpid", ""))

        # skip if Zillow ID already processed in this container
        if conn.execute("SELECT 1 FROM listings WHERE zpid=?", (zpid,)).fetchone():
            continue

        # skip if phone already on the sheet
        listing_phone_set = {r["phone"] for r in all_records if r["phone"]}
        listing_text = (
            row.get("homeDescription")
            or row.get("description")
            or row.get("hdpData", {}).get("homeInfo", {}).get("homeDescription", "")
            or ""
        )
        filter_prompt = (
            "Return YES if the following listing text indicates a qualifying short sale "
            "with none of our excluded terms; otherwise return NO.\n\n"
            f"{listing_text}"
        )
        filt_resp = client.chat.completions.create(
            model="gpt-3.5-turbo-0125",
            messages=[{"role": "user", "content": filter_prompt}],
            temperature=0.2,
        )
        if not filt_resp.choices[0].message.content.strip().upper().startswith("YES"):
            continue

        agent_name = row.get("listingAgent", {}).get("name", "").strip()
        state      = row.get("addressState") or row.get("state", "")
        contact_prompt = (
            f"Find the mobile phone number and email for real estate agent "
            f"{agent_name} in {state}. Respond in JSON with keys 'phone' and 'email'."
        )
        cont_resp = client.chat.completions.create(
            model="gpt-3.5-turbo-0125",
            messages=[{"role": "user", "content": contact_prompt}],
            temperature=0.2,
        )
        try:
            contact = json.loads(cont_resp.choices[0].message.content)
            phone   = (contact.get("phone") or "").strip()
            email   = (contact.get("email") or "").strip()
        except json.JSONDecodeError:
            continue
        if not phone or phone in listing_phone_set:
            continue  # need a phone and must be unique

        name_parts  = agent_name.split(maxsplit=1)
        first       = name_parts[0] if name_parts else ""
        last_name   = name_parts[1] if len(name_parts) > 1 else ""
        address     = row.get("address") or row.get("listing_address") or ""
        city        = row.get("addressCity") or ""
        # state already captured

        SHEET.append_row(
            [
                first,
                last_name,
                phone,
                email,
                address,
                city,
                state,
            ],
            value_input_option="USER_ENTERED",
        )

        conn.execute("INSERT OR IGNORE INTO listings (zpid) VALUES (?)", (zpid,))
        conn.commit()
        new_rows += 1

    print(f"► processed {new_rows} new listings", flush=True)
    conn.close()

