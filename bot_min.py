import os
import json
import logging
import re
import requests
import gspread
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials

CS_API_KEY   = os.environ["CS_API_KEY"]
CS_CX        = os.environ["CS_CX"]
GSHEET_ID    = os.environ["GSHEET_ID"]
SC_JSON      = json.loads(os.environ["GCP_SERVICE_ACCOUNT_JSON"])
SCOPES       = ["https://www.googleapis.com/auth/spreadsheets"]

logging.basicConfig(
    level=os.getenv("LOGLEVEL", "DEBUG"),
    format="%(asctime)s %(levelname)s: %(message)s",
)
LOGGER = logging.getLogger("bot")

SMS_ENABLE      = os.getenv("SMSM_ENABLE", "false").lower() == "true"
SMS_TEST_MODE   = os.getenv("SMSM_TEST_MODE", "true").lower() == "true"
SMS_TEST_NUMBER = os.getenv("SMSM_TEST_NUMBER", "")
SMS_API_KEY     = os.getenv("SMSM_API_KEY", "")
SMS_FROM        = os.getenv("SMSM_FROM", "")
SMS_TEMPLATE    = os.getenv("SMSM_TEMPLATE") or (
    "Hey {first}, this is Yoni Kutler—I saw your short sale listing at {address} and "
    "wanted to introduce myself. I specialize in helping agents get faster bank approvals "
    "and ensure these deals close. I know you likely handle short sales yourself, but I work "
    "behind the scenes to take on lender negotiations so you can focus on selling. "
    "No cost to you or your client—I’m only paid by the buyer at closing. "
    "Would you be open to a quick call to see if this could help?"
)

SHORT_RE  = re.compile(r"\bshort\s+sale\b", re.I)
BAD_RE    = re.compile(r"approved|negotiator|settlement fee|fee at closing", re.I)
PHONE_RE  = re.compile(r"(?<!\d)(?:\+?1[\s\-\.]*)?\(?\d{3}\)?[\s\-\.]*\d{3}[\s\-\.]*\d{4}(?!\d)")
EMAIL_RE  = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.I)
IMG_EXT   = (".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp")

def is_short_sale(text: str) -> bool:
    return bool(SHORT_RE.search(text)) and not BAD_RE.search(text)

def fmt_phone(raw: str) -> str:
    digits = re.sub(r"\D", "", raw)
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    return f"{digits[:3]}-{digits[3:6]}-{digits[6:]}" if len(digits) == 10 else ""

def ok_email(addr: str) -> bool:
    return not addr.lower().endswith(IMG_EXT)

def agent_tokens(name: str) -> list[str]:
    return [t for t in name.lower().split() if len(t) > 2]

def page_matches_agent(html: str, agent: str) -> bool:
    html_l = html.lower()
    return all(tok in html_l for tok in agent_tokens(agent))

creds = Credentials.from_service_account_info(SC_JSON, scopes=SCOPES)
sheets_service = build("sheets", "v4", credentials=creds, cache_discovery=False)
gs = gspread.authorize(creds)
ws = gs.open_by_key(GSHEET_ID).sheet1

def phone_exists(phone: str) -> bool:
    try:
        phones = ws.col_values(3)  # Column C holds phone
        return phone in phones
    except Exception as exc:
        LOGGER.error("Sheet read failed: %s", exc)
        return False

def append_row(values: list[str]):
    """
    Always append starting at column A.  Values order must be:
    first_name, last_name, phone, email, street, city, state
    """
    body = {"values": [values]}
    try:
        sheets_service.spreadsheets().values().append(
            spreadsheetId=GSHEET_ID,
            range="Sheet1!A1",
            valueInputOption="RAW",
            body=body
        ).execute()
        LOGGER.info("Appended row: %s", values)
    except Exception as exc:
        LOGGER.error("Failed to append row: %s → %s", values, exc)

def google_lookup(agent: str, state: str) -> tuple[str, str]:
    def run_query(q: str) -> tuple[str, str]:
        phone = email = ""
        try:
            resp = requests.get(
                "https://www.googleapis.com/customsearch/v1",
                params={"key": CS_API_KEY, "cx": CS_CX, "q": q, "num": 10},
                timeout=10
            ).json()
        except Exception as exc:
            LOGGER.warning("CSE error: %s", exc)
            return "", ""
        for item in resp.get("items", []):
            url = item.get("link", "")
            try:
                html = requests.get(url, timeout=10).text
            except Exception:
                continue
            if not page_matches_agent(html, agent):
                continue
            if not phone and (m := PHONE_RE.search(html)):
                phone = fmt_phone(m.group())
            if not email and (m := EMAIL_RE.search(html)) and ok_email(m.group()):
                email = m.group()
            if phone or email:
                break
        return phone, email

    q1 = f'"{agent}" {state} "mobile" OR "cell" phone email'
    phone, email = run_query(q1)
    if not (phone or email):
        q2 = f'"{agent}" {state} mobile OR cell site:(realtor.com OR redfin.com OR homesnap.com)'
        phone, email = run_query(q2)
    LOGGER.info("Lookup %s → phone=%r email=%r", agent, phone, email)
    return phone, email

def send_sms(to_number: str, first: str, address: str) -> bool:
    if not SMS_ENABLE:
        LOGGER.debug("SMS disabled; skip")
        return False
    digits = re.sub(r"\D", "", to_number)
    if len(digits) == 10:
        to_e164 = "+1" + digits
    elif len(digits) == 11 and digits.startswith("1"):
        to_e164 = "+" + digits
    else:
        LOGGER.error("Bad phone %s", to_number)
        return False
    if SMS_TEST_MODE and SMS_TEST_NUMBER:
        test_digits = re.sub(r"\D", "", SMS_TEST_NUMBER)
        to_e164 = "+1" + test_digits if len(test_digits) == 10 else "+" + test_digits
    text = SMS_TEMPLATE.format(first=first, address=address)
    payload = {"key": SMS_API_KEY, "to": to_e164, "from": SMS_FROM, "text": text}
    try:
        resp = requests.post("https://smsmobileapi.com/api/v1/messages", json=payload, timeout=15)
    except Exception as exc:
        LOGGER.error("SMS request exception: %s", exc)
        return False
    if resp.status_code != 200:
        LOGGER.error("SMS failed (%s) %s", resp.status_code, resp.text[:400])
        return False
    LOGGER.info("SMS sent to %s", to_e164)
    return True

def process_rows(rows: list[dict]):
    LOGGER.info("Processing %d rows", len(rows))
    for r in rows:
        street   = r.get("street", "")
        city     = r.get("city", "")
        state    = r.get("state", "")
        zipc     = r.get("zip", "")
        desc     = r.get("description", "")
        agent    = r.get("agentName", "").strip()

        if not is_short_sale(desc):
            continue
        if not agent:
            continue

        phone_raw, email = google_lookup(agent, state)
        phone = fmt_phone(phone_raw)

        if phone and phone_exists(phone):
            continue

        first_name, *last_parts = agent.split()
        last_name = " ".join(last_parts)

        row_vals = [first_name, last_name, phone, email, street, city, state]
        append_row(row_vals)

        if phone:
            send_sms(phone, first_name, street)
    LOGGER.info("Done")

