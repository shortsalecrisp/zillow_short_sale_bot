import os
import json
import logging
import re
import requests
import gspread
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials

CS_API_KEY = os.environ["CS_API_KEY"]
CS_CX = os.environ["CS_CX"]
GSHEET_ID = os.environ["GSHEET_ID"]
SC_JSON = json.loads(os.environ["GCP_SERVICE_ACCOUNT_JSON"])
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

logging.basicConfig(
    level=os.getenv("LOGLEVEL", "DEBUG"),
    format="%(asctime)s %(levelname)s: %(message)s",
)
LOGGER = logging.getLogger("bot")

SMS_ENABLE = os.getenv("SMSM_ENABLE", "false").lower() == "true"
SMS_TEST_MODE = os.getenv("SMSM_TEST_MODE", "true").lower() == "true"
SMS_TEST_NUMBER = os.getenv("SMSM_TEST_NUMBER", "")
SMS_API_KEY = os.getenv("SMSM_API_KEY", "")
SMS_FROM = os.getenv("SMSM_FROM", "")
SMS_TEMPLATE = os.getenv("SMSM_TEMPLATE") or (
    "Hey {first}, this is Yoni Kutler—I saw your short sale listing at {address} and "
    "wanted to introduce myself. I specialize in helping agents get faster bank approvals "
    "and ensure these deals close. I know you likely handle short sales yourself, but I work "
    "behind the scenes to take on lender negotiations so you can focus on selling. "
    "No cost to you or your client—I’m only paid by the buyer at closing. "
    "Would you be open to a quick call to see if this could help?"
)

SHORT_RE = re.compile(r"\bshort\s+sale\b", re.I)
BAD_RE = re.compile(r"approved|negotiator|settlement fee|fee at closing", re.I)
PHONE_RE = re.compile(r"(?<!\d)(?:\+?1[\s\-\.]*)?\(?\d{3}\)?[\s\-\.]*\d{3}[\s\-\.]*\d{4}(?!\d)")
EMAIL_RE = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.I)
IMG_EXT = (".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp")

def is_short_sale(text: str) -> bool:
    match = bool(SHORT_RE.search(text))
    bad = bool(BAD_RE.search(text))
    LOGGER.debug("is_short_sale? match=%s, bad=%s", match, bad)
    return match and not bad

def fmt_phone(raw: str) -> str:
    digits = re.sub(r"\D", "", raw)
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    formatted = f"{digits[:3]}-{digits[3:6]}-{digits[6:]}" if len(digits) == 10 else ""
    LOGGER.debug("fmt_phone: raw=%r → %r", raw, formatted)
    return formatted

def ok_email(addr: str) -> bool:
    ok = not addr.lower().endswith(IMG_EXT)
    LOGGER.debug("ok_email? %r → %s", addr, ok)
    return ok

def agent_tokens(name: str) -> list[str]:
    tokens = [t for t in name.lower().split() if len(t) > 2]
    LOGGER.debug("agent_tokens: %r → %s", name, tokens)
    return tokens

def page_matches_agent(html: str, agent: str) -> bool:
    html_l = html.lower()
    tokens = agent_tokens(agent)
    result = all(tok in html_l for tok in tokens)
    LOGGER.debug("page_matches_agent? %r → %s", agent, result)
    return result

creds = Credentials.from_service_account_info(SC_JSON, scopes=SCOPES)
sheets = build("sheets", "v4", credentials=creds, cache_discovery=False)
gs = gspread.authorize(creds)
ws = gs.open_by_key(GSHEET_ID).sheet1

def phone_exists(phone: str) -> bool:
    try:
        phones = ws.col_values(6)
        exists = phone in phones
        LOGGER.debug("phone_exists? %r → %s", phone, exists)
        return exists
    except Exception as exc:
        LOGGER.error("Sheet read failed: %s", exc)
        return False

def append_row(values: list[str]):
    LOGGER.debug("Appending row: %s", values)
    ws.append_row(values, value_input_option="RAW")
    LOGGER.info("Appended row: %s", values)

def google_lookup(agent: str, state: str) -> tuple[str, str]:
    def run_query(query: str) -> tuple[str, str]:
        phone = email = ""
        LOGGER.info("CSE run_query: %r", query)
        try:
            resp = requests.get(
                "https://www.googleapis.com/customsearch/v1",
                params={"key": CS_API_KEY, "cx": CS_CX, "q": query, "num": 10},
                timeout=10
            ).json()
        except Exception as exc:
            LOGGER.warning("CSE request failed: %s", exc)
            return "", ""
        for item in resp.get("items", []):
            url = item.get("link", "")
            LOGGER.debug("Inspecting URL: %s", url)
            try:
                html = requests.get(url, timeout=10).text
            except Exception as exc:
                LOGGER.debug("Failed to fetch URL: %s → %s", url, exc)
                continue
            if not page_matches_agent(html, agent):
                LOGGER.debug("Agent tokens not found on page: %s", url)
                continue
            if not phone and (m := PHONE_RE.search(html)):
                phone = fmt_phone(m.group())
                LOGGER.debug("Found phone %r on %s", phone, url)
            if not email and (m := EMAIL_RE.search(html)) and ok_email(m.group()):
                email = m.group()
                LOGGER.debug("Found email %r on %s", email, url)
            if phone or email:
                LOGGER.info("CSE match: phone=%r, email=%r on %s", phone, email, url)
                break
        return phone, email

    phone1, email1 = run_query(f'"{agent}" {state} phone email')
    if not (phone1 and email1):
        phone2, email2 = run_query(f'"{agent}" {state} site:(realtor.com OR redfin.com OR homesnap.com)')
        phone = phone1 or phone2
        email = email1 or email2
    else:
        phone, email = phone1, email1

    LOGGER.info("CSE result: agent=%r → phone=%r, email=%r", agent, phone, email)
    return phone, email

def send_sms(to_number: str, first: str, address: str) -> bool:
    if not SMS_ENABLE:
        LOGGER.debug("SMS disabled; skipping send")
        return False

    digits = re.sub(r"\D", "", to_number)
    if len(digits) == 10:
        to_e164 = "+1" + digits
    elif len(digits) == 11 and digits.startswith("1"):
        to_e164 = "+" + digits
    else:
        LOGGER.error("Invalid phone format, skipping SMS: %s", to_number)
        return False

    if SMS_TEST_MODE and SMS_TEST_NUMBER:
        LOGGER.info("Test mode – overriding %s → %s", to_e164, SMS_TEST_NUMBER)
        digits_test = re.sub(r"\D", "", SMS_TEST_NUMBER)
        to_e164 = "+1" + digits_test if len(digits_test) == 10 else "+" + digits_test

    text = SMS_TEMPLATE.format(first=first, address=address)
    LOGGER.debug("SMS payload: to=%s, text=%r", to_e164, text)

    payload = {"key": SMS_API_KEY, "to": to_e164, "from": SMS_FROM, "text": text}

    try:
        resp = requests.post("https://smsmobileapi.com/api/v1/messages", json=payload, timeout=15)
    except Exception as exc:
        LOGGER.error("SMS request exception: %s", exc)
        return False

    if resp.status_code != 200:
        LOGGER.error("SMS send failed (%s) – %s", resp.status_code, resp.text[:500])
        return False

    LOGGER.info("SMS successfully sent to %s", to_e164)
    return True

def process_rows(rows: list[dict]):
    LOGGER.info("process_rows: starting with %d rows", len(rows))
    for row in rows:
        zpid = row.get("zpid", "")
        street = row.get("street", "")
        city = row.get("city", "")
        state = row.get("state", "")
        zipcode = row.get("zip", "")
        description = row.get("description", "")
        agent_full = row.get("agentName", "").strip()

        LOGGER.debug("Row ZPID=%r, street=%r, city=%r, state=%r, zip=%r", zpid, street, city, state, zipcode)
        LOGGER.debug("Description snippet: %r", (description[:200] + "...") if description else "<none>")

        if not is_short_sale(description):
            LOGGER.info("Skip ZPID=%r – does not qualify as a short sale", zpid)
            continue

        if not agent_full:
            LOGGER.info("Skip ZPID=%r – missing agentName", zpid)
            continue

        LOGGER.info("Processing ZPID=%r – Agent=%r", zpid, agent_full)

        phone_raw, email = google_lookup(agent_full, state)
        phone = fmt_phone(phone_raw)
        LOGGER.debug("After google_lookup: phone=%r, email=%r", phone, email)

        if phone and phone_exists(phone):
            LOGGER.info("Skip ZPID=%r – phone %r already in sheet", zpid, phone)
            continue

        row_values = [street, city, state, zipcode, agent_full, phone, email]
        append_row(row_values)

        if phone:
            first_name = agent_full.split()[0]
            listing_addr = street
            if send_sms(phone, first_name, listing_addr):
                LOGGER.info("✓ SMS sent to %r for ZPID=%r", phone, zpid)
            else:
                LOGGER.warning("✗ SMS NOT sent to %r for ZPID=%r", phone, zpid)

    LOGGER.info("process_rows: completed")

