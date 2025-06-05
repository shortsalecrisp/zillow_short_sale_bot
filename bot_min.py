# bot_min.py  •  Zillow ➔ Sheets bot with (test‐safe) SMS texting and verbose debug logging
# 
─────────────────────────────────────────────────────────────────────────────────────────────
import os
import json
import logging
import re
import requests
import gspread
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials

# ── CONFIGURATION & LOGGING 
─────────────────────────────────────────────────────────────────
CS_API_KEY = os.environ["CS_API_KEY"]      # Google Search API key
CS_CX      = os.environ["CS_CX"]           # Search‐engine ID
GSHEET_ID  = os.environ["GSHEET_ID"]       # Spreadsheet ID
SC_JSON    = json.loads(os.environ["GCP_SERVICE_ACCOUNT_JSON"])

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

logging.basicConfig(
    level=os.getenv("LOGLEVEL", "DEBUG"),  # Set default to DEBUG for maximum detail
    format="%(asctime)s %(levelname)s: %(message)s",
)
LOGGER = logging.getLogger("bot")

# ── SMSMobileAPI SETTINGS (ALL FROM ENV) ────────────────────────────────────────────────────
SMS_ENABLE      = os.getenv("SMSM_ENABLE", "false").lower() == "true"
SMS_TEST_MODE   = os.getenv("SMSM_TEST_MODE", "true").lower() == "true"
SMS_TEST_NUMBER = os.getenv("SMSM_TEST_NUMBER", "")
SMS_API_KEY     = os.getenv("SMSM_API_KEY", "")
SMS_FROM        = os.getenv("SMSM_FROM", "")        # Optional "from" ID
SMS_TEMPLATE    = os.getenv("SMSM_TEMPLATE") or (
    "Hey {first}, this is Yoni Kutler—I saw your short sale listing at {address} and "
    "wanted to introduce myself. I specialize in helping agents get faster bank approvals "
    "and ensure these deals close. I know you likely handle short sales yourself, but I work "
    "behind the scenes to take on lender negotiations so you can focus on selling. "
    "No cost to you or your client—I’m only paid by the buyer at closing. "
    "Would you be open to a quick call to see if this could help?"
)

# ── REGEX & UTILITIES 
────────────────────────────────────────────────────────────────────────
SHORT_RE   = re.compile(r"\bshort\s+sale\b", re.I)
BAD_RE     = re.compile(r"approved|negotiator|settlement fee|fee at closing", re.I)
PHONE_RE   = re.compile(r"(?<!\d)(?:\+?1[\s\-\.]*)?\(?\d{3}\)?[\s\-\.]*\d{3}[\s\-\.]*\d{4}(?!\d)")
EMAIL_RE   = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.I)
IMG_EXT    = (".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp")

def is_short_sale(text: str) -> bool:
    """Return True if 'short sale' appears and none of the disqualifiers appear."""
    match = bool(SHORT_RE.search(text))
    bad = bool(BAD_RE.search(text))
    LOGGER.debug("is_short_sale? match=%s, bad=%s, text-snippet=%r", match, bad, text[:100])
    return match and not bad

def fmt_phone(raw: str) -> str:
    """Strip non‐digits, collapse leading '1', then format as XXX-YYY-ZZZZ if length==10."""
    digits = re.sub(r"\D", "", raw)
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    formatted = f"{digits[:3]}-{digits[3:6]}-{digits[6:]}" if len(digits) == 10 else ""
    LOGGER.debug("fmt_phone: raw=%r → formatted=%r", raw, formatted)
    return formatted

def ok_email(addr: str) -> bool:
    """Return False if the email ends with a common image extension."""
    ok = not addr.lower().endswith(IMG_EXT)
    LOGGER.debug("ok_email? addr=%r → %s", addr, ok)
    return ok

def agent_tokens(name: str) -> list[str]:
    """Split agent name into lowercase tokens > 2 chars for matching."""
    tokens = [t for t in name.lower().split() if len(t) > 2]
    LOGGER.debug("agent_tokens: %r → %s", name, tokens)
    return tokens

def page_matches_agent(html: str, agent: str) -> bool:
    """Return True if every token from agent appears in html (lowercase match)."""
    html_l = html.lower()
    tokens = agent_tokens(agent)
    result = all(tok in html_l for tok in tokens)
    LOGGER.debug("page_matches_agent? agent=%r → %s", agent, result)
    return result

# ── GOOGLE SHEETS SETUP 
──────────────────────────────────────────────────────────────────────
creds   = Credentials.from_service_account_info(SC_JSON, scopes=SCOPES)
sheets  = build("sheets", "v4", credentials=creds, cache_discovery=False)
gs      = gspread.authorize(creds)
ws      = gs.open_by_key(GSHEET_ID).sheet1   # Use the first tab

def phone_exists(phone: str) -> bool:
    """
    Check column F (1-based index=6) for existing phone.
    Returns True if found, False otherwise.
    """
    try:
        phones = ws.col_values(6)
        exists = phone in phones
        LOGGER.debug("phone_exists? %r → %s", phone, exists)
        return exists
    except Exception as exc:
        LOGGER.error("Sheet read (col F) failed: %s", exc)
        return False

def append_row(values: list[str]):
    """Append a new row to the sheet with RAW input; log the row values."""
    LOGGER.debug("Appending row to sheet: %s", values)
    ws.append_row(values, value_input_option="RAW")
    LOGGER.info("Appended row to sheet: %s", values)

# ── GOOGLE CUSTOM SEARCH (CSE) FOR CONTACT LOOKUP ─────────────────────────────────────────────
def google_lookup(agent: str, state: str) -> tuple[str, str]:
    """
    Run up to two CSE queries to find phone & email for the agent.
    1) Broad query: "<agent> <state> phone email"
    2) If still empty, target realtor/redfin/homesnap domains.
    """
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
                LOGGER.debug("Agent tokens not found in page: %s", url)
                continue
            if not phone and (m := PHONE_RE.search(html)):
                phone = fmt_phone(m.group())
                LOGGER.debug("Found candidate phone %r on %s", phone, url)
            if not email and (m := EMAIL_RE.search(html)) and ok_email(m.group()):
                email = m.group()
                LOGGER.debug("Found candidate email %r on %s", email, url)
            if phone or email:
                LOGGER.info("CSE match found on %s → phone=%r, email=%r", url, phone, email)
                break
        return phone, email

    # Pass 1: broad
    phone1, email1 = run_query(f'"{agent}" {state} phone email')
    # Pass 2: restrict to common real‐estate domains if still missing
    if not (phone1 and email1):
        phone2, email2 = run_query(
            f'"{agent}" {state} site:(realtor.com OR redfin.com OR homesnap.com)'
        )
        phone = phone1 or phone2
        email = email1 or email2
    else:
        phone, email = phone1, email1

    LOGGER.info("CSE result for %r: phone=%r, email=%r", agent, phone, email)
    return phone, email

# ── SMS‐SENDING HELPER (ONLY MODIFIED PART) ─────────────────────────────────────────────────
def send_sms(to_number: str, first: str, address: str) -> bool:
    """
    Fire an SMS via SMSMobileAPI.
    Returns True on HTTP 200 OK, False otherwise.
    Steps:
      1) Skip entirely if SMS_ENABLE=False.
      2) Normalize 'to_number' to digits; enforce E.164 by prefixing +1.
      3) If SMS_TEST_MODE=True, override with SMS_TEST_NUMBER (also E.164).
      4) Format the SMS body from SMS_TEMPLATE (with {first} & {address}).
      5) POST to the SMSMobileAPI endpoint with JSON payload.
    """
    if not SMS_ENABLE:
        LOGGER.debug("SMS disabled; skipping send")
        return False

    # Normalize to digits and enforce E.164
    digits = re.sub(r"\D", "", to_number)
    if len(digits) == 10:
        to_e164 = "+1" + digits
    elif len(digits) == 11 and digits.startswith("1"):
        to_e164 = "+" + digits
    else:
        LOGGER.error("Invalid phone format, skipping SMS: %s", to_number)
        return False

    # Test mode: redirect to personal number
    if SMS_TEST_MODE and SMS_TEST_NUMBER:
        LOGGER.info("Test mode – overriding %s -> %s", to_e164, SMS_TEST_NUMBER)
        digits_test = re.sub(r"\D", "", SMS_TEST_NUMBER)
        to_e164 = "+1" + digits_test if len(digits_test) == 10 else "+" + digits_test

    text = SMS_TEMPLATE.format(first=first, address=address)
    LOGGER.debug("SMS payload for %s: text=%r", to_e164, text)

    payload = {
        "key":  SMS_API_KEY,
        "to":   to_e164,
        "from": SMS_FROM,
        "text": text,
    }

    try:
        resp = requests.post(
            "https://smsmobileapi.com/api/v1/messages",
            json=payload,
            timeout=15,
        )
    except Exception as exc:
        LOGGER.error("SMS request exception: %s", exc)
        return False

    if resp.status_code != 200:
        LOGGER.error("SMS send failed (%s) – %s", resp.status_code, resp.text[:500])
        return False

    LOGGER.info("SMS successfully sent to %s", to_e164)
    return True

# ── MAIN WORKFLOW: process_rows 
──────────────────────────────────────────────────────────────
def process_rows(rows: list[dict]):
    """
    For each listing row (from Apify):
      1) Scrape listing description + agent name.
      2) Check if 'short sale' phrase appears and no disqualifiers.
      3) Lookup agent phone & email via Google CSE.
      4) Format phone; skip if invalid or already in sheet.
      5) Append row into Sheet columns A–G: street, city, state, zip, agent, phone, email.
      6) After append, if phone wasn’t previously in sheet, send SMS.
      Detailed debug logging at each step.
    """
    LOGGER.info("process_rows: starting with %d rows", len(rows))
    for row in rows:
        zpid       = row.get("zpid", "")
        street     = row.get("street", "")
        city       = row.get("city", "")
        state      = row.get("state", "")
        zipcode    = row.get("zip", "")
        description= row.get("description", "")
        agent_full = row.get("agentName", "").strip()

        LOGGER.debug("Row ZPID=%r, street=%r, city=%r, state=%r, zip=%r", zpid, street, city, state, zipcode)
        LOGGER.debug("Description snippet: %r", (description[:200] + "...") if description else "<none>")

        # 1) QUALIFICATION: must be short sale + no bad keywords
        if not is_short_sale(description):
            LOGGER.info("Skip ZPID=%r – does not qualify as a short sale", zpid)
            continue

        # 2) AGENT presence
        if not agent_full:
            LOGGER.info("Skip ZPID=%r – missing agentName", zpid)
            continue

        LOGGER.info("Processing ZPID=%r – Agent=%r", zpid, agent_full)

        # 3) LOOKUP: Google CSE for contact info
        phone_raw, email = google_lookup(agent_full, state)
        phone = fmt_phone(phone_raw)
        LOGGER.debug("After google_lookup: phone=%r, email=%r", phone, email)

        # 4) DEDUPE BY PHONE
        if phone and phone_exists(phone):
            LOGGER.info("Skip ZPID=%r – phone %r already in sheet", zpid, phone)
            continue

        # 5) APPEND to Sheet
        row_values = [street, city, state, zipcode, agent_full, phone, email]
        append_row(row_values)

        # 6) SMS: only if phone is present, newly discovered
        if phone:
            first_name   = agent_full.split()[0]
            listing_addr = street
            if send_sms(phone, first_name, listing_addr):
                LOGGER.info("✓ SMS sent to %r for ZPID=%r", phone, zpid)
            else:
                LOGGER.warning("✗ SMS NOT sent to %r for ZPID=%r", phone, zpid)

    LOGGER.info("process_rows: completed")

