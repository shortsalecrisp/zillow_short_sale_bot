import os, json, logging, re, requests, gspread
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials

# ── Google / Sheets creds ───────────────────────────────────
CS_API_KEY = os.environ["CS_API_KEY"]      # Google Custom Search API key
CS_CX      = os.environ["CS_CX"]           # Search-engine ID
GSHEET_ID  = os.environ["GSHEET_ID"]       # Spreadsheet ID
SC_JSON    = json.loads(os.environ["GCP_SERVICE_ACCOUNT_JSON"])

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# ── SMSMobileAPI settings (all come from Render env) ────────
SMS_ENABLE      = os.getenv("SMSM_ENABLE", "false").lower() == "true"
SMS_TEST_MODE   = os.getenv("SMSM_TEST_MODE", "true").lower() == "true"
SMS_TEST_NUMBER = os.getenv("SMSM_TEST_NUMBER", "")
SMS_API_KEY     = os.getenv("SMSM_API_KEY", "")
SMS_FROM        = os.getenv("SMSM_FROM", "")        # optional
SMS_TEMPLATE    = os.getenv("SMSM_TEMPLATE") or (
    "Hey {first}, this is Yoni Kutler—I saw your short sale listing at {address} and "
    "wanted to introduce myself. I specialize in helping agents get faster bank approvals "
    "and ensure these deals close. I know you likely handle short sales yourself, but I work "
    "behind the scenes to take on lender negotiations so you can focus on selling. "
    "No cost to you or your client—I’m only paid by the buyer at closing. "
    "Would you be open to a quick call to see if this could help?"
)

# ── Logging ─────────────────────────────────────────────────
logging.basicConfig(
    level=os.getenv("LOGLEVEL", "INFO"),
    format="%(asctime)s %(levelname)s: %(message)s",
)
LOGGER = logging.getLogger("bot")

# ── Regex helpers ───────────────────────────────────────────
SHORT_RE   = re.compile(r"\bshort\s+sale\b", re.I)
BAD_RE     = re.compile(r"approved|negotiator|settlement fee|fee at closing", re.I)
PHONE_RE   = re.compile(r"(?<!\d)(?:\+?1[\s\-\.]*)?\(?\d{3}\)?[\s\-\.]*\d{3}[\s\-\.]*\d{4}(?!\d)")
EMAIL_RE   = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.I)
IMG_EXT    = (".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp")

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

# ── Google Sheet handles ───────────────────────────────────
creds   = Credentials.from_service_account_info(SC_JSON, scopes=SCOPES)
sheets  = build("sheets", "v4", credentials=creds, cache_discovery=False)
gs      = gspread.authorize(creds)
ws      = gs.open_by_key(GSHEET_ID).sheet1   # first tab

def phone_exists(phone: str) -> bool:
    # column F (1-based index = 6)
    try:
        phones = ws.col_values(6)
    except Exception as exc:
        LOGGER.error("Sheet read failed: %s", exc)
        return False
    return phone in phones

def append_row(values: list[str]):
    ws.append_row(values, value_input_option="RAW")

# ── Google Custom Search helper ─────────────────────────────
def google_lookup(agent: str, state: str) -> tuple[str, str]:
    phone = email = ""
    params = {
        "key": CS_API_KEY,
        "cx":  CS_CX,
        "q":   f'"{agent}" {state} phone email',
        "num": 10,
    }
    try:
        resp = requests.get("https://www.googleapis.com/customsearch/v1", params=params, timeout=10).json()
    except Exception as exc:
        LOGGER.warning("CSE request failed: %s", exc)
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

# ── SMS send helper ─────────────────────────────────────────
def send_sms(to_number: str, first: str, address: str) -> bool:
    """Fire an SMS via SMSMobileAPI. Returns True on 200 OK."""
    if not SMS_ENABLE:
        LOGGER.debug("SMS disabled; skipping send")
        return False

    if SMS_TEST_MODE and SMS_TEST_NUMBER:
        LOGGER.info("Test mode – overriding %s → %s", to_number, SMS_TEST_NUMBER)
        to_number = SMS_TEST_NUMBER

    text = SMS_TEMPLATE.format(first=first, address=address)
    payload = {
        "key":  SMS_API_KEY,
        "to":   to_number,
        "from": SMS_FROM,
        "text": text,
    }
    try:
        resp = requests.post("https://smsmobileapi.com/api/v1/messages", json=payload, timeout=15)
    except Exception as exc:
        LOGGER.error("SMS request error: %s", exc)
        return False

    ok = resp.status_code == 200
    if not ok:
        LOGGER.error("SMS send failed (%s) – %s", resp.status_code, resp.text[:200])
    return ok

# ── Core entry point used by webhook_server.py ──────────────
def process_rows(rows: list[dict]):
    for row in rows:
        # 1️⃣ Verify listing qualifies
        if not is_short_sale(row.get("description", "")):
            continue

        agent = row.get("agentName", "").strip()
        if not agent:
            continue

        # 2️⃣ Enrich with phone/email
        phone, email = google_lookup(agent, row.get("state", ""))
        phone        = fmt_phone(phone)

        # 3️⃣ Prevent duplicate texts (phone already in sheet)
        if phone and phone_exists(phone):
            LOGGER.info("Skipping agent %s – phone already contacted", phone)
            continue

        # 4️⃣ Write row to sheet
        data = [
            row.get("street", ""),
            row.get("city",   ""),
            row.get("state",  ""),
            row.get("zip",    ""),
            agent,
            phone,
            email,
        ]
        append_row(data)
        LOGGER.info("✓ Appended %s, %s", row.get("street", ""), agent)

        # 5️⃣ Send SMS (only if we actually have a phone number)
        if phone:
            first_name = agent.split()[0]
            addr_short = row.get("street", "")
            if send_sms(phone, first_name, addr_short):
                LOGGER.info("✓ SMS sent to %s", phone)
            else:
                LOGGER.warning("SMS NOT sent to %s", phone)

