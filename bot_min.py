# bot_min.py  – Zillow ➜ Sheets bot  +  test-safe SMS texting
# ────────────────────────────────────────────────────────────
import os, json, logging, re, requests, gspread, traceback
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials

# ── Google & Sheets creds ───────────────────────────────────
CS_API_KEY = os.environ["CS_API_KEY"]
CS_CX      = os.environ["CS_CX"]
GSHEET_ID  = os.environ["GSHEET_ID"]
SC_JSON    = json.loads(os.environ["GCP_SERVICE_ACCOUNT_JSON"])
SCOPES     = ["https://www.googleapis.com/auth/spreadsheets"]

# ── SMSMobileAPI settings (all env) ─────────────────────────
SMS_ENABLE      = os.getenv("SMSM_ENABLE", "false").lower() == "true"
SMS_TEST_MODE   = os.getenv("SMSM_TEST_MODE", "true").lower() == "true"
SMS_TEST_NUMBER = os.getenv("SMSM_TEST_NUMBER", "")
SMS_API_KEY     = os.getenv("SMSM_API_KEY", "")
SMS_FROM        = os.getenv("SMSM_FROM", "")         # optional
SMS_TEMPLATE    = os.getenv("SMSM_TEMPLATE") or (
    "Hey {first}, this is Yoni Kutler—I saw your short sale listing at {address} "
    "and wanted to introduce myself. I specialize in helping agents get faster bank "
    "approvals and ensure these deals close. I know you likely handle short sales yourself, "
    "but I work behind the scenes to take on lender negotiations so you can focus on selling. "
    "No cost to you or your client—I’m only paid by the buyer at closing. "
    "Would you be open to a quick call to see if this could help?"
)

# ── Logging ─────────────────────────────────────────────────
logging.basicConfig(
    level=os.getenv("LOGLEVEL", "INFO"),
    format="%(asctime)s %(levelname)s: %(message)s",
)
LOG = logging.getLogger("bot")

# ── Regex helpers ───────────────────────────────────────────
SHORT_RE    = re.compile(r"\bshort\s+sale\b", re.I)
BAD_RE      = re.compile(r"approved|negotiator|settlement fee|fee at closing", re.I)
PHONE_RE    = re.compile(r"(?<!\d)(?:\+?1[\s\-\.]*)?\(?\d{3}\)?[\s\-\.]*\d{3}[\s\-\.]*\d{4}(?!\d)")
EMAIL_RE    = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.I)
BAD_PHONE   = re.compile(r"(0000|1234)$")     # obvious dummies
IMG_EXT     = (".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp")

def is_short_sale(text: str) -> bool:
    return bool(SHORT_RE.search(text)) and not BAD_RE.search(text)

def fmt_phone(raw: str) -> str:
    digits = re.sub(r"\D", "", raw)
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    return f"{digits[:3]}-{digits[3:6]}-{digits[6:]}" if len(digits) == 10 else ""

def ok_email(addr: str) -> bool:
    return not addr.lower().endswith(IMG_EXT)

def tokens(name: str) -> list[str]:
    return [t for t in name.lower().split() if len(t) > 2]

def page_matches_agent(html: str, agent: str) -> bool:
    html_l = html.lower()
    return all(t in html_l for t in tokens(agent))

# ── Google Sheet handles ───────────────────────────────────
creds  = Credentials.from_service_account_info(SC_JSON, scopes=SCOPES)
gs     = gspread.authorize(creds)
ws     = gs.open_by_key(GSHEET_ID).sheet1   # first tab

def phone_exists(phone: str) -> bool:
    try:
        return phone in ws.col_values(3)    # column C holds phone
    except Exception as exc:
        LOG.error("Sheet read failed: %s", exc)
        return False

def append_row(values: list[str]):
    ws.append_row(values, value_input_option="RAW")

# ── Contact-info lookup (2-pass) ────────────────────────────
def google_lookup(agent: str, state: str) -> tuple[str, str]:
    def run_query(q: str) -> tuple[str, str]:
        phone = email = ""
        try:
            resp = requests.get(
                "https://www.googleapis.com/customsearch/v1",
                params={"key": CS_API_KEY, "cx": CS_CX, "q": q, "num": 10},
                timeout=10,
            ).json()
        except Exception as exc:
            LOG.warning("CSE error: %s", exc)
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

    # pass 1 – broad
    phone, email = run_query(f'"{agent}" {state} phone email')
    # pass 2 – target common RE sites if still empty
    if not (phone and email):
        phone2, email2 = run_query(
            f'"{agent}" {state} site:(realtor.com OR redfin.com OR homesnap.com)'
        )
        phone = phone or phone2
        email = email or email2
    return phone, email

# ── SMS send helper ─────────────────────────────────────────
def send_sms(to_number: str, first: str, address: str) -> bool:
    if not SMS_ENABLE:
        LOG.debug("SMS disabled; skipping send")
        return False
    if SMS_TEST_MODE and SMS_TEST_NUMBER:
        LOG.info("Test mode → redirect %s ➜ %s", to_number, SMS_TEST_NUMBER)
        to_number = SMS_TEST_NUMBER

    text = SMS_TEMPLATE.format(first=first, address=address)
    payload = {"key": SMS_API_KEY, "to": to_number, "from": SMS_FROM, "text": text}

    try:
        resp = requests.post(
            "https://smsmobileapi.com/api/v1/messages", json=payload, timeout=15
        )
        ok = resp.status_code == 200
        if not ok:
            LOG.error("SMS send failed (%s) – %s", resp.status_code, resp.text[:200])
        return ok
    except Exception as exc:
        LOG.error("SMS request error: %s", exc)
        return False

# ── Core entry point ───────────────────────────────────────
def process_rows(rows: list[dict]):
    for r in rows:
        try:
            zpid = r.get("zpid")
            desc = r.get("description", "")
            LOG.debug("ZPID %s – checking description length %d", zpid, len(desc))

            if not is_short_sale(desc):
                LOG.debug("⤷ skip – not a qualifying short sale")
                continue

            agent_full = r.get("agentName", "").strip()
            if not agent_full:
                LOG.debug("⤷ skip – no agent name")
                continue

            LOG.info("Processing %s – %s", r.get("street", ""), agent_full)
            phone, email = google_lookup(agent_full, r.get("state", ""))
            phone = fmt_phone(phone)

            # ditch obvious dummies
            if phone and BAD_PHONE.search(phone):
                LOG.debug("Found dummy phone %s – discarding", phone)
                phone = ""

            agent_first, *rest = agent_full.split()
            agent_last = " ".join(rest) or ""

            # abort SMS if we already texted
            already = phone and phone_exists(phone)
            if already:
                LOG.info("⤷ phone %s already contacted – row still logged, no SMS", phone)

            # Assemble row in requested order
            row_vals = [
                agent_first, agent_last,          # Columns A-B
                phone, email,                    # C-D
                r.get("street", ""),             # E  listing_address
                r.get("city",   ""),             # F
                r.get("state",  ""),             # G
            ]
            append_row(row_vals)
            LOG.info("✓ Row appended")

            # Fire SMS after row append
            if phone and not already:
                ok = send_sms(phone, agent_first, r.get("street", ""))
                LOG.info("✓ SMS sent" if ok else "SMS not sent")

        except Exception:
            LOG.error("Unhandled error on row %s\n%s", r.get("zpid"), traceback.format_exc())
            # and continue with the next listing

