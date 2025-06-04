import os, json, logging, re, requests, gspread
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials

CS_API_KEY = os.environ["CS_API_KEY"]      # Google Search API key
CS_CX      = os.environ["CS_CX"]           # Search-engine ID
GSHEET_ID  = os.environ["GSHEET_ID"]       # Spreadsheet ID
SC_JSON    = json.loads(os.environ["GCP_SERVICE_ACCOUNT_JSON"])

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

logging.basicConfig(
    level=os.getenv("LOGLEVEL", "INFO"),
    format="%(asctime)s %(levelname)s: %(message)s",
)
LOGGER = logging.getLogger("bot")

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

creds = Credentials.from_service_account_info(SC_JSON, scopes=SCOPES)
sheets = build("sheets", "v4", credentials=creds, cache_discovery=False)
gs = gspread.authorize(creds)
ws = gs.open_by_key(GSHEET_ID).sheet1   # first tab

def phone_exists(phone: str) -> bool:
    phones = ws.col_values(6)  # column F (1-based index)
    return phone in phones

def append_row(values: list[str]):
    ws.append_row(values, value_input_option="RAW")

def delete_last_row():
    ws.delete_rows(ws.row_count)

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

def process_rows(rows: list[dict]):
    for row in rows:
        if not is_short_sale(row.get("description", "")):
            continue
        agent = row.get("agentName", "").strip()
        if not agent:
            continue
        phone, email = google_lookup(agent, row.get("state", ""))
        phone = fmt_phone(phone)
        data = [
            row.get("street", ""),
            row.get("city", ""),
            row.get("state", ""),
            row.get("zip", ""),
            agent,
            phone,
            email,
        ]
        append_row(data)
        if phone and phone_exists(phone):
            delete_last_row()

