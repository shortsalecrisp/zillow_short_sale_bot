conda activate zillowbotimport os, json, logging, re, requests
from typing import List, Dict, Tuple
import gspread
from google.oauth2.service_account import Credentials
from bs4 import BeautifulSoup

# ── ENV ────────────────────────────────────────────────────────────────────────
SHEET_ID        = os.environ["GSHEET_ID"]
SHEET_NAME      = "Sheet1"
CS_API_KEY = os.environ["CS_API_KEY"] 
CS_CX           = os.environ.get("CS_CX")
SERVICE_ACCOUNT = json.loads(
    os.environ.get("GSERVICE_ACCOUNT") or os.environ["GCP_SERVICE_ACCOUNT_JSON"]
)

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# ── AUTH ───────────────────────────────────────────────────────────────────────
creds  = Credentials.from_service_account_info(SERVICE_ACCOUNT, scopes=SCOPES)
sheet  = gspread.authorize(creds).open_by_key(SHEET_ID).worksheet(SHEET_NAME)

# ── LOGGING ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=os.getenv("LOGLEVEL", "INFO"),
    format="%(asctime)s %(levelname)s: %(message)s",
)
LOGGER = logging.getLogger("bot")

# ── REGEX ──────────────────────────────────────────────────────────────────────
SHORT_SALE_RE = re.compile(r"\bshort\s+sale\b", re.I)
BAD_WORDS_RE  = re.compile(r"approved|negotiator|settlement fee|fee at closing|not a short sale", re.I)
PHONE_RE      = re.compile(
    r"(?<!\d)(?:\+?1[\s\-\.]*)?\(?(\d{3})\)?[\s\-\.]*(\d{3})[\s\-\.]*(\d{4})(?!\d)"
)
EMAIL_RE      = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.I)
IMAGE_EXTS    = (".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp")

# ── HELPERS ────────────────────────────────────────────────────────────────────
def is_short_sale(text: str) -> bool:
    return bool(SHORT_SALE_RE.search(text)) and not BAD_WORDS_RE.search(text)

def fmt_phone(raw: str) -> str:
    m = PHONE_RE.search(raw)
    return f"{m.group(1)}-{m.group(2)}-{m.group(3)}" if m else ""

def ok_email(addr: str) -> bool:
    return not addr.lower().endswith(IMAGE_EXTS)

def page_matches_agent(html: str, agent: str) -> bool:
    tokens = [t for t in agent.lower().split() if len(t) > 2]
    low    = html.lower()
    return all(tok in low for tok in tokens)

# ── SHEET UTILS ────────────────────────────────────────────────────────────────
def existing_phones() -> set:
    try:
        return {p.strip() for p in sheet.col_values(6) if p.strip()}
    except Exception as exc:
        LOGGER.warning("Could not read phone column: %s", exc)
        return set()

def append_row(values: List[str]) -> None:
    sheet.append_row(values, value_input_option="RAW")

# ── GOOGLE SEARCH ──────────────────────────────────────────────────────────────
def google_lookup(agent: str, state: str) -> Tuple[str, str]:
    params = {
        "key": CS_API_KEY,
        "cx":  CS_CX,
        "q":   f'"{agent}" {state} phone email',
        "num": 10,
    }
    try:
        resp = requests.get(
            "https://www.googleapis.com/customsearch/v1",
            params=params,
            timeout=10,
        ).json()
    except Exception as exc:
        LOGGER.warning("CSE request failed: %s", exc)
        return "", ""

    for item in resp.get("items", []):
        url = item.get("link", "")
        try:
            html = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10).text
        except Exception:
            continue
        if not page_matches_agent(html, agent):
            continue
        phone = fmt_phone(html)
        email = ""
        if (m := EMAIL_RE.search(html)) and ok_email(m.group()):
            email = m.group()
        if phone or email:
            return phone, email
    return "", ""

# ── MAIN ───────────────────────────────────────────────────────────────────────
def process_rows(listings: List[Dict]) -> None:
    known = existing_phones()
    for row in listings:
        if not is_short_sale(row.get("description", "")):
            continue

        agent = row.get("agentName", "").strip()
        if not agent:
            continue

        phone, email = google_lookup(agent, row.get("state", ""))
        if not phone or phone in known:
            continue

        append_row([
            row.get("street", ""),
            row.get("city", ""),
            row.get("state", ""),
            row.get("zip", ""),
            agent,
            phone,
            email,
        ])
        known.add(phone)
        LOGGER.info("Saved row for %s – %s", agent, phone)

if __name__ == "__main__":
    LOGGER.info("bot_min ready – call process_rows(listings)")

