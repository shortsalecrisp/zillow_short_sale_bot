import os, re, json, logging, requests
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

SHEET_ID   = os.environ["GSHEET_ID"]
SHEET_NAME = "Sheet1"
CS_API_KEY = os.environ["GS_CSE_KEY"]
CS_CX      = os.environ["GS_CSE_CX"]
SCOPES     = ["https://www.googleapis.com/auth/spreadsheets"]

logging.basicConfig(level=os.getenv("LOGLEVEL", "DEBUG"),
                    format="%(asctime)s %(levelname)s: %(message)s")
LOGGER = logging.getLogger("bot")

SHORT_SALE_RE = re.compile(r"\bshort\s+sale\b", re.I)
BAD_WORDS_RE  = re.compile(r"approved|negotiator|settlement fee|fee at closing", re.I)
PHONE_RE      = re.compile(r"(?<!\d)(?:\+?1[\s\-.]*)?\(?\d{3}\)?[\s\-.]*\d{3}[\s\-.]*\d{4}(?!\d)", re.X)
EMAIL_RE      = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.I)
IMAGE_EXTS    = (".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp")

def is_short_sale(text: str) -> bool:
    return bool(SHORT_SALE_RE.search(text)) and not BAD_WORDS_RE.search(text)

def fmt_phone(raw: str) -> str:
    digits = re.sub(r"\D", "", raw)
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    return f"{digits[:3]}-{digits[3:6]}-{digits[6:]}" if len(digits) == 10 else ""

def ok_email(addr: str) -> bool:
    return not addr.lower().endswith(IMAGE_EXTS)

def page_matches_agent(html: str, agent: str) -> bool:
    tokens = [t for t in agent.lower().split() if len(t) > 2]
    return all(tok in html.lower() for tok in tokens)

creds = Credentials.from_service_account_info(
    json.loads(os.environ["GSERVICE_ACCOUNT"]), scopes=SCOPES
)
svc = build("sheets", "v4", credentials=creds, cache_discovery=False)

def append_row(values: list[str]) -> int:
    resp = svc.spreadsheets().values().append(
        spreadsheetId=SHEET_ID,
        range=f"'{SHEET_NAME}'!A:G",
        valueInputOption="RAW",
        body={"values": [values]},
    ).execute()
    return int(resp["updates"]["updatedRange"].split("!A")[1].split(":")[0])

def phone_exists(phone: str) -> bool:
    col = svc.spreadsheets().values().get(
        spreadsheetId=SHEET_ID,
        range=f"'{SHEET_NAME}'!F1:F",
        majorDimension="COLUMNS",
        valueRenderOption="FORMATTED_VALUE",
    ).execute().get("values", [[]])[0]
    return phone in col

def delete_row(row_num: int):
    svc.spreadsheets().batchUpdate(
        spreadsheetId=SHEET_ID,
        body={
            "requests": [
                {
                    "deleteDimension": {
                        "range": {
                            "sheetId": 0,
                            "dimension": "ROWS",
                            "startIndex": row_num - 1,
                            "endIndex": row_num,
                        }
                    }
                }
            ]
        },
    ).execute()

def google_lookup(agent: str, state: str) -> tuple[str, str]:
    phone = email = ""
    params = {
        "key": CS_API_KEY,
        "cx": CS_CX,
        "q": f'"{agent}" {state} phone email',
        "num": 10,
    }
    try:
        resp = requests.get(
            "https://www.googleapis.com/customsearch/v1",
            params=params,
            timeout=10,
        ).json()
    except Exception as exc:
        LOGGER.warning("CSE fail: %s", exc)
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
        values = [
            row.get("street", ""),
            row.get("city", ""),
            row.get("state", ""),
            row.get("zip", ""),
            agent,
            phone,
            email,
        ]
        row_num = append_row(values)
        if phone and phone_exists(phone):
            delete_row(row_num)

