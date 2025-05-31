import os, re, json, sqlite3, logging, requests
from dotenv import load_dotenv
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from openai import OpenAI

load_dotenv()
OPENAI_MODEL   = "gpt-3.5-turbo-0125"
GOOGLE_API_KEY = os.getenv("GOOGLE_SEARCH_API_KEY")
GOOGLE_CSE_ID  = os.getenv("GOOGLE_CSE_ID")
APIFY_TOKEN    = os.getenv("APIFY_TOKEN", "")
SHEET_URL      = os.getenv("SHEET_URL")

client = OpenAI()

GSCOPE = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]
CREDS = ServiceAccountCredentials.from_json_keyfile_dict(
    json.loads(os.getenv("GOOGLE_CREDENTIALS_JSON")), GSCOPE
)
SHEET = gspread.authorize(CREDS).open_by_url(SHEET_URL).sheet1

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("bot_min")

PHONE_RE    = re.compile(r"\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}")
EMAIL_RE    = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
STRIP_TRAIL = re.compile(r"\b(TREC|DRE|Lic\.?|License)\b.*$", re.I)


def force_fetch_detail(zpid: str) -> str:
    try:
        resp = requests.post(
            "https://api.apify.com/v2/acts/apify~zillow-detail/run-sync-get-dataset-items",
            params={"token": APIFY_TOKEN},
            json={"zpid": zpid},
            timeout=30,
        ).json()
        if isinstance(resp, list) and resp:
            return resp[0].get("homeDescription", "").strip()
    except Exception:
        return ""
    return ""


def get_description(row: dict) -> str:
    text = (
        row.get("fullText")
        or row.get("homeDescription")
        or row.get("descriptionPlainText")
        or row.get("description")
        or ""
    ).strip()
    if text:
        return text
    zpid = row.get("zpid")
    if zpid:
        return force_fetch_detail(zpid)
    return ""


def google_lookup(agent: str, state: str, broker: str) -> tuple[str, str]:
    if not GOOGLE_API_KEY or not GOOGLE_CSE_ID:
        return "", ""
    queries = [
        f'"{agent}" {state} phone email',
        f'"{agent}" "{broker}" phone email' if broker else "",
    ]
    for q in filter(None, queries):
        params = {"key": GOOGLE_API_KEY, "cx": GOOGLE_CSE_ID, "q": q, "num": 10}
        try:
            resp = requests.get("https://www.googleapis.com/customsearch/v1", params=params, timeout=10).json()
        except Exception:
            continue
        for it in resp.get("items", []):
            html = requests.get(it.get("link", ""), timeout=10).text
            phone = PHONE_RE.search(html)
            email = EMAIL_RE.search(html)
            if phone or email:
                return phone.group() if phone else "", email.group() if email else ""
    if APIFY_TOKEN:
        try:
            resp = requests.post(
                "https://api.apify.com/v2/acts/drobnikj~realtor-agent-scraper/run-sync-get-dataset-items",
                params={"token": APIFY_TOKEN},
                json={"search": agent, "state": state},
                timeout=15,
            ).json()
            if isinstance(resp, list) and resp:
                rec = resp[0]
                return (
                    rec.get("mobilePhone") or rec.get("officePhone") or "",
                    rec.get("email") or "",
                )
        except Exception:
            pass
    return "", ""


def process_rows(rows):
    logger.info("START run – %d scraped rows", len(rows))
    conn = sqlite3.connect("seen.db")
    conn.execute("CREATE TABLE IF NOT EXISTS listings (zpid TEXT PRIMARY KEY)")
    conn.commit()
    try:
        next_row = len(SHEET.get_all_values()) + 1
    except Exception:
        next_row = 1
    for idx, row in enumerate(rows, 1):
        zpid = str(row.get("zpid", ""))
        if conn.execute("SELECT 1 FROM listings WHERE zpid=?", (zpid,)).fetchone():
            logger.info("%d/%d zpid %s already processed – skip", idx, len(rows), zpid)
            continue
        listing_text = get_description(row)
        logger.debug("zpid %s description length = %d", zpid, len(listing_text))
        if not listing_text:
            logger.warning("blank description: %s", zpid)
        prompt = (
            "Return YES if the following text contains the phrase 'short sale' "
            "(case-insensitive) and does NOT contain any of: approved, negotiator, "
            "settlement fee, fee at closing. Otherwise return NO.\n\n" + listing_text
        )
        try:
            result = client.chat.completions.create(
                model=OPENAI_MODEL,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.0,
            ).choices[0].message.content.strip()
        except Exception as e:
            logger.error("OpenAI error zpid %s: %s", zpid, e)
            continue
        logger.info("zpid %s OpenAI result %s", zpid, result)
        if not result.upper().startswith("YES"):
            continue
        agent = (
            row.get("listingProvider", {}).get("agents", [{}])[0].get("name")
            or row.get("listingAgentName")
            or row.get("listingAgent", {}).get("name")
            or row.get("agentName")
            or ""
        )
        agent = STRIP_TRAIL.sub("", agent).strip()
        if not agent:
            logger.warning("zpid %s no agent – skip", zpid)
            continue
        parts = agent.split()
        first, last = parts[0], " ".join(parts[1:]) if len(parts) > 1 else ""
        address = row.get("address") or row.get("addressStreet") or ""
        city    = row.get("addressCity") or ""
        st      = row.get("addressState") or row.get("state") or ""
        row_idx = next_row
        try:
            SHEET.append_row([first, last, "", "", address, city, st, "", "", ""])
            next_row += 1
        except Exception as e:
            logger.error("Sheets write failed: %s", e)
        phone, email = google_lookup(agent, st, row.get("brokerName", ""))
        logger.info("%s contact → phone:%s email:%s", zpid, phone, email)
        if phone or email:
            try:
                SHEET.update(f"C{row_idx}:D{row_idx}", [[phone, email]])
            except Exception as e:
                logger.error("Sheets write failed: %s", e)
        conn.execute("INSERT OR IGNORE INTO listings(zpid) VALUES(?)", (zpid,))
        conn.commit()
        logger.info("zpid %s marked processed", zpid)
    conn.close()
    logger.info("END run – processing complete")

