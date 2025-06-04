import os, re, json, sqlite3, logging, time
from typing import Tuple, List

import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from openai import OpenAI
from dotenv import load_dotenv

# ───────────────────────────── env / globals ──────────────────────────────
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
CREDS  = ServiceAccountCredentials.from_json_keyfile_dict(
    json.loads(os.getenv("GOOGLE_CREDENTIALS_JSON")), GSCOPE
)
SHEET  = gspread.authorize(CREDS).open_by_url(SHEET_URL).sheet1

logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s %(levelname)s: %(message)s"
)
logger = logging.getLogger("bot_min")

PHONE_RE     = re.compile(r"\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}")
EMAIL_RE     = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
STRIP_TRAIL  = re.compile(r"\b(TREC|DRE|Lic\.?|License)\b.*$", re.I)

# ───────────────────────────── helpers 
def force_fetch_detail(zpid: str) -> str:
    """Fallback to Apify Zillow-detail actor for long descriptions."""
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
        pass
    return ""


def get_description(row: dict) -> str:
    """Return best-effort description string from many possible keys."""
    ft = row.get("fullText") or ""
    if ft.strip():
        return ft.strip()

    for key in ("homeDescription", "descriptionPlainText", "description"):
        val = (row.get(key) or "").strip()
        if val:
            return val

    detail = row.get("detail", {}) or {}
    nested = (detail.get("homeInfo", {}) or {}).get("homeDescription", "").strip()
    if nested:
        return nested

    hdp = row.get("hdpData", {}) or {}
    nested2 = (hdp.get("homeInfo", {}) or {}).get("homeDescription", "").strip()
    if nested2:
        return nested2

    return force_fetch_detail(str(row.get("zpid", "")))


def _format_phone(raw: str) -> str:
    """Convert any captured phone string to ###-###-####."""
    digits = re.sub(r"\D", "", raw)
    if len(digits) == 10:
        return f"{digits[0:3]}-{digits[3:6]}-{digits[6:]}"
    return raw  # leave untouched if unexpected length


def google_lookup(agent: str, state: str, broker: str) -> Tuple[str, str]:
    """
    Very lightweight contact scraper:
      • Google Custom Search → crawl top 10 pages.
      • Fallback to Apify realtor-agent-scraper.
    Returns (phone, email) – either may be empty.
    """
    if not GOOGLE_API_KEY or not GOOGLE_CSE_ID:
        logger.debug("google_lookup → Custom-search creds missing")
        return "", ""

    queries: List[str] = [
        f'"{agent}" {state} phone email',
        f'"{agent}" "{broker}" phone email' if broker else "",
    ]

    for q in filter(None, queries):
        logger.debug("google_lookup → query: %s", q)
        try:
            resp = requests.get(
                "https://www.googleapis.com/customsearch/v1",
                params={"key": GOOGLE_API_KEY, "cx": GOOGLE_CSE_ID, "q": q, "num": 10},
                timeout=10,
            ).json()
        except Exception as e:
            logger.error("google_lookup HTTP error %s: %s", q, e)
            continue

        for item in resp.get("items", []):
            url = item.get("link", "")
            logger.debug("google_lookup → checking %s", url)
            try:
                html = requests.get(url, timeout=10).text
            except Exception:
                continue

            phone_match  = PHONE_RE.search(html)
            email_match  = EMAIL_RE.search(html)

            phone = _format_phone(phone_match.group(0)) if phone_match else ""
            email = email_match.group(0) if email_match else ""   # ← bug fixed here
            if phone or email:
                logger.debug("  MATCH! %s | %s | %s", phone, email, url)
                return phone, email

    # Apify fallback
    if APIFY_TOKEN:
        logger.debug("google_lookup → fallback Apify agent-scraper")
        try:
            resp = requests.post(
                "https://api.apify.com/v2/acts/drobnikj~realtor-agent-scraper/run-sync-get-dataset-items",
                params={"token": APIFY_TOKEN},
                json={"search": agent, "state": state},
                timeout=15,
            ).json()
            if isinstance(resp, list) and resp:
                record = resp[0]
                phone  = _format_phone(
                    record.get("mobilePhone") or record.get("officePhone") or ""
                )
                email  = record.get("email") or ""
                return phone, email
        except Exception as e:
            logger.error("Apify agent-scraper error: %s", e)

    return "", ""


def _next_free_row() -> int:
    """Return 1-based index of the first completely empty row (col A)."""
    col_a = SHEET.col_values(1)  # col A values only
    return len(col_a) + 1


def _dedupe_by_phone(phone: str, current_row: int) -> None:
    """Delete current_row if phone already exists (excluding current row)."""
    if not phone:
        return
    existing = SHEET.col_values(3)  # phone column (C)
    matches  = [idx + 1 for idx, val in enumerate(existing) if val == phone]
    if len(matches) > 1 and current_row in matches:
        logger.info("Duplicate phone %s detected – deleting row %d", phone, current_row)
        SHEET.delete_rows(current_row)


# ───────────────────────────── main pipeline ──────────────────────────────
def process_rows(rows: List[dict]) -> None:
    logger.info("START run – %d scraped rows", len(rows))
    conn = sqlite3.connect("seen.db")
    conn.execute("CREATE TABLE IF NOT EXISTS listings (zpid TEXT PRIMARY KEY)")
    conn.commit()

    for idx, row in enumerate(rows, 1):
        zpid = str(row.get("zpid", ""))
        if conn.execute("SELECT 1 FROM listings WHERE zpid=?", (zpid,)).fetchone():
            continue

        # 1️⃣  Short-sale classifier
        listing_text = get_description(row)
        prompt = (
            "Return YES if the following text contains the phrase 'short sale' "
            "(case-insensitive) and does NOT contain any of: approved, negotiator, "
            "settlement fee, fee at closing. Otherwise return NO.\n\n"
            + listing_text
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

        if not result.upper().startswith("YES"):
            continue  # skip non-qualified listings

        # 2️⃣  Extract / normalise fields
        agent = (
            row.get("listingProvider", {}).get("agents", [{}])[0].get("name")
            or row.get("listingAgentName")
            or row.get("listingAgent", {}).get("name")
            or row.get("agentName")
            or ""
        )
        agent = STRIP_TRAIL.sub("", agent).strip()
        first, *rest = agent.split()
        last  = " ".join(rest)

        street = row.get("street", "")
        city   = row.get("city", "")
        st     = row.get("state", "")

        # 3️⃣  Contact enrichment (with single quick retry if phone empty)
        phone, email = google_lookup(agent, st, row.get("brokerName", ""))
        if not phone:
            time.sleep(1.0)
            phone, email = google_lookup(agent, st, row.get("brokerName", ""))

        # 4️⃣  Write to Sheet (always start at column A)
        sheet_row = [first, last, phone, email, street, city, st]
        try:
            row_idx = _next_free_row()
            SHEET.update(
                f"A{row_idx}:G{row_idx}",
                [sheet_row],
                value_input_option="RAW",
            )
            logger.debug("Sheet write → row %d OK", row_idx)
        except Exception as e:
            logger.error("Sheets write failed: %s", e)
            continue

        # 5️⃣  Dedupe by phone (remove duplicates, keep first occurrence)
        _dedupe_by_phone(phone, row_idx)

        # 6️⃣  Mark processed
        conn.execute("INSERT OR IGNORE INTO listings(zpid) VALUES(?)", (zpid,))
        conn.commit()

    conn.close()
    logger.info("END run – processing complete")

