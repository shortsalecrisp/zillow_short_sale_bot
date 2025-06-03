# bot_min.py  –  write one clean 7-column row per listing (Option A)
# -----------------------------------------------------------------------------
import os, re, json, sqlite3, logging, requests
from dotenv import load_dotenv
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from openai import OpenAI

# ---------- env / globals ----------
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

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("bot_min")

PHONE_RE    = re.compile(r"\b(?:\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b")
EMAIL_RE    = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
STRIP_TRAIL = re.compile(r"\b(TREC|DRE|Lic\.?|License)\b.*$", re.I)


# ---------- helpers ----------
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
        pass
    return ""


def get_description(row: dict) -> str:
    logger.debug("DEBUG: raw row keys = %s", list(row.keys()))

    for key in (
        "fullText",
        "homeDescription",
        "descriptionPlainText",
        "description",
        ("detail", "homeInfo", "homeDescription"),
        ("hdpData", "homeInfo", "homeDescription"),
    ):
        if isinstance(key, tuple):
            obj = row
            for k in key:
                obj = obj.get(k, {}) if isinstance(obj, dict) else {}
            txt = (obj or "").strip() if isinstance(obj, str) else ""
        else:
            txt = (row.get(key) or "").strip()
        if txt:
            return txt

    zpid = row.get("zpid")
    return force_fetch_detail(zpid) if zpid else ""


def google_lookup(agent: str, state: str, broker: str) -> tuple[str, str]:
    """Return (phone,email) – at most *one* first hit – with verbose diagnostics."""
    if not agent or not GOOGLE_API_KEY or not GOOGLE_CSE_ID:
        return "", ""

    queries = [
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
            logger.error("google_lookup HTTP error for %s: %s", q, e)
            continue

        for it in resp.get("items", []):
            url = it.get("link", "")
            logger.debug("google_lookup → checking %s", url)
            try:
                html = requests.get(url, timeout=10).text
            except Exception as e:
                logger.debug("  fetch failed: %s", e)
                continue

            phone_match = PHONE_RE.search(html)
            email_match = EMAIL_RE.search(html)
            if phone_match or email_match:
                phone = phone_match.group().strip() if phone_match else ""
                email = email_match.group().strip() if email_match else ""
                logger.debug("  MATCH! phone:%s email:%s source:%s", phone, email, url)
                return phone, email

    # fallback – Apify realtor scraper
    if APIFY_TOKEN:
        logger.debug("google_lookup → fallback to Apify agent scraper")
        try:
            resp = requests.post(
                "https://api.apify.com/v2/acts/drobnikj~realtor-agent-scraper/run-sync-get-dataset-items",
                params={"token": APIFY_TOKEN},
                json={"search": agent, "state": state},
                timeout=15,
            ).json()
            if isinstance(resp, list) and resp:
                rec   = resp[0]
                phone = rec.get("mobilePhone") or rec.get("officePhone") or ""
                email = rec.get("email") or ""
                if phone or email:
                    return phone, email
        except Exception as e:
            logger.error("Apify agent scraper error: %s", e)

    return "", ""


# ---------- main pipeline ----------
def process_rows(rows):
    logger.info("START run – %d scraped rows", len(rows))

    # local dedupe cache
    conn = sqlite3.connect("seen.db")
    conn.execute("CREATE TABLE IF NOT EXISTS listings (zpid TEXT PRIMARY KEY)")
    conn.commit()

    # TRUE “next row” = first empty in *column A* (header assumed on row 1)
    def next_free_row() -> int:
        return len(SHEET.col_values(1)) + 1  # col_values(1) returns A-column list

    for idx, row in enumerate(rows, 1):
        zpid = str(row.get("zpid", ""))
        if conn.execute("SELECT 1 FROM listings WHERE zpid=?", (zpid,)).fetchone():
            logger.info("%d/%d zpid %s already processed – skip", idx, len(rows), zpid)
            continue

        # ---------------- text filter ----------------
        listing_text = get_description(row)
        if not listing_text:
            continue

        prompt = (
            "Return YES if the following text contains the phrase 'short sale' "
            "(case-insensitive) and does NOT contain any of: approved, negotiator, "
            "settlement fee, fee at closing. Otherwise return NO.\n\n" + listing_text
        )
        try:
            answer = client.chat.completions.create(
                model=OPENAI_MODEL,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.0,
            ).choices[0].message.content.strip()
        except Exception as e:
            logger.error("OpenAI error zpid %s: %s", zpid, e)
            continue
        logger.info("zpid %s OpenAI result %s", zpid, answer)
        if not answer.upper().startswith("YES"):
            continue

        # ---------------- build sheet row ----------------
        agent_raw = (
            row.get("listingProvider", {}).get("agents", [{}])[0].get("name")
            or row.get("listingAgentName")
            or row.get("listingAgent", {}).get("name")
            or row.get("agentName")
            or ""
        )
        agent = STRIP_TRAIL.sub("", agent_raw).strip()
        first, last = (agent.split(maxsplit=1) + [""])[:2]

        sheet_row = [
            first,
            last,
            "",  # phone placeholder C
            "",  # email placeholder D
            row.get("street", ""),
            row.get("city", ""),
            row.get("state", ""),
        ]

        # ---------------- append row atomically ----------------
        row_idx = next_free_row()
        try:
            SHEET.update(f"A{row_idx}:G{row_idx}", [sheet_row], value_input_option="RAW")
            logger.debug("Sheet write → row %d OK", row_idx)
        except Exception as e:
            logger.error("Sheets append failed row %d: %s", row_idx, e)
            continue  # if we can't write, skip enrichment

        # ---------------- enrich phone / email ----------------
        phone, email = google_lookup(agent, sheet_row[6], row.get("brokerName", ""))
        logger.info("%s contact → phone:%s email:%s", zpid, phone, email)
        if phone or email:
            try:
                SHEET.update(
                    f"C{row_idx}:D{row_idx}", [[phone, email]], value_input_option="RAW"
                )
            except Exception as e:
                logger.error("Sheets update failed row %d: %s", row_idx, e)

        # ---------------- mark processed locally ----------------
        conn.execute("INSERT OR IGNORE INTO listings(zpid) VALUES(?)", (zpid,))
        conn.commit()

    conn.close()
    logger.info("END run – processing complete")

