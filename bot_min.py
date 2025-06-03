import os, re, json, sqlite3, logging, time, requests
from dotenv import load_dotenv
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from openai import OpenAI

# ─────────────────────────── ENV / GLOBALS ────────────────────────────────
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

PHONE_RE    = re.compile(r"\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}")
EMAIL_RE    = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
STRIP_TRAIL = re.compile(r"\b(TREC|DRE|Lic\.?|License)\b.*$", re.I)

# ─────────────────────────── HELPERS 
──────────────────────────────────────
def force_fetch_detail(zpid: str) -> str:
    """Last-ditch Zillow detail call if description missing."""
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
    ):
        val = (row.get(key) or "").strip()
        if val:
            return val

    # nested blobs
    for container in ("detail", "hdpData"):
        blob = row.get(container, {})
        home_info = blob.get("homeInfo", {}) if isinstance(blob, dict) else {}
        val = (home_info.get("homeDescription") or "").strip()
        if val:
            return val

    zpid = row.get("zpid")
    return force_fetch_detail(zpid) if zpid else ""


def google_lookup(agent: str, state: str, broker: str) -> tuple[str, str]:
    """
    Best-effort scrape for phone/email.
    - keeps the first decent email seen (best_email)
    - keeps retrying queries until a phone number surfaces or we exhaust list
    Falls back to Apify realtor-agent-scraper if Google CSE fails.
    """
    if not GOOGLE_API_KEY or not GOOGLE_CSE_ID:
        logger.debug("google_lookup → custom-search creds missing")
        return "", ""

    def run_query(q: str) -> tuple[str, str]:
        params = {"key": GOOGLE_API_KEY, "cx": GOOGLE_CSE_ID, "q": q, "num": 10}
        try:
            resp = requests.get(
                "https://www.googleapis.com/customsearch/v1",
                params=params,
                timeout=10,
            ).json()
        except Exception as e:
            logger.error("google_lookup HTTP error for %s: %s", q, e)
            return "", ""

        for it in resp.get("items", []):
            url = it.get("link", "")
            logger.debug("google_lookup → checking %s", url)
            try:
                html = requests.get(url, timeout=10).text
            except Exception as e:
                logger.debug("  fetch failed: %s", e)
                continue

            phone = PHONE_RE.search(html)
            email = EMAIL_RE.search(html)
            if phone or email:
                logger.debug(
                    "  MATCH! phone:%s email:%s source:%s",
                    phone.group() if phone else "",
                    email.group() if email else "",
                    url,
                )
                return phone.group() if phone else "", email.group() if email else ""
        return "", ""

    queries = [
        f'"{agent}" {state} phone email',
        f'"{agent}" "{broker}" phone email' if broker else "",
    ]

    best_email = ""
    for q in filter(None, queries):
        phone, email = run_query(q)

        if email and not best_email:
            best_email = email

        if phone:                       # success → stop early
            return phone, email or best_email

        time.sleep(0.7)                 # polite pause

    # fallback to Apify scraper
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
                rec = resp[0]
                phone = rec.get("mobilePhone") or rec.get("officePhone") or ""
                email = rec.get("email") or best_email
                if phone or email:
                    logger.debug(
                        "  MATCH! phone:%s email:%s (Apify agent scraper)", phone, email
                    )
                return phone, email
        except Exception as e:
            logger.error("Apify agent scraper error: %s", e)

    logger.debug("google_lookup → no contact found for %s", agent)
    return "", best_email  # might still have an email


# ─────────────────────────── MAIN PIPELINE ────────────────────────────────
def process_rows(rows):
    logger.info("START run – %d scraped rows", len(rows))
    conn = sqlite3.connect("seen.db")
    conn.execute("CREATE TABLE IF NOT EXISTS listings (zpid TEXT PRIMARY KEY)")
    conn.commit()

    # Track current row count once (header row assumed at A1)
    existing_rows = len(SHEET.get_all_values())

    for idx, row in enumerate(rows, 1):
        zpid = str(row.get("zpid", ""))
        if conn.execute("SELECT 1 FROM listings WHERE zpid=?", (zpid,)).fetchone():
            logger.info("%d/%d zpid %s already processed – skip", idx, len(rows), zpid)
            continue

        listing_text = get_description(row)
        logger.debug("zpid %s description length = %d", zpid, len(listing_text))

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

        # ---------------- agent & address fields ----------------
        agent = (
            row.get("listingProvider", {}).get("agents", [{}])[0].get("name")
            or row.get("listingAgentName")
            or row.get("listingAgent", {}).get("name")
            or row.get("agentName")
            or ""
        )
        agent = STRIP_TRAIL.sub("", agent).strip()

        parts = agent.split()
        first, last = parts[0], " ".join(parts[1:]) if len(parts) > 1 else ""

        street = row.get("street", "")
        city   = row.get("city", "")
        st     = row.get("state", "")

        sheet_row = [first, last, "", "", street, city, st]

        # ---------------- append to Sheet ----------------------
        next_row_idx = existing_rows + 1
        try:
            SHEET.update(
                f"A{next_row_idx}:G{next_row_idx}",
                [sheet_row],
                value_input_option="RAW",
            )
            logger.debug("Sheet write → row %d OK", next_row_idx)
            existing_rows += 1
        except Exception as e:
            logger.error("Sheets write failed: %s", e)
            continue  # skip contact lookup if base row failed

        # ---------------- phone/email enrichment ----------------
        phone, email = google_lookup(agent, st, row.get("brokerName", ""))
        logger.info("%s contact → phone:%s email:%s", zpid, phone, email)
        if phone or email:
            try:
                SHEET.update(
                    f"C{next_row_idx}:D{next_row_idx}", [[phone, email]], value_input_option="RAW"
                )
            except Exception as e:
                logger.error("Sheets write failed: %s", e)

        # ---------------- mark processed ------------------------
        conn.execute("INSERT OR IGNORE INTO listings(zpid) VALUES(?)", (zpid,))
        conn.commit()
        logger.info("zpid %s marked processed", zpid)

    conn.close()
    logger.info("END run – processing complete")

