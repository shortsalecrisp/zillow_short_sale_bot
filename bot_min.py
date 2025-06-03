"""
bot_min.py  –  Zillow short-sale scraper → Google Sheets

Changes in this version
-----------------------
* Explicit “A:G” write range => rows ALWAYS start in column A.
* Stricter PHONE_RE; normalise to  xxx-xxx-xxxx.
* Discard image-file “e-mails”.
* Skip writing if the phone already exists in column C.
"""

import os, re, json, sqlite3, logging, requests
from typing import Tuple, List

from dotenv import load_dotenv
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from openai import OpenAI

# ───────────────────────── env / globals ─────────────────────────
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

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("bot_min")

# ───────────────────────────── regex ─────────────────────────────
PHONE_RE = re.compile(
    r"""(?<!\d)(?:            # no digit just before
        \(\d{3}\)\s*\d{3}[-.\s]\d{4} |      # (123) 456-7890
        \d{3}[-.\s]\d{3}[-.\s]\d{4}         # 123-456-7890 / 123.456.7890
    )(?!\d)""",
    re.VERBOSE,
)
EMAIL_RE     = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
STRIP_TRAIL  = re.compile(r"\b(TREC|DRE|Lic\.?|License)\b.*$", re.I)
IMAGE_EXT_RE = re.compile(r"\.(?:png|jpe?g|gif|svg|webp)$", re.I)

# ───────────────────────── helpers ───────────────────────────────
def normalise_phone(raw: str) -> str:
    """Digits →  xxx-xxx-xxxx ; return '' if not 10 digits."""
    digits = re.sub(r"\D", "", raw)
    return f"{digits[0:3]}-{digits[3:6]}-{digits[6:]}" if len(digits) == 10 else ""


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
    for key in ("fullText", "homeDescription", "descriptionPlainText", "description"):
        val = (row.get(key) or "").strip()
        if val:
            return val

    detail = row.get("detail", {})
    home_info = detail.get("homeInfo", {}) if isinstance(detail, dict) else {}
    if home_desc := home_info.get("homeDescription", "").strip():
        return home_desc

    hdp = row.get("hdpData", {})
    home_info2 = hdp.get("homeInfo", {}) if isinstance(hdp, dict) else {}
    if home_desc2 := home_info2.get("homeDescription", "").strip():
        return home_desc2

    if zpid := row.get("zpid"):
        return force_fetch_detail(zpid)

    return ""


def google_lookup(agent: str, state: str, broker: str) -> Tuple[str, str]:
    """
    Return (phone, email) – both may be ''.
    Performs up to two Google CSE passes, then optional Apify fallback.
    """
    if not GOOGLE_API_KEY or not GOOGLE_CSE_ID:
        logger.debug("google_lookup → custom-search creds missing")
        return "", ""

    queries = [
        f'"{agent}" {state} phone email',
        f'"{agent}" "{broker}" phone email' if broker else "",
    ]

    for q in filter(None, queries):
        params = {"key": GOOGLE_API_KEY, "cx": GOOGLE_CSE_ID, "q": q, "num": 10}
        try:
            resp = requests.get(
                "https://www.googleapis.com/customsearch/v1", params=params, timeout=10
            ).json()
        except Exception as e:
            logger.error("google_lookup HTTP error for %s: %s", q, e)
            continue

        for it in resp.get("items", []):
            url = it.get("link", "")
            try:
                html = requests.get(url, timeout=10).text
            except Exception:
                continue

            phone_match = PHONE_RE.search(html)
            email_match = EMAIL_RE.search(html)

            phone = normalise_phone(phone_match.group()) if phone_match else ""
            email = email_match.group("") if email_match else ""

            if email and IMAGE_EXT_RE.search(email):
                email = ""  # throw away *.png etc.

            if phone or email:
                return phone, email

    # --- fallback: Apify realtor-agent-scraper ---
    if APIFY_TOKEN:
        try:
            resp = requests.post(
                "https://api.apify.com/v2/acts/drobnikj~realtor-agent-scraper/run-sync-get-dataset-items",
                params={"token": APIFY_TOKEN},
                json={"search": agent, "state": state},
                timeout=15,
            ).json()
            if isinstance(resp, list) and resp:
                rec   = resp[0]
                phone = normalise_phone(rec.get("mobilePhone") or rec.get("officePhone") or "")
                email = rec.get("email") or ""
                return phone, email
        except Exception as e:
            logger.error("Apify agent scraper error: %s", e)

    return "", ""


def first_empty_row() -> int:
    """Index (1-based) of the first completely empty row in column A."""
    col_a = SHEET.col_values(1)          # non-blank cells only
    return len(col_a) + 1


def phone_in_sheet(clean_digits: str) -> bool:
    """True if phone (digits only) already exists in column C."""
    existing = {re.sub(r"\D", "", p) for p in SHEET.col_values(3)}
    return clean_digits in existing


# ─────────────────────────── pipeline ────────────────────────────
def process_rows(rows: List[dict]) -> None:
    logger.info("START run – %d scraped rows", len(rows))

    conn = sqlite3.connect("seen.db")
    conn.execute("CREATE TABLE IF NOT EXISTS listings (zpid TEXT PRIMARY KEY)")
    conn.commit()

    for idx, row in enumerate(rows, 1):
        zpid = str(row.get("zpid", ""))
        if conn.execute("SELECT 1 FROM listings WHERE zpid=?", (zpid,)).fetchone():
            continue  # already processed earlier

        # ---------- OpenAI filter ----------
        listing_text = get_description(row)
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

        if not result.upper().startswith("YES"):
            continue

        # ---------- Build 7-column row ----------
        agent = (
            row.get("listingProvider", {}).get("agents", [{}])[0].get("name")
            or row.get("listingAgentName")
            or row.get("listingAgent", {}).get("name")
            or row.get("agentName")
            or ""
        )
        agent = STRIP_TRAIL.sub("", agent).strip()
        first, *last_parts = agent.split()
        last = " ".join(last_parts)

        street = row.get("street", "")
        city   = row.get("city", "")
        st     = row.get("state", "")

        # ---------- Phone / email enrichment ----------
        phone, email = google_lookup(agent, st, row.get("brokerName", ""))

        # clean/format phone one more time
        phone_digits = re.sub(r"\D", "", phone)
        phone_formatted = normalise_phone(phone) if phone_digits else ""

        if phone_in_sheet(phone_digits):
            logger.info("Phone %s already seen – skipping write", phone_formatted)
            # still mark zpid so we don't re-process forever
            conn.execute("INSERT OR IGNORE INTO listings(zpid) VALUES(?)", (zpid,))
            conn.commit()
            continue

        # ---------- Write to sheet (explicit A:G range) ----------
        sheet_row = [
            first.strip(),
            last.strip(),
            phone_formatted,
            email.strip(),
            street.strip(),
            city.strip(),
            st.strip(),
        ]

        row_idx = first_empty_row()
        try:
            SHEET.update(f"A{row_idx}:G{row_idx}", [sheet_row])
            logger.info("Sheet write → row %s OK", row_idx)
        except Exception as e:
            logger.error("Sheets write failed: %s", e)
            continue

        # ---------- mark processed ----------
        conn.execute("INSERT OR IGNORE INTO listings(zpid) VALUES(?)", (zpid,))
        conn.commit()

    conn.close()
    logger.info("END run – processing complete")

