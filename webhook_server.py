from fastapi import FastAPI, Request
import os, sqlite3, json, logging

from apify_fetcher import fetch_rows
from bot_min import process_rows

# —————————————————————————————————————————————————————————————————————————————
#  CONFIG & SIMPLE LOGGING
# —————————————————————————————————————————————————————————————————————————————
REQUIRED_ENV_VARS = ["OPENAI_API_KEY", "SMSM_KEY", "SHEET_URL"]
missing = [v for v in REQUIRED_ENV_VARS if not os.getenv(v)]
if missing:
    raise RuntimeError(", ".join(missing))

DB_PATH = "seen.db"
TABLE_SQL = "CREATE TABLE IF NOT EXISTS listings (zpid TEXT PRIMARY KEY)"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("webhook_server")

app = FastAPI()
EXPORTED_ZPIDS: set[str] = set()


def ensure_table() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.execute(TABLE_SQL)
    return conn


# —————————————————————————————————————————————————————————————————————————————
#  HEALTHCHECK & EXPORT ENDPOINTS
# —————————————————————————————————————————————————————————————————————————————
@app.get("/")
def root():
    return {"status": "ok"}


@app.get("/export-zpids")
def export_zpids():
    conn = ensure_table()
    zpids = [row[0] for row in conn.execute("SELECT zpid FROM listings")]
    conn.close()
    EXPORTED_ZPIDS.update(zpids)
    return {"zpids": zpids}


@app.post("/reset-zpids")
def reset_zpids():
    conn = ensure_table()
    conn.execute("DELETE FROM listings")
    conn.commit()
    conn.close()
    EXPORTED_ZPIDS.clear()
    return {"status": "cleared"}


# —————————————————————————————————————————————————————————————————————————————
#  WEBHOOK: RECEIVE NEW LISTINGS FROM APIFY
# —————————————————————————————————————————————————————————————————————————————
@app.post("/apify-hook")
async def apify_hook(request: Request):
    """
    This endpoint now supports two payload types:
      1) { "dataset_id": "...someId..." }
         → we fetch those rows via fetch_rows(dataset_id)
      2) { "listings": [ {...}, {...}, ... ] }
         → we already have the full listing objects right here, so skip fetch_rows()
    """
    body = await request.json()
    logger.debug("Incoming webhook payload: %s", json.dumps(body))

    # Case A: if they POST a 'listings' array directly, use that
    if "listings" in body and isinstance(body["listings"], list):
        rows = body["listings"]
        logger.info("apify-hook: received %d listings directly in payload", len(rows))
    else:
        # Otherwise, fall back to fetching by dataset_id (the old approach)
        dataset_id = body.get("dataset_id") or request.query_params.get("dataset_id")
        if not dataset_id:
            logger.error("apify-hook: missing dataset_id and no listings array found")
            return {"error": "dataset_id missing and no listings provided"}
        # Grab every row from Apify’s dataset, unfiltered
        rows = fetch_rows(dataset_id)
        logger.info("apify-hook: fetched %d rows from dataset %s", len(rows), dataset_id)

    # Filter out any ZPIDs we've already processed
    fresh_rows = [r for r in rows if r.get("zpid") not in EXPORTED_ZPIDS]
    if not fresh_rows:
        logger.info("apify-hook: no fresh rows to process (all zpids already seen)")
        return {"status": "no new rows"}

    # Debug: print out the keys of the first item so we can confirm 'homeDescription' etc. arrived
    first_keys = list(fresh_rows[0].keys())
    logger.debug("Sample fields on first fresh row: %s", first_keys)

    # Now pass the full row dicts (including all description/detail fields) to process_rows()
    process_rows(fresh_rows)

    # Mark each ZPID as processed
    EXPORTED_ZPIDS.update(r.get("zpid") for r in fresh_rows)

    return {"status": "processed", "rows": len(fresh_rows)}


@app.get("/healthz")
def healthz():
    return {"status": "ok"}

