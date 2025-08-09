import os
import sqlite3
import requests
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# Grab your Apify token
APIFY_TOKEN = os.getenv("APIFY_TOKEN")
HEADERS = {"Authorization": f"Bearer {APIFY_TOKEN}"} if APIFY_TOKEN else {}

DB_PATH = "seen.db"


def fetch_rows(dataset_id: str) -> list[dict]:
    """Fetch only new items from an Apify dataset using a persisted offset."""
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        "CREATE TABLE IF NOT EXISTS dataset_offsets (dataset_id TEXT PRIMARY KEY, offset INTEGER)"
    )
    off_row = conn.execute(
        "SELECT offset FROM dataset_offsets WHERE dataset_id=?", (dataset_id,)
    ).fetchone()
    offset = off_row[0] if off_row else 0

    url = f"https://api.apify.com/v2/datasets/{dataset_id}/items"
    params = {"format": "json", "clean": 1, "offset": offset}
    response = requests.get(url, params=params, headers=HEADERS)
    response.raise_for_status()
    items = response.json()

    conn.execute(
        "INSERT OR REPLACE INTO dataset_offsets (dataset_id, offset) VALUES (?,?)",
        (dataset_id, offset + len(items)),
    )
    conn.commit()
    conn.close()
    return items

