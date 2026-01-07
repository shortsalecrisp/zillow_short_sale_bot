import os
from pathlib import Path
import sqlite3
import requests
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# Grab your Apify token
APIFY_TOKEN = os.getenv("APIFY_TOKEN")
HEADERS = {"Authorization": f"Bearer {APIFY_TOKEN}"} if APIFY_TOKEN else {}

# Track dataset offsets in a shared SQLite database so multiple
# processes can coordinate which rows have already been fetched.
DB_PATH = Path("seen.db")


def fetch_rows(dataset_id: str) -> list[dict]:
    """Fetch only new items from an Apify dataset.

    The number of items already retrieved is stored in the ``dataset_progress``
    table within ``seen.db`` keyed by dataset_id. On each call we request items
    starting at that offset and update the stored offset so the next call only
    pulls freshly appended records.
    """

    with sqlite3.connect(DB_PATH) as conn:
        # Ensure the table exists
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS dataset_progress (
                dataset_id TEXT PRIMARY KEY,
                offset INTEGER
            )
            """
        )

        cur = conn.execute(
            "SELECT offset FROM dataset_progress WHERE dataset_id=?",
            (dataset_id,),
        )
        row = cur.fetchone()
        offset = row[0] if row else 0

        url = f"https://api.apify.com/v2/datasets/{dataset_id}/items?format=json&clean=1&offset=0"
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        items = response.json()

        # persist new offset so next call only fetches subsequent rows
        if items:
            new_offset = offset + len(items)
            conn.execute(
                """
                INSERT INTO dataset_progress (dataset_id, offset)
                VALUES (?, ?)
                ON CONFLICT(dataset_id) DO UPDATE SET offset=excluded.offset
                """,
                (dataset_id, new_offset),
            )
            conn.commit()

    return items
