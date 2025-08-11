import os
import json
from pathlib import Path
import requests
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# Grab your Apify token
APIFY_TOKEN = os.getenv("APIFY_TOKEN")
HEADERS = {"Authorization": f"Bearer {APIFY_TOKEN}"} if APIFY_TOKEN else {}

# Track how many items we've already pulled for each dataset so we only
# request newly appended rows on subsequent runs.
PROGRESS_FILE = Path("dataset_progress.json")


def fetch_rows(dataset_id: str) -> list[dict]:
    """Fetch only new items from an Apify dataset.

    The number of items already retrieved is stored in ``dataset_progress.json``
    keyed by dataset_id. On each call we request items starting at that offset
    and update the stored offset so the next call only pulls freshly appended
    records.
    """

    # load previous offsets
    progress = {}
    if PROGRESS_FILE.exists():
        try:
            progress = json.loads(PROGRESS_FILE.read_text())
        except Exception:
            progress = {}

    offset = progress.get(dataset_id, 0)

    url = f"https://api.apify.com/v2/datasets/{dataset_id}/items"
    params = {"format": "json", "clean": 1, "offset": offset}
    response = requests.get(url, params=params, headers=HEADERS)
    response.raise_for_status()
    items = response.json()

    # persist new offset so next call only fetches subsequent rows
    if items:
        progress[dataset_id] = offset + len(items)
        PROGRESS_FILE.write_text(json.dumps(progress))

    return items

