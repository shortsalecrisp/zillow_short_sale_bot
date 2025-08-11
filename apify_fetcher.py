import os
import requests
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# Grab your Apify token
APIFY_TOKEN = os.getenv("APIFY_TOKEN")
HEADERS = {"Authorization": f"Bearer {APIFY_TOKEN}"} if APIFY_TOKEN else {}

def fetch_rows(dataset_id: str) -> list[dict]:
    """
    Fetch all items from an Apify dataset.
    """
    url = f"https://api.apify.com/v2/datasets/{dataset_id}/items"
    params = {"format": "json", "clean": 1}
    response = requests.get(url, params=params, headers=HEADERS)
    response.raise_for_status()
    return response.json()

