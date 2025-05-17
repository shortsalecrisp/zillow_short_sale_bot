import os, requests

APIFY_TOKEN = os.getenv("APIFY_TOKEN")

def fetch_rows(dataset_id: str) -> list[dict]:
    url = (
        f"https://api.apify.com/v2/datasets/{dataset_id}"
        "/items?format=json&clean=1"
    )
    r = requests.get(
        url,
        headers={"Authorization": f"Bearer {APIFY_TOKEN}"},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()
