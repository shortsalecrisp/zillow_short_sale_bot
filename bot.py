#!/usr/bin/env python3
import argparse
import json
import logging
import os
import pickle
import random
import sqlite3
import time
from datetime import datetime, timedelta
from urllib.parse import urlparse, parse_qs, urlencode

import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import smtplib
from email.message import EmailMessage

# --- Configuration paths ---
BASE_DIR = os.path.dirname(__file__)
CONFIG_PATH = os.path.join(BASE_DIR, 'config.json')
COOKIES_PATH = os.path.join(BASE_DIR, 'cookies.pkl')
DB_PATH = os.path.join(BASE_DIR, 'seen.db')
SERVICE_ACCOUNT = os.path.join(BASE_DIR, 'service_account.json')

# --- Logging setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-5s  %(message)s"
)
log = logging.getLogger(__name__)


def load_config():
    with open(CONFIG_PATH) as f:
        return json.load(f)


def build_session(cfg):
    sess = requests.Session()
    # pick a random, ASCII-only User-Agent
    ua = random.choice(cfg.get('user_agents', []))
    ua = ua.encode('latin-1', 'ignore').decode('latin-1')
    sess.headers.update({
        'User-Agent': ua,
        'Referer': cfg['zillow_url'],
        'Accept': 'application/json'
    })
    # load cookies if seeded
    if os.path.exists(COOKIES_PATH):
        with open(COOKIES_PATH, 'rb') as f:
            try:
                cookies = pickle.load(f)
                sess.cookies.update(cookies)
            except Exception:
                log.warning("Could not load cookies.pkl, continuing without cookies")
    return sess


def get_api_url(zillow_url):
    parsed = urlparse(zillow_url)
    qs = parse_qs(parsed.query)
    sq = qs.get('searchQueryState')
    if not sq:
        raise ValueError("searchQueryState parameter missing in zillow_url")
    searchQueryState = sq[0]
    wants = json.dumps({"cat1": ["mapResults"]})
    wants_enc = urlencode({'wants': wants})
    return f"{parsed.scheme}://{parsed.netloc}/search/GetSearchPageState.htm?searchQueryState={searchQueryState}&{wants_enc}&requestId=1"


def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute('CREATE TABLE IF NOT EXISTS seen(zpid TEXT PRIMARY KEY)')
    conn.commit()
    return conn


def poll_once(cfg):
    sess = build_session(cfg)
    api_url = get_api_url(cfg['zillow_url'])
    log.info("Fetching Zillow JSON…")
    resp = sess.get(api_url, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    conn = init_db()
    cur = conn.cursor()
    new_listings = []

    for r in data.get('cat1', {}).get('searchResults', {}).get('mapResults', []):
        zpid = str(r.get('zpid'))
        if cur.execute('SELECT 1 FROM seen WHERE zpid=?', (zpid,)).fetchone():
            continue
        desc = r.get('statusText', '').lower()
        if cfg['must_include'].lower() not in desc:
            continue
        if any(p.lower() in desc for p in cfg.get('disallowed_phrases', [])):
            continue

        home = r.get('hdpData', {}).get('homeInfo', {})
        if home.get('homeType') not in cfg.get('allowed_types', []):
            continue

        address = home.get('address', '')
        state = address.split(',')[-1].strip() if ',' in address else ''
        if state in cfg.get('disallowed_states', []):
            continue

        # passed all filters
        new_listings.append(r)
        cur.execute('INSERT INTO seen(zpid) VALUES (?)', (zpid,))

    conn.commit()
    conn.close()

    if not new_listings:
        log.info("No new listings.")
        return []

    # --- Append to Google Sheet ---
    creds = ServiceAccountCredentials.from_json_keyfile_name(
        SERVICE_ACCOUNT,
        ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    )
    client = gspread.authorize(creds)
    sheet = client.open_by_key(cfg['sheet_id']).worksheet(cfg['sheet_tab'])

    rows = []
    for r in new_listings:
        hi = r.get('hdpData', {}).get('homeInfo', {})
        rows.append([
            datetime.now().isoformat(),
            hi.get('address', ''),
            r.get('detailUrl', ''),
            r.get('zpid', ''),
            hi.get('price', ''),
            hi.get('bedrooms', ''),
            hi.get('bathrooms', ''),
            hi.get('livingArea', '')
        ])
    for row in rows:
        sheet.append_row(row, value_input_option='RAW')

    # --- Send email notification ---
    if cfg.get('email_enabled'):
        msg = EmailMessage()
        msg['Subject'] = f"{len(new_listings)} new Zillow listings"
        msg['From'] = cfg['email_from']
        msg['To'] = cfg['email_to']
        body = "New listings:\n" + "\n".join(r.get('detailUrl', '') for r in new_listings)
        msg.set_content(body)
        with smtplib.SMTP(cfg['smtp_host'], cfg['smtp_port']) as smtp:
            smtp.starttls()
            smtp.login(cfg['smtp_user'], cfg['smtp_pass'])
            smtp.send_message(msg)

    log.info(f"Pushed {len(new_listings)} new listings.")
    return new_listings

def process_rows(rows):
    """
    Called by webhook_server to handle a list of Zillow rows.
    Replace the body with your real dedupe → Sheets → SMS logic.
    """
    for row in rows:
        print("Got row", row.get("zpid", "n/a"))


def main():
    cfg = load_config()
    parser = argparse.ArgumentParser(description='Zillow short sale monitor')
    parser.add_argument('--once', action='store_true', help='Run once and exit')
    args = parser.parse_args()

    if args.once:
        poll_once(cfg)
    else:
        while True:
            poll_once(cfg)
            interval = random.randint(
                int(cfg['poll_interval_min'] * 0.8),
                int(cfg['poll_interval_min'] * 1.2)
            )
            next_run = datetime.now() + timedelta(minutes=interval)
            log.info(f"Sleeping until {next_run.strftime('%I:%M %p')}")
            time.sleep(interval * 60)


if __name__ == '__main__':
    main()

