#!/usr/bin/env python3
"""
Replay Zillow's GetSearchPageState request captured by seed_browser.py,
honouring the exact headers & cookies.  Prints #results; optional SMS.
"""

import json, os, random, sys, time, pathlib, logging, datetime as dt
from typing import Any

import httpx
from rich import print
from rich.logging import RichHandler
from dateutil import tz

AUTH_FILE = pathlib.Path("z_auth.json")
SEND_SMS = bool(os.getenv("SEND_SMS", ""))  # default False
SMS_API_KEY = os.getenv("SMS_API_KEY", "")
TZ = tz.gettz("US/Eastern")

logging.basicConfig(
    level="INFO",
    format="%(asctime)s  %(levelname)-7s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[RichHandler(markup=True, rich_tracebacks=True)],
)
log = logging.getLogger("poller")


def load_auth() -> dict[str, Any]:
    if not AUTH_FILE.exists():
        log.error("Auth file %s missing – run seed_browser.py first.", AUTH_FILE)
        sys.exit(1)
    return json.loads(AUTH_FILE.read_text())


def get_results(auth: dict[str, Any]) -> list[dict]:
    client = httpx.Client(
        headers=auth["headers"],
        cookies={c["name"]: c["value"] for c in auth["cookies"]},
        timeout=30,
        follow_redirects=True,
    )
    resp = client.get(auth["url"])
    resp.raise_for_status()
    data = resp.json()
    return (
        data["cat1"]["searchResults"]["mapResults"]
        if "cat1" in data
        else data["searchResults"]["mapResults"]
    )


def maybe_notify(msg: str) -> None:
    if not SEND_SMS:
        return
    try:
        httpx.post(
            "https://api.smsmobile.io/v1/messages",
            json={"to": "YOUR_NUMBER_HERE", "body": msg},
            headers={"Authorization": f"Bearer {SMS_API_KEY}"},
            timeout=10,
        ).raise_for_status()
    except Exception as e:
        log.warning("SMS send failed: %s", e)


def main(loop: bool, min_s: int, max_s: int) -> None:
    auth = load_auth()

    while True:
        try:
            log.info("Polling Zillow …")
            results = get_results(auth)
            count = len(results)
            log.info("[green]OK[/green] %s homes", count)
            # TODO: push to sheet / filter etc.

        except httpx.HTTPStatusError as e:
            status = e.response.status_code
            log.error("HTTP %s – need new auth?  Saving HTML.", status)
            with open("/tmp/z_fail.html", "wb") as fh:
                fh.write(e.response.content)
            maybe_notify(f"Zillow poll failed with {status}")
            if status in (403, 503):
                break  # let supervisor restart after you reseed
        except Exception as e:
            log.exception("Top-level error: %s", e)
            maybe_notify(f"Zillow poll crashed: {e}")

        if not loop:
            break
        sleep_for = random.randint(min_s, max_s)
        next_run = dt.datetime.now(tz=TZ) + dt.timedelta(seconds=sleep_for)
        log.info("Sleeping %d s — next run %s", sleep_for, next_run.strftime("%I:%M %p %Z"))
        time.sleep(sleep_for)


if __name__ == "__main__":
    import argparse, textwrap

    ap = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=textwrap.dedent(
            """\
            Zillow poller using recorded browser headers/cookies.
            Examples:
              python poller.py            # loop forever, 65–85 s jitter
              python poller.py --once     # single shot (good for tests)
              SEND_SMS=1 python poller.py --notify
            """
        ),
    )
    ap.add_argument("--once", action="store_true", help="Run a single iteration then exit")
    ap.add_argument("--notify", action="store_true", help="Actually send SMS notifications")
    ap.add_argument("--min", type=int, default=65, help="Min seconds between polls")
    ap.add_argument("--max", type=int, default=85, help="Max seconds between polls")
    args = ap.parse_args()

    if args.notify:
        SEND_SMS = True

    main(loop=not args.once, min_s=args.min, max_s=args.max)

