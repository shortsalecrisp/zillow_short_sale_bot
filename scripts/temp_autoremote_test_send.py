#!/usr/bin/env python3
"""
TEMPORARY AUTOREMOTE DIAGNOSTIC TEST UTILITY.
Remove this file after AutoRemote outbound SMS is confirmed working.

Usage:
  python scripts/temp_autoremote_test_send.py --phone 15551234567 --type initial
"""

import argparse
import logging

from sms_providers import get_sender


def main() -> None:
    parser = argparse.ArgumentParser(description="Temporary AutoRemote send diagnostic")
    parser.add_argument("--phone", required=True, help="Test phone number (digits preferred)")
    parser.add_argument(
        "--type",
        default="initial",
        choices=["initial", "followup"],
        help="Message type sent in smsbot payload",
    )
    parser.add_argument(
        "--message",
        default="Temporary AutoRemote diagnostic test from Zillow bot.",
        help="Test message body",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    sender = get_sender()
    result = sender.send_with_diagnostics(
        args.phone,
        args.message,
        sms_type=args.type,
        row_idx="TEMP_TEST",
        attempt=1,
    )
    print("AUTOREMOTE_TEST_RESULT", result)
    print("AUTOREMOTE_TEST_REQUEST_PREVIEW", result.payload_preview)
    if result.success:
        print("SUCCESS: AutoRemote personal URL GET accepted (HTTP 200 + no body error signal)")
    else:
        print("FAILURE: AutoRemote personal URL GET not accepted (status/body validation failed)")


if __name__ == "__main__":
    main()
