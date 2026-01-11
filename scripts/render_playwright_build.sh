#!/usr/bin/env bash
set -euo pipefail

printf 'PLAYWRIGHT_REMOTE_URL=%s\n' "${PLAYWRIGHT_REMOTE_URL:-}"
printf 'PLAYWRIGHT_REMOTE_MODE=%s\n' "${PLAYWRIGHT_REMOTE_MODE:-cdp}"

if [[ -n "${PLAYWRIGHT_REMOTE_URL:-}" ]]; then
  python - <<'PY'
import os
from playwright.sync_api import sync_playwright

remote_url = os.environ.get("PLAYWRIGHT_REMOTE_URL", "")
mode = os.environ.get("PLAYWRIGHT_REMOTE_MODE", "cdp").lower()

with sync_playwright() as p:
    if mode == "playwright":
        browser = p.chromium.connect(remote_url)
    else:
        browser = p.chromium.connect_over_cdp(remote_url)
    browser.close()
print("SMOKE_TEST_REMOTE_OK")
PY
else
  echo "SMOKE_TEST_REMOTE_SKIPPED (PLAYWRIGHT_REMOTE_URL missing)"
fi
