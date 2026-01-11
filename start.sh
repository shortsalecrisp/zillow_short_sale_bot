#!/usr/bin/env bash
set -euo pipefail

echo "STARTUP_SCRIPT_RUNNING"
echo "STARTUP_WHOAMI=$(whoami)"
echo "STARTUP_PWD=$(pwd)"

echo "STARTUP_PLAYWRIGHT_REMOTE_URL=${PLAYWRIGHT_REMOTE_URL:-}"
echo "STARTUP_PLAYWRIGHT_REMOTE_MODE=${PLAYWRIGHT_REMOTE_MODE:-cdp}"
echo "STARTUP_PLAYWRIGHT_BROWSERS_PATH=${PLAYWRIGHT_BROWSERS_PATH:-}"

echo "STARTUP_PLAYWRIGHT_CHROMIUM_CHECK starting"
set +e
python - <<'PY'
from pathlib import Path
from playwright.sync_api import sync_playwright

with sync_playwright() as p:
    chromium_path = Path(p.chromium.executable_path)

print(f"STARTUP_PLAYWRIGHT_CHROMIUM_PATH={chromium_path}")
print(f"STARTUP_PLAYWRIGHT_CHROMIUM_EXISTS={chromium_path.exists()}")
if not chromium_path.exists():
    raise SystemExit(10)
print("STARTUP_PLAYWRIGHT_CHROMIUM_OK")
PY
chromium_status=$?
set -e
if [ "${chromium_status}" -ne 0 ]; then
  echo "STARTUP_PLAYWRIGHT_CHROMIUM_INSTALL starting"
  python -m playwright install chromium
  echo "STARTUP_PLAYWRIGHT_CHROMIUM_INSTALL_DONE"
fi

if [ -n "${PLAYWRIGHT_REMOTE_URL:-}" ]; then
  echo "STARTUP_PLAYWRIGHT_REMOTE_SMOKE starting"
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
print("STARTUP_PLAYWRIGHT_REMOTE_SMOKE_OK")
PY
else
  echo "STARTUP_PLAYWRIGHT_REMOTE_SMOKE_SKIPPED"
fi

echo "STARTUP_SERVICES starting bot and webhook server"
python bot_min.py & uvicorn webhook_server:app --host 0.0.0.0 --port "${PORT:-10000}"
