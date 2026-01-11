#!/usr/bin/env bash
set -euo pipefail

echo "STARTUP_SCRIPT_RUNNING"

export PLAYWRIGHT_BROWSERS_PATH="${PLAYWRIGHT_BROWSERS_PATH:-/tmp/ms-playwright}"

mkdir -p "${PLAYWRIGHT_BROWSERS_PATH}"

echo "PLAYWRIGHT_BROWSERS_PATH=${PLAYWRIGHT_BROWSERS_PATH}"
ls -la "${PLAYWRIGHT_BROWSERS_PATH}"

echo "PLAYWRIGHT_INSTALL starting chromium download"
if ! python -m playwright install chromium; then
  echo "PLAYWRIGHT_INSTALL retrying with --with-deps"
  python -m playwright install --with-deps chromium
fi

python - <<'PY'
from playwright.sync_api import sync_playwright
with sync_playwright() as p:
    print("PLAYWRIGHT_CHROMIUM_EXECUTABLE", p.chromium.executable_path)
PY

echo "PLAYWRIGHT_SMOKE starting"
python - <<'PY'
from playwright.sync_api import sync_playwright
with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    browser.close()
print("PLAYWRIGHT_SMOKE_OK")
PY

python bot_min.py & uvicorn webhook_server:app --host 0.0.0.0 --port "${PORT:-10000}"
