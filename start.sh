#!/usr/bin/env bash
set -euo pipefail

echo "STARTUP_SCRIPT_RUNNING"
echo "STARTUP_WHOAMI=$(whoami)"
echo "STARTUP_PWD=$(pwd)"

if [ -z "${PLAYWRIGHT_BROWSERS_PATH:-}" ]; then
  export PLAYWRIGHT_BROWSERS_PATH="/tmp/ms-playwright"
  echo "STARTUP_PLAYWRIGHT_BROWSERS_PATH_DEFAULTED=${PLAYWRIGHT_BROWSERS_PATH}"
else
  export PLAYWRIGHT_BROWSERS_PATH
  echo "STARTUP_PLAYWRIGHT_BROWSERS_PATH_EXISTING=${PLAYWRIGHT_BROWSERS_PATH}"
fi

mkdir -p "${PLAYWRIGHT_BROWSERS_PATH}"
echo "STARTUP_PLAYWRIGHT_BROWSERS_PATH=${PLAYWRIGHT_BROWSERS_PATH}"
ls -la "${PLAYWRIGHT_BROWSERS_PATH}"

echo "STARTUP_PLAYWRIGHT_INSTALL_WITH_DEPS starting chromium download"
if python -m playwright install --with-deps chromium; then
  echo "STARTUP_PLAYWRIGHT_INSTALL_WITH_DEPS_OK"
else
  echo "STARTUP_PLAYWRIGHT_INSTALL_WITH_DEPS_FAILED"
  echo "STARTUP_PLAYWRIGHT_INSTALL_RETRY starting chromium download without deps"
  if python -m playwright install chromium; then
    echo "STARTUP_PLAYWRIGHT_INSTALL_RETRY_OK"
  else
    echo "STARTUP_PLAYWRIGHT_INSTALL_RETRY_FAILED"
  fi
fi

python - <<'PY'
from playwright.sync_api import sync_playwright
with sync_playwright() as p:
    print("STARTUP_PLAYWRIGHT_CHROMIUM_EXECUTABLE", p.chromium.executable_path)
PY

echo "STARTUP_PLAYWRIGHT_SMOKE starting"
python - <<'PY'
from playwright.sync_api import sync_playwright
with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    browser.close()
print("STARTUP_PLAYWRIGHT_SMOKE_OK")
PY

echo "STARTUP_SERVICES starting bot and webhook server"
python bot_min.py & uvicorn webhook_server:app --host 0.0.0.0 --port "${PORT:-10000}"
