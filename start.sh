#!/usr/bin/env bash
set -u

export PLAYWRIGHT_BROWSERS_PATH=/tmp/ms-playwright

mkdir -p "${PLAYWRIGHT_BROWSERS_PATH}"

echo "PLAYWRIGHT_START browsers_path=${PLAYWRIGHT_BROWSERS_PATH}"

find_chromium() {
  find "${PLAYWRIGHT_BROWSERS_PATH}" -type f \( -name 'chromium*' -o -name 'chrome*' \) -print -quit 2>/dev/null
}

install_playwright() {
  echo "PLAYWRIGHT_INSTALL attempting chromium download"
  python -m playwright install chromium
}

chromium_path="$(find_chromium || true)"
if [[ -z "${chromium_path}" ]]; then
  if ! install_playwright; then
    echo "PLAYWRIGHT_INSTALL retrying chromium download"
    install_playwright || echo "PLAYWRIGHT_INSTALL_FAILED continuing without crash"
  fi
else
  echo "PLAYWRIGHT_INSTALL chromium already present at ${chromium_path}"
fi

echo "PLAYWRIGHT_SMOKE starting"
if python - <<'PY'
from playwright.sync_api import sync_playwright
with sync_playwright() as p:
    b = p.chromium.launch(headless=True)
    b.close()
print("PLAYWRIGHT_SMOKE_OK")
PY
then
  :
else
  echo "PLAYWRIGHT_SMOKE_FAIL continuing startup"
fi

python bot_min.py & uvicorn webhook_server:app --host 0.0.0.0 --port "${PORT:-10000}"
