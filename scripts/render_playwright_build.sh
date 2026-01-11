#!/usr/bin/env bash
set -euo pipefail

CANONICAL_PATH="/opt/render/project/.cache/ms-playwright"

printf 'PLAYWRIGHT_BROWSERS_PATH=%s\n' "${PLAYWRIGHT_BROWSERS_PATH:-}"
printf 'HOME=%s\n' "${HOME:-}"

mapfile -t found_dirs < <(find /opt/render -maxdepth 5 -type d -name ms-playwright -print || true)
printf '%s\n' "${found_dirs[@]}"

candidate_dirs=("${CANONICAL_PATH}")
if [[ -n "${PLAYWRIGHT_BROWSERS_PATH:-}" ]]; then
  candidate_dirs+=("${PLAYWRIGHT_BROWSERS_PATH}")
fi

for dir in "${found_dirs[@]}"; do
  candidate_dirs+=("${dir}")
done

for dir in "${candidate_dirs[@]}"; do
  ls -la "${dir}" || true
done

PLAYWRIGHT_BROWSERS_PATH="${CANONICAL_PATH}" python -m playwright install --with-deps chromium

if [[ ! -d "${CANONICAL_PATH}" ]]; then
  echo "Expected Playwright browsers path missing at ${CANONICAL_PATH}" >&2
  exit 1
fi

ls -la "${CANONICAL_PATH}"

PLAYWRIGHT_BROWSERS_PATH="${CANONICAL_PATH}" python - <<'PY'
from playwright.sync_api import sync_playwright
import os

print("SMOKE_TEST_PLAYWRIGHT_BROWSERS_PATH", os.environ.get("PLAYWRIGHT_BROWSERS_PATH"))
with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    print("SMOKE_TEST_EXECUTABLE", p.chromium.executable_path)
    browser.close()
PY
