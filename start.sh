#!/usr/bin/env bash
set -euo pipefail

echo "STARTUP_SCRIPT_RUNNING"
echo "STARTUP_WHOAMI=$(whoami)"
echo "STARTUP_PWD=$(pwd)"

echo "STARTUP_HEADLESS_BROWSER_CACHE=${HEADLESS_BROWSER_CACHE:-}"
echo "STARTUP_HEADLESS_BROWSER_DOWNLOAD=${HEADLESS_BROWSER_DOWNLOAD:-true}"
echo "STARTUP_HEADLESS_BROWSER_STARTUP_CHECK=${HEADLESS_BROWSER_STARTUP_CHECK:-false}"

if [[ "${HEADLESS_BROWSER_STARTUP_CHECK:-false}" == "true" ]]; then
  echo "STARTUP_HEADLESS_BROWSER_CHECK starting"
  python - <<'PY'
import asyncio
import logging

from headless_browser import ensure_headless_browser, headless_available

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("startup")

if not headless_available():
    logger.warning("STARTUP_HEADLESS_BROWSER_MISSING playwright not installed")
else:
    asyncio.run(ensure_headless_browser(logger))
    logger.info("STARTUP_HEADLESS_BROWSER_READY")
PY
else
  echo "STARTUP_HEADLESS_BROWSER_CHECK skipped"
fi

echo "STARTUP_SERVICES starting webhook server"
uvicorn webhook_server:app --host 0.0.0.0 --port "${PORT:-10000}"
