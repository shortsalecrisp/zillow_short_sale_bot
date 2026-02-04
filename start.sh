#!/usr/bin/env bash
set -euo pipefail

echo "STARTUP_SCRIPT_RUNNING"
echo "STARTUP_WHOAMI=$(whoami)"
echo "STARTUP_PWD=$(pwd)"

echo "STARTUP_HEADLESS_BROWSER_CACHE=${HEADLESS_BROWSER_CACHE:-}"
echo "STARTUP_HEADLESS_BROWSER_DOWNLOAD=${HEADLESS_BROWSER_DOWNLOAD:-true}"

echo "STARTUP_HEADLESS_BROWSER_CHECK starting"
python - <<'PY'
import asyncio
import logging

from headless_browser import ensure_headless_browser, headless_available

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("startup")

if not headless_available():
    logger.warning("STARTUP_HEADLESS_BROWSER_MISSING pyppeteer not installed")
else:
    asyncio.run(ensure_headless_browser(logger))
    logger.info("STARTUP_HEADLESS_BROWSER_READY")
PY

echo "STARTUP_SERVICES starting bot and webhook server"
python bot_min.py & uvicorn webhook_server:app --host 0.0.0.0 --port "${PORT:-10000}"
