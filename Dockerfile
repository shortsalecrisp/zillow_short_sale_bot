FROM python:3.11-slim

ENV PLAYWRIGHT_BROWSERS_PATH=/ms-playwright \
    PIP_NO_CACHE_DIR=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt ./

RUN mkdir -p /ms-playwright \
    && pip install --no-cache-dir -r requirements.txt \
    && python -m playwright install --with-deps chromium \
    && python - <<'PY'
from playwright.sync_api import sync_playwright

with sync_playwright() as playwright:
    browser = playwright.chromium.launch(headless=True)
    browser.close()
PY

COPY . ./

CMD ["bash", "-c", "python bot_min.py & uvicorn webhook_server:app --host 0.0.0.0 --port ${PORT:-10000}"]
