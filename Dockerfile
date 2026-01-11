FROM python:3.11-slim

ENV PLAYWRIGHT_BROWSERS_PATH=/opt/render/project/.cache/ms-playwright \
    PIP_NO_CACHE_DIR=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt ./

RUN mkdir -p /opt/render/project/.cache/ms-playwright \
    && pip install --no-cache-dir -r requirements.txt \
    && python -m playwright install --with-deps chromium \
    && python -c "from playwright.sync_api import sync_playwright; p=sync_playwright().start(); b=p.chromium.launch(headless=True); b.close(); p.stop()"

COPY . ./

CMD ["bash", "-c", "python bot_min.py & uvicorn webhook_server:app --host 0.0.0.0 --port ${PORT:-10000}"]
