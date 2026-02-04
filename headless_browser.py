from __future__ import annotations

import asyncio
import logging
import os
import random
from typing import Any, Dict, Iterable, Optional

try:
    from pyppeteer import launch
    from pyppeteer.chromium_downloader import check_chromium, download_chromium
except ImportError:  # pragma: no cover - optional dependency
    launch = None
    check_chromium = None
    download_chromium = None


HEADLESS_BROWSER_CACHE = os.getenv(
    "HEADLESS_BROWSER_CACHE",
    os.path.join(os.path.expanduser("~"), ".cache", "pyppeteer"),
)
HEADLESS_BROWSER_DOWNLOAD = os.getenv("HEADLESS_BROWSER_DOWNLOAD", "true").lower() == "true"

_browser_ready = False
_browser_ready_lock = asyncio.Lock()


def headless_available() -> bool:
    return launch is not None


async def ensure_headless_browser(logger: Optional[logging.Logger] = None) -> bool:
    global _browser_ready
    if _browser_ready:
        return True
    if not headless_available():
        if logger:
            logger.warning("HEADLESS_MISSING pyppeteer not installed")
        return False
    async with _browser_ready_lock:
        if _browser_ready:
            return True
        os.environ.setdefault("PYPPETEER_HOME", HEADLESS_BROWSER_CACHE)
        if check_chromium and check_chromium():
            _browser_ready = True
            return True
        if not HEADLESS_BROWSER_DOWNLOAD:
            if logger:
                logger.warning("HEADLESS_BROWSER_DOWNLOAD_DISABLED cache=%s", HEADLESS_BROWSER_CACHE)
            return False
        if logger:
            logger.info("HEADLESS_BROWSER_DOWNLOAD starting cache=%s", HEADLESS_BROWSER_CACHE)
        if download_chromium:
            download_chromium()
        _browser_ready = bool(check_chromium and check_chromium())
        if logger:
            logger.info("HEADLESS_BROWSER_DOWNLOAD_READY=%s", _browser_ready)
        return _browser_ready


async def fetch_headless_snapshot(
    url: str,
    *,
    proxy_url: str = "",
    nav_timeout_ms: int,
    wait_ms: int,
    user_agent: str,
    accept_language: str,
    block_resources: bool = True,
    logger: Optional[logging.Logger] = None,
) -> Dict[str, Any]:
    if not headless_available():
        return {}
    browser = None
    page = None
    try:
        args = [
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--disable-gpu",
            "--disable-setuid-sandbox",
            "--no-zygote",
            "--disable-background-networking",
            "--disable-default-apps",
            "--disable-extensions",
        ]
        if proxy_url:
            args.append(f"--proxy-server={proxy_url}")
        browser = await launch(headless=True, args=args)
        page = await browser.newPage()
        await page.setUserAgent(user_agent)
        await page.setExtraHTTPHeaders({"Accept-Language": accept_language})
        if block_resources:
            await page.setRequestInterception(True)

            async def _handle_request(req) -> None:
                if req.resourceType in {"image", "media", "font", "stylesheet"}:
                    await req.abort()
                else:
                    await req.continue_()

            page.on("request", _handle_request)
        await page.setDefaultNavigationTimeout(nav_timeout_ms)
        await page.goto(url, waitUntil="domcontentloaded", timeout=nav_timeout_ms)
        await page.waitForTimeout(wait_ms)
        try:
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await page.waitForTimeout(350)
        except Exception:
            pass
        content = await page.content()
        visible_text = await page.evaluate("() => document.body ? document.body.innerText : ''")
        hrefs = await page.evaluate(
            "() => Array.from(document.querySelectorAll('a[href]')).map(a => a.getAttribute('href') || '')"
        )
        hrefs = hrefs or []
        mailto_links = [h for h in hrefs if h and h.lower().startswith("mailto:")]
        tel_links = [h for h in hrefs if h and h.lower().startswith("tel:")]
        return {
            "html": content or "",
            "visible_text": visible_text or "",
            "mailto_links": mailto_links,
            "tel_links": tel_links,
            "final_url": page.url if page else url,
        }
    except Exception as exc:
        if logger:
            logger.warning("HEADLESS_BROWSER_ERROR url=%s err=%s", url, exc)
        return {}
    finally:
        if page:
            try:
                await page.close()
            except Exception:
                pass
        if browser:
            try:
                await browser.close()
            except Exception:
                pass


def random_accept_language(pool: Iterable[str]) -> str:
    values = list(pool)
    if not values:
        return "en-US,en;q=0.9"
    return random.choice(values)
