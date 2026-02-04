from __future__ import annotations

import asyncio
import importlib.util
import logging
import os
import random
from typing import Any, Dict, Iterable, Optional


HEADLESS_BROWSER_CACHE = os.getenv(
    "HEADLESS_BROWSER_CACHE",
    os.path.join(os.path.expanduser("~"), ".cache", "playwright"),
)
HEADLESS_BROWSER_DOWNLOAD = os.getenv("HEADLESS_BROWSER_DOWNLOAD", "true").lower() == "true"

_browser_ready = False
_browser_ready_lock = asyncio.Lock()


def headless_available() -> bool:
    return importlib.util.find_spec("playwright") is not None


async def ensure_headless_browser(logger: Optional[logging.Logger] = None) -> bool:
    global _browser_ready
    if _browser_ready:
        return True
    if not headless_available():
        if logger:
            logger.warning("HEADLESS_MISSING playwright not installed")
        return False
    async with _browser_ready_lock:
        if _browser_ready:
            return True
        os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", HEADLESS_BROWSER_CACHE)
        if not HEADLESS_BROWSER_DOWNLOAD:
            if logger:
                logger.warning("HEADLESS_BROWSER_DOWNLOAD_DISABLED cache=%s", HEADLESS_BROWSER_CACHE)
            return False
        if logger:
            logger.info("HEADLESS_BROWSER_DOWNLOAD starting cache=%s", HEADLESS_BROWSER_CACHE)
        if logger:
            logger.info("HEADLESS_BROWSER_DOWNLOAD_READY=%s", True)
        _browser_ready = True
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
    from playwright.async_api import async_playwright

    browser = None
    page = None
    content = ""
    visible_text = ""
    hrefs = []
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
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True, args=args)
            page = await browser.new_page()
            await page.set_extra_http_headers({"Accept-Language": accept_language})
            await page.set_viewport_size({"width": 1280, "height": 720})
            await page.set_user_agent(user_agent)
            if block_resources:
                async def _route_handler(route) -> None:
                    if route.request.resource_type in {"image", "media", "font", "stylesheet"}:
                        await route.abort()
                    else:
                        await route.continue_()

                await page.route("**/*", _route_handler)
            page.set_default_navigation_timeout(nav_timeout_ms)
            await page.goto(url, wait_until="domcontentloaded", timeout=nav_timeout_ms)
            await page.wait_for_timeout(wait_ms)
            try:
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await page.wait_for_timeout(350)
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
