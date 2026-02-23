from __future__ import annotations

import asyncio
import glob
import importlib.util
import logging
import os
import random
import subprocess
import sys
from typing import Any, Dict, Iterable, List, Optional


HEADLESS_BROWSER_CACHE = os.getenv(
    "HEADLESS_BROWSER_CACHE",
    os.path.join(os.path.expanduser("~"), ".cache", "playwright"),
)
HEADLESS_BROWSER_DOWNLOAD = os.getenv("HEADLESS_BROWSER_DOWNLOAD", "true").lower() == "true"

_browser_ready = False
_browser_ready_lock = asyncio.Lock()


def headless_available() -> bool:
    return importlib.util.find_spec("playwright") is not None


def _has_chromium_executable(cache_dir: str) -> bool:
    if not cache_dir:
        return False
    patterns = (
        "chromium-*/chrome-linux/headless_shell",
        "chromium-*/chrome-linux/chrome",
        "chromium_headless_shell-*/chrome-linux/headless_shell",
    )
    for pattern in patterns:
        if glob.glob(os.path.join(cache_dir, pattern)):
            return True
    return False


def chromium_available(cache_dir: Optional[str] = None) -> bool:
    cache_dir = cache_dir or HEADLESS_BROWSER_CACHE
    return _has_chromium_executable(cache_dir)


def _install_chromium(cache_dir: str, logger: Optional[logging.Logger]) -> bool:
    if not headless_available():
        return False
    env = os.environ.copy()
    if cache_dir:
        env.setdefault("PLAYWRIGHT_BROWSERS_PATH", cache_dir)
    cmd = [sys.executable, "-m", "playwright", "install", "chromium"]
    try:
        result = subprocess.run(cmd, env=env, check=False, capture_output=True, text=True)
    except Exception as exc:
        if logger:
            logger.warning("HEADLESS_BROWSER_INSTALL_FAILED err=%s", exc)
        return False
    if result.returncode != 0:
        if logger:
            logger.warning(
                "HEADLESS_BROWSER_INSTALL_FAILED code=%s stderr=%s",
                result.returncode,
                (result.stderr or "").strip(),
            )
        return False
    return _has_chromium_executable(cache_dir)


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
        if logger:
            logger.info(
                "HEADLESS_BROWSER_CACHE path=%s chromium_ready=%s download=%s",
                HEADLESS_BROWSER_CACHE,
                chromium_available(HEADLESS_BROWSER_CACHE),
                HEADLESS_BROWSER_DOWNLOAD,
            )
        if not HEADLESS_BROWSER_DOWNLOAD:
            if logger:
                logger.warning("HEADLESS_BROWSER_DOWNLOAD_DISABLED cache=%s", HEADLESS_BROWSER_CACHE)
            return False
        if not _has_chromium_executable(HEADLESS_BROWSER_CACHE):
            if logger:
                logger.info("HEADLESS_BROWSER_DOWNLOAD starting cache=%s", HEADLESS_BROWSER_CACHE)
            if not _install_chromium(HEADLESS_BROWSER_CACHE, logger):
                if logger:
                    logger.warning("HEADLESS_BROWSER_DOWNLOAD_FAILED cache=%s", HEADLESS_BROWSER_CACHE)
                return False
        if logger:
            logger.info(
                "HEADLESS_BROWSER_DOWNLOAD_READY=%s chromium_ready=%s",
                True,
                chromium_available(HEADLESS_BROWSER_CACHE),
            )
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
    extra_headers: Optional[Dict[str, str]] = None,
    cookies: Optional[List[Dict[str, Any]]] = None,
    block_resources: bool = True,
    logger: Optional[logging.Logger] = None,
) -> Dict[str, Any]:
    if not headless_available():
        return {}
    from playwright.async_api import async_playwright

    browser = None
    page = None
    context = None
    route_enabled = False
    content = ""
    visible_text = ""
    hrefs = []
    final_url = url
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
            browser = await pw.chromium.launch(headless=True, args=args, timeout=nav_timeout_ms)
            headers = {"Accept-Language": accept_language}
            if extra_headers:
                headers.update({k: v for k, v in extra_headers.items() if v})
            context = await browser.new_context(
                user_agent=user_agent,
                extra_http_headers=headers,
                viewport={"width": 1280, "height": 720},
            )
            if cookies:
                try:
                    await context.add_cookies(cookies)
                except Exception as exc:
                    if logger:
                        logger.warning("HEADLESS_COOKIE_ERROR url=%s err=%s", url, exc)
            page = await context.new_page()
            page.set_default_timeout(nav_timeout_ms)
            page.set_default_navigation_timeout(nav_timeout_ms)
            if block_resources:
                async def _route_handler(route) -> None:
                    try:
                        if route.request.resource_type in {"image", "media", "font", "stylesheet"}:
                            await route.abort()
                        else:
                            await route.continue_()
                    except Exception as exc:
                        # Route callbacks can still run while the page/context is
                        # shutting down after timeout cancellation.
                        if logger:
                            logger.debug("HEADLESS_ROUTE_IGNORED url=%s err=%s", url, exc)

                await page.route("**/*", _route_handler)
                route_enabled = True
            response = await page.goto(url, wait_until="domcontentloaded", timeout=nav_timeout_ms)
            if response and response.status in {403, 429, 451}:
                if logger:
                    logger.warning(
                        "HEADLESS_BLOCKED_RESPONSE url=%s status=%s",
                        url,
                        response.status,
                    )
                return {}
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
            final_url = page.url
        hrefs = hrefs or []
        mailto_links = [h for h in hrefs if h and h.lower().startswith("mailto:")]
        tel_links = [h for h in hrefs if h and h.lower().startswith("tel:")]
        return {
            "html": content or "",
            "visible_text": visible_text or "",
            "mailto_links": mailto_links,
            "tel_links": tel_links,
            "final_url": final_url,
        }
    except Exception as exc:
        if logger:
            logger.warning("HEADLESS_BROWSER_ERROR url=%s err=%s", url, exc)
        return {}
    finally:
        if page and route_enabled:
            try:
                await page.unroute_all(behavior="ignoreErrors")
            except Exception:
                pass
        if page:
            try:
                await page.close()
            except Exception:
                pass
        if context:
            try:
                await context.close()
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
