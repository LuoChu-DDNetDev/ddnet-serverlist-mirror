"""Cloudflare challenge solver built on nodriver (needs a headed browser / GUI server).

Only solves the JS challenge and returns cookie + UA state; it never proxies
the actual data (Service A replays the request via curl_cffi with the cookies).
"""

from __future__ import annotations

import asyncio
import logging
import time

import nodriver as uc

logger = logging.getLogger(__name__)

CF_COOKIES = ("cf_clearance", "__cf_bm")


async def _all_cookies(browser: uc.Browser) -> list:
    """List every cookie known to the browser (nodriver API shim for two variants)."""
    cookiejar = browser.cookies
    try:
        return await cookiejar.all()
    except TypeError:
        return await cookiejar.get_all()


async def _stop(browser: uc.Browser) -> None:
    try:
        stop = browser.stop()
        if hasattr(stop, "__await__"):
            await stop
    except Exception:  # noqa: BLE001 - best effort shutdown
        pass


async def _user_agent(browser: uc.Browser) -> str:
    ua = getattr(browser, "user_agent", None)
    if callable(ua):
        ua = ua()
    if hasattr(ua, "__await__"):
        ua = await ua
    return str(ua or "")


async def solve_cf(url: str, get_browser, timeout: float = 30.0, settle: float = 2.0) -> dict:
    """Drive the browser to clear the challenge and harvest cf_clearance cookies.

    Returns {"cookies": {name: value}, "user_agent": str, "expires": epoch|None}.
    Raises TimeoutError if no CF cookie appears within `timeout` seconds.
    """
    browser = await get_browser()
    await browser.get(url)
    deadline = time.monotonic() + timeout
    cookies: dict[str, str] = {}
    last_all: list = []

    while time.monotonic() < deadline:
        last_all = await _all_cookies(browser)
        cookies = {c.name: c.value for c in last_all}
        if any(name in cookies for name in CF_COOKIES):
            break
        await asyncio.sleep(0.5)

    if not any(cookies.get(name) for name in CF_COOKIES):
        raise TimeoutError(f"no Cloudflare cookie after {timeout:.0f}s for {url}")

    if settle:
        await asyncio.sleep(settle)
        last_all = await _all_cookies(browser)
        cookies = {c.name: c.value for c in last_all}

    expires = None
    for c in last_all:
        if c.name == "cf_clearance" and getattr(c, "expires", None):
            expires = float(c.expires)
            break

    return {
        "cookies": cookies,
        "user_agent": await _user_agent(browser),
        "expires": expires,
    }


class BrowserManager:
    """Lazily started, reusable browser instance (kept alive across solves)."""

    def __init__(self, headless: bool = False) -> None:
        self.headless = headless
        self.browser: uc.Browser | None = None
        self._lock = asyncio.Lock()

    async def get(self) -> uc.Browser:
        async with self._lock:
            if self.browser is None:
                logger.info("starting nodriver browser (headless=%s)", self.headless)
                self.browser = await uc.start(headless=self.headless)
            return self.browser

    async def stop(self) -> None:
        async with self._lock:
            if self.browser is not None:
                await _stop(self.browser)
                self.browser = None