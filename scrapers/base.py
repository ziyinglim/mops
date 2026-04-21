"""
Playwright session manager for TWSE MOPS scrapers.
Handles browser lifecycle, popup interception, and retry logic.
"""
import asyncio
import logging
from contextlib import asynccontextmanager
from playwright.async_api import async_playwright, Page, BrowserContext, Browser

logger = logging.getLogger(__name__)


class MOPSSession:
    def __init__(self, headless: bool = True, timeout_ms: int = 30000):
        self.headless = headless
        self.timeout_ms = timeout_ms
        self._playwright = None
        self._browser: Browser = None
        self._context: BrowserContext = None

    async def __aenter__(self):
        self._playwright = await async_playwright().start()
        self._browser = await self._playwright.chromium.launch(headless=self.headless)
        self._context = await self._browser.new_context(
            locale="zh-TW",
            timezone_id="Asia/Taipei",
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
        )
        self._context.set_default_timeout(self.timeout_ms)
        return self

    async def __aexit__(self, *args):
        for coro in [
            self._context.close() if self._context else None,
            self._browser.close() if self._browser else None,
            self._playwright.stop() if self._playwright else None,
        ]:
            if coro:
                try:
                    await coro
                except Exception:
                    pass

    async def new_page(self) -> Page:
        page = await self._context.new_page()
        return page

    async def wait_for_popup(self, page: Page, trigger_fn) -> Page:
        """Trigger an action and capture the resulting popup window."""
        async with page.expect_popup() as popup_info:
            await trigger_fn()
        popup = await popup_info.value
        await popup.wait_for_load_state("networkidle")
        return popup


async def switch_to_english(page: Page) -> None:
    """Click the EN language toggle if the page is currently in Chinese."""
    try:
        en_link = page.locator('span#Language:has-text("EN"), a#Language:has-text("EN"), [id="Language"]').first
        if await en_link.count() > 0:
            lang_text = (await en_link.inner_text()).strip()
            # If it shows "EN" it means clicking it switches TO English
            # If it shows "中文" the page is already in English
            if lang_text == "EN":
                await en_link.click()
                await page.wait_for_load_state("networkidle")
                logger.info("Switched page language to English")
    except Exception as exc:
        logger.warning("Language switch skipped: %s", exc)


async def with_retry(coro_fn, attempts: int = 3, delay: float = 5.0):
    """Retry an async callable up to `attempts` times on exception."""
    last_exc = None
    for i in range(attempts):
        try:
            return await coro_fn()
        except Exception as exc:
            last_exc = exc
            logger.warning("Attempt %d/%d failed: %s", i + 1, attempts, exc)
            if i < attempts - 1:
                await asyncio.sleep(delay)
    raise last_exc
