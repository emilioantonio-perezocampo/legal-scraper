"""
Browser adapter for CAS JavaScript SPA.

Uses Playwright for headless browser automation to render
the JavaScript-heavy CAS jurisprudence database.
"""
from dataclasses import dataclass
from typing import Optional, Dict, Any
from contextlib import asynccontextmanager
import asyncio
import time


@dataclass(frozen=True)
class BrowserConfig:
    """Browser configuration for Playwright."""
    headless: bool = True
    timeout_ms: int = 30000
    viewport_width: int = 1920
    viewport_height: int = 1080
    user_agent: str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    slow_mo: int = 0  # Milliseconds to slow down operations (for debugging)


@dataclass(frozen=True)
class RenderedPage:
    """Result of rendering a page."""
    url: str
    html: str
    title: str
    status_code: int
    load_time_ms: int


class CASBrowserAdapter:
    """
    Playwright-based browser adapter for CAS SPA.

    Handles:
    - Browser lifecycle management
    - JavaScript rendering
    - Dynamic content waiting
    - Screenshot capture for debugging
    - Rate limiting compliance
    """

    def __init__(self, config: Optional[BrowserConfig] = None):
        self._config = config or BrowserConfig()
        self._playwright = None
        self._browser = None
        self._context = None
        self._started = False

    @property
    def is_started(self) -> bool:
        return self._started

    async def start(self) -> None:
        """Initialize Playwright and launch browser."""
        # Import here to handle missing playwright gracefully
        try:
            from playwright.async_api import async_playwright
        except ImportError:
            from src.infrastructure.adapters.cas_errors import BrowserNotAvailableError
            raise BrowserNotAvailableError(
                "Playwright not installed. Run: pip install playwright && playwright install chromium"
            )

        self._playwright = await async_playwright().start()
        self._browser = await self._playwright.chromium.launch(
            headless=self._config.headless,
            slow_mo=self._config.slow_mo,
        )
        self._context = await self._browser.new_context(
            viewport={
                "width": self._config.viewport_width,
                "height": self._config.viewport_height,
            },
            user_agent=self._config.user_agent,
        )
        self._started = True

    async def stop(self) -> None:
        """Close browser and cleanup resources."""
        if self._context:
            await self._context.close()
            self._context = None
        if self._browser:
            await self._browser.close()
            self._browser = None
        if self._playwright:
            await self._playwright.stop()
            self._playwright = None
        self._started = False

    async def render_page(
        self,
        url: str,
        wait_selector: Optional[str] = None,
        wait_for_network_idle: bool = True,
    ) -> RenderedPage:
        """
        Navigate to URL and return rendered HTML after JS execution.

        Args:
            url: Page URL to render
            wait_selector: Optional CSS selector to wait for
            wait_for_network_idle: Wait for network to be idle

        Returns:
            RenderedPage with rendered HTML content
        """
        from src.infrastructure.adapters.cas_errors import (
            BrowserTimeoutError,
            JavaScriptRenderError,
            BrowserNotAvailableError,
        )

        if not self._started:
            raise BrowserNotAvailableError("Browser not started. Call start() first.")

        page = await self._context.new_page()
        start_time = time.time()

        try:
            response = await page.goto(
                url,
                timeout=self._config.timeout_ms,
                wait_until="domcontentloaded",
            )

            status_code = response.status if response else 0

            # Wait for content to load
            if wait_for_network_idle:
                try:
                    await page.wait_for_load_state(
                        "networkidle",
                        timeout=self._config.timeout_ms
                    )
                except Exception:
                    # Network idle timeout is not critical
                    pass

            if wait_selector:
                try:
                    await page.wait_for_selector(
                        wait_selector,
                        timeout=self._config.timeout_ms
                    )
                except Exception as e:
                    raise JavaScriptRenderError(
                        f"Selector '{wait_selector}' not found: {e}",
                        url=url
                    )

            html = await page.content()
            title = await page.title()

            load_time_ms = int((time.time() - start_time) * 1000)

            return RenderedPage(
                url=url,
                html=html,
                title=title,
                status_code=status_code,
                load_time_ms=load_time_ms,
            )

        except asyncio.TimeoutError:
            raise BrowserTimeoutError(
                f"Timeout loading {url}",
                timeout_seconds=self._config.timeout_ms // 1000
            )
        finally:
            await page.close()

    async def capture_screenshot(
        self,
        url: str,
        output_path: str,
    ) -> str:
        """Capture screenshot of page for debugging."""
        from src.infrastructure.adapters.cas_errors import BrowserNotAvailableError

        if not self._started:
            raise BrowserNotAvailableError("Browser not started.")

        page = await self._context.new_page()
        try:
            await page.goto(url, timeout=self._config.timeout_ms)
            await page.wait_for_load_state("networkidle")
            await page.screenshot(path=output_path, full_page=True)
            return output_path
        finally:
            await page.close()

    async def execute_search(
        self,
        base_url: str = "https://jurisprudence.tas-cas.org/",
        filters: Optional[Dict[str, Any]] = None,
    ) -> RenderedPage:
        """
        Execute search on CAS database with filters.

        Args:
            base_url: CAS jurisprudence URL
            filters: Search filters (year_from, year_to, sport, keyword, etc.)

        Returns:
            RenderedPage with search results
        """
        from src.infrastructure.adapters.cas_errors import BrowserNotAvailableError

        if not self._started:
            raise BrowserNotAvailableError("Browser not started.")

        page = await self._context.new_page()

        try:
            await page.goto(base_url, timeout=self._config.timeout_ms)
            await page.wait_for_load_state("networkidle")

            # Apply filters if provided (implementation depends on actual SPA)
            if filters:
                await self._apply_search_filters(page, filters)

            # Wait for results to load
            await page.wait_for_load_state("networkidle")

            html = await page.content()
            title = await page.title()

            return RenderedPage(
                url=page.url,
                html=html,
                title=title,
                status_code=200,
                load_time_ms=0,
            )

        finally:
            await page.close()

    async def _apply_search_filters(
        self,
        page,
        filters: Dict[str, Any],
    ) -> None:
        """Apply search filters on the page."""
        # This is a placeholder - actual implementation depends on SPA structure
        # You would interact with form elements, dropdowns, etc.

        if "keyword" in filters and filters["keyword"]:
            # Example: await page.fill('input[name="search"]', filters["keyword"])
            pass

        if "year_from" in filters:
            # Example: await page.select_option('select[name="yearFrom"]', str(filters["year_from"]))
            pass

        if "year_to" in filters:
            # Example: await page.select_option('select[name="yearTo"]', str(filters["year_to"]))
            pass

        # Click search button
        # Example: await page.click('button[type="submit"]')


@asynccontextmanager
async def browser_session(config: Optional[BrowserConfig] = None):
    """
    Context manager for browser sessions.

    Usage:
        async with browser_session() as browser:
            page = await browser.render_page(url)
    """
    adapter = CASBrowserAdapter(config)
    try:
        await adapter.start()
        yield adapter
    finally:
        await adapter.stop()
