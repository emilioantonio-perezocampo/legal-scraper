"""
SCJN Browser Adapter

Uses Playwright for headless browser automation to handle
the JavaScript-heavy SCJN legislacion search interface.
"""
from dataclasses import dataclass
from typing import Optional, Dict, Any
from contextlib import asynccontextmanager
import asyncio
import time
import logging

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SCJNBrowserConfig:
    """Browser configuration for SCJN Playwright automation."""
    headless: bool = True
    timeout_ms: int = 30000
    viewport_width: int = 1920
    viewport_height: int = 1080
    user_agent: str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    slow_mo: int = 0


@dataclass(frozen=True)
class SCJNRenderedPage:
    """Result of rendering an SCJN page."""
    url: str
    html: str
    title: str
    status_code: int
    load_time_ms: int
    result_count: int = 0


class SCJNBrowserAdapter:
    """
    Playwright-based browser adapter for SCJN search.

    Handles:
    - Browser lifecycle management
    - Form interaction for search filters
    - Waiting for JavaScript to render results
    - Pagination handling
    """

    SEARCH_URL = "https://legislacion.scjn.gob.mx/Buscador/Paginas/Buscar.aspx"

    # Element selectors for the search form
    SELECTORS = {
        "search_input": "#ctl00_MainContentPlaceHolder_ucBusqueda1_txtPalabra",
        "category_dropdown": "#ctl00_MainContentPlaceHolder_ucBusqueda1_ddlOrdenamiento",
        "scope_dropdown": "#ctl00_MainContentPlaceHolder_ucBusqueda1_ddlAmbito",
        "entity_dropdown": "#ctl00_MainContentPlaceHolder_ucBusqueda1_ddlEntidad",
        "search_button": "#ctl00_MainContentPlaceHolder_ucBusqueda1_btnBuscar",
        "results_grid": "#gridResultados",
        "results_table": "#gridResultados table.dxgvTable",
        "data_rows": "tr.dxgvDataRow",
        "pagination": ".dxpPager",
        "page_info": ".dxpPagerTotal",
        "loading_panel": "#ctl00_MainContentPlaceHolder_ucBusqueda1_ASPxCallbackPanelBuscador_LPV",
    }

    # Category mappings (display text to value)
    CATEGORY_MAP = {
        "LEY": "LEY",
        "CONSTITUCION": "CONSTITUCION",
        "CODIGO": "CODIGO",
        "DECRETO": "DECRETO",
        "REGLAMENTO": "REGLAMENTO",
        "ACUERDO": "ACUERDO",
        "TRATADO": "TRATADO",
        "CONVENIO": "CONVENIO",
    }

    SCOPE_MAP = {
        "FEDERAL": "FEDERAL",
        "ESTATAL": "ESTATAL",
        "CDMX": "CDMX",
    }

    def __init__(self, config: Optional[SCJNBrowserConfig] = None):
        self._config = config or SCJNBrowserConfig()
        self._playwright = None
        self._browser = None
        self._context = None
        self._started = False

    @property
    def is_started(self) -> bool:
        return self._started

    async def start(self) -> None:
        """Initialize Playwright and launch browser."""
        try:
            from playwright.async_api import async_playwright
        except ImportError:
            raise RuntimeError(
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
        logger.info("SCJN browser adapter started")

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
        logger.info("SCJN browser adapter stopped")

    async def search(
        self,
        category: Optional[str] = None,
        scope: Optional[str] = None,
        keyword: Optional[str] = None,
        page: int = 1,
    ) -> SCJNRenderedPage:
        """
        Execute search on SCJN database with filters.

        Args:
            category: Document category (LEY, CONSTITUCION, etc.)
            scope: Jurisdiction scope (FEDERAL, ESTATAL, CDMX)
            keyword: Search keyword
            page: Page number to fetch (1-indexed)

        Returns:
            SCJNRenderedPage with search results HTML
        """
        if not self._started:
            raise RuntimeError("Browser not started. Call start() first.")

        page_obj = await self._context.new_page()
        start_time = time.time()

        try:
            # Navigate to search page
            logger.info(f"Navigating to SCJN search page")
            response = await page_obj.goto(
                self.SEARCH_URL,
                timeout=self._config.timeout_ms,
                wait_until="domcontentloaded",
            )
            status_code = response.status if response else 0

            # Wait for page to fully load
            await page_obj.wait_for_load_state("networkidle", timeout=self._config.timeout_ms)

            # Apply search filters
            await self._apply_filters(page_obj, category, scope, keyword)

            # Click search button and wait for results
            await self._execute_search(page_obj)

            # Handle pagination if needed
            if page > 1:
                await self._navigate_to_page(page_obj, page)

            # Wait for results grid
            await self._wait_for_results(page_obj)

            # Get result count
            result_count = await self._get_result_count(page_obj)

            html = await page_obj.content()
            title = await page_obj.title()

            load_time_ms = int((time.time() - start_time) * 1000)

            return SCJNRenderedPage(
                url=page_obj.url,
                html=html,
                title=title,
                status_code=status_code,
                load_time_ms=load_time_ms,
                result_count=result_count,
            )

        finally:
            await page_obj.close()

    async def _apply_filters(
        self,
        page: Any,
        category: Optional[str],
        scope: Optional[str],
        keyword: Optional[str],
    ) -> None:
        """Apply search filters to the form."""
        # Enter keyword if provided
        if keyword:
            search_input = page.locator(self.SELECTORS["search_input"])
            if await search_input.count() > 0:
                await search_input.fill(keyword)
                logger.debug(f"Entered keyword: {keyword}")

        # Select category if provided
        if category:
            category_value = self.CATEGORY_MAP.get(category.upper(), category)
            try:
                dropdown = page.locator(self.SELECTORS["category_dropdown"])
                if await dropdown.count() > 0:
                    await dropdown.select_option(value=category_value)
                    logger.debug(f"Selected category: {category_value}")
            except Exception as e:
                logger.warning(f"Could not select category {category}: {e}")

        # Select scope if provided
        if scope:
            scope_value = self.SCOPE_MAP.get(scope.upper(), scope)
            try:
                dropdown = page.locator(self.SELECTORS["scope_dropdown"])
                if await dropdown.count() > 0:
                    await dropdown.select_option(value=scope_value)
                    logger.debug(f"Selected scope: {scope_value}")
            except Exception as e:
                logger.warning(f"Could not select scope {scope}: {e}")

    async def _execute_search(self, page: Any) -> None:
        """Click search button and wait for results to load."""
        search_button = page.locator(self.SELECTORS["search_button"])

        if await search_button.count() == 0:
            # Try alternative selector
            search_button = page.locator("input[value='Buscar']")

        if await search_button.count() > 0:
            await search_button.click()
            logger.debug("Clicked search button")

            # Wait for loading to complete
            await self._wait_for_loading_complete(page)
        else:
            logger.warning("Could not find search button")

    async def _wait_for_loading_complete(self, page: Any) -> None:
        """Wait for AJAX loading to complete."""
        try:
            # Wait for loading panel to disappear
            loading = page.locator(self.SELECTORS["loading_panel"])
            if await loading.count() > 0:
                await loading.wait_for(state="hidden", timeout=self._config.timeout_ms)
        except Exception:
            pass

        # Also wait for network to settle
        await page.wait_for_load_state("networkidle", timeout=self._config.timeout_ms)

    async def _wait_for_results(self, page: Any) -> None:
        """Wait for results grid to be populated."""
        try:
            # Wait for results table to appear
            results_table = page.locator(self.SELECTORS["results_table"])
            await results_table.wait_for(state="visible", timeout=self._config.timeout_ms)
            logger.debug("Results table is visible")
        except Exception as e:
            # Results might not exist (no results found)
            logger.debug(f"Results table not found: {e}")

    async def _get_result_count(self, page: Any) -> int:
        """Extract total result count from page."""
        try:
            # Look for "X resultados" text
            page_info = page.locator(self.SELECTORS["page_info"])
            if await page_info.count() > 0:
                text = await page_info.text_content()
                if text:
                    import re
                    # Pattern: "Página 1 de 10 (100 resultados)"
                    match = re.search(r'\((\d+)\s*resultados?\)', text, re.IGNORECASE)
                    if match:
                        return int(match.group(1))
                    # Alternative: just page count
                    match = re.search(r'de\s+(\d+)', text)
                    if match:
                        return int(match.group(1)) * 10  # Estimate

            # Count visible rows as fallback
            rows = page.locator(f"{self.SELECTORS['results_grid']} {self.SELECTORS['data_rows']}")
            return await rows.count()
        except Exception:
            return 0

    async def _navigate_to_page(self, page: Any, target_page: int) -> None:
        """Navigate to a specific page in the results."""
        # This is complex with ASP.NET callbacks
        # For now, we'll click page numbers in the pager
        try:
            pager = page.locator(self.SELECTORS["pagination"])
            if await pager.count() > 0:
                # Look for page number link
                page_link = pager.locator(f"a:has-text('{target_page}')")
                if await page_link.count() > 0:
                    await page_link.click()
                    await self._wait_for_loading_complete(page)
                    await self._wait_for_results(page)
                    logger.debug(f"Navigated to page {target_page}")
        except Exception as e:
            logger.warning(f"Could not navigate to page {target_page}: {e}")


@asynccontextmanager
async def scjn_browser_session(config: Optional[SCJNBrowserConfig] = None):
    """
    Context manager for SCJN browser sessions.

    Usage:
        async with scjn_browser_session() as browser:
            result = await browser.search(category="LEY", scope="FEDERAL")
            # Parse result.html
    """
    adapter = SCJNBrowserAdapter(config)
    try:
        await adapter.start()
        yield adapter
    finally:
        await adapter.stop()
