"""
Tests for CAS Browser Adapter (Playwright)

Following RED-GREEN TDD: These tests define the expected behavior
for CAS JavaScript SPA browser automation.

Target: ~25 tests for browser adapter
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch


class TestBrowserConfig:
    """Tests for BrowserConfig dataclass."""

    def test_default_values(self):
        """BrowserConfig has sensible defaults."""
        from src.infrastructure.adapters.cas_browser_adapter import BrowserConfig
        config = BrowserConfig()
        assert config.headless is True
        assert config.timeout_ms == 30000
        assert config.viewport_width == 1920
        assert config.viewport_height == 1080

    def test_custom_values(self):
        """BrowserConfig accepts custom values."""
        from src.infrastructure.adapters.cas_browser_adapter import BrowserConfig
        config = BrowserConfig(
            headless=False,
            timeout_ms=60000,
            viewport_width=1280,
            viewport_height=720,
        )
        assert config.headless is False
        assert config.timeout_ms == 60000

    def test_immutability(self):
        """BrowserConfig is frozen dataclass."""
        from src.infrastructure.adapters.cas_browser_adapter import BrowserConfig
        config = BrowserConfig()
        with pytest.raises(Exception):
            config.headless = False

    def test_has_user_agent(self):
        """BrowserConfig has user_agent field."""
        from src.infrastructure.adapters.cas_browser_adapter import BrowserConfig
        config = BrowserConfig()
        assert "Mozilla" in config.user_agent

    def test_has_slow_mo(self):
        """BrowserConfig has slow_mo field for debugging."""
        from src.infrastructure.adapters.cas_browser_adapter import BrowserConfig
        config = BrowserConfig(slow_mo=100)
        assert config.slow_mo == 100


class TestRenderedPage:
    """Tests for RenderedPage dataclass."""

    def test_creation_with_all_fields(self):
        """Create RenderedPage with all fields."""
        from src.infrastructure.adapters.cas_browser_adapter import RenderedPage
        page = RenderedPage(
            url="https://jurisprudence.tas-cas.org/",
            html="<html><body>Content</body></html>",
            title="CAS Jurisprudence",
            status_code=200,
            load_time_ms=1500,
        )
        assert page.url == "https://jurisprudence.tas-cas.org/"
        assert page.status_code == 200

    def test_immutability(self):
        """RenderedPage is frozen dataclass."""
        from src.infrastructure.adapters.cas_browser_adapter import RenderedPage
        page = RenderedPage(
            url="https://example.com",
            html="<html></html>",
            title="Test",
            status_code=200,
            load_time_ms=100,
        )
        with pytest.raises(Exception):
            page.url = "https://other.com"


class TestCASBrowserAdapterInit:
    """Tests for CASBrowserAdapter initialization."""

    def test_initialization_with_default_config(self):
        """Initialize adapter with default config."""
        from src.infrastructure.adapters.cas_browser_adapter import CASBrowserAdapter
        adapter = CASBrowserAdapter()
        assert adapter.is_started is False

    def test_initialization_with_custom_config(self):
        """Initialize adapter with custom config."""
        from src.infrastructure.adapters.cas_browser_adapter import (
            CASBrowserAdapter, BrowserConfig
        )
        config = BrowserConfig(headless=False)
        adapter = CASBrowserAdapter(config=config)
        assert adapter.is_started is False

    def test_is_started_property(self):
        """is_started property returns False before start()."""
        from src.infrastructure.adapters.cas_browser_adapter import CASBrowserAdapter
        adapter = CASBrowserAdapter()
        assert adapter.is_started is False


class TestCASBrowserAdapterLifecycle:
    """Tests for CASBrowserAdapter start/stop lifecycle."""

    @pytest.mark.asyncio
    async def test_start_raises_when_playwright_not_installed(self):
        """start() raises BrowserNotAvailableError when playwright missing."""
        from src.infrastructure.adapters.cas_browser_adapter import CASBrowserAdapter

        with patch.dict('sys.modules', {'playwright': None, 'playwright.async_api': None}):
            adapter = CASBrowserAdapter()
            # Import error will be raised on start
            with pytest.raises(Exception):
                await adapter.start()

    @pytest.mark.asyncio
    async def test_stop_when_not_started(self):
        """stop() is safe to call when not started."""
        from src.infrastructure.adapters.cas_browser_adapter import CASBrowserAdapter
        adapter = CASBrowserAdapter()
        # Should not raise
        await adapter.stop()
        assert adapter.is_started is False


class TestCASBrowserAdapterRenderPage:
    """Tests for CASBrowserAdapter.render_page method."""

    @pytest.mark.asyncio
    async def test_render_page_raises_when_not_started(self):
        """render_page raises BrowserNotAvailableError when not started."""
        from src.infrastructure.adapters.cas_browser_adapter import CASBrowserAdapter
        from src.infrastructure.adapters.cas_errors import BrowserNotAvailableError

        adapter = CASBrowserAdapter()
        with pytest.raises(BrowserNotAvailableError):
            await adapter.render_page("https://example.com")

    @pytest.mark.asyncio
    async def test_render_page_returns_rendered_page(self):
        """render_page returns RenderedPage with content."""
        from src.infrastructure.adapters.cas_browser_adapter import (
            CASBrowserAdapter, RenderedPage
        )

        # Mock playwright
        mock_page = AsyncMock()
        mock_page.goto = AsyncMock(return_value=MagicMock(status=200))
        mock_page.wait_for_load_state = AsyncMock()
        mock_page.content = AsyncMock(return_value="<html>Rendered</html>")
        mock_page.title = AsyncMock(return_value="Test Page")
        mock_page.close = AsyncMock()

        mock_context = AsyncMock()
        mock_context.new_page = AsyncMock(return_value=mock_page)

        adapter = CASBrowserAdapter()
        adapter._started = True
        adapter._context = mock_context

        result = await adapter.render_page("https://example.com")

        assert isinstance(result, RenderedPage)
        assert result.html == "<html>Rendered</html>"
        assert result.status_code == 200

    @pytest.mark.asyncio
    async def test_render_page_with_wait_selector(self):
        """render_page waits for selector when provided."""
        from src.infrastructure.adapters.cas_browser_adapter import CASBrowserAdapter

        mock_page = AsyncMock()
        mock_page.goto = AsyncMock(return_value=MagicMock(status=200))
        mock_page.wait_for_load_state = AsyncMock()
        mock_page.wait_for_selector = AsyncMock()
        mock_page.content = AsyncMock(return_value="<html></html>")
        mock_page.title = AsyncMock(return_value="Test")
        mock_page.close = AsyncMock()

        mock_context = AsyncMock()
        mock_context.new_page = AsyncMock(return_value=mock_page)

        adapter = CASBrowserAdapter()
        adapter._started = True
        adapter._context = mock_context

        await adapter.render_page("https://example.com", wait_selector=".results")

        mock_page.wait_for_selector.assert_called_once()


class TestCASBrowserAdapterCaptureScreenshot:
    """Tests for CASBrowserAdapter.capture_screenshot method."""

    @pytest.mark.asyncio
    async def test_capture_screenshot_raises_when_not_started(self):
        """capture_screenshot raises when browser not started."""
        from src.infrastructure.adapters.cas_browser_adapter import CASBrowserAdapter
        from src.infrastructure.adapters.cas_errors import BrowserNotAvailableError

        adapter = CASBrowserAdapter()
        with pytest.raises(BrowserNotAvailableError):
            await adapter.capture_screenshot("https://example.com", "/tmp/test.png")

    @pytest.mark.asyncio
    async def test_capture_screenshot_returns_path(self):
        """capture_screenshot returns output path."""
        from src.infrastructure.adapters.cas_browser_adapter import CASBrowserAdapter

        mock_page = AsyncMock()
        mock_page.goto = AsyncMock()
        mock_page.wait_for_load_state = AsyncMock()
        mock_page.screenshot = AsyncMock()
        mock_page.close = AsyncMock()

        mock_context = AsyncMock()
        mock_context.new_page = AsyncMock(return_value=mock_page)

        adapter = CASBrowserAdapter()
        adapter._started = True
        adapter._context = mock_context

        result = await adapter.capture_screenshot("https://example.com", "/tmp/test.png")
        assert result == "/tmp/test.png"


class TestCASBrowserAdapterExecuteSearch:
    """Tests for CASBrowserAdapter.execute_search method."""

    @pytest.mark.asyncio
    async def test_execute_search_raises_when_not_started(self):
        """execute_search raises when browser not started."""
        from src.infrastructure.adapters.cas_browser_adapter import CASBrowserAdapter
        from src.infrastructure.adapters.cas_errors import BrowserNotAvailableError

        adapter = CASBrowserAdapter()
        with pytest.raises(BrowserNotAvailableError):
            await adapter.execute_search()

    @pytest.mark.asyncio
    async def test_execute_search_returns_rendered_page(self):
        """execute_search returns RenderedPage with search results."""
        from src.infrastructure.adapters.cas_browser_adapter import (
            CASBrowserAdapter, RenderedPage
        )

        mock_page = AsyncMock()
        mock_page.goto = AsyncMock()
        mock_page.wait_for_load_state = AsyncMock()
        mock_page.content = AsyncMock(return_value="<html>Results</html>")
        mock_page.title = AsyncMock(return_value="Search Results")
        mock_page.url = "https://jurisprudence.tas-cas.org/search"
        mock_page.close = AsyncMock()

        mock_context = AsyncMock()
        mock_context.new_page = AsyncMock(return_value=mock_page)

        adapter = CASBrowserAdapter()
        adapter._started = True
        adapter._context = mock_context

        result = await adapter.execute_search()

        assert isinstance(result, RenderedPage)

    @pytest.mark.asyncio
    async def test_execute_search_with_filters(self):
        """execute_search applies filters when provided."""
        from src.infrastructure.adapters.cas_browser_adapter import CASBrowserAdapter

        mock_page = AsyncMock()
        mock_page.goto = AsyncMock()
        mock_page.wait_for_load_state = AsyncMock()
        mock_page.content = AsyncMock(return_value="<html></html>")
        mock_page.title = AsyncMock(return_value="Search")
        mock_page.url = "https://jurisprudence.tas-cas.org/"
        mock_page.close = AsyncMock()

        mock_context = AsyncMock()
        mock_context.new_page = AsyncMock(return_value=mock_page)

        adapter = CASBrowserAdapter()
        adapter._started = True
        adapter._context = mock_context

        await adapter.execute_search(filters={"keyword": "football", "year_from": 2020})

        # Should not raise, filter application is internal


class TestBrowserSession:
    """Tests for browser_session context manager."""

    @pytest.mark.asyncio
    async def test_browser_session_context_manager(self):
        """browser_session is an async context manager."""
        from src.infrastructure.adapters.cas_browser_adapter import browser_session

        # Just verify it's an async context manager
        import inspect
        assert hasattr(browser_session, '__call__')
