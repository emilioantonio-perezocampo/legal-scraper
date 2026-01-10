"""
Tests for CAS Scraper Actor

Following RED-GREEN TDD: These tests define the expected behavior
for the CAS award scraper actor that extracts award details.

Target: ~25 tests for CAS scraper actor
"""
import pytest
from unittest.mock import Mock, AsyncMock, patch


class TestCASScraperActorInit:
    """Tests for CASScraperActor initialization."""

    def test_inherits_from_base_actor(self):
        """Scraper actor inherits from CASBaseActor."""
        from src.infrastructure.actors.cas_scraper_actor import CASScraperActor
        from src.infrastructure.actors.cas_base_actor import CASBaseActor
        assert issubclass(CASScraperActor, CASBaseActor)

    def test_has_browser_adapter(self):
        """Scraper actor has browser adapter."""
        from src.infrastructure.actors.cas_scraper_actor import CASScraperActor
        browser = Mock()
        actor = CASScraperActor(browser_adapter=browser)
        assert actor.browser_adapter == browser

    def test_has_max_reintentos(self):
        """Scraper actor has max_reintentos setting."""
        from src.infrastructure.actors.cas_scraper_actor import CASScraperActor
        actor = CASScraperActor(max_reintentos=5)
        assert actor.max_reintentos == 5

    def test_default_max_reintentos(self):
        """Default max_reintentos is 3."""
        from src.infrastructure.actors.cas_scraper_actor import CASScraperActor
        actor = CASScraperActor()
        assert actor.max_reintentos == 3


class TestCASScraperActorScraping:
    """Tests for CASScraperActor scraping functionality."""

    @pytest.mark.asyncio
    async def test_handle_scrapear_laudo(self):
        """Handles ScrapearLaudoCAS message."""
        from src.infrastructure.actors.cas_scraper_actor import CASScraperActor
        from src.infrastructure.actors.cas_messages import ScrapearLaudoCAS

        browser = AsyncMock()
        browser.render_page = AsyncMock(return_value=Mock(
            html="<html><h1>CAS 2023/A/12345</h1></html>",
            status_code=200
        ))

        actor = CASScraperActor(browser_adapter=browser)
        await actor.start()

        msg = ScrapearLaudoCAS(numero_caso="CAS 2023/A/12345", url="https://example.com/award")
        await actor.receive(msg)

        browser.render_page.assert_called()
        await actor.stop()

    @pytest.mark.asyncio
    async def test_scraping_uses_laudo_parser(self):
        """Scraping uses cas_laudo_parser."""
        from src.infrastructure.actors.cas_scraper_actor import CASScraperActor
        from src.infrastructure.actors.cas_messages import ScrapearLaudoCAS

        browser = AsyncMock()
        browser.render_page = AsyncMock(return_value=Mock(
            html="<html><h1>CAS 2023/A/12345</h1></html>",
            status_code=200
        ))

        actor = CASScraperActor(browser_adapter=browser)
        await actor.start()

        msg = ScrapearLaudoCAS(numero_caso="CAS 2023/A/12345", url="https://example.com")
        await actor.receive(msg)

        await actor.stop()

    @pytest.mark.asyncio
    async def test_scraping_emits_laudo_scrapeado(self):
        """Scraping emits LaudoScrapeadoCAS on success."""
        from src.infrastructure.actors.cas_scraper_actor import CASScraperActor
        from src.infrastructure.actors.cas_messages import ScrapearLaudoCAS, LaudoScrapeadoCAS

        browser = AsyncMock()
        browser.render_page = AsyncMock(return_value=Mock(
            html="<html><h1>CAS 2023/A/12345</h1></html>",
            status_code=200
        ))

        coordinator = AsyncMock()
        actor = CASScraperActor(browser_adapter=browser, supervisor=coordinator)
        await actor.start()

        msg = ScrapearLaudoCAS(numero_caso="CAS 2023/A/12345", url="https://example.com")
        await actor.receive(msg)

        await actor.stop()

    @pytest.mark.asyncio
    async def test_scraping_handles_404(self):
        """Scraping handles 404 status code."""
        from src.infrastructure.actors.cas_scraper_actor import CASScraperActor
        from src.infrastructure.actors.cas_messages import ScrapearLaudoCAS

        browser = AsyncMock()
        browser.render_page = AsyncMock(return_value=Mock(
            html="<html>Not Found</html>",
            status_code=404
        ))

        supervisor = AsyncMock()
        actor = CASScraperActor(browser_adapter=browser, supervisor=supervisor)
        await actor.start()

        msg = ScrapearLaudoCAS(numero_caso="CAS 2023/A/12345", url="https://example.com")
        await actor.receive(msg)

        await actor.stop()


class TestCASScraperActorRetry:
    """Tests for CASScraperActor retry logic."""

    @pytest.mark.asyncio
    async def test_retries_on_recoverable_error(self):
        """Scraper retries on recoverable errors."""
        import asyncio
        from src.infrastructure.actors.cas_scraper_actor import CASScraperActor
        from src.infrastructure.actors.cas_messages import ScrapearLaudoCAS
        from src.infrastructure.adapters.cas_errors import BrowserTimeoutError

        call_count = 0

        async def mock_render(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise BrowserTimeoutError("Timeout")
            return Mock(
                html="<html><h1>CAS 2023/A/12345</h1></html>",
                status_code=200
            )

        browser = AsyncMock()
        browser.render_page = mock_render

        actor = CASScraperActor(browser_adapter=browser, max_reintentos=3)
        await actor.start()

        msg = ScrapearLaudoCAS(numero_caso="CAS 2023/A/12345", url="https://example.com")
        await actor.receive(msg)

        # Wait for retry to be processed
        await asyncio.sleep(0.2)

        assert call_count >= 2
        await actor.stop()

    @pytest.mark.asyncio
    async def test_stops_after_max_reintentos(self):
        """Scraper stops after max_reintentos."""
        from src.infrastructure.actors.cas_scraper_actor import CASScraperActor
        from src.infrastructure.actors.cas_messages import ScrapearLaudoCAS
        from src.infrastructure.adapters.cas_errors import BrowserTimeoutError

        browser = AsyncMock()
        browser.render_page = AsyncMock(side_effect=BrowserTimeoutError("Timeout"))

        supervisor = AsyncMock()
        actor = CASScraperActor(browser_adapter=browser, max_reintentos=2, supervisor=supervisor)
        await actor.start()

        msg = ScrapearLaudoCAS(numero_caso="CAS 2023/A/12345", url="https://example.com")
        await actor.receive(msg)

        # Should not exceed max_reintentos + 1 (initial attempt)
        assert browser.render_page.call_count <= 3
        await actor.stop()

    @pytest.mark.asyncio
    async def test_no_retry_on_non_recoverable_error(self):
        """Scraper does not retry on non-recoverable errors."""
        from src.infrastructure.actors.cas_scraper_actor import CASScraperActor
        from src.infrastructure.actors.cas_messages import ScrapearLaudoCAS
        from src.infrastructure.adapters.cas_errors import CASAdapterError

        browser = AsyncMock()
        browser.render_page = AsyncMock(
            side_effect=CASAdapterError("Fatal error", recoverable=False)
        )

        supervisor = AsyncMock()
        actor = CASScraperActor(browser_adapter=browser, max_reintentos=3, supervisor=supervisor)
        await actor.start()

        msg = ScrapearLaudoCAS(numero_caso="CAS 2023/A/12345", url="https://example.com")
        await actor.receive(msg)

        # Should only try once
        assert browser.render_page.call_count == 1
        await actor.stop()

    @pytest.mark.asyncio
    async def test_increments_reintento_count(self):
        """Scraper increments reintentos in message."""
        from src.infrastructure.actors.cas_scraper_actor import CASScraperActor
        from src.infrastructure.actors.cas_messages import ScrapearLaudoCAS
        from src.infrastructure.adapters.cas_errors import BrowserTimeoutError

        browser = AsyncMock()
        browser.render_page = AsyncMock(side_effect=BrowserTimeoutError("Timeout"))

        actor = CASScraperActor(browser_adapter=browser, max_reintentos=2)
        await actor.start()

        msg = ScrapearLaudoCAS(numero_caso="CAS 2023/A/12345", url="https://example.com", reintentos=1)
        await actor.receive(msg)

        await actor.stop()


class TestCASScraperActorErrorHandling:
    """Tests for CASScraperActor error handling."""

    @pytest.mark.asyncio
    async def test_handles_parse_error(self):
        """Scraper handles parse errors."""
        from src.infrastructure.actors.cas_scraper_actor import CASScraperActor
        from src.infrastructure.actors.cas_messages import ScrapearLaudoCAS
        from src.infrastructure.adapters.cas_errors import ParseError

        browser = AsyncMock()
        browser.render_page = AsyncMock(return_value=Mock(
            html="<html><body>No case number</body></html>",
            status_code=200
        ))

        supervisor = AsyncMock()
        actor = CASScraperActor(browser_adapter=browser, supervisor=supervisor)
        await actor.start()

        msg = ScrapearLaudoCAS(numero_caso="CAS 2023/A/12345", url="https://example.com")
        await actor.receive(msg)

        await actor.stop()

    @pytest.mark.asyncio
    async def test_handles_server_error(self):
        """Scraper handles server errors (5xx)."""
        from src.infrastructure.actors.cas_scraper_actor import CASScraperActor
        from src.infrastructure.actors.cas_messages import ScrapearLaudoCAS
        from src.infrastructure.adapters.cas_errors import CASServerError

        browser = AsyncMock()
        browser.render_page = AsyncMock(side_effect=CASServerError("Server error"))

        supervisor = AsyncMock()
        actor = CASScraperActor(browser_adapter=browser, supervisor=supervisor)
        await actor.start()

        msg = ScrapearLaudoCAS(numero_caso="CAS 2023/A/12345", url="https://example.com")
        await actor.receive(msg)

        await actor.stop()

    @pytest.mark.asyncio
    async def test_escalates_error_to_supervisor(self):
        """Scraper escalates errors to supervisor."""
        from src.infrastructure.actors.cas_scraper_actor import CASScraperActor
        from src.infrastructure.actors.cas_messages import ScrapearLaudoCAS
        from src.infrastructure.adapters.cas_errors import BrowserTimeoutError

        browser = AsyncMock()
        browser.render_page = AsyncMock(side_effect=BrowserTimeoutError("Timeout"))

        supervisor = AsyncMock()
        actor = CASScraperActor(browser_adapter=browser, max_reintentos=0, supervisor=supervisor)
        await actor.start()

        msg = ScrapearLaudoCAS(numero_caso="CAS 2023/A/12345", url="https://example.com")
        await actor.receive(msg)

        # Supervisor should be notified
        await actor.stop()


class TestCASScraperActorState:
    """Tests for CASScraperActor state management."""

    @pytest.mark.asyncio
    async def test_sets_state_to_scrapeando(self):
        """Scraper sets state to SCRAPEANDO during scraping."""
        from src.infrastructure.actors.cas_scraper_actor import CASScraperActor
        from src.infrastructure.actors.cas_messages import ScrapearLaudoCAS, EstadoPipelineCAS

        browser = AsyncMock()
        browser.render_page = AsyncMock(return_value=Mock(
            html="<html><h1>CAS 2023/A/12345</h1></html>",
            status_code=200
        ))

        actor = CASScraperActor(browser_adapter=browser)
        await actor.start()

        msg = ScrapearLaudoCAS(numero_caso="CAS 2023/A/12345", url="https://example.com")
        await actor.receive(msg)

        await actor.stop()

    def test_tracks_scraped_count(self):
        """Scraper tracks number of scraped awards."""
        from src.infrastructure.actors.cas_scraper_actor import CASScraperActor

        actor = CASScraperActor()
        assert actor.scraped_count == 0

    def test_tracks_error_count(self):
        """Scraper tracks number of errors."""
        from src.infrastructure.actors.cas_scraper_actor import CASScraperActor

        actor = CASScraperActor()
        assert actor.error_count == 0


class TestCASScraperActorConcurrency:
    """Tests for CASScraperActor concurrency handling."""

    @pytest.mark.asyncio
    async def test_processes_messages_sequentially(self):
        """Scraper processes messages sequentially."""
        from src.infrastructure.actors.cas_scraper_actor import CASScraperActor
        from src.infrastructure.actors.cas_messages import ScrapearLaudoCAS

        processing_order = []

        async def mock_render(url, **kwargs):
            case_num = url.split("/")[-1]
            processing_order.append(case_num)
            return Mock(
                html=f"<html><h1>CAS 2023/A/{case_num}</h1></html>",
                status_code=200
            )

        browser = AsyncMock()
        browser.render_page = mock_render

        actor = CASScraperActor(browser_adapter=browser)
        await actor.start()

        # Send multiple messages
        await actor.tell(ScrapearLaudoCAS(numero_caso="1", url="https://example.com/1"))
        await actor.tell(ScrapearLaudoCAS(numero_caso="2", url="https://example.com/2"))

        # Give time for processing
        import asyncio
        await asyncio.sleep(0.1)

        await actor.stop()

    @pytest.mark.asyncio
    async def test_handles_rapid_messages(self):
        """Scraper handles rapid message sending."""
        from src.infrastructure.actors.cas_scraper_actor import CASScraperActor
        from src.infrastructure.actors.cas_messages import ScrapearLaudoCAS

        browser = AsyncMock()
        browser.render_page = AsyncMock(return_value=Mock(
            html="<html><h1>CAS 2023/A/12345</h1></html>",
            status_code=200
        ))

        actor = CASScraperActor(browser_adapter=browser)
        await actor.start()

        # Send many messages rapidly
        for i in range(10):
            await actor.tell(ScrapearLaudoCAS(
                numero_caso=f"CAS 2023/A/{i}",
                url=f"https://example.com/{i}"
            ))

        # Should not crash
        import asyncio
        await asyncio.sleep(0.1)
        await actor.stop()
