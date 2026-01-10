"""
RED Phase Tests: SCJNDiscoveryActor

Tests for SCJN document discovery actor.
These tests must FAIL initially (RED phase).
"""
import pytest
import pytest_asyncio
import asyncio
from unittest.mock import MagicMock, AsyncMock, patch
from datetime import date

# These imports will fail until implementation exists
from src.infrastructure.actors.scjn_discovery_actor import (
    SCJNDiscoveryActor,
    DiscoveryError,
    PaginationError,
)
from src.infrastructure.actors.messages import (
    DescubrirDocumentos,
    DocumentoDescubierto,
    DescubrirPagina,
    PaginaDescubierta,
    ErrorDeActor,
)
from src.infrastructure.actors.rate_limiter import NoOpRateLimiter
from tests.utils.actor_test_helpers import MockCoordinator


@pytest.fixture
def rate_limiter():
    """Create a no-op rate limiter for testing."""
    return NoOpRateLimiter()


@pytest_asyncio.fixture
async def mock_coordinator():
    """Create a mock coordinator for receiving messages."""
    coordinator = MockCoordinator()
    await coordinator.start()
    yield coordinator
    await coordinator.stop()


@pytest.fixture
def search_html_single_page():
    """Create HTML for search results with single page."""
    return """
    <html>
    <body>
    <div id="gridResultados">
        <table class="dxgvTable">
            <tr class="dxgvDataRow">
                <td><a href="wfOrdenamientoDetalle.aspx?q=ABC123">Ley Federal del Trabajo</a></td>
                <td>01/04/1970</td>
                <td>01/04/1970</td>
                <td>VIGENTE</td>
                <td>LEY FEDERAL</td>
                <td>FEDERAL</td>
            </tr>
            <tr class="dxgvDataRow">
                <td><a href="wfOrdenamientoDetalle.aspx?q=DEF456">Codigo Civil Federal</a></td>
                <td>26/05/1928</td>
                <td>26/05/1928</td>
                <td>VIGENTE</td>
                <td>CODIGO</td>
                <td>FEDERAL</td>
            </tr>
        </table>
        <div class="dxpPagerTotal">Página 1 de 1</div>
    </div>
    </body>
    </html>
    """


@pytest.fixture
def search_html_multi_page():
    """Create HTML for search results with multiple pages."""
    return """
    <html>
    <body>
    <div id="gridResultados">
        <table class="dxgvTable">
            <tr class="dxgvDataRow">
                <td><a href="wfOrdenamientoDetalle.aspx?q=ABC123">Ley Federal del Trabajo</a></td>
                <td>01/04/1970</td>
                <td>01/04/1970</td>
                <td>VIGENTE</td>
                <td>LEY FEDERAL</td>
                <td>FEDERAL</td>
            </tr>
        </table>
        <div class="dxpPagerTotal">Página 1 de 3</div>
        <span class="dxpPagerItem" onclick="__doPostBack('grid', 'PN2')">2</span>
    </div>
    </body>
    </html>
    """


@pytest.fixture
def search_html_empty():
    """Create HTML for empty search results."""
    return """
    <html>
    <body>
    <div id="gridResultados">
        <div class="dxgvEmptyDataRow">No se encontraron resultados</div>
    </div>
    </body>
    </html>
    """


class TestSCJNDiscoveryActorLifecycle:
    """Tests for SCJNDiscoveryActor lifecycle."""

    @pytest.mark.asyncio
    async def test_start_creates_session(self, mock_coordinator, rate_limiter):
        """Start should create HTTP session."""
        actor = SCJNDiscoveryActor(
            coordinator=mock_coordinator,
            rate_limiter=rate_limiter,
        )
        await actor.start()

        assert actor._session is not None

        await actor.stop()

    @pytest.mark.asyncio
    async def test_stop_closes_session(self, mock_coordinator, rate_limiter):
        """Stop should close HTTP session."""
        actor = SCJNDiscoveryActor(
            coordinator=mock_coordinator,
            rate_limiter=rate_limiter,
        )
        await actor.start()
        session = actor._session
        await actor.stop()

        assert actor._session is None

    @pytest.mark.asyncio
    async def test_custom_config(self, mock_coordinator, rate_limiter):
        """Should accept custom configuration."""
        actor = SCJNDiscoveryActor(
            coordinator=mock_coordinator,
            rate_limiter=rate_limiter,
            max_pages=50,
            timeout_seconds=60.0,
        )

        assert actor._max_pages == 50


class TestSCJNDiscoveryActorDiscovery:
    """Tests for document discovery."""

    @pytest.mark.asyncio
    async def test_discover_emits_documentos(
        self, mock_coordinator, rate_limiter, search_html_single_page
    ):
        """Discovery should emit DocumentoDescubierto events."""
        actor = SCJNDiscoveryActor(
            coordinator=mock_coordinator,
            rate_limiter=rate_limiter,
        )
        await actor.start()

        # Mock HTTP response
        with patch.object(actor, '_fetch_search_page', new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = search_html_single_page

            result = await actor.ask(
                DescubrirDocumentos(
                    category="LEY FEDERAL",
                    correlation_id="test-corr-001",
                )
            )

        # Should emit DocumentoDescubierto to coordinator
        discovered_events = [
            m for m in mock_coordinator.received
            if isinstance(m, DocumentoDescubierto)
        ]
        assert len(discovered_events) == 2
        assert discovered_events[0].q_param == "ABC123"
        assert discovered_events[1].q_param == "DEF456"

        await actor.stop()

    @pytest.mark.asyncio
    async def test_discover_returns_pagina_descubierta(
        self, mock_coordinator, rate_limiter, search_html_single_page
    ):
        """Discovery should return PaginaDescubierta result."""
        actor = SCJNDiscoveryActor(
            coordinator=mock_coordinator,
            rate_limiter=rate_limiter,
        )
        await actor.start()

        with patch.object(actor, '_fetch_search_page', new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = search_html_single_page

            result = await actor.ask(
                DescubrirDocumentos(
                    category="LEY",
                    correlation_id="test-corr-001",
                )
            )

        assert isinstance(result, PaginaDescubierta)
        assert result.documents_found == 2
        assert result.current_page == 1
        assert result.total_pages == 1

        await actor.stop()

    @pytest.mark.asyncio
    async def test_discover_preserves_correlation_id(
        self, mock_coordinator, rate_limiter, search_html_single_page
    ):
        """Result should preserve correlation_id."""
        actor = SCJNDiscoveryActor(
            coordinator=mock_coordinator,
            rate_limiter=rate_limiter,
        )
        await actor.start()

        with patch.object(actor, '_fetch_search_page', new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = search_html_single_page

            result = await actor.ask(
                DescubrirDocumentos(
                    category="LEY",
                    correlation_id="my-correlation-id",
                )
            )

        assert result.correlation_id == "my-correlation-id"

        await actor.stop()


class TestSCJNDiscoveryActorPagination:
    """Tests for pagination handling."""

    @pytest.mark.asyncio
    async def test_discover_handles_multipage(
        self, mock_coordinator, rate_limiter, search_html_multi_page
    ):
        """Should discover across multiple pages."""
        actor = SCJNDiscoveryActor(
            coordinator=mock_coordinator,
            rate_limiter=rate_limiter,
        )
        await actor.start()

        with patch.object(actor, '_fetch_search_page', new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = search_html_multi_page

            result = await actor.ask(
                DescubrirDocumentos(
                    category="LEY",
                    discover_all_pages=False,  # Only first page
                )
            )

        assert result.total_pages == 3
        assert result.has_more_pages is True

        await actor.stop()

    @pytest.mark.asyncio
    async def test_discover_all_pages(self, mock_coordinator, rate_limiter):
        """Should iterate through all pages when requested."""
        page1_html = """
        <div id="gridResultados">
            <table class="dxgvTable">
                <tr class="dxgvDataRow">
                    <td><a href="wfOrdenamientoDetalle.aspx?q=P1A">Doc Page 1</a></td>
                    <td>01/01/2020</td><td>01/01/2020</td>
                    <td>VIGENTE</td><td>LEY</td><td>FEDERAL</td>
                </tr>
            </table>
            <div class="dxpPagerTotal">Página 1 de 2</div>
            <span class="dxpPagerItem" onclick="next">2</span>
        </div>
        """
        page2_html = """
        <div id="gridResultados">
            <table class="dxgvTable">
                <tr class="dxgvDataRow">
                    <td><a href="wfOrdenamientoDetalle.aspx?q=P2A">Doc Page 2</a></td>
                    <td>02/01/2020</td><td>02/01/2020</td>
                    <td>VIGENTE</td><td>LEY</td><td>FEDERAL</td>
                </tr>
            </table>
            <div class="dxpPagerTotal">Página 2 de 2</div>
        </div>
        """

        actor = SCJNDiscoveryActor(
            coordinator=mock_coordinator,
            rate_limiter=rate_limiter,
        )
        await actor.start()

        call_count = 0
        async def mock_fetch(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            return page1_html if call_count == 1 else page2_html

        with patch.object(actor, '_fetch_search_page', side_effect=mock_fetch):
            result = await actor.ask(
                DescubrirDocumentos(
                    category="LEY",
                    discover_all_pages=True,
                )
            )

        # Should have discovered from both pages
        discovered_events = [
            m for m in mock_coordinator.received
            if isinstance(m, DocumentoDescubierto)
        ]
        q_params = {e.q_param for e in discovered_events}
        assert "P1A" in q_params
        assert "P2A" in q_params

        await actor.stop()

    @pytest.mark.asyncio
    async def test_respects_max_pages(self, mock_coordinator, rate_limiter):
        """Should respect max_pages limit."""
        page_html = """
        <div id="gridResultados">
            <table class="dxgvTable">
                <tr class="dxgvDataRow">
                    <td><a href="wfOrdenamientoDetalle.aspx?q=DOC1">Doc</a></td>
                    <td>01/01/2020</td><td>01/01/2020</td>
                    <td>VIGENTE</td><td>LEY</td><td>FEDERAL</td>
                </tr>
            </table>
            <div class="dxpPagerTotal">Página 1 de 100</div>
            <span class="dxpPagerItem" onclick="next">2</span>
        </div>
        """

        actor = SCJNDiscoveryActor(
            coordinator=mock_coordinator,
            rate_limiter=rate_limiter,
            max_pages=2,
        )
        await actor.start()

        fetch_count = 0
        async def mock_fetch(*args, **kwargs):
            nonlocal fetch_count
            fetch_count += 1
            return page_html

        with patch.object(actor, '_fetch_search_page', side_effect=mock_fetch):
            result = await actor.ask(
                DescubrirDocumentos(
                    category="LEY",
                    discover_all_pages=True,
                )
            )

        # Should have stopped at max_pages
        assert fetch_count <= 2

        await actor.stop()


class TestSCJNDiscoveryActorEmptyResults:
    """Tests for empty result handling."""

    @pytest.mark.asyncio
    async def test_discover_empty_results(
        self, mock_coordinator, rate_limiter, search_html_empty
    ):
        """Should handle empty results gracefully."""
        actor = SCJNDiscoveryActor(
            coordinator=mock_coordinator,
            rate_limiter=rate_limiter,
        )
        await actor.start()

        with patch.object(actor, '_fetch_search_page', new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = search_html_empty

            result = await actor.ask(
                DescubrirDocumentos(category="NONEXISTENT")
            )

        assert isinstance(result, PaginaDescubierta)
        assert result.documents_found == 0

        await actor.stop()


class TestSCJNDiscoveryActorFiltering:
    """Tests for filtering options."""

    @pytest.mark.asyncio
    async def test_filter_by_scope(
        self, mock_coordinator, rate_limiter, search_html_single_page
    ):
        """Should include scope in search query."""
        actor = SCJNDiscoveryActor(
            coordinator=mock_coordinator,
            rate_limiter=rate_limiter,
        )
        await actor.start()

        with patch.object(actor, '_fetch_search_page', new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = search_html_single_page

            await actor.ask(
                DescubrirDocumentos(
                    category="LEY",
                    scope="ESTATAL",
                )
            )

            # Verify scope was passed to fetch
            mock_fetch.assert_called()
            call_kwargs = mock_fetch.call_args
            # The scope should be part of the search parameters
            assert call_kwargs is not None

        await actor.stop()

    @pytest.mark.asyncio
    async def test_filter_by_status(
        self, mock_coordinator, rate_limiter, search_html_single_page
    ):
        """Should include status in search query."""
        actor = SCJNDiscoveryActor(
            coordinator=mock_coordinator,
            rate_limiter=rate_limiter,
        )
        await actor.start()

        with patch.object(actor, '_fetch_search_page', new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = search_html_single_page

            await actor.ask(
                DescubrirDocumentos(
                    category="LEY",
                    status="VIGENTE",
                )
            )

            mock_fetch.assert_called()

        await actor.stop()


class TestSCJNDiscoveryActorRateLimit:
    """Tests for rate limiting."""

    @pytest.mark.asyncio
    async def test_acquires_rate_limit(self, mock_coordinator, search_html_single_page):
        """Should acquire rate limit before each fetch."""
        rate_limiter = MagicMock()
        rate_limiter.acquire = AsyncMock()

        actor = SCJNDiscoveryActor(
            coordinator=mock_coordinator,
            rate_limiter=rate_limiter,
        )
        await actor.start()

        with patch.object(actor, '_fetch_search_page', new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = search_html_single_page

            await actor.ask(DescubrirDocumentos(category="LEY"))

        rate_limiter.acquire.assert_called()

        await actor.stop()


class TestSCJNDiscoveryActorErrors:
    """Tests for error handling."""

    @pytest.mark.asyncio
    async def test_emits_error_on_network_failure(self, mock_coordinator, rate_limiter):
        """Should emit error on network failure."""
        actor = SCJNDiscoveryActor(
            coordinator=mock_coordinator,
            rate_limiter=rate_limiter,
        )
        await actor.start()

        with patch.object(actor, '_fetch_search_page', new_callable=AsyncMock) as mock_fetch:
            mock_fetch.side_effect = Exception("Network error")

            result = await actor.ask(
                DescubrirDocumentos(
                    category="LEY",
                    correlation_id="error-test",
                )
            )

        assert isinstance(result, ErrorDeActor)
        assert result.recoverable is True
        assert result.correlation_id == "error-test"

        await actor.stop()

    @pytest.mark.asyncio
    async def test_emits_error_on_parse_failure(self, mock_coordinator, rate_limiter):
        """Should emit error on parse failure."""
        actor = SCJNDiscoveryActor(
            coordinator=mock_coordinator,
            rate_limiter=rate_limiter,
        )
        await actor.start()

        with patch.object(actor, '_fetch_search_page', new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = "<html><body>Invalid structure</body></html>"

            result = await actor.ask(
                DescubrirDocumentos(category="LEY")
            )

        assert isinstance(result, ErrorDeActor)
        assert "ParseError" in result.error_type

        await actor.stop()

    @pytest.mark.asyncio
    async def test_error_includes_original_command(self, mock_coordinator, rate_limiter):
        """Error should include original command."""
        actor = SCJNDiscoveryActor(
            coordinator=mock_coordinator,
            rate_limiter=rate_limiter,
        )
        await actor.start()

        with patch.object(actor, '_fetch_search_page', new_callable=AsyncMock) as mock_fetch:
            mock_fetch.side_effect = Exception("Failure")

            cmd = DescubrirDocumentos(category="LEY")
            result = await actor.ask(cmd)

        assert result.original_command == cmd

        await actor.stop()


class TestSCJNDiscoveryActorURLBuilding:
    """Tests for URL building."""

    @pytest.mark.asyncio
    async def test_build_search_url(self, mock_coordinator, rate_limiter):
        """Should build correct search URL."""
        actor = SCJNDiscoveryActor(
            coordinator=mock_coordinator,
            rate_limiter=rate_limiter,
        )

        url = actor._build_search_url(category="LEY FEDERAL")

        assert "Buscar.aspx" in url or "buscar" in url.lower()
        assert "LEY" in url or "ley" in url.lower()

    @pytest.mark.asyncio
    async def test_build_search_url_with_filters(self, mock_coordinator, rate_limiter):
        """Should include filters in URL."""
        actor = SCJNDiscoveryActor(
            coordinator=mock_coordinator,
            rate_limiter=rate_limiter,
        )

        url = actor._build_search_url(
            category="LEY",
            scope="FEDERAL",
            status="VIGENTE",
        )

        # URL should include parameters
        assert "?" in url or url.count("=") >= 1


class TestSCJNDiscoveryActorDeduplication:
    """Tests for document deduplication."""

    @pytest.mark.asyncio
    async def test_skips_already_discovered(self, mock_coordinator, rate_limiter):
        """Should not emit duplicate DocumentoDescubierto events."""
        # Same document appears on two pages
        page_html = """
        <div id="gridResultados">
            <table class="dxgvTable">
                <tr class="dxgvDataRow">
                    <td><a href="wfOrdenamientoDetalle.aspx?q=SAME123">Same Doc</a></td>
                    <td>01/01/2020</td><td>01/01/2020</td>
                    <td>VIGENTE</td><td>LEY</td><td>FEDERAL</td>
                </tr>
            </table>
            <div class="dxpPagerTotal">Página 1 de 2</div>
            <span class="dxpPagerItem" onclick="next">2</span>
        </div>
        """

        actor = SCJNDiscoveryActor(
            coordinator=mock_coordinator,
            rate_limiter=rate_limiter,
        )
        await actor.start()

        with patch.object(actor, '_fetch_search_page', new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = page_html

            # First discovery
            await actor.ask(DescubrirDocumentos(category="LEY"))

            first_count = len([
                m for m in mock_coordinator.received
                if isinstance(m, DocumentoDescubierto)
            ])

            # Second discovery (same page)
            await actor.ask(DescubrirDocumentos(category="LEY"))

        # Should not have duplicates
        all_discovered = [
            m for m in mock_coordinator.received
            if isinstance(m, DocumentoDescubierto)
        ]
        q_params = [e.q_param for e in all_discovered]
        # Each q_param should appear only once
        assert q_params.count("SAME123") == 1

        await actor.stop()
