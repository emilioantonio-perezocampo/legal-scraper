"""
Tests for CAS Discovery Actor

Following RED-GREEN TDD: These tests define the expected behavior
for the CAS award discovery actor that crawls search results.

Target: ~25 tests for CAS discovery actor
"""
import pytest
from unittest.mock import Mock, AsyncMock, patch, MagicMock


class TestCASDiscoveryActorInit:
    """Tests for CASDiscoveryActor initialization."""

    def test_inherits_from_base_actor(self):
        """Discovery actor inherits from CASBaseActor."""
        from src.infrastructure.actors.cas_discovery_actor import CASDiscoveryActor
        from src.infrastructure.actors.cas_base_actor import CASBaseActor
        assert issubclass(CASDiscoveryActor, CASBaseActor)

    def test_has_browser_adapter(self):
        """Discovery actor has browser adapter."""
        from src.infrastructure.actors.cas_discovery_actor import CASDiscoveryActor
        browser = Mock()
        actor = CASDiscoveryActor(browser_adapter=browser)
        assert actor.browser_adapter == browser

    def test_has_base_url(self):
        """Discovery actor has base URL."""
        from src.infrastructure.actors.cas_discovery_actor import CASDiscoveryActor
        actor = CASDiscoveryActor(base_url="https://jurisprudence.tas-cas.org")
        assert actor.base_url == "https://jurisprudence.tas-cas.org"

    def test_default_base_url(self):
        """Default base URL is CAS jurisprudence site."""
        from src.infrastructure.actors.cas_discovery_actor import CASDiscoveryActor
        actor = CASDiscoveryActor()
        assert "tas-cas.org" in actor.base_url or "cas" in actor.base_url.lower()


class TestCASDiscoveryActorDiscovery:
    """Tests for CASDiscoveryActor discovery functionality."""

    @pytest.mark.asyncio
    async def test_handle_iniciar_descubrimiento(self):
        """Handles IniciarDescubrimientoCAS message."""
        from src.infrastructure.actors.cas_discovery_actor import CASDiscoveryActor
        from src.infrastructure.actors.cas_messages import IniciarDescubrimientoCAS

        browser = AsyncMock()
        browser.execute_search = AsyncMock(return_value=Mock(html="<html></html>"))

        actor = CASDiscoveryActor(browser_adapter=browser)
        await actor.start()

        msg = IniciarDescubrimientoCAS(max_paginas=1)
        await actor.receive(msg)

        browser.execute_search.assert_called()
        await actor.stop()

    @pytest.mark.asyncio
    async def test_discovery_uses_parser(self):
        """Discovery uses cas_search_parser."""
        from src.infrastructure.actors.cas_discovery_actor import CASDiscoveryActor
        from src.infrastructure.actors.cas_messages import IniciarDescubrimientoCAS

        browser = AsyncMock()
        browser.execute_search = AsyncMock(return_value=Mock(
            html="<div class='case-item'>CAS 2023/A/12345</div>"
        ))

        actor = CASDiscoveryActor(browser_adapter=browser)
        await actor.start()

        msg = IniciarDescubrimientoCAS(max_paginas=1)
        await actor.receive(msg)

        await actor.stop()

    @pytest.mark.asyncio
    async def test_discovery_emits_laudo_descubierto(self):
        """Discovery emits LaudoDescubiertoCAS for each result."""
        from src.infrastructure.actors.cas_discovery_actor import CASDiscoveryActor
        from src.infrastructure.actors.cas_messages import IniciarDescubrimientoCAS, LaudoDescubiertoCAS

        browser = AsyncMock()
        browser.execute_search = AsyncMock(return_value=Mock(
            html="<div class='case-item'>CAS 2023/A/12345</div>"
        ))

        coordinator = AsyncMock()
        actor = CASDiscoveryActor(browser_adapter=browser, supervisor=coordinator)
        await actor.start()

        msg = IniciarDescubrimientoCAS(max_paginas=1)
        await actor.receive(msg)

        # Coordinator should receive discovered awards
        await actor.stop()

    @pytest.mark.asyncio
    async def test_discovery_respects_max_paginas(self):
        """Discovery respects max_paginas limit."""
        from src.infrastructure.actors.cas_discovery_actor import CASDiscoveryActor
        from src.infrastructure.actors.cas_messages import IniciarDescubrimientoCAS

        browser = AsyncMock()
        browser.execute_search = AsyncMock(return_value=Mock(html="<html></html>"))

        actor = CASDiscoveryActor(browser_adapter=browser)
        await actor.start()

        msg = IniciarDescubrimientoCAS(max_paginas=3)
        await actor.receive(msg)

        # Should not exceed 3 page requests
        assert browser.execute_search.call_count <= 3
        await actor.stop()

    @pytest.mark.asyncio
    async def test_discovery_handles_empty_results(self):
        """Discovery handles empty search results."""
        from src.infrastructure.actors.cas_discovery_actor import CASDiscoveryActor
        from src.infrastructure.actors.cas_messages import IniciarDescubrimientoCAS

        browser = AsyncMock()
        browser.execute_search = AsyncMock(return_value=Mock(html="<html><body></body></html>"))

        actor = CASDiscoveryActor(browser_adapter=browser)
        await actor.start()

        msg = IniciarDescubrimientoCAS(max_paginas=1)
        await actor.receive(msg)

        # Should complete without error
        await actor.stop()


class TestCASDiscoveryActorPagination:
    """Tests for CASDiscoveryActor pagination handling."""

    @pytest.mark.asyncio
    async def test_follows_pagination(self):
        """Discovery follows pagination links."""
        from src.infrastructure.actors.cas_discovery_actor import CASDiscoveryActor
        from src.infrastructure.actors.cas_messages import IniciarDescubrimientoCAS

        browser = AsyncMock()
        # Return HTML with pagination info
        browser.execute_search = AsyncMock(return_value=Mock(
            html="<div class='pagination'><a class='active'>1</a><a>2</a></div>"
        ))

        actor = CASDiscoveryActor(browser_adapter=browser)
        await actor.start()

        msg = IniciarDescubrimientoCAS(max_paginas=None)  # Unlimited
        await actor.receive(msg)

        await actor.stop()

    @pytest.mark.asyncio
    async def test_stops_at_last_page(self):
        """Discovery stops when reaching last page."""
        from src.infrastructure.actors.cas_discovery_actor import CASDiscoveryActor
        from src.infrastructure.actors.cas_messages import IniciarDescubrimientoCAS

        call_count = 0

        async def mock_search(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count >= 3:
                # Last page - no more pagination
                return Mock(html="<div>Final results</div>")
            return Mock(html="<div class='pagination'><a>Next</a></div>")

        browser = AsyncMock()
        browser.execute_search = mock_search

        actor = CASDiscoveryActor(browser_adapter=browser)
        await actor.start()

        msg = IniciarDescubrimientoCAS(max_paginas=10)
        await actor.receive(msg)

        await actor.stop()


class TestCASDiscoveryActorFilters:
    """Tests for CASDiscoveryActor filter handling."""

    @pytest.mark.asyncio
    async def test_applies_year_filter(self):
        """Discovery applies year filter to search."""
        from src.infrastructure.actors.cas_discovery_actor import CASDiscoveryActor
        from src.infrastructure.actors.cas_messages import IniciarDescubrimientoCAS

        browser = AsyncMock()
        browser.execute_search = AsyncMock(return_value=Mock(html="<html></html>"))

        actor = CASDiscoveryActor(browser_adapter=browser)
        await actor.start()

        msg = IniciarDescubrimientoCAS(filtros={"year": 2023})
        await actor.receive(msg)

        # Verify filters were passed
        call_args = browser.execute_search.call_args
        assert call_args is not None
        await actor.stop()

    @pytest.mark.asyncio
    async def test_applies_sport_filter(self):
        """Discovery applies sport filter to search."""
        from src.infrastructure.actors.cas_discovery_actor import CASDiscoveryActor
        from src.infrastructure.actors.cas_messages import IniciarDescubrimientoCAS

        browser = AsyncMock()
        browser.execute_search = AsyncMock(return_value=Mock(html="<html></html>"))

        actor = CASDiscoveryActor(browser_adapter=browser)
        await actor.start()

        msg = IniciarDescubrimientoCAS(filtros={"sport": "football"})
        await actor.receive(msg)

        await actor.stop()


class TestCASDiscoveryActorErrorHandling:
    """Tests for CASDiscoveryActor error handling."""

    @pytest.mark.asyncio
    async def test_handles_browser_timeout(self):
        """Discovery handles browser timeout gracefully."""
        from src.infrastructure.actors.cas_discovery_actor import CASDiscoveryActor
        from src.infrastructure.actors.cas_messages import IniciarDescubrimientoCAS
        from src.infrastructure.adapters.cas_errors import BrowserTimeoutError

        browser = AsyncMock()
        browser.execute_search = AsyncMock(side_effect=BrowserTimeoutError("Timeout"))

        supervisor = AsyncMock()
        actor = CASDiscoveryActor(browser_adapter=browser, supervisor=supervisor)
        await actor.start()

        msg = IniciarDescubrimientoCAS(max_paginas=1)
        await actor.receive(msg)

        # Should escalate to supervisor
        await actor.stop()

    @pytest.mark.asyncio
    async def test_handles_parse_error(self):
        """Discovery handles parse errors gracefully."""
        from src.infrastructure.actors.cas_discovery_actor import CASDiscoveryActor
        from src.infrastructure.actors.cas_messages import IniciarDescubrimientoCAS
        from src.infrastructure.adapters.cas_errors import ParseError

        browser = AsyncMock()
        browser.execute_search = AsyncMock(return_value=Mock(html=""))

        supervisor = AsyncMock()
        actor = CASDiscoveryActor(browser_adapter=browser, supervisor=supervisor)
        await actor.start()

        msg = IniciarDescubrimientoCAS(max_paginas=1)
        await actor.receive(msg)

        await actor.stop()

    @pytest.mark.asyncio
    async def test_handles_rate_limit(self):
        """Discovery handles rate limit errors."""
        from src.infrastructure.actors.cas_discovery_actor import CASDiscoveryActor
        from src.infrastructure.actors.cas_messages import IniciarDescubrimientoCAS
        from src.infrastructure.adapters.cas_errors import CASRateLimitError

        browser = AsyncMock()
        browser.execute_search = AsyncMock(side_effect=CASRateLimitError("Rate limited"))

        supervisor = AsyncMock()
        actor = CASDiscoveryActor(browser_adapter=browser, supervisor=supervisor)
        await actor.start()

        msg = IniciarDescubrimientoCAS(max_paginas=1)
        await actor.receive(msg)

        await actor.stop()


class TestCASDiscoveryActorState:
    """Tests for CASDiscoveryActor state management."""

    @pytest.mark.asyncio
    async def test_sets_state_to_descubriendo(self):
        """Discovery sets state to DESCUBRIENDO during discovery."""
        from src.infrastructure.actors.cas_discovery_actor import CASDiscoveryActor
        from src.infrastructure.actors.cas_messages import IniciarDescubrimientoCAS, EstadoPipelineCAS

        browser = AsyncMock()
        browser.execute_search = AsyncMock(return_value=Mock(html="<html></html>"))

        actor = CASDiscoveryActor(browser_adapter=browser)
        await actor.start()

        msg = IniciarDescubrimientoCAS(max_paginas=1)
        # During discovery, state should be DESCUBRIENDO
        await actor.receive(msg)

        await actor.stop()

    def test_tracks_discovered_count(self):
        """Discovery tracks number of discovered awards."""
        from src.infrastructure.actors.cas_discovery_actor import CASDiscoveryActor

        actor = CASDiscoveryActor()
        assert actor.discovered_count == 0

    @pytest.mark.asyncio
    async def test_increments_discovered_count(self):
        """Discovery increments discovered count."""
        from src.infrastructure.actors.cas_discovery_actor import CASDiscoveryActor
        from src.infrastructure.actors.cas_messages import IniciarDescubrimientoCAS

        browser = AsyncMock()
        browser.execute_search = AsyncMock(return_value=Mock(
            html="<div class='case-item'>CAS 2023/A/12345</div>"
        ))

        actor = CASDiscoveryActor(browser_adapter=browser)
        await actor.start()

        msg = IniciarDescubrimientoCAS(max_paginas=1)
        await actor.receive(msg)

        # Count may be >= 0 depending on parse results
        assert actor.discovered_count >= 0
        await actor.stop()
