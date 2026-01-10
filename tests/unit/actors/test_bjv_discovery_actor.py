"""
Tests for BJV discovery actor.

Following RED-GREEN TDD: These tests define the expected behavior
for the BJV discovery actor that searches and paginates results.

Target: ~25 tests
"""
import pytest
import asyncio
from unittest.mock import Mock, AsyncMock, patch, MagicMock


class TestBJVDiscoveryActorInit:
    """Tests for discovery actor initialization."""

    def test_inherits_from_base_actor(self):
        """BJVDiscoveryActor inherits from BJVBaseActor."""
        from src.infrastructure.actors.bjv_discovery_actor import BJVDiscoveryActor
        from src.infrastructure.actors.bjv_base_actor import BJVBaseActor

        assert issubclass(BJVDiscoveryActor, BJVBaseActor)

    def test_creates_with_coordinator(self):
        """BJVDiscoveryActor requires coordinator."""
        from src.infrastructure.actors.bjv_discovery_actor import BJVDiscoveryActor
        from src.infrastructure.actors.bjv_rate_limiter import BJVRateLimiter

        coordinator = Mock()
        rate_limiter = BJVRateLimiter()

        actor = BJVDiscoveryActor(
            coordinator=coordinator,
            rate_limiter=rate_limiter,
        )

        assert actor._coordinator is coordinator

    def test_has_nombre(self):
        """BJVDiscoveryActor has descriptive name."""
        from src.infrastructure.actors.bjv_discovery_actor import BJVDiscoveryActor
        from src.infrastructure.actors.bjv_rate_limiter import BJVRateLimiter

        actor = BJVDiscoveryActor(
            coordinator=Mock(),
            rate_limiter=BJVRateLimiter(),
        )

        assert "Discovery" in actor.nombre


class TestBJVDiscoveryActorLifecycle:
    """Tests for discovery actor lifecycle."""

    @pytest.mark.asyncio
    async def test_start_creates_session(self):
        """start() creates HTTP session."""
        from src.infrastructure.actors.bjv_discovery_actor import BJVDiscoveryActor
        from src.infrastructure.actors.bjv_rate_limiter import BJVRateLimiter

        actor = BJVDiscoveryActor(
            coordinator=Mock(),
            rate_limiter=BJVRateLimiter(),
        )

        await actor.start()

        assert actor._session is not None

        await actor.stop()

    @pytest.mark.asyncio
    async def test_stop_closes_session(self):
        """stop() closes HTTP session."""
        from src.infrastructure.actors.bjv_discovery_actor import BJVDiscoveryActor
        from src.infrastructure.actors.bjv_rate_limiter import BJVRateLimiter

        actor = BJVDiscoveryActor(
            coordinator=Mock(),
            rate_limiter=BJVRateLimiter(),
        )

        await actor.start()
        session = actor._session
        await actor.stop()

        assert session.closed


class TestBJVDiscoveryActorSearch:
    """Tests for search handling."""

    @pytest.mark.asyncio
    async def test_handle_iniciar_busqueda(self):
        """handle_message processes IniciarBusqueda."""
        from src.infrastructure.actors.bjv_discovery_actor import BJVDiscoveryActor
        from src.infrastructure.actors.bjv_rate_limiter import BJVRateLimiter
        from src.infrastructure.actors.bjv_messages import IniciarBusqueda

        coordinator = Mock()
        coordinator.tell = AsyncMock()

        actor = BJVDiscoveryActor(
            coordinator=coordinator,
            rate_limiter=BJVRateLimiter(),
        )

        # Mock the HTTP call
        with patch.object(actor, '_fetch_search_page', new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = "<html><body></body></html>"

            await actor.start()
            msg = IniciarBusqueda(
                correlation_id="corr-123",
                query="derecho civil",
                max_resultados=10,
            )
            await actor.handle_message(msg)
            await actor.stop()

        # Should have attempted to fetch
        mock_fetch.assert_called()

    @pytest.mark.asyncio
    async def test_handle_procesar_pagina(self):
        """handle_message processes ProcesarPaginaResultados."""
        from src.infrastructure.actors.bjv_discovery_actor import BJVDiscoveryActor
        from src.infrastructure.actors.bjv_rate_limiter import BJVRateLimiter
        from src.infrastructure.actors.bjv_messages import ProcesarPaginaResultados

        coordinator = Mock()
        coordinator.tell = AsyncMock()

        actor = BJVDiscoveryActor(
            coordinator=coordinator,
            rate_limiter=BJVRateLimiter(),
        )

        with patch.object(actor, '_fetch_search_page', new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = "<html><body></body></html>"

            await actor.start()
            msg = ProcesarPaginaResultados(
                correlation_id="corr-123",
                numero_pagina=2,
                url_pagina="https://biblio.juridicas.unam.mx/bjv/resultados?page=2",
            )
            await actor.handle_message(msg)
            await actor.stop()

        mock_fetch.assert_called()


class TestBJVDiscoveryActorDeduplication:
    """Tests for discovery deduplication."""

    @pytest.mark.asyncio
    async def test_deduplicate_discoveries(self):
        """Duplicate libro IDs are not forwarded twice."""
        from src.infrastructure.actors.bjv_discovery_actor import BJVDiscoveryActor
        from src.infrastructure.actors.bjv_rate_limiter import BJVRateLimiter

        coordinator = Mock()
        coordinator.tell = AsyncMock()

        actor = BJVDiscoveryActor(
            coordinator=coordinator,
            rate_limiter=BJVRateLimiter(),
        )

        # Simulate discovering same book twice
        actor._discovered_ids.add("libro-123")

        # This should not add again
        added = actor._register_discovery("libro-123")

        assert added is False
        assert "libro-123" in actor._discovered_ids

    def test_new_discovery_registered(self):
        """New libro ID is registered and returns True."""
        from src.infrastructure.actors.bjv_discovery_actor import BJVDiscoveryActor
        from src.infrastructure.actors.bjv_rate_limiter import BJVRateLimiter

        actor = BJVDiscoveryActor(
            coordinator=Mock(),
            rate_limiter=BJVRateLimiter(),
        )

        added = actor._register_discovery("new-libro")

        assert added is True
        assert "new-libro" in actor._discovered_ids


class TestBJVDiscoveryActorEvents:
    """Tests for event emission."""

    @pytest.mark.asyncio
    async def test_emits_libro_descubierto(self):
        """Actor emits LibroDescubierto via coordinator."""
        from src.infrastructure.actors.bjv_discovery_actor import BJVDiscoveryActor
        from src.infrastructure.actors.bjv_rate_limiter import BJVRateLimiter
        from src.infrastructure.actors.bjv_messages import DescargarLibro

        coordinator = Mock()
        coordinator.tell = AsyncMock()

        actor = BJVDiscoveryActor(
            coordinator=coordinator,
            rate_limiter=BJVRateLimiter(),
        )

        await actor._emit_discovery(
            correlation_id="corr-123",
            libro_id="4567",
            titulo="Derecho Civil",
            url_detalle="https://biblio.juridicas.unam.mx/bjv/detalle-libro/4567",
        )

        coordinator.tell.assert_called()
        call_args = coordinator.tell.call_args[0][0]
        assert isinstance(call_args, DescargarLibro)
        assert call_args.libro_id == "4567"

    @pytest.mark.asyncio
    async def test_rate_limiter_used(self):
        """Rate limiter is used before fetching."""
        from src.infrastructure.actors.bjv_discovery_actor import BJVDiscoveryActor
        from src.infrastructure.actors.bjv_rate_limiter import BJVRateLimiter

        rate_limiter = BJVRateLimiter()
        rate_limiter.acquire = AsyncMock()

        coordinator = Mock()
        coordinator.tell = AsyncMock()

        actor = BJVDiscoveryActor(
            coordinator=coordinator,
            rate_limiter=rate_limiter,
        )

        # Create a proper async context manager mock
        mock_response = MagicMock()
        mock_response.text = AsyncMock(return_value="<html></html>")
        mock_response.status = 200
        mock_response.raise_for_status = Mock()

        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_response)
        mock_cm.__aexit__ = AsyncMock(return_value=None)

        mock_session = MagicMock()
        mock_session.get = Mock(return_value=mock_cm)
        actor._session = mock_session

        await actor._fetch_search_page("https://example.com")

        rate_limiter.acquire.assert_called()


class TestBJVDiscoveryActorMaxResults:
    """Tests for max_resultados handling."""

    def test_max_resultados_respected(self):
        """Discovery stops after max_resultados reached."""
        from src.infrastructure.actors.bjv_discovery_actor import BJVDiscoveryActor
        from src.infrastructure.actors.bjv_rate_limiter import BJVRateLimiter

        actor = BJVDiscoveryActor(
            coordinator=Mock(),
            rate_limiter=BJVRateLimiter(),
        )

        actor._max_resultados = 5

        # Add 5 discoveries
        for i in range(5):
            actor._register_discovery(f"libro-{i}")

        # 6th should indicate limit reached
        assert actor._is_limit_reached() is True


class TestBJVDiscoveryActorPagination:
    """Tests for pagination handling."""

    @pytest.mark.asyncio
    async def test_pagination_navigation(self):
        """Actor navigates to next page when available."""
        from src.infrastructure.actors.bjv_discovery_actor import BJVDiscoveryActor
        from src.infrastructure.actors.bjv_rate_limiter import BJVRateLimiter
        from src.infrastructure.adapters.bjv_search_parser import PaginacionInfo

        coordinator = Mock()
        coordinator.tell = AsyncMock()

        actor = BJVDiscoveryActor(
            coordinator=coordinator,
            rate_limiter=BJVRateLimiter(),
        )

        pagination = PaginacionInfo(
            pagina_actual=1,
            total_paginas=5,
            total_resultados=100,
        )

        assert actor._should_continue_pagination(pagination, current_discovered=10, max_results=50)

    def test_pagination_stops_at_last_page(self):
        """Pagination stops at last page."""
        from src.infrastructure.actors.bjv_discovery_actor import BJVDiscoveryActor
        from src.infrastructure.actors.bjv_rate_limiter import BJVRateLimiter
        from src.infrastructure.adapters.bjv_search_parser import PaginacionInfo

        actor = BJVDiscoveryActor(
            coordinator=Mock(),
            rate_limiter=BJVRateLimiter(),
        )

        pagination = PaginacionInfo(
            pagina_actual=5,
            total_paginas=5,
            total_resultados=100,
        )

        assert actor._should_continue_pagination(pagination, current_discovered=50, max_results=100) is False


class TestBJVDiscoveryActorErrorHandling:
    """Tests for error handling."""

    @pytest.mark.asyncio
    async def test_error_handling_network(self):
        """Network errors are handled gracefully."""
        from src.infrastructure.actors.bjv_discovery_actor import BJVDiscoveryActor
        from src.infrastructure.actors.bjv_rate_limiter import BJVRateLimiter
        from src.infrastructure.actors.bjv_messages import ErrorProcesamiento
        import aiohttp

        coordinator = Mock()
        coordinator.tell = AsyncMock()

        actor = BJVDiscoveryActor(
            coordinator=coordinator,
            rate_limiter=BJVRateLimiter(),
        )

        # Simulate network error
        error = aiohttp.ClientError("Connection failed")
        error_msg = actor._create_error_message("corr-123", None, error)

        assert isinstance(error_msg, ErrorProcesamiento)
        assert error_msg.recuperable is True
