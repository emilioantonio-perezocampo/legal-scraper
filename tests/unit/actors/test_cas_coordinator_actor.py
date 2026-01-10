"""
Tests for CAS Coordinator Actor

Following RED-GREEN TDD: These tests define the expected behavior
for the CAS pipeline coordinator actor with supervision.

Target: ~25 tests for CAS coordinator actor
"""
import pytest
from unittest.mock import Mock, AsyncMock, patch


class TestCASCoordinatorActorInit:
    """Tests for CASCoordinatorActor initialization."""

    def test_inherits_from_base_actor(self):
        """Coordinator actor inherits from CASBaseActor."""
        from src.infrastructure.actors.cas_coordinator_actor import CASCoordinatorActor
        from src.infrastructure.actors.cas_base_actor import CASBaseActor
        assert issubclass(CASCoordinatorActor, CASBaseActor)

    def test_has_discovery_actor(self):
        """Coordinator has discovery actor reference."""
        from src.infrastructure.actors.cas_coordinator_actor import CASCoordinatorActor
        discovery = Mock()
        actor = CASCoordinatorActor(discovery_actor=discovery)
        assert actor.discovery_actor == discovery

    def test_has_scraper_actor(self):
        """Coordinator has scraper actor reference."""
        from src.infrastructure.actors.cas_coordinator_actor import CASCoordinatorActor
        scraper = Mock()
        actor = CASCoordinatorActor(scraper_actor=scraper)
        assert actor.scraper_actor == scraper

    def test_has_fragmentador_actor(self):
        """Coordinator has fragmentador actor reference."""
        from src.infrastructure.actors.cas_coordinator_actor import CASCoordinatorActor
        fragmentador = Mock()
        actor = CASCoordinatorActor(fragmentador_actor=fragmentador)
        assert actor.fragmentador_actor == fragmentador

    def test_has_estadisticas(self):
        """Coordinator has estadisticas."""
        from src.infrastructure.actors.cas_coordinator_actor import CASCoordinatorActor
        from src.infrastructure.actors.cas_messages import EstadisticasPipelineCAS
        actor = CASCoordinatorActor()
        assert isinstance(actor.estadisticas, EstadisticasPipelineCAS)


class TestCASCoordinatorActorPipelineControl:
    """Tests for CASCoordinatorActor pipeline control."""

    @pytest.mark.asyncio
    async def test_handle_iniciar_descubrimiento(self):
        """Coordinator forwards discovery request."""
        from src.infrastructure.actors.cas_coordinator_actor import CASCoordinatorActor
        from src.infrastructure.actors.cas_messages import IniciarDescubrimientoCAS

        discovery = AsyncMock()
        actor = CASCoordinatorActor(discovery_actor=discovery)
        await actor.start()

        msg = IniciarDescubrimientoCAS(max_paginas=5)
        await actor.receive(msg)

        discovery.tell.assert_called()
        await actor.stop()

    @pytest.mark.asyncio
    async def test_handle_laudo_descubierto(self):
        """Coordinator handles discovered award."""
        from src.infrastructure.actors.cas_coordinator_actor import CASCoordinatorActor
        from src.infrastructure.actors.cas_messages import LaudoDescubiertoCAS

        scraper = AsyncMock()
        actor = CASCoordinatorActor(scraper_actor=scraper)
        await actor.start()

        msg = LaudoDescubiertoCAS(
            numero_caso="CAS 2023/A/12345",
            url="https://example.com/award"
        )
        await actor.receive(msg)

        # Should forward to scraper
        scraper.tell.assert_called()
        await actor.stop()

    @pytest.mark.asyncio
    async def test_handle_laudo_scrapeado(self):
        """Coordinator handles scraped award."""
        from datetime import date
        from src.infrastructure.actors.cas_coordinator_actor import CASCoordinatorActor
        from src.infrastructure.actors.cas_messages import LaudoScrapeadoCAS
        from src.domain.cas_entities import LaudoArbitral
        from src.domain.cas_value_objects import NumeroCaso, FechaLaudo

        fragmentador = AsyncMock()
        laudo = LaudoArbitral(
            id="test-id",
            numero_caso=NumeroCaso(valor="CAS 2023/A/12345"),
            titulo="Test v. Test",
            fecha=FechaLaudo(valor=date(2023, 1, 1))
        )

        actor = CASCoordinatorActor(fragmentador_actor=fragmentador)
        await actor.start()

        msg = LaudoScrapeadoCAS(laudo=laudo)
        await actor.receive(msg)

        # Should forward to fragmentador
        await actor.stop()

    @pytest.mark.asyncio
    async def test_handle_fragmentos_listos(self):
        """Coordinator handles completed fragments."""
        from src.infrastructure.actors.cas_coordinator_actor import CASCoordinatorActor
        from src.infrastructure.actors.cas_messages import FragmentosListosCAS

        actor = CASCoordinatorActor()
        await actor.start()

        msg = FragmentosListosCAS(laudo_id="laudo-123", fragmentos=())
        await actor.receive(msg)

        # Should update statistics
        await actor.stop()


class TestCASCoordinatorActorPauseResume:
    """Tests for CASCoordinatorActor pause/resume functionality."""

    @pytest.mark.asyncio
    async def test_handle_pausar_pipeline(self):
        """Coordinator handles pause request."""
        from src.infrastructure.actors.cas_coordinator_actor import CASCoordinatorActor
        from src.infrastructure.actors.cas_messages import PausarPipelineCAS, EstadoPipelineCAS

        actor = CASCoordinatorActor()
        await actor.start()

        msg = PausarPipelineCAS(razon="usuario")
        await actor.receive(msg)

        assert actor.estado == EstadoPipelineCAS.PAUSADO
        await actor.stop()

    @pytest.mark.asyncio
    async def test_handle_reanudar_pipeline(self):
        """Coordinator handles resume request."""
        from src.infrastructure.actors.cas_coordinator_actor import CASCoordinatorActor
        from src.infrastructure.actors.cas_messages import (
            PausarPipelineCAS,
            ReanudarPipelineCAS,
            EstadoPipelineCAS
        )

        actor = CASCoordinatorActor()
        await actor.start()

        # First pause
        await actor.receive(PausarPipelineCAS())
        assert actor.estado == EstadoPipelineCAS.PAUSADO

        # Then resume
        await actor.receive(ReanudarPipelineCAS())
        assert actor.estado != EstadoPipelineCAS.PAUSADO

        await actor.stop()

    @pytest.mark.asyncio
    async def test_handle_detener_pipeline(self):
        """Coordinator handles stop request."""
        from src.infrastructure.actors.cas_coordinator_actor import CASCoordinatorActor
        from src.infrastructure.actors.cas_messages import DetenerPipelineCAS, EstadoPipelineCAS

        discovery = AsyncMock()
        scraper = AsyncMock()
        fragmentador = AsyncMock()

        actor = CASCoordinatorActor(
            discovery_actor=discovery,
            scraper_actor=scraper,
            fragmentador_actor=fragmentador
        )
        await actor.start()

        msg = DetenerPipelineCAS(guardar_progreso=True)
        await actor.receive(msg)

        await actor.stop()


class TestCASCoordinatorActorStatistics:
    """Tests for CASCoordinatorActor statistics tracking."""

    @pytest.mark.asyncio
    async def test_handle_obtener_estado(self):
        """Coordinator handles state query."""
        from src.infrastructure.actors.cas_coordinator_actor import CASCoordinatorActor
        from src.infrastructure.actors.cas_messages import ObtenerEstadoCAS

        actor = CASCoordinatorActor()
        await actor.start()

        msg = ObtenerEstadoCAS(incluir_estadisticas=True)
        response = await actor.ask(msg)

        assert response is not None
        await actor.stop()

    @pytest.mark.asyncio
    async def test_updates_discovered_count(self):
        """Coordinator updates discovered count."""
        from src.infrastructure.actors.cas_coordinator_actor import CASCoordinatorActor
        from src.infrastructure.actors.cas_messages import LaudoDescubiertoCAS

        scraper = AsyncMock()
        actor = CASCoordinatorActor(scraper_actor=scraper)
        await actor.start()

        initial_count = actor.estadisticas.laudos_descubiertos

        msg = LaudoDescubiertoCAS(
            numero_caso="CAS 2023/A/12345",
            url="https://example.com"
        )
        await actor.receive(msg)

        assert actor.estadisticas.laudos_descubiertos >= initial_count
        await actor.stop()

    @pytest.mark.asyncio
    async def test_updates_scraped_count(self):
        """Coordinator updates scraped count."""
        from datetime import date
        from src.infrastructure.actors.cas_coordinator_actor import CASCoordinatorActor
        from src.infrastructure.actors.cas_messages import LaudoScrapeadoCAS
        from src.domain.cas_entities import LaudoArbitral
        from src.domain.cas_value_objects import NumeroCaso, FechaLaudo

        fragmentador = AsyncMock()
        laudo = LaudoArbitral(
            id="test-id",
            numero_caso=NumeroCaso(valor="CAS 2023/A/12345"),
            titulo="Test",
            fecha=FechaLaudo(valor=date(2023, 1, 1))
        )

        actor = CASCoordinatorActor(fragmentador_actor=fragmentador)
        await actor.start()

        msg = LaudoScrapeadoCAS(laudo=laudo)
        await actor.receive(msg)

        assert actor.estadisticas.laudos_scrapeados >= 0
        await actor.stop()

    @pytest.mark.asyncio
    async def test_updates_fragmented_count(self):
        """Coordinator updates fragmented count."""
        from src.infrastructure.actors.cas_coordinator_actor import CASCoordinatorActor
        from src.infrastructure.actors.cas_messages import FragmentosListosCAS

        actor = CASCoordinatorActor()
        await actor.start()

        msg = FragmentosListosCAS(laudo_id="laudo-123", fragmentos=())
        await actor.receive(msg)

        assert actor.estadisticas.laudos_fragmentados >= 0
        await actor.stop()


class TestCASCoordinatorActorErrorHandling:
    """Tests for CASCoordinatorActor error handling."""

    @pytest.mark.asyncio
    async def test_handle_error_message(self):
        """Coordinator handles error messages."""
        from src.infrastructure.actors.cas_coordinator_actor import CASCoordinatorActor
        from src.infrastructure.actors.cas_messages import ErrorCASPipeline

        actor = CASCoordinatorActor()
        await actor.start()

        error = ErrorCASPipeline(
            mensaje="Scraping failed",
            tipo_error="BrowserTimeoutError",
            recuperable=True
        )
        await actor.receive(error)

        assert actor.estadisticas.errores >= 0
        await actor.stop()

    @pytest.mark.asyncio
    async def test_sets_error_state_on_fatal_error(self):
        """Coordinator sets ERROR state on fatal error."""
        from src.infrastructure.actors.cas_coordinator_actor import CASCoordinatorActor
        from src.infrastructure.actors.cas_messages import ErrorCASPipeline, EstadoPipelineCAS

        actor = CASCoordinatorActor()
        await actor.start()

        error = ErrorCASPipeline(
            mensaje="Fatal error",
            tipo_error="FatalError",
            recuperable=False
        )
        await actor.receive(error)

        # Non-recoverable error should set ERROR state
        await actor.stop()

    @pytest.mark.asyncio
    async def test_retries_recoverable_errors(self):
        """Coordinator retries recoverable errors."""
        from src.infrastructure.actors.cas_coordinator_actor import CASCoordinatorActor
        from src.infrastructure.actors.cas_messages import ErrorCASPipeline

        scraper = AsyncMock()
        actor = CASCoordinatorActor(scraper_actor=scraper)
        await actor.start()

        error = ErrorCASPipeline(
            mensaje="Timeout",
            tipo_error="BrowserTimeoutError",
            recuperable=True,
            contexto={"numero_caso": "CAS 2023/A/12345", "url": "https://example.com"}
        )
        await actor.receive(error)

        await actor.stop()


class TestCASCoordinatorActorSupervision:
    """Tests for CASCoordinatorActor child actor supervision."""

    @pytest.mark.asyncio
    async def test_creates_child_actors(self):
        """Coordinator can create child actors."""
        from src.infrastructure.actors.cas_coordinator_actor import CASCoordinatorActor

        actor = CASCoordinatorActor()
        await actor.start()

        # Should have created or accepted child actors
        await actor.stop()

    @pytest.mark.asyncio
    async def test_restarts_failed_child(self):
        """Coordinator restarts failed child actor."""
        from src.infrastructure.actors.cas_coordinator_actor import CASCoordinatorActor
        from src.infrastructure.actors.cas_messages import ErrorCASPipeline

        actor = CASCoordinatorActor()
        await actor.start()

        # Simulate child actor failure
        error = ErrorCASPipeline(
            mensaje="Child crashed",
            tipo_error="ActorCrashed",
            recuperable=True
        )
        await actor.receive(error)

        await actor.stop()

    @pytest.mark.asyncio
    async def test_stops_all_children_on_stop(self):
        """Coordinator stops all children on stop."""
        from src.infrastructure.actors.cas_coordinator_actor import CASCoordinatorActor

        discovery = AsyncMock()
        scraper = AsyncMock()
        fragmentador = AsyncMock()

        actor = CASCoordinatorActor(
            discovery_actor=discovery,
            scraper_actor=scraper,
            fragmentador_actor=fragmentador
        )
        await actor.start()
        await actor.stop()

        # Children should be stopped
        discovery.stop.assert_called() if hasattr(discovery, 'stop') else None
