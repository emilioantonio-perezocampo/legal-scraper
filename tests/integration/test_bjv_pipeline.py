"""
Integration tests for BJV scraping pipeline.

Tests the full actor system working together.

Target: ~15 tests
"""
import pytest
import asyncio
from unittest.mock import Mock, MagicMock, AsyncMock, patch


class TestBJVPipelineIntegration:
    """Integration tests for the full pipeline."""

    @pytest.mark.asyncio
    async def test_coordinator_creates_all_actors(self):
        """Coordinator creates all child actors on start."""
        from src.infrastructure.actors.bjv_coordinator_actor import BJVCoordinatorActor
        from src.infrastructure.actors.bjv_discovery_actor import BJVDiscoveryActor
        from src.infrastructure.actors.bjv_scraper_actor import BJVScraperActor
        from src.infrastructure.actors.bjv_pdf_actor import BJVPDFActor
        from src.infrastructure.actors.bjv_fragmentador_actor import BJVFragmentadorActor

        coordinator = BJVCoordinatorActor()
        await coordinator.start()

        try:
            assert isinstance(coordinator._discovery_actor, BJVDiscoveryActor)
            assert isinstance(coordinator._scraper_actor, BJVScraperActor)
            assert isinstance(coordinator._pdf_actor, BJVPDFActor)
            assert isinstance(coordinator._fragmentador_actor, BJVFragmentadorActor)
        finally:
            await coordinator.stop()

    @pytest.mark.asyncio
    async def test_pipeline_state_transitions(self):
        """Pipeline transitions through expected states."""
        from src.infrastructure.actors.bjv_coordinator_actor import (
            BJVCoordinatorActor,
            PipelineState,
        )
        from src.infrastructure.actors.bjv_messages import (
            IniciarPipeline,
            PausarPipeline,
            ReanudarPipeline,
            DetenerPipeline,
        )

        coordinator = BJVCoordinatorActor()
        await coordinator.start()

        try:
            # Initial state
            assert coordinator.state == PipelineState.IDLE

            # Start
            await coordinator.handle_message(IniciarPipeline(session_id="test"))
            assert coordinator.state == PipelineState.RUNNING

            # Pause
            await coordinator.handle_message(PausarPipeline())
            assert coordinator.state == PipelineState.PAUSED

            # Resume
            await coordinator.handle_message(ReanudarPipeline())
            assert coordinator.state == PipelineState.RUNNING

            # Stop
            await coordinator.handle_message(DetenerPipeline())
            assert coordinator.state == PipelineState.COMPLETED
        finally:
            await coordinator.stop()

    @pytest.mark.asyncio
    async def test_rate_limiter_shared_between_actors(self):
        """All actors share the same rate limiter."""
        from src.infrastructure.actors.bjv_coordinator_actor import BJVCoordinatorActor

        coordinator = BJVCoordinatorActor()
        await coordinator.start()

        try:
            # All network actors should use the same rate limiter
            assert coordinator._discovery_actor._rate_limiter is coordinator._rate_limiter
            assert coordinator._scraper_actor._rate_limiter is coordinator._rate_limiter
            assert coordinator._pdf_actor._rate_limiter is coordinator._rate_limiter
        finally:
            await coordinator.stop()

    @pytest.mark.asyncio
    async def test_error_tracking(self):
        """Errors are tracked by coordinator."""
        from src.infrastructure.actors.bjv_coordinator_actor import BJVCoordinatorActor
        from src.infrastructure.actors.bjv_messages import (
            IniciarPipeline,
            ErrorProcesamiento,
        )

        coordinator = BJVCoordinatorActor()
        await coordinator.start()

        try:
            await coordinator.handle_message(IniciarPipeline(session_id="test"))

            # Send multiple errors
            for i in range(5):
                await coordinator.handle_message(ErrorProcesamiento(
                    correlation_id=f"corr-{i}",
                    libro_id=f"libro-{i}",
                    actor_origen="BJVScraper",
                    tipo_error="NetworkError",
                    mensaje_error="Failed",
                ))

            assert coordinator._total_errores == 5
        finally:
            await coordinator.stop()


class TestBJVActorCommunication:
    """Tests for actor message passing."""

    @pytest.mark.asyncio
    async def test_discovery_forwards_to_coordinator(self):
        """Discovery actor forwards discoveries to coordinator."""
        from src.infrastructure.actors.bjv_coordinator_actor import BJVCoordinatorActor
        from src.infrastructure.actors.bjv_messages import IniciarPipeline, DescargarLibro

        coordinator = BJVCoordinatorActor()
        await coordinator.start()

        try:
            await coordinator.handle_message(IniciarPipeline(session_id="test"))

            # Simulate discovery forwarding a download request
            msg = DescargarLibro(
                correlation_id="corr-123",
                libro_id="libro-456",
                url_detalle="https://example.com/book/456",
            )

            await coordinator.handle_message(msg)

            # Book should be tracked as pending
            assert "libro-456" in coordinator._libros_pendientes
        finally:
            await coordinator.stop()

    @pytest.mark.asyncio
    async def test_scraper_results_processed(self):
        """Scraper results are processed by coordinator."""
        from src.infrastructure.actors.bjv_coordinator_actor import BJVCoordinatorActor
        from src.infrastructure.actors.bjv_messages import (
            IniciarPipeline,
            DescargarLibro,
            LibroListo,
        )
        from src.domain.bjv_entities import LibroBJV
        from src.domain.bjv_value_objects import IdentificadorLibro

        coordinator = BJVCoordinatorActor()
        await coordinator.start()

        try:
            await coordinator.handle_message(IniciarPipeline(session_id="test"))

            # Add to pending
            await coordinator.handle_message(DescargarLibro(
                correlation_id="corr-123",
                libro_id="libro-456",
                url_detalle="https://example.com/book/456",
            ))

            # Send result
            libro = LibroBJV(
                id=IdentificadorLibro(bjv_id="libro-456"),
                titulo="Test Book",
            )
            await coordinator.handle_message(LibroListo(
                correlation_id="corr-123",
                libro_id="libro-456",
                libro=libro,
            ))

            # Book should be processed
            assert "libro-456" in coordinator._libros_procesados
            assert coordinator._total_procesados == 1
        finally:
            await coordinator.stop()


class TestBJVPipelineCheckpoint:
    """Tests for checkpoint functionality."""

    @pytest.mark.asyncio
    async def test_checkpoint_contains_state(self):
        """Checkpoint captures current state."""
        from src.infrastructure.actors.bjv_coordinator_actor import BJVCoordinatorActor
        from src.infrastructure.actors.bjv_messages import (
            IniciarPipeline,
            BusquedaCompletada,
            DescargarLibro,
            LibroListo,
            PausarPipeline,
        )
        from src.domain.bjv_entities import LibroBJV
        from src.domain.bjv_value_objects import IdentificadorLibro

        coordinator = BJVCoordinatorActor()
        await coordinator.start()

        try:
            await coordinator.handle_message(IniciarPipeline(session_id="test"))

            # Simulate some progress
            await coordinator.handle_message(BusquedaCompletada(
                correlation_id="corr-123",
                total_descubiertos=10,
                total_paginas=1,
            ))

            # Add libro to pending first (simulating discovery)
            await coordinator.handle_message(DescargarLibro(
                correlation_id="corr-123",
                libro_id="libro-1",
                url_detalle="https://example.com/1",
            ))

            libro = LibroBJV(
                id=IdentificadorLibro(bjv_id="libro-1"),
                titulo="Test Book",
            )
            await coordinator.handle_message(LibroListo(
                correlation_id="corr-123",
                libro_id="libro-1",
                libro=libro,
            ))

            # Pause to create checkpoint
            await coordinator.handle_message(PausarPipeline())

            checkpoint = coordinator.get_checkpoint()

            assert checkpoint is not None
            assert checkpoint.total_libros_descubiertos == 10
            assert checkpoint.total_libros_descargados == 1
        finally:
            await coordinator.stop()

    @pytest.mark.asyncio
    async def test_statistics_available(self):
        """Pipeline statistics are available."""
        from src.infrastructure.actors.bjv_coordinator_actor import BJVCoordinatorActor
        from src.infrastructure.actors.bjv_messages import IniciarPipeline

        coordinator = BJVCoordinatorActor()
        await coordinator.start()

        try:
            await coordinator.handle_message(IniciarPipeline(session_id="test-123"))

            stats = coordinator.get_statistics()

            assert stats["session_id"] == "test-123"
            assert stats["state"] == "RUNNING"
            assert "total_descubiertos" in stats
            assert "total_procesados" in stats
            assert "total_errores" in stats
        finally:
            await coordinator.stop()


class TestBJVActorLifecycle:
    """Tests for actor lifecycle management."""

    @pytest.mark.asyncio
    async def test_actors_stopped_on_coordinator_stop(self):
        """All actors are stopped when coordinator stops."""
        from src.infrastructure.actors.bjv_coordinator_actor import BJVCoordinatorActor

        coordinator = BJVCoordinatorActor()
        await coordinator.start()

        # Get references to child actors
        discovery = coordinator._discovery_actor
        scraper = coordinator._scraper_actor
        pdf = coordinator._pdf_actor
        fragmentador = coordinator._fragmentador_actor

        await coordinator.stop()

        # All should be stopped
        assert not discovery._running
        assert not scraper._running
        assert not pdf._running
        assert not fragmentador._running

    @pytest.mark.asyncio
    async def test_http_sessions_closed(self):
        """HTTP sessions are closed on stop."""
        from src.infrastructure.actors.bjv_coordinator_actor import BJVCoordinatorActor

        coordinator = BJVCoordinatorActor()
        await coordinator.start()

        # Get sessions
        discovery_session = coordinator._discovery_actor._session
        scraper_session = coordinator._scraper_actor._session
        pdf_session = coordinator._pdf_actor._session

        await coordinator.stop()

        # Sessions should be closed
        assert discovery_session.closed
        assert scraper_session.closed
        assert pdf_session.closed


class TestBJVMessageRouting:
    """Tests for message routing."""

    @pytest.mark.asyncio
    async def test_ask_returns_statistics(self):
        """Ask with ObtenerEstado returns statistics."""
        from src.infrastructure.actors.bjv_coordinator_actor import BJVCoordinatorActor
        from src.infrastructure.actors.bjv_messages import IniciarPipeline, ObtenerEstado

        coordinator = BJVCoordinatorActor()
        await coordinator.start()

        try:
            await coordinator.handle_message(IniciarPipeline(session_id="test"))

            result = await coordinator.handle_message(ObtenerEstado())

            assert isinstance(result, dict)
            assert "state" in result
        finally:
            await coordinator.stop()

    @pytest.mark.asyncio
    async def test_completion_detection(self):
        """Pipeline detects completion correctly."""
        from src.infrastructure.actors.bjv_coordinator_actor import (
            BJVCoordinatorActor,
            PipelineState,
        )
        from src.infrastructure.actors.bjv_messages import (
            IniciarPipeline,
            BusquedaCompletada,
            DescargarLibro,
            LibroListo,
        )
        from src.domain.bjv_entities import LibroBJV
        from src.domain.bjv_value_objects import IdentificadorLibro

        coordinator = BJVCoordinatorActor()
        await coordinator.start()

        try:
            await coordinator.handle_message(IniciarPipeline(session_id="test"))

            # Discovery finds 1 book
            await coordinator.handle_message(DescargarLibro(
                correlation_id="corr-123",
                libro_id="libro-1",
                url_detalle="https://example.com/1",
            ))

            await coordinator.handle_message(BusquedaCompletada(
                correlation_id="corr-123",
                total_descubiertos=1,
                total_paginas=1,
            ))

            # Process the book
            libro = LibroBJV(
                id=IdentificadorLibro(bjv_id="libro-1"),
                titulo="Test",
            )
            await coordinator.handle_message(LibroListo(
                correlation_id="corr-123",
                libro_id="libro-1",
                libro=libro,
            ))

            # Should be complete
            assert coordinator.state == PipelineState.COMPLETED
        finally:
            await coordinator.stop()
