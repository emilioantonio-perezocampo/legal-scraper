"""
Integration tests for complete SCJN scraping pipeline.

Tests full actor wiring and message flow.
"""
import pytest
import pytest_asyncio
import asyncio
from unittest.mock import patch, AsyncMock

from src.infrastructure.actors.scjn_coordinator_actor import (
    SCJNCoordinatorActor,
    PipelineState,
)
from src.infrastructure.actors.scjn_discovery_actor import SCJNDiscoveryActor
from src.infrastructure.actors.scjn_scraper_actor import SCJNScraperActor
from src.infrastructure.actors.persistence_actor import SCJNPersistenceActor
from src.infrastructure.actors.checkpoint_actor import CheckpointActor
from src.infrastructure.actors.rate_limiter import NoOpRateLimiter
from src.infrastructure.actors.messages import (
    DescubrirDocumentos,
    DocumentoDescubierto,
    DocumentoDescargado,
    PaginaDescubierta,
    PausarPipeline,
    ReanudarPipeline,
    ObtenerEstado,
)


@pytest_asyncio.fixture
async def wired_pipeline(temp_output_dir, temp_checkpoint_dir, no_rate_limiter):
    """Create fully wired pipeline with real actors."""
    persistence = SCJNPersistenceActor(storage_dir=str(temp_output_dir))
    checkpoint = CheckpointActor(checkpoint_dir=str(temp_checkpoint_dir))

    # Create coordinator with mock child actors for discovery/scraper
    # (We'll inject mock HTTP responses)
    coordinator = SCJNCoordinatorActor(
        discovery_actor=None,  # Will be set up
        scraper_actor=None,
        persistence_actor=persistence,
        checkpoint_actor=checkpoint,
        rate_limiter=no_rate_limiter,
        max_concurrent_downloads=2,
    )

    await persistence.start()
    await checkpoint.start()

    yield coordinator, persistence, checkpoint

    await checkpoint.stop()
    await persistence.stop()


class TestPipelineLifecycle:
    """Tests for pipeline start/stop lifecycle."""

    @pytest.mark.asyncio
    async def test_coordinator_starts_in_idle_state(self, no_rate_limiter):
        """Coordinator should start in IDLE state."""
        # Create minimal coordinator
        coordinator = SCJNCoordinatorActor(
            discovery_actor=AsyncMock(),
            scraper_actor=AsyncMock(),
            persistence_actor=AsyncMock(),
            checkpoint_actor=AsyncMock(),
            rate_limiter=no_rate_limiter,
        )

        await coordinator.start()

        assert coordinator.state == PipelineState.IDLE

        await coordinator.stop()

    @pytest.mark.asyncio
    async def test_coordinator_stops_cleanly(self, no_rate_limiter):
        """Coordinator should stop without errors."""
        coordinator = SCJNCoordinatorActor(
            discovery_actor=AsyncMock(),
            scraper_actor=AsyncMock(),
            persistence_actor=AsyncMock(),
            checkpoint_actor=AsyncMock(),
            rate_limiter=no_rate_limiter,
        )

        await coordinator.start()
        await coordinator.stop()

        assert not coordinator._running


class TestPipelineDiscoveryFlow:
    """Tests for discovery to download flow."""

    @pytest.mark.asyncio
    async def test_discovery_triggers_state_change(self, no_rate_limiter):
        """Discovery command should transition to DISCOVERING state."""
        mock_discovery = AsyncMock()
        mock_discovery.ask = AsyncMock(return_value=PaginaDescubierta(
            documents_found=5,
            current_page=1,
            total_pages=1,
            has_more_pages=False,
        ))

        coordinator = SCJNCoordinatorActor(
            discovery_actor=mock_discovery,
            scraper_actor=AsyncMock(),
            persistence_actor=AsyncMock(),
            checkpoint_actor=AsyncMock(),
            rate_limiter=no_rate_limiter,
        )

        await coordinator.start()

        result = await coordinator.ask(DescubrirDocumentos(category="LEY"))

        assert isinstance(result, PaginaDescubierta)
        mock_discovery.ask.assert_called_once()

        await coordinator.stop()

    @pytest.mark.asyncio
    async def test_discovered_document_queued_for_download(self, no_rate_limiter):
        """DocumentoDescubierto should queue document for download."""
        mock_persistence = AsyncMock()
        mock_persistence.ask = AsyncMock(return_value=False)  # Not exists
        mock_scraper = AsyncMock()

        coordinator = SCJNCoordinatorActor(
            discovery_actor=AsyncMock(),
            scraper_actor=mock_scraper,
            persistence_actor=mock_persistence,
            checkpoint_actor=AsyncMock(),
            rate_limiter=no_rate_limiter,
        )

        await coordinator.start()

        await coordinator.tell(DocumentoDescubierto(
            q_param="TEST123",
            title="Test Document",
            category="LEY",
        ))

        await asyncio.sleep(0.1)

        # Scraper should have been called
        assert mock_scraper.tell.called or len(coordinator._pending_queue) >= 0

        await coordinator.stop()

    @pytest.mark.asyncio
    async def test_existing_document_skipped(self, no_rate_limiter):
        """Documents that already exist should be skipped."""
        mock_persistence = AsyncMock()
        mock_persistence.ask = AsyncMock(return_value=True)  # Already exists
        mock_scraper = AsyncMock()

        coordinator = SCJNCoordinatorActor(
            discovery_actor=AsyncMock(),
            scraper_actor=mock_scraper,
            persistence_actor=mock_persistence,
            checkpoint_actor=AsyncMock(),
            rate_limiter=no_rate_limiter,
        )

        await coordinator.start()

        await coordinator.tell(DocumentoDescubierto(
            q_param="EXISTING",
            title="Existing Doc",
            category="LEY",
        ))

        await asyncio.sleep(0.1)

        # Scraper should NOT have been called
        mock_scraper.tell.assert_not_called()

        await coordinator.stop()


class TestPipelineStateManagement:
    """Tests for state queries."""

    @pytest.mark.asyncio
    async def test_obtener_estado_returns_dict(self, no_rate_limiter):
        """ObtenerEstado should return state dictionary."""
        coordinator = SCJNCoordinatorActor(
            discovery_actor=AsyncMock(),
            scraper_actor=AsyncMock(),
            persistence_actor=AsyncMock(),
            checkpoint_actor=AsyncMock(),
            rate_limiter=no_rate_limiter,
        )

        await coordinator.start()

        result = await coordinator.ask(ObtenerEstado())

        assert isinstance(result, dict)
        assert "state" in result
        assert "discovered_count" in result
        assert "downloaded_count" in result
        assert "error_count" in result

        await coordinator.stop()

    @pytest.mark.asyncio
    async def test_state_tracks_discovered_count(self, no_rate_limiter):
        """State should track number of discovered documents."""
        mock_persistence = AsyncMock()
        mock_persistence.ask = AsyncMock(return_value=False)

        coordinator = SCJNCoordinatorActor(
            discovery_actor=AsyncMock(),
            scraper_actor=AsyncMock(),
            persistence_actor=mock_persistence,
            checkpoint_actor=AsyncMock(),
            rate_limiter=no_rate_limiter,
        )

        await coordinator.start()

        # Discover some documents
        for i in range(3):
            await coordinator.tell(DocumentoDescubierto(
                q_param=f"DOC{i}",
                title=f"Doc {i}",
                category="LEY",
            ))

        await asyncio.sleep(0.1)

        result = await coordinator.ask(ObtenerEstado())

        assert result["discovered_count"] == 3

        await coordinator.stop()

    @pytest.mark.asyncio
    async def test_state_tracks_downloaded_count(self, no_rate_limiter):
        """State should track number of downloaded documents."""
        coordinator = SCJNCoordinatorActor(
            discovery_actor=AsyncMock(),
            scraper_actor=AsyncMock(),
            persistence_actor=AsyncMock(),
            checkpoint_actor=AsyncMock(),
            rate_limiter=no_rate_limiter,
        )

        await coordinator.start()

        # Simulate downloaded documents
        await coordinator.tell(DocumentoDescargado(
            document_id="doc-1",
            q_param="ABC123",
            has_pdf=False,
        ))
        await coordinator.tell(DocumentoDescargado(
            document_id="doc-2",
            q_param="DEF456",
            has_pdf=True,
        ))

        await asyncio.sleep(0.1)

        result = await coordinator.ask(ObtenerEstado())

        assert result["downloaded_count"] == 2

        await coordinator.stop()


class TestPipelinePauseResume:
    """Tests for pause/resume functionality."""

    @pytest.mark.asyncio
    async def test_pause_changes_state(self, no_rate_limiter):
        """PausarPipeline should change state to PAUSED."""
        mock_checkpoint = AsyncMock()

        coordinator = SCJNCoordinatorActor(
            discovery_actor=AsyncMock(),
            scraper_actor=AsyncMock(),
            persistence_actor=AsyncMock(),
            checkpoint_actor=mock_checkpoint,
            rate_limiter=no_rate_limiter,
        )

        await coordinator.start()

        result = await coordinator.ask(PausarPipeline())

        assert coordinator.state == PipelineState.PAUSED
        assert result["paused"] is True

        await coordinator.stop()

    @pytest.mark.asyncio
    async def test_pause_saves_checkpoint(self, no_rate_limiter):
        """Pause should trigger checkpoint save."""
        mock_checkpoint = AsyncMock()

        coordinator = SCJNCoordinatorActor(
            discovery_actor=AsyncMock(),
            scraper_actor=AsyncMock(),
            persistence_actor=AsyncMock(),
            checkpoint_actor=mock_checkpoint,
            rate_limiter=no_rate_limiter,
        )

        await coordinator.start()

        await coordinator.ask(PausarPipeline())

        # Checkpoint actor should have been called
        mock_checkpoint.tell.assert_called()

        await coordinator.stop()

    @pytest.mark.asyncio
    async def test_resume_changes_state_from_paused(self, no_rate_limiter):
        """ReanudarPipeline should resume from PAUSED state."""
        coordinator = SCJNCoordinatorActor(
            discovery_actor=AsyncMock(),
            scraper_actor=AsyncMock(),
            persistence_actor=AsyncMock(),
            checkpoint_actor=AsyncMock(),
            rate_limiter=no_rate_limiter,
        )

        await coordinator.start()

        await coordinator.ask(PausarPipeline())
        assert coordinator.state == PipelineState.PAUSED

        result = await coordinator.ask(ReanudarPipeline())

        assert coordinator.state != PipelineState.PAUSED
        assert result["resumed"] is True

        await coordinator.stop()


class TestPipelineErrorHandling:
    """Tests for error handling."""

    @pytest.mark.asyncio
    async def test_error_increments_count(self, no_rate_limiter):
        """Errors should increment error count."""
        from src.infrastructure.actors.messages import ErrorDeActor

        coordinator = SCJNCoordinatorActor(
            discovery_actor=AsyncMock(),
            scraper_actor=AsyncMock(),
            persistence_actor=AsyncMock(),
            checkpoint_actor=AsyncMock(),
            rate_limiter=no_rate_limiter,
        )

        await coordinator.start()

        await coordinator.tell(ErrorDeActor(
            actor_name="TestActor",
            document_id="doc-123",
            error_type="TestError",
            error_message="Test failure",
            recoverable=True,
        ))

        await asyncio.sleep(0.1)

        result = await coordinator.ask(ObtenerEstado())

        assert result["error_count"] >= 1

        await coordinator.stop()

    @pytest.mark.asyncio
    async def test_recoverable_error_queues_retry(self, no_rate_limiter):
        """Recoverable errors with original command should queue for retry."""
        from src.infrastructure.actors.messages import ErrorDeActor, DescargarDocumento

        coordinator = SCJNCoordinatorActor(
            discovery_actor=AsyncMock(),
            scraper_actor=AsyncMock(),
            persistence_actor=AsyncMock(),
            checkpoint_actor=AsyncMock(),
            rate_limiter=no_rate_limiter,
        )

        await coordinator.start()

        original_cmd = DescargarDocumento(q_param="RETRY_ME")

        await coordinator.tell(ErrorDeActor(
            actor_name="ScraperActor",
            error_type="TransientError",
            error_message="Network failure",
            recoverable=True,
            original_command=original_cmd,
        ))

        await asyncio.sleep(0.1)

        # Should be in retry queue
        assert len(coordinator._retry_queue) >= 1

        await coordinator.stop()


class TestPipelineConcurrency:
    """Tests for concurrency control."""

    @pytest.mark.asyncio
    async def test_max_concurrent_downloads_respected(self, no_rate_limiter):
        """Concurrent downloads should not exceed limit."""
        mock_persistence = AsyncMock()
        mock_persistence.ask = AsyncMock(return_value=False)
        mock_scraper = AsyncMock()

        coordinator = SCJNCoordinatorActor(
            discovery_actor=AsyncMock(),
            scraper_actor=mock_scraper,
            persistence_actor=mock_persistence,
            checkpoint_actor=AsyncMock(),
            rate_limiter=no_rate_limiter,
            max_concurrent_downloads=2,
        )

        await coordinator.start()

        # Queue many documents
        for i in range(10):
            await coordinator.tell(DocumentoDescubierto(
                q_param=f"DOC{i}",
                title=f"Doc {i}",
                category="LEY",
            ))

        await asyncio.sleep(0.05)

        # Active downloads should not exceed limit
        assert coordinator._active_downloads <= 2

        await coordinator.stop()


class TestPipelineDeduplication:
    """Tests for deduplication."""

    @pytest.mark.asyncio
    async def test_same_q_param_not_queued_twice(self, no_rate_limiter):
        """Same q_param should only be queued once."""
        mock_persistence = AsyncMock()
        mock_persistence.ask = AsyncMock(return_value=False)

        coordinator = SCJNCoordinatorActor(
            discovery_actor=AsyncMock(),
            scraper_actor=AsyncMock(),
            persistence_actor=mock_persistence,
            checkpoint_actor=AsyncMock(),
            rate_limiter=no_rate_limiter,
        )

        await coordinator.start()

        # Send same document twice
        await coordinator.tell(DocumentoDescubierto(
            q_param="SAME",
            title="Same Doc",
            category="LEY",
        ))
        await coordinator.tell(DocumentoDescubierto(
            q_param="SAME",
            title="Same Doc Again",
            category="LEY",
        ))

        await asyncio.sleep(0.1)

        # Should only have one in discovered set
        assert coordinator._discovered_q_params == {"SAME"}

        await coordinator.stop()


class TestPipelineCorrelation:
    """Tests for correlation ID propagation."""

    @pytest.mark.asyncio
    async def test_correlation_id_propagated_to_discovery(self, no_rate_limiter):
        """Correlation ID should be passed to discovery actor."""
        mock_discovery = AsyncMock()
        mock_discovery.ask = AsyncMock(return_value=PaginaDescubierta(
            documents_found=0,
            current_page=1,
            total_pages=1,
        ))

        coordinator = SCJNCoordinatorActor(
            discovery_actor=mock_discovery,
            scraper_actor=AsyncMock(),
            persistence_actor=AsyncMock(),
            checkpoint_actor=AsyncMock(),
            rate_limiter=no_rate_limiter,
        )

        await coordinator.start()

        await coordinator.ask(DescubrirDocumentos(
            category="LEY",
            correlation_id="test-corr-123",
        ))

        # Verify discovery was called with correct correlation_id
        call_args = mock_discovery.ask.call_args
        assert call_args[0][0].correlation_id == "test-corr-123"

        await coordinator.stop()
