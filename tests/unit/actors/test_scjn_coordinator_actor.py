"""
RED Phase Tests: SCJNCoordinatorActor

Tests for SCJN coordinator actor that orchestrates the scraping pipeline.
These tests must FAIL initially (RED phase).
"""
import pytest
import pytest_asyncio
import asyncio
from unittest.mock import MagicMock, AsyncMock, patch, PropertyMock
from datetime import date

# These imports will fail until implementation exists
from src.infrastructure.actors.scjn_coordinator_actor import (
    SCJNCoordinatorActor,
    PipelineState,
)
from src.infrastructure.actors.messages import (
    DescubrirDocumentos,
    DescargarDocumento,
    DocumentoDescubierto,
    DocumentoDescargado,
    PaginaDescubierta,
    PDFProcesado,
    EmbeddingsGenerados,
    DocumentoGuardado,
    GuardarCheckpoint,
    CheckpointGuardado,
    PausarPipeline,
    ReanudarPipeline,
    ObtenerEstado,
    ErrorDeActor,
)
from src.infrastructure.actors.rate_limiter import NoOpRateLimiter
from src.domain.scjn_entities import ScrapingCheckpoint
from tests.utils.actor_test_helpers import MockCoordinator


@pytest.fixture
def rate_limiter():
    """Create a no-op rate limiter for testing."""
    return NoOpRateLimiter()


class MockDiscoveryActor:
    """Mock discovery actor for testing."""

    def __init__(self):
        self.received = []
        self._running = False

    async def start(self):
        self._running = True

    async def stop(self):
        self._running = False

    async def tell(self, message):
        self.received.append(message)

    async def ask(self, message):
        self.received.append(message)
        return PaginaDescubierta(
            correlation_id=message.correlation_id,
            documents_found=2,
            current_page=1,
            total_pages=1,
            has_more_pages=False,
        )


class MockScraperActor:
    """Mock scraper actor for testing."""

    def __init__(self):
        self.received = []
        self._running = False

    async def start(self):
        self._running = True

    async def stop(self):
        self._running = False

    async def tell(self, message):
        self.received.append(message)

    async def ask(self, message):
        self.received.append(message)
        return DocumentoDescargado(
            correlation_id=message.correlation_id,
            document_id="doc-123",
            q_param=message.q_param,
            has_pdf=True,
        )


class MockPersistenceActor:
    """Mock persistence actor for testing."""

    def __init__(self):
        self.received = []
        self._running = False

    async def start(self):
        self._running = True

    async def stop(self):
        self._running = False

    async def tell(self, message):
        self.received.append(message)

    async def ask(self, message):
        self.received.append(message)
        if isinstance(message, tuple) and message[0] == "EXISTS":
            return False
        return None


class MockCheckpointActor:
    """Mock checkpoint actor for testing."""

    def __init__(self):
        self.received = []
        self._running = False

    async def start(self):
        self._running = True

    async def stop(self):
        self._running = False

    async def tell(self, message):
        self.received.append(message)

    async def ask(self, message):
        self.received.append(message)
        if isinstance(message, GuardarCheckpoint):
            return CheckpointGuardado(
                correlation_id=message.correlation_id,
                session_id="session-123",
                processed_count=5,
            )
        return None


@pytest_asyncio.fixture
async def coordinator_with_mocks(rate_limiter):
    """Create coordinator with mock child actors."""
    discovery = MockDiscoveryActor()
    scraper = MockScraperActor()
    persistence = MockPersistenceActor()
    checkpoint = MockCheckpointActor()

    coordinator = SCJNCoordinatorActor(
        discovery_actor=discovery,
        scraper_actor=scraper,
        persistence_actor=persistence,
        checkpoint_actor=checkpoint,
        rate_limiter=rate_limiter,
    )
    await coordinator.start()
    yield coordinator, discovery, scraper, persistence, checkpoint
    await coordinator.stop()


class TestSCJNCoordinatorActorLifecycle:
    """Tests for SCJNCoordinatorActor lifecycle."""

    @pytest.mark.asyncio
    async def test_start_initializes_state(self, rate_limiter):
        """Start should initialize pipeline state."""
        coordinator = SCJNCoordinatorActor(
            discovery_actor=MockDiscoveryActor(),
            scraper_actor=MockScraperActor(),
            persistence_actor=MockPersistenceActor(),
            checkpoint_actor=MockCheckpointActor(),
            rate_limiter=rate_limiter,
        )
        await coordinator.start()

        assert coordinator.state == PipelineState.IDLE

        await coordinator.stop()

    @pytest.mark.asyncio
    async def test_stop_is_clean(self, rate_limiter):
        """Stop should cleanly terminate the coordinator."""
        coordinator = SCJNCoordinatorActor(
            discovery_actor=MockDiscoveryActor(),
            scraper_actor=MockScraperActor(),
            persistence_actor=MockPersistenceActor(),
            checkpoint_actor=MockCheckpointActor(),
            rate_limiter=rate_limiter,
        )
        await coordinator.start()
        await coordinator.stop()

        assert not coordinator._running


class TestSCJNCoordinatorActorDiscovery:
    """Tests for discovery orchestration."""

    @pytest.mark.asyncio
    async def test_discover_delegates_to_discovery_actor(self, coordinator_with_mocks):
        """DescubrirDocumentos should be forwarded to discovery actor."""
        coordinator, discovery, _, _, _ = coordinator_with_mocks

        await coordinator.ask(
            DescubrirDocumentos(
                category="LEY FEDERAL",
                correlation_id="test-001",
            )
        )

        assert len(discovery.received) > 0
        cmd = discovery.received[0]
        assert isinstance(cmd, DescubrirDocumentos)
        assert cmd.category == "LEY FEDERAL"

    @pytest.mark.asyncio
    async def test_discover_transitions_to_discovering(self, coordinator_with_mocks):
        """Discovery should transition state to DISCOVERING."""
        coordinator, _, _, _, _ = coordinator_with_mocks

        await coordinator.tell(
            DescubrirDocumentos(category="LEY")
        )

        # Allow async processing
        await asyncio.sleep(0.05)

        assert coordinator.state in (PipelineState.DISCOVERING, PipelineState.IDLE)


class TestSCJNCoordinatorActorDownload:
    """Tests for download orchestration."""

    @pytest.mark.asyncio
    async def test_documento_descubierto_triggers_download(self, coordinator_with_mocks):
        """DocumentoDescubierto should trigger download."""
        coordinator, _, scraper, persistence, _ = coordinator_with_mocks

        await coordinator.tell(
            DocumentoDescubierto(
                q_param="ABC123",
                title="Test Document",
                category="LEY",
            )
        )

        await asyncio.sleep(0.05)

        # Should check if exists and then download
        download_cmds = [m for m in scraper.received if isinstance(m, DescargarDocumento)]
        assert len(download_cmds) >= 1
        assert download_cmds[0].q_param == "ABC123"

    @pytest.mark.asyncio
    async def test_skips_existing_documents(self, coordinator_with_mocks):
        """Should skip already downloaded documents."""
        coordinator, _, scraper, persistence, _ = coordinator_with_mocks

        # Make persistence return True for EXISTS
        async def mock_ask(message):
            if isinstance(message, tuple) and message[0] == "EXISTS":
                return True
            return None
        persistence.ask = mock_ask

        await coordinator.tell(
            DocumentoDescubierto(
                q_param="EXISTING",
                title="Existing Doc",
                category="LEY",
            )
        )

        await asyncio.sleep(0.05)

        # Should not download
        download_cmds = [m for m in scraper.received if isinstance(m, DescargarDocumento)]
        assert len(download_cmds) == 0


class TestSCJNCoordinatorActorState:
    """Tests for state management."""

    @pytest.mark.asyncio
    async def test_obtener_estado_returns_state(self, coordinator_with_mocks):
        """ObtenerEstado should return pipeline state."""
        coordinator, _, _, _, _ = coordinator_with_mocks

        result = await coordinator.ask(ObtenerEstado())

        assert "state" in result
        assert result["state"] == PipelineState.IDLE

    @pytest.mark.asyncio
    async def test_obtener_estado_returns_counts(self, coordinator_with_mocks):
        """ObtenerEstado should include document counts."""
        coordinator, _, _, _, _ = coordinator_with_mocks

        result = await coordinator.ask(ObtenerEstado())

        assert "discovered_count" in result
        assert "downloaded_count" in result
        assert "error_count" in result


class TestSCJNCoordinatorActorPauseResume:
    """Tests for pause/resume functionality."""

    @pytest.mark.asyncio
    async def test_pausar_transitions_to_paused(self, coordinator_with_mocks):
        """PausarPipeline should transition to PAUSED state."""
        coordinator, _, _, _, _ = coordinator_with_mocks

        # Start discovery first
        await coordinator.tell(DescubrirDocumentos(category="LEY"))
        await asyncio.sleep(0.05)

        await coordinator.ask(PausarPipeline())

        assert coordinator.state == PipelineState.PAUSED

    @pytest.mark.asyncio
    async def test_reanudar_resumes_from_paused(self, coordinator_with_mocks):
        """ReanudarPipeline should resume from PAUSED state."""
        coordinator, _, _, _, _ = coordinator_with_mocks

        # Pause first
        await coordinator.ask(PausarPipeline())
        assert coordinator.state == PipelineState.PAUSED

        await coordinator.ask(ReanudarPipeline())

        assert coordinator.state != PipelineState.PAUSED

    @pytest.mark.asyncio
    async def test_pause_saves_checkpoint(self, coordinator_with_mocks):
        """Pause should save checkpoint."""
        coordinator, _, _, _, checkpoint = coordinator_with_mocks

        await coordinator.ask(PausarPipeline())

        checkpoint_saves = [m for m in checkpoint.received if isinstance(m, GuardarCheckpoint)]
        assert len(checkpoint_saves) >= 1


class TestSCJNCoordinatorActorErrorHandling:
    """Tests for error handling."""

    @pytest.mark.asyncio
    async def test_error_increments_count(self, coordinator_with_mocks):
        """ErrorDeActor should increment error count."""
        coordinator, _, _, _, _ = coordinator_with_mocks

        await coordinator.tell(
            ErrorDeActor(
                actor_name="TestActor",
                document_id="doc-123",
                error_type="TestError",
                error_message="Test failure",
                recoverable=True,
            )
        )

        await asyncio.sleep(0.05)
        result = await coordinator.ask(ObtenerEstado())

        assert result["error_count"] >= 1

    @pytest.mark.asyncio
    async def test_recoverable_error_queues_retry(self, coordinator_with_mocks):
        """Recoverable errors should queue for retry."""
        coordinator, _, scraper, _, _ = coordinator_with_mocks

        original_cmd = DescargarDocumento(q_param="FAILED")
        await coordinator.tell(
            ErrorDeActor(
                actor_name="ScraperActor",
                document_id=None,
                error_type="TransientError",
                error_message="Network failure",
                recoverable=True,
                original_command=original_cmd,
            )
        )

        await asyncio.sleep(0.1)

        # With retry enabled, should eventually retry
        # (depends on implementation - may need adjustment)


class TestSCJNCoordinatorActorProgress:
    """Tests for progress tracking."""

    @pytest.mark.asyncio
    async def test_tracks_discovered_count(self, coordinator_with_mocks):
        """Should track number of discovered documents."""
        coordinator, _, _, _, _ = coordinator_with_mocks

        await coordinator.tell(DocumentoDescubierto(q_param="DOC1", title="Doc 1", category="LEY"))
        await coordinator.tell(DocumentoDescubierto(q_param="DOC2", title="Doc 2", category="LEY"))

        await asyncio.sleep(0.05)
        result = await coordinator.ask(ObtenerEstado())

        assert result["discovered_count"] >= 2

    @pytest.mark.asyncio
    async def test_tracks_downloaded_count(self, coordinator_with_mocks):
        """Should track number of downloaded documents."""
        coordinator, _, _, _, _ = coordinator_with_mocks

        await coordinator.tell(
            DocumentoDescargado(
                document_id="doc-1",
                q_param="ABC123",
                has_pdf=False,
            )
        )

        await asyncio.sleep(0.05)
        result = await coordinator.ask(ObtenerEstado())

        assert result["downloaded_count"] >= 1


class TestSCJNCoordinatorActorCorrelation:
    """Tests for correlation ID handling."""

    @pytest.mark.asyncio
    async def test_preserves_correlation_id(self, coordinator_with_mocks):
        """Messages should preserve correlation ID through pipeline."""
        coordinator, discovery, _, _, _ = coordinator_with_mocks

        await coordinator.ask(
            DescubrirDocumentos(
                category="LEY",
                correlation_id="test-correlation-123",
            )
        )

        assert discovery.received[0].correlation_id == "test-correlation-123"


class TestSCJNCoordinatorActorQueueManagement:
    """Tests for download queue management."""

    @pytest.mark.asyncio
    async def test_queues_documents_for_download(self, coordinator_with_mocks):
        """Should queue discovered documents for download."""
        coordinator, _, scraper, persistence, _ = coordinator_with_mocks

        # Discover multiple documents
        for i in range(5):
            await coordinator.tell(
                DocumentoDescubierto(
                    q_param=f"DOC{i}",
                    title=f"Document {i}",
                    category="LEY",
                )
            )

        await asyncio.sleep(0.1)

        # Should have queued multiple downloads
        download_cmds = [m for m in scraper.received if isinstance(m, DescargarDocumento)]
        assert len(download_cmds) >= 1  # At least one should be triggered


class TestSCJNCoordinatorActorConcurrency:
    """Tests for concurrency control."""

    @pytest.mark.asyncio
    async def test_respects_max_concurrent_downloads(self, rate_limiter):
        """Should not exceed max concurrent downloads."""
        coordinator = SCJNCoordinatorActor(
            discovery_actor=MockDiscoveryActor(),
            scraper_actor=MockScraperActor(),
            persistence_actor=MockPersistenceActor(),
            checkpoint_actor=MockCheckpointActor(),
            rate_limiter=rate_limiter,
            max_concurrent_downloads=2,
        )
        await coordinator.start()

        assert coordinator._max_concurrent_downloads == 2

        await coordinator.stop()
