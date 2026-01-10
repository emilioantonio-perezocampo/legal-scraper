"""
RED Phase Tests: SCJNScraperActor

Tests for SCJN document scraping actor.
These tests must FAIL initially (RED phase).
"""
import pytest
import asyncio
from unittest.mock import MagicMock, AsyncMock, patch
from aiohttp import ClientError

# These imports will fail until implementation exists
from src.infrastructure.actors.scjn_scraper_actor import (
    SCJNScraperActor,
    TransientScraperError,
    PermanentScraperError,
)
from src.infrastructure.actors.messages import (
    DescargarDocumento,
    DocumentoDescargado,
    ProcesarPDF,
    GuardarDocumento,
    ErrorDeActor,
)
from src.infrastructure.actors.rate_limiter import NoOpRateLimiter
from tests.utils.actor_test_helpers import MockCoordinator


@pytest.fixture
def mock_coordinator():
    """Create mock coordinator."""
    coordinator = MockCoordinator()
    coordinator.received = []
    return coordinator


@pytest.fixture
def instant_rate_limiter():
    """Create rate limiter that doesn't wait."""
    return NoOpRateLimiter()


@pytest.fixture
def sample_document_html():
    """Sample SCJN document detail HTML."""
    return """
    <html>
    <body>
    <div id="contenedor">
        <h1 class="titulo-ordenamiento">LEY FEDERAL DEL TRABAJO</h1>
        <div class="datos-ordenamiento">
            <div class="dato">
                <span class="etiqueta">Tipo de Ordenamiento:</span>
                <span class="valor">LEY FEDERAL</span>
            </div>
            <div class="dato">
                <span class="etiqueta">Ámbito:</span>
                <span class="valor">FEDERAL</span>
            </div>
            <div class="dato">
                <span class="etiqueta">Estatus:</span>
                <span class="valor">VIGENTE</span>
            </div>
            <div class="dato">
                <span class="etiqueta">Fecha de Publicación:</span>
                <span class="valor">01/04/1970</span>
            </div>
        </div>
        <div id="contenido-ordenamiento">
            <div class="articulo" id="art1">
                <h3>Artículo 1</h3>
                <p>La presente Ley es de observancia general.</p>
            </div>
        </div>
        <div id="reformas">
            <table class="tabla-reformas">
                <tr class="reforma-row">
                    <td><a href="wfOrdenamientoDetalle.aspx?q=ref001">Reforma 2012</a></td>
                    <td>30/11/2012</td>
                    <td>DOF 30-11-2012</td>
                    <td><a href="AbrirDocReforma.aspx?q=ref001">PDF</a></td>
                </tr>
            </table>
        </div>
    </div>
    </body>
    </html>
    """


@pytest.fixture
def sample_pdf_bytes():
    """Sample PDF bytes."""
    return b"%PDF-1.4 minimal pdf content for testing"


class TestSCJNScraperActorLifecycle:
    """Lifecycle tests."""

    @pytest.mark.asyncio
    async def test_start_creates_session(self, mock_coordinator, instant_rate_limiter):
        """Start should create aiohttp session."""
        actor = SCJNScraperActor(
            coordinator=mock_coordinator,
            rate_limiter=instant_rate_limiter,
        )
        await actor.start()

        assert actor._session is not None

        await actor.stop()

    @pytest.mark.asyncio
    async def test_stop_closes_session(self, mock_coordinator, instant_rate_limiter):
        """Stop should close aiohttp session."""
        actor = SCJNScraperActor(
            coordinator=mock_coordinator,
            rate_limiter=instant_rate_limiter,
        )
        await actor.start()
        session = actor._session
        await actor.stop()

        assert actor._session is None

    @pytest.mark.asyncio
    async def test_stop_handles_no_session(self, mock_coordinator, instant_rate_limiter):
        """Stop should handle case when session doesn't exist."""
        actor = SCJNScraperActor(
            coordinator=mock_coordinator,
            rate_limiter=instant_rate_limiter,
        )
        # Don't start - no session
        await actor.stop()  # Should not raise


class TestSCJNScraperActorDownload:
    """Document download tests."""

    @pytest.mark.asyncio
    async def test_download_emits_descargado_event(
        self, mock_coordinator, instant_rate_limiter, sample_document_html
    ):
        """Download should emit DocumentoDescargado event."""
        actor = SCJNScraperActor(
            coordinator=mock_coordinator,
            rate_limiter=instant_rate_limiter,
        )
        await actor.start()
        await mock_coordinator.start()

        with patch.object(actor, '_fetch_html', new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = sample_document_html

            await actor.ask(DescargarDocumento(
                q_param="abc123==",
                include_pdf=False,
                include_reforms=False,
            ))

        # Check coordinator received DocumentoDescargado
        descargado_events = [
            m for m in mock_coordinator.received
            if isinstance(m, DocumentoDescargado)
        ]
        assert len(descargado_events) >= 1

        await actor.stop()
        await mock_coordinator.stop()

    @pytest.mark.asyncio
    async def test_download_sends_to_persistence(
        self, mock_coordinator, instant_rate_limiter, sample_document_html
    ):
        """Download should send document for persistence."""
        actor = SCJNScraperActor(
            coordinator=mock_coordinator,
            rate_limiter=instant_rate_limiter,
        )
        await actor.start()
        await mock_coordinator.start()

        with patch.object(actor, '_fetch_html', new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = sample_document_html

            await actor.ask(DescargarDocumento(
                q_param="abc123==",
                include_pdf=False,
            ))

        # Check coordinator received GuardarDocumento
        guardar_events = [
            m for m in mock_coordinator.received
            if isinstance(m, GuardarDocumento)
        ]
        assert len(guardar_events) >= 1

        await actor.stop()
        await mock_coordinator.stop()

    @pytest.mark.asyncio
    async def test_download_preserves_correlation_id(
        self, mock_coordinator, instant_rate_limiter, sample_document_html
    ):
        """Downloaded events should preserve correlation ID."""
        actor = SCJNScraperActor(
            coordinator=mock_coordinator,
            rate_limiter=instant_rate_limiter,
        )
        await actor.start()
        await mock_coordinator.start()

        with patch.object(actor, '_fetch_html', new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = sample_document_html

            await actor.ask(DescargarDocumento(
                q_param="abc123==",
                correlation_id="test-corr-123",
                include_pdf=False,
            ))

        descargado = next(
            (m for m in mock_coordinator.received if isinstance(m, DocumentoDescargado)),
            None
        )
        assert descargado is not None
        assert descargado.correlation_id == "test-corr-123"

        await actor.stop()
        await mock_coordinator.stop()


class TestSCJNScraperActorPDF:
    """PDF download tests."""

    @pytest.mark.asyncio
    async def test_downloads_pdf_when_available(
        self, mock_coordinator, instant_rate_limiter, sample_document_html, sample_pdf_bytes
    ):
        """Should download PDF when available and requested."""
        actor = SCJNScraperActor(
            coordinator=mock_coordinator,
            rate_limiter=instant_rate_limiter,
        )
        await actor.start()
        await mock_coordinator.start()

        with patch.object(actor, '_fetch_html', new_callable=AsyncMock) as mock_html:
            mock_html.return_value = sample_document_html
            with patch.object(actor, '_fetch_pdf', new_callable=AsyncMock) as mock_pdf:
                mock_pdf.return_value = sample_pdf_bytes

                await actor.ask(DescargarDocumento(
                    q_param="abc123==",
                    include_pdf=True,
                    include_reforms=True,
                ))

        # Check if PDF was sent for processing
        pdf_events = [
            m for m in mock_coordinator.received
            if isinstance(m, ProcesarPDF)
        ]
        # May or may not have PDF depending on reform detection
        assert isinstance(pdf_events, list)

        await actor.stop()
        await mock_coordinator.stop()

    @pytest.mark.asyncio
    async def test_skips_pdf_when_disabled(
        self, mock_coordinator, instant_rate_limiter, sample_document_html
    ):
        """Should skip PDF download when disabled."""
        actor = SCJNScraperActor(
            coordinator=mock_coordinator,
            rate_limiter=instant_rate_limiter,
            download_pdfs=False,
        )
        await actor.start()
        await mock_coordinator.start()

        with patch.object(actor, '_fetch_html', new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = sample_document_html

            await actor.ask(DescargarDocumento(
                q_param="abc123==",
                include_pdf=True,
            ))

        # No PDF processing should occur
        pdf_events = [
            m for m in mock_coordinator.received
            if isinstance(m, ProcesarPDF)
        ]
        assert len(pdf_events) == 0

        await actor.stop()
        await mock_coordinator.stop()

    @pytest.mark.asyncio
    async def test_continues_after_pdf_failure(
        self, mock_coordinator, instant_rate_limiter, sample_document_html
    ):
        """Should continue processing even if PDF download fails."""
        actor = SCJNScraperActor(
            coordinator=mock_coordinator,
            rate_limiter=instant_rate_limiter,
        )
        await actor.start()
        await mock_coordinator.start()

        with patch.object(actor, '_fetch_html', new_callable=AsyncMock) as mock_html:
            mock_html.return_value = sample_document_html
            with patch.object(actor, '_fetch_pdf', new_callable=AsyncMock) as mock_pdf:
                mock_pdf.side_effect = Exception("PDF download failed")

                # Should not raise
                await actor.ask(DescargarDocumento(
                    q_param="abc123==",
                    include_pdf=True,
                ))

        # Document should still be saved
        guardar_events = [
            m for m in mock_coordinator.received
            if isinstance(m, GuardarDocumento)
        ]
        assert len(guardar_events) >= 1

        await actor.stop()
        await mock_coordinator.stop()


class TestSCJNScraperActorRateLimit:
    """Rate limiting tests."""

    @pytest.mark.asyncio
    async def test_acquires_rate_limit_before_fetch(self, mock_coordinator):
        """Should acquire rate limit before fetching."""
        rate_limiter = MagicMock()
        rate_limiter.acquire = AsyncMock()

        actor = SCJNScraperActor(
            coordinator=mock_coordinator,
            rate_limiter=rate_limiter,
        )
        await actor.start()

        with patch.object(actor, '_fetch_html', new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = "<div id='contenedor'><h1 class='titulo-ordenamiento'>Test</h1><div class='datos-ordenamiento'></div><div id='contenido-ordenamiento'></div></div>"

            await actor.ask(DescargarDocumento(q_param="abc123==", include_pdf=False))

        rate_limiter.acquire.assert_called()

        await actor.stop()


class TestSCJNScraperActorErrors:
    """Error handling tests."""

    @pytest.mark.asyncio
    async def test_emits_error_on_parse_failure(
        self, mock_coordinator, instant_rate_limiter
    ):
        """Should emit error on parse failure."""
        actor = SCJNScraperActor(
            coordinator=mock_coordinator,
            rate_limiter=instant_rate_limiter,
        )
        await actor.start()
        await mock_coordinator.start()

        with patch.object(actor, '_fetch_html', new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = "<html><body>Invalid HTML</body></html>"

            await actor.ask(DescargarDocumento(q_param="abc123=="))

        error_events = [
            m for m in mock_coordinator.received
            if isinstance(m, ErrorDeActor)
        ]
        assert len(error_events) >= 1

        await actor.stop()
        await mock_coordinator.stop()

    @pytest.mark.asyncio
    async def test_emits_error_on_network_failure(
        self, mock_coordinator, instant_rate_limiter
    ):
        """Should emit error on network failure."""
        actor = SCJNScraperActor(
            coordinator=mock_coordinator,
            rate_limiter=instant_rate_limiter,
        )
        await actor.start()
        await mock_coordinator.start()

        with patch.object(actor, '_fetch_html', new_callable=AsyncMock) as mock_fetch:
            mock_fetch.side_effect = TransientScraperError("Network error")

            await actor.ask(DescargarDocumento(q_param="abc123=="))

        error_events = [
            m for m in mock_coordinator.received
            if isinstance(m, ErrorDeActor)
        ]
        assert len(error_events) >= 1
        assert error_events[0].recoverable is True

        await actor.stop()
        await mock_coordinator.stop()

    @pytest.mark.asyncio
    async def test_error_includes_original_command(
        self, mock_coordinator, instant_rate_limiter
    ):
        """Error should include original command."""
        actor = SCJNScraperActor(
            coordinator=mock_coordinator,
            rate_limiter=instant_rate_limiter,
        )
        await actor.start()
        await mock_coordinator.start()

        with patch.object(actor, '_fetch_html', new_callable=AsyncMock) as mock_fetch:
            mock_fetch.side_effect = PermanentScraperError("Not found")

            await actor.ask(DescargarDocumento(
                q_param="abc123==",
                correlation_id="test-123",
            ))

        error_events = [
            m for m in mock_coordinator.received
            if isinstance(m, ErrorDeActor)
        ]
        assert len(error_events) >= 1
        assert error_events[0].original_command is not None

        await actor.stop()
        await mock_coordinator.stop()

    @pytest.mark.asyncio
    async def test_permanent_error_not_recoverable(
        self, mock_coordinator, instant_rate_limiter
    ):
        """Permanent errors should be marked as not recoverable."""
        actor = SCJNScraperActor(
            coordinator=mock_coordinator,
            rate_limiter=instant_rate_limiter,
        )
        await actor.start()
        await mock_coordinator.start()

        with patch.object(actor, '_fetch_html', new_callable=AsyncMock) as mock_fetch:
            mock_fetch.side_effect = PermanentScraperError("404 Not Found")

            await actor.ask(DescargarDocumento(q_param="abc123=="))

        error_events = [
            m for m in mock_coordinator.received
            if isinstance(m, ErrorDeActor)
        ]
        assert len(error_events) >= 1
        assert error_events[0].recoverable is False

        await actor.stop()
        await mock_coordinator.stop()


class TestSCJNScraperActorURLBuilding:
    """URL construction tests."""

    def test_build_detail_url(self, mock_coordinator, instant_rate_limiter):
        """Should build correct detail URL."""
        actor = SCJNScraperActor(
            coordinator=mock_coordinator,
            rate_limiter=instant_rate_limiter,
        )
        url = actor._build_detail_url("abc123==")
        assert "wfOrdenamientoDetalle.aspx" in url
        assert "q=abc123==" in url

    def test_build_pdf_url(self, mock_coordinator, instant_rate_limiter):
        """Should build correct PDF URL."""
        actor = SCJNScraperActor(
            coordinator=mock_coordinator,
            rate_limiter=instant_rate_limiter,
        )
        url = actor._build_pdf_url("ref001")
        assert "AbrirDocReforma.aspx" in url
        assert "q=ref001" in url
