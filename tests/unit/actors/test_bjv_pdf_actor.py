"""
Tests for BJV PDF Processor Actor.

Following RED-GREEN TDD: These tests define the expected behavior
for PDF download and text extraction.

Target: ~15 tests
"""
import pytest
import tempfile
import os
from unittest.mock import Mock, MagicMock, AsyncMock, patch
import aiohttp


class TestBJVPDFActorInit:
    """Tests for BJVPDFActor initialization."""

    def test_inherits_from_base_actor(self):
        """BJVPDFActor inherits from BJVBaseActor."""
        from src.infrastructure.actors.bjv_pdf_actor import BJVPDFActor
        from src.infrastructure.actors.bjv_base_actor import BJVBaseActor

        mock_coordinator = Mock()
        mock_rate_limiter = Mock()

        actor = BJVPDFActor(
            coordinator=mock_coordinator,
            rate_limiter=mock_rate_limiter,
        )

        assert isinstance(actor, BJVBaseActor)

    def test_creates_with_coordinator(self):
        """BJVPDFActor stores coordinator reference."""
        from src.infrastructure.actors.bjv_pdf_actor import BJVPDFActor

        mock_coordinator = Mock()
        mock_rate_limiter = Mock()

        actor = BJVPDFActor(
            coordinator=mock_coordinator,
            rate_limiter=mock_rate_limiter,
        )

        assert actor._coordinator is mock_coordinator

    def test_has_nombre(self):
        """BJVPDFActor has descriptive nombre."""
        from src.infrastructure.actors.bjv_pdf_actor import BJVPDFActor

        mock_coordinator = Mock()
        mock_rate_limiter = Mock()

        actor = BJVPDFActor(
            coordinator=mock_coordinator,
            rate_limiter=mock_rate_limiter,
        )

        assert "PDF" in actor.nombre


class TestBJVPDFActorLifecycle:
    """Tests for actor lifecycle."""

    @pytest.mark.asyncio
    async def test_start_creates_session(self):
        """Start creates HTTP session."""
        from src.infrastructure.actors.bjv_pdf_actor import BJVPDFActor

        mock_coordinator = Mock()
        mock_rate_limiter = Mock()

        actor = BJVPDFActor(
            coordinator=mock_coordinator,
            rate_limiter=mock_rate_limiter,
        )

        await actor.start()

        try:
            assert actor._session is not None
            assert isinstance(actor._session, aiohttp.ClientSession)
        finally:
            await actor.stop()

    @pytest.mark.asyncio
    async def test_stop_closes_session(self):
        """Stop closes HTTP session."""
        from src.infrastructure.actors.bjv_pdf_actor import BJVPDFActor

        mock_coordinator = Mock()
        mock_rate_limiter = Mock()

        actor = BJVPDFActor(
            coordinator=mock_coordinator,
            rate_limiter=mock_rate_limiter,
        )

        await actor.start()
        session = actor._session
        await actor.stop()

        assert session.closed


class TestBJVPDFActorDownload:
    """Tests for PDF download handling."""

    @pytest.mark.asyncio
    async def test_handle_descargar_pdf(self):
        """Actor handles DescargarPDF message."""
        from src.infrastructure.actors.bjv_pdf_actor import BJVPDFActor
        from src.infrastructure.actors.bjv_messages import DescargarPDF

        mock_coordinator = Mock()
        mock_coordinator.tell = AsyncMock()
        mock_rate_limiter = Mock()
        mock_rate_limiter.acquire = AsyncMock()

        actor = BJVPDFActor(
            coordinator=mock_coordinator,
            rate_limiter=mock_rate_limiter,
        )

        # Mock session
        mock_response = MagicMock()
        mock_response.read = AsyncMock(return_value=b"fake pdf content")
        mock_response.raise_for_status = Mock()
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_response)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session = MagicMock()
        mock_session.get = Mock(return_value=mock_cm)
        actor._session = mock_session

        msg = DescargarPDF(
            correlation_id="corr-123",
            libro_id="libro-456",
            capitulo_id=None,
            url_pdf="https://example.com/doc.pdf",
        )

        await actor.handle_message(msg)

        mock_coordinator.tell.assert_called_once()

    @pytest.mark.asyncio
    async def test_pdf_saved_to_disk(self):
        """PDF is saved and path forwarded."""
        from src.infrastructure.actors.bjv_pdf_actor import BJVPDFActor
        from src.infrastructure.actors.bjv_messages import DescargarPDF, PDFListo

        mock_coordinator = Mock()
        mock_coordinator.tell = AsyncMock()
        mock_rate_limiter = Mock()
        mock_rate_limiter.acquire = AsyncMock()

        with tempfile.TemporaryDirectory() as tmpdir:
            actor = BJVPDFActor(
                coordinator=mock_coordinator,
                rate_limiter=mock_rate_limiter,
                download_dir=tmpdir,
            )

            # Mock session
            pdf_content = b"fake pdf content"
            mock_response = MagicMock()
            mock_response.read = AsyncMock(return_value=pdf_content)
            mock_response.raise_for_status = Mock()
            mock_cm = MagicMock()
            mock_cm.__aenter__ = AsyncMock(return_value=mock_response)
            mock_cm.__aexit__ = AsyncMock(return_value=None)
            mock_session = MagicMock()
            mock_session.get = Mock(return_value=mock_cm)
            actor._session = mock_session

            msg = DescargarPDF(
                correlation_id="corr-123",
                libro_id="libro-456",
                capitulo_id="cap-1",
                url_pdf="https://example.com/doc.pdf",
            )

            await actor.handle_message(msg)

            # Verify PDFListo was sent with path
            call_args = mock_coordinator.tell.call_args[0][0]
            assert isinstance(call_args, PDFListo)
            assert call_args.libro_id == "libro-456"
            assert call_args.capitulo_id == "cap-1"
            assert call_args.ruta_pdf  # Has a path

    @pytest.mark.asyncio
    async def test_emits_pdf_listo(self):
        """Actor emits PDFListo on success."""
        from src.infrastructure.actors.bjv_pdf_actor import BJVPDFActor
        from src.infrastructure.actors.bjv_messages import DescargarPDF, PDFListo

        mock_coordinator = Mock()
        mock_coordinator.tell = AsyncMock()
        mock_rate_limiter = Mock()
        mock_rate_limiter.acquire = AsyncMock()

        actor = BJVPDFActor(
            coordinator=mock_coordinator,
            rate_limiter=mock_rate_limiter,
        )

        # Mock session
        mock_response = MagicMock()
        mock_response.read = AsyncMock(return_value=b"pdf bytes")
        mock_response.raise_for_status = Mock()
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_response)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session = MagicMock()
        mock_session.get = Mock(return_value=mock_cm)
        actor._session = mock_session

        msg = DescargarPDF(
            correlation_id="corr-123",
            libro_id="libro-456",
            capitulo_id=None,
            url_pdf="https://example.com/doc.pdf",
        )

        await actor.handle_message(msg)

        result = mock_coordinator.tell.call_args[0][0]
        assert isinstance(result, PDFListo)


class TestBJVPDFActorRetry:
    """Tests for retry behavior."""

    @pytest.mark.asyncio
    async def test_retry_on_network_error(self):
        """Actor retries on network errors."""
        from src.infrastructure.actors.bjv_pdf_actor import BJVPDFActor
        from src.infrastructure.actors.bjv_messages import DescargarPDF

        mock_coordinator = Mock()
        mock_coordinator.tell = AsyncMock()
        mock_rate_limiter = Mock()
        mock_rate_limiter.acquire = AsyncMock()

        actor = BJVPDFActor(
            coordinator=mock_coordinator,
            rate_limiter=mock_rate_limiter,
            max_retries=3,
        )

        # Track call count
        call_count = 0

        def get_side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise aiohttp.ClientError("Network error")
            mock_response = MagicMock()
            mock_response.read = AsyncMock(return_value=b"pdf")
            mock_response.raise_for_status = Mock()
            mock_cm = MagicMock()
            mock_cm.__aenter__ = AsyncMock(return_value=mock_response)
            mock_cm.__aexit__ = AsyncMock(return_value=None)
            return mock_cm

        mock_session = MagicMock()
        mock_session.get = Mock(side_effect=get_side_effect)
        actor._session = mock_session

        msg = DescargarPDF(
            correlation_id="corr-123",
            libro_id="libro-456",
            capitulo_id=None,
            url_pdf="https://example.com/doc.pdf",
        )

        await actor.handle_message(msg)

        # Should have retried and succeeded
        assert call_count == 3

    @pytest.mark.asyncio
    async def test_rate_limiter_used(self):
        """Rate limiter is acquired before request."""
        from src.infrastructure.actors.bjv_pdf_actor import BJVPDFActor
        from src.infrastructure.actors.bjv_messages import DescargarPDF

        mock_coordinator = Mock()
        mock_coordinator.tell = AsyncMock()
        mock_rate_limiter = Mock()
        mock_rate_limiter.acquire = AsyncMock()

        actor = BJVPDFActor(
            coordinator=mock_coordinator,
            rate_limiter=mock_rate_limiter,
        )

        # Mock session
        mock_response = MagicMock()
        mock_response.read = AsyncMock(return_value=b"pdf")
        mock_response.raise_for_status = Mock()
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_response)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session = MagicMock()
        mock_session.get = Mock(return_value=mock_cm)
        actor._session = mock_session

        msg = DescargarPDF(
            correlation_id="corr-123",
            libro_id="libro-456",
            capitulo_id=None,
            url_pdf="https://example.com/doc.pdf",
        )

        await actor.handle_message(msg)

        mock_rate_limiter.acquire.assert_called()


class TestBJVPDFActorErrorHandling:
    """Tests for error handling."""

    @pytest.mark.asyncio
    async def test_retry_exhausted_emits_error(self):
        """Exhausted retries emit ErrorProcesamiento."""
        from src.infrastructure.actors.bjv_pdf_actor import BJVPDFActor
        from src.infrastructure.actors.bjv_messages import DescargarPDF, ErrorProcesamiento

        mock_coordinator = Mock()
        mock_coordinator.tell = AsyncMock()
        mock_rate_limiter = Mock()
        mock_rate_limiter.acquire = AsyncMock()

        actor = BJVPDFActor(
            coordinator=mock_coordinator,
            rate_limiter=mock_rate_limiter,
            max_retries=2,
        )

        # Always fail
        mock_session = MagicMock()
        mock_session.get = Mock(side_effect=aiohttp.ClientError("Network error"))
        actor._session = mock_session

        msg = DescargarPDF(
            correlation_id="corr-123",
            libro_id="libro-456",
            capitulo_id=None,
            url_pdf="https://example.com/doc.pdf",
        )

        await actor.handle_message(msg)

        # Should emit error
        call_args = mock_coordinator.tell.call_args[0][0]
        assert isinstance(call_args, ErrorProcesamiento)
        assert call_args.libro_id == "libro-456"

    @pytest.mark.asyncio
    async def test_forwards_to_coordinator(self):
        """All results forwarded to coordinator."""
        from src.infrastructure.actors.bjv_pdf_actor import BJVPDFActor
        from src.infrastructure.actors.bjv_messages import DescargarPDF

        mock_coordinator = Mock()
        mock_coordinator.tell = AsyncMock()
        mock_rate_limiter = Mock()
        mock_rate_limiter.acquire = AsyncMock()

        actor = BJVPDFActor(
            coordinator=mock_coordinator,
            rate_limiter=mock_rate_limiter,
        )

        # Mock session
        mock_response = MagicMock()
        mock_response.read = AsyncMock(return_value=b"pdf")
        mock_response.raise_for_status = Mock()
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_response)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session = MagicMock()
        mock_session.get = Mock(return_value=mock_cm)
        actor._session = mock_session

        msg = DescargarPDF(
            correlation_id="corr-123",
            libro_id="libro-456",
            capitulo_id=None,
            url_pdf="https://example.com/doc.pdf",
        )

        await actor.handle_message(msg)

        mock_coordinator.tell.assert_called_once()
