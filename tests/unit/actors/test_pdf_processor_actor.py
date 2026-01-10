"""
RED Phase Tests: PDFProcessorActor

Tests for PDF processing actor.
These tests must FAIL initially (RED phase).
"""
import pytest
import asyncio
from unittest.mock import MagicMock, patch

# These imports will fail until implementation exists
from src.infrastructure.actors.pdf_processor_actor import PDFProcessorActor
from src.infrastructure.actors.messages import (
    ProcesarPDF,
    PDFProcesado,
    ErrorDeActor,
)
from src.infrastructure.adapters.pdf_extractor import (
    PDFExtractionResult,
    ExtractionMethod,
)
from src.infrastructure.adapters.text_chunker import TextChunkResult


@pytest.fixture
def sample_pdf_bytes():
    """Create minimal valid PDF bytes."""
    return b"""%PDF-1.4
1 0 obj
<< /Type /Catalog /Pages 2 0 R >>
endobj
2 0 obj
<< /Type /Pages /Kids [3 0 R] /Count 1 >>
endobj
3 0 obj
<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 4 0 R >>
endobj
4 0 obj
<< /Length 100 >>
stream
BT /F1 12 Tf 100 700 Td (Articulo 1. La presente Ley es de observancia general en toda la Republica.) Tj ET
endstream
endobj
xref
0 5
0000000000 65535 f
0000000009 00000 n
0000000058 00000 n
0000000115 00000 n
0000000206 00000 n
trailer
<< /Size 5 /Root 1 0 R >>
startxref
350
%%EOF"""


@pytest.fixture
def mock_pdf_extractor():
    """Create mock PDF extractor."""
    mock = MagicMock()
    mock.extract_from_bytes.return_value = PDFExtractionResult(
        text="Artículo 1.- La presente Ley es de observancia general.\n\nArtículo 2.- Las normas del trabajo.",
        page_count=2,
        extraction_method=ExtractionMethod.TEXT_LAYER,
        confidence=0.95,
        warnings=(),
    )
    return mock


@pytest.fixture
def mock_text_chunker():
    """Create mock text chunker."""
    mock = MagicMock()
    mock.chunk.return_value = [
        TextChunkResult(
            content="Artículo 1.- La presente Ley es de observancia general.",
            token_count=50,
            start_char=0,
            end_char=55,
            chunk_index=0,
            boundary_type="article",
        ),
        TextChunkResult(
            content="Artículo 2.- Las normas del trabajo.",
            token_count=35,
            start_char=57,
            end_char=94,
            chunk_index=1,
            boundary_type="article",
        ),
    ]
    return mock


class TestPDFProcessorActorLifecycle:
    """Tests for PDFProcessorActor lifecycle."""

    @pytest.mark.asyncio
    async def test_start_initializes_adapters(self):
        """Start should initialize PDF extractor and chunker."""
        actor = PDFProcessorActor()
        await actor.start()

        assert actor._pdf_extractor is not None
        assert actor._text_chunker is not None

        await actor.stop()

    @pytest.mark.asyncio
    async def test_stop_is_clean(self):
        """Stop should cleanly terminate the actor."""
        actor = PDFProcessorActor()
        await actor.start()
        await actor.stop()

        assert not actor._running

    @pytest.mark.asyncio
    async def test_custom_chunk_config(self):
        """Should accept custom chunk configuration."""
        actor = PDFProcessorActor(max_tokens=256, overlap_tokens=25)
        await actor.start()

        assert actor._max_tokens == 256
        assert actor._overlap_tokens == 25

        await actor.stop()


class TestPDFProcessorActorProcessing:
    """Tests for PDF processing."""

    @pytest.mark.asyncio
    async def test_process_returns_event(
        self, sample_pdf_bytes, mock_pdf_extractor, mock_text_chunker
    ):
        """Processing PDF should return PDFProcesado event."""
        actor = PDFProcessorActor()
        actor._pdf_extractor = mock_pdf_extractor
        actor._text_chunker = mock_text_chunker
        await actor.start()

        result = await actor.ask(
            ProcesarPDF(
                document_id="doc-123",
                pdf_bytes=sample_pdf_bytes,
                source_url="https://example.com/doc.pdf",
            )
        )

        assert isinstance(result, PDFProcesado)
        assert result.document_id == "doc-123"
        assert result.chunk_count == 2

        await actor.stop()

    @pytest.mark.asyncio
    async def test_process_tracks_total_tokens(
        self, sample_pdf_bytes, mock_pdf_extractor, mock_text_chunker
    ):
        """Processing should track total tokens."""
        actor = PDFProcessorActor()
        actor._pdf_extractor = mock_pdf_extractor
        actor._text_chunker = mock_text_chunker
        await actor.start()

        result = await actor.ask(
            ProcesarPDF(document_id="doc-123", pdf_bytes=sample_pdf_bytes)
        )

        assert result.total_tokens == 85  # 50 + 35

        await actor.stop()

    @pytest.mark.asyncio
    async def test_process_includes_confidence(
        self, sample_pdf_bytes, mock_pdf_extractor, mock_text_chunker
    ):
        """Processing should include extraction confidence."""
        actor = PDFProcessorActor()
        actor._pdf_extractor = mock_pdf_extractor
        actor._text_chunker = mock_text_chunker
        await actor.start()

        result = await actor.ask(
            ProcesarPDF(document_id="doc-123", pdf_bytes=sample_pdf_bytes)
        )

        assert result.extraction_confidence == 0.95

        await actor.stop()


class TestPDFProcessorActorChunkRetrieval:
    """Tests for retrieving processed chunks."""

    @pytest.mark.asyncio
    async def test_get_chunks_after_processing(
        self, sample_pdf_bytes, mock_pdf_extractor, mock_text_chunker
    ):
        """Should be able to retrieve chunks after processing."""
        actor = PDFProcessorActor()
        actor._pdf_extractor = mock_pdf_extractor
        actor._text_chunker = mock_text_chunker
        await actor.start()

        await actor.ask(
            ProcesarPDF(document_id="doc-123", pdf_bytes=sample_pdf_bytes)
        )

        chunks = await actor.ask(("GET_CHUNKS", "doc-123"))

        assert len(chunks) == 2
        assert chunks[0].document_id == "doc-123"

        await actor.stop()

    @pytest.mark.asyncio
    async def test_get_chunks_nonexistent_returns_empty(self):
        """Getting chunks for unknown document returns empty."""
        actor = PDFProcessorActor()
        await actor.start()

        chunks = await actor.ask(("GET_CHUNKS", "nonexistent"))
        assert chunks == []

        await actor.stop()


class TestPDFProcessorActorEmptyInput:
    """Tests for empty/edge case inputs."""

    @pytest.mark.asyncio
    async def test_empty_bytes_returns_error(self):
        """Empty PDF bytes should return error."""
        actor = PDFProcessorActor()
        await actor.start()

        result = await actor.ask(
            ProcesarPDF(document_id="doc-123", pdf_bytes=b"")
        )

        assert isinstance(result, ErrorDeActor)
        assert result.document_id == "doc-123"
        assert not result.recoverable  # Empty PDF is not recoverable

        await actor.stop()


class TestPDFProcessorActorCorrelation:
    """Tests for message correlation."""

    @pytest.mark.asyncio
    async def test_preserves_correlation_id(
        self, sample_pdf_bytes, mock_pdf_extractor, mock_text_chunker
    ):
        """Result should preserve correlation_id."""
        actor = PDFProcessorActor()
        actor._pdf_extractor = mock_pdf_extractor
        actor._text_chunker = mock_text_chunker
        await actor.start()

        result = await actor.ask(
            ProcesarPDF(
                document_id="doc-123",
                pdf_bytes=sample_pdf_bytes,
                correlation_id="test-corr-456",
            )
        )

        assert result.correlation_id == "test-corr-456"

        await actor.stop()


class TestPDFProcessorActorErrorHandling:
    """Tests for error handling."""

    @pytest.mark.asyncio
    async def test_handles_corrupted_pdf(self):
        """Should handle corrupted PDF gracefully."""
        actor = PDFProcessorActor()
        await actor.start()

        result = await actor.ask(
            ProcesarPDF(
                document_id="doc-corrupted",
                pdf_bytes=b"not a valid pdf at all",
            )
        )

        assert isinstance(result, ErrorDeActor)
        assert result.document_id == "doc-corrupted"
        assert "PDFCorruptedError" in result.error_type or "Error" in result.error_type

        await actor.stop()

    @pytest.mark.asyncio
    async def test_handles_extraction_error(self, mock_text_chunker):
        """Should handle extraction errors gracefully."""
        mock_extractor = MagicMock()
        mock_extractor.extract_from_bytes.side_effect = Exception("Extraction failed")

        actor = PDFProcessorActor()
        actor._pdf_extractor = mock_extractor
        actor._text_chunker = mock_text_chunker
        await actor.start()

        result = await actor.ask(
            ProcesarPDF(document_id="doc-error", pdf_bytes=b"%PDF-1.4 test")
        )

        assert isinstance(result, ErrorDeActor)
        assert result.recoverable is True

        await actor.stop()
