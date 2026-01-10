"""
Tests for BJV PDF text extractor.

Following RED-GREEN TDD: These tests define the expected behavior
for extracting text from PDF files.

Target: ~10 tests
"""
import pytest
from unittest.mock import Mock, patch, MagicMock
from dataclasses import dataclass


class TestPDFExtractionResult:
    """Tests for PDFExtractionResult dataclass."""

    def test_extraction_result_creation(self):
        """PDFExtractionResult can be created."""
        from src.infrastructure.adapters.bjv_pdf_extractor import PDFExtractionResult

        result = PDFExtractionResult(
            texto="Contenido del PDF",
            total_paginas=10,
            paginas_texto={"1": "Página 1", "2": "Página 2"},
            confianza=0.95,
        )

        assert result.texto == "Contenido del PDF"
        assert result.total_paginas == 10
        assert result.confianza == 0.95

    def test_extraction_result_has_texto_por_pagina(self):
        """PDFExtractionResult tracks text by page."""
        from src.infrastructure.adapters.bjv_pdf_extractor import PDFExtractionResult

        result = PDFExtractionResult(
            texto="Full text",
            total_paginas=3,
            paginas_texto={"1": "Page 1", "2": "Page 2", "3": "Page 3"},
            confianza=0.9,
        )

        assert len(result.paginas_texto) == 3
        assert "1" in result.paginas_texto


class TestBJVPDFExtractor:
    """Tests for BJVPDFExtractor class."""

    def test_extractor_creation(self):
        """BJVPDFExtractor can be created."""
        from src.infrastructure.adapters.bjv_pdf_extractor import BJVPDFExtractor

        extractor = BJVPDFExtractor()

        assert extractor is not None

    @patch("src.infrastructure.adapters.bjv_pdf_extractor.fitz")
    def test_extract_text_from_valid_pdf(self, mock_fitz):
        """Extract text from valid PDF."""
        from src.infrastructure.adapters.bjv_pdf_extractor import BJVPDFExtractor

        # Mock PDF document
        mock_doc = MagicMock()
        mock_doc.is_encrypted = False
        mock_doc.__len__ = Mock(return_value=2)
        mock_page1 = MagicMock()
        mock_page1.get_text.return_value = "Página uno con contenido."
        mock_page2 = MagicMock()
        mock_page2.get_text.return_value = "Página dos con más contenido."
        mock_doc.__iter__ = Mock(return_value=iter([mock_page1, mock_page2]))
        mock_fitz.open.return_value = mock_doc

        extractor = BJVPDFExtractor()
        result = extractor.extract_text(b"fake pdf bytes")

        assert result.total_paginas == 2
        assert "Página uno" in result.texto
        assert result.confianza > 0

    @patch("src.infrastructure.adapters.bjv_pdf_extractor.fitz")
    def test_corrupted_pdf_raises_error(self, mock_fitz):
        """Corrupted PDF raises PDFCorruptedError."""
        from src.infrastructure.adapters.bjv_pdf_extractor import BJVPDFExtractor
        from src.infrastructure.adapters.bjv_errors import PDFCorruptedError

        mock_fitz.open.side_effect = Exception("Cannot open PDF")

        extractor = BJVPDFExtractor()

        with pytest.raises(PDFCorruptedError) as exc_info:
            extractor.extract_text(b"corrupted data")

        assert not exc_info.value.recoverable

    @patch("src.infrastructure.adapters.bjv_pdf_extractor.fitz")
    def test_password_protected_raises_error(self, mock_fitz):
        """Password-protected PDF raises PDFPasswordProtectedError."""
        from src.infrastructure.adapters.bjv_pdf_extractor import BJVPDFExtractor
        from src.infrastructure.adapters.bjv_errors import PDFPasswordProtectedError

        mock_doc = MagicMock()
        mock_doc.is_encrypted = True
        mock_doc.needs_pass = True
        mock_fitz.open.return_value = mock_doc

        extractor = BJVPDFExtractor()

        with pytest.raises(PDFPasswordProtectedError) as exc_info:
            extractor.extract_text(b"encrypted pdf")

        assert not exc_info.value.recoverable

    @patch("src.infrastructure.adapters.bjv_pdf_extractor.fitz")
    def test_empty_pdf_raises_error(self, mock_fitz):
        """PDF with no extractable text raises PDFEmptyError."""
        from src.infrastructure.adapters.bjv_pdf_extractor import BJVPDFExtractor
        from src.infrastructure.adapters.bjv_errors import PDFEmptyError

        mock_doc = MagicMock()
        mock_doc.is_encrypted = False
        mock_doc.__len__ = Mock(return_value=2)
        mock_page1 = MagicMock()
        mock_page1.get_text.return_value = ""
        mock_page2 = MagicMock()
        mock_page2.get_text.return_value = "   "
        mock_doc.__iter__ = Mock(return_value=iter([mock_page1, mock_page2]))
        mock_fitz.open.return_value = mock_doc

        extractor = BJVPDFExtractor()

        with pytest.raises(PDFEmptyError) as exc_info:
            extractor.extract_text(b"empty pdf")

        assert not exc_info.value.recoverable

    @patch("src.infrastructure.adapters.bjv_pdf_extractor.fitz")
    def test_page_numbers_tracked(self, mock_fitz):
        """Page numbers are tracked in extraction result."""
        from src.infrastructure.adapters.bjv_pdf_extractor import BJVPDFExtractor

        mock_doc = MagicMock()
        mock_doc.is_encrypted = False
        mock_doc.__len__ = Mock(return_value=3)
        pages = []
        for i in range(3):
            page = MagicMock()
            page.get_text.return_value = f"Content of page {i+1}"
            pages.append(page)
        mock_doc.__iter__ = Mock(return_value=iter(pages))
        mock_fitz.open.return_value = mock_doc

        extractor = BJVPDFExtractor()
        result = extractor.extract_text(b"pdf bytes")

        assert "1" in result.paginas_texto
        assert "2" in result.paginas_texto
        assert "3" in result.paginas_texto

    @patch("src.infrastructure.adapters.bjv_pdf_extractor.fitz")
    def test_confidence_score_calculated(self, mock_fitz):
        """Confidence score is calculated based on extraction quality."""
        from src.infrastructure.adapters.bjv_pdf_extractor import BJVPDFExtractor

        mock_doc = MagicMock()
        mock_doc.is_encrypted = False
        mock_doc.__len__ = Mock(return_value=5)
        pages = []
        for i in range(5):
            page = MagicMock()
            # Good quality text
            page.get_text.return_value = "Este es contenido de texto normal y legible. " * 10
            pages.append(page)
        mock_doc.__iter__ = Mock(return_value=iter(pages))
        mock_fitz.open.return_value = mock_doc

        extractor = BJVPDFExtractor()
        result = extractor.extract_text(b"pdf bytes")

        assert 0 <= result.confianza <= 1.0

    @patch("src.infrastructure.adapters.bjv_pdf_extractor.fitz")
    def test_confidence_lower_with_low_quality_text(self, mock_fitz):
        """Confidence is lower with gibberish or low-quality text."""
        from src.infrastructure.adapters.bjv_pdf_extractor import BJVPDFExtractor

        mock_doc = MagicMock()
        mock_doc.is_encrypted = False
        mock_doc.__len__ = Mock(return_value=2)
        pages = []
        for i in range(2):
            page = MagicMock()
            # Low quality - lots of special chars and short words
            page.get_text.return_value = "x y z ! @ # $ % ^ & * 1 2 3"
            pages.append(page)
        mock_doc.__iter__ = Mock(return_value=iter(pages))
        mock_fitz.open.return_value = mock_doc

        extractor = BJVPDFExtractor()
        result = extractor.extract_text(b"pdf bytes")

        # Confidence should be lower for low-quality text
        assert result.confianza < 0.8


class TestExtractFromPath:
    """Tests for extracting from file path."""

    @patch("src.infrastructure.adapters.bjv_pdf_extractor.fitz")
    @patch("builtins.open", create=True)
    def test_extract_from_path(self, mock_open, mock_fitz):
        """Extract text from file path."""
        from src.infrastructure.adapters.bjv_pdf_extractor import BJVPDFExtractor

        mock_open.return_value.__enter__ = Mock(return_value=Mock(read=Mock(return_value=b"pdf")))
        mock_open.return_value.__exit__ = Mock(return_value=False)

        mock_doc = MagicMock()
        mock_doc.is_encrypted = False
        mock_doc.__len__ = Mock(return_value=1)
        page = MagicMock()
        page.get_text.return_value = "File content"
        mock_doc.__iter__ = Mock(return_value=iter([page]))
        mock_fitz.open.return_value = mock_doc

        extractor = BJVPDFExtractor()
        result = extractor.extract_from_path("/path/to/file.pdf")

        assert result.total_paginas == 1
