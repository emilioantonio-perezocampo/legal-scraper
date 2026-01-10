"""
RED Phase Tests: PDF Extractor

Tests for PDF text extraction adapter.
These tests must FAIL initially (RED phase).
"""
import pytest
from pathlib import Path
from dataclasses import FrozenInstanceError
from unittest.mock import Mock, patch, MagicMock
import io

# These imports will fail until implementation exists
from src.infrastructure.adapters.pdf_extractor import (
    PDFExtractor,
    PDFExtractionResult,
    ExtractionMethod,
)
from src.infrastructure.adapters.errors import (
    PDFExtractionError,
    PDFCorruptedError,
    PDFPasswordProtectedError,
    PDFEmptyError,
)


class TestExtractionMethod:
    """Tests for ExtractionMethod enum."""

    def test_text_layer_exists(self):
        """TEXT_LAYER method should exist."""
        assert ExtractionMethod.TEXT_LAYER.value == "text_layer"

    def test_ocr_exists(self):
        """OCR method should exist."""
        assert ExtractionMethod.OCR.value == "ocr"

    def test_hybrid_exists(self):
        """HYBRID method should exist."""
        assert ExtractionMethod.HYBRID.value == "hybrid"


class TestPDFExtractionResult:
    """Tests for PDFExtractionResult dataclass."""

    def test_create_result(self):
        """Result should be creatable with all fields."""
        result = PDFExtractionResult(
            text="Sample extracted text",
            page_count=5,
            extraction_method=ExtractionMethod.TEXT_LAYER,
            confidence=0.95,
            warnings=("Minor formatting issue",),
        )
        assert result.text == "Sample extracted text"
        assert result.page_count == 5
        assert result.extraction_method == ExtractionMethod.TEXT_LAYER
        assert result.confidence == 0.95
        assert len(result.warnings) == 1

    def test_result_default_warnings(self):
        """Result should default to empty warnings."""
        result = PDFExtractionResult(
            text="Text",
            page_count=1,
            extraction_method=ExtractionMethod.TEXT_LAYER,
            confidence=1.0,
        )
        assert result.warnings == ()

    def test_result_immutable(self):
        """Result should be frozen/immutable."""
        result = PDFExtractionResult(
            text="Text",
            page_count=1,
            extraction_method=ExtractionMethod.TEXT_LAYER,
            confidence=1.0,
        )
        with pytest.raises(FrozenInstanceError):
            result.text = "Changed"


class TestPDFExtractorConfiguration:
    """Tests for PDFExtractor configuration."""

    def test_default_language_spanish(self):
        """Default language should be Spanish."""
        extractor = PDFExtractor()
        assert extractor.language == "spa"

    def test_ocr_disabled_by_default(self):
        """OCR should be disabled by default."""
        extractor = PDFExtractor()
        assert extractor.ocr_enabled is False

    def test_ocr_can_be_enabled(self):
        """OCR should be enableable."""
        extractor = PDFExtractor(ocr_enabled=True)
        assert extractor.ocr_enabled is True

    def test_custom_language(self):
        """Custom language should be settable."""
        extractor = PDFExtractor(language="eng")
        assert extractor.language == "eng"


class TestPDFExtractorFromBytes:
    """Tests for extracting from bytes."""

    def test_extract_from_bytes_valid_pdf(self):
        """Should extract text from valid PDF bytes."""
        # Create a minimal valid PDF in memory
        extractor = PDFExtractor()

        # Mock pdfplumber to return text
        with patch('src.infrastructure.adapters.pdf_extractor.pdfplumber') as mock_plumber:
            mock_pdf = MagicMock()
            mock_page = MagicMock()
            mock_page.extract_text.return_value = "Sample PDF text content"
            mock_pdf.pages = [mock_page]
            mock_pdf.__enter__ = Mock(return_value=mock_pdf)
            mock_pdf.__exit__ = Mock(return_value=False)
            mock_plumber.open.return_value = mock_pdf

            result = extractor.extract_from_bytes(b"fake pdf bytes")

            assert result.text == "Sample PDF text content"
            assert result.page_count == 1
            assert result.extraction_method == ExtractionMethod.TEXT_LAYER

    def test_extract_from_bytes_empty(self):
        """Should handle empty bytes."""
        extractor = PDFExtractor()

        with pytest.raises(PDFExtractionError):
            extractor.extract_from_bytes(b"")

    def test_extract_from_bytes_invalid(self):
        """Should raise error for invalid PDF bytes."""
        extractor = PDFExtractor()

        with patch('src.infrastructure.adapters.pdf_extractor.pdfplumber') as mock_plumber:
            mock_plumber.open.side_effect = Exception("Invalid PDF")

            with pytest.raises(PDFCorruptedError):
                extractor.extract_from_bytes(b"not a pdf")


class TestPDFExtractorTextQuality:
    """Tests for text extraction quality."""

    def test_extract_preserves_spanish_characters(self):
        """Should preserve Spanish accents and ñ."""
        extractor = PDFExtractor()

        with patch('src.infrastructure.adapters.pdf_extractor.pdfplumber') as mock_plumber:
            mock_pdf = MagicMock()
            mock_page = MagicMock()
            mock_page.extract_text.return_value = "Constitución Política de España"
            mock_pdf.pages = [mock_page]
            mock_pdf.__enter__ = Mock(return_value=mock_pdf)
            mock_pdf.__exit__ = Mock(return_value=False)
            mock_plumber.open.return_value = mock_pdf

            result = extractor.extract_from_bytes(b"fake pdf")

            assert "Constitución" in result.text
            assert "España" in result.text

    def test_extract_multipage_pdf(self):
        """Should extract from multiple pages."""
        extractor = PDFExtractor()

        with patch('src.infrastructure.adapters.pdf_extractor.pdfplumber') as mock_plumber:
            mock_pdf = MagicMock()
            mock_page1 = MagicMock()
            mock_page1.extract_text.return_value = "Page 1 content"
            mock_page2 = MagicMock()
            mock_page2.extract_text.return_value = "Page 2 content"
            mock_page3 = MagicMock()
            mock_page3.extract_text.return_value = "Page 3 content"
            mock_pdf.pages = [mock_page1, mock_page2, mock_page3]
            mock_pdf.__enter__ = Mock(return_value=mock_pdf)
            mock_pdf.__exit__ = Mock(return_value=False)
            mock_plumber.open.return_value = mock_pdf

            result = extractor.extract_from_bytes(b"fake pdf")

            assert result.page_count == 3
            assert "Page 1" in result.text
            assert "Page 2" in result.text
            assert "Page 3" in result.text


class TestPDFExtractorErrorHandling:
    """Tests for error handling."""

    def test_extract_corrupted_pdf_raises_error(self):
        """Should raise PDFCorruptedError for corrupted files."""
        extractor = PDFExtractor()

        with patch('src.infrastructure.adapters.pdf_extractor.pdfplumber') as mock_plumber:
            mock_plumber.open.side_effect = Exception("PDF parsing error")

            with pytest.raises(PDFCorruptedError):
                extractor.extract_from_bytes(b"corrupted data")

    def test_extract_password_protected_raises_error(self):
        """Should raise PDFPasswordProtectedError for encrypted files."""
        extractor = PDFExtractor()

        with patch('src.infrastructure.adapters.pdf_extractor.pdfplumber') as mock_plumber:
            mock_plumber.open.side_effect = Exception("password required")

            with pytest.raises(PDFPasswordProtectedError):
                extractor.extract_from_bytes(b"encrypted pdf")

    def test_extract_empty_pdf_raises_error(self):
        """Should raise PDFEmptyError for PDFs with no text."""
        extractor = PDFExtractor()

        with patch('src.infrastructure.adapters.pdf_extractor.pdfplumber') as mock_plumber:
            mock_pdf = MagicMock()
            mock_page = MagicMock()
            mock_page.extract_text.return_value = None
            mock_pdf.pages = [mock_page]
            mock_pdf.__enter__ = Mock(return_value=mock_pdf)
            mock_pdf.__exit__ = Mock(return_value=False)
            mock_plumber.open.return_value = mock_pdf

            with pytest.raises(PDFEmptyError):
                extractor.extract_from_bytes(b"empty pdf")


class TestPDFExtractorFromPath:
    """Tests for extracting from file path."""

    def test_extract_nonexistent_file_raises_error(self, tmp_path):
        """Should raise error for nonexistent file."""
        extractor = PDFExtractor()
        fake_path = tmp_path / "nonexistent.pdf"

        with pytest.raises(PDFExtractionError):
            extractor.extract(fake_path)

    def test_extract_non_pdf_file_raises_error(self, tmp_path):
        """Should raise error for non-PDF file."""
        extractor = PDFExtractor()
        text_file = tmp_path / "document.txt"
        text_file.write_text("This is not a PDF")

        with pytest.raises(PDFCorruptedError):
            extractor.extract(text_file)


class TestPDFExtractorConfidence:
    """Tests for confidence scoring."""

    def test_confidence_high_for_clean_text(self):
        """Clean text should have high confidence."""
        extractor = PDFExtractor()

        with patch('src.infrastructure.adapters.pdf_extractor.pdfplumber') as mock_plumber:
            mock_pdf = MagicMock()
            mock_page = MagicMock()
            # Clean, well-formatted text
            mock_page.extract_text.return_value = (
                "Artículo 1. Esta es una disposición legal clara y bien formateada. "
                "Contiene palabras normales en español sin caracteres extraños."
            )
            mock_pdf.pages = [mock_page]
            mock_pdf.__enter__ = Mock(return_value=mock_pdf)
            mock_pdf.__exit__ = Mock(return_value=False)
            mock_plumber.open.return_value = mock_pdf

            result = extractor.extract_from_bytes(b"fake pdf")

            assert result.confidence >= 0.8

    def test_confidence_low_for_garbled_text(self):
        """Garbled OCR text should have low confidence."""
        extractor = PDFExtractor()

        with patch('src.infrastructure.adapters.pdf_extractor.pdfplumber') as mock_plumber:
            mock_pdf = MagicMock()
            mock_page = MagicMock()
            # Garbled text with many special chars and short words
            mock_page.extract_text.return_value = "a§ b¶ c@ d# e% f& g* h! i? j"
            mock_pdf.pages = [mock_page]
            mock_pdf.__enter__ = Mock(return_value=mock_pdf)
            mock_pdf.__exit__ = Mock(return_value=False)
            mock_plumber.open.return_value = mock_pdf

            result = extractor.extract_from_bytes(b"fake pdf")

            assert result.confidence < 0.5


class TestPDFExtractorOCRFallback:
    """Tests for OCR fallback scenarios."""

    def test_ocr_disabled_returns_empty_for_scanned(self):
        """With OCR disabled, scanned PDF should return empty or raise error."""
        extractor = PDFExtractor(ocr_enabled=False)

        with patch('src.infrastructure.adapters.pdf_extractor.pdfplumber') as mock_plumber:
            mock_pdf = MagicMock()
            mock_page = MagicMock()
            mock_page.extract_text.return_value = None  # No text layer
            mock_pdf.pages = [mock_page]
            mock_pdf.__enter__ = Mock(return_value=mock_pdf)
            mock_pdf.__exit__ = Mock(return_value=False)
            mock_plumber.open.return_value = mock_pdf

            with pytest.raises(PDFEmptyError):
                extractor.extract_from_bytes(b"scanned pdf")


class TestPDFExtractorWarnings:
    """Tests for warning generation."""

    def test_warnings_for_partial_extraction(self):
        """Should warn when some pages have no text."""
        extractor = PDFExtractor()

        with patch('src.infrastructure.adapters.pdf_extractor.pdfplumber') as mock_plumber:
            mock_pdf = MagicMock()
            mock_page1 = MagicMock()
            mock_page1.extract_text.return_value = "Page 1 has text"
            mock_page2 = MagicMock()
            mock_page2.extract_text.return_value = None  # No text
            mock_page3 = MagicMock()
            mock_page3.extract_text.return_value = "Page 3 has text"
            mock_pdf.pages = [mock_page1, mock_page2, mock_page3]
            mock_pdf.__enter__ = Mock(return_value=mock_pdf)
            mock_pdf.__exit__ = Mock(return_value=False)
            mock_plumber.open.return_value = mock_pdf

            result = extractor.extract_from_bytes(b"fake pdf")

            # Should have warning about page 2
            assert len(result.warnings) >= 1
            assert any("page" in w.lower() for w in result.warnings)
