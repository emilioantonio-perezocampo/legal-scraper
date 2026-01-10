"""
PDF Extractor Adapter

Extracts text from PDF files with optional OCR fallback.
Supports both file paths and byte content.
"""
import io
import re
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Optional, List

from .errors import (
    PDFExtractionError,
    PDFCorruptedError,
    PDFPasswordProtectedError,
    PDFEmptyError,
)

# Import pdfplumber at module level for patching
try:
    import pdfplumber
except ImportError:
    pdfplumber = None


class ExtractionMethod(Enum):
    """Method used to extract text from PDF."""
    TEXT_LAYER = "text_layer"
    OCR = "ocr"
    HYBRID = "hybrid"


@dataclass(frozen=True)
class PDFExtractionResult:
    """
    Result of PDF text extraction.

    Attributes:
        text: Extracted text content
        page_count: Number of pages in PDF
        extraction_method: Method used (text_layer, ocr, hybrid)
        confidence: Estimated quality 0.0 to 1.0
        warnings: Tuple of warning messages
    """
    text: str
    page_count: int
    extraction_method: ExtractionMethod
    confidence: float
    warnings: tuple = field(default_factory=tuple)


class PDFExtractor:
    """
    Extracts text from PDF files.

    Supports text layer extraction via pdfplumber with optional
    OCR fallback for scanned documents.
    """

    def __init__(self, ocr_enabled: bool = False, language: str = "spa"):
        """
        Initialize the PDF extractor.

        Args:
            ocr_enabled: Whether to use OCR for scanned PDFs
            language: Language for OCR (default: Spanish)
        """
        self.ocr_enabled = ocr_enabled
        self.language = language

    def extract(self, pdf_path: Path) -> PDFExtractionResult:
        """
        Extract text from a PDF file.

        Args:
            pdf_path: Path to PDF file

        Returns:
            PDFExtractionResult with extracted text

        Raises:
            PDFExtractionError: If file doesn't exist
            PDFCorruptedError: If PDF is invalid
            PDFPasswordProtectedError: If PDF is encrypted
            PDFEmptyError: If PDF has no extractable text
        """
        pdf_path = Path(pdf_path)

        if not pdf_path.exists():
            raise PDFExtractionError(f"File not found: {pdf_path}")

        try:
            pdf_bytes = pdf_path.read_bytes()
            return self.extract_from_bytes(pdf_bytes)
        except (PDFExtractionError, PDFCorruptedError, PDFPasswordProtectedError, PDFEmptyError):
            raise
        except Exception as e:
            raise PDFCorruptedError(f"Failed to read PDF: {e}")

    def extract_from_bytes(self, pdf_bytes: bytes) -> PDFExtractionResult:
        """
        Extract text from PDF bytes.

        Args:
            pdf_bytes: Raw PDF file bytes

        Returns:
            PDFExtractionResult with extracted text

        Raises:
            PDFExtractionError: If bytes are empty
            PDFCorruptedError: If PDF is invalid
            PDFPasswordProtectedError: If PDF is encrypted
            PDFEmptyError: If PDF has no extractable text
        """
        if not pdf_bytes:
            raise PDFExtractionError("Empty PDF bytes provided")

        if pdfplumber is None:
            raise PDFExtractionError("pdfplumber not installed")

        # Try text layer extraction
        text, page_count, warnings = self._extract_with_pdfplumber(pdf_bytes)

        if text and text.strip():
            confidence = self._calculate_confidence(text)
            return PDFExtractionResult(
                text=text,
                page_count=page_count,
                extraction_method=ExtractionMethod.TEXT_LAYER,
                confidence=confidence,
                warnings=tuple(warnings),
            )

        # No text extracted
        if self.ocr_enabled:
            # OCR fallback would go here
            # For now, raise empty error
            raise PDFEmptyError("PDF has no text layer and OCR is not implemented")

        raise PDFEmptyError("PDF has no extractable text (OCR disabled)")

    def _extract_with_pdfplumber(self, pdf_bytes: bytes) -> tuple:
        """
        Extract text using pdfplumber.

        Returns:
            (text, page_count, warnings)
        """
        warnings = []
        text_parts = []

        try:
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                page_count = len(pdf.pages)
                pages_without_text = []

                for i, page in enumerate(pdf.pages, 1):
                    try:
                        page_text = page.extract_text()
                        if page_text:
                            text_parts.append(page_text)
                        else:
                            pages_without_text.append(i)
                    except Exception as e:
                        warnings.append(f"Error extracting page {i}: {e}")
                        pages_without_text.append(i)

                if pages_without_text:
                    if len(pages_without_text) == page_count:
                        # All pages empty
                        pass
                    else:
                        warnings.append(
                            f"Pages without text: {pages_without_text}"
                        )

                text = "\n\n".join(text_parts)
                return text, page_count, warnings

        except Exception as e:
            error_str = str(e).lower()

            if "password" in error_str or "encrypted" in error_str:
                raise PDFPasswordProtectedError(f"PDF is password protected: {e}")

            raise PDFCorruptedError(f"Failed to parse PDF: {e}")

    def _calculate_confidence(self, text: str) -> float:
        """
        Estimate extraction quality based on text characteristics.

        Returns confidence score between 0.0 and 1.0.
        """
        if not text:
            return 0.0

        # Factors that indicate good extraction
        word_count = len(text.split())
        if word_count == 0:
            return 0.0

        # Check for reasonable word lengths
        words = text.split()
        avg_word_length = sum(len(w) for w in words) / len(words)

        # Spanish average word length is about 5-6 characters
        word_length_score = 1.0 if 3 <= avg_word_length <= 12 else 0.3

        # Check for excessive special characters (sign of bad OCR)
        special_char_ratio = len(re.findall(r'[§¶@#$%&*]', text)) / len(text)
        special_char_score = 1.0 if special_char_ratio < 0.02 else 0.2

        # Check for reasonable sentence structure (periods, spaces)
        has_sentences = bool(re.search(r'\w+[.!?]\s+\w+', text))
        sentence_score = 1.0 if has_sentences else 0.5

        # Check for Spanish characters (good sign)
        has_spanish = bool(re.search(r'[áéíóúüñÁÉÍÓÚÜÑ]', text))
        spanish_score = 1.1 if has_spanish else 1.0  # Slight bonus

        # Check for very short words (sign of garbled text)
        short_words = sum(1 for w in words if len(w) <= 2)
        short_word_ratio = short_words / len(words)
        short_word_score = 1.0 if short_word_ratio < 0.3 else 0.2

        # Combine scores
        confidence = (
            word_length_score * 0.25 +
            special_char_score * 0.25 +
            sentence_score * 0.2 +
            short_word_score * 0.3
        ) * spanish_score

        return min(1.0, max(0.0, confidence))
