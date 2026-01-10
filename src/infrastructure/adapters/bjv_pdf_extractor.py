"""
BJV PDF text extractor.

Extracts text from PDF files using PyMuPDF (fitz).
Tracks page numbers and calculates extraction confidence.
"""
import re
from dataclasses import dataclass
from typing import Dict
from pathlib import Path

try:
    import fitz  # PyMuPDF
except ImportError:
    fitz = None

from src.infrastructure.adapters.bjv_errors import (
    PDFCorruptedError,
    PDFPasswordProtectedError,
    PDFEmptyError,
)


@dataclass(frozen=True)
class PDFExtractionResult:
    """Result of PDF text extraction."""

    texto: str
    total_paginas: int
    paginas_texto: Dict[str, str]
    confianza: float


class BJVPDFExtractor:
    """
    PDF text extractor for BJV documents.

    Uses PyMuPDF to extract text while tracking page numbers
    and calculating extraction quality confidence.
    """

    # Minimum characters per page to consider valid extraction
    MIN_CHARS_PER_PAGE = 50

    # Patterns for quality assessment
    WORD_PATTERN = re.compile(r"\b[a-záéíóúüñA-ZÁÉÍÓÚÜÑ]{3,}\b")
    NOISE_PATTERN = re.compile(r"[^\w\s]")

    def __init__(self):
        """Initialize the PDF extractor."""
        if fitz is None:
            raise ImportError("PyMuPDF (fitz) is required for PDF extraction")

    def extract_text(self, pdf_bytes: bytes) -> PDFExtractionResult:
        """
        Extract text from PDF bytes.

        Args:
            pdf_bytes: Raw PDF file bytes

        Returns:
            PDFExtractionResult with extracted text and metadata

        Raises:
            PDFCorruptedError: If PDF cannot be opened
            PDFPasswordProtectedError: If PDF is encrypted
            PDFEmptyError: If PDF has no extractable text
        """
        try:
            doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        except Exception as e:
            raise PDFCorruptedError(f"No se pudo abrir el PDF: {str(e)}")

        # Check for password protection
        if hasattr(doc, "is_encrypted") and doc.is_encrypted:
            if hasattr(doc, "needs_pass") and doc.needs_pass:
                raise PDFPasswordProtectedError("El PDF está protegido con contraseña")

        # Extract text from each page
        paginas_texto = {}
        all_text_parts = []

        for i, page in enumerate(doc, start=1):
            page_text = page.get_text()
            paginas_texto[str(i)] = page_text
            if page_text.strip():
                all_text_parts.append(page_text)

        # Join all text
        texto_completo = "\n\n".join(all_text_parts)

        # Check if text was extracted
        if not texto_completo.strip():
            raise PDFEmptyError("El PDF no contiene texto extraíble")

        # Calculate confidence
        confianza = self._calculate_confidence(texto_completo, len(doc))

        return PDFExtractionResult(
            texto=texto_completo,
            total_paginas=len(doc),
            paginas_texto=paginas_texto,
            confianza=confianza,
        )

    def extract_from_path(self, file_path: str) -> PDFExtractionResult:
        """
        Extract text from PDF file path.

        Args:
            file_path: Path to PDF file

        Returns:
            PDFExtractionResult with extracted text and metadata
        """
        path = Path(file_path)
        with open(path, "rb") as f:
            pdf_bytes = f.read()
        return self.extract_text(pdf_bytes)

    def _calculate_confidence(self, texto: str, total_paginas: int) -> float:
        """
        Calculate extraction quality confidence score.

        Based on:
        - Ratio of valid words to total characters
        - Average text length per page
        - Presence of Spanish/legal vocabulary

        Args:
            texto: Extracted text
            total_paginas: Total number of pages

        Returns:
            Confidence score between 0.0 and 1.0
        """
        if not texto:
            return 0.0

        # Count valid words (3+ chars)
        words = self.WORD_PATTERN.findall(texto)
        word_count = len(words)

        # Calculate average word length
        avg_word_len = sum(len(w) for w in words) / max(word_count, 1)

        # Calculate text density (chars per page)
        chars_per_page = len(texto) / max(total_paginas, 1)

        # Calculate word to noise ratio
        noise_count = len(self.NOISE_PATTERN.findall(texto))
        word_to_noise = word_count / max(noise_count, 1)

        # Score components
        word_len_score = min(avg_word_len / 6.0, 1.0)  # 6 chars is good avg
        density_score = min(chars_per_page / 500.0, 1.0)  # 500 chars/page is good
        ratio_score = min(word_to_noise / 2.0, 1.0)  # 2:1 word to noise is good

        # Weighted average
        confidence = (
            word_len_score * 0.3 +
            density_score * 0.4 +
            ratio_score * 0.3
        )

        return round(min(max(confidence, 0.0), 1.0), 2)
