"""
Adapter Error Types

Custom exceptions for infrastructure adapters.
Provides categorized errors for parsing, PDF extraction, and text processing.
"""


class AdapterError(Exception):
    """Base exception for all adapter errors."""
    pass


class ParseError(AdapterError):
    """
    HTML parsing failed.

    Raised when BeautifulSoup cannot extract expected elements from HTML.
    """
    def __init__(self, message: str, html_snippet: str = ""):
        super().__init__(message)
        self.html_snippet = html_snippet[:200] if html_snippet else ""  # Truncate for debugging


class PDFExtractionError(AdapterError):
    """
    PDF text extraction failed.

    Base class for PDF-related errors.
    """
    pass


class PDFCorruptedError(PDFExtractionError):
    """
    PDF file is corrupted or invalid.

    Raised when the PDF cannot be opened or parsed.
    """
    pass


class PDFPasswordProtectedError(PDFExtractionError):
    """
    PDF requires password to open.

    Raised when PDF is encrypted and password is not provided.
    """
    pass


class PDFEmptyError(PDFExtractionError):
    """
    PDF contains no extractable text.

    Raised when PDF opens but yields no text content.
    """
    pass


class ChunkingError(AdapterError):
    """
    Text chunking failed.

    Raised when text cannot be properly segmented into chunks.
    """
    pass


class TokenizationError(ChunkingError):
    """
    Token counting failed.

    Raised when tiktoken cannot encode the text.
    """
    pass
