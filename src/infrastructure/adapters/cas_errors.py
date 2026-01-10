"""
CAS adapter error hierarchy.

Distinguishes recoverable from permanent errors.
Includes SPA-specific errors for browser automation.
"""


class CASAdapterError(Exception):
    """Base class for CAS adapter errors."""

    def __init__(self, message: str, recoverable: bool = True):
        super().__init__(message)
        self.recoverable = recoverable


class ParseError(CASAdapterError):
    """HTML/JSON parsing error."""

    def __init__(self, message: str, html_sample: str = ""):
        super().__init__(message, recoverable=True)
        self.html_sample = html_sample[:500] if html_sample else ""


class JavaScriptRenderError(CASAdapterError):
    """JavaScript rendering failed - SPA did not load properly."""

    def __init__(self, message: str, url: str = ""):
        super().__init__(message, recoverable=True)
        self.url = url


class BrowserTimeoutError(CASAdapterError):
    """Browser operation timed out."""

    def __init__(self, message: str, timeout_seconds: int = 30):
        super().__init__(message, recoverable=True)
        self.timeout_seconds = timeout_seconds


class BrowserNotAvailableError(CASAdapterError):
    """Playwright/browser not available."""

    def __init__(self, message: str):
        super().__init__(message, recoverable=False)


class CASRateLimitError(CASAdapterError):
    """CAS server rate limit (429)."""

    def __init__(self, message: str, retry_after: int = 60):
        super().__init__(message, recoverable=True)
        self.retry_after = retry_after


class CASServerError(CASAdapterError):
    """CAS server error (5xx)."""

    def __init__(self, message: str, status_code: int = 500):
        super().__init__(message, recoverable=True)
        self.status_code = status_code


class PDFExtractionError(CASAdapterError):
    """PDF text extraction error."""
    pass


class PDFNotFoundError(PDFExtractionError):
    """PDF file not found or not available."""

    def __init__(self, message: str):
        super().__init__(message, recoverable=False)


class CaseNumberParseError(CASAdapterError):
    """Failed to parse CAS case number format."""

    def __init__(self, message: str, raw_text: str = ""):
        super().__init__(message, recoverable=False)
        self.raw_text = raw_text


class ChunkingError(CASAdapterError):
    """Text chunking error."""
    pass
