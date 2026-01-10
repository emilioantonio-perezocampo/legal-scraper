"""
BJV adapter error hierarchy.

Distinguishes recoverable from permanent errors for
proper retry and circuit breaker behavior.
"""


class BJVAdapterError(Exception):
    """Base class for BJV adapter errors."""

    def __init__(self, message: str, recoverable: bool = True):
        super().__init__(message)
        self.recoverable = recoverable


class ParseError(BJVAdapterError):
    """HTML parsing error."""

    def __init__(self, message: str, html_sample: str = ""):
        super().__init__(message, recoverable=True)
        self.html_sample = html_sample[:500] if html_sample else ""


class PDFExtractionError(BJVAdapterError):
    """PDF text extraction error."""

    pass


class PDFCorruptedError(PDFExtractionError):
    """PDF file is corrupted."""

    def __init__(self, message: str):
        super().__init__(message, recoverable=False)


class PDFPasswordProtectedError(PDFExtractionError):
    """PDF requires password."""

    def __init__(self, message: str):
        super().__init__(message, recoverable=False)


class PDFEmptyError(PDFExtractionError):
    """PDF has no extractable text."""

    def __init__(self, message: str):
        super().__init__(message, recoverable=False)


class ChunkingError(BJVAdapterError):
    """Text chunking error."""

    pass


class NetworkError(BJVAdapterError):
    """Network-related error."""

    def __init__(self, message: str, status_code: int = 0):
        super().__init__(message, recoverable=True)
        self.status_code = status_code


class RateLimitError(NetworkError):
    """Rate limit exceeded."""

    def __init__(self, message: str, retry_after: int = 60):
        super().__init__(message, status_code=429)
        self.retry_after = retry_after
