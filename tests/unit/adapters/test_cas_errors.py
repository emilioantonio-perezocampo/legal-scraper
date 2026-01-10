"""
Tests for CAS Adapter Error Hierarchy

Following RED-GREEN TDD: These tests define the expected behavior
for CAS adapter error classes.

Target: ~15 tests for error hierarchy
"""
import pytest


class TestCASAdapterError:
    """Tests for base CASAdapterError class."""

    def test_instantiation_with_message(self):
        """Create CASAdapterError with message."""
        from src.infrastructure.adapters.cas_errors import CASAdapterError
        error = CASAdapterError("Test error message")
        assert str(error) == "Test error message"

    def test_default_recoverable_is_true(self):
        """Default recoverable flag is True."""
        from src.infrastructure.adapters.cas_errors import CASAdapterError
        error = CASAdapterError("Test")
        assert error.recoverable is True

    def test_recoverable_can_be_set_false(self):
        """Recoverable flag can be set to False."""
        from src.infrastructure.adapters.cas_errors import CASAdapterError
        error = CASAdapterError("Test", recoverable=False)
        assert error.recoverable is False

    def test_inherits_from_exception(self):
        """CASAdapterError inherits from Exception."""
        from src.infrastructure.adapters.cas_errors import CASAdapterError
        assert issubclass(CASAdapterError, Exception)


class TestParseError:
    """Tests for ParseError class."""

    def test_instantiation_with_message(self):
        """Create ParseError with message."""
        from src.infrastructure.adapters.cas_errors import ParseError
        error = ParseError("Invalid HTML structure")
        assert "Invalid HTML structure" in str(error)

    def test_has_html_sample_attribute(self):
        """ParseError has html_sample attribute."""
        from src.infrastructure.adapters.cas_errors import ParseError
        html = "<div>Test HTML</div>"
        error = ParseError("Parse failed", html_sample=html)
        assert error.html_sample == html

    def test_html_sample_truncated_to_500_chars(self):
        """Long html_sample is truncated to 500 chars."""
        from src.infrastructure.adapters.cas_errors import ParseError
        long_html = "x" * 1000
        error = ParseError("Parse failed", html_sample=long_html)
        assert len(error.html_sample) == 500

    def test_inherits_from_cas_adapter_error(self):
        """ParseError inherits from CASAdapterError."""
        from src.infrastructure.adapters.cas_errors import ParseError, CASAdapterError
        assert issubclass(ParseError, CASAdapterError)


class TestJavaScriptRenderError:
    """Tests for JavaScriptRenderError class."""

    def test_instantiation_with_message(self):
        """Create JavaScriptRenderError with message."""
        from src.infrastructure.adapters.cas_errors import JavaScriptRenderError
        error = JavaScriptRenderError("SPA failed to load")
        assert "SPA failed to load" in str(error)

    def test_has_url_attribute(self):
        """JavaScriptRenderError has url attribute."""
        from src.infrastructure.adapters.cas_errors import JavaScriptRenderError
        error = JavaScriptRenderError("Failed", url="https://example.com")
        assert error.url == "https://example.com"

    def test_recoverable_is_true(self):
        """JavaScriptRenderError is recoverable by default."""
        from src.infrastructure.adapters.cas_errors import JavaScriptRenderError
        error = JavaScriptRenderError("Failed")
        assert error.recoverable is True


class TestBrowserTimeoutError:
    """Tests for BrowserTimeoutError class."""

    def test_instantiation_with_message(self):
        """Create BrowserTimeoutError with message."""
        from src.infrastructure.adapters.cas_errors import BrowserTimeoutError
        error = BrowserTimeoutError("Page load timeout")
        assert "Page load timeout" in str(error)

    def test_has_timeout_seconds_attribute(self):
        """BrowserTimeoutError has timeout_seconds attribute."""
        from src.infrastructure.adapters.cas_errors import BrowserTimeoutError
        error = BrowserTimeoutError("Timeout", timeout_seconds=60)
        assert error.timeout_seconds == 60

    def test_default_timeout_is_30(self):
        """Default timeout_seconds is 30."""
        from src.infrastructure.adapters.cas_errors import BrowserTimeoutError
        error = BrowserTimeoutError("Timeout")
        assert error.timeout_seconds == 30


class TestBrowserNotAvailableError:
    """Tests for BrowserNotAvailableError class."""

    def test_instantiation_with_message(self):
        """Create BrowserNotAvailableError with message."""
        from src.infrastructure.adapters.cas_errors import BrowserNotAvailableError
        error = BrowserNotAvailableError("Playwright not installed")
        assert "Playwright not installed" in str(error)

    def test_recoverable_is_false(self):
        """BrowserNotAvailableError is NOT recoverable."""
        from src.infrastructure.adapters.cas_errors import BrowserNotAvailableError
        error = BrowserNotAvailableError("Not available")
        assert error.recoverable is False


class TestCASRateLimitError:
    """Tests for CASRateLimitError class."""

    def test_instantiation_with_message(self):
        """Create CASRateLimitError with message."""
        from src.infrastructure.adapters.cas_errors import CASRateLimitError
        error = CASRateLimitError("Rate limited")
        assert "Rate limited" in str(error)

    def test_has_retry_after_attribute(self):
        """CASRateLimitError has retry_after attribute."""
        from src.infrastructure.adapters.cas_errors import CASRateLimitError
        error = CASRateLimitError("Rate limited", retry_after=120)
        assert error.retry_after == 120

    def test_default_retry_after_is_60(self):
        """Default retry_after is 60 seconds."""
        from src.infrastructure.adapters.cas_errors import CASRateLimitError
        error = CASRateLimitError("Rate limited")
        assert error.retry_after == 60


class TestCASServerError:
    """Tests for CASServerError class."""

    def test_instantiation_with_message(self):
        """Create CASServerError with message."""
        from src.infrastructure.adapters.cas_errors import CASServerError
        error = CASServerError("Internal server error")
        assert "Internal server error" in str(error)

    def test_has_status_code_attribute(self):
        """CASServerError has status_code attribute."""
        from src.infrastructure.adapters.cas_errors import CASServerError
        error = CASServerError("Error", status_code=503)
        assert error.status_code == 503

    def test_default_status_code_is_500(self):
        """Default status_code is 500."""
        from src.infrastructure.adapters.cas_errors import CASServerError
        error = CASServerError("Error")
        assert error.status_code == 500


class TestPDFErrors:
    """Tests for PDF-related error classes."""

    def test_pdf_extraction_error_instantiation(self):
        """Create PDFExtractionError."""
        from src.infrastructure.adapters.cas_errors import PDFExtractionError
        error = PDFExtractionError("Failed to extract text")
        assert "Failed to extract text" in str(error)

    def test_pdf_not_found_error_is_not_recoverable(self):
        """PDFNotFoundError is NOT recoverable."""
        from src.infrastructure.adapters.cas_errors import PDFNotFoundError
        error = PDFNotFoundError("PDF not found")
        assert error.recoverable is False

    def test_pdf_not_found_inherits_from_pdf_extraction_error(self):
        """PDFNotFoundError inherits from PDFExtractionError."""
        from src.infrastructure.adapters.cas_errors import PDFNotFoundError, PDFExtractionError
        assert issubclass(PDFNotFoundError, PDFExtractionError)


class TestCaseNumberParseError:
    """Tests for CaseNumberParseError class."""

    def test_instantiation_with_message(self):
        """Create CaseNumberParseError with message."""
        from src.infrastructure.adapters.cas_errors import CaseNumberParseError
        error = CaseNumberParseError("Invalid case number format")
        assert "Invalid case number format" in str(error)

    def test_has_raw_text_attribute(self):
        """CaseNumberParseError has raw_text attribute."""
        from src.infrastructure.adapters.cas_errors import CaseNumberParseError
        error = CaseNumberParseError("Invalid", raw_text="ABC-123")
        assert error.raw_text == "ABC-123"

    def test_is_not_recoverable(self):
        """CaseNumberParseError is NOT recoverable."""
        from src.infrastructure.adapters.cas_errors import CaseNumberParseError
        error = CaseNumberParseError("Invalid")
        assert error.recoverable is False


class TestChunkingError:
    """Tests for ChunkingError class."""

    def test_instantiation_with_message(self):
        """Create ChunkingError with message."""
        from src.infrastructure.adapters.cas_errors import ChunkingError
        error = ChunkingError("Chunking failed")
        assert "Chunking failed" in str(error)

    def test_inherits_from_cas_adapter_error(self):
        """ChunkingError inherits from CASAdapterError."""
        from src.infrastructure.adapters.cas_errors import ChunkingError, CASAdapterError
        assert issubclass(ChunkingError, CASAdapterError)
