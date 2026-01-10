"""
Tests for BJV adapter error hierarchy.

Following RED-GREEN TDD: These tests define the expected behavior
for the BJV adapter error classes.

Target: ~15 tests
"""
import pytest


class TestBJVAdapterError:
    """Tests for base BJVAdapterError class."""

    def test_inherits_from_exception(self):
        """BJVAdapterError inherits from Exception."""
        from src.infrastructure.adapters.bjv_errors import BJVAdapterError

        assert issubclass(BJVAdapterError, Exception)

    def test_default_recoverable_is_true(self):
        """BJVAdapterError defaults to recoverable=True."""
        from src.infrastructure.adapters.bjv_errors import BJVAdapterError

        error = BJVAdapterError("Test error")

        assert error.recoverable is True

    def test_message_accessible(self):
        """BJVAdapterError message is accessible via str()."""
        from src.infrastructure.adapters.bjv_errors import BJVAdapterError

        error = BJVAdapterError("Error de conexión")

        assert str(error) == "Error de conexión"

    def test_recoverable_can_be_set_false(self):
        """BJVAdapterError recoverable can be set to False."""
        from src.infrastructure.adapters.bjv_errors import BJVAdapterError

        error = BJVAdapterError("Error permanente", recoverable=False)

        assert error.recoverable is False


class TestParseError:
    """Tests for ParseError class."""

    def test_inherits_from_bjv_adapter_error(self):
        """ParseError inherits from BJVAdapterError."""
        from src.infrastructure.adapters.bjv_errors import ParseError, BJVAdapterError

        assert issubclass(ParseError, BJVAdapterError)

    def test_is_recoverable_by_default(self):
        """ParseError is recoverable by default."""
        from src.infrastructure.adapters.bjv_errors import ParseError

        error = ParseError("Error de parsing")

        assert error.recoverable is True

    def test_stores_html_sample(self):
        """ParseError stores HTML sample."""
        from src.infrastructure.adapters.bjv_errors import ParseError

        html = "<div>Sample HTML</div>"
        error = ParseError("Parse failed", html_sample=html)

        assert error.html_sample == html

    def test_truncates_long_html_sample(self):
        """ParseError truncates HTML sample to 500 chars."""
        from src.infrastructure.adapters.bjv_errors import ParseError

        long_html = "x" * 1000
        error = ParseError("Parse failed", html_sample=long_html)

        assert len(error.html_sample) == 500

    def test_empty_html_sample_default(self):
        """ParseError defaults to empty html_sample."""
        from src.infrastructure.adapters.bjv_errors import ParseError

        error = ParseError("Parse failed")

        assert error.html_sample == ""


class TestPDFExtractionError:
    """Tests for PDF extraction error classes."""

    def test_pdf_extraction_error_inherits_from_base(self):
        """PDFExtractionError inherits from BJVAdapterError."""
        from src.infrastructure.adapters.bjv_errors import (
            PDFExtractionError,
            BJVAdapterError,
        )

        assert issubclass(PDFExtractionError, BJVAdapterError)

    def test_pdf_corrupted_error_not_recoverable(self):
        """PDFCorruptedError is not recoverable."""
        from src.infrastructure.adapters.bjv_errors import PDFCorruptedError

        error = PDFCorruptedError("PDF corrupto")

        assert error.recoverable is False

    def test_pdf_password_protected_not_recoverable(self):
        """PDFPasswordProtectedError is not recoverable."""
        from src.infrastructure.adapters.bjv_errors import PDFPasswordProtectedError

        error = PDFPasswordProtectedError("PDF protegido con contraseña")

        assert error.recoverable is False

    def test_pdf_empty_error_not_recoverable(self):
        """PDFEmptyError is not recoverable."""
        from src.infrastructure.adapters.bjv_errors import PDFEmptyError

        error = PDFEmptyError("PDF sin texto extraíble")

        assert error.recoverable is False


class TestChunkingError:
    """Tests for ChunkingError class."""

    def test_inherits_from_bjv_adapter_error(self):
        """ChunkingError inherits from BJVAdapterError."""
        from src.infrastructure.adapters.bjv_errors import ChunkingError, BJVAdapterError

        assert issubclass(ChunkingError, BJVAdapterError)


class TestNetworkError:
    """Tests for NetworkError class."""

    def test_inherits_from_bjv_adapter_error(self):
        """NetworkError inherits from BJVAdapterError."""
        from src.infrastructure.adapters.bjv_errors import NetworkError, BJVAdapterError

        assert issubclass(NetworkError, BJVAdapterError)

    def test_is_recoverable_by_default(self):
        """NetworkError is recoverable by default."""
        from src.infrastructure.adapters.bjv_errors import NetworkError

        error = NetworkError("Error de red")

        assert error.recoverable is True

    def test_stores_status_code(self):
        """NetworkError stores status code."""
        from src.infrastructure.adapters.bjv_errors import NetworkError

        error = NetworkError("Server error", status_code=500)

        assert error.status_code == 500

    def test_default_status_code_is_zero(self):
        """NetworkError defaults status_code to 0."""
        from src.infrastructure.adapters.bjv_errors import NetworkError

        error = NetworkError("Connection failed")

        assert error.status_code == 0


class TestRateLimitError:
    """Tests for RateLimitError class."""

    def test_inherits_from_network_error(self):
        """RateLimitError inherits from NetworkError."""
        from src.infrastructure.adapters.bjv_errors import RateLimitError, NetworkError

        assert issubclass(RateLimitError, NetworkError)

    def test_status_code_is_429(self):
        """RateLimitError has status_code 429."""
        from src.infrastructure.adapters.bjv_errors import RateLimitError

        error = RateLimitError("Límite de solicitudes excedido")

        assert error.status_code == 429

    def test_stores_retry_after(self):
        """RateLimitError stores retry_after seconds."""
        from src.infrastructure.adapters.bjv_errors import RateLimitError

        error = RateLimitError("Rate limited", retry_after=120)

        assert error.retry_after == 120

    def test_default_retry_after_is_60(self):
        """RateLimitError defaults retry_after to 60."""
        from src.infrastructure.adapters.bjv_errors import RateLimitError

        error = RateLimitError("Rate limited")

        assert error.retry_after == 60
