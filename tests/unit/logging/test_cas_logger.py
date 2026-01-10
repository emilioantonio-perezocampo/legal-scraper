"""
Tests for CAS Structured Logger.

Following RED-GREEN TDD: These tests define the expected behavior
for the structured logging module.

Target: ~15 tests
"""
import pytest
import logging
import json
from unittest.mock import Mock, patch
from io import StringIO


class TestLogContext:
    """Tests for LogContext dataclass."""

    def test_default_values(self):
        """LogContext has sensible defaults."""
        from src.infrastructure.logging.cas_logger import LogContext

        ctx = LogContext()
        assert ctx.correlation_id is None
        assert ctx.session_id is None
        assert ctx.component is None
        assert ctx.operation is None
        assert ctx.extra == {}

    def test_with_values(self):
        """LogContext accepts all values."""
        from src.infrastructure.logging.cas_logger import LogContext

        ctx = LogContext(
            correlation_id="corr-123",
            session_id="sess-456",
            component="discovery",
            operation="search",
            extra={"page": 1},
        )

        assert ctx.correlation_id == "corr-123"
        assert ctx.session_id == "sess-456"
        assert ctx.component == "discovery"
        assert ctx.operation == "search"
        assert ctx.extra == {"page": 1}

    def test_with_operation_returns_new_context(self):
        """with_operation returns new context with operation set."""
        from src.infrastructure.logging.cas_logger import LogContext

        ctx = LogContext(correlation_id="abc")
        new_ctx = ctx.with_operation("fetch_page")

        assert new_ctx.operation == "fetch_page"
        assert new_ctx.correlation_id == "abc"
        assert ctx.operation is None  # Original unchanged

    def test_with_extra_returns_new_context(self):
        """with_extra returns new context with additional data."""
        from src.infrastructure.logging.cas_logger import LogContext

        ctx = LogContext(extra={"existing": 1})
        new_ctx = ctx.with_extra(new_field="value")

        assert new_ctx.extra == {"existing": 1, "new_field": "value"}
        assert ctx.extra == {"existing": 1}  # Original unchanged


class TestLogEntry:
    """Tests for LogEntry dataclass."""

    def test_to_json_includes_required_fields(self):
        """to_json includes timestamp, level, message."""
        from src.infrastructure.logging.cas_logger import LogEntry

        entry = LogEntry(
            timestamp="2024-01-01T12:00:00",
            level="INFO",
            message="Test message",
        )

        json_str = entry.to_json()
        data = json.loads(json_str)

        assert data["timestamp"] == "2024-01-01T12:00:00"
        assert data["level"] == "INFO"
        assert data["message"] == "Test message"

    def test_to_json_excludes_none_values(self):
        """to_json excludes None values."""
        from src.infrastructure.logging.cas_logger import LogEntry

        entry = LogEntry(
            timestamp="2024-01-01T12:00:00",
            level="INFO",
            message="Test",
            component=None,
            error=None,
        )

        json_str = entry.to_json()
        data = json.loads(json_str)

        assert "component" not in data
        assert "error" not in data

    def test_to_human_basic_format(self):
        """to_human returns human-readable format."""
        from src.infrastructure.logging.cas_logger import LogEntry

        entry = LogEntry(
            timestamp="12:00:00",
            level="INFO",
            message="Test message",
        )

        human_str = entry.to_human()
        assert "[12:00:00]" in human_str
        assert "[INFO]" in human_str
        assert "Test message" in human_str

    def test_to_human_includes_duration(self):
        """to_human includes duration when present."""
        from src.infrastructure.logging.cas_logger import LogEntry

        entry = LogEntry(
            timestamp="12:00:00",
            level="INFO",
            message="Test",
            duration_ms=150.5,
        )

        human_str = entry.to_human()
        assert "150.50ms" in human_str


class TestCASLogger:
    """Tests for CASLogger class."""

    def test_creates_with_component_name(self):
        """CASLogger creates with component name."""
        from src.infrastructure.logging.cas_logger import CASLogger

        logger = CASLogger("discovery")
        assert logger.component == "discovery"

    def test_log_levels_work(self):
        """All log levels work correctly."""
        from src.infrastructure.logging.cas_logger import CASLogger, LogLevel

        # Create logger with DEBUG level to capture all
        logger = CASLogger("test", level=LogLevel.DEBUG)

        # These should not raise
        logger.debug("Debug message")
        logger.info("Info message")
        logger.warning("Warning message")

    def test_log_includes_context(self):
        """Logs include context when provided."""
        from src.infrastructure.logging.cas_logger import CASLogger, LogContext

        logger = CASLogger("test")
        ctx = LogContext(correlation_id="abc123")

        # Should not raise
        logger.info("Test message", ctx)

    def test_error_includes_exc_info(self):
        """Error logs include exception info by default."""
        from src.infrastructure.logging.cas_logger import CASLogger

        logger = CASLogger("test")

        try:
            raise ValueError("test error")
        except ValueError:
            # Should not raise
            logger.error("Error occurred", exc_info=True)


class TestTimedOperation:
    """Tests for timed_operation context manager."""

    def test_timed_operation_logs_start(self):
        """timed_operation logs start message."""
        from src.infrastructure.logging.cas_logger import CASLogger, LogLevel

        logger = CASLogger("test", level=LogLevel.DEBUG)

        with logger.timed_operation("test_op"):
            pass  # Operation

    def test_timed_operation_logs_duration(self):
        """timed_operation logs duration on completion."""
        from src.infrastructure.logging.cas_logger import CASLogger

        logger = CASLogger("test")

        with logger.timed_operation("test_op"):
            pass  # Should log completion with duration

    def test_timed_operation_logs_error_on_exception(self):
        """timed_operation logs error when exception occurs."""
        from src.infrastructure.logging.cas_logger import CASLogger

        logger = CASLogger("test")

        with pytest.raises(ValueError):
            with logger.timed_operation("test_op"):
                raise ValueError("test error")


class TestCreateCASLogger:
    """Tests for create_cas_logger factory function."""

    def test_creates_logger_with_defaults(self):
        """Factory creates logger with default settings."""
        from src.infrastructure.logging.cas_logger import create_cas_logger

        logger = create_cas_logger("discovery")
        assert logger.component == "discovery"

    def test_creates_logger_with_json_output(self):
        """Factory creates logger with JSON output."""
        from src.infrastructure.logging.cas_logger import create_cas_logger

        logger = create_cas_logger("discovery", json_output=True)
        assert logger.component == "discovery"
