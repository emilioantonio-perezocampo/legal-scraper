"""
Structured logging for CAS scraper.

Provides consistent, structured logging with:
- JSON output for production
- Human-readable output for development
- Context tracking (correlation_id, session_id)
- Performance metrics
"""
import logging
import json
import sys
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Optional, Dict, Any
from pathlib import Path
from enum import Enum


class LogLevel(Enum):
    """Log levels."""
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


@dataclass
class LogContext:
    """Context for structured logging."""
    correlation_id: Optional[str] = None
    session_id: Optional[str] = None
    component: Optional[str] = None
    operation: Optional[str] = None
    extra: Dict[str, Any] = field(default_factory=dict)

    def with_operation(self, operation: str) -> 'LogContext':
        """Return new context with operation set."""
        return LogContext(
            correlation_id=self.correlation_id,
            session_id=self.session_id,
            component=self.component,
            operation=operation,
            extra=self.extra.copy(),
        )

    def with_extra(self, **kwargs) -> 'LogContext':
        """Return new context with additional data."""
        new_extra = self.extra.copy()
        new_extra.update(kwargs)
        return LogContext(
            correlation_id=self.correlation_id,
            session_id=self.session_id,
            component=self.component,
            operation=self.operation,
            extra=new_extra,
        )


@dataclass
class LogEntry:
    """Structured log entry."""
    timestamp: str
    level: str
    message: str
    component: Optional[str] = None
    correlation_id: Optional[str] = None
    session_id: Optional[str] = None
    operation: Optional[str] = None
    duration_ms: Optional[float] = None
    error: Optional[str] = None
    error_type: Optional[str] = None
    extra: Dict[str, Any] = field(default_factory=dict)

    def to_json(self) -> str:
        """Convert to JSON string."""
        data = {k: v for k, v in asdict(self).items() if v is not None}
        if not data.get('extra'):
            data.pop('extra', None)
        return json.dumps(data)

    def to_human(self) -> str:
        """Convert to human-readable string."""
        parts = [
            f"[{self.timestamp}]",
            f"[{self.level}]",
        ]

        if self.component:
            parts.append(f"[{self.component}]")

        parts.append(self.message)

        if self.duration_ms is not None:
            parts.append(f"({self.duration_ms:.2f}ms)")

        if self.error:
            parts.append(f"ERROR: {self.error}")

        return " ".join(parts)


class JSONFormatter(logging.Formatter):
    """JSON log formatter for production."""

    def format(self, record: logging.LogRecord) -> str:
        entry = LogEntry(
            timestamp=datetime.utcnow().isoformat(),
            level=record.levelname,
            message=record.getMessage(),
            component=getattr(record, 'component', None),
            correlation_id=getattr(record, 'correlation_id', None),
            session_id=getattr(record, 'session_id', None),
            operation=getattr(record, 'operation', None),
            duration_ms=getattr(record, 'duration_ms', None),
            error=str(record.exc_info[1]) if record.exc_info else None,
            error_type=record.exc_info[0].__name__ if record.exc_info and record.exc_info[0] else None,
            extra=getattr(record, 'extra', {}),
        )
        return entry.to_json()


class HumanFormatter(logging.Formatter):
    """Human-readable formatter for development."""

    def format(self, record: logging.LogRecord) -> str:
        entry = LogEntry(
            timestamp=datetime.utcnow().strftime("%H:%M:%S"),
            level=record.levelname,
            message=record.getMessage(),
            component=getattr(record, 'component', None),
            duration_ms=getattr(record, 'duration_ms', None),
            error=str(record.exc_info[1]) if record.exc_info else None,
        )
        return entry.to_human()


class CASLogger:
    """
    Structured logger for CAS scraper.

    Usage:
        logger = CASLogger("discovery")

        ctx = LogContext(correlation_id="abc123")
        logger.info("Starting search", ctx)

        with logger.timed_operation("fetch_page", ctx):
            await fetch_page()
    """

    def __init__(
        self,
        component: str,
        level: LogLevel = LogLevel.INFO,
        json_output: bool = False,
        log_file: Optional[Path] = None,
    ):
        self._component = component
        self._logger = logging.getLogger(f"cas.{component}")
        self._logger.setLevel(getattr(logging, level.value))

        # Remove existing handlers
        self._logger.handlers.clear()

        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(getattr(logging, level.value))

        if json_output:
            console_handler.setFormatter(JSONFormatter())
        else:
            console_handler.setFormatter(HumanFormatter())

        self._logger.addHandler(console_handler)

        # File handler
        if log_file:
            file_handler = logging.FileHandler(log_file)
            file_handler.setLevel(logging.DEBUG)
            file_handler.setFormatter(JSONFormatter())
            self._logger.addHandler(file_handler)

    @property
    def component(self) -> str:
        return self._component

    def _log(
        self,
        level: int,
        message: str,
        context: Optional[LogContext] = None,
        duration_ms: Optional[float] = None,
        exc_info: bool = False,
        **kwargs,
    ) -> None:
        """Internal log method."""
        extra = {
            'component': self._component,
            'duration_ms': duration_ms,
            'extra': kwargs,
        }

        if context:
            extra['correlation_id'] = context.correlation_id
            extra['session_id'] = context.session_id
            extra['operation'] = context.operation
            extra['extra'].update(context.extra)

        self._logger.log(level, message, extra=extra, exc_info=exc_info)

    def debug(self, message: str, context: Optional[LogContext] = None, **kwargs) -> None:
        self._log(logging.DEBUG, message, context, **kwargs)

    def info(self, message: str, context: Optional[LogContext] = None, **kwargs) -> None:
        self._log(logging.INFO, message, context, **kwargs)

    def warning(self, message: str, context: Optional[LogContext] = None, **kwargs) -> None:
        self._log(logging.WARNING, message, context, **kwargs)

    def error(
        self,
        message: str,
        context: Optional[LogContext] = None,
        exc_info: bool = True,
        **kwargs,
    ) -> None:
        self._log(logging.ERROR, message, context, exc_info=exc_info, **kwargs)

    def critical(
        self,
        message: str,
        context: Optional[LogContext] = None,
        exc_info: bool = True,
        **kwargs,
    ) -> None:
        self._log(logging.CRITICAL, message, context, exc_info=exc_info, **kwargs)

    def timed_operation(
        self,
        operation: str,
        context: Optional[LogContext] = None,
    ) -> 'TimedOperation':
        """Context manager for timing operations."""
        return TimedOperation(self, operation, context)


class TimedOperation:
    """Context manager for timing operations."""

    def __init__(
        self,
        logger: CASLogger,
        operation: str,
        context: Optional[LogContext] = None,
    ):
        self._logger = logger
        self._operation = operation
        self._context = context.with_operation(operation) if context else LogContext(operation=operation)
        self._start_time: Optional[datetime] = None

    def __enter__(self):
        self._start_time = datetime.utcnow()
        self._logger.debug(f"Starting {self._operation}", self._context)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        duration_ms = (datetime.utcnow() - self._start_time).total_seconds() * 1000

        if exc_type:
            self._logger.error(
                f"Failed {self._operation}",
                self._context,
                duration_ms=duration_ms,
            )
        else:
            self._logger.info(
                f"Completed {self._operation}",
                self._context,
                duration_ms=duration_ms,
            )

        return False  # Don't suppress exceptions


def create_cas_logger(
    component: str,
    json_output: bool = False,
    log_dir: Optional[str] = None,
) -> CASLogger:
    """Factory function to create a configured logger."""
    log_file = None
    if log_dir:
        log_path = Path(log_dir)
        log_path.mkdir(parents=True, exist_ok=True)
        log_file = log_path / f"cas_{component}.log"

    return CASLogger(
        component=component,
        json_output=json_output,
        log_file=log_file,
    )
