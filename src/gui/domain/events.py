"""
GUI Domain Events

Domain events represent significant occurrences in the domain.
They are used for communication between actors and layers.
"""
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from .entities import JobProgress, ScraperConfiguration, LogLevel


@dataclass(frozen=True)
class DomainEvent:
    """Base class for all domain events."""
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass(frozen=True)
class JobStartedEvent(DomainEvent):
    """Event fired when a scraping job starts."""
    job_id: str = ""
    configuration: Optional[ScraperConfiguration] = None


@dataclass(frozen=True)
class JobProgressEvent(DomainEvent):
    """Event fired when job progress is updated."""
    job_id: str = ""
    progress: Optional[JobProgress] = None


@dataclass(frozen=True)
class JobCompletedEvent(DomainEvent):
    """Event fired when a job completes successfully."""
    job_id: str = ""
    total_processed: int = 0
    successful: int = 0
    failed: int = 0


@dataclass(frozen=True)
class JobFailedEvent(DomainEvent):
    """Event fired when a job fails."""
    job_id: str = ""
    error_message: str = ""


@dataclass(frozen=True)
class JobPausedEvent(DomainEvent):
    """Event fired when a job is paused."""
    job_id: str = ""


@dataclass(frozen=True)
class JobResumedEvent(DomainEvent):
    """Event fired when a job is resumed."""
    job_id: str = ""


@dataclass(frozen=True)
class JobCancelledEvent(DomainEvent):
    """Event fired when a job is cancelled."""
    job_id: str = ""


@dataclass(frozen=True)
class LogAddedEvent(DomainEvent):
    """Event fired when a log entry is added."""
    job_id: str = ""
    level: LogLevel = LogLevel.INFO
    message: str = ""
    source: str = ""


@dataclass(frozen=True)
class ConnectionStatusEvent(DomainEvent):
    """Event fired when scraper connection status changes."""
    is_connected: bool = False
    error: Optional[str] = None
