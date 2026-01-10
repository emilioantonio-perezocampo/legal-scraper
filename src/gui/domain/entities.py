"""
GUI Domain Entities

Entities are objects with identity that persists over time.
They contain business logic and maintain invariants.
"""
from dataclasses import dataclass, field, replace
from datetime import date, datetime
from enum import Enum
from typing import List, Optional
from uuid import uuid4

from .value_objects import TargetSource, OutputFormat, ScraperMode


class JobStatus(Enum):
    """
    Represents the lifecycle states of a scraper job.
    """
    IDLE = "idle"
    QUEUED = "queued"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class LogLevel(Enum):
    """
    Log severity levels for job logging.
    """
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    SUCCESS = "success"


@dataclass(frozen=True)
class LogEntry:
    """
    Immutable log entry captured during job execution.

    Value Object: Identity is defined by all its attributes.
    """
    level: LogLevel
    message: str
    source: str
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass(frozen=True)
class DateRange:
    """
    Represents a range of dates for scraping.

    Value Object: Immutable, defined by start and end dates.
    Validates that end >= start.
    """
    start: date
    end: date

    def __post_init__(self):
        if self.end < self.start:
            raise ValueError("end date must be >= start date")

    @property
    def days_count(self) -> int:
        """Returns the number of days in the range (inclusive)."""
        return (self.end - self.start).days + 1


@dataclass(frozen=True)
class JobProgress:
    """
    Tracks progress of a scraping job.

    Immutable - create new instances for updates.
    """
    total_items: int
    processed_items: int
    successful_items: int
    failed_items: int

    @property
    def percentage(self) -> float:
        """Calculate completion percentage."""
        if self.total_items == 0:
            return 0.0
        return (self.processed_items / self.total_items) * 100

    def increment_success(self) -> 'JobProgress':
        """Return new progress with success incremented."""
        return JobProgress(
            total_items=self.total_items,
            processed_items=self.processed_items + 1,
            successful_items=self.successful_items + 1,
            failed_items=self.failed_items
        )

    def increment_failure(self) -> 'JobProgress':
        """Return new progress with failure incremented."""
        return JobProgress(
            total_items=self.total_items,
            processed_items=self.processed_items + 1,
            successful_items=self.successful_items,
            failed_items=self.failed_items + 1
        )


@dataclass(frozen=True)
class ScraperConfiguration:
    """
    Configuration settings for a scraping job.

    Immutable value object containing all settings needed to run a scrape.
    """
    target_source: TargetSource
    mode: ScraperMode
    output_format: OutputFormat
    output_directory: str
    date_range: Optional[DateRange] = None
    rate_limit_seconds: float = 2.0
    single_date: Optional[date] = None

    def __post_init__(self):
        # Validate that date_range is provided for DATE_RANGE mode
        if self.mode == ScraperMode.DATE_RANGE and self.date_range is None:
            raise ValueError("date_range required for DATE_RANGE mode")
        if self.mode == ScraperMode.SINGLE_DATE and self.single_date is None and self.date_range is None:
            raise ValueError("single_date or date_range required for SINGLE_DATE mode")


@dataclass(frozen=True)
class ScraperJob:
    """
    Aggregate Root: Represents a complete scraping job.

    Contains all state and behavior for managing a scraping job lifecycle.
    Immutable - state transitions return new instances.
    """
    configuration: ScraperConfiguration
    id: str = field(default_factory=lambda: str(uuid4()))
    status: JobStatus = JobStatus.IDLE
    progress: Optional[JobProgress] = None
    logs: tuple = field(default_factory=tuple)  # Tuple for immutability
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None

    def start(self) -> 'ScraperJob':
        """Start the job, transitioning from IDLE to RUNNING."""
        if self.status == JobStatus.RUNNING:
            raise ValueError("Job is already running")
        if self.status in (JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED):
            raise ValueError(f"Cannot start a job with status {self.status}")

        return replace(
            self,
            status=JobStatus.RUNNING,
            started_at=datetime.now()
        )

    def pause(self) -> 'ScraperJob':
        """Pause a running job."""
        if self.status != JobStatus.RUNNING:
            raise ValueError("Can only pause a running job")

        return replace(self, status=JobStatus.PAUSED)

    def resume(self) -> 'ScraperJob':
        """Resume a paused job."""
        if self.status != JobStatus.PAUSED:
            raise ValueError("Can only resume a paused job")

        return replace(self, status=JobStatus.RUNNING)

    def complete(self) -> 'ScraperJob':
        """Mark job as completed."""
        if self.status not in (JobStatus.RUNNING, JobStatus.PAUSED):
            raise ValueError("Can only complete a running or paused job")

        return replace(
            self,
            status=JobStatus.COMPLETED,
            completed_at=datetime.now()
        )

    def fail(self, error_message: str) -> 'ScraperJob':
        """Mark job as failed with error message."""
        return replace(
            self,
            status=JobStatus.FAILED,
            error_message=error_message,
            completed_at=datetime.now()
        )

    def cancel(self) -> 'ScraperJob':
        """Cancel the job."""
        if self.status in (JobStatus.COMPLETED, JobStatus.FAILED):
            raise ValueError(f"Cannot cancel a job with status {self.status}")

        return replace(
            self,
            status=JobStatus.CANCELLED,
            completed_at=datetime.now()
        )

    def add_log(self, level: LogLevel, message: str, source: str) -> 'ScraperJob':
        """Add a log entry to the job."""
        entry = LogEntry(level=level, message=message, source=source)
        return replace(self, logs=self.logs + (entry,))

    def update_progress(self, progress: JobProgress) -> 'ScraperJob':
        """Update job progress."""
        return replace(self, progress=progress)
