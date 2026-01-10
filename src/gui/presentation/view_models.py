"""
GUI View Models

View models translate domain entities into UI-friendly representations.
They contain display logic but no business logic.
"""
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from src.gui.domain.entities import (
    ScraperJob,
    JobStatus,
    JobProgress,
    ScraperConfiguration,
    LogLevel,
    LogEntry,
)


# Color mappings for statuses
STATUS_COLORS = {
    JobStatus.IDLE: "gray",
    JobStatus.QUEUED: "yellow",
    JobStatus.RUNNING: "green",
    JobStatus.PAUSED: "orange",
    JobStatus.COMPLETED: "blue",
    JobStatus.FAILED: "red",
    JobStatus.CANCELLED: "gray",
}

STATUS_DISPLAY = {
    JobStatus.IDLE: "Idle",
    JobStatus.QUEUED: "Queued",
    JobStatus.RUNNING: "Running",
    JobStatus.PAUSED: "Paused",
    JobStatus.COMPLETED: "Completed",
    JobStatus.FAILED: "Failed",
    JobStatus.CANCELLED: "Cancelled",
}

LOG_LEVEL_COLORS = {
    LogLevel.DEBUG: "gray",
    LogLevel.INFO: "blue",
    LogLevel.WARNING: "orange",
    LogLevel.ERROR: "red",
    LogLevel.SUCCESS: "green",
}

LOG_LEVEL_DISPLAY = {
    LogLevel.DEBUG: "DEBUG",
    LogLevel.INFO: "INFO",
    LogLevel.WARNING: "WARNING",
    LogLevel.ERROR: "ERROR",
    LogLevel.SUCCESS: "SUCCESS",
}

MODE_DISPLAY = {
    "today": "Today",
    "single": "Single Date",
    "range": "Date Range",
    "historical": "Historical",
}


@dataclass(frozen=True)
class JobViewModel:
    """View model for ScraperJob display."""
    job_id: str
    status: JobStatus
    status_display: str
    status_color: str
    source_display: str
    mode_display: str
    can_pause: bool
    can_resume: bool
    can_cancel: bool
    started_at_display: str
    completed_at_display: str
    error_message: Optional[str]

    @classmethod
    def from_domain(cls, job: ScraperJob) -> 'JobViewModel':
        """Create view model from domain entity."""
        return cls(
            job_id=job.id,
            status=job.status,
            status_display=STATUS_DISPLAY.get(job.status, str(job.status)),
            status_color=STATUS_COLORS.get(job.status, "gray"),
            source_display=job.configuration.target_source.display_name,
            mode_display=MODE_DISPLAY.get(job.configuration.mode.value, job.configuration.mode.value),
            can_pause=job.status == JobStatus.RUNNING,
            can_resume=job.status == JobStatus.PAUSED,
            can_cancel=job.status in (JobStatus.RUNNING, JobStatus.PAUSED, JobStatus.QUEUED),
            started_at_display=cls._format_datetime(job.started_at),
            completed_at_display=cls._format_datetime(job.completed_at),
            error_message=job.error_message
        )

    @staticmethod
    def _format_datetime(dt: Optional[datetime]) -> str:
        """Format datetime for display."""
        if dt is None:
            return ""
        return dt.strftime("%Y-%m-%d %H:%M:%S")


@dataclass(frozen=True)
class ProgressViewModel:
    """View model for JobProgress display."""
    percentage: float
    percentage_display: str
    items_display: str
    success_count: int
    failure_count: int
    total_items: int
    processed_items: int

    @classmethod
    def from_domain(cls, progress: Optional[JobProgress]) -> 'ProgressViewModel':
        """Create view model from domain entity."""
        if progress is None:
            return cls(
                percentage=0.0,
                percentage_display="0%",
                items_display="0/0",
                success_count=0,
                failure_count=0,
                total_items=0,
                processed_items=0
            )

        return cls(
            percentage=progress.percentage,
            percentage_display=f"{int(progress.percentage)}%",
            items_display=f"{progress.processed_items}/{progress.total_items}",
            success_count=progress.successful_items,
            failure_count=progress.failed_items,
            total_items=progress.total_items,
            processed_items=progress.processed_items
        )


@dataclass(frozen=True)
class LogEntryViewModel:
    """View model for LogEntry display."""
    level: LogLevel
    level_display: str
    level_color: str
    message: str
    source: str
    timestamp_display: str

    @classmethod
    def from_domain(cls, entry: LogEntry) -> 'LogEntryViewModel':
        """Create view model from domain entity."""
        return cls(
            level=entry.level,
            level_display=LOG_LEVEL_DISPLAY.get(entry.level, str(entry.level)),
            level_color=LOG_LEVEL_COLORS.get(entry.level, "gray"),
            message=entry.message,
            source=entry.source,
            timestamp_display=f"[{entry.timestamp.strftime('%H:%M:%S')}]"
        )


@dataclass(frozen=True)
class ConfigurationViewModel:
    """View model for ScraperConfiguration display."""
    source_display: str
    mode_display: str
    date_range_display: str
    output_format_display: str
    output_directory: str
    rate_limit_display: str

    @classmethod
    def from_domain(cls, config: ScraperConfiguration) -> 'ConfigurationViewModel':
        """Create view model from domain entity."""
        # Build date range display
        if config.date_range:
            date_range_display = (
                f"{config.date_range.start.isoformat()} to "
                f"{config.date_range.end.isoformat()}"
            )
        elif config.single_date:
            date_range_display = config.single_date.isoformat()
        elif config.mode.value == "today":
            date_range_display = ""
        else:
            date_range_display = ""

        return cls(
            source_display=config.target_source.display_name,
            mode_display=MODE_DISPLAY.get(config.mode.value, config.mode.value),
            date_range_display=date_range_display,
            output_format_display=config.output_format.value.upper(),
            output_directory=config.output_directory,
            rate_limit_display=f"{config.rate_limit_seconds}s"
        )
