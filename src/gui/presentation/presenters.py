"""
GUI Presenters

Presenters handle the logic for updating views based on state changes.
They translate domain events and state into view updates.
"""
from typing import Optional, Any, Protocol
from abc import abstractmethod

from src.gui.domain.entities import (
    ScraperJob,
    JobProgress,
    LogEntry,
)
from .view_models import (
    JobViewModel,
    ProgressViewModel,
    LogEntryViewModel,
)


class MainViewProtocol(Protocol):
    """Protocol defining what methods the main view must implement."""

    @abstractmethod
    def update_job_status(self, job_vm: JobViewModel) -> None:
        """Update the job status display."""
        ...

    @abstractmethod
    def update_progress(self, progress_vm: ProgressViewModel) -> None:
        """Update the progress display."""
        ...

    @abstractmethod
    def add_log_entry(self, log_vm: LogEntryViewModel) -> None:
        """Add a log entry to the log display."""
        ...

    @abstractmethod
    def clear_logs(self) -> None:
        """Clear all log entries."""
        ...

    @abstractmethod
    def set_connection_status(self, connected: bool) -> None:
        """Update connection status indicator."""
        ...


class JobListViewProtocol(Protocol):
    """Protocol defining what methods the job list view must implement."""

    @abstractmethod
    def add_job_to_history(self, job_vm: JobViewModel) -> None:
        """Add a completed job to the history list."""
        ...

    @abstractmethod
    def clear_history(self) -> None:
        """Clear the job history."""
        ...


class MainPresenter:
    """
    Presenter for the main application view.

    Handles updates to job status, progress, and logs.
    """

    def __init__(self, view: Any):
        """Initialize with a view that implements MainViewProtocol."""
        self._view = view
        self._current_job_vm: Optional[JobViewModel] = None

    @property
    def current_job_vm(self) -> Optional[JobViewModel]:
        """Get the current job view model."""
        return self._current_job_vm

    def on_job_updated(self, job: ScraperJob) -> None:
        """Handle job state changes."""
        self._current_job_vm = JobViewModel.from_domain(job)
        if hasattr(self._view, 'update_job_status'):
            self._view.update_job_status(self._current_job_vm)

    def on_progress_updated(self, progress: JobProgress) -> None:
        """Handle progress updates."""
        progress_vm = ProgressViewModel.from_domain(progress)
        if hasattr(self._view, 'update_progress'):
            self._view.update_progress(progress_vm)

    def on_log_added(self, entry: LogEntry) -> None:
        """Handle new log entries."""
        log_vm = LogEntryViewModel.from_domain(entry)
        if hasattr(self._view, 'add_log_entry'):
            self._view.add_log_entry(log_vm)

    def on_connection_changed(self, connected: bool) -> None:
        """Handle connection status changes."""
        if hasattr(self._view, 'set_connection_status'):
            self._view.set_connection_status(connected)

    def clear_job(self) -> None:
        """Clear current job display."""
        self._current_job_vm = None

    def clear_logs(self) -> None:
        """Clear all logs from view."""
        if hasattr(self._view, 'clear_logs'):
            self._view.clear_logs()


class JobListPresenter:
    """
    Presenter for the job history list.

    Handles updates to the list of completed jobs.
    """

    def __init__(self, view: Any):
        """Initialize with a view that implements JobListViewProtocol."""
        self._view = view
        self._jobs: list = []

    def add_completed_job(self, job: ScraperJob) -> None:
        """Add a completed job to the history."""
        job_vm = JobViewModel.from_domain(job)
        self._jobs.append(job_vm)
        if hasattr(self._view, 'add_job_to_history'):
            self._view.add_job_to_history(job_vm)

    def format_job_for_list(self, job: ScraperJob) -> str:
        """Format a job for display in a list."""
        job_vm = JobViewModel.from_domain(job)
        return (
            f"{job_vm.source_display} - {job_vm.status_display} "
            f"({job_vm.started_at_display or 'Not started'})"
        )

    def clear_history(self) -> None:
        """Clear the job history."""
        self._jobs.clear()
        if hasattr(self._view, 'clear_history'):
            self._view.clear_history()

    @property
    def job_count(self) -> int:
        """Get the number of jobs in history."""
        return len(self._jobs)
