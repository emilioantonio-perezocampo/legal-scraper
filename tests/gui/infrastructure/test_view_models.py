"""
RED Phase Tests: GUI View Models and Presenters

Tests for the presentation layer that bridges domain models to UI.
These tests should run without requiring actual Tkinter windows.
"""
import pytest
from datetime import date, datetime
from unittest.mock import MagicMock, AsyncMock

from src.gui.domain.entities import (
    ScraperJob,
    JobStatus,
    JobProgress,
    ScraperConfiguration,
    DateRange,
    LogLevel,
    LogEntry,
)
from src.gui.domain.value_objects import (
    TargetSource,
    OutputFormat,
    ScraperMode,
)
from src.gui.presentation.view_models import (
    JobViewModel,
    ProgressViewModel,
    LogEntryViewModel,
    ConfigurationViewModel,
)
from src.gui.presentation.presenters import (
    MainPresenter,
    JobListPresenter,
)


class TestJobViewModel:
    """Tests for JobViewModel - UI representation of ScraperJob."""

    def test_view_model_from_domain_entity(self):
        """ViewModel should convert domain entity to UI-friendly format."""
        config = ScraperConfiguration(
            target_source=TargetSource.DOF,
            mode=ScraperMode.TODAY,
            output_format=OutputFormat.JSON,
            output_directory="scraped_data"
        )
        job = ScraperJob(configuration=config)
        running_job = job.start()

        view_model = JobViewModel.from_domain(running_job)

        assert view_model.job_id == running_job.id
        assert view_model.status_display == "Running"
        assert view_model.status_color == "green"
        assert view_model.source_display == "Diario Oficial de la Federación"
        assert view_model.can_pause is True
        assert view_model.can_resume is False
        assert view_model.can_cancel is True

    def test_view_model_status_colors(self):
        """ViewModel should provide appropriate colors for each status."""
        config = ScraperConfiguration(
            target_source=TargetSource.DOF,
            mode=ScraperMode.TODAY,
            output_format=OutputFormat.JSON,
            output_directory="scraped_data"
        )

        # Test different statuses
        idle_job = ScraperJob(configuration=config)
        assert JobViewModel.from_domain(idle_job).status_color == "gray"

        running_job = idle_job.start()
        assert JobViewModel.from_domain(running_job).status_color == "green"

        paused_job = running_job.pause()
        assert JobViewModel.from_domain(paused_job).status_color == "orange"

        failed_job = running_job.fail("Error")
        assert JobViewModel.from_domain(failed_job).status_color == "red"

        completed_job = running_job.complete()
        assert JobViewModel.from_domain(completed_job).status_color == "blue"

    def test_view_model_action_availability(self):
        """ViewModel should correctly determine available actions."""
        config = ScraperConfiguration(
            target_source=TargetSource.DOF,
            mode=ScraperMode.TODAY,
            output_format=OutputFormat.JSON,
            output_directory="scraped_data"
        )

        # Idle job - can only start (via controller, not direct action)
        idle_vm = JobViewModel.from_domain(ScraperJob(configuration=config))
        assert idle_vm.can_pause is False
        assert idle_vm.can_resume is False
        assert idle_vm.can_cancel is False

        # Running job - can pause or cancel
        running_vm = JobViewModel.from_domain(ScraperJob(configuration=config).start())
        assert running_vm.can_pause is True
        assert running_vm.can_resume is False
        assert running_vm.can_cancel is True

        # Paused job - can resume or cancel
        paused_vm = JobViewModel.from_domain(ScraperJob(configuration=config).start().pause())
        assert paused_vm.can_pause is False
        assert paused_vm.can_resume is True
        assert paused_vm.can_cancel is True


class TestProgressViewModel:
    """Tests for ProgressViewModel - UI representation of JobProgress."""

    def test_progress_view_model_creation(self):
        """ProgressViewModel should format progress for display."""
        progress = JobProgress(
            total_items=100,
            processed_items=45,
            successful_items=42,
            failed_items=3
        )

        view_model = ProgressViewModel.from_domain(progress)

        assert view_model.percentage == 45.0
        assert view_model.percentage_display == "45%"
        assert view_model.items_display == "45/100"
        assert view_model.success_count == 42
        assert view_model.failure_count == 3

    def test_progress_view_model_zero_handling(self):
        """ProgressViewModel should handle zero values gracefully."""
        progress = JobProgress(
            total_items=0,
            processed_items=0,
            successful_items=0,
            failed_items=0
        )

        view_model = ProgressViewModel.from_domain(progress)

        assert view_model.percentage == 0.0
        assert view_model.percentage_display == "0%"
        assert view_model.items_display == "0/0"

    def test_progress_view_model_none_handling(self):
        """ProgressViewModel should handle None progress."""
        view_model = ProgressViewModel.from_domain(None)

        assert view_model.percentage == 0.0
        assert view_model.percentage_display == "0%"
        assert view_model.items_display == "0/0"


class TestLogEntryViewModel:
    """Tests for LogEntryViewModel - UI representation of LogEntry."""

    def test_log_entry_view_model(self):
        """LogEntryViewModel should format log entries for display."""
        entry = LogEntry(
            level=LogLevel.INFO,
            message="Scraping started",
            source="DofScraperActor"
        )

        view_model = LogEntryViewModel.from_domain(entry)

        assert view_model.level_display == "INFO"
        assert view_model.level_color == "blue"
        assert view_model.message == "Scraping started"
        assert view_model.source == "DofScraperActor"
        assert "[" in view_model.timestamp_display  # Has time format

    def test_log_entry_colors(self):
        """LogEntryViewModel should provide appropriate colors for levels."""
        levels_colors = {
            LogLevel.DEBUG: "gray",
            LogLevel.INFO: "blue",
            LogLevel.WARNING: "orange",
            LogLevel.ERROR: "red",
            LogLevel.SUCCESS: "green",
        }

        for level, expected_color in levels_colors.items():
            entry = LogEntry(level=level, message="Test", source="Test")
            vm = LogEntryViewModel.from_domain(entry)
            assert vm.level_color == expected_color, f"Expected {expected_color} for {level}"


class TestConfigurationViewModel:
    """Tests for ConfigurationViewModel - UI representation of ScraperConfiguration."""

    def test_configuration_view_model(self):
        """ConfigurationViewModel should format configuration for display."""
        config = ScraperConfiguration(
            target_source=TargetSource.DOF,
            mode=ScraperMode.DATE_RANGE,
            date_range=DateRange(
                start=date(2021, 3, 1),
                end=date(2021, 3, 5)
            ),
            output_format=OutputFormat.JSON,
            output_directory="scraped_data",
            rate_limit_seconds=2.0
        )

        view_model = ConfigurationViewModel.from_domain(config)

        assert view_model.source_display == "Diario Oficial de la Federación"
        assert view_model.mode_display == "Date Range"
        assert "2021-03-01" in view_model.date_range_display
        assert "2021-03-05" in view_model.date_range_display
        assert view_model.output_format_display == "JSON"

    def test_configuration_view_model_today_mode(self):
        """ConfigurationViewModel should handle TODAY mode."""
        config = ScraperConfiguration(
            target_source=TargetSource.DOF,
            mode=ScraperMode.TODAY,
            output_format=OutputFormat.JSON,
            output_directory="scraped_data"
        )

        view_model = ConfigurationViewModel.from_domain(config)

        assert view_model.mode_display == "Today"
        assert "today" in view_model.date_range_display.lower() or view_model.date_range_display == ""


class TestMainPresenter:
    """Tests for MainPresenter - orchestrates view updates."""

    def test_presenter_initialization(self):
        """MainPresenter should initialize with no job."""
        mock_view = MagicMock()
        presenter = MainPresenter(view=mock_view)

        assert presenter.current_job_vm is None

    def test_presenter_updates_view_on_job_change(self):
        """MainPresenter should update view when job changes."""
        mock_view = MagicMock()
        presenter = MainPresenter(view=mock_view)

        config = ScraperConfiguration(
            target_source=TargetSource.DOF,
            mode=ScraperMode.TODAY,
            output_format=OutputFormat.JSON,
            output_directory="scraped_data"
        )
        job = ScraperJob(configuration=config).start()

        presenter.on_job_updated(job)

        mock_view.update_job_status.assert_called()

    def test_presenter_updates_progress(self):
        """MainPresenter should update progress display."""
        mock_view = MagicMock()
        presenter = MainPresenter(view=mock_view)

        progress = JobProgress(
            total_items=100,
            processed_items=50,
            successful_items=48,
            failed_items=2
        )

        presenter.on_progress_updated(progress)

        mock_view.update_progress.assert_called()

    def test_presenter_adds_log_entries(self):
        """MainPresenter should add log entries to view."""
        mock_view = MagicMock()
        presenter = MainPresenter(view=mock_view)

        entry = LogEntry(
            level=LogLevel.INFO,
            message="Test log",
            source="Test"
        )

        presenter.on_log_added(entry)

        mock_view.add_log_entry.assert_called()


class TestJobListPresenter:
    """Tests for JobListPresenter - manages job history display."""

    def test_job_list_presenter_add_job(self):
        """JobListPresenter should add jobs to history."""
        mock_view = MagicMock()
        presenter = JobListPresenter(view=mock_view)

        config = ScraperConfiguration(
            target_source=TargetSource.DOF,
            mode=ScraperMode.TODAY,
            output_format=OutputFormat.JSON,
            output_directory="scraped_data"
        )
        job = ScraperJob(configuration=config).start().complete()

        presenter.add_completed_job(job)

        mock_view.add_job_to_history.assert_called()

    def test_job_list_presenter_formats_jobs(self):
        """JobListPresenter should format jobs for display."""
        mock_view = MagicMock()
        presenter = JobListPresenter(view=mock_view)

        config = ScraperConfiguration(
            target_source=TargetSource.DOF,
            mode=ScraperMode.TODAY,
            output_format=OutputFormat.JSON,
            output_directory="scraped_data"
        )
        job = ScraperJob(configuration=config).start().complete()

        formatted = presenter.format_job_for_list(job)

        assert formatted is not None
        assert "DOF" in formatted or "Diario" in formatted
        assert "Completed" in formatted
