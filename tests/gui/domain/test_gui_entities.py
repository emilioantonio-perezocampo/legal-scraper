"""
RED Phase Tests: GUI Domain Entities

These tests define the expected behavior for GUI domain entities.
They should FAIL initially until we implement the domain layer.
"""
import pytest
from datetime import date, datetime
from src.gui.domain.entities import (
    ScraperJob,
    JobStatus,
    JobProgress,
    ScraperConfiguration,
    DateRange,
    LogEntry,
    LogLevel,
)
from src.gui.domain.value_objects import (
    TargetSource,
    OutputFormat,
    ScraperMode,
)


class TestJobStatus:
    """Tests for JobStatus enumeration."""

    def test_job_status_has_required_states(self):
        """JobStatus must have all required workflow states."""
        assert hasattr(JobStatus, 'IDLE')
        assert hasattr(JobStatus, 'QUEUED')
        assert hasattr(JobStatus, 'RUNNING')
        assert hasattr(JobStatus, 'PAUSED')
        assert hasattr(JobStatus, 'COMPLETED')
        assert hasattr(JobStatus, 'FAILED')
        assert hasattr(JobStatus, 'CANCELLED')

    def test_job_status_is_serializable(self):
        """JobStatus values should be string-serializable for persistence."""
        assert JobStatus.RUNNING.value == "running"
        assert JobStatus.IDLE.value == "idle"


class TestLogLevel:
    """Tests for LogLevel enumeration."""

    def test_log_level_has_required_levels(self):
        """LogLevel must have standard logging levels."""
        assert hasattr(LogLevel, 'DEBUG')
        assert hasattr(LogLevel, 'INFO')
        assert hasattr(LogLevel, 'WARNING')
        assert hasattr(LogLevel, 'ERROR')
        assert hasattr(LogLevel, 'SUCCESS')


class TestLogEntry:
    """Tests for LogEntry value object."""

    def test_log_entry_creation(self):
        """LogEntry should capture timestamp, level, and message."""
        entry = LogEntry(
            level=LogLevel.INFO,
            message="Scraping started",
            source="DofScraperActor"
        )
        assert entry.level == LogLevel.INFO
        assert entry.message == "Scraping started"
        assert entry.source == "DofScraperActor"
        assert isinstance(entry.timestamp, datetime)

    def test_log_entry_is_immutable(self):
        """LogEntry should be a frozen dataclass (immutable)."""
        entry = LogEntry(
            level=LogLevel.INFO,
            message="Test",
            source="Test"
        )
        with pytest.raises((AttributeError, TypeError)):
            entry.message = "Modified"


class TestDateRange:
    """Tests for DateRange value object."""

    def test_date_range_creation(self):
        """DateRange should store start and end dates."""
        date_range = DateRange(
            start=date(2021, 1, 1),
            end=date(2021, 12, 31)
        )
        assert date_range.start == date(2021, 1, 1)
        assert date_range.end == date(2021, 12, 31)

    def test_date_range_validation_end_after_start(self):
        """DateRange should validate that end >= start."""
        with pytest.raises(ValueError, match="end date must be >= start date"):
            DateRange(
                start=date(2021, 12, 31),
                end=date(2021, 1, 1)
            )

    def test_date_range_days_count(self):
        """DateRange should calculate total days in range."""
        date_range = DateRange(
            start=date(2021, 3, 1),
            end=date(2021, 3, 5)
        )
        assert date_range.days_count == 5  # Inclusive of both start and end

    def test_date_range_is_immutable(self):
        """DateRange should be immutable."""
        date_range = DateRange(
            start=date(2021, 1, 1),
            end=date(2021, 1, 1)
        )
        with pytest.raises((AttributeError, TypeError)):
            date_range.start = date(2020, 1, 1)


class TestTargetSource:
    """Tests for TargetSource value object."""

    def test_target_source_dof(self):
        """Should have DOF (Diario Oficial de la Federación) source."""
        assert TargetSource.DOF.value == "dof"
        assert TargetSource.DOF.display_name == "Diario Oficial de la Federación"

    def test_target_source_has_base_url(self):
        """Each source should have a base URL."""
        assert TargetSource.DOF.base_url == "https://dof.gob.mx/"


class TestOutputFormat:
    """Tests for OutputFormat value object."""

    def test_output_format_json(self):
        """Should support JSON output format."""
        assert OutputFormat.JSON.value == "json"

    def test_output_format_has_extension(self):
        """Each format should have a file extension."""
        assert OutputFormat.JSON.extension == ".json"


class TestScraperMode:
    """Tests for ScraperMode value object."""

    def test_scraper_mode_values(self):
        """Should have required scraping modes."""
        assert hasattr(ScraperMode, 'TODAY')
        assert hasattr(ScraperMode, 'SINGLE_DATE')
        assert hasattr(ScraperMode, 'DATE_RANGE')
        assert hasattr(ScraperMode, 'HISTORICAL')


class TestJobProgress:
    """Tests for JobProgress entity."""

    def test_job_progress_creation(self):
        """JobProgress should track scraping progress."""
        progress = JobProgress(
            total_items=100,
            processed_items=0,
            successful_items=0,
            failed_items=0
        )
        assert progress.total_items == 100
        assert progress.processed_items == 0
        assert progress.percentage == 0.0

    def test_job_progress_percentage_calculation(self):
        """JobProgress should calculate percentage correctly."""
        progress = JobProgress(
            total_items=200,
            processed_items=50,
            successful_items=45,
            failed_items=5
        )
        assert progress.percentage == 25.0

    def test_job_progress_percentage_when_zero_total(self):
        """Percentage should be 0 when total_items is 0."""
        progress = JobProgress(
            total_items=0,
            processed_items=0,
            successful_items=0,
            failed_items=0
        )
        assert progress.percentage == 0.0

    def test_job_progress_increment(self):
        """JobProgress should support incrementing counters."""
        progress = JobProgress(
            total_items=10,
            processed_items=0,
            successful_items=0,
            failed_items=0
        )
        updated = progress.increment_success()
        assert updated.processed_items == 1
        assert updated.successful_items == 1
        assert progress.processed_items == 0  # Original unchanged (immutable)


class TestScraperConfiguration:
    """Tests for ScraperConfiguration entity."""

    def test_configuration_creation(self):
        """ScraperConfiguration should hold all scraper settings."""
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
        assert config.target_source == TargetSource.DOF
        assert config.mode == ScraperMode.DATE_RANGE
        assert config.date_range is not None
        assert config.output_directory == "scraped_data"
        assert config.rate_limit_seconds == 2.0

    def test_configuration_today_mode_no_date_range_required(self):
        """TODAY mode should not require date_range."""
        config = ScraperConfiguration(
            target_source=TargetSource.DOF,
            mode=ScraperMode.TODAY,
            output_format=OutputFormat.JSON,
            output_directory="scraped_data"
        )
        assert config.date_range is None
        assert config.mode == ScraperMode.TODAY

    def test_configuration_date_range_mode_requires_date_range(self):
        """DATE_RANGE mode should require date_range."""
        with pytest.raises(ValueError, match="date_range required"):
            ScraperConfiguration(
                target_source=TargetSource.DOF,
                mode=ScraperMode.DATE_RANGE,
                output_format=OutputFormat.JSON,
                output_directory="scraped_data"
            )


class TestScraperJob:
    """Tests for ScraperJob aggregate root."""

    def test_scraper_job_creation(self):
        """ScraperJob should be created with IDLE status."""
        config = ScraperConfiguration(
            target_source=TargetSource.DOF,
            mode=ScraperMode.TODAY,
            output_format=OutputFormat.JSON,
            output_directory="scraped_data"
        )
        job = ScraperJob(configuration=config)

        assert job.status == JobStatus.IDLE
        assert job.configuration == config
        assert job.progress is None
        assert job.id is not None  # Should auto-generate UUID

    def test_scraper_job_start(self):
        """Starting a job should transition status to RUNNING."""
        config = ScraperConfiguration(
            target_source=TargetSource.DOF,
            mode=ScraperMode.TODAY,
            output_format=OutputFormat.JSON,
            output_directory="scraped_data"
        )
        job = ScraperJob(configuration=config)
        started_job = job.start()

        assert started_job.status == JobStatus.RUNNING
        assert started_job.started_at is not None

    def test_scraper_job_cannot_start_if_already_running(self):
        """Starting a running job should raise error."""
        config = ScraperConfiguration(
            target_source=TargetSource.DOF,
            mode=ScraperMode.TODAY,
            output_format=OutputFormat.JSON,
            output_directory="scraped_data"
        )
        job = ScraperJob(configuration=config)
        running_job = job.start()

        with pytest.raises(ValueError, match="already running"):
            running_job.start()

    def test_scraper_job_pause(self):
        """Pausing a running job should transition to PAUSED."""
        config = ScraperConfiguration(
            target_source=TargetSource.DOF,
            mode=ScraperMode.TODAY,
            output_format=OutputFormat.JSON,
            output_directory="scraped_data"
        )
        job = ScraperJob(configuration=config)
        running_job = job.start()
        paused_job = running_job.pause()

        assert paused_job.status == JobStatus.PAUSED

    def test_scraper_job_resume(self):
        """Resuming a paused job should transition to RUNNING."""
        config = ScraperConfiguration(
            target_source=TargetSource.DOF,
            mode=ScraperMode.TODAY,
            output_format=OutputFormat.JSON,
            output_directory="scraped_data"
        )
        job = ScraperJob(configuration=config)
        paused_job = job.start().pause()
        resumed_job = paused_job.resume()

        assert resumed_job.status == JobStatus.RUNNING

    def test_scraper_job_complete(self):
        """Completing a job should set status and timestamp."""
        config = ScraperConfiguration(
            target_source=TargetSource.DOF,
            mode=ScraperMode.TODAY,
            output_format=OutputFormat.JSON,
            output_directory="scraped_data"
        )
        job = ScraperJob(configuration=config)
        running_job = job.start()
        completed_job = running_job.complete()

        assert completed_job.status == JobStatus.COMPLETED
        assert completed_job.completed_at is not None

    def test_scraper_job_fail(self):
        """Failing a job should set status and error message."""
        config = ScraperConfiguration(
            target_source=TargetSource.DOF,
            mode=ScraperMode.TODAY,
            output_format=OutputFormat.JSON,
            output_directory="scraped_data"
        )
        job = ScraperJob(configuration=config)
        running_job = job.start()
        failed_job = running_job.fail("Connection timeout")

        assert failed_job.status == JobStatus.FAILED
        assert failed_job.error_message == "Connection timeout"

    def test_scraper_job_cancel(self):
        """Cancelling a job should set status to CANCELLED."""
        config = ScraperConfiguration(
            target_source=TargetSource.DOF,
            mode=ScraperMode.TODAY,
            output_format=OutputFormat.JSON,
            output_directory="scraped_data"
        )
        job = ScraperJob(configuration=config)
        running_job = job.start()
        cancelled_job = running_job.cancel()

        assert cancelled_job.status == JobStatus.CANCELLED

    def test_scraper_job_log_entries(self):
        """ScraperJob should maintain a list of log entries."""
        config = ScraperConfiguration(
            target_source=TargetSource.DOF,
            mode=ScraperMode.TODAY,
            output_format=OutputFormat.JSON,
            output_directory="scraped_data"
        )
        job = ScraperJob(configuration=config)

        updated_job = job.add_log(LogLevel.INFO, "Initialized", "System")

        assert len(updated_job.logs) == 1
        assert updated_job.logs[0].message == "Initialized"

    def test_scraper_job_update_progress(self):
        """ScraperJob should support updating progress."""
        config = ScraperConfiguration(
            target_source=TargetSource.DOF,
            mode=ScraperMode.TODAY,
            output_format=OutputFormat.JSON,
            output_directory="scraped_data"
        )
        job = ScraperJob(configuration=config)
        running_job = job.start()

        progress = JobProgress(
            total_items=100,
            processed_items=10,
            successful_items=10,
            failed_items=0
        )
        updated_job = running_job.update_progress(progress)

        assert updated_job.progress == progress
        assert updated_job.progress.percentage == 10.0
