"""
RED Phase Tests: GUI Application Use Cases

Tests for the application layer that orchestrates domain logic.
"""
import pytest
from datetime import date
from unittest.mock import MagicMock, AsyncMock, patch

from src.gui.domain.entities import (
    ScraperJob,
    JobStatus,
    JobProgress,
    ScraperConfiguration,
    DateRange,
    LogLevel,
)
from src.gui.domain.value_objects import (
    TargetSource,
    OutputFormat,
    ScraperMode,
)
from src.gui.application.use_cases import (
    StartScrapingUseCase,
    PauseScrapingUseCase,
    ResumeScrapingUseCase,
    CancelScrapingUseCase,
    GetJobStatusUseCase,
    CreateConfigurationUseCase,
)
from src.gui.application.services import (
    ScraperService,
    ConfigurationService,
)


class TestStartScrapingUseCase:
    """Tests for starting a scraping job."""

    @pytest.mark.asyncio
    async def test_start_scraping_success(self):
        """Should successfully start a scraping job."""
        mock_controller = AsyncMock()
        mock_controller.ask.return_value = {"success": True, "data": {"job_id": "test-123"}}

        use_case = StartScrapingUseCase(controller=mock_controller)

        config = ScraperConfiguration(
            target_source=TargetSource.DOF,
            mode=ScraperMode.TODAY,
            output_format=OutputFormat.JSON,
            output_directory="scraped_data"
        )

        result = await use_case.execute(config)

        assert result.success is True
        assert result.job_id == "test-123"
        mock_controller.ask.assert_called_once()

    @pytest.mark.asyncio
    async def test_start_scraping_failure(self):
        """Should handle failure to start scraping."""
        mock_controller = AsyncMock()
        mock_controller.ask.return_value = {
            "success": False,
            "error": "A job is already running"
        }

        use_case = StartScrapingUseCase(controller=mock_controller)

        config = ScraperConfiguration(
            target_source=TargetSource.DOF,
            mode=ScraperMode.TODAY,
            output_format=OutputFormat.JSON,
            output_directory="scraped_data"
        )

        result = await use_case.execute(config)

        assert result.success is False
        assert "already running" in result.error


class TestPauseScrapingUseCase:
    """Tests for pausing a scraping job."""

    @pytest.mark.asyncio
    async def test_pause_scraping_success(self):
        """Should successfully pause a running job."""
        mock_controller = AsyncMock()
        mock_controller.ask.return_value = {"success": True}

        use_case = PauseScrapingUseCase(controller=mock_controller)

        result = await use_case.execute()

        assert result.success is True
        mock_controller.ask.assert_called_with("PAUSE_JOB")

    @pytest.mark.asyncio
    async def test_pause_scraping_failure_not_running(self):
        """Should fail if no job is running."""
        mock_controller = AsyncMock()
        mock_controller.ask.return_value = {
            "success": False,
            "error": "No job to pause"
        }

        use_case = PauseScrapingUseCase(controller=mock_controller)

        result = await use_case.execute()

        assert result.success is False


class TestResumeScrapingUseCase:
    """Tests for resuming a paused job."""

    @pytest.mark.asyncio
    async def test_resume_scraping_success(self):
        """Should successfully resume a paused job."""
        mock_controller = AsyncMock()
        mock_controller.ask.return_value = {"success": True}

        use_case = ResumeScrapingUseCase(controller=mock_controller)

        result = await use_case.execute()

        assert result.success is True
        mock_controller.ask.assert_called_with("RESUME_JOB")


class TestCancelScrapingUseCase:
    """Tests for cancelling a job."""

    @pytest.mark.asyncio
    async def test_cancel_scraping_success(self):
        """Should successfully cancel a job."""
        mock_controller = AsyncMock()
        mock_controller.ask.return_value = {"success": True}

        use_case = CancelScrapingUseCase(controller=mock_controller)

        result = await use_case.execute()

        assert result.success is True
        mock_controller.ask.assert_called_with("CANCEL_JOB")


class TestGetJobStatusUseCase:
    """Tests for getting job status."""

    @pytest.mark.asyncio
    async def test_get_status_with_running_job(self):
        """Should return status of running job."""
        mock_controller = AsyncMock()
        mock_controller.ask.return_value = {
            "success": True,
            "data": {
                "status": "running",
                "job_id": "test-123",
                "progress": None
            }
        }

        use_case = GetJobStatusUseCase(controller=mock_controller)

        result = await use_case.execute()

        assert result.success is True
        assert result.status == "running"
        assert result.job_id == "test-123"

    @pytest.mark.asyncio
    async def test_get_status_idle(self):
        """Should return idle status when no job."""
        mock_controller = AsyncMock()
        mock_controller.ask.return_value = {
            "success": True,
            "data": {
                "status": "idle",
                "job": None
            }
        }

        use_case = GetJobStatusUseCase(controller=mock_controller)

        result = await use_case.execute()

        assert result.success is True
        assert result.status == "idle"


class TestCreateConfigurationUseCase:
    """Tests for creating scraper configuration."""

    def test_create_today_configuration(self):
        """Should create configuration for TODAY mode."""
        use_case = CreateConfigurationUseCase()

        config = use_case.execute(
            source=TargetSource.DOF,
            mode=ScraperMode.TODAY,
            output_directory="scraped_data"
        )

        assert config.target_source == TargetSource.DOF
        assert config.mode == ScraperMode.TODAY
        assert config.output_directory == "scraped_data"

    def test_create_date_range_configuration(self):
        """Should create configuration for DATE_RANGE mode."""
        use_case = CreateConfigurationUseCase()

        config = use_case.execute(
            source=TargetSource.DOF,
            mode=ScraperMode.DATE_RANGE,
            output_directory="scraped_data",
            start_date=date(2021, 3, 1),
            end_date=date(2021, 3, 5)
        )

        assert config.mode == ScraperMode.DATE_RANGE
        assert config.date_range is not None
        assert config.date_range.start == date(2021, 3, 1)
        assert config.date_range.end == date(2021, 3, 5)

    def test_create_configuration_validates_date_range(self):
        """Should validate date range for DATE_RANGE mode."""
        use_case = CreateConfigurationUseCase()

        with pytest.raises(ValueError, match="start_date and end_date required"):
            use_case.execute(
                source=TargetSource.DOF,
                mode=ScraperMode.DATE_RANGE,
                output_directory="scraped_data"
            )


class TestScraperService:
    """Tests for the ScraperService that coordinates scraping operations."""

    @pytest.mark.asyncio
    async def test_service_initializes_actors(self):
        """ScraperService should initialize required actors."""
        service = ScraperService()

        assert service.state_actor is not None
        assert service.controller_actor is not None
        assert service.bridge_actor is not None

    @pytest.mark.asyncio
    async def test_service_start_stop_lifecycle(self):
        """ScraperService should properly start and stop actors."""
        service = ScraperService()

        await service.start()
        assert service.is_running is True

        await service.stop()
        assert service.is_running is False

    @pytest.mark.asyncio
    async def test_service_start_scraping(self):
        """ScraperService should delegate to use cases."""
        service = ScraperService()
        await service.start()

        config = ScraperConfiguration(
            target_source=TargetSource.DOF,
            mode=ScraperMode.TODAY,
            output_format=OutputFormat.JSON,
            output_directory="scraped_data"
        )

        result = await service.start_scraping(config)

        assert result is not None

        await service.stop()


class TestConfigurationService:
    """Tests for the ConfigurationService."""

    def test_get_available_sources(self):
        """Should return list of available sources."""
        service = ConfigurationService()

        sources = service.get_available_sources()

        assert len(sources) > 0
        assert TargetSource.DOF in sources

    def test_get_available_modes(self):
        """Should return list of available modes."""
        service = ConfigurationService()

        modes = service.get_available_modes()

        assert ScraperMode.TODAY in modes
        assert ScraperMode.DATE_RANGE in modes

    def test_get_default_configuration(self):
        """Should return a default configuration."""
        service = ConfigurationService()

        config = service.get_default_configuration()

        assert config.target_source == TargetSource.DOF
        assert config.mode == ScraperMode.TODAY
        assert config.output_format == OutputFormat.JSON

    def test_validate_output_directory(self):
        """Should validate output directory."""
        service = ConfigurationService()

        # Valid directory
        assert service.validate_output_directory("scraped_data") is True

        # Empty directory should be invalid
        assert service.validate_output_directory("") is False
