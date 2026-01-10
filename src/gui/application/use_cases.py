"""
GUI Application Use Cases

Use cases encapsulate application-specific business rules.
They orchestrate the flow of data to and from entities,
and direct those entities to use their domain logic.
"""
from dataclasses import dataclass
from datetime import date
from typing import Optional, Any

from src.gui.domain.entities import (
    ScraperConfiguration,
    DateRange,
)
from src.gui.domain.value_objects import (
    TargetSource,
    OutputFormat,
    ScraperMode,
)


@dataclass
class UseCaseResult:
    """Base result class for use cases."""
    success: bool
    error: Optional[str] = None


@dataclass
class StartScrapingResult(UseCaseResult):
    """Result of starting a scraping job."""
    job_id: Optional[str] = None


@dataclass
class JobStatusResult(UseCaseResult):
    """Result of getting job status."""
    status: str = "idle"
    job_id: Optional[str] = None
    progress: Optional[Any] = None


class StartScrapingUseCase:
    """
    Use case for starting a new scraping job.

    Coordinates with the controller actor to initiate scraping.
    """

    def __init__(self, controller: Any):
        self._controller = controller

    async def execute(self, config: ScraperConfiguration) -> StartScrapingResult:
        """
        Execute the use case to start scraping.

        Args:
            config: The scraping configuration

        Returns:
            StartScrapingResult with success status and job_id if successful
        """
        try:
            result = await self._controller.ask(("START_JOB", config))

            if result.get("success"):
                return StartScrapingResult(
                    success=True,
                    job_id=result.get("data", {}).get("job_id")
                )
            else:
                return StartScrapingResult(
                    success=False,
                    error=result.get("error", "Unknown error")
                )
        except Exception as e:
            return StartScrapingResult(success=False, error=str(e))


class PauseScrapingUseCase:
    """Use case for pausing a running scraping job."""

    def __init__(self, controller: Any):
        self._controller = controller

    async def execute(self) -> UseCaseResult:
        """Execute the use case to pause scraping."""
        try:
            result = await self._controller.ask("PAUSE_JOB")
            return UseCaseResult(
                success=result.get("success", False),
                error=result.get("error")
            )
        except Exception as e:
            return UseCaseResult(success=False, error=str(e))


class ResumeScrapingUseCase:
    """Use case for resuming a paused scraping job."""

    def __init__(self, controller: Any):
        self._controller = controller

    async def execute(self) -> UseCaseResult:
        """Execute the use case to resume scraping."""
        try:
            result = await self._controller.ask("RESUME_JOB")
            return UseCaseResult(
                success=result.get("success", False),
                error=result.get("error")
            )
        except Exception as e:
            return UseCaseResult(success=False, error=str(e))


class CancelScrapingUseCase:
    """Use case for cancelling a scraping job."""

    def __init__(self, controller: Any):
        self._controller = controller

    async def execute(self) -> UseCaseResult:
        """Execute the use case to cancel scraping."""
        try:
            result = await self._controller.ask("CANCEL_JOB")
            return UseCaseResult(
                success=result.get("success", False),
                error=result.get("error")
            )
        except Exception as e:
            return UseCaseResult(success=False, error=str(e))


class GetJobStatusUseCase:
    """Use case for getting the current job status."""

    def __init__(self, controller: Any):
        self._controller = controller

    async def execute(self) -> JobStatusResult:
        """Execute the use case to get job status."""
        try:
            result = await self._controller.ask("GET_STATUS")

            if result.get("success"):
                data = result.get("data", {})
                return JobStatusResult(
                    success=True,
                    status=data.get("status", "idle"),
                    job_id=data.get("job_id"),
                    progress=data.get("progress")
                )
            else:
                return JobStatusResult(
                    success=False,
                    error=result.get("error")
                )
        except Exception as e:
            return JobStatusResult(success=False, error=str(e))


class CreateConfigurationUseCase:
    """Use case for creating a scraper configuration."""

    def execute(
        self,
        source: TargetSource,
        mode: ScraperMode,
        output_directory: str,
        output_format: OutputFormat = OutputFormat.JSON,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        single_date: Optional[date] = None,
        rate_limit_seconds: float = 2.0
    ) -> ScraperConfiguration:
        """
        Create a scraper configuration.

        Args:
            source: Target source to scrape
            mode: Scraping mode
            output_directory: Directory to save scraped data
            output_format: Output format (default: JSON)
            start_date: Start date for DATE_RANGE mode
            end_date: End date for DATE_RANGE mode
            single_date: Date for SINGLE_DATE mode
            rate_limit_seconds: Delay between requests

        Returns:
            ScraperConfiguration object

        Raises:
            ValueError: If required parameters are missing for the mode
        """
        # Build date range if needed
        date_range = None
        if mode == ScraperMode.DATE_RANGE:
            if start_date is None or end_date is None:
                raise ValueError("start_date and end_date required for DATE_RANGE mode")
            date_range = DateRange(start=start_date, end=end_date)
        elif mode == ScraperMode.HISTORICAL:
            if start_date is not None and end_date is not None:
                date_range = DateRange(start=start_date, end=end_date)

        return ScraperConfiguration(
            target_source=source,
            mode=mode,
            output_format=output_format,
            output_directory=output_directory,
            date_range=date_range,
            single_date=single_date,
            rate_limit_seconds=rate_limit_seconds
        )
