"""
GUI Application Services

Services coordinate between use cases, actors, and infrastructure.
They provide a high-level API for the presentation layer.
"""
from typing import Optional, List, Callable, Any
import asyncio

from src.gui.domain.entities import (
    ScraperConfiguration,
    ScraperJob,
    JobProgress,
)
from src.gui.domain.value_objects import (
    TargetSource,
    OutputFormat,
    ScraperMode,
)
from src.gui.infrastructure.actors.gui_state import GuiStateActor
from src.gui.infrastructure.actors.gui_controller import GuiControllerActor
from src.gui.infrastructure.actors.gui_bridge import GuiBridgeActor
from .use_cases import (
    StartScrapingUseCase,
    PauseScrapingUseCase,
    ResumeScrapingUseCase,
    CancelScrapingUseCase,
    GetJobStatusUseCase,
    CreateConfigurationUseCase,
    UseCaseResult,
    StartScrapingResult,
    JobStatusResult,
)


class ScraperService:
    """
    High-level service for scraper operations.

    Coordinates actors, use cases, and provides a simple API
    for the presentation layer.
    """

    def __init__(
        self,
        scheduler_actor: Any = None,
        discovery_actor: Any = None,
        persistence_actor: Any = None,
        state_change_callback: Optional[Callable] = None
    ):
        """
        Initialize the scraper service.

        Args:
            scheduler_actor: Optional existing scheduler actor
            discovery_actor: Optional existing discovery actor
            persistence_actor: Optional existing persistence actor
            state_change_callback: Callback for state changes
        """
        self._state_actor = GuiStateActor(state_change_callback=state_change_callback)
        self._bridge_actor = GuiBridgeActor(
            scheduler_actor=scheduler_actor,
            discovery_actor=discovery_actor,
            persistence_actor=persistence_actor
        )
        self._controller_actor = GuiControllerActor(
            state_actor=self._state_actor,
            scraper_bridge=self._bridge_actor
        )
        self._is_running = False

    @property
    def state_actor(self) -> GuiStateActor:
        """Get the state actor."""
        return self._state_actor

    @property
    def controller_actor(self) -> GuiControllerActor:
        """Get the controller actor."""
        return self._controller_actor

    @property
    def bridge_actor(self) -> GuiBridgeActor:
        """Get the bridge actor."""
        return self._bridge_actor

    @property
    def is_running(self) -> bool:
        """Check if service is running."""
        return self._is_running

    async def start(self) -> None:
        """Start all actors."""
        await self._state_actor.start()
        await self._bridge_actor.start()
        await self._controller_actor.start()
        self._is_running = True

    async def stop(self) -> None:
        """Stop all actors."""
        await self._controller_actor.stop()
        await self._bridge_actor.stop()
        await self._state_actor.stop()
        self._is_running = False

    async def start_scraping(self, config: ScraperConfiguration) -> StartScrapingResult:
        """
        Start a new scraping job.

        Args:
            config: Scraping configuration

        Returns:
            StartScrapingResult
        """
        use_case = StartScrapingUseCase(controller=self._controller_actor)
        return await use_case.execute(config)

    async def pause_scraping(self) -> UseCaseResult:
        """Pause the current scraping job."""
        use_case = PauseScrapingUseCase(controller=self._controller_actor)
        return await use_case.execute()

    async def resume_scraping(self) -> UseCaseResult:
        """Resume the paused scraping job."""
        use_case = ResumeScrapingUseCase(controller=self._controller_actor)
        return await use_case.execute()

    async def cancel_scraping(self) -> UseCaseResult:
        """Cancel the current scraping job."""
        use_case = CancelScrapingUseCase(controller=self._controller_actor)
        return await use_case.execute()

    async def get_status(self) -> JobStatusResult:
        """Get the current job status."""
        use_case = GetJobStatusUseCase(controller=self._controller_actor)
        return await use_case.execute()

    def connect_scraper_actors(
        self,
        scheduler_actor: Any,
        discovery_actor: Any,
        persistence_actor: Any
    ) -> None:
        """
        Connect to existing scraper actors.

        Use this to integrate with an already-running scraper system.
        """
        self._bridge_actor._scheduler_actor = scheduler_actor
        self._bridge_actor._discovery_actor = discovery_actor
        self._bridge_actor._persistence_actor = persistence_actor
        self._bridge_actor._is_connected = True


class ConfigurationService:
    """
    Service for managing scraper configurations.

    Provides methods for getting available options and
    creating/validating configurations.
    """

    def __init__(self):
        self._create_use_case = CreateConfigurationUseCase()

    def get_available_sources(self) -> List[TargetSource]:
        """Get list of available scraping sources."""
        return list(TargetSource)

    def get_available_modes(self) -> List[ScraperMode]:
        """Get list of available scraping modes."""
        return list(ScraperMode)

    def get_available_formats(self) -> List[OutputFormat]:
        """Get list of available output formats."""
        return list(OutputFormat)

    def get_default_configuration(self) -> ScraperConfiguration:
        """Get a default configuration."""
        return ScraperConfiguration(
            target_source=TargetSource.DOF,
            mode=ScraperMode.TODAY,
            output_format=OutputFormat.JSON,
            output_directory="scraped_data",
            rate_limit_seconds=2.0
        )

    def validate_output_directory(self, directory: str) -> bool:
        """
        Validate an output directory.

        Args:
            directory: Directory path to validate

        Returns:
            True if valid, False otherwise
        """
        if not directory or not directory.strip():
            return False
        return True

    def create_configuration(self, **kwargs) -> ScraperConfiguration:
        """
        Create a configuration using the use case.

        Args:
            **kwargs: Configuration parameters

        Returns:
            ScraperConfiguration
        """
        return self._create_use_case.execute(**kwargs)
