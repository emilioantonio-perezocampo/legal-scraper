"""
RED Phase Tests: GUI Actor System

These tests define the expected behavior for GUI actors.
They should FAIL initially until we implement the actor layer.
"""
import pytest
import asyncio
from datetime import date
from unittest.mock import MagicMock, AsyncMock

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
from src.gui.infrastructure.actors.gui_controller import GuiControllerActor
from src.gui.infrastructure.actors.gui_state import GuiStateActor
from src.gui.infrastructure.actors.gui_bridge import GuiBridgeActor
from src.gui.domain.events import (
    JobStartedEvent,
    JobProgressEvent,
    JobCompletedEvent,
    JobFailedEvent,
    LogAddedEvent,
)


class TestGuiStateActor:
    """Tests for the GUI State Actor (holds application state)."""

    @pytest.mark.asyncio
    async def test_state_actor_initialization(self):
        """State actor should start with empty state."""
        actor = GuiStateActor()
        await actor.start()

        state = await actor.ask("GET_STATE")

        assert state.current_job is None
        assert state.job_history == ()  # Tuple for immutability
        assert state.is_connected is False

        await actor.stop()

    @pytest.mark.asyncio
    async def test_state_actor_create_job(self):
        """State actor should create and store a new job."""
        actor = GuiStateActor()
        await actor.start()

        config = ScraperConfiguration(
            target_source=TargetSource.DOF,
            mode=ScraperMode.TODAY,
            output_format=OutputFormat.JSON,
            output_directory="scraped_data"
        )
        job = await actor.ask(("CREATE_JOB", config))

        assert job is not None
        assert job.status == JobStatus.IDLE
        assert job.configuration == config

        state = await actor.ask("GET_STATE")
        assert state.current_job is not None

        await actor.stop()

    @pytest.mark.asyncio
    async def test_state_actor_update_job_status(self):
        """State actor should handle job status updates."""
        actor = GuiStateActor()
        await actor.start()

        config = ScraperConfiguration(
            target_source=TargetSource.DOF,
            mode=ScraperMode.TODAY,
            output_format=OutputFormat.JSON,
            output_directory="scraped_data"
        )
        job = await actor.ask(("CREATE_JOB", config))

        # Simulate job starting
        updated_job = await actor.ask(("UPDATE_JOB_STATUS", JobStatus.RUNNING))

        assert updated_job.status == JobStatus.RUNNING

        await actor.stop()

    @pytest.mark.asyncio
    async def test_state_actor_update_progress(self):
        """State actor should handle progress updates."""
        actor = GuiStateActor()
        await actor.start()

        config = ScraperConfiguration(
            target_source=TargetSource.DOF,
            mode=ScraperMode.TODAY,
            output_format=OutputFormat.JSON,
            output_directory="scraped_data"
        )
        await actor.ask(("CREATE_JOB", config))
        await actor.ask(("UPDATE_JOB_STATUS", JobStatus.RUNNING))

        progress = JobProgress(
            total_items=100,
            processed_items=25,
            successful_items=25,
            failed_items=0
        )
        updated_job = await actor.ask(("UPDATE_PROGRESS", progress))

        assert updated_job.progress is not None
        assert updated_job.progress.percentage == 25.0

        await actor.stop()

    @pytest.mark.asyncio
    async def test_state_actor_add_log(self):
        """State actor should handle log additions."""
        actor = GuiStateActor()
        await actor.start()

        config = ScraperConfiguration(
            target_source=TargetSource.DOF,
            mode=ScraperMode.TODAY,
            output_format=OutputFormat.JSON,
            output_directory="scraped_data"
        )
        await actor.ask(("CREATE_JOB", config))

        updated_job = await actor.ask(("ADD_LOG", LogLevel.INFO, "Test message", "TestSource"))

        assert len(updated_job.logs) == 1
        assert updated_job.logs[0].message == "Test message"

        await actor.stop()

    @pytest.mark.asyncio
    async def test_state_actor_connection_status(self):
        """State actor should track scraper connection status."""
        actor = GuiStateActor()
        await actor.start()

        await actor.tell(("SET_CONNECTED", True))
        await asyncio.sleep(0.01)

        state = await actor.ask("GET_STATE")
        assert state.is_connected is True

        await actor.stop()

    @pytest.mark.asyncio
    async def test_state_actor_job_completion_adds_to_history(self):
        """Completed jobs should be added to history."""
        actor = GuiStateActor()
        await actor.start()

        config = ScraperConfiguration(
            target_source=TargetSource.DOF,
            mode=ScraperMode.TODAY,
            output_format=OutputFormat.JSON,
            output_directory="scraped_data"
        )
        await actor.ask(("CREATE_JOB", config))
        await actor.ask(("UPDATE_JOB_STATUS", JobStatus.RUNNING))
        await actor.ask(("UPDATE_JOB_STATUS", JobStatus.COMPLETED))

        state = await actor.ask("GET_STATE")
        assert len(state.job_history) == 1
        assert state.current_job is None  # Current job cleared after completion

        await actor.stop()


class TestGuiControllerActor:
    """Tests for the GUI Controller Actor (handles user actions)."""

    @pytest.mark.asyncio
    async def test_controller_start_job(self):
        """Controller should orchestrate starting a job."""
        state_actor = GuiStateActor()
        mock_scraper_bridge = AsyncMock()

        controller = GuiControllerActor(
            state_actor=state_actor,
            scraper_bridge=mock_scraper_bridge
        )

        await state_actor.start()
        await controller.start()

        config = ScraperConfiguration(
            target_source=TargetSource.DOF,
            mode=ScraperMode.TODAY,
            output_format=OutputFormat.JSON,
            output_directory="scraped_data"
        )

        result = await controller.ask(("START_JOB", config))

        assert result["success"] is True
        mock_scraper_bridge.tell.assert_called()

        await controller.stop()
        await state_actor.stop()

    @pytest.mark.asyncio
    async def test_controller_pause_job(self):
        """Controller should handle pause requests."""
        state_actor = GuiStateActor()
        mock_scraper_bridge = AsyncMock()

        controller = GuiControllerActor(
            state_actor=state_actor,
            scraper_bridge=mock_scraper_bridge
        )

        await state_actor.start()
        await controller.start()

        # First create and start a job
        config = ScraperConfiguration(
            target_source=TargetSource.DOF,
            mode=ScraperMode.TODAY,
            output_format=OutputFormat.JSON,
            output_directory="scraped_data"
        )
        await controller.ask(("START_JOB", config))

        # Then pause it
        result = await controller.ask("PAUSE_JOB")

        assert result["success"] is True

        state = await state_actor.ask("GET_STATE")
        assert state.current_job.status == JobStatus.PAUSED

        await controller.stop()
        await state_actor.stop()

    @pytest.mark.asyncio
    async def test_controller_resume_job(self):
        """Controller should handle resume requests."""
        state_actor = GuiStateActor()
        mock_scraper_bridge = AsyncMock()

        controller = GuiControllerActor(
            state_actor=state_actor,
            scraper_bridge=mock_scraper_bridge
        )

        await state_actor.start()
        await controller.start()

        config = ScraperConfiguration(
            target_source=TargetSource.DOF,
            mode=ScraperMode.TODAY,
            output_format=OutputFormat.JSON,
            output_directory="scraped_data"
        )
        await controller.ask(("START_JOB", config))
        await controller.ask("PAUSE_JOB")

        result = await controller.ask("RESUME_JOB")

        assert result["success"] is True

        state = await state_actor.ask("GET_STATE")
        assert state.current_job.status == JobStatus.RUNNING

        await controller.stop()
        await state_actor.stop()

    @pytest.mark.asyncio
    async def test_controller_cancel_job(self):
        """Controller should handle cancel requests."""
        state_actor = GuiStateActor()
        mock_scraper_bridge = AsyncMock()

        controller = GuiControllerActor(
            state_actor=state_actor,
            scraper_bridge=mock_scraper_bridge
        )

        await state_actor.start()
        await controller.start()

        config = ScraperConfiguration(
            target_source=TargetSource.DOF,
            mode=ScraperMode.TODAY,
            output_format=OutputFormat.JSON,
            output_directory="scraped_data"
        )
        await controller.ask(("START_JOB", config))

        result = await controller.ask("CANCEL_JOB")

        assert result["success"] is True
        mock_scraper_bridge.tell.assert_called()

        await controller.stop()
        await state_actor.stop()

    @pytest.mark.asyncio
    async def test_controller_cannot_start_if_job_running(self):
        """Controller should prevent starting a new job if one is running."""
        state_actor = GuiStateActor()
        mock_scraper_bridge = AsyncMock()

        controller = GuiControllerActor(
            state_actor=state_actor,
            scraper_bridge=mock_scraper_bridge
        )

        await state_actor.start()
        await controller.start()

        config = ScraperConfiguration(
            target_source=TargetSource.DOF,
            mode=ScraperMode.TODAY,
            output_format=OutputFormat.JSON,
            output_directory="scraped_data"
        )

        await controller.ask(("START_JOB", config))

        # Try to start another job
        result = await controller.ask(("START_JOB", config))

        assert result["success"] is False
        assert "already running" in result["error"].lower()

        await controller.stop()
        await state_actor.stop()


class TestGuiBridgeActor:
    """Tests for the GUI Bridge Actor (connects GUI to scraper system)."""

    @pytest.mark.asyncio
    async def test_bridge_connects_to_scraper_actors(self):
        """Bridge should connect GUI to existing scraper actors."""
        mock_scheduler = AsyncMock()
        mock_discovery = AsyncMock()
        mock_persistence = AsyncMock()

        bridge = GuiBridgeActor(
            scheduler_actor=mock_scheduler,
            discovery_actor=mock_discovery,
            persistence_actor=mock_persistence
        )

        await bridge.start()

        assert bridge.is_connected is True

        await bridge.stop()

    @pytest.mark.asyncio
    async def test_bridge_forwards_scrape_commands(self):
        """Bridge should forward scrape commands to discovery actor."""
        mock_scheduler = AsyncMock()
        mock_discovery = AsyncMock()
        mock_persistence = AsyncMock()

        bridge = GuiBridgeActor(
            scheduler_actor=mock_scheduler,
            discovery_actor=mock_discovery,
            persistence_actor=mock_persistence
        )

        await bridge.start()

        config = ScraperConfiguration(
            target_source=TargetSource.DOF,
            mode=ScraperMode.TODAY,
            output_format=OutputFormat.JSON,
            output_directory="scraped_data"
        )

        await bridge.tell(("START_SCRAPE", config))
        await asyncio.sleep(0.01)

        mock_discovery.tell.assert_called()

        await bridge.stop()

    @pytest.mark.asyncio
    async def test_bridge_forwards_stop_commands(self):
        """Bridge should forward stop commands to actors."""
        mock_scheduler = AsyncMock()
        mock_discovery = AsyncMock()
        mock_persistence = AsyncMock()

        bridge = GuiBridgeActor(
            scheduler_actor=mock_scheduler,
            discovery_actor=mock_discovery,
            persistence_actor=mock_persistence
        )

        await bridge.start()
        await bridge.tell("STOP_SCRAPE")
        await asyncio.sleep(0.01)

        # Should signal to stop (implementation may vary)
        await bridge.stop()

    @pytest.mark.asyncio
    async def test_bridge_receives_progress_events(self):
        """Bridge should receive and forward progress events from scrapers."""
        mock_scheduler = AsyncMock()
        mock_discovery = AsyncMock()
        mock_persistence = AsyncMock()
        event_handler = AsyncMock()

        bridge = GuiBridgeActor(
            scheduler_actor=mock_scheduler,
            discovery_actor=mock_discovery,
            persistence_actor=mock_persistence,
            event_handler=event_handler
        )

        await bridge.start()

        # Simulate receiving a progress event
        progress_event = JobProgressEvent(
            job_id="test-123",
            progress=JobProgress(
                total_items=100,
                processed_items=50,
                successful_items=48,
                failed_items=2
            )
        )
        await bridge.tell(("PROGRESS_EVENT", progress_event))
        await asyncio.sleep(0.01)

        event_handler.assert_called()

        await bridge.stop()


class TestDomainEvents:
    """Tests for domain events used in actor communication."""

    def test_job_started_event(self):
        """JobStartedEvent should carry job information."""
        event = JobStartedEvent(
            job_id="test-123",
            configuration=ScraperConfiguration(
                target_source=TargetSource.DOF,
                mode=ScraperMode.TODAY,
                output_format=OutputFormat.JSON,
                output_directory="scraped_data"
            )
        )

        assert event.job_id == "test-123"
        assert event.configuration.target_source == TargetSource.DOF

    def test_job_progress_event(self):
        """JobProgressEvent should carry progress information."""
        progress = JobProgress(
            total_items=100,
            processed_items=50,
            successful_items=50,
            failed_items=0
        )
        event = JobProgressEvent(job_id="test-123", progress=progress)

        assert event.job_id == "test-123"
        assert event.progress.percentage == 50.0

    def test_job_completed_event(self):
        """JobCompletedEvent should carry completion information."""
        event = JobCompletedEvent(
            job_id="test-123",
            total_processed=100,
            successful=95,
            failed=5
        )

        assert event.job_id == "test-123"
        assert event.total_processed == 100

    def test_job_failed_event(self):
        """JobFailedEvent should carry error information."""
        event = JobFailedEvent(
            job_id="test-123",
            error_message="Connection timeout"
        )

        assert event.job_id == "test-123"
        assert event.error_message == "Connection timeout"

    def test_log_added_event(self):
        """LogAddedEvent should carry log information."""
        event = LogAddedEvent(
            job_id="test-123",
            level=LogLevel.INFO,
            message="Scraping started",
            source="DofScraperActor"
        )

        assert event.job_id == "test-123"
        assert event.level == LogLevel.INFO
        assert event.message == "Scraping started"
