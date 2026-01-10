"""
Tests for SCJN GUI Bridge Actor.
"""
import pytest
import pytest_asyncio
import asyncio
from unittest.mock import AsyncMock, MagicMock
from datetime import datetime

from src.gui.infrastructure.actors.scjn_bridge import (
    SCJNGuiBridgeActor,
    SCJNSearchConfig,
    SCJNProgress,
    SCJNJobStarted,
    SCJNJobProgress,
    SCJNJobCompleted,
    SCJNJobFailed,
)
from src.infrastructure.actors.messages import (
    DescubrirDocumentos,
    PausarPipeline,
    ReanudarPipeline,
    ObtenerEstado,
    PaginaDescubierta,
)


class TestSCJNSearchConfig:
    """Tests for SCJNSearchConfig dataclass."""

    def test_config_creation_with_defaults(self):
        """Config should create with sensible defaults."""
        config = SCJNSearchConfig()

        assert config.category is None
        assert config.scope is None
        assert config.max_results == 100
        assert config.rate_limit == 0.5

    def test_config_creation_with_values(self):
        """Config should accept custom values."""
        config = SCJNSearchConfig(
            category="LEY",
            scope="FEDERAL",
            max_results=50,
            skip_pdfs=True,
        )

        assert config.category == "LEY"
        assert config.scope == "FEDERAL"
        assert config.max_results == 50
        assert config.skip_pdfs is True

    def test_config_is_frozen(self):
        """Config should be immutable."""
        config = SCJNSearchConfig()

        with pytest.raises(Exception):
            config.max_results = 200


class TestSCJNProgress:
    """Tests for SCJNProgress dataclass."""

    def test_progress_creation_with_defaults(self):
        """Progress should create with zero defaults."""
        progress = SCJNProgress()

        assert progress.discovered_count == 0
        assert progress.downloaded_count == 0
        assert progress.error_count == 0
        assert progress.state == "IDLE"

    def test_progress_creation_with_values(self):
        """Progress should accept custom values."""
        progress = SCJNProgress(
            discovered_count=100,
            downloaded_count=75,
            pending_count=25,
            active_downloads=5,
            error_count=3,
            state="DOWNLOADING",
        )

        assert progress.discovered_count == 100
        assert progress.downloaded_count == 75
        assert progress.pending_count == 25

    def test_progress_is_frozen(self):
        """Progress should be immutable."""
        progress = SCJNProgress()

        with pytest.raises(Exception):
            progress.downloaded_count = 10


class TestSCJNEvents:
    """Tests for SCJN event dataclasses."""

    def test_job_started_event(self):
        """JobStarted event should carry config."""
        config = SCJNSearchConfig(category="LEY")
        event = SCJNJobStarted(job_id="job-123", config=config)

        assert event.job_id == "job-123"
        assert event.config.category == "LEY"
        assert isinstance(event.timestamp, datetime)

    def test_job_progress_event(self):
        """JobProgress event should carry progress."""
        progress = SCJNProgress(discovered_count=50)
        event = SCJNJobProgress(job_id="job-123", progress=progress)

        assert event.job_id == "job-123"
        assert event.progress.discovered_count == 50

    def test_job_completed_event(self):
        """JobCompleted event should carry totals."""
        event = SCJNJobCompleted(
            job_id="job-123",
            total_discovered=100,
            total_downloaded=95,
            total_errors=5,
        )

        assert event.job_id == "job-123"
        assert event.total_discovered == 100
        assert event.total_downloaded == 95
        assert event.total_errors == 5

    def test_job_failed_event(self):
        """JobFailed event should carry error message."""
        event = SCJNJobFailed(
            job_id="job-123",
            error_message="Connection timeout",
        )

        assert event.job_id == "job-123"
        assert event.error_message == "Connection timeout"


class TestSCJNGuiBridgeActorCreation:
    """Tests for SCJNGuiBridgeActor initialization."""

    def test_actor_creation_without_coordinator(self):
        """Actor should create without coordinator."""
        actor = SCJNGuiBridgeActor()

        assert actor.is_connected is False
        assert actor.current_job_id is None

    def test_actor_creation_with_coordinator(self):
        """Actor should create with coordinator."""
        mock_coordinator = AsyncMock()
        actor = SCJNGuiBridgeActor(coordinator_actor=mock_coordinator)

        assert actor.is_connected is True

    def test_actor_creation_with_event_handler(self):
        """Actor should accept event handler."""
        handler = MagicMock()
        actor = SCJNGuiBridgeActor(event_handler=handler)

        assert actor._event_handler is handler


class TestSCJNGuiBridgeActorLifecycle:
    """Tests for actor lifecycle."""

    @pytest.mark.asyncio
    async def test_actor_starts(self):
        """Actor should start successfully."""
        actor = SCJNGuiBridgeActor()
        await actor.start()

        assert actor._running

        await actor.stop()

    @pytest.mark.asyncio
    async def test_actor_stops_cleanly(self):
        """Actor should stop without errors."""
        actor = SCJNGuiBridgeActor()
        await actor.start()
        await actor.stop()

        assert not actor._running


class TestSCJNGuiBridgeActorStartSearch:
    """Tests for starting searches."""

    @pytest.mark.asyncio
    async def test_start_search_requires_coordinator(self):
        """Start should fail without coordinator."""
        actor = SCJNGuiBridgeActor()
        await actor.start()

        config = SCJNSearchConfig()
        result = await actor.ask(("START_SEARCH", config))

        assert result["success"] is False
        assert "not connected" in result["error"].lower()

        await actor.stop()

    @pytest.mark.asyncio
    async def test_start_search_creates_job(self):
        """Start should create job ID."""
        mock_coordinator = AsyncMock()
        mock_coordinator.ask = AsyncMock(return_value=PaginaDescubierta(
            documents_found=10,
            current_page=1,
            total_pages=1,
        ))

        actor = SCJNGuiBridgeActor(
            coordinator_actor=mock_coordinator,
            poll_interval=10.0,  # Slow polling to avoid interference
        )
        await actor.start()

        config = SCJNSearchConfig(category="LEY")
        result = await actor.ask(("START_SEARCH", config))

        assert result["success"] is True
        assert result["job_id"] is not None
        assert actor.current_job_id is not None

        await actor.stop()

    @pytest.mark.asyncio
    async def test_start_search_sends_discovery_command(self):
        """Start should send DescubrirDocumentos to coordinator."""
        mock_coordinator = AsyncMock()
        mock_coordinator.ask = AsyncMock(return_value=PaginaDescubierta(
            documents_found=10,
            current_page=1,
            total_pages=1,
        ))

        actor = SCJNGuiBridgeActor(
            coordinator_actor=mock_coordinator,
            poll_interval=10.0,
        )
        await actor.start()

        config = SCJNSearchConfig(
            category="LEY",
            scope="FEDERAL",
            max_results=50,
        )
        await actor.ask(("START_SEARCH", config))

        # Verify DescubrirDocumentos was sent (may be first call before polling starts)
        mock_coordinator.ask.assert_called()
        # Find the DescubrirDocumentos call among all calls
        discovery_calls = [
            c[0][0] for c in mock_coordinator.ask.call_args_list
            if isinstance(c[0][0], DescubrirDocumentos)
        ]
        assert len(discovery_calls) >= 1
        discovery_cmd = discovery_calls[0]
        assert discovery_cmd.category == "LEY"
        assert discovery_cmd.scope == "FEDERAL"
        assert discovery_cmd.max_results == 50

        await actor.stop()

    @pytest.mark.asyncio
    async def test_start_search_prevents_duplicate(self):
        """Cannot start search while one is in progress."""
        mock_coordinator = AsyncMock()
        mock_coordinator.ask = AsyncMock(return_value=PaginaDescubierta(
            documents_found=10,
            current_page=1,
            total_pages=1,
        ))

        actor = SCJNGuiBridgeActor(
            coordinator_actor=mock_coordinator,
            poll_interval=10.0,
        )
        await actor.start()

        config = SCJNSearchConfig()

        # First start succeeds
        result1 = await actor.ask(("START_SEARCH", config))
        assert result1["success"] is True

        # Second start fails
        result2 = await actor.ask(("START_SEARCH", config))
        assert result2["success"] is False
        assert "in progress" in result2["error"].lower()

        await actor.stop()

    @pytest.mark.asyncio
    async def test_start_search_emits_event(self):
        """Start should emit JobStarted event."""
        mock_coordinator = AsyncMock()
        mock_coordinator.ask = AsyncMock(return_value=PaginaDescubierta(
            documents_found=10,
            current_page=1,
            total_pages=1,
        ))
        events = []

        async def handler(event):
            events.append(event)

        actor = SCJNGuiBridgeActor(
            coordinator_actor=mock_coordinator,
            event_handler=handler,
            poll_interval=10.0,
        )
        await actor.start()

        config = SCJNSearchConfig(category="LEY")
        await actor.ask(("START_SEARCH", config))

        # Wait a bit for async event handling
        await asyncio.sleep(0.05)

        assert len(events) >= 1
        assert isinstance(events[0], SCJNJobStarted)
        assert events[0].config.category == "LEY"

        await actor.stop()


class TestSCJNGuiBridgeActorControlCommands:
    """Tests for control commands (stop, pause, resume)."""

    @pytest.mark.asyncio
    async def test_stop_search(self):
        """Stop should clear current job."""
        mock_coordinator = AsyncMock()
        mock_coordinator.ask = AsyncMock(return_value=PaginaDescubierta(
            documents_found=10,
            current_page=1,
            total_pages=1,
        ))

        actor = SCJNGuiBridgeActor(
            coordinator_actor=mock_coordinator,
            poll_interval=10.0,
        )
        await actor.start()

        # Start a search
        await actor.ask(("START_SEARCH", SCJNSearchConfig()))

        # Stop it
        result = await actor.ask("STOP_SEARCH")

        assert result["success"] is True
        assert actor.current_job_id is None

        await actor.stop()

    @pytest.mark.asyncio
    async def test_pause_search(self):
        """Pause should send PausarPipeline to coordinator."""
        mock_coordinator = AsyncMock()
        mock_coordinator.ask = AsyncMock(return_value={"paused": True})

        actor = SCJNGuiBridgeActor(coordinator_actor=mock_coordinator)
        await actor.start()

        result = await actor.ask("PAUSE_SEARCH")

        assert result["success"] is True
        # Verify PausarPipeline was sent
        calls = [c[0][0] for c in mock_coordinator.ask.call_args_list]
        assert any(isinstance(c, PausarPipeline) for c in calls)

        await actor.stop()

    @pytest.mark.asyncio
    async def test_resume_search(self):
        """Resume should send ReanudarPipeline to coordinator."""
        mock_coordinator = AsyncMock()
        mock_coordinator.ask = AsyncMock(return_value={"resumed": True})

        actor = SCJNGuiBridgeActor(coordinator_actor=mock_coordinator)
        await actor.start()

        result = await actor.ask("RESUME_SEARCH")

        assert result["success"] is True
        # Verify ReanudarPipeline was sent
        calls = [c[0][0] for c in mock_coordinator.ask.call_args_list]
        assert any(isinstance(c, ReanudarPipeline) for c in calls)

        await actor.stop()


class TestSCJNGuiBridgeActorStatus:
    """Tests for status queries."""

    @pytest.mark.asyncio
    async def test_get_status(self):
        """Should return connection and job status."""
        mock_coordinator = AsyncMock()
        actor = SCJNGuiBridgeActor(coordinator_actor=mock_coordinator)
        await actor.start()

        result = await actor.ask("GET_STATUS")

        assert result["connected"] is True
        assert result["current_job_id"] is None
        assert result["is_polling"] is False

        await actor.stop()

    @pytest.mark.asyncio
    async def test_get_progress(self):
        """Should return progress from coordinator."""
        mock_coordinator = AsyncMock()
        mock_coordinator.ask = AsyncMock(return_value={
            "discovered_count": 50,
            "downloaded_count": 30,
            "pending_count": 20,
            "active_downloads": 3,
            "error_count": 2,
            "state": "DOWNLOADING",
        })

        actor = SCJNGuiBridgeActor(coordinator_actor=mock_coordinator)
        await actor.start()

        progress = await actor.ask("GET_PROGRESS")

        assert isinstance(progress, SCJNProgress)
        assert progress.discovered_count == 50
        assert progress.downloaded_count == 30
        assert progress.pending_count == 20

        await actor.stop()

    @pytest.mark.asyncio
    async def test_get_progress_without_coordinator(self):
        """Should return None without coordinator."""
        actor = SCJNGuiBridgeActor()
        await actor.start()

        progress = await actor.ask("GET_PROGRESS")

        assert progress is None

        await actor.stop()


class TestSCJNGuiBridgeActorSetCoordinator:
    """Tests for dynamic coordinator setting."""

    @pytest.mark.asyncio
    async def test_set_coordinator(self):
        """Should allow setting coordinator dynamically."""
        actor = SCJNGuiBridgeActor()
        await actor.start()

        assert actor.is_connected is False

        mock_coordinator = AsyncMock()
        result = await actor.ask(("SET_COORDINATOR", mock_coordinator))

        assert result["success"] is True
        assert actor.is_connected is True

        await actor.stop()


class TestSCJNGuiBridgeActorEventHandling:
    """Tests for event handling."""

    @pytest.mark.asyncio
    async def test_sync_event_handler(self):
        """Should work with sync event handlers."""
        mock_coordinator = AsyncMock()
        mock_coordinator.ask = AsyncMock(return_value=PaginaDescubierta(
            documents_found=10,
            current_page=1,
            total_pages=1,
        ))
        events = []

        def sync_handler(event):
            events.append(event)

        actor = SCJNGuiBridgeActor(
            coordinator_actor=mock_coordinator,
            event_handler=sync_handler,
            poll_interval=10.0,
        )
        await actor.start()

        await actor.ask(("START_SEARCH", SCJNSearchConfig()))
        await asyncio.sleep(0.05)

        assert len(events) >= 1

        await actor.stop()

    @pytest.mark.asyncio
    async def test_async_event_handler(self):
        """Should work with async event handlers."""
        mock_coordinator = AsyncMock()
        mock_coordinator.ask = AsyncMock(return_value=PaginaDescubierta(
            documents_found=10,
            current_page=1,
            total_pages=1,
        ))
        events = []

        async def async_handler(event):
            events.append(event)

        actor = SCJNGuiBridgeActor(
            coordinator_actor=mock_coordinator,
            event_handler=async_handler,
            poll_interval=10.0,
        )
        await actor.start()

        await actor.ask(("START_SEARCH", SCJNSearchConfig()))
        await asyncio.sleep(0.05)

        assert len(events) >= 1

        await actor.stop()

    @pytest.mark.asyncio
    async def test_handler_error_does_not_break_bridge(self):
        """Handler errors should not break the bridge."""
        mock_coordinator = AsyncMock()
        mock_coordinator.ask = AsyncMock(return_value=PaginaDescubierta(
            documents_found=10,
            current_page=1,
            total_pages=1,
        ))

        def bad_handler(event):
            raise Exception("Handler error")

        actor = SCJNGuiBridgeActor(
            coordinator_actor=mock_coordinator,
            event_handler=bad_handler,
            poll_interval=10.0,
        )
        await actor.start()

        # Should not raise
        result = await actor.ask(("START_SEARCH", SCJNSearchConfig()))
        assert result["success"] is True

        await actor.stop()
