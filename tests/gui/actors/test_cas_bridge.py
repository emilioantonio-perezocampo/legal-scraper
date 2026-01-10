"""
Tests for CAS GUI Bridge Actor.

Following RED-GREEN TDD: These tests define the expected behavior
for the CAS GUI bridge actor that connects GUI to CAS coordinator.

Target: ~15 tests
"""
import pytest
from unittest.mock import Mock, AsyncMock, patch
from datetime import datetime


class TestCASGuiConfig:
    """Tests for CASGuiConfig dataclass."""

    def test_default_values(self):
        """CASGuiConfig has sensible defaults."""
        from src.gui.infrastructure.actors.cas_gui_bridge import CASGuiConfig

        config = CASGuiConfig()

        assert config.year_from is None
        assert config.year_to is None
        assert config.sport is None
        assert config.matter is None
        assert config.max_results == 100

    def test_with_all_values(self):
        """CASGuiConfig accepts all values."""
        from src.gui.infrastructure.actors.cas_gui_bridge import CASGuiConfig

        config = CASGuiConfig(
            year_from=2020,
            year_to=2024,
            sport="football",
            matter="doping",
            max_results=50,
        )

        assert config.year_from == 2020
        assert config.year_to == 2024
        assert config.sport == "football"
        assert config.matter == "doping"
        assert config.max_results == 50

    def test_to_dict(self):
        """CASGuiConfig.to_dict() returns dictionary."""
        from src.gui.infrastructure.actors.cas_gui_bridge import CASGuiConfig

        config = CASGuiConfig(year_from=2020, sport="football")
        data = config.to_dict()

        assert data["year_from"] == 2020
        assert data["sport"] == "football"
        assert "max_results" in data


class TestCASJobEvents:
    """Tests for CAS job event dataclasses."""

    def test_cas_job_started_event(self):
        """CASJobStarted event has required fields."""
        from src.gui.infrastructure.actors.cas_gui_bridge import CASJobStarted

        event = CASJobStarted(
            job_id="job-123",
            timestamp="2024-01-01T12:00:00",
            filters={"sport": "football"},
        )

        assert event.job_id == "job-123"
        assert event.timestamp == "2024-01-01T12:00:00"
        assert event.filters == {"sport": "football"}

    def test_cas_job_progress_event(self):
        """CASJobProgress event has required fields."""
        from src.gui.infrastructure.actors.cas_gui_bridge import CASJobProgress

        event = CASJobProgress(
            job_id="job-123",
            estado="DESCUBRIENDO",
            descubiertos=10,
            descargados=5,
            procesados=3,
            errores=1,
            porcentaje=50.0,
        )

        assert event.job_id == "job-123"
        assert event.descubiertos == 10
        assert event.porcentaje == 50.0

    def test_cas_job_completed_event(self):
        """CASJobCompleted event has required fields."""
        from src.gui.infrastructure.actors.cas_gui_bridge import CASJobCompleted

        event = CASJobCompleted(
            job_id="job-123",
            timestamp="2024-01-01T12:30:00",
            estadisticas={"total": 100},
            exito=True,
        )

        assert event.job_id == "job-123"
        assert event.exito is True
        assert event.estadisticas == {"total": 100}

    def test_cas_job_error_event(self):
        """CASJobError event has required fields."""
        from src.gui.infrastructure.actors.cas_gui_bridge import CASJobError

        event = CASJobError(
            job_id="job-123",
            timestamp="2024-01-01T12:00:00",
            mensaje="Connection failed",
            recuperable=True,
        )

        assert event.job_id == "job-123"
        assert event.mensaje == "Connection failed"
        assert event.recuperable is True


class TestCASGuiBridgeActor:
    """Tests for CASGuiBridgeActor class."""

    def test_inherits_from_base_actor(self):
        """CASGuiBridgeActor inherits from CASBaseActor."""
        from src.gui.infrastructure.actors.cas_gui_bridge import CASGuiBridgeActor
        from src.infrastructure.actors.cas_base_actor import CASBaseActor

        assert issubclass(CASGuiBridgeActor, CASBaseActor)

    def test_creates_with_defaults(self):
        """CASGuiBridgeActor creates with default values."""
        from src.gui.infrastructure.actors.cas_gui_bridge import CASGuiBridgeActor

        bridge = CASGuiBridgeActor()

        assert bridge._on_event is None
        assert bridge._output_dir == "cas_data"
        assert bridge._current_job_id is None

    def test_creates_with_event_callback(self):
        """CASGuiBridgeActor accepts event callback."""
        from src.gui.infrastructure.actors.cas_gui_bridge import CASGuiBridgeActor

        callback = AsyncMock()
        bridge = CASGuiBridgeActor(on_event=callback)

        assert bridge._on_event == callback

    def test_creates_with_output_dir(self):
        """CASGuiBridgeActor accepts output directory."""
        from src.gui.infrastructure.actors.cas_gui_bridge import CASGuiBridgeActor

        bridge = CASGuiBridgeActor(output_dir="/custom/path")

        assert bridge._output_dir == "/custom/path"


class TestCASGuiBridgeActorStartJob:
    """Tests for CASGuiBridgeActor.start_job method."""

    @pytest.mark.asyncio
    async def test_start_job_returns_job_id(self):
        """start_job returns a job ID."""
        from src.gui.infrastructure.actors.cas_gui_bridge import (
            CASGuiBridgeActor,
            CASGuiConfig,
        )

        bridge = CASGuiBridgeActor()
        # Mock the coordinator
        bridge._coordinator = AsyncMock()
        bridge._coordinator.tell = AsyncMock()

        config = CASGuiConfig(max_results=50)
        job_id = await bridge.start_job(config)

        assert job_id is not None
        assert job_id.startswith("cas-gui-")

    @pytest.mark.asyncio
    async def test_start_job_sets_current_job_id(self):
        """start_job sets current job ID."""
        from src.gui.infrastructure.actors.cas_gui_bridge import (
            CASGuiBridgeActor,
            CASGuiConfig,
        )

        bridge = CASGuiBridgeActor()
        bridge._coordinator = AsyncMock()
        bridge._coordinator.tell = AsyncMock()

        config = CASGuiConfig()
        job_id = await bridge.start_job(config)

        assert bridge._current_job_id == job_id

    @pytest.mark.asyncio
    async def test_start_job_emits_started_event(self):
        """start_job emits CASJobStarted event."""
        from src.gui.infrastructure.actors.cas_gui_bridge import (
            CASGuiBridgeActor,
            CASGuiConfig,
            CASJobStarted,
        )

        events = []

        async def capture_event(event):
            events.append(event)

        bridge = CASGuiBridgeActor(on_event=capture_event)
        bridge._coordinator = AsyncMock()
        bridge._coordinator.tell = AsyncMock()

        config = CASGuiConfig(sport="football")
        await bridge.start_job(config)

        assert len(events) == 1
        assert isinstance(events[0], CASJobStarted)
        assert events[0].filters["sport"] == "football"


class TestCASGuiBridgeActorControls:
    """Tests for CASGuiBridgeActor pause/resume/stop methods."""

    @pytest.mark.asyncio
    async def test_pause_job_sends_message(self):
        """pause_job sends pause message to coordinator."""
        from src.gui.infrastructure.actors.cas_gui_bridge import CASGuiBridgeActor

        bridge = CASGuiBridgeActor()
        bridge._coordinator = AsyncMock()
        bridge._coordinator.tell = AsyncMock()
        bridge._current_job_id = "job-123"

        await bridge.pause_job()

        bridge._coordinator.tell.assert_called()

    @pytest.mark.asyncio
    async def test_resume_job_sends_message(self):
        """resume_job sends resume message to coordinator."""
        from src.gui.infrastructure.actors.cas_gui_bridge import CASGuiBridgeActor

        bridge = CASGuiBridgeActor()
        bridge._coordinator = AsyncMock()
        bridge._coordinator.tell = AsyncMock()
        bridge._current_job_id = "job-123"

        await bridge.resume_job()

        bridge._coordinator.tell.assert_called()

    @pytest.mark.asyncio
    async def test_stop_job_sends_message(self):
        """stop_job sends stop message to coordinator."""
        from src.gui.infrastructure.actors.cas_gui_bridge import CASGuiBridgeActor

        bridge = CASGuiBridgeActor()
        bridge._coordinator = AsyncMock()
        bridge._coordinator.tell = AsyncMock()
        bridge._current_job_id = "job-123"

        await bridge.stop_job()

        bridge._coordinator.tell.assert_called()
