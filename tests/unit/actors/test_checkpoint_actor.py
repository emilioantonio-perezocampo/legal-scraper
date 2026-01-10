"""
RED Phase Tests: CheckpointActor

Tests for checkpoint persistence actor.
These tests must FAIL initially (RED phase).
"""
import pytest
import asyncio
import tempfile
import json
from pathlib import Path
from datetime import datetime

# These imports will fail until implementation exists
from src.infrastructure.actors.checkpoint_actor import CheckpointActor
from src.infrastructure.actors.messages import (
    GuardarCheckpoint,
    CargarCheckpoint,
    CheckpointGuardado,
)
from src.domain.scjn_entities import ScrapingCheckpoint


@pytest.fixture
def temp_checkpoint_dir(tmp_path):
    """Create a temporary directory for checkpoints."""
    return tmp_path / "checkpoints"


@pytest.fixture
def sample_checkpoint():
    """Create a sample checkpoint for testing."""
    return ScrapingCheckpoint(
        session_id="test-session-123",
        last_processed_q_param="abc123==",
        processed_count=50,
        failed_q_params=("fail1", "fail2"),
    )


@pytest.fixture
def another_checkpoint():
    """Create another sample checkpoint."""
    return ScrapingCheckpoint(
        session_id="another-session-456",
        last_processed_q_param="xyz789==",
        processed_count=100,
        failed_q_params=(),
    )


class TestCheckpointActorLifecycle:
    """Tests for CheckpointActor lifecycle management."""

    @pytest.mark.asyncio
    async def test_start_creates_directory(self, temp_checkpoint_dir):
        """Start should create checkpoint directory if not exists."""
        actor = CheckpointActor(checkpoint_dir=str(temp_checkpoint_dir))
        await actor.start()

        assert temp_checkpoint_dir.exists()
        assert temp_checkpoint_dir.is_dir()

        await actor.stop()

    @pytest.mark.asyncio
    async def test_start_loads_existing_checkpoints(self, temp_checkpoint_dir):
        """Start should load any existing checkpoint files."""
        # Create directory and pre-existing checkpoint
        temp_checkpoint_dir.mkdir(parents=True)
        checkpoint_file = temp_checkpoint_dir / "existing-session.json"
        checkpoint_data = {
            "session_id": "existing-session",
            "last_processed_q_param": "pre-existing",
            "processed_count": 25,
            "failed_q_params": [],
            "created_at": datetime.now().isoformat(),
        }
        checkpoint_file.write_text(json.dumps(checkpoint_data))

        actor = CheckpointActor(checkpoint_dir=str(temp_checkpoint_dir))
        await actor.start()

        # Should be able to load the pre-existing checkpoint
        result = await actor.ask(CargarCheckpoint(session_id="existing-session"))
        assert result is not None
        assert result.session_id == "existing-session"
        assert result.processed_count == 25

        await actor.stop()

    @pytest.mark.asyncio
    async def test_stop_is_clean(self, temp_checkpoint_dir):
        """Stop should cleanly terminate the actor."""
        actor = CheckpointActor(checkpoint_dir=str(temp_checkpoint_dir))
        await actor.start()
        await actor.stop()

        # Should be able to stop without errors
        assert not actor._running


class TestCheckpointActorSave:
    """Tests for saving checkpoints."""

    @pytest.mark.asyncio
    async def test_save_checkpoint_creates_file(
        self, temp_checkpoint_dir, sample_checkpoint
    ):
        """Saving checkpoint should create a file."""
        actor = CheckpointActor(checkpoint_dir=str(temp_checkpoint_dir))
        await actor.start()

        await actor.tell(GuardarCheckpoint(checkpoint=sample_checkpoint))
        await asyncio.sleep(0.1)  # Allow processing

        checkpoint_file = temp_checkpoint_dir / f"{sample_checkpoint.session_id}.json"
        assert checkpoint_file.exists()

        await actor.stop()

    @pytest.mark.asyncio
    async def test_save_checkpoint_returns_event(
        self, temp_checkpoint_dir, sample_checkpoint
    ):
        """Saving checkpoint should return CheckpointGuardado event."""
        actor = CheckpointActor(checkpoint_dir=str(temp_checkpoint_dir))
        await actor.start()

        result = await actor.ask(GuardarCheckpoint(checkpoint=sample_checkpoint))

        assert isinstance(result, CheckpointGuardado)
        assert result.session_id == sample_checkpoint.session_id
        assert result.processed_count == sample_checkpoint.processed_count

        await actor.stop()

    @pytest.mark.asyncio
    async def test_save_checkpoint_preserves_data(
        self, temp_checkpoint_dir, sample_checkpoint
    ):
        """Saved checkpoint should preserve all data."""
        actor = CheckpointActor(checkpoint_dir=str(temp_checkpoint_dir))
        await actor.start()

        await actor.ask(GuardarCheckpoint(checkpoint=sample_checkpoint))

        # Read back the file
        checkpoint_file = temp_checkpoint_dir / f"{sample_checkpoint.session_id}.json"
        data = json.loads(checkpoint_file.read_text())

        assert data["session_id"] == sample_checkpoint.session_id
        assert data["last_processed_q_param"] == sample_checkpoint.last_processed_q_param
        assert data["processed_count"] == sample_checkpoint.processed_count
        assert data["failed_q_params"] == list(sample_checkpoint.failed_q_params)

        await actor.stop()


class TestCheckpointActorLoad:
    """Tests for loading checkpoints."""

    @pytest.mark.asyncio
    async def test_load_checkpoint_returns_checkpoint(
        self, temp_checkpoint_dir, sample_checkpoint
    ):
        """Loading checkpoint should return the checkpoint."""
        actor = CheckpointActor(checkpoint_dir=str(temp_checkpoint_dir))
        await actor.start()

        # Save first
        await actor.ask(GuardarCheckpoint(checkpoint=sample_checkpoint))

        # Load
        result = await actor.ask(
            CargarCheckpoint(session_id=sample_checkpoint.session_id)
        )

        assert result is not None
        assert result.session_id == sample_checkpoint.session_id
        assert result.last_processed_q_param == sample_checkpoint.last_processed_q_param
        assert result.processed_count == sample_checkpoint.processed_count

        await actor.stop()

    @pytest.mark.asyncio
    async def test_load_nonexistent_returns_none(self, temp_checkpoint_dir):
        """Loading non-existent checkpoint should return None."""
        actor = CheckpointActor(checkpoint_dir=str(temp_checkpoint_dir))
        await actor.start()

        result = await actor.ask(CargarCheckpoint(session_id="nonexistent"))
        assert result is None

        await actor.stop()


class TestCheckpointActorList:
    """Tests for listing checkpoints."""

    @pytest.mark.asyncio
    async def test_list_checkpoints_empty(self, temp_checkpoint_dir):
        """Listing checkpoints when none exist should return empty."""
        actor = CheckpointActor(checkpoint_dir=str(temp_checkpoint_dir))
        await actor.start()

        result = await actor.ask(("LIST",))
        assert result == ()

        await actor.stop()

    @pytest.mark.asyncio
    async def test_list_checkpoints_multiple(
        self, temp_checkpoint_dir, sample_checkpoint, another_checkpoint
    ):
        """Listing should return all checkpoint session IDs."""
        actor = CheckpointActor(checkpoint_dir=str(temp_checkpoint_dir))
        await actor.start()

        await actor.ask(GuardarCheckpoint(checkpoint=sample_checkpoint))
        await actor.ask(GuardarCheckpoint(checkpoint=another_checkpoint))

        result = await actor.ask(("LIST",))

        assert sample_checkpoint.session_id in result
        assert another_checkpoint.session_id in result

        await actor.stop()


class TestCheckpointActorDelete:
    """Tests for deleting checkpoints."""

    @pytest.mark.asyncio
    async def test_delete_checkpoint_removes_file(
        self, temp_checkpoint_dir, sample_checkpoint
    ):
        """Deleting checkpoint should remove the file."""
        actor = CheckpointActor(checkpoint_dir=str(temp_checkpoint_dir))
        await actor.start()

        await actor.ask(GuardarCheckpoint(checkpoint=sample_checkpoint))
        checkpoint_file = temp_checkpoint_dir / f"{sample_checkpoint.session_id}.json"
        assert checkpoint_file.exists()

        await actor.ask(("DELETE", sample_checkpoint.session_id))
        assert not checkpoint_file.exists()

        await actor.stop()


class TestCheckpointActorPersistence:
    """Tests for checkpoint persistence across restarts."""

    @pytest.mark.asyncio
    async def test_checkpoint_survives_restart(
        self, temp_checkpoint_dir, sample_checkpoint
    ):
        """Checkpoint should survive actor restart."""
        # First instance - save
        actor1 = CheckpointActor(checkpoint_dir=str(temp_checkpoint_dir))
        await actor1.start()
        await actor1.ask(GuardarCheckpoint(checkpoint=sample_checkpoint))
        await actor1.stop()

        # Second instance - load
        actor2 = CheckpointActor(checkpoint_dir=str(temp_checkpoint_dir))
        await actor2.start()
        result = await actor2.ask(
            CargarCheckpoint(session_id=sample_checkpoint.session_id)
        )
        await actor2.stop()

        assert result is not None
        assert result.session_id == sample_checkpoint.session_id
        assert result.processed_count == sample_checkpoint.processed_count


class TestCheckpointActorErrorHandling:
    """Tests for error handling in CheckpointActor."""

    @pytest.mark.asyncio
    async def test_handles_corrupted_checkpoint_file(self, temp_checkpoint_dir):
        """Should handle corrupted checkpoint files gracefully."""
        temp_checkpoint_dir.mkdir(parents=True)
        corrupted_file = temp_checkpoint_dir / "corrupted-session.json"
        corrupted_file.write_text("this is not valid json {{{")

        actor = CheckpointActor(checkpoint_dir=str(temp_checkpoint_dir))
        await actor.start()

        # Should start without crashing
        # Corrupted checkpoint should be skipped or handled
        result = await actor.ask(CargarCheckpoint(session_id="corrupted-session"))
        assert result is None  # Should not crash, just return None

        await actor.stop()

    @pytest.mark.asyncio
    async def test_handles_missing_fields(self, temp_checkpoint_dir):
        """Should handle checkpoint files with missing fields."""
        temp_checkpoint_dir.mkdir(parents=True)
        partial_file = temp_checkpoint_dir / "partial-session.json"
        partial_file.write_text(json.dumps({"session_id": "partial-session"}))

        actor = CheckpointActor(checkpoint_dir=str(temp_checkpoint_dir))
        await actor.start()

        result = await actor.ask(CargarCheckpoint(session_id="partial-session"))
        # Should handle gracefully - either return None or partial data
        # Depending on implementation decision

        await actor.stop()
