"""
Tests for BJV Session Manager.

Following RED-GREEN TDD: Tests define checkpoint persistence behavior.

Target: ~8 tests
"""
import pytest
import json
import tempfile
from pathlib import Path


class TestBJVSessionManager:
    """Tests for BJVSessionManager."""

    def test_init_creates_output_dir(self, tmp_path):
        """Session manager creates output directory."""
        from src.infrastructure.bjv_session_manager import BJVSessionManager

        output_dir = tmp_path / "new_dir"
        manager = BJVSessionManager(output_dir=str(output_dir))

        assert output_dir.exists()

    def test_save_checkpoint(self, tmp_path):
        """Checkpoint is saved to disk."""
        from src.infrastructure.bjv_session_manager import BJVSessionManager

        manager = BJVSessionManager(output_dir=str(tmp_path))

        filepath = manager.save_checkpoint(
            session_id="test-123",
            state="running",
            stats={"descubiertos": 10, "procesados": 5},
            pending_ids=("id1", "id2"),
            processed_ids=("id3", "id4"),
        )

        assert filepath.exists()
        assert "checkpoint_test-123.json" in str(filepath)

        with open(filepath) as f:
            data = json.load(f)

        assert data["session_id"] == "test-123"
        assert data["state"] == "running"
        assert data["stats"]["descubiertos"] == 10

    def test_load_checkpoint_exists(self, tmp_path):
        """Existing checkpoint is loaded."""
        from src.infrastructure.bjv_session_manager import BJVSessionManager

        # Create checkpoint file manually
        checkpoint_file = tmp_path / "checkpoint_abc123.json"
        checkpoint_data = {
            "session_id": "abc123",
            "state": "paused",
            "stats": {"procesados": 5},
            "pending_ids": ["id1"],
            "processed_ids": ["id2"],
            "updated_at": "2024-01-01T00:00:00",
        }
        with open(checkpoint_file, "w") as f:
            json.dump(checkpoint_data, f)

        manager = BJVSessionManager(output_dir=str(tmp_path))
        loaded = manager.load_checkpoint("abc123")

        assert loaded is not None
        assert loaded["session_id"] == "abc123"
        assert loaded["state"] == "paused"

    def test_load_checkpoint_not_found(self, tmp_path):
        """Missing checkpoint returns None."""
        from src.infrastructure.bjv_session_manager import BJVSessionManager

        manager = BJVSessionManager(output_dir=str(tmp_path))
        loaded = manager.load_checkpoint("nonexistent")

        assert loaded is None

    def test_list_sessions_empty(self, tmp_path):
        """Empty directory returns empty list."""
        from src.infrastructure.bjv_session_manager import BJVSessionManager

        manager = BJVSessionManager(output_dir=str(tmp_path))
        sessions = manager.list_sessions()

        assert sessions == []

    def test_list_sessions_multiple(self, tmp_path):
        """Multiple sessions are listed."""
        from src.infrastructure.bjv_session_manager import BJVSessionManager

        # Create multiple checkpoint files
        for session_id in ["sess_001", "sess_002", "sess_003"]:
            filepath = tmp_path / f"checkpoint_{session_id}.json"
            with open(filepath, "w") as f:
                json.dump({"session_id": session_id}, f)

        manager = BJVSessionManager(output_dir=str(tmp_path))
        sessions = manager.list_sessions()

        assert len(sessions) == 3
        assert "sess_001" in sessions
        assert "sess_002" in sessions
        assert "sess_003" in sessions

    def test_get_latest_session(self, tmp_path):
        """Latest session is returned."""
        from src.infrastructure.bjv_session_manager import BJVSessionManager

        # Create checkpoint files (sorted alphabetically, latest = highest)
        for session_id in ["20240101_100000", "20240102_100000", "20240103_100000"]:
            filepath = tmp_path / f"checkpoint_{session_id}.json"
            with open(filepath, "w") as f:
                json.dump({"session_id": session_id}, f)

        manager = BJVSessionManager(output_dir=str(tmp_path))
        latest = manager.get_latest_session()

        assert latest == "20240103_100000"

    def test_get_latest_session_empty(self, tmp_path):
        """Empty directory returns None for latest."""
        from src.infrastructure.bjv_session_manager import BJVSessionManager

        manager = BJVSessionManager(output_dir=str(tmp_path))
        latest = manager.get_latest_session()

        assert latest is None
