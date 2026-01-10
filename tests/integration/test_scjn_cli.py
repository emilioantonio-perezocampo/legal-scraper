"""
Tests for SCJN CLI entry point.
"""
import pytest
import sys
from io import StringIO
from pathlib import Path
from unittest.mock import patch, MagicMock, AsyncMock

from src.scjn_main import main


class TestCLIArgumentParsing:
    """Tests for CLI argument parsing."""

    def test_discover_command_defaults(self):
        """Discover command should have sensible defaults."""
        with patch.object(sys, 'argv', ['scjn_main', 'discover', '--max-results', '5']):
            with patch('src.scjn_main.asyncio.run') as mock_run:
                main()
                mock_run.assert_called_once()
                args = mock_run.call_args[0][0]
                # asyncio.run receives a coroutine

    def test_discover_accepts_category_filter(self):
        """Discover should accept category filter."""
        with patch.object(sys, 'argv', ['scjn_main', 'discover', '--category', 'LEY']):
            with patch('src.scjn_main.asyncio.run') as mock_run:
                main()
                mock_run.assert_called_once()

    def test_discover_accepts_scope_filter(self):
        """Discover should accept scope filter."""
        with patch.object(sys, 'argv', ['scjn_main', 'discover', '--scope', 'FEDERAL']):
            with patch('src.scjn_main.asyncio.run') as mock_run:
                main()
                mock_run.assert_called_once()

    def test_discover_accepts_rate_limit(self):
        """Discover should accept rate limit."""
        with patch.object(sys, 'argv', ['scjn_main', 'discover', '--rate-limit', '0.2']):
            with patch('src.scjn_main.asyncio.run') as mock_run:
                main()
                mock_run.assert_called_once()

    def test_discover_accepts_concurrency(self):
        """Discover should accept concurrency setting."""
        with patch.object(sys, 'argv', ['scjn_main', 'discover', '--concurrency', '5']):
            with patch('src.scjn_main.asyncio.run') as mock_run:
                main()
                mock_run.assert_called_once()

    def test_status_command_works(self):
        """Status command should be recognized."""
        with patch.object(sys, 'argv', ['scjn_main', 'status']):
            with patch('src.scjn_main.asyncio.run') as mock_run:
                main()
                mock_run.assert_called_once()

    def test_resume_requires_session_id(self):
        """Resume should require session-id argument."""
        with patch.object(sys, 'argv', ['scjn_main', 'resume']):
            with pytest.raises(SystemExit):
                main()

    def test_resume_accepts_session_id(self):
        """Resume should accept session-id."""
        with patch.object(sys, 'argv', ['scjn_main', 'resume', '--session-id', 'test123']):
            with patch('src.scjn_main.asyncio.run') as mock_run:
                main()
                mock_run.assert_called_once()

    def test_no_command_shows_help(self):
        """No command should show help and exit."""
        with patch.object(sys, 'argv', ['scjn_main']):
            with pytest.raises(SystemExit):
                main()


class TestCLIDiscovery:
    """Tests for discovery command execution."""

    @pytest.mark.asyncio
    async def test_run_discovery_creates_directories(self, tmp_path):
        """Discovery should create output directories."""
        from src.scjn_main import run_discovery
        from types import SimpleNamespace

        output_dir = tmp_path / "output"
        checkpoint_dir = tmp_path / "checkpoints"

        args = SimpleNamespace(
            output_dir=str(output_dir),
            checkpoint_dir=str(checkpoint_dir),
            max_results=1,
            category=None,
            scope=None,
            status=None,
            rate_limit=10.0,  # Fast for test
            concurrency=1,
            skip_pdfs=True,
            all_pages=False,
        )

        # Mock the coordinator to avoid real HTTP
        with patch('src.scjn_main.create_pipeline') as mock_create:
            mock_coordinator = AsyncMock()
            mock_coordinator.ask = AsyncMock(side_effect=[
                # First call: discovery result
                MagicMock(documents_found=0, total_pages=1),
                # Second call: status
                {
                    'discovered_count': 0,
                    'downloaded_count': 0,
                    'pending_count': 0,
                    'active_downloads': 0,
                    'error_count': 0,
                },
            ])
            mock_create.return_value = mock_coordinator

            with patch('src.scjn_main.stop_pipeline') as mock_stop:
                mock_stop.return_value = None

                await run_discovery(args)

        # Directories should exist
        assert output_dir.exists()
        assert checkpoint_dir.exists()


class TestCLIStatus:
    """Tests for status command."""

    @pytest.mark.asyncio
    async def test_show_status_no_checkpoints(self, tmp_path, capsys):
        """Status with no checkpoints should report empty."""
        from src.scjn_main import show_status
        from types import SimpleNamespace

        checkpoint_dir = tmp_path / "checkpoints"
        checkpoint_dir.mkdir()

        args = SimpleNamespace(checkpoint_dir=str(checkpoint_dir))

        await show_status(args)

        captured = capsys.readouterr()
        assert "No checkpoints found" in captured.out

    @pytest.mark.asyncio
    async def test_show_status_with_checkpoints(self, tmp_path, capsys):
        """Status with checkpoints should list them."""
        from src.scjn_main import show_status
        from types import SimpleNamespace
        import json

        checkpoint_dir = tmp_path / "checkpoints"
        checkpoint_dir.mkdir()

        # Create a checkpoint file
        checkpoint_file = checkpoint_dir / "session123.json"
        checkpoint_file.write_text(json.dumps({
            "session_id": "session123",
            "processed_count": 10,
        }))

        args = SimpleNamespace(checkpoint_dir=str(checkpoint_dir))

        await show_status(args)

        captured = capsys.readouterr()
        assert "session123" in captured.out

    @pytest.mark.asyncio
    async def test_show_status_no_directory(self, tmp_path, capsys):
        """Status with missing directory should report it."""
        from src.scjn_main import show_status
        from types import SimpleNamespace

        args = SimpleNamespace(checkpoint_dir=str(tmp_path / "nonexistent"))

        await show_status(args)

        captured = capsys.readouterr()
        assert "No checkpoint directory found" in captured.out


class TestCLIResume:
    """Tests for resume command."""

    @pytest.mark.asyncio
    async def test_resume_missing_checkpoint(self, tmp_path, capsys):
        """Resume with missing checkpoint should report error."""
        from src.scjn_main import resume_session
        from types import SimpleNamespace

        checkpoint_dir = tmp_path / "checkpoints"
        checkpoint_dir.mkdir()

        args = SimpleNamespace(
            session_id="nonexistent",
            checkpoint_dir=str(checkpoint_dir),
            output_dir=str(tmp_path / "output"),
        )

        await resume_session(args)

        captured = capsys.readouterr()
        assert "Checkpoint not found" in captured.out

    @pytest.mark.asyncio
    async def test_resume_loads_checkpoint(self, tmp_path, capsys):
        """Resume should load and display checkpoint info."""
        from src.scjn_main import resume_session
        from types import SimpleNamespace
        import json

        checkpoint_dir = tmp_path / "checkpoints"
        checkpoint_dir.mkdir()

        # Create checkpoint
        checkpoint_file = checkpoint_dir / "test_session.json"
        checkpoint_file.write_text(json.dumps({
            "session_id": "test_session",
            "processed_count": 25,
            "last_processed_q_param": "ABC123",
            "failed_q_params": ["FAIL1", "FAIL2"],
        }))

        args = SimpleNamespace(
            session_id="test_session",
            checkpoint_dir=str(checkpoint_dir),
            output_dir=str(tmp_path / "output"),
        )

        await resume_session(args)

        captured = capsys.readouterr()
        assert "test_session" in captured.out
        assert "25" in captured.out
        assert "ABC123" in captured.out
