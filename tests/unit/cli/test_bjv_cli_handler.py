"""
Tests for BJV CLI Handler.

Following RED-GREEN TDD: Tests define handler behavior.

Target: ~15 tests
"""
import pytest
import asyncio
import json
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from argparse import Namespace
from pathlib import Path


class TestBJVCLIHandler:
    """Tests for BJVCLIHandler initialization."""

    def test_init_with_args(self):
        """Handler initializes with args."""
        from src.bjv_main import BJVCLIHandler

        args = Namespace(
            command="discover",
            json=False,
            verbose=False,
        )

        handler = BJVCLIHandler(args)

        assert handler.args is args
        assert handler.coordinator is None
        assert handler._interrupted is False

    def test_setup_signal_handlers(self):
        """Signal handlers are set up."""
        from src.bjv_main import BJVCLIHandler
        import signal

        args = Namespace(command="discover", json=False, verbose=False)
        handler = BJVCLIHandler(args)

        # Should not raise
        handler.setup_signal_handlers()

    @pytest.mark.asyncio
    async def test_run_no_command_error(self):
        """Run with no command returns error."""
        from src.bjv_main import BJVCLIHandler, EXIT_ERROR

        args = Namespace(command=None, json=False, verbose=False)
        handler = BJVCLIHandler(args)

        result = await handler.run()

        assert result == EXIT_ERROR


class TestDiscoverHandler:
    """Tests for discover command handler."""

    @pytest.mark.asyncio
    async def test_cmd_discover_success(self):
        """Discover command succeeds with mocked coordinator."""
        from src.bjv_main import BJVCLIHandler, EXIT_SUCCESS

        args = Namespace(
            command="discover",
            query=None,
            area=None,
            year_from=None,
            year_to=None,
            max_results=10,
            json=False,
            verbose=False,
        )

        handler = BJVCLIHandler(args)

        # Mock the coordinator
        mock_coordinator = AsyncMock()
        mock_coordinator.start = AsyncMock()
        mock_coordinator.stop = AsyncMock()
        mock_coordinator.ask = AsyncMock(return_value={
            "total": 2,
            "libros": [
                {"titulo": "Libro 1"},
                {"titulo": "Libro 2"},
            ],
        })

        with patch.object(handler, '_create_coordinator', return_value=mock_coordinator):
            handler.coordinator = mock_coordinator
            result = await handler.cmd_discover()

        assert result == EXIT_SUCCESS

    @pytest.mark.asyncio
    async def test_cmd_discover_error(self):
        """Discover command handles errors."""
        from src.bjv_main import BJVCLIHandler, EXIT_ERROR

        args = Namespace(
            command="discover",
            query=None,
            area=None,
            year_from=None,
            year_to=None,
            max_results=10,
            json=False,
            verbose=False,
        )

        handler = BJVCLIHandler(args)

        # Mock coordinator that raises error
        mock_coordinator = AsyncMock()
        mock_coordinator.start = AsyncMock()
        mock_coordinator.stop = AsyncMock()
        mock_coordinator.ask = AsyncMock(side_effect=Exception("Network error"))

        with patch.object(handler, '_create_coordinator', return_value=mock_coordinator):
            handler.coordinator = mock_coordinator
            result = await handler.cmd_discover()

        assert result == EXIT_ERROR

    @pytest.mark.asyncio
    async def test_cmd_discover_json_output(self, capsys):
        """Discover outputs JSON when flag is set."""
        from src.bjv_main import BJVCLIHandler, EXIT_SUCCESS

        args = Namespace(
            command="discover",
            query=None,
            area=None,
            year_from=None,
            year_to=None,
            max_results=5,
            json=True,
            verbose=False,
        )

        handler = BJVCLIHandler(args)

        mock_coordinator = AsyncMock()
        mock_coordinator.start = AsyncMock()
        mock_coordinator.stop = AsyncMock()
        mock_coordinator.ask = AsyncMock(return_value={
            "total": 1,
            "libros": [{"titulo": "Test"}],
        })

        with patch.object(handler, '_create_coordinator', return_value=mock_coordinator):
            handler.coordinator = mock_coordinator
            await handler.cmd_discover()

        captured = capsys.readouterr()
        # JSON output should be parseable
        assert "total" in captured.out or "libros" in captured.out


class TestScrapeHandler:
    """Tests for scrape command handler."""

    @pytest.mark.asyncio
    async def test_cmd_scrape_creates_output_dir(self, tmp_path):
        """Scrape creates output directory."""
        from src.bjv_main import BJVCLIHandler

        output_dir = tmp_path / "new_output"

        args = Namespace(
            command="scrape",
            query=None,
            area=None,
            year_from=None,
            year_to=None,
            max_results=5,
            output_dir=str(output_dir),
            skip_embeddings=False,
            rate_limit=0.5,
            concurrent=3,
            json=False,
            verbose=False,
        )

        handler = BJVCLIHandler(args)

        # Mock coordinator to complete immediately
        mock_coordinator = AsyncMock()
        mock_coordinator.start = AsyncMock()
        mock_coordinator.stop = AsyncMock()
        mock_coordinator.tell = AsyncMock()
        mock_coordinator.ask = AsyncMock(return_value={
            "state": "completed",
            "stats": {},
        })

        with patch.object(handler, '_create_coordinator', return_value=mock_coordinator):
            handler.coordinator = mock_coordinator
            await handler.cmd_scrape()

        assert output_dir.exists()

    @pytest.mark.asyncio
    async def test_cmd_scrape_success(self, tmp_path):
        """Scrape command completes successfully."""
        from src.bjv_main import BJVCLIHandler, EXIT_SUCCESS

        args = Namespace(
            command="scrape",
            query=None,
            area=None,
            year_from=None,
            year_to=None,
            max_results=5,
            output_dir=str(tmp_path),
            skip_embeddings=False,
            rate_limit=0.5,
            concurrent=3,
            json=False,
            verbose=False,
        )

        handler = BJVCLIHandler(args)

        mock_coordinator = AsyncMock()
        mock_coordinator.start = AsyncMock()
        mock_coordinator.stop = AsyncMock()
        mock_coordinator.tell = AsyncMock()
        mock_coordinator.ask = AsyncMock(return_value={
            "state": "completed",
            "stats": {"procesados": 5},
        })

        with patch.object(handler, '_create_coordinator', return_value=mock_coordinator):
            handler.coordinator = mock_coordinator
            result = await handler.cmd_scrape()

        assert result == EXIT_SUCCESS

    @pytest.mark.asyncio
    async def test_cmd_scrape_interrupted(self, tmp_path):
        """Scrape handles interrupt correctly."""
        from src.bjv_main import BJVCLIHandler, EXIT_INTERRUPTED

        args = Namespace(
            command="scrape",
            query=None,
            area=None,
            year_from=None,
            year_to=None,
            max_results=5,
            output_dir=str(tmp_path),
            skip_embeddings=False,
            rate_limit=0.5,
            concurrent=3,
            json=False,
            verbose=False,
        )

        handler = BJVCLIHandler(args)
        handler._interrupted = True  # Simulate interrupt

        mock_coordinator = AsyncMock()
        mock_coordinator.start = AsyncMock()
        mock_coordinator.stop = AsyncMock()
        mock_coordinator.tell = AsyncMock()
        mock_coordinator.ask = AsyncMock(return_value={
            "state": "running",
            "stats": {},
        })

        with patch.object(handler, '_create_coordinator', return_value=mock_coordinator):
            handler.coordinator = mock_coordinator
            result = await handler.cmd_scrape()

        assert result == EXIT_INTERRUPTED

    @pytest.mark.asyncio
    async def test_cmd_scrape_error(self, tmp_path):
        """Scrape handles errors correctly."""
        from src.bjv_main import BJVCLIHandler, EXIT_ERROR

        args = Namespace(
            command="scrape",
            query=None,
            area=None,
            year_from=None,
            year_to=None,
            max_results=5,
            output_dir=str(tmp_path),
            skip_embeddings=False,
            rate_limit=0.5,
            concurrent=3,
            json=False,
            verbose=False,
        )

        handler = BJVCLIHandler(args)

        mock_coordinator = AsyncMock()
        mock_coordinator.start = AsyncMock(side_effect=Exception("Start failed"))
        mock_coordinator.stop = AsyncMock()

        with patch.object(handler, '_create_coordinator', return_value=mock_coordinator):
            handler.coordinator = mock_coordinator
            result = await handler.cmd_scrape()

        assert result == EXIT_ERROR


class TestStatusHandler:
    """Tests for status command handler."""

    @pytest.mark.asyncio
    async def test_cmd_status_no_sessions(self, tmp_path):
        """Status with no sessions returns success."""
        from src.bjv_main import BJVCLIHandler, EXIT_SUCCESS

        args = Namespace(
            command="status",
            session_id=None,
            output_dir=str(tmp_path),
            json=False,
            verbose=False,
        )

        handler = BJVCLIHandler(args)
        result = await handler.cmd_status()

        assert result == EXIT_SUCCESS

    @pytest.mark.asyncio
    async def test_cmd_status_with_sessions(self, tmp_path):
        """Status shows existing sessions."""
        from src.bjv_main import BJVCLIHandler, EXIT_SUCCESS

        # Create a checkpoint file
        checkpoint = tmp_path / "checkpoint_test123.json"
        checkpoint.write_text(json.dumps({
            "session_id": "test123",
            "state": "completed",
            "updated_at": "2024-01-01T00:00:00",
            "stats": {"procesados": 10},
        }))

        args = Namespace(
            command="status",
            session_id=None,
            output_dir=str(tmp_path),
            json=False,
            verbose=False,
        )

        handler = BJVCLIHandler(args)
        result = await handler.cmd_status()

        assert result == EXIT_SUCCESS

    @pytest.mark.asyncio
    async def test_cmd_status_session_not_found(self, tmp_path):
        """Status with invalid session returns error."""
        from src.bjv_main import BJVCLIHandler, EXIT_ERROR

        args = Namespace(
            command="status",
            session_id="nonexistent",
            output_dir=str(tmp_path),
            json=False,
            verbose=False,
        )

        handler = BJVCLIHandler(args)
        result = await handler.cmd_status()

        assert result == EXIT_ERROR


class TestResumeHandler:
    """Tests for resume command handler."""

    @pytest.mark.asyncio
    async def test_cmd_resume_success(self, tmp_path):
        """Resume command succeeds with checkpoint."""
        from src.bjv_main import BJVCLIHandler, EXIT_SUCCESS

        # Create checkpoint
        checkpoint = tmp_path / "checkpoint_sess123.json"
        checkpoint.write_text(json.dumps({
            "session_id": "sess123",
            "state": "paused",
            "stats": {},
            "pending_ids": [],
            "processed_ids": [],
        }))

        args = Namespace(
            command="resume",
            session_id="sess123",
            output_dir=str(tmp_path),
            json=False,
            verbose=False,
        )

        handler = BJVCLIHandler(args)

        mock_coordinator = AsyncMock()
        mock_coordinator.start = AsyncMock()
        mock_coordinator.stop = AsyncMock()
        mock_coordinator.tell = AsyncMock()
        mock_coordinator.ask = AsyncMock(return_value={
            "state": "completed",
            "stats": {},
        })

        with patch.object(handler, '_create_coordinator', return_value=mock_coordinator):
            handler.coordinator = mock_coordinator
            result = await handler.cmd_resume()

        assert result == EXIT_SUCCESS

    @pytest.mark.asyncio
    async def test_cmd_resume_checkpoint_not_found(self, tmp_path):
        """Resume without checkpoint returns error."""
        from src.bjv_main import BJVCLIHandler, EXIT_ERROR

        args = Namespace(
            command="resume",
            session_id="nonexistent",
            output_dir=str(tmp_path),
            json=False,
            verbose=False,
        )

        handler = BJVCLIHandler(args)
        result = await handler.cmd_resume()

        assert result == EXIT_ERROR
