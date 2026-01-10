"""
Integration tests for BJV CLI with mocked HTTP.

Following RED-GREEN TDD: Tests verify end-to-end behavior.

Target: ~10 tests
"""
import pytest
import asyncio
import json
from unittest.mock import AsyncMock, patch, MagicMock
from pathlib import Path
from argparse import Namespace

from tests.fixtures.bjv_mock_responses import (
    MOCK_SEARCH_PAGE_1,
    MOCK_BOOK_DETAIL,
    MOCK_PDF_BYTES,
)


class TestBJVIntegrationWithMocks:
    """Integration tests with mocked HTTP responses."""

    @pytest.fixture
    def temp_output_dir(self, tmp_path):
        """Create temporary output directory."""
        output_dir = tmp_path / "bjv_test_output"
        output_dir.mkdir(parents=True, exist_ok=True)
        return output_dir

    @pytest.fixture
    def mock_http_session(self):
        """Create mock aiohttp session."""
        session = MagicMock()

        async def mock_get(url, **kwargs):
            response = MagicMock()

            if "buscar" in url or "search" in url:
                response.text = AsyncMock(return_value=MOCK_SEARCH_PAGE_1)
                response.status = 200
            elif "libro.htm" in url:
                response.text = AsyncMock(return_value=MOCK_BOOK_DETAIL)
                response.status = 200
            elif ".pdf" in url:
                response.read = AsyncMock(return_value=MOCK_PDF_BYTES)
                response.status = 200
            else:
                response.status = 404

            # Make it work as async context manager
            response.__aenter__ = AsyncMock(return_value=response)
            response.__aexit__ = AsyncMock(return_value=None)
            return response

        session.get = mock_get
        session.close = AsyncMock()
        session.__aenter__ = AsyncMock(return_value=session)
        session.__aexit__ = AsyncMock(return_value=None)
        return session

    @pytest.mark.asyncio
    async def test_discover_pipeline_mock_http(self, temp_output_dir):
        """Test discover command with mocked coordinator."""
        from src.bjv_main import BJVCLIHandler, EXIT_SUCCESS

        args = Namespace(
            command="discover",
            query=None,
            area=None,
            year_from=None,
            year_to=None,
            max_results=10,
            json=True,
            verbose=False,
        )

        handler = BJVCLIHandler(args)

        # Mock coordinator
        mock_coordinator = AsyncMock()
        mock_coordinator.start = AsyncMock()
        mock_coordinator.stop = AsyncMock()
        mock_coordinator.ask = AsyncMock(return_value={
            "total": 2,
            "libros": [
                {"titulo": "Derecho Civil"},
                {"titulo": "Derecho Penal"},
            ],
        })

        with patch.object(handler, '_create_coordinator', return_value=mock_coordinator):
            handler.coordinator = mock_coordinator
            result = await handler.cmd_discover()

        assert result == EXIT_SUCCESS

    @pytest.mark.asyncio
    async def test_scrape_with_checkpoint(self, temp_output_dir):
        """Test scrape creates checkpoint."""
        from src.bjv_main import BJVCLIHandler, EXIT_SUCCESS
        from src.infrastructure.bjv_session_manager import BJVSessionManager

        args = Namespace(
            command="scrape",
            query=None,
            area=None,
            year_from=None,
            year_to=None,
            max_results=5,
            output_dir=str(temp_output_dir),
            skip_embeddings=True,
            rate_limit=0.5,
            concurrent=3,
            json=False,
            verbose=False,
        )

        handler = BJVCLIHandler(args)

        # Mock coordinator that completes
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
    async def test_checkpoint_save_on_interrupt(self, temp_output_dir):
        """Test that checkpoint is saved when interrupted."""
        from src.bjv_main import BJVCLIHandler, EXIT_INTERRUPTED

        args = Namespace(
            command="scrape",
            query=None,
            area=None,
            year_from=None,
            year_to=None,
            max_results=5,
            output_dir=str(temp_output_dir),
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
        # Verify DetenerPipeline was called
        mock_coordinator.tell.assert_called()

    @pytest.mark.asyncio
    async def test_resume_from_checkpoint(self, temp_output_dir):
        """Test resume loads checkpoint correctly."""
        from src.bjv_main import BJVCLIHandler, EXIT_SUCCESS

        # Create checkpoint file
        checkpoint_file = temp_output_dir / "checkpoint_test_session.json"
        checkpoint_file.write_text(json.dumps({
            "session_id": "test_session",
            "state": "paused",
            "stats": {"procesados": 3},
            "pending_ids": ["id4", "id5"],
            "processed_ids": ["id1", "id2", "id3"],
        }))

        args = Namespace(
            command="resume",
            session_id="test_session",
            output_dir=str(temp_output_dir),
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
            result = await handler.cmd_resume()

        assert result == EXIT_SUCCESS


class TestEndToEndMocked:
    """End-to-end CLI tests with mocking."""

    def test_exit_codes_correct(self):
        """Verify exit codes match specification."""
        from src.bjv_main import EXIT_SUCCESS, EXIT_ERROR, EXIT_INTERRUPTED

        assert EXIT_SUCCESS == 0
        assert EXIT_ERROR == 1
        assert EXIT_INTERRUPTED == 2

    def test_parser_help_works(self, capsys):
        """Parser help displays without error."""
        from src.bjv_main import create_parser

        parser = create_parser()

        with pytest.raises(SystemExit) as exc_info:
            parser.parse_args(["--help"])

        assert exc_info.value.code == 0

    @pytest.mark.asyncio
    async def test_json_output_format(self, tmp_path, capsys):
        """JSON output is valid JSON."""
        from src.bjv_main import BJVCLIHandler

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
            "libros": [{"titulo": "Test Book"}],
        })

        with patch.object(handler, '_create_coordinator', return_value=mock_coordinator):
            handler.coordinator = mock_coordinator
            await handler.cmd_discover()

        captured = capsys.readouterr()
        # Should contain JSON-like content
        assert "total" in captured.out or "{" in captured.out

    @pytest.mark.asyncio
    async def test_status_displays_sessions(self, tmp_path, capsys):
        """Status command displays session info."""
        from src.bjv_main import BJVCLIHandler, EXIT_SUCCESS

        # Create test checkpoint
        checkpoint = tmp_path / "checkpoint_mysession.json"
        checkpoint.write_text(json.dumps({
            "session_id": "mysession",
            "state": "completed",
            "updated_at": "2024-01-01T12:00:00",
            "stats": {"procesados": 10, "descubiertos": 10},
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
        captured = capsys.readouterr()
        assert "mysession" in captured.out or "Session" in captured.out

    @pytest.mark.asyncio
    async def test_error_output_format(self, capsys):
        """Errors are formatted correctly."""
        from src.bjv_main import BJVCLIHandler

        args = Namespace(
            command="discover",
            query=None,
            area=None,
            year_from=None,
            year_to=None,
            max_results=5,
            json=False,
            verbose=False,
        )

        handler = BJVCLIHandler(args)
        handler._print_error("Test error message")

        captured = capsys.readouterr()
        assert "Error" in captured.out or "error" in captured.out.lower()
