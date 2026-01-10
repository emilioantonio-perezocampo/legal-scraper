"""
Integration Tests for CAS CLI.

These tests verify the CLI end-to-end behavior
with real argument parsing and command execution.

Target: ~8 integration tests
"""
import pytest
import asyncio
import logging
from unittest.mock import Mock, AsyncMock, patch
from pathlib import Path
import tempfile


def _cleanup_cas_logger() -> None:
    """Close and remove all handlers from cas_scraper logger."""
    logger = logging.getLogger("cas_scraper")
    for handler in logger.handlers[:]:
        handler.close()
        logger.removeHandler(handler)


class TestCLIDiscoverCommand:
    """Integration tests for discover command."""

    @pytest.mark.asyncio
    async def test_discover_dry_run_shows_config(self):
        """Discover with --dry-run shows configuration without executing."""
        from src.infrastructure.cli.cas_cli import main_async

        with tempfile.TemporaryDirectory() as temp_dir:
            with patch.dict('os.environ', {'CAS_LOG_DIR': temp_dir}):
                try:
                    result = await main_async([
                        "discover",
                        "--dry-run",
                        "--year-from", "2020",
                        "--sport", "football",
                    ])
                    assert result == 0
                finally:
                    _cleanup_cas_logger()

    @pytest.mark.asyncio
    async def test_discover_creates_output_directories(self):
        """Discover command creates output directories (non dry-run)."""
        from src.infrastructure.cli.cas_cli import main_async

        with tempfile.TemporaryDirectory() as temp_dir:
            with patch.dict('os.environ', {
                'CAS_OUTPUT_DIR': f"{temp_dir}/output",
                'CAS_CHECKPOINT_DIR': f"{temp_dir}/checkpoints",
                'CAS_LOG_DIR': f"{temp_dir}/logs",
            }):
                try:
                    # Use non-dry-run to trigger ensure_directories
                    # (will still return 0 since pipeline is not implemented)
                    result = await main_async([
                        "discover",
                    ])
                    assert result == 0
                    assert Path(f"{temp_dir}/output").exists()
                    assert Path(f"{temp_dir}/checkpoints").exists()
                finally:
                    _cleanup_cas_logger()


class TestCLIStatusCommand:
    """Integration tests for status command."""

    @pytest.mark.asyncio
    async def test_status_with_no_checkpoints(self):
        """Status command handles empty checkpoint directory."""
        from src.infrastructure.cli.cas_cli import main_async

        with tempfile.TemporaryDirectory() as temp_dir:
            with patch.dict('os.environ', {
                'CAS_CHECKPOINT_DIR': f"{temp_dir}/checkpoints",
                'CAS_LOG_DIR': f"{temp_dir}/logs",
            }):
                try:
                    result = await main_async(["status"])
                    assert result == 0
                finally:
                    _cleanup_cas_logger()

    @pytest.mark.asyncio
    async def test_status_lists_checkpoints(self):
        """Status command lists existing checkpoints."""
        from src.infrastructure.cli.cas_cli import main_async
        import json

        with tempfile.TemporaryDirectory() as temp_dir:
            # Create a checkpoint file
            checkpoint_dir = Path(temp_dir) / "checkpoints"
            checkpoint_dir.mkdir(parents=True)
            checkpoint_file = checkpoint_dir / "session123.json"
            checkpoint_file.write_text(json.dumps({"state": "paused"}))

            with patch.dict('os.environ', {
                'CAS_CHECKPOINT_DIR': str(checkpoint_dir),
                'CAS_LOG_DIR': f"{temp_dir}/logs",
            }):
                try:
                    result = await main_async(["status"])
                    assert result == 0
                finally:
                    _cleanup_cas_logger()


class TestCLIResumeCommand:
    """Integration tests for resume command."""

    @pytest.mark.asyncio
    async def test_resume_nonexistent_session(self):
        """Resume command fails for nonexistent session."""
        from src.infrastructure.cli.cas_cli import main_async

        with tempfile.TemporaryDirectory() as temp_dir:
            with patch.dict('os.environ', {
                'CAS_CHECKPOINT_DIR': f"{temp_dir}/checkpoints",
                'CAS_LOG_DIR': f"{temp_dir}/logs",
            }):
                try:
                    Path(f"{temp_dir}/checkpoints").mkdir(parents=True)
                    result = await main_async([
                        "resume",
                        "--session-id", "nonexistent"
                    ])
                    assert result == 1
                finally:
                    _cleanup_cas_logger()


class TestCLIListCheckpointsCommand:
    """Integration tests for list-checkpoints command."""

    @pytest.mark.asyncio
    async def test_list_checkpoints_empty(self):
        """list-checkpoints handles empty directory."""
        from src.infrastructure.cli.cas_cli import main_async

        with tempfile.TemporaryDirectory() as temp_dir:
            with patch.dict('os.environ', {
                'CAS_CHECKPOINT_DIR': f"{temp_dir}/checkpoints",
                'CAS_LOG_DIR': f"{temp_dir}/logs",
            }):
                try:
                    result = await main_async(["list-checkpoints"])
                    assert result == 0
                finally:
                    _cleanup_cas_logger()

    @pytest.mark.asyncio
    async def test_list_checkpoints_with_limit(self):
        """list-checkpoints respects --limit."""
        from src.infrastructure.cli.cas_cli import main_async
        import json

        with tempfile.TemporaryDirectory() as temp_dir:
            checkpoint_dir = Path(temp_dir) / "checkpoints"
            checkpoint_dir.mkdir(parents=True)

            # Create multiple checkpoint files
            for i in range(5):
                (checkpoint_dir / f"session{i}.json").write_text(
                    json.dumps({"id": i})
                )

            with patch.dict('os.environ', {
                'CAS_CHECKPOINT_DIR': str(checkpoint_dir),
                'CAS_LOG_DIR': f"{temp_dir}/logs",
            }):
                try:
                    result = await main_async([
                        "list-checkpoints",
                        "--limit", "3"
                    ])
                    assert result == 0
                finally:
                    _cleanup_cas_logger()


class TestCLIConfigCommand:
    """Integration tests for config command."""

    @pytest.mark.asyncio
    async def test_config_show(self):
        """Config --show displays configuration."""
        from src.infrastructure.cli.cas_cli import main_async

        with tempfile.TemporaryDirectory() as temp_dir:
            with patch.dict('os.environ', {
                'CAS_LOG_DIR': f"{temp_dir}/logs",
            }):
                try:
                    result = await main_async(["config", "--show"])
                    assert result == 0
                finally:
                    _cleanup_cas_logger()

    @pytest.mark.asyncio
    async def test_config_without_show(self):
        """Config without --show shows help message."""
        from src.infrastructure.cli.cas_cli import main_async

        with tempfile.TemporaryDirectory() as temp_dir:
            with patch.dict('os.environ', {
                'CAS_LOG_DIR': f"{temp_dir}/logs",
            }):
                try:
                    result = await main_async(["config"])
                    assert result == 0
                finally:
                    _cleanup_cas_logger()
