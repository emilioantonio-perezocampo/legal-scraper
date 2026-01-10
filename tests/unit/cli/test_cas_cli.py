"""
Tests for CAS CLI Command-Line Interface

Following RED-GREEN TDD: These tests define the expected behavior
for the CAS scraper CLI commands and argument parsing.

Target: ~20 tests for CAS CLI
"""
import pytest
import logging
from unittest.mock import Mock, patch
from pathlib import Path
import tempfile


class TestCreateParser:
    """Tests for create_parser function."""

    def test_returns_argument_parser(self):
        """create_parser returns ArgumentParser."""
        from src.infrastructure.cli.cas_cli import create_parser
        import argparse

        parser = create_parser()
        assert isinstance(parser, argparse.ArgumentParser)

    def test_has_discover_command(self):
        """Parser has discover subcommand."""
        from src.infrastructure.cli.cas_cli import create_parser

        parser = create_parser()
        # Check that subparsers exist
        assert parser._subparsers is not None

    def test_has_status_command(self):
        """Parser has status subcommand."""
        from src.infrastructure.cli.cas_cli import parse_args

        args = parse_args(["status"])
        assert args.command == "status"

    def test_has_resume_command(self):
        """Parser has resume subcommand."""
        from src.infrastructure.cli.cas_cli import parse_args

        args = parse_args(["resume", "--session-id", "test123"])
        assert args.command == "resume"

    def test_has_list_checkpoints_command(self):
        """Parser has list-checkpoints subcommand."""
        from src.infrastructure.cli.cas_cli import parse_args

        args = parse_args(["list-checkpoints"])
        assert args.command == "list-checkpoints"

    def test_has_config_command(self):
        """Parser has config subcommand."""
        from src.infrastructure.cli.cas_cli import parse_args

        args = parse_args(["config", "--show"])
        assert args.command == "config"


class TestParseArgsDiscover:
    """Tests for parse_args with discover command."""

    def test_discover_no_filters(self):
        """Discover with no filters uses defaults."""
        from src.infrastructure.cli.cas_cli import parse_args

        args = parse_args(["discover"])
        assert args.command == "discover"
        assert args.max_results == 100
        assert args.year_from is None
        assert args.sport is None

    def test_discover_with_year_from(self):
        """Discover with year-from filter."""
        from src.infrastructure.cli.cas_cli import parse_args

        args = parse_args(["discover", "--year-from", "2020"])
        assert args.year_from == 2020

    def test_discover_with_year_to(self):
        """Discover with year-to filter."""
        from src.infrastructure.cli.cas_cli import parse_args

        args = parse_args(["discover", "--year-to", "2024"])
        assert args.year_to == 2024

    def test_discover_with_sport(self):
        """Discover with sport filter."""
        from src.infrastructure.cli.cas_cli import parse_args

        args = parse_args(["discover", "--sport", "football"])
        assert args.sport == "football"

    def test_discover_with_matter(self):
        """Discover with matter filter."""
        from src.infrastructure.cli.cas_cli import parse_args

        args = parse_args(["discover", "--matter", "doping"])
        assert args.matter == "doping"

    def test_discover_with_max_results(self):
        """Discover with max-results."""
        from src.infrastructure.cli.cas_cli import parse_args

        args = parse_args(["discover", "--max-results", "50"])
        assert args.max_results == 50

    def test_discover_with_dry_run(self):
        """Discover with dry-run flag."""
        from src.infrastructure.cli.cas_cli import parse_args

        args = parse_args(["discover", "--dry-run"])
        assert args.dry_run is True

    def test_discover_with_no_headless(self):
        """Discover with no-headless flag."""
        from src.infrastructure.cli.cas_cli import parse_args

        args = parse_args(["discover", "--no-headless"])
        assert args.no_headless is True

    def test_discover_all_filters(self):
        """Discover with all filter options."""
        from src.infrastructure.cli.cas_cli import parse_args

        args = parse_args([
            "discover",
            "--year-from", "2020",
            "--year-to", "2024",
            "--sport", "football",
            "--matter", "doping",
            "--keyword", "test",
            "--max-results", "50",
        ])

        assert args.year_from == 2020
        assert args.year_to == 2024
        assert args.sport == "football"
        assert args.matter == "doping"
        assert args.keyword == "test"
        assert args.max_results == 50


class TestParseArgsOtherCommands:
    """Tests for parse_args with other commands."""

    def test_status_with_session_id(self):
        """Status with optional session-id."""
        from src.infrastructure.cli.cas_cli import parse_args

        args = parse_args(["status", "--session-id", "abc123"])
        assert args.session_id == "abc123"

    def test_resume_requires_session_id(self):
        """Resume command requires session-id."""
        from src.infrastructure.cli.cas_cli import parse_args

        with pytest.raises(SystemExit):
            parse_args(["resume"])  # Missing required --session-id

    def test_list_checkpoints_default_limit(self):
        """list-checkpoints has default limit."""
        from src.infrastructure.cli.cas_cli import parse_args

        args = parse_args(["list-checkpoints"])
        assert args.limit == 10

    def test_list_checkpoints_custom_limit(self):
        """list-checkpoints with custom limit."""
        from src.infrastructure.cli.cas_cli import parse_args

        args = parse_args(["list-checkpoints", "--limit", "5"])
        assert args.limit == 5


class TestParseArgsGlobalFlags:
    """Tests for global CLI flags."""

    def test_verbose_flag(self):
        """Verbose flag is parsed."""
        from src.infrastructure.cli.cas_cli import parse_args

        args = parse_args(["-v", "discover"])
        assert args.verbose is True

    def test_quiet_flag(self):
        """Quiet flag is parsed."""
        from src.infrastructure.cli.cas_cli import parse_args

        args = parse_args(["-q", "discover"])
        assert args.quiet is True


class TestSetupLogging:
    """Tests for setup_logging function."""

    def _cleanup_logger(self, logger: logging.Logger) -> None:
        """Close and remove all handlers from logger."""
        for handler in logger.handlers[:]:
            handler.close()
            logger.removeHandler(handler)

    def test_returns_logger(self):
        """setup_logging returns a logger."""
        from src.infrastructure.cli.cas_cli import setup_logging

        with tempfile.TemporaryDirectory() as temp_dir:
            logger = setup_logging(log_dir=temp_dir)
            try:
                assert isinstance(logger, logging.Logger)
            finally:
                self._cleanup_logger(logger)

    def test_verbose_sets_debug_level(self):
        """Verbose mode sets DEBUG level."""
        from src.infrastructure.cli.cas_cli import setup_logging

        with tempfile.TemporaryDirectory() as temp_dir:
            logger = setup_logging(verbose=True, log_dir=temp_dir)
            try:
                assert logger.level == logging.DEBUG
            finally:
                self._cleanup_logger(logger)

    def test_quiet_sets_warning_level(self):
        """Quiet mode sets WARNING level."""
        from src.infrastructure.cli.cas_cli import setup_logging

        with tempfile.TemporaryDirectory() as temp_dir:
            logger = setup_logging(quiet=True, log_dir=temp_dir)
            try:
                assert logger.level == logging.WARNING
            finally:
                self._cleanup_logger(logger)

    def test_default_is_info_level(self):
        """Default logging level is INFO."""
        from src.infrastructure.cli.cas_cli import setup_logging

        with tempfile.TemporaryDirectory() as temp_dir:
            logger = setup_logging(log_dir=temp_dir)
            try:
                assert logger.level == logging.INFO
            finally:
                self._cleanup_logger(logger)


class TestMappers:
    """Tests for sport and matter mapping functions."""

    def test_map_sport_football(self):
        """Map football to CategoriaDeporte.FUTBOL."""
        from src.infrastructure.cli.cas_cli import _map_sport_to_categoria
        from src.domain.cas_value_objects import CategoriaDeporte

        result = _map_sport_to_categoria("football")
        assert result == CategoriaDeporte.FUTBOL

    def test_map_sport_cycling(self):
        """Map cycling to CategoriaDeporte.CICLISMO."""
        from src.infrastructure.cli.cas_cli import _map_sport_to_categoria
        from src.domain.cas_value_objects import CategoriaDeporte

        result = _map_sport_to_categoria("cycling")
        assert result == CategoriaDeporte.CICLISMO

    def test_map_sport_unknown_returns_none(self):
        """Unknown sport returns None."""
        from src.infrastructure.cli.cas_cli import _map_sport_to_categoria

        result = _map_sport_to_categoria("unknown_sport")
        assert result is None

    def test_map_matter_doping(self):
        """Map doping to TipoMateria.DOPAJE."""
        from src.infrastructure.cli.cas_cli import _map_matter_to_tipo
        from src.domain.cas_value_objects import TipoMateria

        result = _map_matter_to_tipo("doping")
        assert result == TipoMateria.DOPAJE

    def test_map_matter_transfer(self):
        """Map transfer to TipoMateria.TRANSFERENCIA."""
        from src.infrastructure.cli.cas_cli import _map_matter_to_tipo
        from src.domain.cas_value_objects import TipoMateria

        result = _map_matter_to_tipo("transfer")
        assert result == TipoMateria.TRANSFERENCIA

    def test_map_matter_unknown_returns_none(self):
        """Unknown matter returns None."""
        from src.infrastructure.cli.cas_cli import _map_matter_to_tipo

        result = _map_matter_to_tipo("unknown_matter")
        assert result is None
