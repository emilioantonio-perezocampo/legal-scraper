"""
Tests for BJV CLI argument parser.

Following RED-GREEN TDD: Tests define expected CLI interface.

Target: ~12 tests
"""
import pytest


class TestCreateParser:
    """Tests for parser creation."""

    def test_parser_created(self):
        """Parser is created successfully."""
        from src.bjv_main import create_parser

        parser = create_parser()

        assert parser is not None
        assert parser.prog == "bjv_scraper"

    def test_version_flag(self):
        """--version flag shows version."""
        from src.bjv_main import create_parser

        parser = create_parser()

        with pytest.raises(SystemExit) as exc_info:
            parser.parse_args(["--version"])

        assert exc_info.value.code == 0

    def test_json_flag(self):
        """--json flag is parsed."""
        from src.bjv_main import create_parser

        parser = create_parser()
        args = parser.parse_args(["--json", "discover"])

        assert args.json is True

    def test_verbose_flag(self):
        """--verbose flag is parsed."""
        from src.bjv_main import create_parser

        parser = create_parser()
        args = parser.parse_args(["-v", "discover"])

        assert args.verbose is True


class TestDiscoverCommand:
    """Tests for discover command parsing."""

    def test_discover_defaults(self):
        """Discover command has sensible defaults."""
        from src.bjv_main import create_parser

        parser = create_parser()
        args = parser.parse_args(["discover"])

        assert args.command == "discover"
        assert args.max_results == 50
        assert args.query is None
        assert args.area is None

    def test_discover_with_query(self):
        """Discover parses query argument."""
        from src.bjv_main import create_parser

        parser = create_parser()
        args = parser.parse_args(["discover", "-q", "derecho civil"])

        assert args.query == "derecho civil"

    def test_discover_with_area(self):
        """Discover parses area filter."""
        from src.bjv_main import create_parser

        parser = create_parser()
        args = parser.parse_args(["discover", "--area", "Derecho Penal"])

        assert args.area == "Derecho Penal"

    def test_discover_with_year_range(self):
        """Discover parses year range."""
        from src.bjv_main import create_parser

        parser = create_parser()
        args = parser.parse_args([
            "discover",
            "--year-from", "2020",
            "--year-to", "2023",
        ])

        assert args.year_from == 2020
        assert args.year_to == 2023

    def test_discover_max_results(self):
        """Discover parses max results."""
        from src.bjv_main import create_parser

        parser = create_parser()
        args = parser.parse_args(["discover", "--max-results", "100"])

        assert args.max_results == 100


class TestScrapeCommand:
    """Tests for scrape command parsing."""

    def test_scrape_defaults(self):
        """Scrape command has sensible defaults."""
        from src.bjv_main import create_parser

        parser = create_parser()
        args = parser.parse_args(["scrape"])

        assert args.command == "scrape"
        assert args.max_results == 20
        assert args.output_dir == "bjv_data"
        assert args.rate_limit == 0.5
        assert args.concurrent == 3

    def test_scrape_output_dir(self):
        """Scrape parses output directory."""
        from src.bjv_main import create_parser

        parser = create_parser()
        args = parser.parse_args(["scrape", "-o", "./custom_output"])

        assert args.output_dir == "./custom_output"

    def test_scrape_rate_limit(self):
        """Scrape parses rate limit."""
        from src.bjv_main import create_parser

        parser = create_parser()
        args = parser.parse_args(["scrape", "--rate-limit", "1.0"])

        assert args.rate_limit == 1.0

    def test_scrape_concurrent(self):
        """Scrape parses concurrent limit."""
        from src.bjv_main import create_parser

        parser = create_parser()
        args = parser.parse_args(["scrape", "--concurrent", "5"])

        assert args.concurrent == 5

    def test_scrape_skip_embeddings(self):
        """Scrape parses skip embeddings flag."""
        from src.bjv_main import create_parser

        parser = create_parser()
        args = parser.parse_args(["scrape", "--skip-embeddings"])

        assert args.skip_embeddings is True


class TestStatusCommand:
    """Tests for status command parsing."""

    def test_status_defaults(self):
        """Status command has sensible defaults."""
        from src.bjv_main import create_parser

        parser = create_parser()
        args = parser.parse_args(["status"])

        assert args.command == "status"
        assert args.session_id is None
        assert args.output_dir == "bjv_data"

    def test_status_session_id(self):
        """Status parses session ID."""
        from src.bjv_main import create_parser

        parser = create_parser()
        args = parser.parse_args(["status", "--session-id", "abc123"])

        assert args.session_id == "abc123"


class TestResumeCommand:
    """Tests for resume command parsing."""

    def test_resume_requires_session(self):
        """Resume requires session ID."""
        from src.bjv_main import create_parser

        parser = create_parser()

        with pytest.raises(SystemExit):
            parser.parse_args(["resume"])

    def test_resume_output_dir(self):
        """Resume parses output directory."""
        from src.bjv_main import create_parser

        parser = create_parser()
        args = parser.parse_args([
            "resume",
            "--session-id", "abc123",
            "-o", "./data",
        ])

        assert args.session_id == "abc123"
        assert args.output_dir == "./data"
