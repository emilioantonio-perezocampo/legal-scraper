"""
Tests for CAS CLI Configuration

Following RED-GREEN TDD: These tests define the expected behavior
for the CAS CLI configuration classes.

Target: ~12 tests for CAS config
"""
import pytest
import os
import tempfile
from pathlib import Path


class TestCASCliConfigDefaults:
    """Tests for CASCliConfig default values."""

    def test_default_output_dir(self):
        """Default output_dir is 'cas_data'."""
        from src.infrastructure.cli.cas_config import CASCliConfig
        config = CASCliConfig()
        assert config.output_dir == "cas_data"

    def test_default_checkpoint_dir(self):
        """Default checkpoint_dir is 'cas_checkpoints'."""
        from src.infrastructure.cli.cas_config import CASCliConfig
        config = CASCliConfig()
        assert config.checkpoint_dir == "cas_checkpoints"

    def test_default_headless(self):
        """Default headless is True."""
        from src.infrastructure.cli.cas_config import CASCliConfig
        config = CASCliConfig()
        assert config.headless is True

    def test_default_max_retries(self):
        """Default max_retries is 3."""
        from src.infrastructure.cli.cas_config import CASCliConfig
        config = CASCliConfig()
        assert config.max_retries == 3

    def test_default_rate_limit_delay(self):
        """Default rate_limit_delay is 3.0."""
        from src.infrastructure.cli.cas_config import CASCliConfig
        config = CASCliConfig()
        assert config.rate_limit_delay == 3.0


class TestCASCliConfigFromEnv:
    """Tests for CASCliConfig.from_env()."""

    def test_reads_output_dir_from_env(self):
        """from_env() reads CAS_OUTPUT_DIR."""
        from src.infrastructure.cli.cas_config import CASCliConfig

        original = os.environ.get("CAS_OUTPUT_DIR")
        os.environ["CAS_OUTPUT_DIR"] = "test_output"

        try:
            config = CASCliConfig.from_env()
            assert config.output_dir == "test_output"
        finally:
            if original:
                os.environ["CAS_OUTPUT_DIR"] = original
            else:
                os.environ.pop("CAS_OUTPUT_DIR", None)

    def test_reads_headless_from_env(self):
        """from_env() reads CAS_HEADLESS."""
        from src.infrastructure.cli.cas_config import CASCliConfig

        original = os.environ.get("CAS_HEADLESS")
        os.environ["CAS_HEADLESS"] = "false"

        try:
            config = CASCliConfig.from_env()
            assert config.headless is False
        finally:
            if original:
                os.environ["CAS_HEADLESS"] = original
            else:
                os.environ.pop("CAS_HEADLESS", None)


class TestCASCliConfigMethods:
    """Tests for CASCliConfig methods."""

    def test_ensure_directories_creates_dirs(self):
        """ensure_directories() creates all required directories."""
        from src.infrastructure.cli.cas_config import CASCliConfig

        with tempfile.TemporaryDirectory() as temp_dir:
            config = CASCliConfig(
                output_dir=str(Path(temp_dir) / "output"),
                checkpoint_dir=str(Path(temp_dir) / "checkpoints"),
                log_dir=str(Path(temp_dir) / "logs"),
            )
            config.ensure_directories()

            assert Path(config.output_dir).exists()
            assert Path(config.checkpoint_dir).exists()
            assert Path(config.log_dir).exists()

    def test_to_dict_returns_dict(self):
        """to_dict() returns dictionary with all config values."""
        from src.infrastructure.cli.cas_config import CASCliConfig

        config = CASCliConfig()
        d = config.to_dict()

        assert isinstance(d, dict)
        assert "output_dir" in d
        assert "headless" in d
        assert "max_retries" in d


class TestSearchFilters:
    """Tests for SearchFilters dataclass."""

    def test_default_values(self):
        """Default values are set correctly."""
        from src.infrastructure.cli.cas_config import SearchFilters

        filters = SearchFilters()
        assert filters.year_from is None
        assert filters.year_to is None
        assert filters.sport is None
        assert filters.max_results == 100

    def test_to_dict_excludes_none(self):
        """to_dict() excludes None values."""
        from src.infrastructure.cli.cas_config import SearchFilters

        filters = SearchFilters(year_from=2020, max_results=50)
        d = filters.to_dict()

        assert "year_from" in d
        assert "max_results" in d
        assert "year_to" not in d
        assert "sport" not in d

    def test_describe_with_filters(self):
        """describe() returns human-readable description."""
        from src.infrastructure.cli.cas_config import SearchFilters

        filters = SearchFilters(
            year_from=2020,
            year_to=2024,
            sport="football",
            max_results=100,
        )
        desc = filters.describe()

        assert "2020" in desc
        assert "2024" in desc
        assert "football" in desc

    def test_describe_empty_filters(self):
        """describe() handles empty filters."""
        from src.infrastructure.cli.cas_config import SearchFilters

        filters = SearchFilters(max_results=100)
        desc = filters.describe()

        assert "máx: 100" in desc


class TestConstants:
    """Tests for module constants."""

    def test_available_sports_defined(self):
        """AVAILABLE_SPORTS is defined and not empty."""
        from src.infrastructure.cli.cas_config import AVAILABLE_SPORTS

        assert len(AVAILABLE_SPORTS) > 0
        assert "football" in AVAILABLE_SPORTS

    def test_available_matters_defined(self):
        """AVAILABLE_MATTERS is defined and not empty."""
        from src.infrastructure.cli.cas_config import AVAILABLE_MATTERS

        assert len(AVAILABLE_MATTERS) > 0
        assert "doping" in AVAILABLE_MATTERS

    def test_available_procedures_defined(self):
        """AVAILABLE_PROCEDURES is defined and not empty."""
        from src.infrastructure.cli.cas_config import AVAILABLE_PROCEDURES

        assert len(AVAILABLE_PROCEDURES) > 0
        assert "appeal" in AVAILABLE_PROCEDURES
