"""
Tests for CAS Live Validator Script.

Following RED-GREEN TDD: Tests for the validation report classes.
Note: Actual live validation tests require network access.

Target: ~10 tests
"""
import pytest
import json
from datetime import datetime


class TestValidationResult:
    """Tests for ValidationResult dataclass."""

    def test_default_values(self):
        """ValidationResult has sensible defaults."""
        from scripts.validate_cas_live import ValidationResult

        result = ValidationResult(
            test_name="test",
            passed=True,
            duration_ms=100.0,
        )

        assert result.test_name == "test"
        assert result.passed is True
        assert result.duration_ms == 100.0
        assert result.message == ""
        assert result.details == {}
        assert result.error is None

    def test_with_all_values(self):
        """ValidationResult accepts all values."""
        from scripts.validate_cas_live import ValidationResult

        result = ValidationResult(
            test_name="homepage",
            passed=False,
            duration_ms=250.5,
            message="Failed to load",
            details={"status_code": 500},
            error="Connection timeout",
        )

        assert result.test_name == "homepage"
        assert result.passed is False
        assert result.duration_ms == 250.5
        assert result.message == "Failed to load"
        assert result.details == {"status_code": 500}
        assert result.error == "Connection timeout"


class TestValidationReport:
    """Tests for ValidationReport dataclass."""

    def test_success_rate_calculation(self):
        """success_rate calculates percentage correctly."""
        from scripts.validate_cas_live import ValidationReport

        report = ValidationReport(
            timestamp="2024-01-01T12:00:00",
            total_tests=10,
            passed_tests=8,
            failed_tests=2,
            duration_seconds=5.5,
        )

        assert report.success_rate == 80.0

    def test_success_rate_zero_tests(self):
        """success_rate handles zero tests."""
        from scripts.validate_cas_live import ValidationReport

        report = ValidationReport(
            timestamp="2024-01-01T12:00:00",
            total_tests=0,
            passed_tests=0,
            failed_tests=0,
            duration_seconds=0.0,
        )

        assert report.success_rate == 0.0

    def test_to_dict_includes_all_fields(self):
        """to_dict includes all report fields."""
        from scripts.validate_cas_live import ValidationReport, ValidationResult

        result = ValidationResult(
            test_name="test1",
            passed=True,
            duration_ms=100.0,
        )

        report = ValidationReport(
            timestamp="2024-01-01T12:00:00",
            total_tests=1,
            passed_tests=1,
            failed_tests=0,
            duration_seconds=1.0,
            results=[result],
        )

        data = report.to_dict()

        assert data["timestamp"] == "2024-01-01T12:00:00"
        assert data["total_tests"] == 1
        assert data["passed_tests"] == 1
        assert data["failed_tests"] == 0
        assert data["success_rate"] == "100.0%"
        assert len(data["results"]) == 1

    def test_to_json_is_valid_json(self):
        """to_json returns valid JSON string."""
        from scripts.validate_cas_live import ValidationReport

        report = ValidationReport(
            timestamp="2024-01-01T12:00:00",
            total_tests=5,
            passed_tests=4,
            failed_tests=1,
            duration_seconds=2.5,
        )

        json_str = report.to_json()
        data = json.loads(json_str)  # Should not raise

        assert data["total_tests"] == 5


class TestCASLiveValidator:
    """Tests for CASLiveValidator class."""

    def test_initializes_with_defaults(self):
        """CASLiveValidator initializes with defaults."""
        from scripts.validate_cas_live import CASLiveValidator

        validator = CASLiveValidator()
        assert validator._verbose is False
        assert validator._results == []

    def test_initializes_with_verbose(self):
        """CASLiveValidator accepts verbose flag."""
        from scripts.validate_cas_live import CASLiveValidator

        validator = CASLiveValidator(verbose=True)
        assert validator._verbose is True

    def test_add_result_stores_result(self):
        """_add_result stores results in list."""
        from scripts.validate_cas_live import CASLiveValidator, ValidationResult

        validator = CASLiveValidator()
        result = ValidationResult(
            test_name="test",
            passed=True,
            duration_ms=100.0,
        )

        validator._add_result(result)

        assert len(validator._results) == 1
        assert validator._results[0] == result

    def test_cas_base_url_is_defined(self):
        """CAS_BASE_URL is defined correctly."""
        from scripts.validate_cas_live import CASLiveValidator

        assert CASLiveValidator.CAS_BASE_URL == "https://jurisprudence.tas-cas.org/"
