"""
Tests for SCJN live validation script.

These tests use mocked HTTP responses to test the validation logic
without hitting the real SCJN website.
"""
import pytest
import asyncio
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch
from aiohttp import ClientError

from scripts.validate_scjn_live import (
    SCJNLiveValidator,
    ValidationMode,
    ValidationResult,
    ValidationReport,
)
from src.infrastructure.adapters.scjn_search_parser import (
    parse_search_results,
    extract_pagination_info,
)
from src.infrastructure.adapters.scjn_document_parser import (
    parse_document_detail,
)


class TestValidationResult:
    """Tests for ValidationResult dataclass."""

    def test_validation_result_creation(self):
        """ValidationResult should be creatable with required fields."""
        result = ValidationResult(
            check_name="test_check",
            passed=True,
            message="Test passed",
        )

        assert result.check_name == "test_check"
        assert result.passed is True
        assert result.message == "Test passed"
        assert result.details is None

    def test_validation_result_with_details(self):
        """ValidationResult should accept optional details."""
        result = ValidationResult(
            check_name="test_check",
            passed=False,
            message="Test failed",
            details={"error_code": 500},
        )

        assert result.details == {"error_code": 500}

    def test_validation_result_is_frozen(self):
        """ValidationResult should be immutable."""
        result = ValidationResult(
            check_name="test",
            passed=True,
            message="ok",
        )

        with pytest.raises(Exception):  # FrozenInstanceError
            result.passed = False


class TestValidationReport:
    """Tests for ValidationReport class."""

    def test_report_creation(self):
        """ValidationReport should initialize with default values."""
        report = ValidationReport(
            mode=ValidationMode.SEARCH,
            started_at=datetime.now(),
        )

        assert report.mode == ValidationMode.SEARCH
        assert report.completed_at is None
        assert report.results == []
        assert report.errors == []

    def test_report_add_result(self):
        """Report should track added results."""
        report = ValidationReport(
            mode=ValidationMode.SEARCH,
            started_at=datetime.now(),
        )

        report.add_result(ValidationResult("check1", True, "ok"))
        report.add_result(ValidationResult("check2", False, "fail"))

        assert len(report.results) == 2
        assert report.passed_count == 1
        assert report.failed_count == 1

    def test_report_success_rate(self):
        """Report should calculate success rate correctly."""
        report = ValidationReport(
            mode=ValidationMode.FULL,
            started_at=datetime.now(),
        )

        report.add_result(ValidationResult("c1", True, "ok"))
        report.add_result(ValidationResult("c2", True, "ok"))
        report.add_result(ValidationResult("c3", False, "fail"))
        report.add_result(ValidationResult("c4", True, "ok"))

        assert report.success_rate == 0.75

    def test_report_success_rate_empty(self):
        """Empty report should have 0% success rate."""
        report = ValidationReport(
            mode=ValidationMode.SEARCH,
            started_at=datetime.now(),
        )

        assert report.success_rate == 0.0

    def test_report_add_error(self):
        """Report should track errors."""
        report = ValidationReport(
            mode=ValidationMode.DETAIL,
            started_at=datetime.now(),
        )

        report.add_error("Connection timeout")
        report.add_error("Parse error")

        assert len(report.errors) == 2
        assert "Connection timeout" in report.errors

    def test_report_to_dict(self):
        """Report should serialize to dictionary."""
        start = datetime(2024, 1, 15, 10, 0, 0)
        end = datetime(2024, 1, 15, 10, 0, 30)

        report = ValidationReport(
            mode=ValidationMode.SEARCH,
            started_at=start,
        )
        report.completed_at = end
        report.add_result(ValidationResult("check1", True, "ok"))

        data = report.to_dict()

        assert data["mode"] == "search"
        assert data["started_at"] == start.isoformat()
        assert data["completed_at"] == end.isoformat()
        assert data["passed_count"] == 1
        assert data["failed_count"] == 0
        assert len(data["results"]) == 1


class TestValidationMode:
    """Tests for ValidationMode enum."""

    def test_search_mode(self):
        """SEARCH mode should have correct value."""
        assert ValidationMode.SEARCH.value == "search"

    def test_detail_mode(self):
        """DETAIL mode should have correct value."""
        assert ValidationMode.DETAIL.value == "detail"

    def test_full_mode(self):
        """FULL mode should have correct value."""
        assert ValidationMode.FULL.value == "full"


class TestSCJNLiveValidator:
    """Tests for SCJNLiveValidator class."""

    def test_validator_creation(self):
        """Validator should initialize with default settings."""
        validator = SCJNLiveValidator()

        assert validator.rate_limit_seconds == 2.0
        assert validator.timeout_seconds == 30.0

    def test_validator_custom_rate_limit(self):
        """Validator should accept custom rate limit."""
        validator = SCJNLiveValidator(rate_limit_seconds=5.0)

        assert validator.rate_limit_seconds == 5.0

    def test_validator_custom_timeout(self):
        """Validator should accept custom timeout."""
        validator = SCJNLiveValidator(timeout_seconds=60.0)

        assert validator.timeout_seconds == 60.0


class TestValidatorSearchValidation:
    """Tests for search page validation."""

    @pytest.fixture
    def mock_search_html(self):
        """Mock search HTML response."""
        return '''
        <html>
        <body>
        <div id="gridResultados">
            <table class="dxgvTable">
                <tr class="dxgvDataRow">
                    <td><a href="wfOrdenamientoDetalle.aspx?q=ABC123">Ley Federal del Trabajo</a></td>
                    <td>01/04/1970</td>
                    <td>01/04/1970</td>
                    <td>VIGENTE</td>
                    <td>LEY FEDERAL</td>
                    <td>FEDERAL</td>
                </tr>
            </table>
            <div class="dxpPagerTotal">Página 1 de 10</div>
        </div>
        </body>
        </html>
        '''

    @pytest.mark.asyncio
    async def test_validate_search_success(self, mock_search_html):
        """Successful search validation should pass all checks."""
        validator = SCJNLiveValidator(rate_limit_seconds=0)

        mock_response = AsyncMock()
        mock_response.text = AsyncMock(return_value=mock_search_html)
        mock_response.status = 200
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_response)

        results = await validator.validate_search_page(mock_session)

        # Should have multiple passing checks
        assert len(results) >= 3
        assert all(r.passed for r in results)

    @pytest.mark.asyncio
    async def test_validate_search_http_error(self):
        """Search validation should handle HTTP errors."""
        validator = SCJNLiveValidator(rate_limit_seconds=0)

        mock_response = AsyncMock()
        mock_response.text = AsyncMock(return_value="Error")
        mock_response.status = 500
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_response)

        results = await validator.validate_search_page(mock_session)

        # HTTP status check should fail
        http_check = next(r for r in results if r.check_name == "search_http_status")
        assert not http_check.passed

    @pytest.mark.asyncio
    async def test_validate_search_connection_error(self):
        """Search validation should handle connection errors."""
        validator = SCJNLiveValidator(rate_limit_seconds=0)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(side_effect=ClientError("Connection failed"))

        results = await validator.validate_search_page(mock_session)

        assert len(results) >= 1
        assert not results[0].passed
        assert "Connection error" in results[0].message


class TestValidatorDetailValidation:
    """Tests for detail page validation."""

    @pytest.fixture
    def mock_detail_html(self):
        """Mock detail HTML response."""
        return '''
        <html>
        <body>
        <div id="contenedor">
            <h1 class="titulo-ordenamiento">LEY FEDERAL DEL TRABAJO</h1>
            <div class="datos-ordenamiento">
                <div class="dato">
                    <span class="etiqueta">Tipo de Ordenamiento:</span>
                    <span class="valor">LEY FEDERAL</span>
                </div>
                <div class="dato">
                    <span class="etiqueta">Ámbito:</span>
                    <span class="valor">FEDERAL</span>
                </div>
                <div class="dato">
                    <span class="etiqueta">Estatus:</span>
                    <span class="valor">VIGENTE</span>
                </div>
            </div>
        </div>
        </body>
        </html>
        '''

    @pytest.mark.asyncio
    async def test_validate_detail_success(self, mock_detail_html):
        """Successful detail validation should pass all checks."""
        validator = SCJNLiveValidator(rate_limit_seconds=0)

        mock_response = AsyncMock()
        mock_response.text = AsyncMock(return_value=mock_detail_html)
        mock_response.status = 200
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_response)

        results = await validator.validate_detail_page(mock_session, "ABC123")

        # Should have multiple passing checks
        assert len(results) >= 3
        http_check = next(r for r in results if r.check_name == "detail_http_status")
        assert http_check.passed

    @pytest.mark.asyncio
    async def test_validate_detail_not_found(self):
        """Detail validation should handle 404 errors."""
        validator = SCJNLiveValidator(rate_limit_seconds=0)

        mock_response = AsyncMock()
        mock_response.text = AsyncMock(return_value="Not found")
        mock_response.status = 404
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_response)

        results = await validator.validate_detail_page(mock_session, "INVALID")

        http_check = next(r for r in results if r.check_name == "detail_http_status")
        assert not http_check.passed


class TestValidatorRunValidation:
    """Tests for run_validation orchestration."""

    @pytest.mark.asyncio
    async def test_run_validation_search_mode(self):
        """run_validation should work in search mode."""
        validator = SCJNLiveValidator(rate_limit_seconds=0)

        mock_html = '<html><body><div id="gridResultados"><div class="dxpPagerTotal">Página 1 de 1</div></div></body></html>'

        with patch.object(validator, '_fetch_html', new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = (mock_html, 200)

            report = await validator.run_validation(ValidationMode.SEARCH)

        assert report.mode == ValidationMode.SEARCH
        assert report.completed_at is not None
        assert len(report.results) >= 1

    @pytest.mark.asyncio
    async def test_run_validation_detail_requires_q_param(self):
        """run_validation detail mode should require q_param."""
        validator = SCJNLiveValidator(rate_limit_seconds=0)

        report = await validator.run_validation(ValidationMode.DETAIL)

        assert len(report.errors) >= 1
        assert "q_param required" in report.errors[0]

    @pytest.mark.asyncio
    async def test_run_validation_detail_with_q_param(self):
        """run_validation detail mode should work with q_param."""
        validator = SCJNLiveValidator(rate_limit_seconds=0)

        mock_html = '''
        <html><body>
        <div id="contenedor">
            <h1 class="titulo-ordenamiento">Test Law</h1>
        </div>
        </body></html>
        '''

        with patch.object(validator, '_fetch_html', new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = (mock_html, 200)

            report = await validator.run_validation(
                ValidationMode.DETAIL,
                q_param="TEST123",
            )

        assert report.mode == ValidationMode.DETAIL
        assert len(report.errors) == 0


class TestValidatorRateLimiting:
    """Tests for rate limiting behavior."""

    @pytest.mark.asyncio
    async def test_rate_limit_respected(self):
        """Rate limiting should delay requests."""
        validator = SCJNLiveValidator(rate_limit_seconds=0.1)

        # Reset internal state
        validator._last_request_time = None

        start = asyncio.get_event_loop().time()

        await validator._rate_limit()
        await validator._rate_limit()

        elapsed = asyncio.get_event_loop().time() - start

        # Second call should have waited
        assert elapsed >= 0.1

    @pytest.mark.asyncio
    async def test_first_request_no_wait(self):
        """First request should not wait."""
        validator = SCJNLiveValidator(rate_limit_seconds=1.0)
        validator._last_request_time = None

        start = asyncio.get_event_loop().time()
        await validator._rate_limit()
        elapsed = asyncio.get_event_loop().time() - start

        # Should be nearly instant
        assert elapsed < 0.1
