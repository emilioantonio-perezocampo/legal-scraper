"""
RED Phase Tests: Web API Endpoints

Tests for the FastAPI web server that provides REST API for the GUI.
"""
import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from httpx import ASGITransport, AsyncClient

from src.gui.web.api import create_app, ScraperAPI
from src.gui.domain.entities import (
    ScraperJob,
    JobStatus,
    JobProgress,
    ScraperConfiguration,
    LogLevel,
)
from src.gui.domain.value_objects import (
    TargetSource,
    OutputFormat,
    ScraperMode,
)


class MockUseCaseResult:
    """Simple mock result class."""
    def __init__(self, success=True, error=None, job_id=None, status="idle", progress=None):
        self.success = success
        self.error = error
        self.job_id = job_id
        self.status = status
        self.progress = progress


class StubService:
    """Lightweight service stub for API lifecycle tests."""
    def __init__(self):
        self.is_running = False

    async def start(self):
        self.is_running = True

    async def stop(self):
        self.is_running = False


class TestWebAPIEndpoints:
    """Tests for REST API endpoints."""
    pytestmark = pytest.mark.asyncio

    @pytest.fixture
    def mock_service(self):
        """Create a mock scraper service."""
        service = MagicMock()
        service.is_running = True
        # Setup state_actor mock for logs endpoint
        service.state_actor = MagicMock()
        service.state_actor.ask = AsyncMock(return_value=MagicMock(
            current_job=None
        ))
        # Setup default async returns
        service.get_status = AsyncMock(return_value=MockUseCaseResult(status="idle"))
        service.start_scraping = AsyncMock(return_value=MockUseCaseResult(success=True, job_id="test-123"))
        service.pause_scraping = AsyncMock(return_value=MockUseCaseResult(success=True))
        service.resume_scraping = AsyncMock(return_value=MockUseCaseResult(success=True))
        service.cancel_scraping = AsyncMock(return_value=MockUseCaseResult(success=True))
        return service

    @pytest.fixture
    def app(self, mock_service, monkeypatch):
        """Create test application."""
        monkeypatch.setenv("SCRAPER_AUTH_DISABLED", "true")
        return create_app(service=mock_service)

    @pytest_asyncio.fixture
    async def client(self, app):
        """Create async test client."""
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            yield client

    async def test_health_check(self, client):
        """GET /health should return service status."""
        response = await client.get("/api/health")
        assert response.status_code == 200
        data = response.json()
        assert "status" in data
        assert data["status"] == "healthy"

    async def test_get_status_idle(self, client, mock_service):
        """GET /api/status should return idle when no job."""
        mock_service.get_status = AsyncMock(return_value=MockUseCaseResult(
            status="idle",
            job_id=None,
            progress=None
        ))

        response = await client.get("/api/status")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "idle"

    async def test_get_status_running(self, client, mock_service):
        """GET /api/status should return running job info."""
        mock_service.get_status = AsyncMock(return_value=MockUseCaseResult(
            status="running",
            job_id="test-123",
            progress=JobProgress(
                total_items=100,
                processed_items=50,
                successful_items=48,
                failed_items=2
            )
        ))

        response = await client.get("/api/status")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "running"
        assert data["job_id"] == "test-123"
        assert data["progress"]["percentage"] == 50.0

    async def test_start_scraping_today(self, client, mock_service):
        """POST /api/start should start a TODAY scraping job."""
        mock_service.start_scraping = AsyncMock(return_value=MockUseCaseResult(
            success=True,
            job_id="new-job-123"
        ))

        response = await client.post("/api/start", json={
            "source": "dof",
            "mode": "today",
            "output_directory": "scraped_data"
        })

        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert data["job_id"] == "new-job-123"

    async def test_start_scraping_date_range(self, client, mock_service):
        """POST /api/start should start a DATE_RANGE job."""
        mock_service.start_scraping = AsyncMock(return_value=MockUseCaseResult(
            success=True,
            job_id="range-job-123"
        ))

        response = await client.post("/api/start", json={
            "source": "dof",
            "mode": "range",
            "output_directory": "scraped_data",
            "start_date": "2021-03-01",
            "end_date": "2021-03-05"
        })

        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True

    async def test_start_scraping_validation_error(self, client, mock_service):
        """POST /api/start should return error for invalid config."""
        response = await client.post("/api/start", json={
            "source": "dof",
            "mode": "range",
            "output_directory": "scraped_data"
            # Missing start_date and end_date for range mode
        })

        assert response.status_code == 400

    async def test_start_scraping_already_running(self, client, mock_service):
        """POST /api/start should fail if job already running."""
        mock_service.start_scraping = AsyncMock(return_value=MockUseCaseResult(
            success=False,
            error="A job is already running"
        ))

        response = await client.post("/api/start", json={
            "source": "dof",
            "mode": "today",
            "output_directory": "scraped_data"
        })

        assert response.status_code == 200
        data = response.json()
        assert data["success"] is False
        assert "already running" in data["error"]

    async def test_pause_job(self, client, mock_service):
        """POST /api/pause should pause running job."""
        mock_service.pause_scraping = AsyncMock(return_value=MockUseCaseResult(success=True))

        response = await client.post("/api/pause")
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True

    async def test_resume_job(self, client, mock_service):
        """POST /api/resume should resume paused job."""
        mock_service.resume_scraping = AsyncMock(return_value=MockUseCaseResult(success=True))

        response = await client.post("/api/resume")
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True

    async def test_cancel_job(self, client, mock_service):
        """POST /api/cancel should cancel job."""
        mock_service.cancel_scraping = AsyncMock(return_value=MockUseCaseResult(success=True))

        response = await client.post("/api/cancel")
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True

    async def test_get_logs(self, client, mock_service):
        """GET /api/logs should return job logs."""
        mock_service.state_actor.ask.return_value = MagicMock(
            current_job=MagicMock(
                logs=(
                    MagicMock(
                        level=LogLevel.INFO,
                        message="Test log",
                        source="Test",
                        timestamp=MagicMock(isoformat=lambda: "2021-03-01T10:00:00")
                    ),
                )
            )
        )

        response = await client.get("/api/logs")
        assert response.status_code == 200
        data = response.json()
        assert "logs" in data

    async def test_get_configuration_options(self, client):
        """GET /api/config/options should return available options."""
        response = await client.get("/api/config/options")
        assert response.status_code == 200
        data = response.json()
        assert "sources" in data
        assert "modes" in data
        assert "dof" in [s["value"] for s in data["sources"]]


class TestWebAPISSE:
    """Tests for Server-Sent Events (real-time updates)."""

    def test_sse_endpoint_documentation(self, monkeypatch):
        """SSE endpoint is documented and available."""
        # SSE testing with sync client is problematic due to streaming nature
        # This test documents that the endpoint exists
        # Full integration testing requires async client or manual testing
        monkeypatch.setenv("SCRAPER_AUTH_DISABLED", "true")
        app = create_app(service=AsyncMock(is_running=True, state_actor=AsyncMock()))
        routes = [route.path for route in app.routes]
        assert "/api/events" in routes


class TestScraperAPIClass:
    """Tests for the ScraperAPI orchestrator class."""

    @pytest.fixture
    def stub_service(self):
        """Provide a lightweight service stub."""
        return StubService()

    @pytest.fixture
    def pipeline_mocks(self):
        """Patch pipeline/bridge startup to avoid heavy initialization."""
        with (
            patch(
                "src.gui.web.api.create_pipeline",
                new=AsyncMock(return_value=MagicMock())
            ),
            patch(
                "src.gui.web.api.stop_pipeline",
                new=AsyncMock()
            ),
            patch("src.gui.web.api.SCJNGuiBridgeActor") as scjn_bridge_cls,
            patch("src.gui.web.api.BJVGuiBridgeActor") as bjv_bridge_cls,
        ):
            scjn_bridge = MagicMock()
            scjn_bridge.start = AsyncMock()
            scjn_bridge.stop = AsyncMock()
            scjn_bridge_cls.return_value = scjn_bridge

            bjv_bridge = MagicMock()
            bjv_bridge.start = AsyncMock()
            bjv_bridge.stop = AsyncMock()
            bjv_bridge_cls.return_value = bjv_bridge
            yield

    @pytest.mark.asyncio
    async def test_api_initialization(self, stub_service):
        """ScraperAPI should initialize service on startup."""
        api = ScraperAPI(service=stub_service)
        assert api.service is not None

    @pytest.mark.asyncio
    async def test_api_lifecycle(self, stub_service, pipeline_mocks):
        """ScraperAPI should manage service lifecycle."""
        api = ScraperAPI(service=stub_service)
        await api.startup()
        assert api.service.is_running is True

        await api.shutdown()
        assert api.service.is_running is False
