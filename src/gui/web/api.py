"""
Web API - FastAPI-based REST API for the Legal Scraper GUI

Provides endpoints for:
- Job control (start, pause, resume, cancel)
- Status and progress monitoring
- Log retrieval
- Configuration options
- Server-Sent Events for real-time updates
"""
import asyncio
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()
from datetime import date
from typing import Optional, List, Any, AsyncGenerator
from contextlib import asynccontextmanager
from pathlib import Path
from dataclasses import dataclass

from fastapi import Depends, FastAPI, HTTPException, Request, Response, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field, validator

from src.scjn_main import create_pipeline, stop_pipeline
from src.gui.domain.entities import (
    ScraperConfiguration,
    DateRange,
    JobProgress,
    LogLevel,
)
from src.gui.domain.value_objects import (
    TargetSource,
    OutputFormat,
    ScraperMode,
)
from src.gui.application.services import ScraperService, ConfigurationService
from src.gui.infrastructure.actors import (
    SCJNGuiBridgeActor,
    SCJNSearchConfig,
    SCJNProgress,
    SCJNJobStarted,
    SCJNJobProgress,
    SCJNJobCompleted,
    SCJNJobFailed,
    BJVGuiBridgeActor,
    BJVSearchConfig,
    BJVProgress,
    BJVJobStarted,
    BJVJobProgress,
    BJVJobCompleted,
    BJVJobFailed,
    CASGuiBridgeActor,
    CASGuiConfig,
    CASJobStarted,
    CASJobProgress,
    CASJobCompleted,
    CASJobError,
    DOFGuiBridgeActor,
    DOFGuiConfig,
    DOFJobStarted,
    DOFJobProgress,
    DOFJobCompleted,
    DOFJobError,
)
from src.domain.bjv_value_objects import AreaDerecho
from src.gui.web.auth import (
    AuthError,
    AuthSettings,
    create_access_token,
    decode_access_token,
    verify_password,
)


@dataclass
class ScraperArgs:
    """Helper class to mimic argparse args for create_pipeline."""
    output_dir: str = "scjn_data"
    checkpoint_dir: str = "checkpoints"
    rate_limit: float = 0.5
    concurrency: int = 3
    skip_pdfs: bool = True
    max_results: int = 100
    category: Optional[str] = None
    scope: Optional[str] = None
    status: Optional[str] = None
    all_pages: bool = False


auth_bearer = HTTPBearer(auto_error=False)


async def require_auth(
    request: Request,
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(auth_bearer),
) -> str:
    settings: AuthSettings = request.app.state.auth_settings
    if settings.disabled:
        return "anonymous"

    token = None
    if credentials:
        token = credentials.credentials
    else:
        cookie_token = request.cookies.get(settings.cookie_name)
        if cookie_token:
            token = cookie_token

    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated.",
        )

    try:
        payload = decode_access_token(token, settings)
    except AuthError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=str(exc),
        ) from exc

    return str(payload.get("sub", ""))


# Pydantic models for API request/response
class StartJobRequest(BaseModel):
    """Request model for starting a scraping job."""
    source: str = Field(default="dof", description="Target source to scrape")
    mode: str = Field(default="today", description="Scraping mode")
    output_directory: str = Field(default="scraped_data")
    start_date: Optional[str] = Field(default=None, description="Start date (YYYY-MM-DD)")
    end_date: Optional[str] = Field(default=None, description="End date (YYYY-MM-DD)")
    rate_limit: float = Field(default=2.0, description="Rate limit in seconds")

    @validator('start_date', 'end_date', pre=True)
    def parse_date(cls, v):
        if v is None or v == '':
            return None
        return v


class StartJobResponse(BaseModel):
    """Response model for start job."""
    success: bool
    job_id: Optional[str] = None
    error: Optional[str] = None


class StatusResponse(BaseModel):
    """Response model for status endpoint."""
    status: str
    job_id: Optional[str] = None
    progress: Optional[dict] = None
    source: Optional[str] = None
    mode: Optional[str] = None


class ActionResponse(BaseModel):
    """Response model for action endpoints (pause, resume, cancel)."""
    success: bool
    error: Optional[str] = None


class LogEntry(BaseModel):
    """Log entry model."""
    level: str
    message: str
    source: str
    timestamp: str


class LogsResponse(BaseModel):
    """Response model for logs endpoint."""
    logs: List[LogEntry]


class ConfigOption(BaseModel):
    """Configuration option model."""
    value: str
    label: str


class ConfigOptionsResponse(BaseModel):
    """Response model for config options."""
    sources: List[ConfigOption]
    modes: List[ConfigOption]
    formats: List[ConfigOption]


class HealthResponse(BaseModel):
    """Health check response."""
    status: str
    version: str = "1.0.0"


class LoginRequest(BaseModel):
    """Request model for login."""
    username: str
    password: str


class LoginResponse(BaseModel):
    """Response model for login."""
    success: bool
    access_token: Optional[str] = None
    token_type: str = "bearer"
    error: Optional[str] = None


# SCJN-specific request/response models
class SCJNStartRequest(BaseModel):
    """Request model for starting SCJN scraping job."""
    category: Optional[str] = Field(default=None, description="Legislation category filter")
    scope: Optional[str] = Field(default=None, description="Federal or State scope")
    max_results: int = Field(default=100, description="Maximum results to fetch")
    output_directory: str = Field(default="scjn_data")


class SCJNStatusResponse(BaseModel):
    """Response model for SCJN status endpoint."""
    status: str
    job_id: Optional[str] = None
    connected: bool = False
    progress: Optional[dict] = None


class SCJNProgressResponse(BaseModel):
    """Response model for SCJN progress endpoint."""
    discovered_count: int = 0
    downloaded_count: int = 0
    pending_count: int = 0
    active_downloads: int = 0
    error_count: int = 0
    state: str = "IDLE"


# BJV-specific request/response models
class BJVStartRequest(BaseModel):
    """Request model for starting BJV scraping job."""
    termino_busqueda: Optional[str] = Field(default=None, description="Search term")
    area_derecho: Optional[str] = Field(default=None, description="Legal area filter")
    max_resultados: int = Field(default=100, description="Maximum results to fetch")
    incluir_capitulos: bool = Field(default=True, description="Include chapter info")
    descargar_pdfs: bool = Field(default=True, description="Download PDF files")
    output_directory: str = Field(default="bjv_data")


class BJVStatusResponse(BaseModel):
    """Response model for BJV status endpoint."""
    status: str
    job_id: Optional[str] = None
    connected: bool = False
    progress: Optional[dict] = None


class BJVProgressResponse(BaseModel):
    """Response model for BJV progress endpoint."""
    libros_descubiertos: int = 0
    libros_descargados: int = 0
    libros_pendientes: int = 0
    descargas_activas: int = 0
    errores: int = 0
    estado: str = "IDLE"


# CAS-specific request/response models
class CASStartRequest(BaseModel):
    """Request model for starting CAS scraping job."""
    year_from: Optional[int] = Field(default=None, description="Start year filter")
    year_to: Optional[int] = Field(default=None, description="End year filter")
    sport: Optional[str] = Field(default=None, description="Sport filter")
    matter: Optional[str] = Field(default=None, description="Matter/subject filter")
    max_results: int = Field(default=100, ge=1, le=1000, description="Maximum results to fetch")


class CASStatusResponse(BaseModel):
    """Response model for CAS status endpoint."""
    status: str
    job_id: Optional[str] = None
    progress: Optional[dict] = None


class CASFiltersResponse(BaseModel):
    """Response model for CAS filters endpoint."""
    sports: List[str]
    matters: List[str]
    year_range: dict


# DOF-specific request/response models
class DOFStartRequest(BaseModel):
    """Request model for starting DOF scraping job."""
    mode: str = Field(default="today", description="Scraping mode: 'today' or 'range'")
    start_date: Optional[str] = Field(default=None, description="Start date (YYYY-MM-DD) for range mode")
    end_date: Optional[str] = Field(default=None, description="End date (YYYY-MM-DD) for range mode")
    section: Optional[str] = Field(default=None, description="DOF section filter")
    download_pdfs: bool = Field(default=True, description="Download PDF files")
    output_directory: str = Field(default="dof_data", description="Output directory")


class DOFStatusResponse(BaseModel):
    """Response model for DOF status endpoint."""
    status: str
    job_id: Optional[str] = None
    progress: Optional[dict] = None


class DOFSectionsResponse(BaseModel):
    """Response model for DOF sections endpoint."""
    sections: List[dict]
    modes: List[dict]


class ScraperAPI:
    """
    Orchestrator class for the Web API.

    Manages the scraper service lifecycle and provides
    methods for API endpoints.
    """

    def __init__(self, service: Optional[ScraperService] = None):
        self._service = service or ScraperService()
        self._config_service = ConfigurationService()
        self._event_subscribers: List[asyncio.Queue] = []
        self._scjn_bridge: Optional[SCJNGuiBridgeActor] = None
        self._scjn_coordinator = None
        self._scjn_logs: List[dict] = []
        self._bjv_bridge: Optional[BJVGuiBridgeActor] = None
        self._bjv_logs: List[dict] = []
        self._cas_bridge: Optional[CASGuiBridgeActor] = None
        self._cas_logs: List[dict] = []
        self._dof_bridge: Optional[DOFGuiBridgeActor] = None
        self._dof_logs: List[dict] = []

    @property
    def service(self) -> ScraperService:
        return self._service

    @property
    def scjn_bridge(self) -> Optional[SCJNGuiBridgeActor]:
        return self._scjn_bridge

    @property
    def bjv_bridge(self) -> Optional[BJVGuiBridgeActor]:
        return self._bjv_bridge

    @property
    def cas_bridge(self) -> Optional[CASGuiBridgeActor]:
        return self._cas_bridge

    @property
    def dof_bridge(self) -> Optional[DOFGuiBridgeActor]:
        return self._dof_bridge

    async def startup(self):
        """Start the scraper service and bridges."""
        await self._service.start()
        
        # Initialize SCJN pipeline
        args = ScraperArgs()
        try:
            self._scjn_coordinator = await create_pipeline(args)
            self._scjn_logs.append({
                "level": "info",
                "message": "SCJN pipeline initialized",
                "source": "System",
                "timestamp": __import__('datetime').datetime.now().isoformat()
            })
        except Exception as e:
            self._scjn_logs.append({
                "level": "error",
                "message": f"Failed to initialize SCJN pipeline: {e}",
                "source": "System",
                "timestamp": __import__('datetime').datetime.now().isoformat()
            })

        # Initialize SCJN bridge
        self._scjn_bridge = SCJNGuiBridgeActor(
            coordinator_actor=self._scjn_coordinator,
            event_handler=self._handle_scjn_event
        )
        await self._scjn_bridge.start()
        
        # Initialize BJV bridge
        self._bjv_bridge = BJVGuiBridgeActor(
            event_handler=self._handle_bjv_event
        )
        await self._bjv_bridge.start()

        # Initialize CAS bridge
        self._cas_bridge = CASGuiBridgeActor(
            on_event=self._handle_cas_event
        )
        # Note: CAS bridge doesn't require start() as it inherits from CASBaseActor
        self._cas_logs.append({
            "level": "info",
            "message": "CAS bridge initialized",
            "source": "System",
            "timestamp": __import__('datetime').datetime.now().isoformat()
        })

        # Initialize DOF bridge
        self._dof_bridge = DOFGuiBridgeActor(
            on_event=self._handle_dof_event
        )
        self._dof_logs.append({
            "level": "info",
            "message": "DOF bridge initialized",
            "source": "System",
            "timestamp": __import__('datetime').datetime.now().isoformat()
        })

    async def shutdown(self):
        """Stop the scraper service and bridges."""
        await self._service.stop()
        if self._scjn_bridge:
            await self._scjn_bridge.stop()
        if self._scjn_coordinator:
            await stop_pipeline(self._scjn_coordinator)
        if self._bjv_bridge:
            await self._bjv_bridge.stop()
        # CAS bridge cleanup (no explicit stop needed for CASBaseActor)
        self._cas_bridge = None
        # DOF bridge cleanup
        self._dof_bridge = None

    async def _handle_scjn_event(self, event):
        """Handle SCJN events for logging and broadcasting."""
        import json
        from datetime import datetime

        if isinstance(event, SCJNJobStarted):
            self._scjn_logs.append({
                "level": "info",
                "message": f"SCJN job started: {event.job_id}",
                "source": "SCJN",
                "timestamp": event.timestamp.isoformat()
            })
        elif isinstance(event, SCJNJobProgress):
            # Don't spam logs with progress updates
            pass
        elif isinstance(event, SCJNJobCompleted):
            self._scjn_logs.append({
                "level": "success",
                "message": f"SCJN job completed: {event.total_downloaded} documents",
                "source": "SCJN",
                "timestamp": event.timestamp.isoformat()
            })
        elif isinstance(event, SCJNJobFailed):
            self._scjn_logs.append({
                "level": "error",
                "message": f"SCJN job failed: {event.error_message}",
                "source": "SCJN",
                "timestamp": event.timestamp.isoformat()
            })

        # Broadcast to SSE subscribers
        await self.broadcast_event("scjn_event", {"type": type(event).__name__})

    async def _handle_bjv_event(self, event):
        """Handle BJV events for logging and broadcasting."""
        from datetime import datetime

        if isinstance(event, BJVJobStarted):
            self._bjv_logs.append({
                "level": "info",
                "message": f"BJV job started: {event.job_id}",
                "source": "BJV",
                "timestamp": event.timestamp.isoformat()
            })
        elif isinstance(event, BJVJobProgress):
            # Don't spam logs with progress updates
            pass
        elif isinstance(event, BJVJobCompleted):
            self._bjv_logs.append({
                "level": "success",
                "message": f"BJV job completed: {event.total_libros_descargados} libros",
                "source": "BJV",
                "timestamp": event.timestamp.isoformat()
            })
        elif isinstance(event, BJVJobFailed):
            self._bjv_logs.append({
                "level": "error",
                "message": f"BJV job failed: {event.mensaje_error}",
                "source": "BJV",
                "timestamp": event.timestamp.isoformat()
            })

        # Broadcast to SSE subscribers
        await self.broadcast_event("bjv_event", {"type": type(event).__name__})

    async def _handle_cas_event(self, event):
        """Handle CAS events for logging and broadcasting."""
        from datetime import datetime

        if isinstance(event, CASJobStarted):
            self._cas_logs.append({
                "level": "info",
                "message": f"CAS job started: {event.job_id}",
                "source": "CAS",
                "timestamp": event.timestamp
            })
        elif isinstance(event, CASJobProgress):
            # Don't spam logs with progress updates
            pass
        elif isinstance(event, CASJobCompleted):
            self._cas_logs.append({
                "level": "success",
                "message": f"CAS job completed: {event.job_id}",
                "source": "CAS",
                "timestamp": event.timestamp
            })
        elif isinstance(event, CASJobError):
            self._cas_logs.append({
                "level": "error",
                "message": f"CAS job error: {event.mensaje}",
                "source": "CAS",
                "timestamp": event.timestamp
            })

        # Broadcast to SSE subscribers
        await self.broadcast_event("cas_event", {"type": type(event).__name__})

    async def _handle_dof_event(self, event):
        """Handle DOF events for logging and broadcasting."""
        from datetime import datetime

        if isinstance(event, DOFJobStarted):
            self._dof_logs.append({
                "level": "info",
                "message": f"DOF job started: {event.job_id}",
                "source": "DOF",
                "timestamp": event.timestamp
            })
        elif isinstance(event, DOFJobProgress):
            # Don't spam logs with progress updates
            pass
        elif isinstance(event, DOFJobCompleted):
            self._dof_logs.append({
                "level": "success",
                "message": f"DOF job completed: {event.job_id}",
                "source": "DOF",
                "timestamp": event.timestamp
            })
        elif isinstance(event, DOFJobError):
            self._dof_logs.append({
                "level": "error",
                "message": f"DOF job error: {event.mensaje}",
                "source": "DOF",
                "timestamp": event.timestamp
            })

        # Broadcast to SSE subscribers
        await self.broadcast_event("dof_event", {"type": type(event).__name__})

    def add_event_subscriber(self, queue: asyncio.Queue):
        """Add a subscriber for real-time events."""
        self._event_subscribers.append(queue)

    def remove_event_subscriber(self, queue: asyncio.Queue):
        """Remove an event subscriber."""
        if queue in self._event_subscribers:
            self._event_subscribers.remove(queue)

    async def broadcast_event(self, event_type: str, data: dict):
        """Broadcast an event to all subscribers."""
        event = {"type": event_type, "data": data}
        for queue in self._event_subscribers:
            try:
                queue.put_nowait(event)
            except asyncio.QueueFull:
                pass  # Skip if queue is full


def create_app(service: Optional[ScraperService] = None) -> FastAPI:
    """
    Create and configure the FastAPI application.

    Args:
        service: Optional pre-configured scraper service (for testing)

    Returns:
        Configured FastAPI application
    """
    api = ScraperAPI(service=service)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        """Manage application lifecycle."""
        # Startup
        if service is None:  # Only start if not injected (testing)
            await api.startup()
        yield
        # Shutdown
        if service is None:
            await api.shutdown()

    app = FastAPI(
        title="Legal Scraper API",
        description="REST API for controlling the DOF legal document scraper",
        version="1.0.0",
        lifespan=lifespan
    )

    # Store API instance in app state
    app.state.api = api
    app.state.auth_settings = AuthSettings.from_env()

    # Setup templates
    templates_dir = Path(__file__).parent / "templates"
    if templates_dir.exists():
        templates = Jinja2Templates(directory=str(templates_dir))
    else:
        templates = None

    # Static files
    static_dir = Path(__file__).parent / "static"
    if static_dir.exists():
        app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

    # Download mounts
    scjn_data_dir = Path("scjn_data")
    scjn_data_dir.mkdir(parents=True, exist_ok=True)
    app.mount("/downloads/scjn", StaticFiles(directory=str(scjn_data_dir)), name="scjn_downloads")

    # ============ API Endpoints ============

    @app.get("/api/health", response_model=HealthResponse, tags=["Health"])
    async def health_check():
        """Check API health status."""
        return HealthResponse(
            status="healthy" if api.service.is_running else "unhealthy"
        )

    @app.post("/api/auth/login", response_model=LoginResponse, tags=["Auth"])
    async def login(request: LoginRequest, response: Response):
        """Exchange username/password for an access token."""
        settings = app.state.auth_settings
        if settings.disabled:
            return LoginResponse(success=True, access_token="disabled")

        if request.username != settings.username:
            return LoginResponse(success=False, error="Invalid credentials.")

        if not verify_password(request.password, settings.password_hash):
            return LoginResponse(success=False, error="Invalid credentials.")

        token = create_access_token(request.username, settings)
        response.set_cookie(
            settings.cookie_name,
            token,
            httponly=True,
            secure=settings.cookie_secure,
            samesite="lax",
        )
        return LoginResponse(success=True, access_token=token)

    @app.post("/api/auth/logout", tags=["Auth"])
    async def logout(response: Response):
        """Clear authentication cookie."""
        settings = app.state.auth_settings
        response.delete_cookie(settings.cookie_name)
        return {"success": True}

    @app.get(
        "/api/status",
        response_model=StatusResponse,
        tags=["Jobs"],
        dependencies=[Depends(require_auth)],
    )
    async def get_status():
        """Get current job status."""
        result = await api.service.get_status()

        response = StatusResponse(status=result.status)

        if result.job_id:
            response.job_id = result.job_id

        if result.progress:
            response.progress = {
                "total_items": result.progress.total_items,
                "processed_items": result.progress.processed_items,
                "successful_items": result.progress.successful_items,
                "failed_items": result.progress.failed_items,
                "percentage": result.progress.percentage
            }

        return response

    @app.post(
        "/api/start",
        response_model=StartJobResponse,
        tags=["Jobs"],
        dependencies=[Depends(require_auth)],
    )
    async def start_scraping(request: StartJobRequest):
        """Start a new scraping job."""
        try:
            # Parse source
            try:
                source = TargetSource(request.source)
            except ValueError:
                raise HTTPException(status_code=400, detail=f"Invalid source: {request.source}")

            # Parse mode
            try:
                mode = ScraperMode(request.mode)
            except ValueError:
                raise HTTPException(status_code=400, detail=f"Invalid mode: {request.mode}")

            # Build date range if needed
            date_range = None
            if mode in (ScraperMode.DATE_RANGE, ScraperMode.HISTORICAL):
                if not request.start_date or not request.end_date:
                    raise HTTPException(
                        status_code=400,
                        detail="start_date and end_date required for range mode"
                    )
                try:
                    start = date.fromisoformat(request.start_date)
                    end = date.fromisoformat(request.end_date)
                    date_range = DateRange(start=start, end=end)
                except ValueError as e:
                    raise HTTPException(status_code=400, detail=f"Invalid date format: {e}")

            # Create configuration
            config = ScraperConfiguration(
                target_source=source,
                mode=mode,
                output_format=OutputFormat.JSON,
                output_directory=request.output_directory,
                date_range=date_range,
                rate_limit_seconds=request.rate_limit
            )

            # Start scraping
            result = await api.service.start_scraping(config)

            return StartJobResponse(
                success=result.success,
                job_id=result.job_id,
                error=result.error
            )

        except HTTPException:
            raise
        except Exception as e:
            return StartJobResponse(success=False, error=str(e))

    @app.post(
        "/api/pause",
        response_model=ActionResponse,
        tags=["Jobs"],
        dependencies=[Depends(require_auth)],
    )
    async def pause_scraping():
        """Pause the current scraping job."""
        result = await api.service.pause_scraping()
        return ActionResponse(success=result.success, error=result.error)

    @app.post(
        "/api/resume",
        response_model=ActionResponse,
        tags=["Jobs"],
        dependencies=[Depends(require_auth)],
    )
    async def resume_scraping():
        """Resume a paused scraping job."""
        result = await api.service.resume_scraping()
        return ActionResponse(success=result.success, error=result.error)

    @app.post(
        "/api/cancel",
        response_model=ActionResponse,
        tags=["Jobs"],
        dependencies=[Depends(require_auth)],
    )
    async def cancel_scraping():
        """Cancel the current scraping job."""
        result = await api.service.cancel_scraping()
        return ActionResponse(success=result.success, error=result.error)

    @app.get(
        "/api/logs",
        response_model=LogsResponse,
        tags=["Logs"],
        dependencies=[Depends(require_auth)],
    )
    async def get_logs():
        """Get current job logs."""
        state = await api.service.state_actor.ask("GET_STATE")

        logs = []
        if state.current_job and state.current_job.logs:
            for log in state.current_job.logs:
                logs.append(LogEntry(
                    level=log.level.value,
                    message=log.message,
                    source=log.source,
                    timestamp=log.timestamp.isoformat()
                ))

        return LogsResponse(logs=logs)

    @app.get(
        "/api/config/options",
        response_model=ConfigOptionsResponse,
        tags=["Configuration"],
        dependencies=[Depends(require_auth)],
    )
    async def get_config_options():
        """Get available configuration options."""
        config_service = ConfigurationService()

        sources = [
            ConfigOption(value=s.value, label=s.display_name)
            for s in config_service.get_available_sources()
        ]

        modes = [
            ConfigOption(value=m.value, label=m.value.replace("_", " ").title())
            for m in config_service.get_available_modes()
        ]

        formats = [
            ConfigOption(value=f.value, label=f.value.upper())
            for f in config_service.get_available_formats()
        ]

        return ConfigOptionsResponse(
            sources=sources,
            modes=modes,
            formats=formats
        )

    @app.get(
        "/api/events",
        tags=["Events"],
        dependencies=[Depends(require_auth)],
    )
    async def event_stream():
        """Server-Sent Events stream for real-time updates."""
        async def generate() -> AsyncGenerator[str, None]:
            queue: asyncio.Queue = asyncio.Queue(maxsize=100)
            api.add_event_subscriber(queue)
            try:
                while True:
                    try:
                        event = await asyncio.wait_for(queue.get(), timeout=30)
                        yield f"data: {event}\n\n"
                    except asyncio.TimeoutError:
                        # Send keepalive
                        yield f": keepalive\n\n"
            finally:
                api.remove_event_subscriber(queue)

        return StreamingResponse(
            generate(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
            }
        )

    # ============ SCJN API Endpoints ============

    @app.get(
        "/api/scjn/status",
        response_model=SCJNStatusResponse,
        tags=["SCJN"],
        dependencies=[Depends(require_auth)],
    )
    async def get_scjn_status():
        """Get current SCJN job status."""
        if not api.scjn_bridge:
            return SCJNStatusResponse(status="not_initialized", connected=False)

        status = await api.scjn_bridge.ask("GET_STATUS")
        progress = await api.scjn_bridge.ask("GET_PROGRESS")

        progress_dict = None
        if progress:
            progress_dict = {
                "discovered_count": progress.discovered_count,
                "downloaded_count": progress.downloaded_count,
                "pending_count": progress.pending_count,
                "active_downloads": progress.active_downloads,
                "error_count": progress.error_count,
                "state": progress.state,
            }

        state = "idle"
        if status.get("current_job_id"):
            state = "running" if status.get("is_polling") else "paused"

        return SCJNStatusResponse(
            status=state,
            job_id=status.get("current_job_id"),
            connected=status.get("connected", False),
            progress=progress_dict
        )

    @app.post(
        "/api/scjn/start",
        response_model=StartJobResponse,
        tags=["SCJN"],
        dependencies=[Depends(require_auth)],
    )
    async def start_scjn_scraping(request: SCJNStartRequest):
        """Start a new SCJN scraping job."""
        if not api.scjn_bridge:
            return StartJobResponse(success=False, error="SCJN bridge not initialized")

        try:
            # Create SCJN search config
            config = SCJNSearchConfig(
                category=request.category,
                scope=request.scope,
                max_results=request.max_results,
            )

            # Start search via bridge
            result = await api.scjn_bridge.ask(("START_SEARCH", config))

            if result.get("success"):
                api._scjn_logs.append({
                    "level": "info",
                    "message": f"Starting SCJN search (category={request.category}, scope={request.scope})",
                    "source": "SCJN",
                    "timestamp": __import__('datetime').datetime.now().isoformat()
                })

            return StartJobResponse(
                success=result.get("success", False),
                job_id=result.get("job_id"),
                error=result.get("error")
            )
        except Exception as e:
            return StartJobResponse(success=False, error=str(e))

    @app.post(
        "/api/scjn/pause",
        response_model=ActionResponse,
        tags=["SCJN"],
        dependencies=[Depends(require_auth)],
    )
    async def pause_scjn_scraping():
        """Pause the current SCJN scraping job."""
        if not api.scjn_bridge:
            return ActionResponse(success=False, error="SCJN bridge not initialized")

        result = await api.scjn_bridge.ask("PAUSE_SEARCH")
        return ActionResponse(success=result.get("success", False), error=result.get("error"))

    @app.post(
        "/api/scjn/resume",
        response_model=ActionResponse,
        tags=["SCJN"],
        dependencies=[Depends(require_auth)],
    )
    async def resume_scjn_scraping():
        """Resume a paused SCJN scraping job."""
        if not api.scjn_bridge:
            return ActionResponse(success=False, error="SCJN bridge not initialized")

        result = await api.scjn_bridge.ask("RESUME_SEARCH")
        return ActionResponse(success=result.get("success", False), error=result.get("error"))

    @app.post(
        "/api/scjn/cancel",
        response_model=ActionResponse,
        tags=["SCJN"],
        dependencies=[Depends(require_auth)],
    )
    async def cancel_scjn_scraping():
        """Cancel the current SCJN scraping job."""
        if not api.scjn_bridge:
            return ActionResponse(success=False, error="SCJN bridge not initialized")

        result = await api.scjn_bridge.ask("STOP_SEARCH")
        return ActionResponse(success=result.get("success", False))

    @app.get(
        "/api/scjn/logs",
        response_model=LogsResponse,
        tags=["SCJN"],
        dependencies=[Depends(require_auth)],
    )
    async def get_scjn_logs():
        """Get SCJN job logs."""
        logs = [
            LogEntry(
                level=log["level"],
                message=log["message"],
                source=log["source"],
                timestamp=log["timestamp"]
            )
            for log in api._scjn_logs[-100:]  # Last 100 logs
        ]
        return LogsResponse(logs=logs)

    @app.get(
        "/api/scjn/categories",
        tags=["SCJN"],
        dependencies=[Depends(require_auth)],
    )
    async def get_scjn_categories():
        """Get available SCJN legislation categories."""
        return {
            "categories": [
                {"value": "", "label": "Todas las categorías"},
                {"value": "LEY", "label": "Leyes"},
                {"value": "CODIGO", "label": "Códigos"},
                {"value": "REGLAMENTO", "label": "Reglamentos"},
                {"value": "DECRETO", "label": "Decretos"},
                {"value": "ACUERDO", "label": "Acuerdos"},
                {"value": "CONSTITUCION", "label": "Constitución"},
            ],
            "scopes": [
                {"value": "", "label": "Todos los ámbitos"},
                {"value": "FEDERAL", "label": "Federal"},
                {"value": "ESTATAL", "label": "Estatal"},
            ]
        }

    # ============ BJV API Endpoints ============

    @app.get(
        "/api/bjv/status",
        response_model=BJVStatusResponse,
        tags=["BJV"],
        dependencies=[Depends(require_auth)],
    )
    async def get_bjv_status():
        """Get current BJV job status."""
        if not api.bjv_bridge:
            return BJVStatusResponse(status="not_initialized", connected=False)

        status = await api.bjv_bridge.ask("GET_STATUS")
        progress = await api.bjv_bridge.ask("GET_PROGRESS")

        progress_dict = None
        if progress:
            progress_dict = {
                "libros_descubiertos": progress.libros_descubiertos,
                "libros_descargados": progress.libros_descargados,
                "libros_pendientes": progress.libros_pendientes,
                "descargas_activas": progress.descargas_activas,
                "errores": progress.errores,
                "estado": progress.estado,
            }

        state = status.get("state", "idle")

        return BJVStatusResponse(
            status=state,
            job_id=status.get("current_job_id"),
            connected=status.get("connected", False),
            progress=progress_dict
        )

    @app.post(
        "/api/bjv/start",
        response_model=StartJobResponse,
        tags=["BJV"],
        dependencies=[Depends(require_auth)],
    )
    async def start_bjv_scraping(request: BJVStartRequest):
        """Start a new BJV scraping job."""
        if not api.bjv_bridge:
            return StartJobResponse(success=False, error="BJV bridge not initialized")

        try:
            # Parse area_derecho if provided
            area = None
            if request.area_derecho:
                try:
                    area = AreaDerecho(request.area_derecho.lower())
                except ValueError:
                    pass  # Invalid area, ignore

            # Create BJV search config
            config = BJVSearchConfig(
                termino_busqueda=request.termino_busqueda,
                area_derecho=area,
                max_resultados=request.max_resultados,
                incluir_capitulos=request.incluir_capitulos,
                descargar_pdfs=request.descargar_pdfs,
            )

            # Start search via bridge
            result = await api.bjv_bridge.ask(("START_SEARCH", config))

            if result.get("success"):
                api._bjv_logs.append({
                    "level": "info",
                    "message": f"Starting BJV search (term={request.termino_busqueda}, area={request.area_derecho})",
                    "source": "BJV",
                    "timestamp": __import__('datetime').datetime.now().isoformat()
                })

            return StartJobResponse(
                success=result.get("success", False),
                job_id=result.get("job_id"),
                error=result.get("error")
            )
        except Exception as e:
            return StartJobResponse(success=False, error=str(e))

    @app.post(
        "/api/bjv/pause",
        response_model=ActionResponse,
        tags=["BJV"],
        dependencies=[Depends(require_auth)],
    )
    async def pause_bjv_scraping():
        """Pause the current BJV scraping job."""
        if not api.bjv_bridge:
            return ActionResponse(success=False, error="BJV bridge not initialized")

        result = await api.bjv_bridge.ask("PAUSE_SEARCH")
        return ActionResponse(success=result.get("success", False), error=result.get("error"))

    @app.post(
        "/api/bjv/resume",
        response_model=ActionResponse,
        tags=["BJV"],
        dependencies=[Depends(require_auth)],
    )
    async def resume_bjv_scraping():
        """Resume a paused BJV scraping job."""
        if not api.bjv_bridge:
            return ActionResponse(success=False, error="BJV bridge not initialized")

        result = await api.bjv_bridge.ask("RESUME_SEARCH")
        return ActionResponse(success=result.get("success", False), error=result.get("error"))

    @app.post(
        "/api/bjv/cancel",
        response_model=ActionResponse,
        tags=["BJV"],
        dependencies=[Depends(require_auth)],
    )
    async def cancel_bjv_scraping():
        """Cancel the current BJV scraping job."""
        if not api.bjv_bridge:
            return ActionResponse(success=False, error="BJV bridge not initialized")

        result = await api.bjv_bridge.ask("STOP_SEARCH")
        return ActionResponse(success=result.get("success", False))

    @app.get(
        "/api/bjv/logs",
        response_model=LogsResponse,
        tags=["BJV"],
        dependencies=[Depends(require_auth)],
    )
    async def get_bjv_logs():
        """Get BJV job logs."""
        logs = [
            LogEntry(
                level=log["level"],
                message=log["message"],
                source=log["source"],
                timestamp=log["timestamp"]
            )
            for log in api._bjv_logs[-100:]  # Last 100 logs
        ]
        return LogsResponse(logs=logs)

    @app.get(
        "/api/bjv/areas",
        tags=["BJV"],
        dependencies=[Depends(require_auth)],
    )
    async def get_bjv_areas():
        """Get available BJV legal areas."""
        return {
            "areas": [
                {"value": "", "label": "Todas las áreas"},
                {"value": "civil", "label": "Derecho Civil"},
                {"value": "penal", "label": "Derecho Penal"},
                {"value": "constitucional", "label": "Derecho Constitucional"},
                {"value": "administrativo", "label": "Derecho Administrativo"},
                {"value": "mercantil", "label": "Derecho Mercantil"},
                {"value": "laboral", "label": "Derecho Laboral"},
                {"value": "fiscal", "label": "Derecho Fiscal"},
                {"value": "internacional", "label": "Derecho Internacional"},
                {"value": "general", "label": "General"},
            ]
        }

    # ============ CAS API Endpoints ============

    @app.get(
        "/api/cas/status",
        response_model=CASStatusResponse,
        tags=["CAS"],
        dependencies=[Depends(require_auth)],
    )
    async def get_cas_status():
        """Get CAS scraper job status."""
        if not api.cas_bridge:
            return CASStatusResponse(status="idle")

        # Get current job status from bridge
        job_id = api.cas_bridge._current_job_id
        status = "running" if job_id else "idle"

        progress = None
        if job_id:
            progress = {
                "discovered": 0,
                "downloaded": 0,
                "processed": 0,
                "errors": 0,
            }

        return CASStatusResponse(
            status=status,
            job_id=job_id,
            progress=progress
        )

    @app.get(
        "/api/cas/filters",
        response_model=CASFiltersResponse,
        tags=["CAS"],
        dependencies=[Depends(require_auth)],
    )
    async def get_cas_filters():
        """Get available CAS filter options (sports, matters, years)."""
        return CASFiltersResponse(
            sports=[
                "football",
                "athletics",
                "cycling",
                "swimming",
                "tennis",
                "basketball",
                "skiing",
                "ice hockey",
            ],
            matters=[
                "doping",
                "eligibility",
                "transfer",
                "contract",
                "disciplinary",
                "governance",
            ],
            year_range={
                "min": 1986,
                "max": 2026,
            }
        )

    @app.post(
        "/api/cas/start",
        response_model=StartJobResponse,
        tags=["CAS"],
        dependencies=[Depends(require_auth)],
    )
    async def start_cas_scraping(request: CASStartRequest):
        """Start a new CAS scraping job."""
        if not api.cas_bridge:
            return StartJobResponse(success=False, error="CAS bridge not initialized")

        try:
            # Create CAS config from request
            config = CASGuiConfig(
                year_from=request.year_from,
                year_to=request.year_to,
                sport=request.sport,
                matter=request.matter,
                max_results=request.max_results,
            )

            # Start job via bridge
            job_id = await api.cas_bridge.start_job(config)

            api._cas_logs.append({
                "level": "info",
                "message": f"Starting CAS search (sport={request.sport}, matter={request.matter})",
                "source": "CAS",
                "timestamp": __import__('datetime').datetime.now().isoformat()
            })

            return StartJobResponse(success=True, job_id=job_id)
        except Exception as e:
            return StartJobResponse(success=False, error=str(e))

    @app.post(
        "/api/cas/pause",
        response_model=ActionResponse,
        tags=["CAS"],
        dependencies=[Depends(require_auth)],
    )
    async def pause_cas_scraping():
        """Pause the current CAS scraping job."""
        if not api.cas_bridge:
            return ActionResponse(success=False, error="CAS bridge not initialized")

        try:
            await api.cas_bridge.pause_job()
            return ActionResponse(success=True)
        except Exception as e:
            return ActionResponse(success=False, error=str(e))

    @app.post(
        "/api/cas/resume",
        response_model=ActionResponse,
        tags=["CAS"],
        dependencies=[Depends(require_auth)],
    )
    async def resume_cas_scraping():
        """Resume a paused CAS scraping job."""
        if not api.cas_bridge:
            return ActionResponse(success=False, error="CAS bridge not initialized")

        try:
            await api.cas_bridge.resume_job()
            return ActionResponse(success=True)
        except Exception as e:
            return ActionResponse(success=False, error=str(e))

    @app.post(
        "/api/cas/cancel",
        response_model=ActionResponse,
        tags=["CAS"],
        dependencies=[Depends(require_auth)],
    )
    async def cancel_cas_scraping():
        """Cancel the current CAS scraping job."""
        if not api.cas_bridge:
            return ActionResponse(success=False, error="CAS bridge not initialized")

        try:
            await api.cas_bridge.stop_job()
            return ActionResponse(success=True)
        except Exception as e:
            return ActionResponse(success=False, error=str(e))

    @app.get(
        "/api/cas/logs",
        response_model=LogsResponse,
        tags=["CAS"],
        dependencies=[Depends(require_auth)],
    )
    async def get_cas_logs(
        limit: int = 100,
        level: Optional[str] = None,
    ):
        """Get CAS job logs."""
        logs = api._cas_logs[-limit:]  # Last N logs

        # Filter by level if specified
        if level:
            logs = [log for log in logs if log.get("level") == level]

        log_entries = [
            LogEntry(
                level=log["level"],
                message=log["message"],
                source=log["source"],
                timestamp=log["timestamp"]
            )
            for log in logs
        ]
        return LogsResponse(logs=log_entries)

    # ============ DOF API Endpoints ============

    @app.get(
        "/api/dof/status",
        response_model=DOFStatusResponse,
        tags=["DOF"],
        dependencies=[Depends(require_auth)],
    )
    async def get_dof_status():
        """Get DOF scraper job status."""
        if not api.dof_bridge:
            return DOFStatusResponse(status="idle")

        # Get current job status from bridge
        job_id = api.dof_bridge._current_job_id
        status = "running" if job_id else "idle"

        progress = None
        if job_id:
            progress = {
                "total_documents": 0,
                "processed_documents": 0,
            }

        return DOFStatusResponse(
            status=status,
            job_id=job_id,
            progress=progress
        )

    @app.get(
        "/api/dof/sections",
        response_model=DOFSectionsResponse,
        tags=["DOF"],
        dependencies=[Depends(require_auth)],
    )
    async def get_dof_sections():
        """Get available DOF sections and scraping modes."""
        return DOFSectionsResponse(
            sections=[
                {"value": "primera", "name": "Primera Sección", "label": "Primera Sección"},
                {"value": "segunda", "name": "Segunda Sección", "label": "Segunda Sección"},
                {"value": "tercera", "name": "Tercera Sección", "label": "Tercera Sección"},
                {"value": "cuarta", "name": "Cuarta Sección", "label": "Cuarta Sección"},
                {"value": "quinta", "name": "Quinta Sección", "label": "Quinta Sección"},
            ],
            modes=[
                {"value": "today", "label": "Hoy"},
                {"value": "range", "label": "Rango de fechas"},
            ]
        )

    @app.post(
        "/api/dof/start",
        response_model=StartJobResponse,
        tags=["DOF"],
        dependencies=[Depends(require_auth)],
    )
    async def start_dof_scraping(request: DOFStartRequest):
        """Start a new DOF scraping job."""
        # Validate date range mode requires dates (before bridge check)
        if request.mode == "range":
            if not request.start_date or not request.end_date:
                raise HTTPException(
                    status_code=400,
                    detail="start_date and end_date are required for range mode"
                )

        if not api.dof_bridge:
            return StartJobResponse(success=False, error="DOF bridge not initialized")

        try:
            # Create DOF config from request
            config = DOFGuiConfig(
                mode=request.mode,
                start_date=request.start_date,
                end_date=request.end_date,
                section=request.section,
                download_pdfs=request.download_pdfs,
                output_directory=request.output_directory,
            )

            # Start job via bridge
            job_id = await api.dof_bridge.start_job(config)

            api._dof_logs.append({
                "level": "info",
                "message": f"Starting DOF scrape (mode={request.mode}, section={request.section})",
                "source": "DOF",
                "timestamp": __import__('datetime').datetime.now().isoformat()
            })

            return StartJobResponse(success=True, job_id=job_id)
        except Exception as e:
            return StartJobResponse(success=False, error=str(e))

    @app.post(
        "/api/dof/pause",
        response_model=ActionResponse,
        tags=["DOF"],
        dependencies=[Depends(require_auth)],
    )
    async def pause_dof_scraping():
        """Pause the current DOF scraping job."""
        if not api.dof_bridge:
            return ActionResponse(success=False, error="DOF bridge not initialized")

        try:
            await api.dof_bridge.pause_job()
            return ActionResponse(success=True)
        except Exception as e:
            return ActionResponse(success=False, error=str(e))

    @app.post(
        "/api/dof/resume",
        response_model=ActionResponse,
        tags=["DOF"],
        dependencies=[Depends(require_auth)],
    )
    async def resume_dof_scraping():
        """Resume a paused DOF scraping job."""
        if not api.dof_bridge:
            return ActionResponse(success=False, error="DOF bridge not initialized")

        try:
            await api.dof_bridge.resume_job()
            return ActionResponse(success=True)
        except Exception as e:
            return ActionResponse(success=False, error=str(e))

    @app.post(
        "/api/dof/cancel",
        response_model=ActionResponse,
        tags=["DOF"],
        dependencies=[Depends(require_auth)],
    )
    async def cancel_dof_scraping():
        """Cancel the current DOF scraping job."""
        if not api.dof_bridge:
            return ActionResponse(success=False, error="DOF bridge not initialized")

        try:
            await api.dof_bridge.stop_job()
            return ActionResponse(success=True)
        except Exception as e:
            return ActionResponse(success=False, error=str(e))

    @app.get(
        "/api/dof/logs",
        response_model=LogsResponse,
        tags=["DOF"],
        dependencies=[Depends(require_auth)],
    )
    async def get_dof_logs(
        limit: int = 100,
        level: Optional[str] = None,
    ):
        """Get DOF job logs."""
        logs = api._dof_logs[-limit:]  # Last N logs

        # Filter by level if specified
        if level:
            logs = [log for log in logs if log.get("level") == level]

        log_entries = [
            LogEntry(
                level=log["level"],
                message=log["message"],
                source=log["source"],
                timestamp=log["timestamp"]
            )
            for log in logs
        ]
        return LogsResponse(logs=log_entries)

    # ============ Web UI Routes ============

    @app.get(
        "/",
        response_class=HTMLResponse,
        tags=["Web UI"],
        dependencies=[Depends(require_auth)],
    )
    async def index(request: Request):
        """Serve the main web UI."""
        if templates:
            return templates.TemplateResponse("index.html", {"request": request})
        else:
            # Return inline HTML if no templates
            return HTMLResponse(content=get_inline_html())

    return app


def get_inline_html() -> str:
    """Return inline HTML for the web UI (fallback if no templates)."""
    return """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Legal Scraper - Mexican Legal Documents</title>
    <style>
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #1a1a2e; color: #eee; min-height: 100vh; }
        .container { max-width: 1200px; margin: 0 auto; padding: 20px; }
        header { background: #16213e; padding: 20px; border-radius: 10px; margin-bottom: 20px; }
        h1 { color: #00d9ff; }
        .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }
        @media (max-width: 768px) { .grid { grid-template-columns: 1fr; } }
        .card { background: #16213e; border-radius: 10px; padding: 20px; }
        .card h2 { color: #00d9ff; margin-bottom: 15px; font-size: 1.2em; }
        .status { font-size: 2em; font-weight: bold; text-align: center; padding: 20px; }
        .status.idle { color: #888; }
        .status.running { color: #00ff88; }
        .status.paused { color: #ffaa00; }
        .status.completed { color: #00d9ff; }
        .status.failed { color: #ff4444; }
        .progress-bar { height: 30px; background: #0a0a1a; border-radius: 15px; overflow: hidden; margin: 10px 0; }
        .progress-fill { height: 100%; background: linear-gradient(90deg, #00d9ff, #00ff88); transition: width 0.3s; }
        .progress-text { text-align: center; margin-top: 5px; }
        label { display: block; margin-bottom: 5px; color: #aaa; }
        select, input { width: 100%; padding: 10px; margin-bottom: 15px; border: 1px solid #333; border-radius: 5px; background: #0a0a1a; color: #eee; }
        .btn-group { display: flex; gap: 10px; flex-wrap: wrap; }
        button { padding: 12px 24px; border: none; border-radius: 5px; cursor: pointer; font-weight: bold; transition: all 0.2s; }
        .btn-start { background: #00ff88; color: #000; }
        .btn-pause { background: #ffaa00; color: #000; }
        .btn-resume { background: #00d9ff; color: #000; }
        .btn-cancel { background: #ff4444; color: #fff; }
        button:disabled { opacity: 0.5; cursor: not-allowed; }
        button:hover:not(:disabled) { transform: translateY(-2px); }
        .logs { background: #0a0a1a; border-radius: 5px; padding: 10px; height: 300px; overflow-y: auto; font-family: monospace; font-size: 0.9em; }
        .log-entry { padding: 5px 0; border-bottom: 1px solid #222; }
        .log-info { color: #00d9ff; }
        .log-success { color: #00ff88; }
        .log-warning { color: #ffaa00; }
        .log-error { color: #ff4444; }
        .connection { display: flex; align-items: center; gap: 10px; }
        .connection-dot { width: 12px; height: 12px; border-radius: 50%; background: #888; }
        .connection-dot.connected { background: #00ff88; }
        .date-inputs { display: none; }
        .date-inputs.show { display: block; }
        .scjn-options { display: none; }
        .scjn-options.show { display: block; }
        .dof-options { display: block; }
        .dof-options.hide { display: none; }
        .tabs { display: flex; gap: 0; margin-bottom: 20px; }
        .tab { padding: 12px 24px; background: #0a0a1a; border: none; color: #888; cursor: pointer; font-weight: bold; transition: all 0.2s; }
        .tab:first-child { border-radius: 10px 0 0 10px; }
        .tab:last-child { border-radius: 0 10px 10px 0; }
        .tab.active { background: #00d9ff; color: #000; }
        .tab:hover:not(.active) { color: #eee; }
        .source-badge { display: inline-block; padding: 4px 8px; border-radius: 4px; font-size: 0.8em; margin-left: 10px; }
        .source-badge.dof { background: #e74c3c; }
        .source-badge.scjn { background: #9b59b6; }
        .source-badge.bjv { background: #27ae60; }
        .bjv-options { display: none; }
        .bjv-options.show { display: block; }
        .tab:nth-child(2) { border-radius: 0; }
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>Legal Scraper</h1>
            <p>Mexican Legal Document Collector - Control Panel</p>
            <div class="connection">
                <div class="connection-dot" id="connectionDot"></div>
                <span id="connectionText">Connecting...</span>
            </div>
        </header>

        <div class="tabs">
            <button class="tab active" id="tabDof" onclick="switchSource('dof')">DOF - Diario Oficial</button>
            <button class="tab" id="tabScjn" onclick="switchSource('scjn')">SCJN - Legislacion</button>
            <button class="tab" id="tabBjv" onclick="switchSource('bjv')">BJV - Biblioteca UNAM</button>
        </div>

        <div class="grid">
            <div class="card">
                <h2>Status <span class="source-badge dof" id="sourceBadge">DOF</span></h2>
                <div class="status idle" id="statusDisplay">Idle</div>
                <div class="progress-bar">
                    <div class="progress-fill" id="progressBar" style="width: 0%"></div>
                </div>
                <div class="progress-text" id="progressText">0% (0/0)</div>
            </div>

            <div class="card">
                <h2>Configuration</h2>

                <!-- DOF Options -->
                <div class="dof-options" id="dofOptions">
                    <label>Mode</label>
                    <select id="mode">
                        <option value="today">Today</option>
                        <option value="single">Single Date</option>
                        <option value="range">Date Range</option>
                        <option value="historical">Historical</option>
                    </select>

                    <div class="date-inputs" id="dateInputs">
                        <label>Start Date</label>
                        <input type="date" id="startDate">
                        <label>End Date</label>
                        <input type="date" id="endDate">
                    </div>

                    <label>Output Directory</label>
                    <input type="text" id="outputDir" value="scraped_data">
                </div>

                <!-- SCJN Options -->
                <div class="scjn-options" id="scjnOptions">
                    <label>Category</label>
                    <select id="scjnCategory">
                        <option value="">Todas las categorias</option>
                        <option value="LEY">Leyes</option>
                        <option value="CODIGO">Codigos</option>
                        <option value="REGLAMENTO">Reglamentos</option>
                        <option value="DECRETO">Decretos</option>
                        <option value="ACUERDO">Acuerdos</option>
                        <option value="CONSTITUCION">Constitucion</option>
                    </select>

                    <label>Scope</label>
                    <select id="scjnScope">
                        <option value="">Todos los ambitos</option>
                        <option value="FEDERAL">Federal</option>
                        <option value="ESTATAL">Estatal</option>
                    </select>

                    <label>Max Results</label>
                    <input type="number" id="scjnMaxResults" value="100" min="1" max="1000">

                    <label>Output Directory</label>
                    <input type="text" id="scjnOutputDir" value="scjn_data">
                </div>

                <!-- BJV Options -->
                <div class="bjv-options" id="bjvOptions">
                    <label>Search Term (optional)</label>
                    <input type="text" id="bjvSearchTerm" placeholder="e.g., derecho civil, contratos">

                    <label>Legal Area</label>
                    <select id="bjvArea">
                        <option value="">Todas las areas</option>
                        <option value="civil">Derecho Civil</option>
                        <option value="penal">Derecho Penal</option>
                        <option value="constitucional">Derecho Constitucional</option>
                        <option value="administrativo">Derecho Administrativo</option>
                        <option value="mercantil">Derecho Mercantil</option>
                        <option value="laboral">Derecho Laboral</option>
                        <option value="fiscal">Derecho Fiscal</option>
                        <option value="internacional">Derecho Internacional</option>
                        <option value="general">General</option>
                    </select>

                    <label>Max Books</label>
                    <input type="number" id="bjvMaxResults" value="50" min="1" max="500">

                    <label>
                        <input type="checkbox" id="bjvDownloadPdfs" checked> Download PDFs
                    </label>

                    <label>Output Directory</label>
                    <input type="text" id="bjvOutputDir" value="bjv_data">
                </div>
            </div>
        </div>

        <div class="card" style="margin-top: 20px;">
            <h2>Controls</h2>
            <div class="btn-group">
                <button class="btn-start" id="btnStart" onclick="startJob()">Start</button>
                <button class="btn-pause" id="btnPause" onclick="pauseJob()" disabled>Pause</button>
                <button class="btn-resume" id="btnResume" onclick="resumeJob()" disabled>Resume</button>
                <button class="btn-cancel" id="btnCancel" onclick="cancelJob()" disabled>Cancel</button>
            </div>
        </div>

        <div class="card" style="margin-top: 20px;">
            <h2>Logs</h2>
            <div class="logs" id="logsContainer"></div>
        </div>
    </div>

    <script>
        // State
        let currentStatus = 'idle';
        let currentSource = 'dof';

        // DOM elements
        const statusDisplay = document.getElementById('statusDisplay');
        const progressBar = document.getElementById('progressBar');
        const progressText = document.getElementById('progressText');
        const logsContainer = document.getElementById('logsContainer');
        const connectionDot = document.getElementById('connectionDot');
        const connectionText = document.getElementById('connectionText');
        const modeSelect = document.getElementById('mode');
        const dateInputs = document.getElementById('dateInputs');
        const dofOptions = document.getElementById('dofOptions');
        const scjnOptions = document.getElementById('scjnOptions');
        const bjvOptions = document.getElementById('bjvOptions');
        const sourceBadge = document.getElementById('sourceBadge');
        const tabDof = document.getElementById('tabDof');
        const tabScjn = document.getElementById('tabScjn');
        const tabBjv = document.getElementById('tabBjv');

        // Buttons
        const btnStart = document.getElementById('btnStart');
        const btnPause = document.getElementById('btnPause');
        const btnResume = document.getElementById('btnResume');
        const btnCancel = document.getElementById('btnCancel');

        // Source switching
        function switchSource(source) {
            currentSource = source;
            // Reset all tabs and options
            tabDof.classList.remove('active');
            tabScjn.classList.remove('active');
            tabBjv.classList.remove('active');
            dofOptions.classList.add('hide');
            scjnOptions.classList.remove('show');
            bjvOptions.classList.remove('show');

            if (source === 'dof') {
                tabDof.classList.add('active');
                dofOptions.classList.remove('hide');
                sourceBadge.textContent = 'DOF';
                sourceBadge.className = 'source-badge dof';
            } else if (source === 'scjn') {
                tabScjn.classList.add('active');
                scjnOptions.classList.add('show');
                sourceBadge.textContent = 'SCJN';
                sourceBadge.className = 'source-badge scjn';
            } else if (source === 'bjv') {
                tabBjv.classList.add('active');
                bjvOptions.classList.add('show');
                sourceBadge.textContent = 'BJV';
                sourceBadge.className = 'source-badge bjv';
            }
            // Fetch status for the selected source
            fetchStatus();
            fetchLogs();
        }

        // Mode change handler
        modeSelect.addEventListener('change', function() {
            if (['single', 'range', 'historical'].includes(this.value)) {
                dateInputs.classList.add('show');
            } else {
                dateInputs.classList.remove('show');
            }
        });

        // API functions
        async function fetchStatus() {
            try {
                let endpoint = '/api/status';
                if (currentSource === 'scjn') endpoint = '/api/scjn/status';
                else if (currentSource === 'bjv') endpoint = '/api/bjv/status';

                const response = await fetch(endpoint);
                const data = await response.json();
                updateStatus(data);
                setConnected(true);
            } catch (e) {
                setConnected(false);
            }
        }

        async function fetchLogs() {
            try {
                let endpoint = '/api/logs';
                if (currentSource === 'scjn') endpoint = '/api/scjn/logs';
                else if (currentSource === 'bjv') endpoint = '/api/bjv/logs';

                const response = await fetch(endpoint);
                const data = await response.json();
                updateLogs(data.logs);
            } catch (e) {}
        }

        async function startJob() {
            if (currentSource === 'scjn') {
                await startScjnJob();
            } else if (currentSource === 'bjv') {
                await startBjvJob();
            } else {
                await startDofJob();
            }
        }

        async function startDofJob() {
            const config = {
                source: 'dof',
                mode: modeSelect.value,
                output_directory: document.getElementById('outputDir').value,
                start_date: document.getElementById('startDate').value || null,
                end_date: document.getElementById('endDate').value || null
            };

            try {
                const response = await fetch('/api/start', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(config)
                });
                const data = await response.json();
                if (!data.success) {
                    addLog('error', data.error || 'Failed to start');
                }
            } catch (e) {
                addLog('error', 'Failed to start: ' + e.message);
            }
        }

        async function startScjnJob() {
            const config = {
                category: document.getElementById('scjnCategory').value || null,
                scope: document.getElementById('scjnScope').value || null,
                max_results: parseInt(document.getElementById('scjnMaxResults').value) || 100,
                output_directory: document.getElementById('scjnOutputDir').value
            };

            try {
                const response = await fetch('/api/scjn/start', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(config)
                });
                const data = await response.json();
                if (!data.success) {
                    addLog('error', data.error || 'Failed to start SCJN job');
                } else {
                    addLog('info', 'SCJN job started: ' + data.job_id);
                }
            } catch (e) {
                addLog('error', 'Failed to start SCJN: ' + e.message);
            }
        }

        async function startBjvJob() {
            const config = {
                termino_busqueda: document.getElementById('bjvSearchTerm').value || null,
                area_derecho: document.getElementById('bjvArea').value || null,
                max_resultados: parseInt(document.getElementById('bjvMaxResults').value) || 50,
                descargar_pdfs: document.getElementById('bjvDownloadPdfs').checked,
                output_directory: document.getElementById('bjvOutputDir').value
            };

            try {
                const response = await fetch('/api/bjv/start', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(config)
                });
                const data = await response.json();
                if (!data.success) {
                    addLog('error', data.error || 'Failed to start BJV job');
                } else {
                    addLog('info', 'BJV job started: ' + data.job_id);
                }
            } catch (e) {
                addLog('error', 'Failed to start BJV: ' + e.message);
            }
        }

        async function pauseJob() {
            let endpoint = '/api/pause';
            if (currentSource === 'scjn') endpoint = '/api/scjn/pause';
            else if (currentSource === 'bjv') endpoint = '/api/bjv/pause';
            await fetch(endpoint, { method: 'POST' });
        }

        async function resumeJob() {
            let endpoint = '/api/resume';
            if (currentSource === 'scjn') endpoint = '/api/scjn/resume';
            else if (currentSource === 'bjv') endpoint = '/api/bjv/resume';
            await fetch(endpoint, { method: 'POST' });
        }

        async function cancelJob() {
            if (confirm('Are you sure you want to cancel?')) {
                let endpoint = '/api/cancel';
                if (currentSource === 'scjn') endpoint = '/api/scjn/cancel';
                else if (currentSource === 'bjv') endpoint = '/api/bjv/cancel';
                await fetch(endpoint, { method: 'POST' });
            }
        }

        // UI update functions
        function updateStatus(data) {
            currentStatus = data.status;
            statusDisplay.textContent = data.status.charAt(0).toUpperCase() + data.status.slice(1);
            statusDisplay.className = 'status ' + data.status;

            if (currentSource === 'scjn' && data.progress) {
                // SCJN progress format
                const discovered = data.progress.discovered_count || 0;
                const downloaded = data.progress.downloaded_count || 0;
                const percentage = discovered > 0 ? Math.round((downloaded / discovered) * 100) : 0;
                progressBar.style.width = percentage + '%';
                progressText.textContent = `${percentage}% (${downloaded}/${discovered} downloaded)`;
            } else if (currentSource === 'bjv' && data.progress) {
                // BJV progress format (Spanish field names)
                const discovered = data.progress.libros_descubiertos || 0;
                const downloaded = data.progress.libros_descargados || 0;
                const percentage = discovered > 0 ? Math.round((downloaded / discovered) * 100) : 0;
                progressBar.style.width = percentage + '%';
                progressText.textContent = `${percentage}% (${downloaded}/${discovered} libros)`;
            } else if (data.progress) {
                // DOF progress format
                progressBar.style.width = data.progress.percentage + '%';
                progressText.textContent = `${Math.round(data.progress.percentage)}% (${data.progress.processed_items}/${data.progress.total_items})`;
            } else {
                progressBar.style.width = '0%';
                progressText.textContent = '0% (0/0)';
            }

            // Update buttons
            const isRunning = data.status === 'running';
            const isPaused = data.status === 'paused';
            const isIdle = data.status === 'idle' || data.status === 'not_initialized';

            btnStart.disabled = !isIdle;
            btnPause.disabled = !isRunning;
            btnResume.disabled = !isPaused;
            btnCancel.disabled = isIdle || data.status === 'completed' || data.status === 'failed';
        }

        function updateLogs(logs) {
            logsContainer.innerHTML = logs.map(log =>
                `<div class="log-entry log-${log.level}">[${log.timestamp.split('T')[1].split('.')[0]}] ${log.source}: ${log.message}</div>`
            ).join('');
            logsContainer.scrollTop = logsContainer.scrollHeight;
        }

        function addLog(level, message) {
            const entry = document.createElement('div');
            entry.className = 'log-entry log-' + level;
            entry.textContent = `[${new Date().toLocaleTimeString()}] UI: ${message}`;
            logsContainer.appendChild(entry);
            logsContainer.scrollTop = logsContainer.scrollHeight;
        }

        function setConnected(connected) {
            connectionDot.classList.toggle('connected', connected);
            connectionText.textContent = connected ? 'Connected' : 'Disconnected';
        }

        // Polling
        setInterval(fetchStatus, 2000);
        setInterval(fetchLogs, 3000);

        // Initial fetch
        fetchStatus();
        fetchLogs();

        // Set default dates
        const today = new Date().toISOString().split('T')[0];
        document.getElementById('startDate').value = today;
        document.getElementById('endDate').value = today;
    </script>
</body>
</html>
"""


# Entry point for running the server
def run_server(host: str = "0.0.0.0", port: int = 8000):
    """Run the web server."""
    import uvicorn
    app = create_app()
    uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":
    run_server()
