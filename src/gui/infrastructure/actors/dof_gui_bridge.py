"""
DOF GUI Bridge Actor.

Connects the GUI layer to the DOF (Diario Oficial de la Federación) scraper,
providing a clean interface for starting, monitoring,
and controlling DOF scraping jobs.
"""
from dataclasses import dataclass, field, asdict
from typing import Optional, Callable, Awaitable, Dict, Any
from datetime import datetime, timezone
import uuid


@dataclass
class DOFGuiConfig:
    """Configuration for DOF GUI scraping jobs."""
    mode: str = "today"  # "today" or "range"
    start_date: Optional[str] = None  # YYYY-MM-DD format
    end_date: Optional[str] = None  # YYYY-MM-DD format
    section: Optional[str] = None  # primera, segunda, tercera, etc.
    download_pdfs: bool = True
    output_directory: str = "dof_data"

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return asdict(self)


@dataclass
class DOFJobStarted:
    """Event emitted when a DOF job starts."""
    job_id: str
    timestamp: str
    config: Dict[str, Any]


@dataclass
class DOFJobProgress:
    """Event emitted for DOF job progress updates."""
    job_id: str
    estado: str
    total_documents: int
    processed_documents: int
    errores: int
    porcentaje: float


@dataclass
class DOFJobCompleted:
    """Event emitted when a DOF job completes."""
    job_id: str
    timestamp: str
    estadisticas: Dict[str, Any]
    exito: bool


@dataclass
class DOFJobError:
    """Event emitted when a DOF job encounters an error."""
    job_id: str
    timestamp: str
    mensaje: str
    recuperable: bool


class DOFGuiBridgeActor:
    """
    Bridge actor connecting GUI to DOF scraper.

    Provides a simple interface for:
    - Starting scraping jobs with date/section filters
    - Monitoring job progress
    - Pausing, resuming, and stopping jobs
    - Receiving event callbacks for UI updates
    """

    def __init__(
        self,
        on_event: Optional[Callable[[Any], Awaitable[None]]] = None,
        output_dir: str = "dof_data",
    ):
        """
        Initialize the DOF GUI bridge actor.

        Args:
            on_event: Async callback for job events
            output_dir: Directory for output files
        """
        self._on_event = on_event
        self._output_dir = output_dir
        self._current_job_id: Optional[str] = None
        self._coordinator = None

    async def _emit_event(self, event: Any) -> None:
        """Emit an event to the callback if registered."""
        if self._on_event:
            await self._on_event(event)

    async def start_job(self, config: DOFGuiConfig) -> str:
        """
        Start a new DOF scraping job.

        Args:
            config: Configuration for the job

        Returns:
            Job ID for tracking
        """
        # Generate unique job ID
        job_id = f"dof-gui-{uuid.uuid4().hex[:8]}"
        self._current_job_id = job_id

        # Build config dict
        config_dict = config.to_dict()

        # Emit started event
        event = DOFJobStarted(
            job_id=job_id,
            timestamp=datetime.now(timezone.utc).isoformat(),
            config=config_dict,
        )
        await self._emit_event(event)

        # Send message to coordinator to start the job (if connected)
        if self._coordinator:
            await self._coordinator.tell({
                "type": "start_job",
                "job_id": job_id,
                "config": config_dict,
                "output_dir": self._output_dir,
            })

        return job_id

    async def pause_job(self) -> None:
        """Pause the current job."""
        if self._coordinator and self._current_job_id:
            await self._coordinator.tell({
                "type": "pause_job",
                "job_id": self._current_job_id,
            })

    async def resume_job(self) -> None:
        """Resume the current job."""
        if self._coordinator and self._current_job_id:
            await self._coordinator.tell({
                "type": "resume_job",
                "job_id": self._current_job_id,
            })

    async def stop_job(self) -> None:
        """Stop the current job."""
        if self._coordinator and self._current_job_id:
            await self._coordinator.tell({
                "type": "stop_job",
                "job_id": self._current_job_id,
            })

    async def handle_coordinator_event(self, event: Dict[str, Any]) -> None:
        """
        Handle events from the coordinator.

        Translates coordinator events to GUI events.
        """
        event_type = event.get("type")
        job_id = event.get("job_id", self._current_job_id)

        if event_type == "progress":
            gui_event = DOFJobProgress(
                job_id=job_id,
                estado=event.get("estado", "DESCONOCIDO"),
                total_documents=event.get("total_documents", 0),
                processed_documents=event.get("processed_documents", 0),
                errores=event.get("errores", 0),
                porcentaje=event.get("porcentaje", 0.0),
            )
            await self._emit_event(gui_event)

        elif event_type == "completed":
            gui_event = DOFJobCompleted(
                job_id=job_id,
                timestamp=datetime.now(timezone.utc).isoformat(),
                estadisticas=event.get("estadisticas", {}),
                exito=event.get("exito", True),
            )
            await self._emit_event(gui_event)

        elif event_type == "error":
            gui_event = DOFJobError(
                job_id=job_id,
                timestamp=datetime.now(timezone.utc).isoformat(),
                mensaje=event.get("mensaje", "Error desconocido"),
                recuperable=event.get("recuperable", False),
            )
            await self._emit_event(gui_event)
