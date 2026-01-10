"""
CAS GUI Bridge Actor.

Connects the GUI layer to the CAS coordinator actor,
providing a clean interface for starting, monitoring,
and controlling CAS scraping jobs.
"""
from dataclasses import dataclass, field, asdict
from typing import Optional, Callable, Awaitable, Dict, Any
from datetime import datetime
import uuid

from src.infrastructure.actors.cas_base_actor import CASBaseActor


@dataclass
class CASGuiConfig:
    """Configuration for CAS GUI scraping jobs."""
    year_from: Optional[int] = None
    year_to: Optional[int] = None
    sport: Optional[str] = None
    matter: Optional[str] = None
    max_results: int = 100

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return asdict(self)


@dataclass
class CASJobStarted:
    """Event emitted when a CAS job starts."""
    job_id: str
    timestamp: str
    filters: Dict[str, Any]


@dataclass
class CASJobProgress:
    """Event emitted for CAS job progress updates."""
    job_id: str
    estado: str
    descubiertos: int
    descargados: int
    procesados: int
    errores: int
    porcentaje: float


@dataclass
class CASJobCompleted:
    """Event emitted when a CAS job completes."""
    job_id: str
    timestamp: str
    estadisticas: Dict[str, Any]
    exito: bool


@dataclass
class CASJobError:
    """Event emitted when a CAS job encounters an error."""
    job_id: str
    timestamp: str
    mensaje: str
    recuperable: bool


class CASGuiBridgeActor(CASBaseActor):
    """
    Bridge actor connecting GUI to CAS coordinator.

    Provides a simple interface for:
    - Starting scraping jobs with filters
    - Monitoring job progress
    - Pausing, resuming, and stopping jobs
    - Receiving event callbacks for UI updates
    """

    def __init__(
        self,
        on_event: Optional[Callable[[Any], Awaitable[None]]] = None,
        output_dir: str = "cas_data",
    ):
        """
        Initialize the CAS GUI bridge actor.

        Args:
            on_event: Async callback for job events
            output_dir: Directory for output files
        """
        super().__init__()
        self._on_event = on_event
        self._output_dir = output_dir
        self._current_job_id: Optional[str] = None
        self._coordinator = None

    async def _emit_event(self, event: Any) -> None:
        """Emit an event to the callback if registered."""
        if self._on_event:
            await self._on_event(event)

    async def start_job(self, config: CASGuiConfig) -> str:
        """
        Start a new CAS scraping job.

        Args:
            config: Configuration for the job

        Returns:
            Job ID for tracking
        """
        # Generate unique job ID
        job_id = f"cas-gui-{uuid.uuid4().hex[:8]}"
        self._current_job_id = job_id

        # Build filters from config
        filters = {}
        if config.year_from:
            filters["year_from"] = config.year_from
        if config.year_to:
            filters["year_to"] = config.year_to
        if config.sport:
            filters["sport"] = config.sport
        if config.matter:
            filters["matter"] = config.matter
        filters["max_results"] = config.max_results

        # Emit started event
        event = CASJobStarted(
            job_id=job_id,
            timestamp=datetime.utcnow().isoformat(),
            filters=filters,
        )
        await self._emit_event(event)

        # Send message to coordinator to start the job
        if self._coordinator:
            await self._coordinator.tell({
                "type": "start_job",
                "job_id": job_id,
                "filters": filters,
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
            gui_event = CASJobProgress(
                job_id=job_id,
                estado=event.get("estado", "DESCONOCIDO"),
                descubiertos=event.get("descubiertos", 0),
                descargados=event.get("descargados", 0),
                procesados=event.get("procesados", 0),
                errores=event.get("errores", 0),
                porcentaje=event.get("porcentaje", 0.0),
            )
            await self._emit_event(gui_event)

        elif event_type == "completed":
            gui_event = CASJobCompleted(
                job_id=job_id,
                timestamp=datetime.utcnow().isoformat(),
                estadisticas=event.get("estadisticas", {}),
                exito=event.get("exito", True),
            )
            await self._emit_event(gui_event)

        elif event_type == "error":
            gui_event = CASJobError(
                job_id=job_id,
                timestamp=datetime.utcnow().isoformat(),
                mensaje=event.get("mensaje", "Error desconocido"),
                recuperable=event.get("recuperable", False),
            )
            await self._emit_event(gui_event)
