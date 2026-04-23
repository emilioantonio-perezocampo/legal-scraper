"""
DOF GUI Bridge Actor.

Connects the GUI layer to the DOF (Diario Oficial de la Federación) scraper,
providing a clean interface for starting, monitoring,
and controlling DOF scraping jobs.
"""
from dataclasses import dataclass, field, asdict
from typing import Optional, Callable, Awaitable, Dict, Any
from datetime import datetime, timezone, date as dt_date, timedelta
import asyncio
import logging
import uuid

logger = logging.getLogger(__name__)


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

        Creates the actor chain (discovery → scraper → persistence) and
        starts the discovery process directly.
        """
        job_id = f"dof-gui-{uuid.uuid4().hex[:8]}"
        self._current_job_id = job_id
        config_dict = config.to_dict()

        event = DOFJobStarted(
            job_id=job_id,
            timestamp=datetime.now(timezone.utc).isoformat(),
            config=config_dict,
        )
        await self._emit_event(event)

        # Run DOF scraping in background task
        asyncio.create_task(self._run_dof_scrape(config, job_id))

        return job_id

    async def _run_dof_scrape(self, config: DOFGuiConfig, job_id: str):
        """Run DOF discovery and scraping directly."""
        import aiohttp
        from src.infrastructure.adapters.dof_index_parser import parse_dof_index
        import os

        total_saved = 0
        total_errors = 0

        # Build date list
        if config.mode == "today":
            dates = [dt_date.today()]
        elif config.mode == "range" and config.start_date and config.end_date:
            start = dt_date.fromisoformat(config.start_date)
            end = dt_date.fromisoformat(config.end_date)
            dates = []
            current = start
            while current <= end:
                dates.append(current)
                current += timedelta(days=1)
        else:
            dates = [dt_date.today()]

        logger.info(f"DOF scrape: {len(dates)} days to process")

        # Supabase client for persistence
        supabase_client = None
        url = os.environ.get("SUPABASE_URL")
        key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
        if url and key:
            try:
                from supabase import create_client
                supabase_client = create_client(url, key)
            except Exception as e:
                logger.warning(f"Supabase not available: {e}")

        try:
            async with aiohttp.ClientSession() as session:
                for i, target_date in enumerate(dates):
                    dof_url = (
                        f"https://dof.gob.mx/index.php?"
                        f"year={target_date.year}&month={target_date.month:02d}"
                        f"&day={target_date.day:02d}"
                    )
                    try:
                        async with session.get(dof_url, ssl=False, timeout=aiohttp.ClientTimeout(total=30)) as response:
                            if response.status == 200:
                                html = await response.text(encoding='utf-8')
                                items = parse_dof_index(html)
                                if items:
                                    for item in items:
                                        title = item.get("title", "Sin título")
                                        doc_url = item.get("url", "")
                                        # Extract numeric DOF code from URL
                                        import re as _re
                                        code_match = _re.search(r'codigo=(\d+)', doc_url)
                                        ext_id = code_match.group(1) if code_match else f"dof-{target_date}-{total_saved}"

                                        if supabase_client:
                                            try:
                                                supabase_client.table("scraper_documents").upsert({
                                                    "source_type": "dof",
                                                    "external_id": ext_id,
                                                    "title": title,
                                                    "publication_date": str(target_date),
                                                }, on_conflict="source_type,external_id").execute()
                                                total_saved += 1
                                            except Exception as e:
                                                total_errors += 1
                                                logger.debug(f"Upsert error: {e}")
                                        else:
                                            total_saved += 1

                                    logger.info(f"DOF {target_date}: {len(items)} docs (total: {total_saved})")
                    except Exception as e:
                        total_errors += 1
                        logger.warning(f"DOF error {target_date}: {e}")

                    # Emit progress
                    if i % 5 == 0:
                        await self._emit_event(DOFJobProgress(
                            job_id=job_id,
                            estado="RUNNING",
                            total_documents=total_saved,
                            processed_documents=i + 1,
                            errores=total_errors,
                            porcentaje=((i + 1) / len(dates)) * 100,
                        ))

                    # Rate limit
                    await asyncio.sleep(1)

        except Exception as e:
            logger.error(f"DOF scrape failed: {e}")

        # Emit completion
        await self._emit_event(DOFJobCompleted(
            job_id=job_id,
            timestamp=datetime.now(timezone.utc).isoformat(),
            estadisticas={"documents_saved": total_saved, "errors": total_errors},
            exito=total_errors == 0,
        ))
        self._current_job_id = None
        logger.info(f"DOF scrape complete: {total_saved} docs, {total_errors} errors")

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
