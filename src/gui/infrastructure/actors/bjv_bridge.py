"""
BJV GUI Bridge Actor

Bridges the GUI to the BJV scraper actor system.
Translates GUI commands into BJV-specific actor messages and
forwards scraper events back to the GUI.
"""
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Any, Callable, Dict, Tuple
from uuid import uuid4
import asyncio

from src.infrastructure.actors.base import BaseActor
from src.domain.bjv_value_objects import AreaDerecho
from src.domain.bjv_events import (
    BusquedaIniciada,
    LibroDescubierto,
    LibroDescargado,
    LibroProcesado,
    ErrorScraping,
    ScrapingCompletado,
    ScrapingPausado,
    ScrapingReanudado,
)


@dataclass(frozen=True)
class BJVSearchConfig:
    """
    Configuration for BJV book search.

    Immutable configuration for a search operation.
    """
    termino_busqueda: Optional[str] = None
    area_derecho: Optional[AreaDerecho] = None
    max_resultados: int = 100
    incluir_capitulos: bool = True
    descargar_pdfs: bool = True
    rate_limit: float = 1.0
    concurrency: int = 2


@dataclass(frozen=True)
class BJVProgress:
    """
    Progress information for BJV scraping.

    Immutable progress snapshot.
    """
    libros_descubiertos: int = 0
    libros_descargados: int = 0
    libros_pendientes: int = 0
    descargas_activas: int = 0
    errores: int = 0
    estado: str = "IDLE"


@dataclass(frozen=True)
class BJVJobStarted:
    """Event: BJV scraping job started."""
    job_id: str
    config: BJVSearchConfig
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass(frozen=True)
class BJVJobProgress:
    """Event: BJV scraping progress update."""
    job_id: str
    progress: BJVProgress
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass(frozen=True)
class BJVJobCompleted:
    """Event: BJV scraping job completed."""
    job_id: str
    total_libros_descubiertos: int
    total_libros_descargados: int
    total_errores: int
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass(frozen=True)
class BJVJobFailed:
    """Event: BJV scraping job failed."""
    job_id: str
    mensaje_error: str
    timestamp: datetime = field(default_factory=datetime.now)


class BJVGuiBridgeActor(BaseActor):
    """
    Actor that bridges the GUI with the BJV scraper actor system.

    Responsibilities:
    - Connect to BJV coordinator actor (when implemented)
    - Translate GUI commands into BJV-specific messages
    - Poll for progress updates
    - Forward events to GUI event handler

    Messages:
    - ("START_SEARCH", config) -> Starts BJV search
    - "STOP_SEARCH" -> Stops current search
    - "PAUSE_SEARCH" -> Pauses search
    - "RESUME_SEARCH" -> Resumes search
    - "GET_STATUS" -> Returns current status
    - "GET_PROGRESS" -> Returns progress info
    """

    def __init__(
        self,
        coordinator_actor: Any = None,
        event_handler: Optional[Callable] = None,
        poll_interval: float = 2.0,
    ):
        super().__init__()
        self._coordinator = coordinator_actor
        self._event_handler = event_handler
        self._poll_interval = poll_interval
        self._current_job_id: Optional[str] = None
        self._is_polling = False
        self._poll_task: Optional[asyncio.Task] = None
        self._current_progress = BJVProgress()
        self._is_paused = False
        # Simulated state for demo (will be replaced with real coordinator)
        self._simulated_discovered = 0
        self._simulated_downloaded = 0

    @property
    def is_connected(self) -> bool:
        """Check if bridge is connected to coordinator."""
        # For now, always return True for demo purposes
        # Will check coordinator when implemented
        return True

    @property
    def current_job_id(self) -> Optional[str]:
        """Get current job ID if any."""
        return self._current_job_id

    async def start(self):
        """Start the bridge actor."""
        await super().start()

    async def stop(self):
        """Stop the bridge actor and cleanup."""
        await self._stop_polling()
        await super().stop()

    async def handle_message(self, message):
        """Process incoming messages."""
        # String messages
        if isinstance(message, str):
            if message == "STOP_SEARCH":
                return await self._handle_stop_search()
            elif message == "PAUSE_SEARCH":
                return await self._handle_pause_search()
            elif message == "RESUME_SEARCH":
                return await self._handle_resume_search()
            elif message == "GET_STATUS":
                return await self._handle_get_status()
            elif message == "GET_PROGRESS":
                return await self._handle_get_progress()

        # Tuple messages
        elif isinstance(message, tuple):
            command = message[0]

            if command == "START_SEARCH":
                config = message[1]
                return await self._handle_start_search(config)
            elif command == "SET_COORDINATOR":
                self._coordinator = message[1]
                return {"success": True}

        return None

    async def _handle_start_search(self, config: BJVSearchConfig) -> Dict[str, Any]:
        """Handle START_SEARCH command."""
        if self._current_job_id:
            return {"success": False, "error": "Search already in progress"}

        # Generate job ID
        self._current_job_id = str(uuid4())
        self._is_paused = False
        self._simulated_discovered = 0
        self._simulated_downloaded = 0

        # Emit job started event
        if self._event_handler:
            await self._emit_event(BJVJobStarted(
                job_id=self._current_job_id,
                config=config,
            ))

        # Start polling for progress (simulated for now)
        await self._start_polling(config)
        return {"success": True, "job_id": self._current_job_id}

    async def _handle_stop_search(self) -> Dict[str, Any]:
        """Handle STOP_SEARCH command."""
        await self._stop_polling()

        job_id = self._current_job_id
        self._current_job_id = None
        self._is_paused = False

        return {"success": True, "stopped_job_id": job_id}

    async def _handle_pause_search(self) -> Dict[str, Any]:
        """Handle PAUSE_SEARCH command."""
        if not self._current_job_id:
            return {"success": False, "error": "No active search to pause"}

        self._is_paused = True

        if self._event_handler:
            await self._emit_event(ScrapingPausado(
                razon="User requested pause",
                libros_pendientes=self._current_progress.libros_pendientes,
            ))

        return {"success": True, "paused": True}

    async def _handle_resume_search(self) -> Dict[str, Any]:
        """Handle RESUME_SEARCH command."""
        if not self._current_job_id:
            return {"success": False, "error": "No paused search to resume"}

        self._is_paused = False

        if self._event_handler:
            await self._emit_event(ScrapingReanudado(
                checkpoint_id=self._current_job_id,
                libros_restantes=self._current_progress.libros_pendientes,
            ))

        return {"success": True, "resumed": True}

    async def _handle_get_status(self) -> Dict[str, Any]:
        """Handle GET_STATUS command."""
        state = "idle"
        if self._current_job_id:
            state = "paused" if self._is_paused else "running"

        return {
            "connected": self.is_connected,
            "current_job_id": self._current_job_id,
            "is_polling": self._is_polling,
            "state": state,
        }

    async def _handle_get_progress(self) -> Optional[BJVProgress]:
        """Handle GET_PROGRESS command."""
        return self._current_progress

    async def _start_polling(self, config: BJVSearchConfig):
        """Start polling for progress updates."""
        if self._is_polling:
            return

        self._is_polling = True
        self._poll_task = asyncio.create_task(self._poll_loop(config))

    async def _stop_polling(self):
        """Stop polling for progress updates."""
        self._is_polling = False
        if self._poll_task:
            self._poll_task.cancel()
            try:
                await self._poll_task
            except asyncio.CancelledError:
                pass
            self._poll_task = None

    async def _poll_loop(self, config: BJVSearchConfig):
        """Main polling loop for progress updates (simulated)."""
        # Simulate discovering books
        max_books = min(config.max_resultados, 50)  # Cap at 50 for demo

        while self._is_polling and self._current_job_id:
            try:
                if not self._is_paused:
                    # Simulate discovery
                    if self._simulated_discovered < max_books:
                        self._simulated_discovered += 5
                        self._simulated_discovered = min(self._simulated_discovered, max_books)

                    # Simulate downloads (lag behind discovery)
                    if self._simulated_downloaded < self._simulated_discovered:
                        self._simulated_downloaded += 2
                        self._simulated_downloaded = min(
                            self._simulated_downloaded,
                            self._simulated_discovered
                        )

                # Update progress
                pending = self._simulated_discovered - self._simulated_downloaded
                self._current_progress = BJVProgress(
                    libros_descubiertos=self._simulated_discovered,
                    libros_descargados=self._simulated_downloaded,
                    libros_pendientes=pending,
                    descargas_activas=min(2, pending) if not self._is_paused else 0,
                    errores=0,
                    estado="PAUSED" if self._is_paused else "RUNNING",
                )

                # Emit progress event
                if self._event_handler:
                    await self._emit_event(BJVJobProgress(
                        job_id=self._current_job_id,
                        progress=self._current_progress,
                    ))

                # Check for completion
                if (
                    self._simulated_discovered >= max_books and
                    self._simulated_downloaded >= self._simulated_discovered
                ):
                    # Job completed
                    await self._emit_event(BJVJobCompleted(
                        job_id=self._current_job_id,
                        total_libros_descubiertos=self._simulated_discovered,
                        total_libros_descargados=self._simulated_downloaded,
                        total_errores=0,
                    ))
                    self._current_job_id = None
                    self._is_polling = False
                    break

                await asyncio.sleep(self._poll_interval)

            except asyncio.CancelledError:
                break
            except Exception as e:
                # Emit failure event
                if self._current_job_id and self._event_handler:
                    await self._emit_event(BJVJobFailed(
                        job_id=self._current_job_id,
                        mensaje_error=str(e),
                    ))
                break

    async def _emit_event(self, event: Any):
        """Emit event to handler."""
        if self._event_handler:
            try:
                if asyncio.iscoroutinefunction(self._event_handler):
                    await self._event_handler(event)
                else:
                    self._event_handler(event)
            except Exception:
                pass  # Don't let handler errors break the bridge
