"""
SCJN GUI Bridge Actor

Bridges the GUI to the SCJN scraper actor system.
Translates GUI commands into SCJN-specific actor messages and
forwards scraper events back to the GUI.
"""
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Any, Callable, Dict
from uuid import uuid4
import asyncio

from src.infrastructure.actors.base import BaseActor
from src.infrastructure.actors.messages import (
    DescubrirDocumentos,
    ObtenerEstado,
    PausarPipeline,
    ReanudarPipeline,
    DocumentoDescubierto,
    DocumentoDescargado,
    PaginaDescubierta,
    ErrorDeActor,
)


@dataclass(frozen=True)
class SCJNSearchConfig:
    """
    Configuration for SCJN document search.

    Immutable configuration for a search operation.
    """
    category: Optional[str] = None
    scope: Optional[str] = None
    status: Optional[str] = None
    max_results: int = 100
    discover_all_pages: bool = False
    skip_pdfs: bool = False
    rate_limit: float = 0.5
    concurrency: int = 3


@dataclass(frozen=True)
class SCJNProgress:
    """
    Progress information for SCJN scraping.

    Immutable progress snapshot.
    """
    discovered_count: int = 0
    downloaded_count: int = 0
    pending_count: int = 0
    active_downloads: int = 0
    error_count: int = 0
    state: str = "IDLE"


@dataclass(frozen=True)
class SCJNJobStarted:
    """Event: SCJN scraping job started."""
    job_id: str
    config: SCJNSearchConfig
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass(frozen=True)
class SCJNJobProgress:
    """Event: SCJN scraping progress update."""
    job_id: str
    progress: SCJNProgress
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass(frozen=True)
class SCJNJobCompleted:
    """Event: SCJN scraping job completed."""
    job_id: str
    total_discovered: int
    total_downloaded: int
    total_errors: int
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass(frozen=True)
class SCJNJobFailed:
    """Event: SCJN scraping job failed."""
    job_id: str
    error_message: str
    timestamp: datetime = field(default_factory=datetime.now)


class SCJNGuiBridgeActor(BaseActor):
    """
    Actor that bridges the GUI with the SCJN scraper actor system.

    Responsibilities:
    - Connect to SCJN coordinator actor
    - Translate GUI commands into SCJN-specific messages
    - Poll for progress updates
    - Forward events to GUI event handler

    Messages:
    - ("START_SEARCH", config) -> Starts SCJN search
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

    @property
    def is_connected(self) -> bool:
        """Check if bridge is connected to coordinator."""
        return self._coordinator is not None

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

    async def _handle_start_search(self, config: SCJNSearchConfig) -> Dict[str, Any]:
        """Handle START_SEARCH command."""
        if not self._coordinator:
            return {"success": False, "error": "Coordinator not connected"}

        if self._current_job_id:
            return {"success": False, "error": "Search already in progress"}

        # Generate job ID
        self._current_job_id = str(uuid4())

        # Emit job started event
        if self._event_handler:
            await self._emit_event(SCJNJobStarted(
                job_id=self._current_job_id,
                config=config,
            ))

        # Send discovery command to coordinator
        cmd = DescubrirDocumentos(
            category=config.category,
            scope=config.scope,
            status=config.status,
            max_results=config.max_results,
            discover_all_pages=config.discover_all_pages,
        )

        try:
            result = await self._coordinator.ask(cmd)
            # Start polling for progress
            await self._start_polling()
            return {"success": True, "job_id": self._current_job_id}
        except Exception as e:
            self._current_job_id = None
            return {"success": False, "error": str(e)}

    async def _handle_stop_search(self) -> Dict[str, Any]:
        """Handle STOP_SEARCH command."""
        await self._stop_polling()

        if self._coordinator:
            try:
                await self._coordinator.ask(PausarPipeline())
            except Exception:
                pass

        job_id = self._current_job_id
        self._current_job_id = None

        return {"success": True, "stopped_job_id": job_id}

    async def _handle_pause_search(self) -> Dict[str, Any]:
        """Handle PAUSE_SEARCH command."""
        if not self._coordinator:
            return {"success": False, "error": "Coordinator not connected"}

        try:
            result = await self._coordinator.ask(PausarPipeline())
            return {"success": True, "paused": True}
        except Exception as e:
            return {"success": False, "error": str(e)}

    async def _handle_resume_search(self) -> Dict[str, Any]:
        """Handle RESUME_SEARCH command."""
        if not self._coordinator:
            return {"success": False, "error": "Coordinator not connected"}

        try:
            result = await self._coordinator.ask(ReanudarPipeline())
            return {"success": True, "resumed": True}
        except Exception as e:
            return {"success": False, "error": str(e)}

    async def _handle_get_status(self) -> Dict[str, Any]:
        """Handle GET_STATUS command."""
        return {
            "connected": self.is_connected,
            "current_job_id": self._current_job_id,
            "is_polling": self._is_polling,
        }

    async def _handle_get_progress(self) -> Optional[SCJNProgress]:
        """Handle GET_PROGRESS command."""
        if not self._coordinator:
            return None

        try:
            result = await self._coordinator.ask(ObtenerEstado())
            return SCJNProgress(
                discovered_count=result.get("discovered_count", 0),
                downloaded_count=result.get("downloaded_count", 0),
                pending_count=result.get("pending_count", 0),
                active_downloads=result.get("active_downloads", 0),
                error_count=result.get("error_count", 0),
                state=result.get("state", "UNKNOWN"),
            )
        except Exception:
            return None

    async def _start_polling(self):
        """Start polling for progress updates."""
        if self._is_polling:
            return

        self._is_polling = True
        self._poll_task = asyncio.create_task(self._poll_loop())

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

    async def _poll_loop(self):
        """Main polling loop for progress updates."""
        last_downloaded = 0
        stall_count = 0

        while self._is_polling and self._current_job_id:
            try:
                progress = await self._handle_get_progress()

                if progress:
                    # Emit progress event
                    await self._emit_event(SCJNJobProgress(
                        job_id=self._current_job_id,
                        progress=progress,
                    ))

                    # Check for completion
                    if (
                        progress.pending_count == 0 and
                        progress.active_downloads == 0 and
                        progress.discovered_count > 0
                    ):
                        # Job completed
                        await self._emit_event(SCJNJobCompleted(
                            job_id=self._current_job_id,
                            total_discovered=progress.discovered_count,
                            total_downloaded=progress.downloaded_count,
                            total_errors=progress.error_count,
                        ))
                        self._current_job_id = None
                        self._is_polling = False
                        break

                    # Check for stall
                    if progress.downloaded_count == last_downloaded:
                        stall_count += 1
                    else:
                        stall_count = 0
                        last_downloaded = progress.downloaded_count

                    if stall_count >= 10:
                        # Stalled - treat as completed
                        await self._emit_event(SCJNJobCompleted(
                            job_id=self._current_job_id,
                            total_discovered=progress.discovered_count,
                            total_downloaded=progress.downloaded_count,
                            total_errors=progress.error_count,
                        ))
                        self._current_job_id = None
                        self._is_polling = False
                        break

                await asyncio.sleep(self._poll_interval)

            except asyncio.CancelledError:
                break
            except Exception as e:
                # Emit failure event
                if self._current_job_id:
                    await self._emit_event(SCJNJobFailed(
                        job_id=self._current_job_id,
                        error_message=str(e),
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
