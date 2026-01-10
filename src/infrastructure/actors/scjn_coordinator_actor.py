"""
SCJN Coordinator Actor

Orchestrates the SCJN scraping pipeline.
Coordinates discovery, download, persistence, and checkpoint actors.
"""
import asyncio
from enum import Enum
from typing import Optional, Dict, Set, Any
from collections import deque

from .base import BaseActor
from .messages import (
    DescubrirDocumentos,
    DescargarDocumento,
    DocumentoDescubierto,
    DocumentoDescargado,
    PaginaDescubierta,
    PDFProcesado,
    EmbeddingsGenerados,
    DocumentoGuardado,
    GuardarCheckpoint,
    CheckpointGuardado,
    PausarPipeline,
    ReanudarPipeline,
    ObtenerEstado,
    ErrorDeActor,
)
from .rate_limiter import RateLimiter


class PipelineState(Enum):
    """Pipeline execution states."""
    IDLE = "idle"
    DISCOVERING = "discovering"
    DOWNLOADING = "downloading"
    PROCESSING = "processing"
    PAUSED = "paused"
    COMPLETED = "completed"
    ERROR = "error"


class SCJNCoordinatorActor(BaseActor):
    """
    Orchestrates the SCJN document scraping pipeline.

    Coordinates:
    - Discovery actor for finding documents
    - Scraper actor for downloading documents
    - Persistence actor for saving data
    - Checkpoint actor for progress saving

    Message Protocol:
    - DescubrirDocumentos → starts discovery
    - DocumentoDescubierto → queues for download
    - DocumentoDescargado → updates progress
    - PausarPipeline → pauses and saves checkpoint
    - ReanudarPipeline → resumes from checkpoint
    - ObtenerEstado → returns current state (ask pattern)
    """

    def __init__(
        self,
        discovery_actor: 'BaseActor',
        scraper_actor: 'BaseActor',
        persistence_actor: 'BaseActor',
        checkpoint_actor: 'BaseActor',
        rate_limiter: RateLimiter,
        max_concurrent_downloads: int = 3,
        checkpoint_interval: int = 10,
    ):
        super().__init__()
        self._discovery_actor = discovery_actor
        self._scraper_actor = scraper_actor
        self._persistence_actor = persistence_actor
        self._checkpoint_actor = checkpoint_actor
        self._rate_limiter = rate_limiter
        self._max_concurrent_downloads = max_concurrent_downloads
        self._checkpoint_interval = checkpoint_interval

        # State tracking
        self._state = PipelineState.IDLE
        self._discovered_q_params: Set[str] = set()
        self._downloaded_q_params: Set[str] = set()
        self._pending_queue: deque = deque()
        self._active_downloads: int = 0
        self._error_count: int = 0
        self._current_correlation_id: Optional[str] = None

        # Retry tracking
        self._retry_queue: deque = deque()
        self._max_retries: int = 3
        self._retry_counts: Dict[str, int] = {}

    @property
    def state(self) -> PipelineState:
        """Get current pipeline state."""
        return self._state

    async def start(self):
        """Start the coordinator and child actors."""
        await super().start()
        self._state = PipelineState.IDLE

    async def stop(self):
        """Stop the coordinator."""
        await super().stop()

    async def handle_message(self, message):
        """Handle incoming messages."""
        # Commands
        if isinstance(message, DescubrirDocumentos):
            return await self._handle_discover(message)

        elif isinstance(message, DescargarDocumento):
            return await self._handle_download_request(message)

        elif isinstance(message, PausarPipeline):
            return await self._handle_pause(message)

        elif isinstance(message, ReanudarPipeline):
            return await self._handle_resume(message)

        elif isinstance(message, ObtenerEstado):
            return self._get_state()

        # Events from child actors
        elif isinstance(message, DocumentoDescubierto):
            return await self._handle_documento_descubierto(message)

        elif isinstance(message, DocumentoDescargado):
            return await self._handle_documento_descargado(message)

        elif isinstance(message, PaginaDescubierta):
            return await self._handle_pagina_descubierta(message)

        elif isinstance(message, ErrorDeActor):
            return await self._handle_error(message)

        return None

    async def _handle_discover(self, cmd: DescubrirDocumentos):
        """Handle discovery command."""
        self._state = PipelineState.DISCOVERING
        self._current_correlation_id = cmd.correlation_id

        # Forward to discovery actor
        result = await self._discovery_actor.ask(cmd)

        if isinstance(result, PaginaDescubierta):
            if not result.has_more_pages:
                self._state = PipelineState.DOWNLOADING if self._pending_queue else PipelineState.IDLE

        return result

    async def _handle_download_request(self, cmd: DescargarDocumento):
        """Handle direct download request."""
        return await self._scraper_actor.ask(cmd)

    async def _handle_documento_descubierto(self, event: DocumentoDescubierto):
        """Handle discovered document event."""
        q_param = event.q_param

        # Skip if already discovered
        if q_param in self._discovered_q_params:
            return None

        self._discovered_q_params.add(q_param)

        # Check if already downloaded
        exists = await self._persistence_actor.ask(("EXISTS", q_param))
        if exists:
            return None

        # Queue for download
        self._pending_queue.append(event)

        # Trigger download if capacity available
        await self._process_download_queue()

        return None

    async def _handle_documento_descargado(self, event: DocumentoDescargado):
        """Handle downloaded document event."""
        self._downloaded_q_params.add(event.q_param)
        self._active_downloads = max(0, self._active_downloads - 1)

        # Check for checkpoint
        if len(self._downloaded_q_params) % self._checkpoint_interval == 0:
            await self._save_checkpoint()

        # Process more downloads
        await self._process_download_queue()

        # Update state
        if not self._pending_queue and self._active_downloads == 0:
            if self._state == PipelineState.DOWNLOADING:
                self._state = PipelineState.IDLE

        return None

    async def _handle_pagina_descubierta(self, event: PaginaDescubierta):
        """Handle page discovered event."""
        if not event.has_more_pages:
            if self._pending_queue:
                self._state = PipelineState.DOWNLOADING
            elif self._state == PipelineState.DISCOVERING:
                self._state = PipelineState.IDLE

        return None

    async def _handle_pause(self, cmd: PausarPipeline):
        """Handle pause command."""
        self._state = PipelineState.PAUSED
        await self._save_checkpoint()
        return {"paused": True, "state": self._state}

    async def _handle_resume(self, cmd: ReanudarPipeline):
        """Handle resume command."""
        if self._state == PipelineState.PAUSED:
            if self._pending_queue:
                self._state = PipelineState.DOWNLOADING
                await self._process_download_queue()
            else:
                self._state = PipelineState.IDLE

        return {"resumed": True, "state": self._state}

    async def _handle_error(self, error: ErrorDeActor):
        """Handle error from child actor."""
        self._error_count += 1

        if error.recoverable and error.original_command:
            # Check retry count
            cmd = error.original_command
            if isinstance(cmd, DescargarDocumento):
                q_param = cmd.q_param
                count = self._retry_counts.get(q_param, 0)
                if count < self._max_retries:
                    self._retry_counts[q_param] = count + 1
                    self._retry_queue.append(cmd)

        return None

    async def _process_download_queue(self):
        """Process pending downloads up to concurrency limit."""
        if self._state == PipelineState.PAUSED:
            return

        while self._pending_queue and self._active_downloads < self._max_concurrent_downloads:
            event = self._pending_queue.popleft()
            self._active_downloads += 1

            # Create download command
            cmd = DescargarDocumento(
                q_param=event.q_param,
                correlation_id=event.correlation_id,
            )

            # Fire and forget - response comes back as event
            asyncio.create_task(self._execute_download(cmd))

    async def _execute_download(self, cmd: DescargarDocumento):
        """Execute a single download."""
        try:
            await self._scraper_actor.tell(cmd)
        except Exception:
            self._active_downloads = max(0, self._active_downloads - 1)

    async def _save_checkpoint(self):
        """Save current progress to checkpoint."""
        from src.domain.scjn_entities import ScrapingCheckpoint

        checkpoint = ScrapingCheckpoint(
            session_id=self._current_correlation_id or "default",
            last_processed_q_param=list(self._downloaded_q_params)[-1] if self._downloaded_q_params else "",
            processed_count=len(self._downloaded_q_params),
            failed_q_params=tuple(self._retry_counts.keys()),
        )

        await self._checkpoint_actor.tell(
            GuardarCheckpoint(
                correlation_id=self._current_correlation_id or "",
                checkpoint=checkpoint,
            )
        )

    def _get_state(self) -> Dict[str, Any]:
        """Get current pipeline state."""
        return {
            "state": self._state,
            "discovered_count": len(self._discovered_q_params),
            "downloaded_count": len(self._downloaded_q_params),
            "pending_count": len(self._pending_queue),
            "active_downloads": self._active_downloads,
            "error_count": self._error_count,
        }
