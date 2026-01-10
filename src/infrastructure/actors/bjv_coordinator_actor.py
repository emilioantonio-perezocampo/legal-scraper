"""
BJV Coordinator Actor.

Orchestrates the scraping pipeline by coordinating all child actors.
"""
import uuid
from datetime import datetime, timezone
from enum import Enum, auto
from typing import Dict, Optional, Set, Any

from src.infrastructure.actors.bjv_base_actor import BJVBaseActor
from src.infrastructure.actors.bjv_messages import (
    IniciarPipeline,
    PausarPipeline,
    ReanudarPipeline,
    DetenerPipeline,
    ObtenerEstado,
    IniciarBusqueda,
    DescargarLibro,
    BusquedaCompletada,
    LibroListo,
    PDFListo,
    TextoFragmentado,
    ErrorProcesamiento,
)
from src.infrastructure.actors.bjv_rate_limiter import BJVRateLimiter, RateLimiterConfig
from src.infrastructure.actors.bjv_discovery_actor import BJVDiscoveryActor
from src.infrastructure.actors.bjv_scraper_actor import BJVScraperActor
from src.infrastructure.actors.bjv_pdf_actor import BJVPDFActor
from src.infrastructure.actors.bjv_fragmentador_actor import BJVFragmentadorActor
from src.domain.bjv_entities import PuntoControlScraping


class PipelineState(Enum):
    """Pipeline state machine."""
    IDLE = auto()
    RUNNING = auto()
    PAUSED = auto()
    STOPPING = auto()
    COMPLETED = auto()
    FAILED = auto()


class BJVCoordinatorActor(BJVBaseActor):
    """
    Coordinator actor that orchestrates the scraping pipeline.

    Responsibilities:
    - Manage pipeline state
    - Create and supervise child actors
    - Route messages between actors
    - Track statistics and progress
    - Handle checkpointing
    """

    def __init__(
        self,
        rate_limiter_config: Optional[RateLimiterConfig] = None,
        download_dir: Optional[str] = None,
    ):
        """
        Initialize the coordinator actor.

        Args:
            rate_limiter_config: Rate limiter configuration
            download_dir: Directory for PDF downloads
        """
        super().__init__(nombre="BJVCoordinator")
        self._rate_limiter_config = rate_limiter_config or RateLimiterConfig()
        self._download_dir = download_dir

        # Child actors (created on start)
        self._rate_limiter: Optional[BJVRateLimiter] = None
        self._discovery_actor: Optional[BJVDiscoveryActor] = None
        self._scraper_actor: Optional[BJVScraperActor] = None
        self._pdf_actor: Optional[BJVPDFActor] = None
        self._fragmentador_actor: Optional[BJVFragmentadorActor] = None

        # Pipeline state
        self._state = PipelineState.IDLE
        self._session_id: Optional[str] = None
        self._max_resultados = 100

        # Statistics
        self._total_descubiertos = 0
        self._total_procesados = 0
        self._total_errores = 0
        self._libros_procesados: Set[str] = set()
        self._libros_pendientes: Set[str] = set()
        self._discovery_complete = False

        # Checkpoint
        self._last_checkpoint: Optional[PuntoControlScraping] = None

    @property
    def state(self) -> PipelineState:
        """Get current pipeline state."""
        return self._state

    async def start(self) -> None:
        """Start the coordinator and create child actors."""
        await super().start()

        # Create rate limiter
        self._rate_limiter = BJVRateLimiter(config=self._rate_limiter_config)

        # Create child actors
        self._discovery_actor = BJVDiscoveryActor(
            coordinator=self,
            rate_limiter=self._rate_limiter,
        )
        self._scraper_actor = BJVScraperActor(
            coordinator=self,
            rate_limiter=self._rate_limiter,
        )
        self._pdf_actor = BJVPDFActor(
            coordinator=self,
            rate_limiter=self._rate_limiter,
            download_dir=self._download_dir,
        )
        self._fragmentador_actor = BJVFragmentadorActor(coordinator=self)

        # Start child actors
        await self._discovery_actor.start()
        await self._scraper_actor.start()
        await self._pdf_actor.start()
        await self._fragmentador_actor.start()

    async def stop(self) -> None:
        """Stop the coordinator and all child actors."""
        # Stop child actors
        if self._discovery_actor:
            await self._discovery_actor.stop()
        if self._scraper_actor:
            await self._scraper_actor.stop()
        if self._pdf_actor:
            await self._pdf_actor.stop()
        if self._fragmentador_actor:
            await self._fragmentador_actor.stop()

        await super().stop()

    async def handle_message(self, message) -> Any:
        """
        Handle incoming messages.

        Args:
            message: Message to process

        Returns:
            Optional response for ask() calls
        """
        # Pipeline control
        if isinstance(message, IniciarPipeline):
            await self._handle_iniciar_pipeline(message)
        elif isinstance(message, PausarPipeline):
            await self._handle_pausar_pipeline(message)
        elif isinstance(message, ReanudarPipeline):
            await self._handle_reanudar_pipeline(message)
        elif isinstance(message, DetenerPipeline):
            await self._handle_detener_pipeline(message)
        elif isinstance(message, ObtenerEstado):
            return self.get_statistics()

        # Discovery results
        elif isinstance(message, BusquedaCompletada):
            await self._handle_busqueda_completada(message)
        elif isinstance(message, DescargarLibro):
            await self._handle_descargar_libro(message)

        # Processing results
        elif isinstance(message, LibroListo):
            await self._handle_libro_listo(message)
        elif isinstance(message, PDFListo):
            await self._handle_pdf_listo(message)
        elif isinstance(message, TextoFragmentado):
            await self._handle_texto_fragmentado(message)

        # Errors
        elif isinstance(message, ErrorProcesamiento):
            await self._handle_error(message)

    async def _handle_iniciar_pipeline(self, msg: IniciarPipeline) -> None:
        """Handle pipeline start."""
        if self._state != PipelineState.IDLE:
            return

        self._state = PipelineState.RUNNING
        self._session_id = msg.session_id
        self._max_resultados = msg.max_resultados

        # Reset statistics
        self._total_descubiertos = 0
        self._total_procesados = 0
        self._total_errores = 0
        self._libros_procesados.clear()
        self._libros_pendientes.clear()
        self._discovery_complete = False

        # Start discovery
        search_msg = IniciarBusqueda(
            correlation_id=str(uuid.uuid4()),
            query=msg.query,
            max_resultados=msg.max_resultados,
        )
        await self._discovery_actor.tell(search_msg)

    async def _handle_pausar_pipeline(self, msg: PausarPipeline) -> None:
        """Handle pipeline pause."""
        if self._state == PipelineState.RUNNING:
            self._state = PipelineState.PAUSED
            self._create_checkpoint()

    async def _handle_reanudar_pipeline(self, msg: ReanudarPipeline) -> None:
        """Handle pipeline resume."""
        if self._state == PipelineState.PAUSED:
            self._state = PipelineState.RUNNING

    async def _handle_detener_pipeline(self, msg: DetenerPipeline) -> None:
        """Handle pipeline stop."""
        if self._state in (PipelineState.RUNNING, PipelineState.PAUSED):
            self._state = PipelineState.STOPPING
            if msg.guardar_checkpoint:
                self._create_checkpoint()
            self._state = PipelineState.COMPLETED

    async def _handle_busqueda_completada(self, msg: BusquedaCompletada) -> None:
        """Handle discovery completion."""
        self._total_descubiertos = msg.total_descubiertos
        self._discovery_complete = True
        self._check_completion()

    async def _handle_descargar_libro(self, msg: DescargarLibro) -> None:
        """Handle book download request from discovery."""
        if self._state != PipelineState.RUNNING:
            return

        self._libros_pendientes.add(msg.libro_id)
        await self._scraper_actor.tell(msg)

    async def _handle_libro_listo(self, msg: LibroListo) -> None:
        """Handle book ready event."""
        if msg.libro_id in self._libros_pendientes:
            self._libros_pendientes.discard(msg.libro_id)
            self._libros_procesados.add(msg.libro_id)
            self._total_procesados += 1

        self._check_completion()

    async def _handle_pdf_listo(self, msg: PDFListo) -> None:
        """Handle PDF ready event."""
        # Could trigger text extraction here if needed
        pass

    async def _handle_texto_fragmentado(self, msg: TextoFragmentado) -> None:
        """Handle text fragmented event."""
        # Could trigger embedding generation here
        pass

    async def _handle_error(self, msg: ErrorProcesamiento) -> None:
        """Handle processing error."""
        self._total_errores += 1

        if msg.libro_id and msg.libro_id in self._libros_pendientes:
            self._libros_pendientes.discard(msg.libro_id)

        self._check_completion()

    def _check_completion(self) -> None:
        """Check if pipeline is complete."""
        if not self._discovery_complete:
            return

        if self._state != PipelineState.RUNNING:
            return

        # Complete if all discovered books are processed or errored
        if len(self._libros_pendientes) == 0:
            pending_count = self._total_descubiertos - self._total_procesados - self._total_errores
            if pending_count <= 0:
                self._state = PipelineState.COMPLETED

    def _create_checkpoint(self) -> None:
        """Create a checkpoint of current state."""
        self._last_checkpoint = PuntoControlScraping(
            id=str(uuid.uuid4()),
            ultima_pagina_procesada=0,  # Would need to track this
            ultimo_libro_id=list(self._libros_procesados)[-1] if self._libros_procesados else None,
            total_libros_descubiertos=self._total_descubiertos,
            total_libros_descargados=self._total_procesados,
            timestamp=datetime.now(timezone.utc),
            total_errores=self._total_errores,
        )

    def get_checkpoint(self) -> Optional[PuntoControlScraping]:
        """Get the last checkpoint."""
        return self._last_checkpoint

    def get_statistics(self) -> Dict[str, Any]:
        """Get pipeline statistics."""
        return {
            "state": self._state.name,
            "session_id": self._session_id,
            "total_descubiertos": self._total_descubiertos,
            "total_procesados": self._total_procesados,
            "total_errores": self._total_errores,
            "pendientes": len(self._libros_pendientes),
            "discovery_complete": self._discovery_complete,
        }
