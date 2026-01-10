"""
CAS Coordinator Actor

Supervisor actor that coordinates the CAS scraping pipeline.
Manages discovery, scraping, and fragmentation actors.
"""
import logging
from typing import Optional, Any

from src.infrastructure.actors.cas_base_actor import CASBaseActor
from src.infrastructure.actors.cas_messages import (
    EstadoPipelineCAS,
    EstadisticasPipelineCAS,
    IniciarDescubrimientoCAS,
    LaudoDescubiertoCAS,
    LaudoScrapeadoCAS,
    FragmentosListosCAS,
    ScrapearLaudoCAS,
    FragmentarLaudoCAS,
    PausarPipelineCAS,
    ReanudarPipelineCAS,
    DetenerPipelineCAS,
    ObtenerEstadoCAS,
    ErrorCASPipeline,
)


logger = logging.getLogger(__name__)


class CASCoordinatorActor(CASBaseActor):
    """
    Coordinator actor for the CAS scraping pipeline.

    Supervises child actors and coordinates the flow:
    Discovery → Scraping → Fragmentation

    Features:
    - Pipeline state management
    - Statistics tracking
    - Pause/Resume support
    - Error handling and retry coordination
    """

    def __init__(
        self,
        discovery_actor: Optional[CASBaseActor] = None,
        scraper_actor: Optional[CASBaseActor] = None,
        fragmentador_actor: Optional[CASBaseActor] = None,
        actor_id: Optional[str] = None,
        supervisor: Optional[CASBaseActor] = None,
    ):
        super().__init__(actor_id=actor_id or "cas-coordinator", supervisor=supervisor)
        self._discovery_actor = discovery_actor
        self._scraper_actor = scraper_actor
        self._fragmentador_actor = fragmentador_actor
        self._estadisticas = EstadisticasPipelineCAS()
        self._estado_previo: Optional[EstadoPipelineCAS] = None

    @property
    def discovery_actor(self) -> Optional[CASBaseActor]:
        """Get discovery actor."""
        return self._discovery_actor

    @property
    def scraper_actor(self) -> Optional[CASBaseActor]:
        """Get scraper actor."""
        return self._scraper_actor

    @property
    def fragmentador_actor(self) -> Optional[CASBaseActor]:
        """Get fragmentador actor."""
        return self._fragmentador_actor

    @property
    def estadisticas(self) -> EstadisticasPipelineCAS:
        """Get pipeline statistics."""
        return self._estadisticas

    # Pipeline control handlers

    async def handle_iniciar_descubrimiento_cas(
        self, msg: IniciarDescubrimientoCAS
    ) -> None:
        """
        Start discovery phase.

        Args:
            msg: Discovery start message
        """
        self._set_estado(EstadoPipelineCAS.DESCUBRIENDO)
        logger.info("Starting CAS pipeline discovery")

        if self._discovery_actor:
            await self._discovery_actor.tell(msg)
        else:
            logger.warning("No discovery actor configured")

    async def handle_laudo_descubierto_cas(self, msg: LaudoDescubiertoCAS) -> None:
        """
        Handle discovered award - forward to scraper.

        Args:
            msg: Discovered award message
        """
        self._estadisticas = self._estadisticas.con_incremento("laudos_descubiertos")
        logger.debug(f"Award discovered: {msg.numero_caso}")

        if self._estado == EstadoPipelineCAS.PAUSADO:
            logger.debug("Pipeline paused, queuing scrape request")
            return

        if self._scraper_actor:
            scrape_msg = ScrapearLaudoCAS(
                numero_caso=msg.numero_caso,
                url=msg.url,
            )
            await self._scraper_actor.tell(scrape_msg)
        else:
            logger.warning("No scraper actor configured")

    async def handle_laudo_scrapeado_cas(self, msg: LaudoScrapeadoCAS) -> None:
        """
        Handle scraped award - forward to fragmentador.

        Args:
            msg: Scraped award message
        """
        self._estadisticas = self._estadisticas.con_incremento("laudos_scrapeados")
        logger.debug(f"Award scraped: {msg.laudo.numero_caso.valor}")

        if self._estado == EstadoPipelineCAS.PAUSADO:
            logger.debug("Pipeline paused, queuing fragment request")
            return

        if self._fragmentador_actor:
            # Get text content from laudo (resumen or other text field)
            texto = msg.laudo.resumen or ""
            if hasattr(msg.laudo, 'texto_completo') and msg.laudo.texto_completo:
                texto = msg.laudo.texto_completo

            fragment_msg = FragmentarLaudoCAS(
                laudo_id=msg.laudo.id,
                texto=texto,
            )
            await self._fragmentador_actor.tell(fragment_msg)
        else:
            logger.warning("No fragmentador actor configured")

    async def handle_fragmentos_listos_cas(self, msg: FragmentosListosCAS) -> None:
        """
        Handle completed fragments.

        Args:
            msg: Fragments ready message
        """
        self._estadisticas = self._estadisticas.con_incremento("laudos_fragmentados")
        logger.debug(f"Fragments ready for {msg.laudo_id}: {len(msg.fragmentos)} fragments")

    # Pause/Resume handlers

    async def handle_pausar_pipeline_cas(self, msg: PausarPipelineCAS) -> None:
        """
        Pause the pipeline.

        Args:
            msg: Pause message
        """
        self._estado_previo = self._estado
        self._set_estado(EstadoPipelineCAS.PAUSADO)
        logger.info(f"Pipeline paused: {msg.razon}")

    async def handle_reanudar_pipeline_cas(self, msg: ReanudarPipelineCAS) -> None:
        """
        Resume the pipeline.

        Args:
            msg: Resume message
        """
        if self._estado == EstadoPipelineCAS.PAUSADO:
            if self._estado_previo:
                self._set_estado(self._estado_previo)
            else:
                self._set_estado(EstadoPipelineCAS.INICIANDO)
            self._estado_previo = None
            logger.info("Pipeline resumed")
        else:
            logger.warning("Pipeline not paused, ignoring resume")

    async def handle_detener_pipeline_cas(self, msg: DetenerPipelineCAS) -> None:
        """
        Stop the pipeline.

        Args:
            msg: Stop message
        """
        logger.info(f"Stopping pipeline (save_progress={msg.guardar_progreso})")

        if msg.guardar_progreso:
            logger.info(f"Final stats: {self._estadisticas}")

        # Stop child actors
        for actor in [self._discovery_actor, self._scraper_actor, self._fragmentador_actor]:
            if actor and hasattr(actor, 'stop'):
                try:
                    await actor.stop()
                except Exception as e:
                    logger.error(f"Error stopping actor: {e}")

        self._set_estado(EstadoPipelineCAS.COMPLETADO)

    # State query handler

    async def handle_obtener_estado_cas(self, msg: ObtenerEstadoCAS) -> dict:
        """
        Handle state query.

        Args:
            msg: State query message

        Returns:
            State information dict
        """
        result = {
            "estado": self._estado,
            "actor_id": self._actor_id,
        }

        if msg.incluir_estadisticas:
            result["estadisticas"] = {
                "laudos_descubiertos": self._estadisticas.laudos_descubiertos,
                "laudos_scrapeados": self._estadisticas.laudos_scrapeados,
                "laudos_fragmentados": self._estadisticas.laudos_fragmentados,
                "errores": self._estadisticas.errores,
                "tasa_exito": self._estadisticas.tasa_exito,
            }

        return result

    # Error handling

    async def handle_error_cas_pipeline(self, error: ErrorCASPipeline) -> None:
        """
        Handle error from child actor.

        Args:
            error: Error message
        """
        self._estadisticas = self._estadisticas.con_incremento("errores")
        logger.error(f"Pipeline error: {error.tipo_error} - {error.mensaje}")

        if not error.recuperable:
            self._set_estado(EstadoPipelineCAS.ERROR)
            logger.error("Non-recoverable error, pipeline entering ERROR state")
        else:
            # Retry if context available
            if error.contexto and "numero_caso" in error.contexto:
                logger.info(f"Scheduling retry for {error.contexto['numero_caso']}")
                if self._scraper_actor:
                    retry_msg = ScrapearLaudoCAS(
                        numero_caso=error.contexto["numero_caso"],
                        url=error.contexto.get("url", ""),
                        reintentos=error.contexto.get("reintentos", 0) + 1,
                    )
                    await self._scraper_actor.tell(retry_msg)

    # Lifecycle

    async def on_stop(self) -> None:
        """Stop all child actors."""
        for actor in [self._discovery_actor, self._scraper_actor, self._fragmentador_actor]:
            if actor and hasattr(actor, 'stop'):
                try:
                    await actor.stop()
                except Exception as e:
                    logger.error(f"Error stopping child actor: {e}")
