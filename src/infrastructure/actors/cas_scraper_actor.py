"""
CAS Scraper Actor

Actor responsible for scraping individual CAS award detail pages.
"""
import logging
from typing import Optional, Any

from src.infrastructure.actors.cas_base_actor import CASBaseActor
from src.infrastructure.actors.cas_messages import (
    EstadoPipelineCAS,
    ScrapearLaudoCAS,
    LaudoScrapeadoCAS,
    ErrorCASPipeline,
)
from src.infrastructure.adapters.cas_laudo_parser import parse_laudo_detalle
from src.infrastructure.adapters.cas_errors import (
    CASAdapterError,
    ParseError,
    BrowserTimeoutError,
    CASServerError,
)


logger = logging.getLogger(__name__)


class CASScraperActor(CASBaseActor):
    """
    Actor for scraping CAS arbitration award details.

    Fetches and parses individual award pages,
    extracting structured data for storage.
    """

    def __init__(
        self,
        browser_adapter: Optional[Any] = None,
        max_reintentos: int = 3,
        actor_id: Optional[str] = None,
        supervisor: Optional[CASBaseActor] = None,
    ):
        super().__init__(actor_id=actor_id, supervisor=supervisor)
        self._browser_adapter = browser_adapter
        self._max_reintentos = max_reintentos
        self._scraped_count = 0
        self._error_count = 0

    @property
    def browser_adapter(self) -> Optional[Any]:
        """Get browser adapter."""
        return self._browser_adapter

    @property
    def max_reintentos(self) -> int:
        """Get max retry count."""
        return self._max_reintentos

    @property
    def scraped_count(self) -> int:
        """Get count of scraped awards."""
        return self._scraped_count

    @property
    def error_count(self) -> int:
        """Get error count."""
        return self._error_count

    async def handle_scrapear_laudo_cas(self, msg: ScrapearLaudoCAS) -> None:
        """
        Handle scrape award message.

        Args:
            msg: Scrape request with case number and URL
        """
        self._set_estado(EstadoPipelineCAS.SCRAPEANDO)
        logger.info(f"Scraping: {msg.numero_caso}")

        try:
            laudo = await self._scrape_award(msg)
            if laudo:
                self._scraped_count += 1
                result_msg = LaudoScrapeadoCAS(laudo=laudo)
                if self._supervisor:
                    await self._supervisor.tell(result_msg)
        except CASAdapterError as e:
            await self._handle_scrape_error(msg, e)
        except Exception as e:
            await self._handle_scrape_error(msg, e)

    async def _scrape_award(self, msg: ScrapearLaudoCAS) -> Optional[Any]:
        """
        Scrape a single award.

        Args:
            msg: Scrape request message

        Returns:
            Parsed LaudoArbitral or None

        Raises:
            CASAdapterError: On scraping errors
        """
        if not self._browser_adapter:
            logger.warning("No browser adapter configured")
            return None

        # Render page
        rendered = await self._browser_adapter.render_page(
            msg.url,
            wait_for_network_idle=True,
        )

        # Check status code
        if rendered.status_code == 404:
            logger.warning(f"Award not found: {msg.numero_caso}")
            return None

        if rendered.status_code >= 500:
            raise CASServerError(f"Server error {rendered.status_code}")

        # Parse award
        try:
            laudo = parse_laudo_detalle(rendered.html, msg.url)
            logger.debug(f"Scraped: {msg.numero_caso}")
            return laudo
        except ParseError as e:
            logger.warning(f"Parse error for {msg.numero_caso}: {e}")
            raise

    async def _handle_scrape_error(
        self,
        msg: ScrapearLaudoCAS,
        exception: Exception,
    ) -> None:
        """
        Handle scraping error with retry logic.

        Args:
            msg: Original scrape message
            exception: The exception that occurred
        """
        self._error_count += 1

        # Check if recoverable
        recuperable = True
        if isinstance(exception, CASAdapterError):
            recuperable = exception.recoverable

        # Check retry limit
        if not recuperable or msg.reintentos >= self._max_reintentos:
            error = ErrorCASPipeline(
                mensaje=str(exception),
                tipo_error=type(exception).__name__,
                recuperable=False,
                contexto={
                    "numero_caso": msg.numero_caso,
                    "url": msg.url,
                    "reintentos": msg.reintentos,
                },
            )
            await self.escalate(error)
            return

        # Retry with incremented counter
        logger.info(f"Retrying {msg.numero_caso} ({msg.reintentos + 1}/{self._max_reintentos})")
        retry_msg = ScrapearLaudoCAS(
            numero_caso=msg.numero_caso,
            url=msg.url,
            reintentos=msg.reintentos + 1,
        )
        await self.tell(retry_msg)
