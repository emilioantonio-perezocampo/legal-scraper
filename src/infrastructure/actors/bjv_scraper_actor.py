"""
BJV Scraper Actor.

Fetches book detail pages and parses them into LibroBJV entities.
"""
import aiohttp
from typing import Optional

from src.infrastructure.actors.bjv_base_actor import BJVBaseActor
from src.infrastructure.actors.bjv_messages import (
    DescargarLibro,
    LibroListo,
    ErrorProcesamiento,
)
from src.infrastructure.actors.bjv_rate_limiter import BJVRateLimiter
from src.infrastructure.adapters.bjv_libro_parser import BJVLibroParser


BJV_BASE_URL = "https://biblio.juridicas.unam.mx"
USER_AGENT = "BJVScraper/1.0 (Educational Research)"


class BJVScraperActor(BJVBaseActor):
    """
    Actor for fetching book detail pages.

    Responsibilities:
    - Fetch book detail HTML
    - Parse into LibroBJV entities
    - Retry on transient failures
    - Forward LibroListo to coordinator
    """

    def __init__(
        self,
        coordinator: BJVBaseActor,
        rate_limiter: BJVRateLimiter,
        max_retries: int = 3,
    ):
        """
        Initialize the scraper actor.

        Args:
            coordinator: Parent coordinator actor
            rate_limiter: Rate limiter for requests
            max_retries: Maximum retry attempts
        """
        super().__init__(nombre="BJVScraper")
        self._coordinator = coordinator
        self._rate_limiter = rate_limiter
        self._max_retries = max_retries
        self._session: Optional[aiohttp.ClientSession] = None
        self._parser = BJVLibroParser()

    async def start(self) -> None:
        """Start the actor and create HTTP session."""
        await super().start()
        timeout = aiohttp.ClientTimeout(total=30)
        self._session = aiohttp.ClientSession(
            timeout=timeout,
            headers={"User-Agent": USER_AGENT},
        )

    async def stop(self) -> None:
        """Stop the actor and close HTTP session."""
        if self._session:
            await self._session.close()
        await super().stop()

    async def handle_message(self, message) -> None:
        """
        Handle incoming messages.

        Args:
            message: Message to process
        """
        if isinstance(message, DescargarLibro):
            await self._process_libro(
                message.correlation_id,
                message.libro_id,
                message.url_detalle,
            )

    async def _process_libro(
        self,
        correlation_id: str,
        libro_id: str,
        url_detalle: str,
    ) -> None:
        """Process a book download request."""
        last_error = None

        for attempt in range(self._max_retries):
            try:
                html = await self._fetch_libro_page(url_detalle)
                libro = self._parser.parse_libro_detalle(html, libro_id)

                msg = LibroListo(
                    correlation_id=correlation_id,
                    libro_id=libro_id,
                    libro=libro,
                )
                await self._coordinator.tell(msg)
                return

            except Exception as e:
                last_error = e
                if not self._is_retryable(e):
                    break

        # Retries exhausted or non-retryable error
        error_msg = self._create_error_message(correlation_id, libro_id, last_error)
        await self._coordinator.tell(error_msg)

    async def _fetch_libro_page(self, url: str) -> str:
        """
        Fetch a book detail page with rate limiting.

        Args:
            url: URL to fetch

        Returns:
            HTML content
        """
        await self._rate_limiter.acquire()

        async with self._session.get(url) as response:
            response.raise_for_status()
            return await response.text()

    def _is_retryable(self, error: Exception) -> bool:
        """Check if error is retryable."""
        return isinstance(error, (aiohttp.ClientError, TimeoutError))

    def _create_error_message(
        self, correlation_id: str, libro_id: str, error: Exception
    ) -> ErrorProcesamiento:
        """Create error message from exception."""
        recoverable = self._is_retryable(error)
        return ErrorProcesamiento(
            correlation_id=correlation_id,
            libro_id=libro_id,
            actor_origen=self.nombre,
            tipo_error=type(error).__name__,
            mensaje_error=str(error),
            recuperable=recoverable,
        )
