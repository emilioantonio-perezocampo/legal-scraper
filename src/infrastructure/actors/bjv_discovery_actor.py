"""
BJV Discovery Actor.

Handles search and pagination to discover books in the
Biblioteca Jurídica Virtual.

Supports two modes:
1. CSS parsing (legacy) - Direct HTTP fetch with BeautifulSoup
2. LLM parsing (recommended) - Crawl4AI with LLM extraction for JS-rendered content
"""
import logging
import os
import aiohttp
from typing import Optional, Set
from urllib.parse import urlencode, urljoin

from src.infrastructure.actors.bjv_base_actor import BJVBaseActor
from src.infrastructure.actors.bjv_messages import (
    IniciarBusqueda,
    ProcesarPaginaResultados,
    BusquedaCompletada,
    DescargarLibro,
    ErrorProcesamiento,
)
from src.infrastructure.actors.bjv_rate_limiter import BJVRateLimiter
from src.infrastructure.adapters.bjv_search_parser import (
    BJVSearchParser,
    PaginacionInfo,
)

logger = logging.getLogger(__name__)


BJV_BASE_URL = "https://biblio.juridicas.unam.mx"
BJV_SEARCH_URL = f"{BJV_BASE_URL}/bjv/resultados"
USER_AGENT = "BJVScraper/1.0 (Educational Research)"


class BJVDiscoveryActor(BJVBaseActor):
    """
    Actor for discovering books from BJV search.

    Supports two modes:
    - LLM mode (default): Uses Crawl4AI with LLM extraction for JS-rendered content
    - CSS mode (fallback): Direct HTTP fetch with BeautifulSoup parsing

    Responsibilities:
    - Execute search queries
    - Navigate pagination
    - Deduplicate discoveries
    - Forward DescargarLibro messages to coordinator
    """

    def __init__(
        self,
        coordinator: BJVBaseActor,
        rate_limiter: BJVRateLimiter,
        use_llm: bool = True,
    ):
        """
        Initialize the discovery actor.

        Args:
            coordinator: Parent coordinator actor
            rate_limiter: Rate limiter for requests
            use_llm: Use LLM-based extraction (recommended for JS content)
        """
        super().__init__(nombre="BJVDiscovery")
        self._coordinator = coordinator
        self._rate_limiter = rate_limiter
        self._session: Optional[aiohttp.ClientSession] = None
        self._discovered_ids: Set[str] = set()
        self._parser = BJVSearchParser()
        self._max_resultados = 100
        self._use_llm = use_llm and bool(os.getenv("OPENROUTER_API_KEY"))
        self._llm_parser = None

    async def start(self) -> None:
        """Start the actor and create HTTP session."""
        await super().start()
        timeout = aiohttp.ClientTimeout(total=30)
        self._session = aiohttp.ClientSession(
            timeout=timeout,
            headers={"User-Agent": USER_AGENT},
        )

        # Initialize LLM parser if enabled
        if self._use_llm:
            try:
                from src.infrastructure.adapters.bjv_llm_parser import BJVLLMParser
                self._llm_parser = BJVLLMParser()
                logger.info("BJV Discovery Actor started with LLM parser")
            except ImportError as e:
                logger.warning(f"LLM parser not available, falling back to CSS: {e}")
                self._use_llm = False
            except Exception as e:
                logger.warning(f"Failed to initialize LLM parser: {e}")
                self._use_llm = False

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
        if isinstance(message, IniciarBusqueda):
            await self._handle_iniciar_busqueda(message)
        elif isinstance(message, ProcesarPaginaResultados):
            await self._handle_procesar_pagina(message)

    async def _handle_iniciar_busqueda(self, msg: IniciarBusqueda) -> None:
        """Handle search initiation."""
        self._max_resultados = msg.max_resultados
        self._discovered_ids.clear()

        # Use LLM parser if available (better for JavaScript-rendered content)
        if self._use_llm and self._llm_parser:
            try:
                await self._discover_with_llm(msg)
                return
            except Exception as e:
                logger.warning(f"LLM discovery failed, falling back to CSS: {e}")
                # Fall through to CSS parsing

        # CSS parsing fallback
        params = {}
        if msg.query:
            params["ti"] = msg.query
        params["radio-libros"] = "on"

        url = f"{BJV_SEARCH_URL}?{urlencode(params)}"

        try:
            html = await self._fetch_search_page(url)
            await self._process_search_results(msg.correlation_id, html, 1)
        except Exception as e:
            error_msg = self._create_error_message(msg.correlation_id, None, e)
            await self._coordinator.tell(error_msg)

    async def _discover_with_llm(self, msg: IniciarBusqueda) -> None:
        """
        Discover books using LLM-based extraction.

        Args:
            msg: Search initiation message
        """
        logger.info(f"Starting LLM-based BJV discovery: query={msg.query}")

        try:
            # Use LLM parser with multi-page support
            results = await self._llm_parser.search_multiple_pages(
                query=msg.query or "derecho",
                area=msg.area if hasattr(msg, 'area') else None,
                max_pages=10,
                max_results=self._max_resultados,
            )

            # Emit discoveries
            for result in results:
                libro_id = result.libro_id.bjv_id
                if self._register_discovery(libro_id):
                    await self._emit_discovery(
                        correlation_id=msg.correlation_id,
                        libro_id=libro_id,
                        titulo=result.titulo,
                        url_detalle=result.url,
                    )

            # Notify completion
            completion = BusquedaCompletada(
                correlation_id=msg.correlation_id,
                total_descubiertos=len(self._discovered_ids),
                total_paginas=1,  # LLM parser handles pagination internally
            )
            await self._coordinator.tell(completion)

            logger.info(f"LLM discovery complete: {len(self._discovered_ids)} books found")

        except Exception as e:
            logger.error(f"LLM discovery error: {e}")
            raise

    async def _handle_procesar_pagina(self, msg: ProcesarPaginaResultados) -> None:
        """Handle page processing."""
        try:
            html = await self._fetch_search_page(msg.url_pagina)
            await self._process_search_results(
                msg.correlation_id, html, msg.numero_pagina
            )
        except Exception as e:
            error_msg = self._create_error_message(msg.correlation_id, None, e)
            await self._coordinator.tell(error_msg)

    async def _fetch_search_page(self, url: str) -> str:
        """
        Fetch a search page with rate limiting.

        Args:
            url: URL to fetch

        Returns:
            HTML content
        """
        await self._rate_limiter.acquire()

        async with self._session.get(url) as response:
            response.raise_for_status()
            return await response.text()

    async def _process_search_results(
        self, correlation_id: str, html: str, page_num: int
    ) -> None:
        """Process search results HTML."""
        try:
            results = self._parser.parse_search_results(html)
            pagination = self._parser.extract_pagination_info(html)

            for result in results:
                if self._is_limit_reached():
                    break

                libro_id = result.libro_id.bjv_id
                if self._register_discovery(libro_id):
                    await self._emit_discovery(
                        correlation_id=correlation_id,
                        libro_id=libro_id,
                        titulo=result.titulo,
                        url_detalle=result.libro_id.url,
                    )

            # Check if we should continue pagination
            if self._should_continue_pagination(
                pagination, len(self._discovered_ids), self._max_resultados
            ):
                next_page = page_num + 1
                next_url = f"{BJV_SEARCH_URL}?page={next_page}"
                next_msg = ProcesarPaginaResultados(
                    correlation_id=correlation_id,
                    numero_pagina=next_page,
                    url_pagina=next_url,
                )
                await self.tell(next_msg)
            else:
                # Notify completion
                completion = BusquedaCompletada(
                    correlation_id=correlation_id,
                    total_descubiertos=len(self._discovered_ids),
                    total_paginas=pagination.total_paginas,
                )
                await self._coordinator.tell(completion)

        except Exception as e:
            error_msg = self._create_error_message(correlation_id, None, e)
            await self._coordinator.tell(error_msg)

    def _register_discovery(self, libro_id: str) -> bool:
        """
        Register a new discovery.

        Args:
            libro_id: Book ID to register

        Returns:
            True if new, False if duplicate
        """
        if libro_id in self._discovered_ids:
            return False
        self._discovered_ids.add(libro_id)
        return True

    def _is_limit_reached(self) -> bool:
        """Check if max_resultados limit is reached."""
        return len(self._discovered_ids) >= self._max_resultados

    def _should_continue_pagination(
        self, pagination: PaginacionInfo, current_discovered: int, max_results: int
    ) -> bool:
        """
        Check if pagination should continue.

        Args:
            pagination: Current pagination info
            current_discovered: Number of books discovered
            max_results: Maximum results limit

        Returns:
            True if should continue to next page
        """
        if current_discovered >= max_results:
            return False
        if pagination.pagina_actual >= pagination.total_paginas:
            return False
        return True

    async def _emit_discovery(
        self,
        correlation_id: str,
        libro_id: str,
        titulo: str,
        url_detalle: str,
    ) -> None:
        """
        Emit a book discovery to coordinator.

        Args:
            correlation_id: Correlation ID
            libro_id: Book ID
            titulo: Book title
            url_detalle: Detail page URL
        """
        msg = DescargarLibro(
            correlation_id=correlation_id,
            libro_id=libro_id,
            url_detalle=url_detalle,
        )
        await self._coordinator.tell(msg)

    def _create_error_message(
        self, correlation_id: str, libro_id: Optional[str], error: Exception
    ) -> ErrorProcesamiento:
        """Create error message from exception."""
        recoverable = isinstance(error, (aiohttp.ClientError, TimeoutError))
        return ErrorProcesamiento(
            correlation_id=correlation_id,
            libro_id=libro_id,
            actor_origen=self.nombre,
            tipo_error=type(error).__name__,
            mensaje_error=str(error),
            recuperable=recoverable,
        )
