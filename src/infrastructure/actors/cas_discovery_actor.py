"""
CAS Discovery Actor

Actor responsible for discovering CAS awards by crawling search results.

Supports two modes:
1. LLM mode (default): Uses Crawl4AI with LLM extraction for JS-rendered content
2. Browser mode (fallback): Playwright-based rendering with CSS parsing
"""
import logging
import os
from typing import Optional, Any

from src.infrastructure.actors.cas_base_actor import CASBaseActor
from src.infrastructure.actors.cas_messages import (
    EstadoPipelineCAS,
    IniciarDescubrimientoCAS,
    LaudoDescubiertoCAS,
    ErrorCASPipeline,
)
from src.infrastructure.adapters.cas_search_parser import (
    parse_search_results,
    extract_pagination_info,
)
from src.infrastructure.adapters.cas_errors import (
    CASAdapterError,
    ParseError,
    BrowserTimeoutError,
    CASRateLimitError,
)


logger = logging.getLogger(__name__)


class CASDiscoveryActor(CASBaseActor):
    """
    Actor for discovering CAS arbitration awards.

    Supports two modes:
    - LLM mode (default): Uses Crawl4AI with LLM extraction for JS-rendered content
    - Browser mode (fallback): Playwright-based rendering with CSS parsing

    Crawls the CAS jurisprudence database search results,
    extracting case numbers and URLs for detailed scraping.
    """

    DEFAULT_BASE_URL = "https://jurisprudence.tas-cas.org/Shared%20Documents/Forms/AllItems.aspx"

    def __init__(
        self,
        browser_adapter: Optional[Any] = None,
        base_url: Optional[str] = None,
        actor_id: Optional[str] = None,
        supervisor: Optional[CASBaseActor] = None,
        use_llm: bool = True,
    ):
        super().__init__(actor_id=actor_id, supervisor=supervisor)
        self._browser_adapter = browser_adapter
        self._base_url = base_url or self.DEFAULT_BASE_URL
        self._discovered_count = 0
        self._use_llm = use_llm and bool(os.getenv("OPENROUTER_API_KEY"))
        self._llm_parser = None

        # Initialize LLM parser if enabled
        if self._use_llm:
            try:
                from src.infrastructure.adapters.cas_llm_parser import CASLLMParser
                self._llm_parser = CASLLMParser()
                logger.info("CAS Discovery Actor initialized with LLM parser")
            except ImportError as e:
                logger.warning(f"LLM parser not available, falling back to browser: {e}")
                self._use_llm = False
            except Exception as e:
                logger.warning(f"Failed to initialize LLM parser: {e}")
                self._use_llm = False

    @property
    def browser_adapter(self) -> Optional[Any]:
        """Get browser adapter."""
        return self._browser_adapter

    @property
    def base_url(self) -> str:
        """Get base URL."""
        return self._base_url

    @property
    def discovered_count(self) -> int:
        """Get count of discovered awards."""
        return self._discovered_count

    async def handle_iniciar_descubrimiento_cas(
        self, msg: IniciarDescubrimientoCAS
    ) -> None:
        """
        Handle discovery start message.

        Args:
            msg: Discovery start message with filters and limits
        """
        self._set_estado(EstadoPipelineCAS.DESCUBRIENDO)
        logger.info(f"Starting CAS discovery, max_paginas={msg.max_paginas}, use_llm={self._use_llm}")

        try:
            # Try LLM-based discovery first
            if self._use_llm and self._llm_parser:
                await self._discover_with_llm(msg)
            else:
                # Fall back to browser-based discovery
                await self._discover_awards(
                    max_paginas=msg.max_paginas,
                    filtros=msg.filtros,
                )
        except CASAdapterError as e:
            error = ErrorCASPipeline(
                mensaje=str(e),
                tipo_error=type(e).__name__,
                recuperable=e.recoverable,
            )
            await self.escalate(error)
        except Exception as e:
            error = self._handle_error(e)
            await self.escalate(error)

    async def _discover_with_llm(self, msg: IniciarDescubrimientoCAS) -> None:
        """
        Discover awards using LLM-based extraction.

        Args:
            msg: Discovery start message
        """
        logger.info("Starting LLM-based CAS discovery")

        try:
            # Extract filters
            year = None
            sport = None
            if msg.filtros:
                year = msg.filtros.get('year')
                sport = msg.filtros.get('sport')

            # Use LLM parser with multi-page support
            max_results = msg.max_paginas * 20 if msg.max_paginas else 100
            results = await self._llm_parser.search_multiple_pages(
                year=year,
                sport=sport,
                max_pages=msg.max_paginas or 5,
                max_results=max_results,
            )

            # Emit discovered awards
            for result in results:
                url = result.url.valor if result.url else ""
                laudo_msg = LaudoDescubiertoCAS(
                    numero_caso=result.numero_caso.valor,
                    url=url,
                    titulo=result.titulo,
                )
                self._discovered_count += 1
                if self._supervisor:
                    await self._supervisor.tell(laudo_msg)
                logger.debug(f"Discovered via LLM: {result.numero_caso.valor}")

            logger.info(f"LLM discovery complete: {self._discovered_count} awards found")

        except Exception as e:
            logger.error(f"LLM discovery error: {e}")
            # Fall back to browser-based discovery
            if self._browser_adapter:
                logger.info("Falling back to browser-based discovery")
                await self._discover_awards(
                    max_paginas=msg.max_paginas,
                    filtros=msg.filtros,
                )
            else:
                raise

    async def _discover_awards(
        self,
        max_paginas: Optional[int],
        filtros: Optional[dict],
    ) -> None:
        """
        Discover awards by crawling search results.

        Args:
            max_paginas: Maximum pages to crawl (None = unlimited)
            filtros: Search filters to apply
        """
        if not self._browser_adapter:
            logger.warning("No browser adapter configured")
            return

        pagina_actual = 1
        tiene_siguiente = True

        while tiene_siguiente and self._running:
            # Check page limit
            if max_paginas and pagina_actual > max_paginas:
                logger.info(f"Reached max_paginas limit: {max_paginas}")
                break

            try:
                # Execute search
                rendered = await self._browser_adapter.execute_search(
                    self._base_url,
                    filters=filtros,
                    page=pagina_actual,
                )

                if not rendered or not rendered.html:
                    logger.warning(f"Empty response for page {pagina_actual}")
                    break

                # Parse results
                try:
                    resultados = parse_search_results(rendered.html)
                except ParseError as e:
                    logger.warning(f"Parse error on page {pagina_actual}: {e}")
                    break

                # Emit discovered awards
                for resultado in resultados:
                    await self._emit_discovered(resultado)

                # Check pagination
                paginacion = extract_pagination_info(rendered.html)
                tiene_siguiente = paginacion.tiene_siguiente
                pagina_actual += 1

            except BrowserTimeoutError as e:
                logger.error(f"Browser timeout on page {pagina_actual}: {e}")
                raise
            except CASRateLimitError as e:
                logger.warning(f"Rate limited on page {pagina_actual}: {e}")
                raise
            except Exception as e:
                logger.error(f"Error on page {pagina_actual}: {e}")
                raise

        logger.info(f"Discovery complete: {self._discovered_count} awards found")

    async def _emit_discovered(self, resultado: Any) -> None:
        """
        Emit LaudoDescubiertoCAS message for discovered award.

        Args:
            resultado: Search result item
        """
        # Build URL from case number if not present
        url = getattr(resultado, 'url', '') or self._build_award_url(resultado.numero_caso)

        msg = LaudoDescubiertoCAS(
            numero_caso=resultado.numero_caso.valor,
            url=url,
            titulo=resultado.titulo,
        )

        self._discovered_count += 1

        if self._supervisor:
            await self._supervisor.tell(msg)

        logger.debug(f"Discovered: {msg.numero_caso}")

    def _build_award_url(self, numero_caso: Any) -> str:
        """
        Build award detail URL from case number.

        Args:
            numero_caso: Case number value object

        Returns:
            URL string
        """
        # Extract case number string
        caso_str = numero_caso.valor if hasattr(numero_caso, 'valor') else str(numero_caso)
        # URL encode the case number
        encoded = caso_str.replace(" ", "%20").replace("/", "%2F")
        return f"{self._base_url}?case={encoded}"
