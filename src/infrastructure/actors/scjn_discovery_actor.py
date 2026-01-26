"""
SCJN Document Discovery Actor

Discovers documents from SCJN search results.
Supports multiple modes:
1. LLM-based extraction (default, most robust)
2. Playwright browser automation (fallback)
3. HTTP + CSS parsing (legacy, often fails)

Handles pagination and emits DocumentoDescubierto events.
"""
import asyncio
import logging
import os
from typing import Optional, Set, List

from .base import BaseActor
from .messages import (
    DescubrirDocumentos,
    DescubrirPagina,
    DocumentoDescubierto,
    PaginaDescubierta,
    ErrorDeActor,
)
from .rate_limiter import RateLimiter
from src.infrastructure.adapters.scjn_search_parser import (
    parse_search_results,
    extract_pagination_info,
)
from src.infrastructure.adapters.errors import ParseError

logger = logging.getLogger(__name__)


class DiscoveryError(Exception):
    """Base discovery error."""
    pass


class PaginationError(DiscoveryError):
    """Error during pagination."""
    pass


class SCJNDiscoveryActor(BaseActor):
    """
    Discovers SCJN documents from search results.

    Supports extraction modes:
    - "llm": Uses OpenRouter LLM for semantic extraction (most robust)
    - "browser": Uses Playwright for JavaScript rendering
    - "http": Direct HTTP + CSS parsing (legacy, often fails)

    Message Protocol:
    - DescubrirDocumentos → searches and emits DocumentoDescubierto events
    - DescubrirPagina → fetches a specific page
    - Returns: PaginaDescubierta or ErrorDeActor
    """

    def __init__(
        self,
        coordinator: 'BaseActor',
        rate_limiter: RateLimiter,
        max_pages: int = 100,
        timeout_seconds: float = 60.0,
        use_browser: bool = True,
        use_llm: bool = True,  # New: prefer LLM extraction
    ):
        super().__init__()
        self._coordinator = coordinator
        self._rate_limiter = rate_limiter
        self._max_pages = max_pages
        self._timeout_seconds = timeout_seconds
        self._use_browser = use_browser
        self._use_llm = use_llm and bool(os.getenv("OPENROUTER_API_KEY"))
        self._discovered_q_params: Set[str] = set()
        self._browser_adapter = None
        self._llm_parser = None

    async def start(self):
        """Start the actor and initialize LLM parser or browser."""
        await super().start()

        # Try LLM parser first (most robust)
        if self._use_llm:
            try:
                from src.infrastructure.adapters.scjn_llm_parser import SCJNLLMParser
                self._llm_parser = SCJNLLMParser()
                logger.info("SCJN Discovery Actor started with LLM parser (OpenRouter)")
                return  # LLM parser is preferred, no need for browser
            except ImportError as e:
                logger.warning(f"LLM parser not available: {e}")
                self._use_llm = False
            except Exception as e:
                logger.warning(f"Failed to initialize LLM parser: {e}")
                self._use_llm = False

        # Fallback to browser automation
        if self._use_browser:
            try:
                from src.infrastructure.adapters.scjn_browser_adapter import (
                    SCJNBrowserAdapter,
                    SCJNBrowserConfig,
                )
                config = SCJNBrowserConfig(
                    timeout_ms=int(self._timeout_seconds * 1000),
                    headless=True,
                )
                self._browser_adapter = SCJNBrowserAdapter(config)
                await self._browser_adapter.start()
                logger.info("SCJN Discovery Actor started with Playwright browser")
            except ImportError as e:
                logger.warning(f"Playwright not available, falling back to HTTP: {e}")
                self._use_browser = False
            except Exception as e:
                logger.warning(f"Failed to start browser, falling back to HTTP: {e}")
                self._use_browser = False

    async def stop(self):
        """Stop the actor and close browser."""
        if self._browser_adapter:
            await self._browser_adapter.stop()
            self._browser_adapter = None
        await super().stop()

    async def handle_message(self, message):
        """Handle incoming messages."""
        if isinstance(message, DescubrirDocumentos):
            return await self._handle_discover(message)
        elif isinstance(message, DescubrirPagina):
            return await self._handle_discover_page(message)
        return None

    async def _handle_discover(self, cmd: DescubrirDocumentos):
        """Handle document discovery command."""
        try:
            await self._rate_limiter.acquire()

            # Use LLM parser if available (most robust)
            if self._use_llm and self._llm_parser:
                return await self._discover_with_llm(cmd)

            # Fallback to browser/HTTP
            html = await self._fetch_search_page(
                category=cmd.category,
                scope=cmd.scope,
                status=cmd.status,
                page=1,
            )

            # Parse results
            try:
                results = parse_search_results(html)
            except ParseError as e:
                logger.warning(f"Parse error on page 1: {e}")
                results = []

            # Get pagination info
            try:
                current_page, total_pages, _ = extract_pagination_info(html)
            except Exception:
                current_page, total_pages = 1, 1

            # Emit discovered documents
            documents_found = 0
            for result in results:
                if result.q_param not in self._discovered_q_params:
                    self._discovered_q_params.add(result.q_param)
                    await self._coordinator.tell(DocumentoDescubierto(
                        correlation_id=cmd.correlation_id,
                        q_param=result.q_param,
                        title=result.title,
                        category=result.category,
                    ))
                    documents_found += 1

            logger.info(f"Page 1: Found {documents_found} documents, {total_pages} total pages")

            # Handle pagination if requested
            if cmd.discover_all_pages and current_page < total_pages:
                pages_fetched = 1
                while current_page < total_pages and pages_fetched < self._max_pages:
                    await self._rate_limiter.acquire()
                    current_page += 1
                    pages_fetched += 1

                    try:
                        html = await self._fetch_search_page(
                            category=cmd.category,
                            scope=cmd.scope,
                            status=cmd.status,
                            page=current_page,
                        )

                        try:
                            results = parse_search_results(html)
                        except ParseError:
                            continue

                        page_found = 0
                        for result in results:
                            if result.q_param not in self._discovered_q_params:
                                self._discovered_q_params.add(result.q_param)
                                await self._coordinator.tell(DocumentoDescubierto(
                                    correlation_id=cmd.correlation_id,
                                    q_param=result.q_param,
                                    title=result.title,
                                    category=result.category,
                                ))
                                documents_found += 1
                                page_found += 1

                        logger.info(f"Page {current_page}: Found {page_found} documents")

                        try:
                            _, total_pages, _ = extract_pagination_info(html)
                        except Exception:
                            pass

                    except Exception as e:
                        logger.warning(f"Error on page {current_page}: {e}")
                        continue

            return PaginaDescubierta(
                correlation_id=cmd.correlation_id,
                documents_found=documents_found,
                current_page=current_page,
                total_pages=total_pages,
                has_more_pages=current_page < total_pages,
            )

        except ParseError as e:
            return ErrorDeActor(
                correlation_id=cmd.correlation_id,
                actor_name="SCJNDiscoveryActor",
                document_id=None,
                error_type="ParseError",
                error_message=str(e),
                recoverable=False,
                original_command=cmd,
            )
        except Exception as e:
            logger.error(f"Discovery error: {e}")
            return ErrorDeActor(
                correlation_id=cmd.correlation_id,
                actor_name="SCJNDiscoveryActor",
                document_id=None,
                error_type=type(e).__name__,
                error_message=str(e),
                recoverable=True,
                original_command=cmd,
            )

    async def _handle_discover_page(self, cmd: DescubrirPagina):
        """Handle single page discovery command."""
        try:
            await self._rate_limiter.acquire()

            html = await self._fetch_search_page(
                category=cmd.category,
                scope=cmd.scope,
                status=cmd.status,
                page=cmd.page_number,
            )

            try:
                results = parse_search_results(html)
            except ParseError:
                results = []

            try:
                current_page, total_pages, _ = extract_pagination_info(html)
            except Exception:
                current_page, total_pages = cmd.page_number, cmd.page_number

            documents_found = 0
            for result in results:
                if result.q_param not in self._discovered_q_params:
                    self._discovered_q_params.add(result.q_param)
                    await self._coordinator.tell(DocumentoDescubierto(
                        correlation_id=cmd.correlation_id,
                        q_param=result.q_param,
                        title=result.title,
                        category=result.category,
                    ))
                    documents_found += 1

            return PaginaDescubierta(
                correlation_id=cmd.correlation_id,
                documents_found=documents_found,
                current_page=current_page,
                total_pages=total_pages,
                has_more_pages=current_page < total_pages,
            )

        except ParseError as e:
            return ErrorDeActor(
                correlation_id=cmd.correlation_id,
                actor_name="SCJNDiscoveryActor",
                document_id=None,
                error_type="ParseError",
                error_message=str(e),
                recoverable=False,
                original_command=cmd,
            )
        except Exception as e:
            return ErrorDeActor(
                correlation_id=cmd.correlation_id,
                actor_name="SCJNDiscoveryActor",
                document_id=None,
                error_type=type(e).__name__,
                error_message=str(e),
                recoverable=True,
                original_command=cmd,
            )

    async def _discover_with_llm(self, cmd: DescubrirDocumentos):
        """Discover documents using LLM-based extraction."""
        try:
            documents_found = 0
            current_page = 1
            total_pages = 1

            # Calculate max pages based on max_results
            max_pages_to_fetch = min(self._max_pages, (cmd.max_results + 9) // 10) if cmd.max_results else self._max_pages

            # Parse pages using LLM
            logger.info(f"Starting LLM-based discovery: category={cmd.category}, scope={cmd.scope}")

            documents, has_next = await self._llm_parser.parse_search_page(
                page=1,
                category=cmd.category,
                scope=cmd.scope,
                status=cmd.status,
            )

            # Emit discovered documents
            for doc in documents:
                if doc.q_param and doc.q_param not in self._discovered_q_params:
                    self._discovered_q_params.add(doc.q_param)
                    await self._coordinator.tell(DocumentoDescubierto(
                        correlation_id=cmd.correlation_id,
                        q_param=doc.q_param,
                        title=doc.title,
                        category=str(doc.category.value) if doc.category else "LEY",
                    ))
                    documents_found += 1

            logger.info(f"Page 1: Found {documents_found} documents via LLM")

            # Handle pagination if requested
            if cmd.discover_all_pages and has_next:
                while has_next and current_page < max_pages_to_fetch:
                    await self._rate_limiter.acquire()
                    current_page += 1

                    try:
                        documents, has_next = await self._llm_parser.parse_search_page(
                            page=current_page,
                            category=cmd.category,
                            scope=cmd.scope,
                            status=cmd.status,
                        )

                        page_found = 0
                        for doc in documents:
                            if doc.q_param and doc.q_param not in self._discovered_q_params:
                                self._discovered_q_params.add(doc.q_param)
                                await self._coordinator.tell(DocumentoDescubierto(
                                    correlation_id=cmd.correlation_id,
                                    q_param=doc.q_param,
                                    title=doc.title,
                                    category=str(doc.category.value) if doc.category else "LEY",
                                ))
                                documents_found += 1
                                page_found += 1

                        logger.info(f"Page {current_page}: Found {page_found} documents via LLM")

                        # Check max_results limit
                        if cmd.max_results and documents_found >= cmd.max_results:
                            break

                    except Exception as e:
                        logger.warning(f"Error on LLM page {current_page}: {e}")
                        continue

            total_pages = current_page if has_next else current_page

            return PaginaDescubierta(
                correlation_id=cmd.correlation_id,
                documents_found=documents_found,
                current_page=current_page,
                total_pages=total_pages,
                has_more_pages=has_next,
            )

        except Exception as e:
            logger.error(f"LLM discovery error: {e}")
            return ErrorDeActor(
                correlation_id=cmd.correlation_id,
                actor_name="SCJNDiscoveryActor",
                document_id=None,
                error_type=type(e).__name__,
                error_message=str(e),
                recoverable=True,
                original_command=cmd,
            )

    async def _fetch_search_page(
        self,
        category: Optional[str] = None,
        scope: Optional[str] = None,
        status: Optional[str] = None,
        page: int = 1,
    ) -> str:
        """Fetch search results page using browser or HTTP fallback."""
        if self._use_browser and self._browser_adapter:
            result = await self._browser_adapter.search(
                category=category,
                scope=scope,
                page=page,
            )
            return result.html
        else:
            # HTTP fallback - will likely not work but kept for compatibility
            return await self._fetch_http(category, scope, status, page)

    async def _fetch_http(
        self,
        category: Optional[str] = None,
        scope: Optional[str] = None,
        status: Optional[str] = None,
        page: int = 1,
    ) -> str:
        """HTTP fallback for search (may not work with current SCJN site)."""
        import aiohttp
        from urllib.parse import urlencode

        SCJN_SEARCH_URL = "https://legislacion.scjn.gob.mx/Buscador/Paginas/Buscar.aspx"
        USER_AGENT = "LegalScraper/1.0 (Educational Research)"

        params = {}
        if category:
            params["categoria"] = category
        if scope:
            params["ambito"] = scope
        if status:
            params["estatus"] = status
        if page > 1:
            params["pagina"] = str(page)

        url = f"{SCJN_SEARCH_URL}?{urlencode(params)}" if params else SCJN_SEARCH_URL

        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=self._timeout_seconds),
            headers={"User-Agent": USER_AGENT},
        ) as session:
            async with session.get(url) as response:
                if response.status != 200:
                    raise DiscoveryError(f"Search failed: {response.status}")
                return await response.text()
