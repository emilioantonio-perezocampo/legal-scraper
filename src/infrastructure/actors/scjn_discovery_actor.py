"""
SCJN Document Discovery Actor

Discovers documents from SCJN search results.
Handles pagination and emits DocumentoDescubierto events.
"""
import asyncio
import aiohttp
from typing import Optional, Set
from urllib.parse import urlencode

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


class DiscoveryError(Exception):
    """Base discovery error."""
    pass


class PaginationError(DiscoveryError):
    """Error during pagination."""
    pass


class SCJNDiscoveryActor(BaseActor):
    """
    Discovers SCJN documents from search results.

    Message Protocol:
    - DescubrirDocumentos → searches and emits DocumentoDescubierto events
    - DescubrirPagina → fetches a specific page
    - Returns: PaginaDescubierta or ErrorDeActor
    """

    SCJN_SEARCH_URL = "https://legislacion.scjn.gob.mx/Buscador/Paginas/Buscar.aspx"
    USER_AGENT = "LegalScraper/1.0 (Educational Research)"

    def __init__(
        self,
        coordinator: 'BaseActor',
        rate_limiter: RateLimiter,
        max_pages: int = 100,
        timeout_seconds: float = 30.0,
    ):
        super().__init__()
        self._coordinator = coordinator
        self._rate_limiter = rate_limiter
        self._max_pages = max_pages
        self._timeout = aiohttp.ClientTimeout(total=timeout_seconds)
        self._session: Optional[aiohttp.ClientSession] = None
        self._discovered_q_params: Set[str] = set()

    async def start(self):
        """Start the actor and create HTTP session."""
        await super().start()
        self._session = aiohttp.ClientSession(
            timeout=self._timeout,
            headers={"User-Agent": self.USER_AGENT},
        )

    async def stop(self):
        """Stop the actor and close HTTP session."""
        if self._session:
            await self._session.close()
            self._session = None
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

            # Fetch first page
            html = await self._fetch_search_page(
                category=cmd.category,
                scope=cmd.scope,
                status=cmd.status,
                page=1,
            )

            # Parse results
            results = parse_search_results(html)
            current_page, total_pages, _ = extract_pagination_info(html)

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
                        results = parse_search_results(html)

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

                        _, total_pages, _ = extract_pagination_info(html)
                    except Exception:
                        # Continue to next page on individual page failure
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

            results = parse_search_results(html)
            current_page, total_pages, _ = extract_pagination_info(html)

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

    async def _fetch_search_page(
        self,
        category: Optional[str] = None,
        scope: Optional[str] = None,
        status: Optional[str] = None,
        page: int = 1,
    ) -> str:
        """Fetch search results page."""
        url = self._build_search_url(
            category=category,
            scope=scope,
            status=status,
            page=page,
        )

        async with self._session.get(url) as response:
            if response.status != 200:
                raise DiscoveryError(f"Search failed: {response.status}")
            return await response.text()

    def _build_search_url(
        self,
        category: Optional[str] = None,
        scope: Optional[str] = None,
        status: Optional[str] = None,
        page: int = 1,
    ) -> str:
        """Build search URL with parameters."""
        params = {}

        if category:
            params["categoria"] = category

        if scope:
            params["ambito"] = scope

        if status:
            params["estatus"] = status

        if page > 1:
            params["pagina"] = str(page)

        if params:
            return f"{self.SCJN_SEARCH_URL}?{urlencode(params)}"
        return self.SCJN_SEARCH_URL
