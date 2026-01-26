"""
BJV LLM-based parser using Crawl4AI.

Extracts books from UNAM's Biblioteca Jurídica Virtual using
LLM-powered semantic extraction for JavaScript-rendered content.
"""
import logging
import urllib.parse
from typing import List, Optional

from pydantic import BaseModel, Field

from src.domain.bjv_value_objects import IdentificadorLibro, TipoContenido
from src.infrastructure.adapters.crawl4ai_adapter import Crawl4AIAdapter

logger = logging.getLogger(__name__)


# --- Pydantic Schemas for LLM Extraction ---

class ExtractedBJVBook(BaseModel):
    """Schema for a single book extracted by LLM."""

    title: str = Field(description="Full title of the book")
    book_id: Optional[str] = Field(
        default=None,
        description="BJV book ID number (from URL like /detalle/12345)"
    )
    detail_url: Optional[str] = Field(
        default=None,
        description="URL to the book detail page"
    )
    authors: Optional[str] = Field(
        default=None,
        description="Author names, comma separated"
    )
    year: Optional[int] = Field(
        default=None,
        description="Publication year (4-digit number)"
    )
    content_type: Optional[str] = Field(
        default=None,
        description="Type: libro, capitulo, articulo, revista"
    )
    area: Optional[str] = Field(
        default=None,
        description="Legal area: civil, penal, constitucional, etc."
    )
    snippet: Optional[str] = Field(
        default=None,
        description="Preview text or description"
    )


class ExtractedBJVSearchResults(BaseModel):
    """Schema for BJV search results page."""

    books: List[ExtractedBJVBook] = Field(
        default_factory=list,
        description="List of all books found on the page"
    )
    total_results: Optional[int] = Field(
        default=None,
        description="Total number of search results"
    )
    current_page: Optional[int] = Field(
        default=None,
        description="Current page number"
    )
    total_pages: Optional[int] = Field(
        default=None,
        description="Total number of pages"
    )
    has_next_page: bool = Field(
        default=False,
        description="Whether there is a next page"
    )


# --- Result Data Class ---

class BJVSearchResult:
    """Search result from BJV with domain-compatible fields."""

    def __init__(
        self,
        libro_id: IdentificadorLibro,
        titulo: str,
        tipo_contenido: TipoContenido,
        autores_texto: Optional[str] = None,
        anio: Optional[int] = None,
        area: Optional[str] = None,
        fragmento: Optional[str] = None,
    ):
        self.libro_id = libro_id
        self.titulo = titulo
        self.tipo_contenido = tipo_contenido
        self.autores_texto = autores_texto
        self.anio = anio
        self.area = area
        self.fragmento = fragmento

    @property
    def url(self) -> str:
        return self.libro_id.url


# --- Main Parser Class ---

class BJVLLMParser:
    """
    LLM-powered parser for BJV search results.

    Uses Crawl4AI with JavaScript rendering to handle
    the dynamically-loaded content on biblio.juridicas.unam.mx.

    Strategy: Uses the main BJV catalog page which loads books without AJAX,
    rather than the search page which requires complex AJAX rendering.
    """

    BASE_URL = "https://biblio.juridicas.unam.mx"
    # Use the main BJV catalog page which has books visible without AJAX
    CATALOG_URL = f"{BASE_URL}/bjv"
    # Legacy search URL for fallback
    SEARCH_URL = f"{BASE_URL}/bjv/resultados"

    EXTRACTION_INSTRUCTION = """
You are extracting legal books from the UNAM Biblioteca Jurídica Virtual (BJV) website.

Extract ALL books/publications visible on this page. Look for book links and listings.
Books appear in various formats - cards, lists, links with titles.

For each book found:

1. **title**: The complete title of the book or publication

2. **book_id**: Extract the numeric ID from URLs like:
   - /bjv/detalle-libro/7867-derecho-natural → "7867"
   - /bjv/detalle-libro/12345 → "12345"
   - The ID is the number BEFORE the hyphen-title in the URL

3. **detail_url**: The full URL path to view the book details (e.g., /bjv/detalle-libro/7867-...)

4. **authors**: Names of authors if visible, comma-separated if multiple

5. **year**: Publication year as a 4-digit number if visible

6. **content_type**: Classify as "libro" for books

7. **area**: Legal subject area if visible:
   - civil, penal, constitucional, administrativo, mercantil, laboral, fiscal, internacional, general

8. **snippet**: Any preview text or description shown

Also extract pagination if present:
- **total_results**: Total count if shown
- **current_page**: Current page number
- **total_pages**: Total pages
- **has_next_page**: True if there's pagination showing more pages

IMPORTANT: Look for ALL book links on the page, including:
- Featured books in sliders/carousels
- Recent publications
- Book listings in any section
- Links containing "/bjv/detalle-libro/"

Extract every unique book you can find on the page.
"""

    def __init__(
        self,
        model: str = "openrouter/google/gemini-3-flash-preview",
        api_token: Optional[str] = None,
    ):
        """
        Initialize the BJV LLM parser.

        Args:
            model: LLM model to use (LiteLLM format)
            api_token: API token for the model provider
        """
        self.adapter = Crawl4AIAdapter(
            provider=model,
            api_token=api_token,
        )

    async def search(
        self,
        query: Optional[str] = None,
        area: Optional[str] = None,
        page: int = 1,
    ) -> tuple[List[BJVSearchResult], bool]:
        """
        Search BJV and extract book results.

        Strategy: Uses HTML pattern matching for book links on the catalog page,
        with LLM fallback for search results pages.

        Args:
            query: Search query string (optional - if None, uses catalog page)
            area: Filter by legal area
            page: Page number (1-indexed)

        Returns:
            Tuple of (list of BJVSearchResult, has_more_pages)
        """
        # For page 1 without specific query, use the main catalog page
        # which has books visible without AJAX loading
        if page == 1 and not query and not area:
            url = self.CATALOG_URL
            use_html_extraction = True
        else:
            # Build search URL for filtered/paginated searches
            params = {}
            if query:
                params["ti"] = query  # Title search parameter
            if area:
                params["area"] = area
            if page > 1:
                params["page"] = str(page)

            if params:
                url = f"{self.SEARCH_URL}?{urllib.parse.urlencode(params)}"
            else:
                url = self.CATALOG_URL
            use_html_extraction = not query and not area

        logger.info(f"Searching BJV: {url} (html_extraction={use_html_extraction})")

        if use_html_extraction:
            # Use HTML extraction for catalog page (more reliable)
            return await self._extract_from_html(url)
        else:
            # Use LLM extraction for search results
            return await self._extract_with_llm(url, query)

    async def _extract_from_html(self, url: str) -> tuple[List[BJVSearchResult], bool]:
        """
        Extract book links directly from HTML using pattern matching.

        More reliable for the BJV catalog page which has consistent HTML structure.
        """
        import re
        from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig

        browser_config = BrowserConfig(headless=True, browser_type="chromium")
        run_config = CrawlerRunConfig(delay_before_return_html=2.0)

        try:
            async with AsyncWebCrawler(config=browser_config) as crawler:
                result = await crawler.arun(url=url, config=run_config)

                if not result.success or not result.html:
                    logger.warning(f"Failed to fetch BJV page: {result.error_message}")
                    return [], False

                # Extract book links using regex pattern
                # Pattern: /bjv/detalle-libro/{id}-{title-slug}
                book_pattern = re.compile(
                    r'/bjv/detalle-libro/(\d+)-([^"\'<>\s]+)',
                    re.IGNORECASE
                )

                # Find all unique books
                seen_ids = set()
                search_results = []

                for match in book_pattern.finditer(result.html):
                    book_id = match.group(1)
                    slug = match.group(2)

                    if book_id in seen_ids:
                        continue
                    seen_ids.add(book_id)

                    # Convert slug to title (replace hyphens with spaces, capitalize)
                    title = slug.replace('-', ' ').title()

                    libro_id = IdentificadorLibro(bjv_id=book_id)
                    search_results.append(BJVSearchResult(
                        libro_id=libro_id,
                        titulo=title,
                        tipo_contenido=TipoContenido.LIBRO,
                        autores_texto=None,
                        anio=None,
                        area=None,
                        fragmento=None,
                    ))

                logger.info(f"BJV HTML extraction: Found {len(search_results)} books")
                # Catalog page doesn't have pagination
                return search_results, False

        except Exception as e:
            logger.error(f"BJV HTML extraction error: {e}")
            return [], False

    async def _extract_with_llm(
        self, url: str, query: Optional[str]
    ) -> tuple[List[BJVSearchResult], bool]:
        """
        Extract books using LLM for search results pages.
        """
        # Extract using Crawl4AI with JavaScript rendering and delay
        result = await self.adapter.extract_structured(
            url=url,
            schema=ExtractedBJVSearchResults,
            instruction=self.EXTRACTION_INSTRUCTION,
            wait_for_js=True,
            delay_seconds=3.0,
        )

        if not result:
            logger.warning(f"LLM extraction returned None for BJV search: {query}")
            return [], False

        if not result.books:
            logger.warning(f"No books extracted from BJV search: {query}")
            return [], False

        # Convert to domain results
        search_results = []
        for book in result.books:
            try:
                domain_result = self._to_domain_result(book)
                if domain_result:
                    search_results.append(domain_result)
            except Exception as e:
                logger.warning(f"Error converting book result: {e}")
                continue

        logger.info(
            f"BJV LLM search '{query}': Found {len(search_results)} books "
            f"(page {result.current_page or 1}/{result.total_pages or '?'}, "
            f"has_next: {result.has_next_page})"
        )

        return search_results, result.has_next_page

    async def search_multiple_pages(
        self,
        query: str,
        area: Optional[str] = None,
        max_pages: int = 5,
        max_results: Optional[int] = None,
    ) -> List[BJVSearchResult]:
        """
        Search BJV across multiple pages.

        Args:
            query: Search query string
            area: Filter by legal area
            max_pages: Maximum pages to fetch
            max_results: Maximum total results to return

        Returns:
            List of BJVSearchResult objects
        """
        all_results = []

        for page in range(1, max_pages + 1):
            results, has_next = await self.search(
                query=query,
                area=area,
                page=page,
            )

            all_results.extend(results)

            # Check limits
            if max_results and len(all_results) >= max_results:
                all_results = all_results[:max_results]
                break

            if not has_next or not results:
                break

        return all_results

    def _to_domain_result(self, book: ExtractedBJVBook) -> Optional[BJVSearchResult]:
        """Convert extracted book to domain result."""
        if not book.title:
            return None

        # Extract book ID
        book_id = book.book_id
        if not book_id and book.detail_url:
            # Try to extract from URL
            import re
            match = re.search(r"/(?:detalle-libro|detalle|capitulo)/(\d+)", book.detail_url)
            if match:
                book_id = match.group(1)

        if not book_id:
            # Generate a placeholder ID from title hash
            import hashlib
            book_id = hashlib.md5(book.title.encode()).hexdigest()[:8]

        # Map content type
        tipo = TipoContenido.LIBRO
        if book.content_type:
            tipo_map = {
                "libro": TipoContenido.LIBRO,
                "capitulo": TipoContenido.CAPITULO,
                "articulo": TipoContenido.ARTICULO,
                "revista": TipoContenido.REVISTA,
            }
            tipo = tipo_map.get(book.content_type.lower(), TipoContenido.LIBRO)

        return BJVSearchResult(
            libro_id=IdentificadorLibro(bjv_id=book_id),
            titulo=book.title,
            tipo_contenido=tipo,
            autores_texto=book.authors,
            anio=book.year,
            area=book.area,
            fragmento=book.snippet,
        )
