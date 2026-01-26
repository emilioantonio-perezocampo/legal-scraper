"""
SCJN scraper using LLM extraction via OpenRouter.
Replaces brittle CSS selectors with semantic AI extraction.
"""
import asyncio
from typing import List, Optional
from datetime import date
from pydantic import BaseModel, Field
import logging

from src.infrastructure.openrouter_client import OpenRouterClient
from src.infrastructure.async_fetcher import AsyncFetcher
from src.domain.scjn_entities import SCJNDocument
from src.domain.scjn_value_objects import (
    DocumentCategory,
    DocumentScope,
    DocumentStatus,
)

logger = logging.getLogger(__name__)


# --- Pydantic Schemas for LLM Extraction ---

class ExtractedDocument(BaseModel):
    """Schema for a single document extracted by LLM."""

    title: str = Field(description="Full name of the law, code, or decree")
    category: Optional[str] = Field(
        default=None,
        description=(
            "Type: CONSTITUCION, LEY, LEY_FEDERAL, LEY_GENERAL, "
            "LEY_ORGANICA, CODIGO, DECRETO, REGLAMENTO, ACUERDO, TRATADO, CONVENIO"
        )
    )
    scope: Optional[str] = Field(
        default=None,
        description="Scope: FEDERAL, ESTATAL, CDMX, INTERNACIONAL, EXTRANJERA"
    )
    status: Optional[str] = Field(
        default=None,
        description=(
            "Status: VIGENTE (active), ABROGADA (repealed), "
            "DEROGADA (partially repealed), SUSTITUIDA, EXTINTA"
        )
    )
    publication_date: Optional[str] = Field(
        default=None,
        description="Publication date in YYYY-MM-DD format"
    )
    q_param: Optional[str] = Field(
        default=None,
        description="The 'q' parameter value from detail URL (encrypted ID)"
    )
    detail_url: Optional[str] = Field(
        default=None,
        description="URL link to the full document"
    )


class ExtractedSearchResults(BaseModel):
    """Schema for search results page."""

    documents: List[ExtractedDocument] = Field(
        default_factory=list,
        description="List of all legal documents found on the page"
    )
    total_results: Optional[int] = Field(
        default=None,
        description="Total number of results if shown"
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
        description="Whether there is a next page of results"
    )


# --- Main Parser Class ---

class SCJNLLMParser:
    """LLM-powered parser for SCJN legal documents using OpenRouter."""

    BASE_URL = "https://legislacion.scjn.gob.mx"
    SEARCH_URL = f"{BASE_URL}/Buscador/Paginas/Buscar.aspx"
    # URL for recent reforms (shows actual results without form submission)
    REFORMAS_URL = f"{BASE_URL}/Buscador/Paginas/wfReformasResultados.aspx?q=78tXSP4D9DqLbEkEfwPY3A=="

    EXTRACTION_INSTRUCTION = """
You are extracting Mexican legal documents from the SCJN (Supreme Court of Justice) website.

Extract ALL legal documents visible on this search results page. For each document found:

1. **title**: The complete official name of the law, code, or decree
   Example: "Constitucion Politica de los Estados Unidos Mexicanos"
   Example: "Ley Federal del Trabajo"

2. **category**: Classify the document type:
   - CONSTITUCION (Constitution)
   - LEY (General law)
   - LEY_FEDERAL (Federal Law)
   - LEY_GENERAL (General Law)
   - LEY_ORGANICA (Organic Law)
   - CODIGO (Code - Civil, Penal, Commercial, etc.)
   - DECRETO (Decree)
   - REGLAMENTO (Regulation)
   - ACUERDO (Agreement)
   - TRATADO (International Treaty)
   - CONVENIO (Convention)

3. **scope**: Geographic/jurisdictional scope:
   - FEDERAL (Federal level)
   - ESTATAL (State level)
   - CDMX (Mexico City)
   - INTERNACIONAL (International)
   - EXTRANJERA (Foreign)

4. **status**: Current legal status:
   - VIGENTE (Currently in force)
   - ABROGADA (Completely repealed)
   - DEROGADA (Partially repealed)
   - SUSTITUIDA (Replaced)
   - EXTINTA (Extinct)

5. **publication_date**: Date in YYYY-MM-DD format (if visible)

6. **q_param**: Extract the 'q' parameter from URLs like:
   Documento.aspx?q=... (this is the encrypted document ID)

7. **detail_url**: The link/URL to view the full document

Also determine:
- **total_results**: The total count of documents if shown on the page
- **current_page**: Current page number
- **total_pages**: Total number of pages
- **has_next_page**: True if there's pagination showing more pages

Be thorough - extract every document listing visible on the page.
If a field is not visible or cannot be determined, use null.
Look for patterns in the HTML like tables, divs with document listings, links with 'Documento.aspx?q=' etc.
"""

    def __init__(
        self,
        model: str = "google/gemini-3-flash-preview",  # Fast Gemini 3 Flash model
        rate_limit_delay: float = 2.0,
        api_key: Optional[str] = None
    ):
        """
        Initialize the LLM parser.

        Args:
            model: OpenRouter model to use
            rate_limit_delay: Seconds between HTTP requests
            api_key: OpenRouter API key (or use env var)
        """
        self.llm = OpenRouterClient(model=model, api_key=api_key)
        self.fetcher = AsyncFetcher(rate_limit_delay=rate_limit_delay)
        self.model = model

    async def parse_search_page(
        self,
        page: int = 1,
        category: Optional[str] = None,
        scope: Optional[str] = None,
        status: Optional[str] = None,
        query: Optional[str] = None
    ) -> tuple[List[SCJNDocument], bool]:
        """
        Parse a search results page and return domain entities.

        Args:
            page: Page number (1-indexed)
            category: Filter by document category
            scope: Filter by scope
            status: Filter by status
            query: Search query string

        Returns:
            Tuple of (list of SCJNDocument entities, has_more_pages)
        """
        # Use reforms URL which shows actual results (search requires POST)
        # For page 1 with no filters, use the recent reforms page
        if page == 1 and not any([category, scope, status, query]):
            url = self.REFORMAS_URL
        else:
            # Build URL with parameters for filtered searches
            params = []
            if category:
                params.append(f"categoria={category}")
            if scope:
                params.append(f"ambito={scope}")
            if status:
                params.append(f"estatus={status}")
            if page > 1:
                params.append(f"pagina={page}")

            url = self.SEARCH_URL
            if params:
                url += "?" + "&".join(params)

        logger.info(f"Fetching SCJN page: {url}")

        # Fetch HTML
        html = await self.fetcher.fetch(url)
        if not html:
            logger.error(f"Failed to fetch {url}")
            return [], False

        logger.info(f"Fetched {len(html)} chars, extracting with {self.model}...")

        # Extract using LLM with higher max_tokens for full JSON output
        result = await self.llm.extract_from_html(
            html=html,
            schema=ExtractedSearchResults,
            instruction=self.EXTRACTION_INSTRUCTION,
            truncate_html=True,
            max_html_chars=40000  # Reduced to avoid output truncation
        )

        if not result:
            logger.warning(f"LLM extraction returned None for page {page}")
            return [], False

        if not result.documents:
            logger.warning(f"No documents extracted from page {page}")
            return [], False

        # Extract real q_params from HTML using regex as fallback
        import re
        real_q_params = re.findall(r'wfOrdenamientoDetalle\.aspx\?q=([A-Za-z0-9+/=]{20,})', html)
        if not real_q_params:
            # Try alternate pattern
            real_q_params = re.findall(r'q=([A-Za-z0-9+/=]{30,})', html)

        logger.info(f"Found {len(real_q_params)} real q_params via regex")

        # Convert to domain entities, using real q_params when available
        documents = []
        for i, doc in enumerate(result.documents):
            try:
                # Use regex-extracted q_param if LLM didn't get a valid one
                if real_q_params and i < len(real_q_params):
                    if not doc.q_param or len(doc.q_param) < 10:
                        doc = ExtractedDocument(
                            title=doc.title,
                            category=doc.category,
                            scope=doc.scope,
                            status=doc.status,
                            publication_date=doc.publication_date,
                            q_param=real_q_params[i],
                            detail_url=f"https://legislacion.scjn.gob.mx/Buscador/Paginas/wfOrdenamientoDetalle.aspx?q={real_q_params[i]}"
                        )

                domain_doc = self._to_domain_entity(doc)
                if domain_doc:
                    documents.append(domain_doc)
            except Exception as e:
                logger.warning(f"Error creating document entity: {e}")
                continue

        logger.info(
            f"Extracted {len(documents)} documents from page {page}"
            f" (total: {result.total_results}, has_next: {result.has_next_page})"
        )

        return documents, result.has_next_page

    async def parse_multiple_pages(
        self,
        max_pages: int = 5,
        max_results: Optional[int] = None,
        category: Optional[str] = None,
        scope: Optional[str] = None,
        status: Optional[str] = None
    ) -> List[SCJNDocument]:
        """Parse multiple pages of search results."""
        all_documents = []

        for page in range(1, max_pages + 1):
            documents, has_next = await self.parse_search_page(
                page=page,
                category=category,
                scope=scope,
                status=status
            )

            all_documents.extend(documents)

            # Check if we have enough results
            if max_results and len(all_documents) >= max_results:
                all_documents = all_documents[:max_results]
                break

            # If no more pages, stop
            if not has_next or not documents:
                break

            # Rate limit between pages
            await asyncio.sleep(1.0)

        return all_documents

    def _to_domain_entity(self, doc: ExtractedDocument) -> Optional[SCJNDocument]:
        """Convert extracted document to domain entity."""
        if not doc.title:
            return None

        # Parse publication date
        pub_date = None
        if doc.publication_date:
            try:
                pub_date = date.fromisoformat(doc.publication_date)
            except ValueError:
                logger.debug(f"Could not parse date: {doc.publication_date}")

        return SCJNDocument(
            q_param=doc.q_param or "",
            title=doc.title,
            category=self._map_category(doc.category),
            scope=self._map_scope(doc.scope),
            status=self._map_status(doc.status),
            publication_date=pub_date,
            source_url=self._normalize_url(doc.detail_url),
        )

    def _map_category(self, cat: Optional[str]) -> DocumentCategory:
        """Map extracted category string to domain enum."""
        if not cat:
            return DocumentCategory.LEY

        cat_upper = cat.upper().replace(" ", "_")

        mapping = {
            "CONSTITUCION": DocumentCategory.CONSTITUCION,
            "LEY": DocumentCategory.LEY,
            "LEY_FEDERAL": DocumentCategory.LEY_FEDERAL,
            "LEY_GENERAL": DocumentCategory.LEY_GENERAL,
            "LEY_ORGANICA": DocumentCategory.LEY_ORGANICA,
            "CODIGO": DocumentCategory.CODIGO,
            "DECRETO": DocumentCategory.DECRETO,
            "REGLAMENTO": DocumentCategory.REGLAMENTO,
            "ACUERDO": DocumentCategory.ACUERDO,
            "TRATADO": DocumentCategory.TRATADO,
            "CONVENIO": DocumentCategory.CONVENIO,
        }

        return mapping.get(cat_upper, DocumentCategory.LEY)

    def _map_scope(self, scope: Optional[str]) -> DocumentScope:
        """Map extracted scope string to domain enum."""
        if not scope:
            return DocumentScope.FEDERAL

        scope_upper = scope.upper()

        mapping = {
            "FEDERAL": DocumentScope.FEDERAL,
            "ESTATAL": DocumentScope.ESTATAL,
            "CDMX": DocumentScope.CDMX,
            "INTERNACIONAL": DocumentScope.INTERNACIONAL,
            "EXTRANJERA": DocumentScope.EXTRANJERA,
        }

        return mapping.get(scope_upper, DocumentScope.FEDERAL)

    def _map_status(self, status: Optional[str]) -> DocumentStatus:
        """Map extracted status string to domain enum."""
        if not status:
            return DocumentStatus.VIGENTE

        status_upper = status.upper()

        mapping = {
            "VIGENTE": DocumentStatus.VIGENTE,
            "ABROGADA": DocumentStatus.ABROGADA,
            "DEROGADA": DocumentStatus.DEROGADA,
            "SUSTITUIDA": DocumentStatus.SUSTITUIDA,
            "EXTINTA": DocumentStatus.EXTINTA,
        }

        return mapping.get(status_upper, DocumentStatus.VIGENTE)

    def _normalize_url(self, url: Optional[str]) -> str:
        """Convert relative URLs to absolute."""
        if not url:
            return ""
        if url.startswith("http"):
            return url
        if url.startswith("/"):
            return f"{self.BASE_URL}{url}"
        return f"{self.BASE_URL}/{url}"
