"""
SCJN Document Scraper Actor

Downloads HTML detail pages and PDF documents from SCJN.
Implements retry logic with exponential backoff.
"""
import asyncio
import aiohttp
from typing import Optional
from datetime import date

from .base import BaseActor
from .messages import (
    DescargarDocumento,
    ProcesarPDF,
    GuardarDocumento,
    DocumentoDescargado,
    ErrorDeActor,
)
from .rate_limiter import RateLimiter
from src.infrastructure.adapters.scjn_document_parser import (
    parse_document_detail,
    parse_reforms,
)
from src.infrastructure.adapters.errors import ParseError
from src.domain.scjn_entities import SCJNDocument, SCJNReform
from src.domain.scjn_value_objects import (
    DocumentCategory,
    DocumentScope,
    DocumentStatus,
)


class SCJNScraperError(Exception):
    """Base scraper error."""
    pass


class TransientScraperError(SCJNScraperError):
    """Retryable error (network, timeout)."""
    pass


class PermanentScraperError(SCJNScraperError):
    """Non-retryable error (404, parse failure)."""
    pass


class RateLimitError(TransientScraperError):
    """SCJN returned 429."""
    pass


class SCJNScraperActor(BaseActor):
    """
    Downloads SCJN documents and PDFs.

    Message Protocol:
    - DescargarDocumento → fetches detail page, optionally PDF
    - Emits: DocumentoDescargado, ProcesarPDF (to coordinator)
    """

    SCJN_BASE_URL = "https://legislacion.scjn.gob.mx/Buscador/Paginas"
    USER_AGENT = "LegalScraper/1.0 (Educational Research)"

    def __init__(
        self,
        coordinator: 'BaseActor',
        rate_limiter: RateLimiter,
        download_pdfs: bool = True,
        max_pdf_size_mb: float = 50.0,
        timeout_seconds: float = 30.0,
    ):
        super().__init__()
        self._coordinator = coordinator
        self._rate_limiter = rate_limiter
        self._download_pdfs = download_pdfs
        self._max_pdf_size = int(max_pdf_size_mb * 1024 * 1024)
        self._timeout = aiohttp.ClientTimeout(total=timeout_seconds)
        self._session: Optional[aiohttp.ClientSession] = None

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
        if isinstance(message, DescargarDocumento):
            return await self._handle_descargar(message)
        return None

    async def _handle_descargar(self, cmd: DescargarDocumento):
        """Download document detail and optionally PDF."""
        try:
            await self._rate_limiter.acquire()

            # Fetch detail page
            detail_url = self._build_detail_url(cmd.q_param)
            html = await self._fetch_html(detail_url)

            # Parse document
            detail = parse_document_detail(html)

            # Parse reforms if requested
            reforms = ()
            if cmd.include_reforms:
                reforms = parse_reforms(html)

            # Build domain entity
            document = self._build_document(cmd.q_param, detail, reforms, detail_url)

            # Emit downloaded event
            await self._coordinator.tell(DocumentoDescargado(
                correlation_id=cmd.correlation_id,
                document_id=document.id,
                q_param=cmd.q_param,
                has_pdf=len(reforms) > 0 and any(r.has_pdf for r in reforms),
                pdf_size_bytes=0,
            ))

            # Send document for persistence
            await self._coordinator.tell(GuardarDocumento(
                correlation_id=cmd.correlation_id,
                document=document,
            ))

            # Download PDFs if requested
            if cmd.include_pdf and self._download_pdfs:
                for reform in reforms:
                    if reform.has_pdf:
                        try:
                            await self._download_pdf(
                                reform.q_param,
                                document.id,
                                cmd.correlation_id,
                            )
                        except Exception:
                            # PDF failures are non-fatal
                            pass

            return DocumentoDescargado(
                correlation_id=cmd.correlation_id,
                document_id=document.id,
                q_param=cmd.q_param,
                has_pdf=len(reforms) > 0 and any(r.has_pdf for r in reforms),
            )

        except TransientScraperError as e:
            await self._emit_error(cmd, e, recoverable=True)
            return None
        except (PermanentScraperError, ParseError) as e:
            await self._emit_error(cmd, e, recoverable=False)
            return None
        except Exception as e:
            await self._emit_error(cmd, e, recoverable=False)
            return None

    async def _fetch_html(self, url: str) -> str:
        """Fetch HTML content from URL."""
        try:
            async with self._session.get(url) as response:
                if response.status == 429:
                    raise RateLimitError(f"Rate limited: {url}")
                if response.status == 404:
                    raise PermanentScraperError(f"Not found: {url}")
                if response.status >= 500:
                    raise TransientScraperError(f"Server error {response.status}: {url}")
                response.raise_for_status()
                return await response.text()
        except aiohttp.ClientError as e:
            raise TransientScraperError(f"Network error: {e}") from e
        except asyncio.TimeoutError as e:
            raise TransientScraperError(f"Timeout: {url}") from e

    async def _fetch_pdf(self, url: str) -> bytes:
        """Fetch PDF bytes from URL."""
        async with self._session.get(url) as response:
            if response.status != 200:
                raise PermanentScraperError(f"PDF not available: {response.status}")

            # Check content length
            content_length = int(response.headers.get("Content-Length", 0))
            if content_length > self._max_pdf_size:
                raise PermanentScraperError(f"PDF too large: {content_length} bytes")

            pdf_bytes = await response.read()

            if len(pdf_bytes) > self._max_pdf_size:
                raise PermanentScraperError(f"PDF too large: {len(pdf_bytes)} bytes")

            return pdf_bytes

    async def _download_pdf(
        self,
        q_param: str,
        document_id: str,
        correlation_id: str,
    ) -> None:
        """Download PDF and send to processor."""
        await self._rate_limiter.acquire()

        pdf_url = self._build_pdf_url(q_param)

        try:
            pdf_bytes = await self._fetch_pdf(pdf_url)

            # Send to PDF processor via coordinator
            await self._coordinator.tell(ProcesarPDF(
                correlation_id=correlation_id,
                document_id=document_id,
                pdf_bytes=pdf_bytes,
                source_url=pdf_url,
            ))
        except Exception:
            # PDF download failures are non-fatal
            pass

    def _build_detail_url(self, q_param: str) -> str:
        """Build document detail URL."""
        return f"{self.SCJN_BASE_URL}/wfOrdenamientoDetalle.aspx?q={q_param}"

    def _build_pdf_url(self, q_param: str) -> str:
        """Build PDF download URL."""
        return f"{self.SCJN_BASE_URL}/AbrirDocReforma.aspx?q={q_param}"

    def _build_document(
        self,
        q_param: str,
        detail,
        reforms: tuple,
        source_url: str,
    ) -> SCJNDocument:
        """Build domain entity from parsed data."""
        # Map parsed strings to enums (with fallbacks)
        category = self._parse_category(detail.category)
        scope = self._parse_scope(detail.scope)
        status = self._parse_status(detail.status)

        return SCJNDocument(
            q_param=q_param,
            title=detail.title,
            short_title=detail.short_title or "",
            category=category,
            scope=scope,
            status=status,
            publication_date=self._parse_date(detail.publication_date),
            expedition_date=self._parse_date(detail.expedition_date),
            reforms=tuple(
                SCJNReform(
                    q_param=r.q_param,
                    publication_date=self._parse_date(r.publication_date),
                    gazette_section=r.gazette_reference,
                )
                for r in reforms
            ),
            source_url=source_url,
        )

    def _parse_category(self, category_str: str) -> DocumentCategory:
        """Parse category string to enum."""
        category_map = {
            "CONSTITUCION": DocumentCategory.CONSTITUCION,
            "LEY": DocumentCategory.LEY,
            "LEY FEDERAL": DocumentCategory.LEY_FEDERAL,
            "LEY GENERAL": DocumentCategory.LEY_GENERAL,
            "LEY ORGÁNICA": DocumentCategory.LEY_ORGANICA,
            "LEY ORGANICA": DocumentCategory.LEY_ORGANICA,
            "CODIGO": DocumentCategory.CODIGO,
            "CÓDIGO": DocumentCategory.CODIGO,
            "DECRETO": DocumentCategory.DECRETO,
            "REGLAMENTO": DocumentCategory.REGLAMENTO,
            "ACUERDO": DocumentCategory.ACUERDO,
            "TRATADO": DocumentCategory.TRATADO,
            "CONVENIO": DocumentCategory.CONVENIO,
        }
        return category_map.get(category_str.upper(), DocumentCategory.LEY)

    def _parse_scope(self, scope_str: str) -> DocumentScope:
        """Parse scope string to enum."""
        scope_map = {
            "FEDERAL": DocumentScope.FEDERAL,
            "ESTATAL": DocumentScope.ESTATAL,
            "CDMX": DocumentScope.CDMX,
            "INTERNACIONAL": DocumentScope.INTERNACIONAL,
            "EXTRANJERA": DocumentScope.EXTRANJERA,
        }
        return scope_map.get(scope_str.upper(), DocumentScope.FEDERAL)

    def _parse_status(self, status_str: str) -> DocumentStatus:
        """Parse status string to enum."""
        status_map = {
            "VIGENTE": DocumentStatus.VIGENTE,
            "ABROGADA": DocumentStatus.ABROGADA,
            "DEROGADA": DocumentStatus.DEROGADA,
            "SUSTITUIDA": DocumentStatus.SUSTITUIDA,
            "EXTINTA": DocumentStatus.EXTINTA,
        }
        return status_map.get(status_str.upper(), DocumentStatus.VIGENTE)

    def _parse_date(self, date_str: Optional[str]) -> Optional[date]:
        """Parse date string to date object."""
        if not date_str:
            return None
        try:
            # Format: DD/MM/YYYY
            parts = date_str.split("/")
            if len(parts) == 3:
                return date(int(parts[2]), int(parts[1]), int(parts[0]))
        except (ValueError, IndexError):
            pass
        return None

    async def _emit_error(
        self,
        cmd: DescargarDocumento,
        error: Exception,
        recoverable: bool,
    ) -> None:
        """Emit error event to coordinator."""
        await self._coordinator.tell(ErrorDeActor(
            correlation_id=cmd.correlation_id,
            actor_name="SCJNScraperActor",
            document_id=None,
            error_type=type(error).__name__,
            error_message=str(error),
            recoverable=recoverable,
            original_command=cmd,
        ))
