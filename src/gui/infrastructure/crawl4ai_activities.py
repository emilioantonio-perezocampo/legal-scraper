"""
Temporal Activities for Crawl4AI-based document extraction.

This module provides direct Crawl4AI extraction activities that bypass
the API layer for more efficient and observable scraping workflows.

These activities use the new LLM-based parsers:
- SCJN: scjn_llm_parser.py (OpenRouter + Gemini)
- BJV: bjv_llm_parser.py (HTML regex + Crawl4AI)
- CAS: cas_llm_parser.py (Playwright + table extraction)
- DOF: dof_index_parser.py (HTML parsing)

PDF/Document Download Support:
- SCJN: AbrirDocReforma.aspx?q={q_param} for PDF downloads
- DOF: nota_detalle.php for detail pages with PDF links
- BJV: url_pdf field from book detail pages
- CAS: URLLaudo for arbitral award PDFs
"""
import json
import logging
import os
import re
import urllib.parse
import aiohttp
from dataclasses import dataclass, asdict, field
from datetime import date, datetime
from pathlib import Path
from typing import Any, List, Optional

from temporalio import activity

logger = logging.getLogger(__name__)

# User agent for HTTP requests
USER_AGENT = "LegalScraper/1.0 (Educational Research)"
SOURCE_FILE_PENDING_STATUS = "pending"
HTML_FALLBACK_SOURCES = {"bjv", "cas"}
SCRAPER_DOCUMENT_ALIASES_TABLE = "scraper_document_aliases"


def _normalize_embedding_status(value: Any) -> str:
    return str(value or "").strip().lower()


def _coerce_chunk_count(value: Any) -> int:
    try:
        return max(int(value or 0), 0)
    except Exception:
        return 0


def _build_scraper_document_row(
    source: str,
    doc: dict,
    existing_row: Optional[dict] = None,
) -> dict:
    """Merge scraper_documents upserts without clobbering indexing state."""
    pub_date = doc.get("publication_date")
    if pub_date and len(pub_date) > 10:
        pub_date = pub_date[:10]

    incoming_storage_path = doc.get("pdf_storage_path") or doc.get("storage_path")
    existing_storage_path = (existing_row or {}).get("storage_path")
    effective_storage_path = incoming_storage_path or existing_storage_path

    row = {
        "source_type": source,
        "external_id": doc.get("external_id"),
        "title": doc.get("title", "Untitled")[:500],
        "publication_date": pub_date,
        "storage_path": effective_storage_path,
    }

    existing_status = _normalize_embedding_status((existing_row or {}).get("embedding_status"))
    existing_chunk_count = _coerce_chunk_count((existing_row or {}).get("chunk_count"))

    if existing_row is None:
        row["embedding_status"] = SOURCE_FILE_PENDING_STATUS
    elif effective_storage_path and existing_chunk_count == 0:
        if existing_status in {"", "pending", "awaiting_source_file", "failed"}:
            row["embedding_status"] = "pending"
    elif not effective_storage_path and existing_status in {"", "pending", "awaiting_source_file"}:
        row["embedding_status"] = SOURCE_FILE_PENDING_STATUS

    return row


def _looks_like_scjn_q_param(value: Optional[str]) -> bool:
    candidate = str(value or "").strip()
    if not candidate or candidate.startswith("scjn-") or candidate.startswith("legislacion-q:"):
        return False
    return bool(re.fullmatch(r"[A-Za-z0-9+/=]{20,}", candidate))


def _canonicalize_document_identity(
    source: str,
    doc: dict[str, Any],
) -> tuple[str, list[tuple[str, str]]]:
    external_id = str(doc.get("external_id") or "").strip()
    metadata = doc.get("metadata") if isinstance(doc.get("metadata"), dict) else {}
    aliases: list[tuple[str, str]] = []

    if source == "scjn":
        q_param = str((metadata or {}).get("q_param") or "").strip()
        if q_param and not q_param.startswith("legislacion-q:"):
            aliases.append(("q_param", q_param))
            return f"legislacion-q:{q_param}", aliases
        if external_id.startswith("legislacion-q:"):
            raw_q_param = external_id.split(":", 1)[1].strip()
            if raw_q_param:
                aliases.append(("q_param", raw_q_param))
            return external_id, aliases
        if _looks_like_scjn_q_param(external_id):
            aliases.append(("q_param", external_id))
            return f"legislacion-q:{external_id}", aliases
        return external_id, aliases

    return external_id, aliases


def _lookup_existing_scraper_document(
    client: Any,
    source: str,
    external_id: str,
    aliases: list[tuple[str, str]],
) -> Optional[dict[str, Any]]:
    candidates = [external_id, *[alias_value for _alias_type, alias_value in aliases]]
    seen: set[str] = set()
    for candidate in candidates:
        normalized = str(candidate or "").strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        lookup = (
            client.table("scraper_documents")
            .select("id, storage_path, embedding_status, chunk_count")
            .eq("source_type", source)
            .eq("external_id", normalized)
            .limit(1)
            .execute()
        )
        if getattr(lookup, "data", None):
            return lookup.data[0]

    for alias_type, alias_value in aliases:
        normalized = str(alias_value or "").strip()
        if not normalized:
            continue
        try:
            alias_lookup = (
                client.table(SCRAPER_DOCUMENT_ALIASES_TABLE)
                .select("document_id")
                .eq("source_type", source)
                .eq("alias_value", normalized)
                .limit(1)
                .execute()
            )
        except Exception:
            return None
        if not getattr(alias_lookup, "data", None):
            continue
        document_id = alias_lookup.data[0].get("document_id")
        if not document_id:
            continue
        lookup = (
            client.table("scraper_documents")
            .select("id, storage_path, embedding_status, chunk_count")
            .eq("id", document_id)
            .limit(1)
            .execute()
        )
        if getattr(lookup, "data", None):
            return lookup.data[0]
    return None


def _upsert_scraper_document_aliases(
    client: Any,
    source: str,
    document_id: str,
    aliases: list[tuple[str, str]],
) -> None:
    rows = []
    seen: set[str] = set()
    for alias_type, alias_value in aliases:
        normalized = str(alias_value or "").strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        rows.append(
            {
                "document_id": document_id,
                "source_type": source,
                "alias_type": alias_type,
                "alias_value": normalized,
            }
        )
    if not rows:
        return
    try:
        client.table(SCRAPER_DOCUMENT_ALIASES_TABLE).upsert(
            rows,
            on_conflict="source_type,alias_value",
        ).execute()
    except Exception:
        return


@dataclass
class ExtractedDocument:
    """Unified document structure for all sources."""
    source: str
    external_id: str
    title: str
    content_type: str
    publication_date: Optional[str] = None
    url: Optional[str] = None
    metadata: Optional[dict] = None
    # PDF/Document download fields
    pdf_url: Optional[str] = None
    pdf_downloaded: bool = False
    pdf_path: Optional[str] = None
    pdf_storage_path: Optional[str] = None  # Supabase storage path


@dataclass
class ExtractionResult:
    """Result of a Crawl4AI extraction activity."""
    source: str
    success: bool
    document_count: int
    documents: List[dict]
    errors: List[str]
    output_directory: Optional[str] = None
    pdfs_downloaded: int = 0


@dataclass
class DownloadResult:
    """Result of a document download activity."""
    source: str
    success: bool
    total_documents: int
    downloaded_count: int
    failed_count: int
    skipped_count: int
    errors: List[str] = field(default_factory=list)
    storage_paths: List[str] = field(default_factory=list)
    downloaded_documents: List[dict] = field(default_factory=list)


# =============================================================================
# PDF URL Builders
# =============================================================================

def build_scjn_pdf_url(q_param: str) -> str:
    """Build SCJN PDF download URL from q_param."""
    return f"https://legislacion.scjn.gob.mx/Buscador/Paginas/AbrirDocReforma.aspx?q={q_param}"


def build_dof_detail_url(cod_diario: str) -> str:
    """Build DOF detail page URL."""
    return f"https://dof.gob.mx/nota_detalle.php?codigo={cod_diario}"


def build_cas_pdf_url(case_url: str) -> Optional[str]:
    """Extract PDF URL from CAS case URL if it's a direct PDF link."""
    if case_url and '.pdf' in case_url.lower():
        return case_url
    return case_url  # CAS detail pages contain the PDF link


def _normalize_absolute_url(base_url: str, url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    return urllib.parse.urljoin(base_url, url)


def _extract_bjv_libro_id(detail_url: str) -> str:
    match = re.search(r"/(?:detalle-libro|detalle|capitulo)/(\d+)", detail_url or "")
    if match:
        return match.group(1)
    return "unknown"


def _normalize_source_url(source: str, url: Optional[str]) -> Optional[str]:
    if not url:
        return None

    base_urls = {
        "bjv": "https://biblio.juridicas.unam.mx/",
        "cas": "https://jurisprudence.tas-cas.org/",
    }
    return _normalize_absolute_url(base_urls.get(source, ""), url) if source in base_urls else url


def _looks_like_html_document(content: bytes, content_type: str = "") -> bool:
    snippet = (content or b"")[:256].lstrip().lower()
    normalized_type = (content_type or "").lower()
    return (
        "html" in normalized_type
        or snippet.startswith(b"<!doctype html")
        or snippet.startswith(b"<!doc")
        or snippet.startswith(b"<html")
    )


def _is_bjv_detail_page_html(source_url: str, html: str) -> bool:
    normalized_url = (source_url or "").lower()
    normalized_html = (html or "").lower()

    if not any(
        marker in normalized_url
        for marker in (
            "/bjv/detalle-libro/",
            "/bjv/detallelibro/",
            "/es/bjv/detallelibro/",
            "/en/bjv/detallelibro/",
        )
    ):
        return False

    return any(
        marker in normalized_html
        for marker in (
            'property="dc:identifier"',
            'class="col-md-12 titulo"',
            "contenido-indice-titulo",
            "fecha_publicacion",
        )
    )


def _is_cas_detail_page_html(source_url: str, html: str) -> bool:
    normalized_url = (source_url or "").lower()
    normalized_html = (html or "").lower()

    if "tas-cas.org" not in normalized_url:
        return False

    return len(normalized_html) >= 1500 and any(
        marker in normalized_html
        for marker in (
            "<html",
            "court of arbitration for sport",
            "tribunal arbitral du sport",
            "cas ",
        )
    )


def _should_store_html_source(source: str, source_url: str, html: str) -> bool:
    if source not in HTML_FALLBACK_SOURCES:
        return False
    if not html or len(html.strip()) < 300:
        return False
    if source == "bjv":
        return _is_bjv_detail_page_html(source_url, html)
    if source == "cas":
        return _is_cas_detail_page_html(source_url, html)
    return False


async def _download_html_source_snapshot(
    session: aiohttp.ClientSession,
    source: str,
    source_url: Optional[str],
) -> Optional[dict]:
    normalized_url = _normalize_source_url(source, source_url)
    if not normalized_url:
        return None

    try:
        async with session.get(normalized_url, ssl=False, timeout=30) as response:
            if response.status != 200:
                return None
            html = await response.text(encoding="utf-8", errors="replace")
    except Exception:
        return None

    if not _should_store_html_source(source, normalized_url, html):
        return None

    return {
        "content": html.encode("utf-8"),
        "mime_type": "text/html",
        "source_url": normalized_url,
    }


async def _extract_bjv_pdf_url(
    session: aiohttp.ClientSession,
    detail_url: Optional[str],
) -> Optional[str]:
    """Resolve the real BJV PDF URL from a book detail page."""
    if not detail_url:
        return None

    normalized_detail_url = _normalize_absolute_url("https://biblio.juridicas.unam.mx/", detail_url)
    if not normalized_detail_url:
        return None

    try:
        from src.infrastructure.adapters.bjv_libro_parser import BJVLibroParser

        async with session.get(normalized_detail_url, ssl=False, timeout=30) as response:
            if response.status != 200:
                return None
            html = await response.text(encoding="utf-8", errors="replace")

        parser = BJVLibroParser()
        libro = parser.parse_libro_detalle(html, _extract_bjv_libro_id(normalized_detail_url))
        if libro.url_pdf and libro.url_pdf.url:
            return _normalize_absolute_url(BJVLibroParser.BASE_URL, libro.url_pdf.url)
    except Exception:
        return None

    return None


# =============================================================================
# SCJN Extraction Activity
# =============================================================================

@activity.defn
async def extract_scjn_documents(
    max_results: int = 100,
    category: Optional[str] = None,
    scope: Optional[str] = None,
    output_directory: str = "scjn_data",
    download_pdfs: bool = False,
) -> dict:
    """
    Extract SCJN documents using LLM-based parser.

    Args:
        max_results: Maximum documents to extract
        category: Filter by category (LEY, CODIGO, etc.)
        scope: Filter by scope (FEDERAL, ESTATAL, CDMX)
        output_directory: Directory to save extracted documents
        download_pdfs: Whether to download PDFs (default: False)

    Returns:
        ExtractionResult as dict
    """
    from src.infrastructure.adapters.scjn_llm_parser import SCJNLLMParser

    activity.logger.info(f"Starting SCJN extraction: max={max_results}, category={category}, scope={scope}")

    parser = SCJNLLMParser()
    all_documents = []
    errors = []
    pdfs_downloaded = 0
    page = 1
    max_pages = (max_results // 10) + 1  # Assuming ~10 docs per page

    try:
        while len(all_documents) < max_results and page <= max_pages:
            activity.logger.info(f"Fetching SCJN page {page}...")

            try:
                documents, has_next = await parser.parse_search_page(page=page)

                for doc in documents:
                    # Apply filters
                    if category and hasattr(doc, 'category') and doc.category:
                        if doc.category.value != category:
                            continue
                    if scope and hasattr(doc, 'scope') and doc.scope:
                        if doc.scope.value != scope:
                            continue

                    # Build PDF URL from q_param
                    pdf_url = build_scjn_pdf_url(doc.q_param) if doc.q_param else None

                    # Convert to unified format
                    extracted = ExtractedDocument(
                        source="scjn",
                        external_id=doc.q_param or f"scjn-{len(all_documents)}",
                        title=doc.title,
                        content_type=doc.category.value if doc.category else "unknown",
                        publication_date=doc.publication_date.isoformat() if doc.publication_date else None,
                        url=doc.source_url,
                        metadata={
                            "scope": doc.scope.value if doc.scope else None,
                            "status": doc.status.value if doc.status else None,
                            "q_param": doc.q_param,
                        },
                        pdf_url=pdf_url,
                    )
                    all_documents.append(asdict(extracted))

                    if len(all_documents) >= max_results:
                        break

                if not has_next:
                    break
                page += 1

            except Exception as e:
                errors.append(f"Page {page} error: {str(e)}")
                activity.logger.warning(f"SCJN page {page} error: {e}")
                break

        # Save documents to output directory
        output_path = Path(output_directory)
        output_path.mkdir(parents=True, exist_ok=True)

        for i, doc in enumerate(all_documents):
            # Sanitize filename - replace special characters
            safe_id = doc['external_id'].replace('/', '_').replace('=', '').replace('+', '-')
            # Truncate if too long
            if len(safe_id) > 100:
                safe_id = safe_id[:100]
            doc_file = output_path / f"scjn-{i:04d}-{safe_id[:50]}.json"
            with open(doc_file, 'w', encoding='utf-8') as f:
                json.dump(doc, f, ensure_ascii=False, indent=2)

        activity.logger.info(f"SCJN extraction complete: {len(all_documents)} documents")

        return asdict(ExtractionResult(
            source="scjn",
            success=True,
            document_count=len(all_documents),
            documents=all_documents,
            errors=errors,
            output_directory=output_directory,
            pdfs_downloaded=pdfs_downloaded,
        ))

    except Exception as e:
        activity.logger.error(f"SCJN extraction failed: {e}")
        return asdict(ExtractionResult(
            source="scjn",
            success=False,
            document_count=0,
            documents=[],
            errors=[str(e)],
            output_directory=output_directory,
            pdfs_downloaded=0,
        ))


# =============================================================================
# DOF Extraction Activity
# =============================================================================

@activity.defn
async def extract_dof_documents(
    mode: str = "today",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    output_directory: str = "dof_data",
    download_pdfs: bool = False,
) -> dict:
    """
    Extract DOF publications using HTML parser.

    Args:
        mode: 'today' or 'range'
        start_date: Start date for range mode (YYYY-MM-DD)
        end_date: End date for range mode (YYYY-MM-DD)
        output_directory: Directory to save extracted documents
        download_pdfs: Whether to download PDFs (default: False)

    Returns:
        ExtractionResult as dict
    """
    import aiohttp
    from src.infrastructure.adapters.dof_index_parser import parse_dof_index
    from src.infrastructure.adapters.dof_sumario_parser import parse_dof_sumario

    activity.logger.info(f"Starting DOF extraction: mode={mode}")

    all_documents = []
    errors = []
    pdfs_downloaded = 0

    try:
        # For today mode, try sumario.xml first (structured, includes section/agency)
        if mode == "today":
            try:
                async with aiohttp.ClientSession(headers={"User-Agent": USER_AGENT}) as session:
                    async with session.get("https://dof.gob.mx/sumario.xml", ssl=False, timeout=30) as resp:
                        if resp.status == 200:
                            xml_bytes = await resp.read()
                            items = parse_dof_sumario(xml_bytes)
                            if items:
                                activity.logger.info(f"DOF sumario.xml: {len(items)} publications (structured)")
                                for item in items:
                                    cod_diario = item.get("cod_diario")
                                    extracted = ExtractedDocument(
                                        source="dof",
                                        external_id=cod_diario,
                                        title=item.get("title", "Unknown"),
                                        content_type="publicacion",
                                        publication_date=date.today().isoformat(),
                                        url=item.get("url"),
                                        metadata={
                                            "section": item.get("section"),
                                            "cod_diario": cod_diario,
                                            "discovery_source": "sumario.xml",
                                        },
                                        pdf_url=item.get("url"),
                                    )
                                    all_documents.append(asdict(extracted))
                                # Skip the date-based HTML fallback
                                return asdict(ExtractionResult(
                                    source="dof",
                                    success=True,
                                    document_count=len(all_documents),
                                    documents=all_documents,
                                    errors=errors,
                                    output_directory=output_directory,
                                ))
            except Exception as e:
                activity.logger.warning(f"sumario.xml failed, falling back to HTML: {e}")

        # Determine dates to fetch (range mode or today fallback)
        if mode == "today":
            dates_to_fetch = [date.today()]
        elif mode == "range" and start_date and end_date:
            from datetime import timedelta
            start = datetime.strptime(start_date, "%Y-%m-%d").date()
            end = datetime.strptime(end_date, "%Y-%m-%d").date()
            dates_to_fetch = []
            current = start
            while current <= end:
                dates_to_fetch.append(current)
                current += timedelta(days=1)
        else:
            dates_to_fetch = [date.today()]

        async with aiohttp.ClientSession(headers={"User-Agent": USER_AGENT}) as session:
            for fetch_date in dates_to_fetch:
                url = f"https://dof.gob.mx/index.php?year={fetch_date.year}&month={fetch_date.month:02d}&day={fetch_date.day:02d}"
                activity.logger.info(f"Fetching DOF for {fetch_date}: {url}")

                try:
                    async with session.get(url, ssl=False, timeout=30) as response:
                        if response.status == 200:
                            html = await response.text(encoding='utf-8')
                            items = parse_dof_index(html)

                            for item in items:
                                # Extract cod_diario from URL if not present
                                detail_url = item.get('url', '')
                                cod_diario = item.get('cod_diario')
                                if not cod_diario and 'codigo=' in detail_url:
                                    match = re.search(r'codigo=(\d+)', detail_url)
                                    if match:
                                        cod_diario = match.group(1)

                                extracted = ExtractedDocument(
                                    source="dof",
                                    external_id=cod_diario or f"dof-{fetch_date}-{len(all_documents)}",
                                    title=item.get('title', 'Unknown'),
                                    content_type="publicacion",
                                    publication_date=fetch_date.isoformat(),
                                    url=detail_url,
                                    metadata={
                                        "section": item.get('section'),
                                        "cod_diario": cod_diario,
                                    },
                                    # DOF PDFs are available via detail page - URL is the same as detail
                                    pdf_url=detail_url,  # Detail page contains PDF link
                                )
                                all_documents.append(asdict(extracted))

                            activity.logger.info(f"DOF {fetch_date}: Found {len(items)} publications")
                        else:
                            errors.append(f"DOF {fetch_date}: HTTP {response.status}")

                except Exception as e:
                    errors.append(f"DOF {fetch_date} error: {str(e)}")
                    activity.logger.warning(f"DOF {fetch_date} error: {e}")

        # Save documents
        output_path = Path(output_directory)
        output_path.mkdir(parents=True, exist_ok=True)

        for doc in all_documents:
            doc_file = output_path / f"{doc['external_id']}.json"
            with open(doc_file, 'w', encoding='utf-8') as f:
                json.dump(doc, f, ensure_ascii=False, indent=2)

        activity.logger.info(f"DOF extraction complete: {len(all_documents)} documents")

        return asdict(ExtractionResult(
            source="dof",
            success=True,
            document_count=len(all_documents),
            documents=all_documents,
            errors=errors,
            output_directory=output_directory,
            pdfs_downloaded=pdfs_downloaded,
        ))

    except Exception as e:
        activity.logger.error(f"DOF extraction failed: {e}")
        return asdict(ExtractionResult(
            source="dof",
            success=False,
            document_count=0,
            documents=[],
            errors=[str(e)],
            output_directory=output_directory,
            pdfs_downloaded=0,
        ))


# =============================================================================
# BJV Extraction Activity
# =============================================================================

@activity.defn
async def extract_bjv_documents(
    max_results: int = 50,
    search_term: Optional[str] = None,
    area: Optional[str] = None,
    output_directory: str = "bjv_data",
    download_pdfs: bool = False,
) -> dict:
    """
    Extract BJV books using Crawl4AI + HTML extraction.

    Args:
        max_results: Maximum books to extract
        search_term: Search query
        area: Legal area filter
        output_directory: Directory to save extracted documents
        download_pdfs: Whether to download PDFs (default: False)

    Returns:
        ExtractionResult as dict
    """
    from src.infrastructure.adapters.bjv_llm_parser import BJVLLMParser

    activity.logger.info(f"Starting BJV extraction: max={max_results}, search={search_term}")

    parser = BJVLLMParser()
    all_documents = []
    errors = []
    pdfs_downloaded = 0

    try:
        # Use search or catalog based on parameters
        if search_term:
            results = await parser.search_multiple_pages(
                query=search_term,
                area=area,
                max_pages=5,
                max_results=max_results,
            )
        else:
            results, _ = await parser.search(query=None, area=area, page=1)

        async with aiohttp.ClientSession(headers={"User-Agent": USER_AGENT}) as session:
            for book in results[:max_results]:
                detail_url = getattr(book, "detail_url", None) or getattr(book, "url", None)
                pdf_url = getattr(book, "url_pdf", None)
                if pdf_url and hasattr(pdf_url, "url"):
                    pdf_url = pdf_url.url
                pdf_url = _normalize_absolute_url("https://biblio.juridicas.unam.mx/", pdf_url)
                if not pdf_url:
                    pdf_url = await _extract_bjv_pdf_url(session, detail_url)

                extracted = ExtractedDocument(
                    source="bjv",
                    external_id=book.libro_id.bjv_id,
                    title=book.titulo,
                    content_type=book.tipo_contenido.value if book.tipo_contenido else "libro",
                    publication_date=str(book.anio) if book.anio else None,
                    url=detail_url,
                    metadata={
                        "authors": book.autores_texto,
                        "area": book.area,
                        "bjv_id": book.libro_id.bjv_id,
                    },
                    pdf_url=pdf_url,
                )
                all_documents.append(asdict(extracted))

        # Save documents
        output_path = Path(output_directory)
        output_path.mkdir(parents=True, exist_ok=True)

        for doc in all_documents:
            doc_file = output_path / f"bjv-{doc['external_id']}.json"
            with open(doc_file, 'w', encoding='utf-8') as f:
                json.dump(doc, f, ensure_ascii=False, indent=2)

        activity.logger.info(f"BJV extraction complete: {len(all_documents)} books")

        return asdict(ExtractionResult(
            source="bjv",
            success=True,
            document_count=len(all_documents),
            documents=all_documents,
            errors=errors,
            output_directory=output_directory,
            pdfs_downloaded=pdfs_downloaded,
        ))

    except Exception as e:
        activity.logger.error(f"BJV extraction failed: {e}")
        import traceback
        traceback.print_exc()
        return asdict(ExtractionResult(
            source="bjv",
            success=False,
            document_count=0,
            documents=[],
            errors=[str(e)],
            output_directory=output_directory,
            pdfs_downloaded=0,
        ))


# =============================================================================
# CAS Extraction Activity
# =============================================================================

@activity.defn
async def extract_cas_documents(
    max_results: int = 100,
    sport: Optional[str] = None,
    year: Optional[int] = None,
    year_from: Optional[int] = None,
    year_to: Optional[int] = None,
    matter: Optional[str] = None,
    output_directory: str = "cas_data",
    download_pdfs: bool = False,
) -> dict:
    """
    Extract CAS arbitration awards using Playwright.

    Args:
        max_results: Maximum cases to extract
        sport: Sport filter (Football, Athletics, etc.)
        year: Exact year filter
        year_from: Range start year
        year_to: Range end year
        matter: Optional matter substring filter
        output_directory: Directory to save extracted documents
        download_pdfs: Whether to download PDFs (default: False)

    Returns:
        ExtractionResult as dict
    """
    from src.infrastructure.adapters.cas_llm_parser import CASLLMParser

    activity.logger.info(
        "Starting CAS extraction: max=%s, sport=%s, year=%s, year_from=%s, year_to=%s, matter=%s",
        max_results,
        sport,
        year,
        year_from,
        year_to,
        matter,
    )

    parser = CASLLMParser()
    all_documents = []
    errors = []
    pdfs_downloaded = 0

    try:
        if year is not None:
            query_years = [year]
        elif year_from is not None or year_to is not None:
            start_year = year_from if year_from is not None else year_to
            end_year = year_to if year_to is not None else year_from
            query_years = list(range(min(start_year, end_year), max(start_year, end_year) + 1))
        else:
            query_years = [None]

        seen_case_numbers = set()

        for query_year in query_years:
            results, _has_next = await parser.search(
                sport=sport,
                year=query_year,
            )

            for case in results:
                case_number = case.numero_caso.valor
                if case_number in seen_case_numbers:
                    continue
                if matter:
                    haystack = " ".join(
                        filter(None, [case.titulo, case.partes, case.resumen])
                    ).lower()
                    if matter.lower() not in haystack:
                        continue

                seen_case_numbers.add(case_number)
                if len(all_documents) >= max_results:
                    break

                # Extract PDF URL from URLLaudo
                pdf_url = build_cas_pdf_url(case.url.valor) if case.url else None

                extracted = ExtractedDocument(
                    source="cas",
                    external_id=case_number,
                    title=case.titulo,
                    content_type=case.tipo_procedimiento.value if case.tipo_procedimiento else "arbitration",
                    publication_date=case.fecha.valor.isoformat() if case.fecha else None,
                    url=case.url.valor if case.url else None,
                    metadata={
                        "case_number": case_number,
                        "sport": case.categoria_deporte.value if case.categoria_deporte else None,
                        "procedure_type": case.tipo_procedimiento.value if case.tipo_procedimiento else None,
                        "parties": case.partes,
                    },
                    pdf_url=pdf_url,  # CAS awards are typically PDF links
                )
                all_documents.append(asdict(extracted))

            if len(all_documents) >= max_results:
                break

        # Save documents
        output_path = Path(output_directory)
        output_path.mkdir(parents=True, exist_ok=True)

        for doc in all_documents:
            # Sanitize case number for filename
            safe_id = doc['external_id'].replace('/', '-').replace(' ', '_')
            doc_file = output_path / f"{safe_id}.json"
            with open(doc_file, 'w', encoding='utf-8') as f:
                json.dump(doc, f, ensure_ascii=False, indent=2)

        activity.logger.info(f"CAS extraction complete: {len(all_documents)} cases")

        return asdict(ExtractionResult(
            source="cas",
            success=True,
            document_count=len(all_documents),
            documents=all_documents,
            errors=errors,
            output_directory=output_directory,
            pdfs_downloaded=pdfs_downloaded,
        ))

    except Exception as e:
        activity.logger.error(f"CAS extraction failed: {e}")
        import traceback
        traceback.print_exc()
        return asdict(ExtractionResult(
            source="cas",
            success=False,
            document_count=0,
            documents=[],
            errors=[str(e)],
            output_directory=output_directory,
            pdfs_downloaded=0,
        ))


# =============================================================================
# Unified Extraction Activity
# =============================================================================

@activity.defn
async def extract_documents_crawl4ai(config: dict) -> dict:
    """
    Unified Crawl4AI extraction activity that dispatches to source-specific extractors.

    Args:
        config: Configuration dict with 'source' and source-specific options
            - source: Source type (scjn, dof, bjv, cas)
            - max_results: Maximum documents to extract
            - download_pdfs: Whether to download PDF/Word files
            - output_directory: Directory to save documents

    Returns:
        ExtractionResult as dict
    """
    source = config.get("source", "").lower()
    download_pdfs = config.get("download_pdfs", False)

    if source == "scjn":
        return await extract_scjn_documents(
            max_results=config.get("max_results", 100),
            category=config.get("category"),
            scope=config.get("scope"),
            output_directory=config.get("output_directory", "scjn_data"),
            download_pdfs=download_pdfs,
        )
    elif source == "dof":
        return await extract_dof_documents(
            mode=config.get("mode", "today"),
            start_date=config.get("start_date"),
            end_date=config.get("end_date"),
            output_directory=config.get("output_directory", "dof_data"),
            download_pdfs=download_pdfs,
        )
    elif source == "bjv":
        return await extract_bjv_documents(
            max_results=config.get("max_results", 50),
            search_term=config.get("search_term"),
            area=config.get("area"),
            output_directory=config.get("output_directory", "bjv_data"),
            download_pdfs=download_pdfs,
        )
    elif source == "cas":
        return await extract_cas_documents(
            max_results=config.get("max_results", 100),
            sport=config.get("sport"),
            year=config.get("year"),
            year_from=config.get("year_from"),
            year_to=config.get("year_to"),
            matter=config.get("matter"),
            output_directory=config.get("output_directory", "cas_data"),
            download_pdfs=download_pdfs,
        )
    else:
        return asdict(ExtractionResult(
            source=source,
            success=False,
            document_count=0,
            documents=[],
            errors=[f"Unknown source: {source}"],
        ))


# =============================================================================
# PDF/Document Download Activity
# =============================================================================

@activity.defn
async def download_documents_pdfs(
    documents: List[dict],
    source: str,
    output_directory: str = "downloads",
    upload_to_storage: bool = False,
) -> dict:
    """
    Download PDF/Word files for extracted documents.

    This activity downloads files from the pdf_url field of each document,
    and falls back to storing canonical HTML detail pages for sources whose
    primary content lives on the page itself.

    Args:
        documents: List of ExtractedDocument dicts with pdf_url field
        source: Source type (scjn, dof, bjv, cas)
        output_directory: Local directory to save files
        upload_to_storage: Whether to upload to Supabase Storage

    Returns:
        DownloadResult as dict
    """
    activity.logger.info(f"Starting PDF download for {len(documents)} {source} documents")

    output_path = Path(output_directory)
    output_path.mkdir(parents=True, exist_ok=True)

    downloaded = 0
    failed = 0
    skipped = 0
    errors = []
    storage_paths = []
    downloaded_documents = []

    # Initialize Supabase storage if needed
    storage_adapter = None
    if upload_to_storage:
        try:
            from supabase import create_client
            supabase_url = os.environ.get("SUPABASE_URL")
            supabase_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
            if supabase_url and supabase_key:
                client = create_client(supabase_url, supabase_key)
                from src.infrastructure.adapters.supabase_storage import SupabaseStorageAdapter
                storage_adapter = SupabaseStorageAdapter(client=client)
                activity.logger.info("Supabase storage initialized")
        except Exception as e:
            activity.logger.warning(f"Could not initialize Supabase storage: {e}")

    # Create HTTP session for downloads
    timeout = aiohttp.ClientTimeout(total=60)  # Longer timeout for PDFs
    async with aiohttp.ClientSession(
        timeout=timeout,
        headers={"User-Agent": USER_AGENT}
    ) as session:
        for doc in documents:
            pdf_url = doc.get("pdf_url")
            source_url = doc.get("url")
            external_id = doc.get("external_id", "unknown")

            if source == "bjv" and not pdf_url:
                pdf_url = await _extract_bjv_pdf_url(session, source_url)
            elif source == "cas" and not pdf_url:
                pdf_url = build_cas_pdf_url(source_url)

            html_snapshot = None

            if not pdf_url:
                html_snapshot = await _download_html_source_snapshot(session, source, source_url)
                if html_snapshot is None:
                    skipped += 1
                    continue

            try:
                # Download the file
                activity.logger.info(f"Downloading: {external_id}")

                # Handle DOF special case - use nota_to_doc.php directly
                if source == "dof":
                    cod = doc.get("external_id") or ""
                    if not cod and "codigo=" in (pdf_url or ""):
                        import re as _re
                        m = _re.search(r'codigo=(\d+)', pdf_url)
                        if m:
                            cod = m.group(1)
                    if cod:
                        pdf_url = f"https://dof.gob.mx/nota_to_doc.php?codnota={cod}"
                    else:
                        skipped += 1
                        continue

                if html_snapshot is not None:
                    content = html_snapshot["content"]
                    mime_type = html_snapshot["mime_type"]
                    extension = ".html"
                else:
                    async with session.get(pdf_url, ssl=False) as response:
                        if response.status != 200:
                            errors.append(f"{external_id}: HTTP {response.status}")
                            failed += 1
                            continue

                        content = await response.read()
                        content_type = response.headers.get("Content-Type", "")
                        response_url = str(getattr(response, "url", pdf_url) or pdf_url)

                        if _looks_like_html_document(content, content_type):
                            html_text = content.decode("utf-8", errors="replace")
                            if _should_store_html_source(source, response_url, html_text):
                                content = html_text.encode("utf-8")
                                extension = ".html"
                                mime_type = "text/html"
                            else:
                                errors.append(f"{external_id}: Received HTML instead of document")
                                failed += 1
                                continue
                        elif content[:4] == b'%PDF' or "pdf" in content_type.lower():
                            extension = ".pdf"
                            mime_type = "application/pdf"
                        elif content[:4] == b'PK\x03\x04':  # DOCX/ZIP magic bytes
                            extension = ".docx"
                            mime_type = "application/pdf"
                        elif content[:8] == b'\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1':  # DOC magic bytes
                            extension = ".doc"
                            mime_type = "application/pdf"
                        elif "word" in content_type.lower() or pdf_url.lower().endswith(".docx"):
                            extension = ".docx"
                            mime_type = "application/pdf"
                        elif pdf_url.lower().endswith(".doc"):
                            extension = ".doc"
                            mime_type = "application/pdf"
                        elif pdf_url.lower().endswith(".pdf"):
                            extension = ".pdf"
                            mime_type = "application/pdf"
                        else:
                            # Default to PDF for legal document sources
                            extension = ".pdf"
                            mime_type = "application/pdf"
                            activity.logger.warning(
                                f"{external_id}: Unrecognized file type, defaulting to PDF"
                            )

                # Sanitize filename
                safe_id = external_id.replace("/", "_").replace("=", "").replace("+", "-")
                if len(safe_id) > 100:
                    safe_id = safe_id[:100]

                # Save locally
                local_path = output_path / f"{safe_id}{extension}"
                with open(local_path, "wb") as f:
                    f.write(content)

                activity.logger.info(f"Saved: {local_path}")
                downloaded += 1

                # Upload to Supabase Storage if enabled
                storage_path = None
                if storage_adapter and upload_to_storage:
                    try:
                        storage_path = await storage_adapter.upload(
                            content=content,
                            source_type=source,
                            doc_id=safe_id,
                            content_type=mime_type,
                            file_extension=extension,
                        )
                        storage_paths.append(storage_path)
                        activity.logger.info(f"Uploaded to storage: {storage_path}")
                    except Exception as e:
                        error_message = str(e)
                        if any(
                            marker in error_message
                            for marker in ("Duplicate", "already exists", "statusCode': 409")
                        ):
                            storage_path = storage_adapter.build_path(
                                source_type=source,
                                doc_id=safe_id,
                                content_type=mime_type,
                                file_extension=extension,
                            )
                            storage_paths.append(storage_path)
                            activity.logger.info(
                                f"Reusing existing storage object: {storage_path}"
                            )
                        else:
                            errors.append(f"{external_id} storage upload: {error_message}")

                downloaded_documents.append(
                    {
                        "external_id": external_id,
                        "pdf_downloaded": True,
                        "pdf_path": str(local_path),
                        "pdf_storage_path": storage_path,
                        "content_type": mime_type,
                    }
                )

            except aiohttp.ClientError as e:
                errors.append(f"{external_id}: {str(e)}")
                failed += 1
            except Exception as e:
                errors.append(f"{external_id}: {str(e)}")
                failed += 1

    activity.logger.info(
        f"Download complete: {downloaded} downloaded, {failed} failed, {skipped} skipped"
    )

    return asdict(DownloadResult(
        source=source,
        success=failed == 0,
        total_documents=len(documents),
        downloaded_count=downloaded,
        failed_count=failed,
        skipped_count=skipped,
        errors=errors[:20],  # Limit error messages
        storage_paths=storage_paths,
        downloaded_documents=downloaded_documents,
    ))


async def _extract_dof_pdf_url(session: aiohttp.ClientSession, detail_url: str) -> Optional[str]:
    """
    Extract actual PDF URL from DOF detail page.

    DOF detail pages contain links to PDF versions. This function
    fetches the detail page and extracts the PDF download link.

    DOF uses several URL patterns for PDFs:
    - Direct PDF links ending in .pdf
    - "version_impresa" for printable versions
    - "nota_to_doc_completo.php" for full document
    - "nota_to_imagen_fs.php" for image-based viewing

    Args:
        session: aiohttp session
        detail_url: DOF detail page URL

    Returns:
        PDF URL if found, None otherwise
    """
    try:
        # DOF requires specific headers to avoid blocking
        headers = {
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "es-MX,es;q=0.9,en;q=0.8",
            "Referer": "https://dof.gob.mx/",
        }

        async with session.get(detail_url, ssl=False, timeout=30, headers=headers) as response:
            if response.status != 200:
                logger.warning(f"DOF detail page returned {response.status}: {detail_url}")
                return None

            html = await response.text(encoding='utf-8', errors='replace')

            # Look for PDF link patterns in DOF pages (ordered by reliability)
            pdf_patterns = [
                # Direct PDF links
                r'href=["\']([^"\']*\.pdf)["\']',
                # DOF document viewer
                r'href=["\']([^"\']*nota_to_doc_completo\.php[^"\']*)["\']',
                # Print version
                r'href=["\']([^"\']*version_impresa[^"\']*)["\']',
                # Image-based full screen viewer
                r'href=["\']([^"\']*nota_to_imagen_fs\.php[^"\']*)["\']',
                # Generic download links
                r'href=["\']([^"\']*descargar[^"\']*)["\']',
            ]

            for pattern in pdf_patterns:
                match = re.search(pattern, html, re.IGNORECASE)
                if match:
                    pdf_url = match.group(1)
                    # Make absolute URL if relative
                    if not pdf_url.startswith("http"):
                        if pdf_url.startswith("/"):
                            pdf_url = f"https://dof.gob.mx{pdf_url}"
                        else:
                            pdf_url = f"https://dof.gob.mx/{pdf_url}"
                    logger.debug(f"Found DOF PDF URL: {pdf_url}")
                    return pdf_url

            logger.warning(f"No PDF link found in DOF detail page: {detail_url}")

    except aiohttp.ClientError as e:
        logger.warning(f"Network error fetching DOF detail: {e}")
    except Exception as e:
        logger.warning(f"Error extracting DOF PDF URL: {e}")

    return None


# =============================================================================
# Database Persistence Activity
# =============================================================================

@activity.defn
async def persist_documents_to_supabase(
    documents: List[dict],
    source: str,
) -> dict:
    """
    Persist extracted documents to Supabase via direct upserts.

    Inserts into scraper_documents using the (source_type, external_id) unique
    constraint for idempotent re-scraping. Does NOT populate source-specific
    child tables (scjn_documents, cas_laudos, etc.).

    Args:
        documents: List of ExtractedDocument dicts
        source: Source type (scjn, dof, bjv, cas)

    Returns:
        Dict with success status, counts, and persisted document UUIDs
    """
    activity.logger.info(f"Persisting {len(documents)} {source} documents to Supabase")

    if not os.environ.get("ENABLE_SUPABASE_PERSISTENCE", "").lower() == "true":
        activity.logger.info("Supabase persistence disabled, skipping")
        return {
            "success": True,
            "persisted_count": 0,
            "skipped": True,
            "document_ids": [],
        }

    try:
        from supabase import create_client

        supabase_url = os.environ.get("SUPABASE_URL")
        supabase_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

        if not supabase_url or not supabase_key:
            activity.logger.warning("Supabase URL or key not configured")
            return {
                "success": False,
                "persisted_count": 0,
                "errors": ["Supabase not configured"],
                "document_ids": [],
            }

        client = create_client(supabase_url, supabase_key)
        persisted = 0
        skipped = 0
        errors = []
        document_ids = []

        for doc in documents:
            try:
                raw_external_id = doc.get("external_id")
                if not raw_external_id:
                    skipped += 1
                    continue

                canonical_external_id, aliases = _canonicalize_document_identity(source, doc)
                existing_row = _lookup_existing_scraper_document(
                    client,
                    source,
                    canonical_external_id,
                    aliases,
                )

                normalized_doc = {**doc, "external_id": canonical_external_id}
                row = _build_scraper_document_row(source, normalized_doc, existing_row)

                # Upsert using unique constraint (source_type, external_id)
                upsert_result = client.table("scraper_documents").upsert(
                    row, on_conflict="source_type,external_id"
                ).execute()
                document_id = None
                if getattr(upsert_result, "data", None):
                    document_id = upsert_result.data[0].get("id")
                if not document_id:
                    lookup = (
                        client.table("scraper_documents")
                        .select("id")
                        .eq("source_type", source)
                        .eq("external_id", canonical_external_id)
                        .limit(1)
                        .execute()
                    )
                    if getattr(lookup, "data", None):
                        document_id = lookup.data[0].get("id")
                if document_id:
                    document_ids.append(document_id)
                    _upsert_scraper_document_aliases(client, source, document_id, aliases)
                persisted += 1

            except Exception as e:
                errors.append(f"{doc.get('external_id', '?')}: {str(e)}")
                activity.logger.warning(f"Failed to persist {doc.get('external_id')}: {e}")

        activity.logger.info(
            f"Persisted {persisted}/{len(documents)} documents ({skipped} skipped)"
        )

        return {
            "success": len(errors) == 0,
            "persisted_count": persisted,
            "skipped_count": skipped,
            "error_count": len(errors),
            "errors": errors[:10],
            "document_ids": list(dict.fromkeys(document_ids)),
        }

    except Exception as e:
        activity.logger.error(f"Supabase persistence failed: {e}")
        return {
            "success": False,
            "persisted_count": 0,
            "error_count": len(documents),
            "errors": [str(e)],
            "document_ids": [],
        }
