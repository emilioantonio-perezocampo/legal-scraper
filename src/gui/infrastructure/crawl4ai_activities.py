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
import aiohttp
from dataclasses import dataclass, asdict, field
from datetime import date, datetime
from pathlib import Path
from typing import Any, List, Optional

from temporalio import activity

logger = logging.getLogger(__name__)

# User agent for HTTP requests
USER_AGENT = "LegalScraper/1.0 (Educational Research)"


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

    activity.logger.info(f"Starting DOF extraction: mode={mode}")

    all_documents = []
    errors = []
    pdfs_downloaded = 0

    try:
        # Determine dates to fetch
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

        for book in results[:max_results]:
            # Extract PDF URL if available
            pdf_url = None
            if hasattr(book, 'url_pdf') and book.url_pdf:
                pdf_url = book.url_pdf.url if hasattr(book.url_pdf, 'url') else str(book.url_pdf)

            extracted = ExtractedDocument(
                source="bjv",
                external_id=book.libro_id.bjv_id,
                title=book.titulo,
                content_type=book.tipo_contenido.value if book.tipo_contenido else "libro",
                publication_date=str(book.anio) if book.anio else None,
                url=book.url,
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
    output_directory: str = "cas_data",
    download_pdfs: bool = False,
) -> dict:
    """
    Extract CAS arbitration awards using Playwright.

    Args:
        max_results: Maximum cases to extract
        sport: Sport filter (Football, Athletics, etc.)
        year: Year filter
        output_directory: Directory to save extracted documents
        download_pdfs: Whether to download PDFs (default: False)

    Returns:
        ExtractionResult as dict
    """
    from src.infrastructure.adapters.cas_llm_parser import CASLLMParser

    activity.logger.info(f"Starting CAS extraction: max={max_results}, sport={sport}, year={year}")

    parser = CASLLMParser()
    all_documents = []
    errors = []
    pdfs_downloaded = 0

    try:
        results, has_next = await parser.search(
            sport=sport or "Football",
            year=year,
        )

        for case in results[:max_results]:
            # Extract PDF URL from URLLaudo
            pdf_url = case.url.valor if case.url else None

            extracted = ExtractedDocument(
                source="cas",
                external_id=case.numero_caso.valor,
                title=case.titulo,
                content_type=case.tipo_procedimiento.value if case.tipo_procedimiento else "arbitration",
                publication_date=case.fecha.valor.isoformat() if case.fecha else None,
                url=pdf_url,
                metadata={
                    "case_number": case.numero_caso.valor,
                    "sport": case.categoria_deporte.value if case.categoria_deporte else None,
                    "procedure_type": case.tipo_procedimiento.value if case.tipo_procedimiento else None,
                    "parties": case.partes,
                },
                pdf_url=pdf_url,  # CAS awards are typically PDF links
            )
            all_documents.append(asdict(extracted))

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

    This activity downloads files from the pdf_url field of each document
    and optionally uploads them to Supabase Storage.

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
            external_id = doc.get("external_id", "unknown")

            if not pdf_url:
                skipped += 1
                continue

            try:
                # Download the file
                activity.logger.info(f"Downloading: {external_id}")

                # Handle DOF special case - need to fetch detail page for actual PDF link
                if source == "dof" and "nota_detalle.php" in pdf_url:
                    pdf_url = await _extract_dof_pdf_url(session, pdf_url)
                    if not pdf_url:
                        skipped += 1
                        continue

                async with session.get(pdf_url, ssl=False) as response:
                    if response.status != 200:
                        errors.append(f"{external_id}: HTTP {response.status}")
                        failed += 1
                        continue

                    content = await response.read()
                    content_type = response.headers.get("Content-Type", "")

                    # Validate content is actually a document, not HTML error page
                    if content[:5] == b'<!DOC' or content[:5] == b'<html' or content[:5] == b'<HTML':
                        errors.append(f"{external_id}: Received HTML instead of document")
                        failed += 1
                        continue

                    # Determine file extension based on content magic bytes and headers
                    if content[:4] == b'%PDF' or "pdf" in content_type.lower():
                        extension = ".pdf"
                        mime_type = "application/pdf"
                    elif content[:4] == b'PK\x03\x04':  # DOCX/ZIP magic bytes
                        extension = ".docx"
                        mime_type = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
                    elif content[:8] == b'\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1':  # DOC magic bytes
                        extension = ".doc"
                        mime_type = "application/msword"
                    elif "word" in content_type.lower() or pdf_url.lower().endswith(".docx"):
                        extension = ".docx"
                        mime_type = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
                    elif pdf_url.lower().endswith(".doc"):
                        extension = ".doc"
                        mime_type = "application/msword"
                    elif pdf_url.lower().endswith(".pdf"):
                        extension = ".pdf"
                        mime_type = "application/pdf"
                    else:
                        # Unknown type, save as binary
                        extension = ".bin"
                        mime_type = "application/octet-stream"
                        activity.logger.warning(f"{external_id}: Unknown file type, saving as .bin")

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
                    if storage_adapter and upload_to_storage:
                        try:
                            storage_path = await storage_adapter.upload(
                                content=content,
                                source_type=source,
                                doc_id=safe_id,
                                content_type=mime_type,
                            )
                            storage_paths.append(storage_path)
                            activity.logger.info(f"Uploaded to storage: {storage_path}")
                        except Exception as e:
                            errors.append(f"{external_id} storage upload: {str(e)}")

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
    Persist extracted documents to Supabase.

    Args:
        documents: List of ExtractedDocument dicts
        source: Source type for logging

    Returns:
        Dict with success status and counts
    """
    activity.logger.info(f"Persisting {len(documents)} {source} documents to Supabase")

    # Check if Supabase is enabled
    if not os.environ.get("ENABLE_SUPABASE_PERSISTENCE", "").lower() == "true":
        activity.logger.info("Supabase persistence disabled, skipping")
        return {"success": True, "persisted_count": 0, "skipped": True}

    try:
        from src.infrastructure.adapters.supabase_repository import SupabaseDocumentRepository

        repo = SupabaseDocumentRepository()
        persisted = 0
        errors = []

        for doc in documents:
            try:
                # Convert to source-specific format and save
                await repo.save_document(
                    source_type=source,
                    external_id=doc.get("external_id"),
                    title=doc.get("title"),
                    publication_date=doc.get("publication_date"),
                    metadata=doc.get("metadata", {}),
                )
                persisted += 1
            except Exception as e:
                errors.append(f"Document {doc.get('external_id')}: {str(e)}")

        activity.logger.info(f"Persisted {persisted}/{len(documents)} documents")

        return {
            "success": len(errors) == 0,
            "persisted_count": persisted,
            "error_count": len(errors),
            "errors": errors[:10],  # Limit error messages
        }

    except Exception as e:
        activity.logger.error(f"Supabase persistence failed: {e}")
        return {
            "success": False,
            "persisted_count": 0,
            "error_count": len(documents),
            "errors": [str(e)],
        }
