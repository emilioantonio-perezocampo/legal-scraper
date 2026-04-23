"""
Scraper Document Workflow for Temporal.

This workflow handles document embedding generation after scraping completes.
It processes documents in batches, converts the stored source files with
Unstructured, generates embeddings using OpenRouter, and stores the resulting
chunks in Supabase for RAG search.

Workflow Input:
    job_id: The scraper job ID
    source_type: Source type (scjn, bjv, cas, dof)
    document_ids: Optional list of specific document IDs (empty = fetch all pending)
    batch_size: Documents per batch (default: 10)
    max_documents: Maximum documents to process (default: 100)
"""
from __future__ import annotations

import json
import logging
import os
import re
import subprocess
from html import unescape
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, List
from xml.etree import ElementTree
from zipfile import ZipFile

from temporalio import activity, workflow

logger = logging.getLogger(__name__)

EMBEDDING_MODEL = "qwen/qwen3-embedding-8b"
EMBEDDING_VECTOR_SIZE = 4000
EMBEDDING_BATCH_SIZE = 32
CHUNK_MAX_TOKENS = 900
CHUNK_MAX_CHARS = 6000
UNSTRUCTURED_SKIP_TYPES = {"Header", "Footer", "PageBreak", "Image"}
UNSTRUCTURED_HEADING_TYPES = {"Title"}
SENTENCE_BOUNDARY_RE = re.compile(r"(?<=[.!?])\s+(?=[A-Z0-9ÁÉÍÓÚÑ])")
MOUNTED_STORAGE_PREFIX = "mounted://"
SCJN_JSON_METADATA_FIELDS = (
    ("epoca", "Época"),
    ("fuente", "Fuente"),
    ("volumen", "Volumen"),
    ("instancia", "Instancia"),
    ("sala", "Sala"),
    ("ponente", "Ponente"),
    ("promovente", "Promovente"),
    ("fechaPublicacion", "Fecha publicación"),
    ("tipoAsunto", "Tipo asunto"),
)
SCRAPER_DOCUMENT_SELECT = (
    "id, title, external_id, source_type, storage_path, embedding_status, chunk_count, "
    "created_at, updated_at, processed_at, processing_started_at"
)
SOURCE_FILE_PENDING_STATUS = "pending"
FETCH_PAGE_SIZE = 200
DOCUMENT_ID_QUERY_BATCH_SIZE = 100
DEFAULT_STALE_PROCESSING_HOURS = 24.0


@dataclass
class DocumentEmbeddingInput:
    """Input for the ScraperDocumentWorkflow."""

    job_id: str
    source_type: str
    document_ids: List[str]
    batch_size: int = 10
    max_documents: int = 10000


@dataclass
class EmbeddingResult:
    """Result of embedding generation."""

    success: bool
    documents_processed: int
    embeddings_generated: int
    errors: List[str]


@lru_cache(maxsize=1)
def _get_token_encoder():
    try:
        import tiktoken

        return tiktoken.get_encoding("cl100k_base")
    except Exception:
        return None


def _count_tokens(text: str) -> int:
    if not text:
        return 0

    encoder = _get_token_encoder()
    if encoder is not None:
        try:
            return len(encoder.encode(text))
        except Exception:
            pass

    return max(1, int(len(text.split()) / 0.75))


def _split_long_text(text: str) -> list[str]:
    text = (text or "").strip()
    if not text:
        return []

    if len(text) <= CHUNK_MAX_CHARS and _count_tokens(text) <= CHUNK_MAX_TOKENS:
        return [text]

    segments = re.split(r"\n{2,}", text)
    pieces: list[str] = []
    current_parts: list[str] = []

    def _flush_current() -> None:
        if current_parts:
            pieces.append("\n\n".join(current_parts).strip())
            current_parts.clear()

    for segment in segments:
        candidate = segment.strip()
        if not candidate:
            continue

        if len(candidate) > CHUNK_MAX_CHARS or _count_tokens(candidate) > CHUNK_MAX_TOKENS:
            subsegments = [s.strip() for s in SENTENCE_BOUNDARY_RE.split(candidate) if s.strip()]
            if len(subsegments) <= 1:
                subsegments = []
                current_words: list[str] = []
                for word in candidate.split():
                    tentative = " ".join(current_words + [word]).strip()
                    if current_words and (
                        len(tentative) > CHUNK_MAX_CHARS
                        or _count_tokens(tentative) > CHUNK_MAX_TOKENS
                    ):
                        subsegments.append(" ".join(current_words).strip())
                        current_words = [word]
                    else:
                        current_words.append(word)
                if current_words:
                    subsegments.append(" ".join(current_words).strip())

            for subsegment in subsegments:
                tentative = "\n\n".join(current_parts + [subsegment]).strip()
                if current_parts and (
                    len(tentative) > CHUNK_MAX_CHARS
                    or _count_tokens(tentative) > CHUNK_MAX_TOKENS
                ):
                    _flush_current()
                current_parts.append(subsegment)
            continue

        tentative = "\n\n".join(current_parts + [candidate]).strip()
        if current_parts and (
            len(tentative) > CHUNK_MAX_CHARS or _count_tokens(tentative) > CHUNK_MAX_TOKENS
        ):
            _flush_current()
        current_parts.append(candidate)

    _flush_current()
    return [piece for piece in pieces if piece]


def _normalize_storage_path(storage_path: str, bucket_name: str) -> str:
    cleaned = (storage_path or "").strip().lstrip("/")
    bucket_prefix = f"{bucket_name}/"
    if cleaned.startswith(bucket_prefix):
        return cleaned[len(bucket_prefix) :]
    return cleaned


def _resolve_mounted_storage_path(storage_path: str) -> Path | None:
    cleaned = (storage_path or "").strip()
    if not cleaned.startswith(MOUNTED_STORAGE_PREFIX):
        return None
    relative_path = cleaned[len(MOUNTED_STORAGE_PREFIX) :].lstrip("/")
    mounted_root = Path(
        os.environ.get("SCRAPER_MOUNTED_DATA_ROOT", "/app/mounted_data")
    )
    return mounted_root / relative_path


def _coerce_chunk_count(value: Any) -> int:
    try:
        return max(int(value or 0), 0)
    except Exception:
        return 0


def _document_index_completed(doc: dict) -> bool:
    status = str(doc.get("embedding_status") or "").strip().lower()
    return status == "completed" and _coerce_chunk_count(doc.get("chunk_count")) > 0


def _document_has_source_file(doc: dict) -> bool:
    return bool(str(doc.get("storage_path") or "").strip())


def _processing_stale_after() -> timedelta:
    raw_value = os.environ.get(
        "SCRAPER_EMBEDDING_PROCESSING_STALE_HOURS",
        str(DEFAULT_STALE_PROCESSING_HOURS),
    )
    try:
        hours = float(raw_value)
    except Exception:
        hours = DEFAULT_STALE_PROCESSING_HOURS
    return timedelta(hours=max(hours, 0.0))


def _parse_document_timestamp(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _document_processing_is_stale(doc: dict, *, now: datetime | None = None) -> bool:
    status = str(doc.get("embedding_status") or "").strip().lower()
    if status != "processing":
        return False

    now = now or datetime.now(timezone.utc)
    threshold = _processing_stale_after()
    if threshold <= timedelta(0):
        return True

    last_updated = _parse_document_timestamp(doc.get("updated_at")) or _parse_document_timestamp(
        doc.get("created_at")
    )
    if last_updated is None:
        # Missing timestamps are safer to requeue than to leave stuck forever.
        return True

    return (now - last_updated) >= threshold


def _safe_update_document_state(client: Any, document_id: str, **fields: Any) -> None:
    if not document_id or not fields:
        return
    now_iso = datetime.now(timezone.utc).isoformat()
    payload = {**fields, "updated_at": now_iso}
    status = str(fields.get("embedding_status") or "").strip().lower()
    if status == "processing":
        payload.setdefault("processing_started_at", now_iso)
        payload["processed_at"] = None
    elif status == "completed":
        payload.setdefault("processed_at", now_iso)
    elif status in {"pending", "awaiting_source_file"}:
        payload["processing_started_at"] = None
        payload["processed_at"] = None
    elif status == "failed":
        payload.setdefault("processed_at", now_iso)
    try:
        client.table("scraper_documents").update(payload).eq("id", document_id).execute()
    except Exception as exc:
        activity.logger.warning(
            f"Failed to update document state for {document_id}: {exc}"
        )


def _partition_document_elements(file_path: Path) -> list[Any]:
    suffix = file_path.suffix.lower()
    if suffix == ".pdf":
        try:
            from unstructured.partition.pdf import partition_pdf

            try:
                return partition_pdf(
                    filename=str(file_path),
                    strategy="fast",
                    languages=["spa", "eng", "fra"],
                )
            except TypeError:
                return partition_pdf(filename=str(file_path), strategy="fast")
        except ModuleNotFoundError as exc:
            if "unstructured_inference" not in str(exc):
                raise

            # Fall back to text extraction when the lightweight image omits
            # the optional PDF inference dependency.
            from pypdf import PdfReader
            from unstructured.partition.text import partition_text

            reader = PdfReader(str(file_path))
            extracted_pages = []
            for page in reader.pages:
                text = (page.extract_text() or "").strip()
                if text:
                    extracted_pages.append(text)

            if not extracted_pages:
                raise

            return partition_text(text="\n\n".join(extracted_pages))
    if suffix in {".html", ".htm"}:
        from unstructured.partition.html import partition_html

        return partition_html(filename=str(file_path))
    if suffix in {".txt", ".md"}:
        from unstructured.partition.text import partition_text

        return partition_text(filename=str(file_path))
    if suffix == ".docx":
        try:
            from unstructured.partition.docx import partition_docx

            return partition_docx(filename=str(file_path))
        except ModuleNotFoundError as exc:
            if "docx" not in str(exc):
                raise

            from unstructured.partition.text import partition_text

            with ZipFile(file_path) as archive:
                document_xml = archive.read("word/document.xml")

            root = ElementTree.fromstring(document_xml)
            text_fragments = [node.text.strip() for node in root.iter() if (node.text or "").strip()]
            extracted_text = "\n".join(text_fragments).strip()
            if not extracted_text:
                raise
            return partition_text(text=extracted_text)
    if suffix == ".doc":
        try:
            from unstructured.partition.doc import partition_doc

            return partition_doc(filename=str(file_path))
        except ModuleNotFoundError as exc:
            if "docx" not in str(exc):
                raise

            from unstructured.partition.text import partition_text

            result = subprocess.run(
                ["antiword", str(file_path)],
                check=False,
                capture_output=True,
                text=True,
            )
            extracted_text = result.stdout.strip()
            if result.returncode != 0 or not extracted_text:
                raise RuntimeError(
                    f"antiword failed for {file_path.name}: {result.stderr.strip() or result.returncode}"
                )
            return partition_text(text=extracted_text)

    from unstructured.partition.auto import partition

    return partition(filename=str(file_path))


def _chunk_unstructured_elements(elements: list[Any], fallback_title: str) -> list[dict]:
    chunks: list[dict] = []
    current_parts: list[str] = []
    current_pages: list[int] = []
    current_types: list[str] = []
    current_heading_path = []
    if (fallback_title or "").strip():
        current_heading_path = [fallback_title.strip()]
    current_heading = current_heading_path[-1] if current_heading_path else None
    current_heading_level = len(current_heading_path) if current_heading_path else None
    current_chunk_heading = current_heading
    current_chunk_heading_path = list(current_heading_path)
    current_chunk_heading_level = current_heading_level

    def _flush() -> None:
        nonlocal current_parts, current_pages, current_types
        nonlocal current_chunk_heading, current_chunk_heading_path, current_chunk_heading_level
        text = "\n\n".join(part for part in current_parts if part.strip()).strip()
        if not text:
            current_parts = []
            current_pages = []
            current_types = []
            current_chunk_heading = current_heading
            current_chunk_heading_path = list(current_heading_path)
            current_chunk_heading_level = current_heading_level
            return

        pages = [page for page in current_pages if page is not None]
        chunks.append(
            {
                "text": text,
                "title": current_chunk_heading or fallback_title or text[:120],
                "metadata": {
                    "heading": current_chunk_heading,
                    "heading_path": list(current_chunk_heading_path),
                    "heading_level": current_chunk_heading_level,
                    "page_start": min(pages) if pages else None,
                    "page_end": max(pages) if pages else None,
                    "element_types": list(dict.fromkeys(current_types)),
                    "token_count": _count_tokens(text),
                },
            }
        )
        current_parts = []
        current_pages = []
        current_types = []
        current_chunk_heading = current_heading
        current_chunk_heading_path = list(current_heading_path)
        current_chunk_heading_level = current_heading_level

    for element in elements:
        element_type = type(element).__name__
        text = str(element).strip()
        if not text or element_type in UNSTRUCTURED_SKIP_TYPES:
            continue

        if element_type in UNSTRUCTURED_HEADING_TYPES:
            _flush()
            depth = getattr(getattr(element, "metadata", None), "category_depth", None) or 0
            level = min(max(int(depth) + 1, 1), 6)
            while len(current_heading_path) >= level:
                current_heading_path.pop()
            current_heading_path.append(text)
            current_heading = current_heading_path[-1]
            current_heading_level = level
            current_chunk_heading = current_heading
            current_chunk_heading_path = list(current_heading_path)
            current_chunk_heading_level = current_heading_level
            continue

        page_number = getattr(getattr(element, "metadata", None), "page_number", None)
        for piece in _split_long_text(text):
            if not current_parts:
                current_chunk_heading = current_heading
                current_chunk_heading_path = list(current_heading_path)
                current_chunk_heading_level = current_heading_level
                if current_heading:
                    current_parts.append(current_heading)
                    current_types.append("HeadingContext")
                    if page_number is not None:
                        current_pages.append(page_number)

            tentative_text = "\n\n".join(current_parts + [piece]).strip()
            if current_parts and (
                len(tentative_text) > CHUNK_MAX_CHARS
                or _count_tokens(tentative_text) > CHUNK_MAX_TOKENS
            ):
                _flush()
                current_chunk_heading = current_heading
                current_chunk_heading_path = list(current_heading_path)
                current_chunk_heading_level = current_heading_level
                if current_heading:
                    current_parts.append(current_heading)
                    current_types.append("HeadingContext")
                    if page_number is not None:
                        current_pages.append(page_number)

            current_parts.append(piece)
            current_types.append(element_type)
            if page_number is not None:
                current_pages.append(page_number)

    _flush()

    if not chunks and current_heading:
        return [
            {
                "text": current_heading,
                "title": current_heading,
                "metadata": {
                    "heading": current_heading,
                    "heading_path": list(current_heading_path),
                    "heading_level": current_heading_level,
                    "page_start": None,
                    "page_end": None,
                    "element_types": ["HeadingOnly"],
                    "token_count": _count_tokens(current_heading),
                },
            }
        ]

    return chunks


def _extract_document_chunks(file_path: Path, fallback_title: str) -> list[dict]:
    return _chunk_unstructured_elements(_partition_document_elements(file_path), fallback_title)


def _strip_html_fragment(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    try:
        from bs4 import BeautifulSoup

        cleaned = BeautifulSoup(text, "html.parser").get_text("\n")
    except Exception:
        cleaned = re.sub(r"<br\s*/?>", "\n", text, flags=re.IGNORECASE)
        cleaned = re.sub(r"</p\s*>", "\n\n", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"<[^>]+>", " ", cleaned)
    cleaned = unescape(cleaned)
    cleaned = cleaned.replace("\r\n", "\n").replace("\r", "\n")
    cleaned = re.sub(r"[ \t]+\n", "\n", cleaned)
    cleaned = re.sub(r"\n[ \t]+", "\n", cleaned)
    cleaned = re.sub(r"[ \t]{2,}", " ", cleaned)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    return cleaned.strip()


def _extract_scjn_api_json_chunks(file_path: Path, fallback_title: str) -> list[dict]:
    payload = json.loads(file_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        return []

    rubro = _strip_html_fragment(payload.get("rubro"))
    texto = _strip_html_fragment(payload.get("texto"))
    texto_publicacion = _strip_html_fragment(payload.get("textoPublicacion"))
    metadata_lines = [
        f"{label}: {cleaned}"
        for field, label in SCJN_JSON_METADATA_FIELDS
        if (cleaned := _strip_html_fragment(payload.get(field)))
    ]
    title = rubro or fallback_title
    base_context = [part for part in (title, "\n".join(metadata_lines)) if part]
    sections: list[tuple[str, str]] = []
    if rubro:
        sections.append(("Rubro", rubro))
    if texto:
        sections.append(("Texto", texto))
    if texto_publicacion and texto_publicacion != texto:
        sections.append(("Texto publicación", texto_publicacion))
    if not sections:
        return []

    chunks: list[dict] = []
    chunk_index = 0
    for heading, section_text in sections:
        parts = list(base_context)
        if rubro and heading != "Rubro":
            parts.append(f"Rubro\n{rubro}")
        parts.append(f"{heading}\n{section_text}")
        combined_text = "\n\n".join(part for part in parts if part).strip()
        for piece in _split_long_text(combined_text):
            chunk_index += 1
            chunks.append(
                {
                    "text": piece,
                    "title": title,
                    "metadata": {
                        "heading": heading,
                        "heading_path": [heading],
                        "heading_level": 1,
                        "section_types": ["api_json"],
                        "source_format": "api_json",
                        "chunking_method": "scjn_api_json_v1",
                        "chunk_index": chunk_index,
                        "token_count": _count_tokens(piece),
                    },
                }
            )
    return chunks


async def _load_document_chunks(doc: dict, storage_adapter: Any, bucket_name: str) -> list[dict]:
    storage_path = doc.get("storage_path")
    if not storage_path:
        raise ValueError(
            f"Document {doc.get('id')} has no storage_path; cannot run unstructured chunking"
        )

    source_type = doc.get("source_type")
    fallback_title = doc.get("title") or doc.get("external_id") or doc.get("id", "")

    mounted_path = _resolve_mounted_storage_path(storage_path)
    if mounted_path is not None:
        if not mounted_path.exists():
            raise FileNotFoundError(f"Mounted storage path not found: {mounted_path}")
        if doc.get("source_type") == "cas" and mounted_path.suffix.lower() == ".json":
            from src.gui.infrastructure.cas_chunking import (
                extract_cas_canonical_chunks,
                is_cas_canonical_json,
            )

            if is_cas_canonical_json(mounted_path):
                try:
                    chunks = extract_cas_canonical_chunks(
                        mounted_path,
                        doc.get("title") or doc.get("external_id") or doc.get("id", ""),
                    )
                    if chunks:
                        return chunks
                except Exception as exc:
                    logger.warning("CAS canonical chunking failed for %s: %s", mounted_path, exc)
        if source_type == "scjn" and mounted_path.suffix.lower() == ".json":
            try:
                chunks = _extract_scjn_api_json_chunks(mounted_path, fallback_title)
                if chunks:
                    return chunks
            except Exception as exc:
                logger.warning("SCJN API JSON chunking failed for %s: %s", mounted_path, exc)
        if source_type == "bjv" and mounted_path.suffix.lower() == ".pdf":
            from src.gui.infrastructure.biblio_chunking import extract_biblio_chunks

            try:
                chunks = extract_biblio_chunks(mounted_path, fallback_title)
                if chunks:
                    return chunks
            except Exception as exc:
                logger.warning("Biblio chunking failed for %s: %s", mounted_path, exc)
        return _extract_document_chunks(
            mounted_path,
            fallback_title,
        )

    normalized_path = _normalize_storage_path(storage_path, bucket_name)
    content = await storage_adapter.download(normalized_path)
    suffix = Path(normalized_path).suffix or ".pdf"

    temp_path: Path | None = None
    try:
        with NamedTemporaryFile(suffix=suffix, delete=False) as temp_file:
            temp_file.write(content)
            temp_path = Path(temp_file.name)

        if doc.get("source_type") == "cas" and temp_path.suffix.lower() == ".json":
            from src.gui.infrastructure.cas_chunking import extract_cas_canonical_chunks

            try:
                chunks = extract_cas_canonical_chunks(
                    temp_path,
                    doc.get("title") or doc.get("external_id") or doc.get("id", ""),
                )
                if chunks:
                    return chunks
            except Exception as exc:
                logger.warning("CAS canonical chunking failed for %s: %s", temp_path, exc)
        if source_type == "scjn" and temp_path.suffix.lower() == ".json":
            try:
                chunks = _extract_scjn_api_json_chunks(temp_path, fallback_title)
                if chunks:
                    return chunks
            except Exception as exc:
                logger.warning("SCJN API JSON chunking failed for %s: %s", temp_path, exc)
        if source_type == "bjv" and temp_path.suffix.lower() == ".pdf":
            from src.gui.infrastructure.biblio_chunking import extract_biblio_chunks

            try:
                chunks = extract_biblio_chunks(temp_path, fallback_title)
                if chunks:
                    return chunks
            except Exception as exc:
                logger.warning("Biblio chunking failed for %s: %s", temp_path, exc)

        return _extract_document_chunks(
            temp_path,
            fallback_title,
        )
    finally:
        if temp_path is not None:
            temp_path.unlink(missing_ok=True)


async def _embed_text_batch(session: Any, openrouter_key: str, texts: list[str]) -> list[list[float]]:
    async with session.post(
        "https://openrouter.ai/api/v1/embeddings",
        headers={
            "Authorization": f"Bearer {openrouter_key}",
            "Content-Type": "application/json",
        },
        json={"model": EMBEDDING_MODEL, "input": [text[:8000] for text in texts]},
    ) as resp:
        if resp.status != 200:
            raise RuntimeError(f"Embedding request failed with HTTP {resp.status}")
        data = await resp.json()

    return [item["embedding"][:EMBEDDING_VECTOR_SIZE] for item in data["data"]]


# =============================================================================
# Activities
# =============================================================================


@activity.defn
async def fetch_documents_for_embedding(
    source_type: str,
    document_ids: List[str],
    max_documents: int,
) -> List[dict]:
    """
    Fetch documents that need embedding generation.

    Args:
        source_type: Source type (scjn, bjv, cas, dof)
        document_ids: Specific IDs to fetch, or empty for all pending
        max_documents: Maximum number to fetch

    Returns:
        List of document dicts with the persisted file location needed for chunking
    """
    import os
    from supabase import create_client

    activity.logger.info(f"Fetching documents for embedding: source={source_type}, max={max_documents}")

    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

    if not url or not key:
        activity.logger.warning("Supabase not configured, returning empty list")
        return []

    client = create_client(url, key)

    if document_ids:
        unique_ids = list(dict.fromkeys(document_ids))
        fetched: list[dict] = []
        for start in range(0, len(unique_ids), DOCUMENT_ID_QUERY_BATCH_SIZE):
            if len(fetched) >= max_documents:
                break
            id_batch = unique_ids[start : start + DOCUMENT_ID_QUERY_BATCH_SIZE]
            result = (
                client.table("scraper_documents")
                .select(SCRAPER_DOCUMENT_SELECT)
                .in_("id", id_batch)
                .limit(max_documents - len(fetched))
                .execute()
            )
            batch_rows = result.data if result.data else []
            fetched.extend(batch_rows)

        fetched_by_id = {str(doc.get("id")): doc for doc in fetched if doc.get("id")}
        documents: list[dict] = []
        completed_count = 0
        blocked_count = 0
        stale_requeued_count = 0
        for requested_id in unique_ids:
            doc = fetched_by_id.get(str(requested_id))
            if doc is None:
                continue
            if _document_index_completed(doc):
                completed_count += 1
                continue
            if not _document_has_source_file(doc):
                blocked_count += 1
                if str(doc.get("embedding_status") or "").strip().lower() != SOURCE_FILE_PENDING_STATUS:
                    _safe_update_document_state(
                        client,
                        doc.get("id"),
                        embedding_status=SOURCE_FILE_PENDING_STATUS,
                    )
                continue
            if _document_processing_is_stale(doc):
                stale_requeued_count += 1
                _safe_update_document_state(
                    client,
                    doc.get("id"),
                    embedding_status=SOURCE_FILE_PENDING_STATUS,
                )
                doc = {**doc, "embedding_status": SOURCE_FILE_PENDING_STATUS}
            documents.append(doc)
        if completed_count:
            activity.logger.info(
                f"Skipped {completed_count} completed requested docs for {source_type}"
            )
        if blocked_count:
            activity.logger.info(
                f"Skipped {blocked_count} requested docs for {source_type} without storage_path"
            )
        if stale_requeued_count:
            activity.logger.info(
                f"Requeued {stale_requeued_count} stale requested docs for {source_type}"
            )
    else:
        documents = []
        scanned = 0
        offset = 0
        page_size = min(max(max_documents, 1), FETCH_PAGE_SIZE)
        completed_count = 0
        blocked_count = 0
        active_processing_count = 0
        stale_requeued_count = 0

        while len(documents) < max_documents:
            result = (
                client.table("scraper_documents")
                .select(SCRAPER_DOCUMENT_SELECT)
                .eq("source_type", source_type)
                .order("created_at", desc=True)
                .range(offset, offset + page_size - 1)
                .execute()
            )
            page = result.data if result.data else []
            if not page:
                break
            scanned += len(page)
            for doc in page:
                if _document_index_completed(doc):
                    completed_count += 1
                    continue
                if not _document_has_source_file(doc):
                    blocked_count += 1
                    if str(doc.get("embedding_status") or "").strip().lower() != SOURCE_FILE_PENDING_STATUS:
                        _safe_update_document_state(
                            client,
                            doc.get("id"),
                            embedding_status=SOURCE_FILE_PENDING_STATUS,
                        )
                    continue
                if str(doc.get("embedding_status") or "").strip().lower() == "processing":
                    if _document_processing_is_stale(doc):
                        stale_requeued_count += 1
                        _safe_update_document_state(
                            client,
                            doc.get("id"),
                            embedding_status=SOURCE_FILE_PENDING_STATUS,
                        )
                        doc = {**doc, "embedding_status": SOURCE_FILE_PENDING_STATUS}
                    else:
                        active_processing_count += 1
                        continue
                documents.append(doc)
            if len(page) < page_size:
                break
            offset += page_size

        documents = documents[:max_documents]
        activity.logger.info(
            f"Scanned {scanned} docs for {source_type}, "
            f"queued {len(documents)} docs needing indexing "
            f"({completed_count} completed, {blocked_count} awaiting source files, "
            f"{active_processing_count} active processing, {stale_requeued_count} stale requeued)"
        )

    activity.logger.info(f"Fetched {len(documents)} documents")
    return documents


@activity.defn
async def generate_and_store_embeddings(
    documents: List[dict],
    source_type: str,
) -> dict:
    """
    Generate embeddings and store chunk rows directly to Supabase.

    The workflow now embeds the real downloaded file contents via Unstructured
    instead of emitting a single placeholder chunk from the document title.
    """
    import os

    activity.logger.info(f"Generating+storing embeddings for {len(documents)} docs ({source_type})")

    if not documents:
        return {"stored_count": 0, "error_count": 0}

    openrouter_key = os.environ.get("OPENROUTER_API_KEY")
    sb_url = os.environ.get("SUPABASE_URL")
    sb_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
    bucket_name = os.environ.get("SCRAPER_PDF_BUCKET", "legal-scraper-pdfs")

    if not sb_url or not sb_key:
        return {"stored_count": 0, "error_count": len(documents), "error": "Supabase not configured"}
    if not openrouter_key:
        return {
            "stored_count": 0,
            "error_count": len(documents),
            "error": "OPENROUTER_API_KEY not configured",
        }

    import aiohttp
    from supabase import create_client
    from src.infrastructure.adapters.supabase_storage import SupabaseStorageAdapter

    sb_client = create_client(sb_url, sb_key)
    storage_adapter = SupabaseStorageAdapter(client=sb_client, bucket_name=bucket_name)

    stored = 0
    errors = 0
    stored_documents = 0
    error_messages: list[str] = []

    async with aiohttp.ClientSession() as session:
        for doc in documents:
            doc = {**doc, "source_type": doc.get("source_type") or source_type}
            doc_id = doc.get("id")
            try:
                _safe_update_document_state(sb_client, doc_id, embedding_status="processing")
                doc_chunks = await _load_document_chunks(doc, storage_adapter, bucket_name)
                if not doc_chunks:
                    raise ValueError(f"No unstructured chunks extracted for document {doc_id}")

                vectors: list[list[float]] = []
                for start in range(0, len(doc_chunks), EMBEDDING_BATCH_SIZE):
                    batch = doc_chunks[start : start + EMBEDDING_BATCH_SIZE]
                    vectors.extend(
                        await _embed_text_batch(
                            session,
                            openrouter_key,
                            [chunk["text"] for chunk in batch],
                        )
                    )

                if len(vectors) != len(doc_chunks):
                    raise RuntimeError(
                        f"Embedding count mismatch for {doc.get('id')}: "
                        f"{len(vectors)} vectors for {len(doc_chunks)} chunks"
                    )

                chunk_rows = []
                for index, (chunk, embedding) in enumerate(zip(doc_chunks, vectors)):
                    chunk_metadata = {
                        **chunk["metadata"],
                        "source": "scraper_document_workflow",
                        "storage_path": doc.get("storage_path"),
                    }
                    token_count = _coerce_chunk_count(
                        chunk_metadata.get("token_count")
                    ) or _count_tokens(chunk["text"])
                    chunk_rows.append(
                        {
                            "chunk_id": f"{source_type}-{doc_id}-{index}",
                            "document_id": doc_id,
                            "source_type": source_type,
                            "chunk_index": index,
                            "text": chunk["text"],
                            "titulo": chunk["title"],
                            "token_count": token_count,
                            "embedding": embedding,
                            "metadata": chunk_metadata,
                        }
                    )

                sb_client.table("scraper_chunks").delete().eq("document_id", doc_id).execute()
                sb_client.table("scraper_chunks").upsert(
                    chunk_rows, on_conflict="chunk_id"
                ).execute()
                _safe_update_document_state(
                    sb_client,
                    doc_id,
                    embedding_status="completed",
                    chunk_count=len(chunk_rows),
                )
                stored += len(chunk_rows)
                stored_documents += 1

            except Exception as e:
                _safe_update_document_state(sb_client, doc_id, embedding_status="failed")
                message = f"{doc.get('id', '?')}: {e}"
                activity.logger.warning(f"Error for {doc.get('id','?')}: {e}")
                errors += 1
                error_messages.append(message)

    activity.logger.info(
        f"Done: {stored} chunks stored across {stored_documents} docs, {errors} errors"
    )
    return {
        "stored_count": stored,
        "stored_documents": stored_documents,
        "error_count": errors,
        "errors": error_messages[:10],
    }


# =============================================================================
# Workflow
# =============================================================================


@workflow.defn
class ScraperDocumentWorkflow:
    """
    Workflow for processing scraped documents and generating embeddings.

    This workflow:
    1. Fetches documents that need embedding
    2. Generates embeddings in batches
    3. Stores embeddings in Supabase for RAG search
    """

    @workflow.run
    async def run(self, input: dict) -> dict:
        """
        Execute the document embedding workflow.

        Args:
            input: Dict with job_id, source_type, document_ids, batch_size, max_documents

        Returns:
            Dict with success status and processing stats
        """
        job_id = input.get("job_id", "unknown")
        source_type = input.get("source_type", "unknown")
        document_ids = input.get("document_ids", [])
        batch_size = input.get("batch_size", 10)
        max_documents = input.get("max_documents", 10000)

        workflow.logger.info(f"Starting ScraperDocumentWorkflow for job {job_id}")

        total_processed = 0
        total_embeddings = 0
        all_errors = []

        try:
            documents = await workflow.execute_activity(
                fetch_documents_for_embedding,
                args=[source_type, document_ids, max_documents],
                start_to_close_timeout=timedelta(minutes=5),
            )

            if not documents:
                workflow.logger.info("No documents to process")
                return {
                    "success": True,
                    "job_id": job_id,
                    "documents_processed": 0,
                    "embeddings_generated": 0,
                }

            for i in range(0, len(documents), batch_size):
                batch = documents[i : i + batch_size]
                workflow.logger.info(f"Processing batch {i // batch_size + 1}")

                result = await workflow.execute_activity(
                    generate_and_store_embeddings,
                    args=[batch, source_type],
                    start_to_close_timeout=timedelta(minutes=10),
                )

                total_embeddings += result.get("stored_count", 0)
                total_processed += result.get("stored_documents", 0)
                batch_errors = result.get("errors", []) or []
                if batch_errors:
                    all_errors.extend(batch_errors)
                elif result.get("error"):
                    all_errors.append(str(result["error"]))
                elif result.get("error_count", 0):
                    all_errors.append(
                        f"Batch {i // batch_size + 1} had {result['error_count']} document errors"
                    )

            workflow.logger.info(
                f"Workflow complete: processed={total_processed}, embeddings={total_embeddings}"
            )

            return {
                "success": len(all_errors) == 0,
                "job_id": job_id,
                "documents_processed": total_processed,
                "embeddings_generated": total_embeddings,
                "errors": all_errors[:10],
            }

        except Exception as e:
            workflow.logger.error(f"Workflow failed: {e}")
            return {
                "success": False,
                "job_id": job_id,
                "documents_processed": total_processed,
                "embeddings_generated": total_embeddings,
                "error": str(e),
            }
