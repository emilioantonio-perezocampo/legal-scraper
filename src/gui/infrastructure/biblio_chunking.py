"""
Biblio-specific chunking for the UNAM legal library corpus.

This module uses a routed extraction strategy based on benchmark results:
- text-rich / mixed PDFs: PyMuPDF4LLM -> Unstructured by-title chunking
- OCR-needed / copy-protected PDFs: PyMuPDF4LLM routed OCR -> heading chunking

The goal is to produce cleaner, less-duplicative chunks than the generic
Unstructured PDF path while remaining fully self-hostable.
"""
from __future__ import annotations

import logging
import io
import re
import os
import subprocess
import sys
import warnings
from contextlib import contextmanager, redirect_stderr, redirect_stdout
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

logger = logging.getLogger(__name__)
_NOISY_PDF_LOGGERS = (
    "RapidOCR",
    "docling.models.stages.ocr.rapid_ocr_model",
    "pdfminer",
    "pdfminer.pdfinterp",
    "pypdf",
    "pypdf._reader",
)
_UNSTRUCTURED_NOISY_PDF_LOGGERS = _NOISY_PDF_LOGGERS + ("unstructured.partition.pdf",)

BIBLIO_BALANCED_MAX_CHARS = 3000
BIBLIO_BALANCED_NEW_AFTER = 2200
BIBLIO_BALANCED_COMBINE_UNDER = 1000
BIBLIO_BALANCED_OVERLAP = 80
BIBLIO_DENSE_MAX_CHARS = 2200
BIBLIO_DENSE_NEW_AFTER = 1500
BIBLIO_DENSE_COMBINE_UNDER = 700
BIBLIO_DENSE_OVERLAP = 100
BIBLIO_TARGET_MAX_TOKENS = 650
BIBLIO_MAX_CHUNK_TOKENS = 800
BIBLIO_EXTRACTABLE_CHAR_THRESHOLD = 1200
BIBLIO_SKIP_MARKDOWN_LINE_RE = re.compile(
    r"^\*{0,2}==>\s+(?:picture|graphic|image)\b.*omitted.*<==\*{0,2}$",
    re.IGNORECASE,
)
BIBLIO_HEADING_RE = re.compile(
    r"^(?:#+\s+.+|CAP[IÍ]TULO\b|T[IÍ]TULO\b|SECCI[ÓO]N\b|LIBRO\b|PARTE\b|ART[IÍ]CULO\b)",
    re.IGNORECASE,
)
BIBLIO_NUMERIC_HEADING_RE = re.compile(r"^(?:\d+(?:\.\d+)*|[IVXLCDM]+)[.)-]?\s+\S", re.IGNORECASE)
BIBLIO_ALL_CAPS_RE = re.compile(r"^[A-ZÁÉÍÓÚÑ0-9][A-ZÁÉÍÓÚÑ0-9 ,.;:()/-]{5,}$")
TOKEN_RE = re.compile(r"[A-Za-zÀ-ÿ0-9][A-Za-zÀ-ÿ0-9'/_-]*")


@dataclass(frozen=True)
class BiblioChunkingStrategy:
    chunking_method: str
    extraction_method: str
    use_ocr: bool
    force_ocr: bool
    extractable_chars: int
    encrypted: bool
    reason: str


def decide_biblio_chunking_strategy(pdf_path: Path) -> BiblioChunkingStrategy:
    extractable_chars = _sample_extractable_chars(pdf_path)
    encrypted = _pdf_is_encrypted(pdf_path)

    if extractable_chars == 0 or encrypted:
        return BiblioChunkingStrategy(
            chunking_method="biblio_pymupdf_routed_heading_v1",
            extraction_method="pymupdf4llm_routed_ocr",
            use_ocr=True,
            force_ocr=True,
            extractable_chars=extractable_chars,
            encrypted=encrypted,
            reason="copy_protected_or_no_text_layer",
        )

    if extractable_chars < BIBLIO_EXTRACTABLE_CHAR_THRESHOLD:
        return BiblioChunkingStrategy(
            chunking_method="biblio_pymupdf_unstructured_balanced_v1",
            extraction_method="pymupdf4llm_auto_ocr",
            use_ocr=True,
            force_ocr=False,
            extractable_chars=extractable_chars,
            encrypted=encrypted,
            reason="mixed_text_density",
        )

    return BiblioChunkingStrategy(
        chunking_method="biblio_pymupdf_unstructured_balanced_v1",
        extraction_method="pymupdf4llm_text_only",
        use_ocr=False,
        force_ocr=False,
        extractable_chars=extractable_chars,
        encrypted=encrypted,
        reason="extractable_text",
    )


def extract_biblio_chunks(file_path: Path, fallback_title: str) -> list[dict[str, Any]]:
    strategy = decide_biblio_chunking_strategy(file_path)
    page_chunks = _extract_pymupdf_page_chunks(file_path, strategy)
    metadata_seed = _biblio_asset_metadata(file_path)
    metadata_seed.update(
        {
            "chunking_method": strategy.chunking_method,
            "extraction_method": strategy.extraction_method,
            "ocr_mode": "forced" if strategy.force_ocr else ("auto" if strategy.use_ocr else "disabled"),
            "extractable_chars": strategy.extractable_chars,
            "encrypted_pdf": strategy.encrypted,
            "strategy_reason": strategy.reason,
        }
    )

    if strategy.chunking_method == "biblio_pymupdf_unstructured_balanced_v1":
        try:
            chunks = _chunk_markdown_with_unstructured(page_chunks, fallback_title, metadata_seed)
            if chunks:
                return chunks
        except Exception as exc:
            logger.warning("Biblio balanced chunking failed for %s: %s", file_path, exc)

    return _chunk_markdown_with_heading(page_chunks, fallback_title, metadata_seed)


def _sample_extractable_chars(pdf_path: Path) -> int:
    result = subprocess.run(
        ["pdftotext", "-f", "1", "-l", "3", str(pdf_path), "-"],
        capture_output=True,
        check=False,
        text=True,
    )
    if result.returncode != 0:
        return 0
    return len((result.stdout or "").strip())


def _pdf_is_encrypted(pdf_path: Path) -> bool:
    result = subprocess.run(
        ["pdfinfo", str(pdf_path)],
        capture_output=True,
        check=False,
        text=True,
    )
    if result.returncode != 0:
        return False
    for line in result.stdout.splitlines():
        if line.startswith("Encrypted:"):
            return "yes" in line.lower()
    return False


def _extract_pymupdf_page_chunks(
    file_path: Path,
    strategy: BiblioChunkingStrategy,
) -> list[dict[str, Any]]:
    import pymupdf4llm  # type: ignore

    payload = _call_quietly(
        pymupdf4llm.to_markdown,
        str(file_path),
        page_chunks=True,
        header=False,
        footer=False,
        ignore_code=True,
        use_ocr=strategy.use_ocr,
        force_ocr=strategy.force_ocr,
        ocr_language="spa+eng",
    )

    if isinstance(payload, str):
        return [{"text": _clean_markdown_text(payload), "page_number": None}]

    page_chunks: list[dict[str, Any]] = []
    for item in payload if isinstance(payload, list) else []:
        if isinstance(item, dict):
            metadata = item.get("metadata") or {}
            page_number = metadata.get("page_number")
            text = _clean_markdown_text(str(item.get("text") or ""))
        else:
            page_number = None
            text = _clean_markdown_text(str(item))
        if text:
            page_chunks.append({"text": text, "page_number": page_number})
    return page_chunks


def _call_quietly(func, /, *args, **kwargs):
    stdout_buffer = io.StringIO()
    stderr_buffer = io.StringIO()
    stdout_fd = os.dup(1)
    stderr_fd = os.dup(2)
    try:
        sys.stdout.flush()
        sys.stderr.flush()
        with _suppress_noisy_pdf_logs(include_unstructured=True):
            with open(os.devnull, "w", encoding="utf-8") as devnull:
                os.dup2(devnull.fileno(), 1)
                os.dup2(devnull.fileno(), 2)
                with redirect_stdout(stdout_buffer), redirect_stderr(stderr_buffer):
                    return func(*args, **kwargs)
    finally:
        os.dup2(stdout_fd, 1)
        os.dup2(stderr_fd, 2)
        os.close(stdout_fd)
        os.close(stderr_fd)


@contextmanager
def _suppress_noisy_pdf_logs(include_unstructured: bool = False):
    previous: list[tuple[logging.Logger, int]] = []
    logger_names = (
        _UNSTRUCTURED_NOISY_PDF_LOGGERS if include_unstructured else _NOISY_PDF_LOGGERS
    )
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message=r".*should not allow text extraction.*",
        )
        try:
            for name in logger_names:
                noisy_logger = logging.getLogger(name)
                previous.append((noisy_logger, noisy_logger.level))
                noisy_logger.setLevel(logging.ERROR)
            yield
        finally:
            for noisy_logger, level in previous:
                noisy_logger.setLevel(level)


def _clean_markdown_text(markdown: str) -> str:
    cleaned_lines = [
        line.rstrip()
        for line in (markdown or "").splitlines()
        if not BIBLIO_SKIP_MARKDOWN_LINE_RE.match(line.strip())
    ]
    return "\n".join(cleaned_lines).strip()


def _biblio_asset_metadata(file_path: Path) -> dict[str, Any]:
    family_id = None
    asset_role = "document"
    chapter_number = None

    parts = list(file_path.parts)
    if "books" in parts:
        books_index = parts.index("books")
        if len(parts) > books_index + 1:
            family_id = parts[books_index + 1]
        if "chapters" in parts:
            asset_role = "chapter"
            chapter_number = _chapter_number_from_name(file_path.stem)
        elif file_path.name.lower() == "book.pdf":
            asset_role = "book"

    return {
        "family_id": family_id,
        "asset_role": asset_role,
        "chapter_number": chapter_number,
    }


def _chapter_number_from_name(stem: str) -> str | None:
    match = re.match(r"(\d+[a-z]?)", stem, re.IGNORECASE)
    if match:
        return match.group(1)
    match = re.search(r"(\d+[a-z]?)_(\d+)$", stem, re.IGNORECASE)
    if match:
        return match.group(1)
    return None


def _chunk_markdown_with_unstructured(
    page_chunks: list[dict[str, Any]],
    fallback_title: str,
    metadata_seed: dict[str, Any],
) -> list[dict[str, Any]]:
    from unstructured.chunking.title import chunk_by_title
    from unstructured.partition.md import partition_md

    markdown = "\n\n".join(chunk["text"] for chunk in page_chunks if chunk.get("text")).strip()
    if not markdown:
        return []

    with _suppress_noisy_pdf_logs(include_unstructured=True):
        elements = partition_md(text=markdown)
        composites = chunk_by_title(
            elements,
            max_characters=BIBLIO_BALANCED_MAX_CHARS,
            new_after_n_chars=BIBLIO_BALANCED_NEW_AFTER,
            combine_text_under_n_chars=BIBLIO_BALANCED_COMBINE_UNDER,
            overlap=BIBLIO_BALANCED_OVERLAP,
            overlap_all=False,
            multipage_sections=True,
            include_orig_elements=True,
        )

    chunks: list[dict[str, Any]] = []
    for element in composites:
        text = str(getattr(element, "text", "") or "").strip()
        if not text:
            continue
        title = _best_chunk_title(text, fallback_title)
        chunks.append(
            {
                "text": text,
                "title": title,
                "metadata": {
                    **metadata_seed,
                    "heading": title,
                    "heading_path": [fallback_title, title] if fallback_title and title != fallback_title else [title],
                    "heading_level": 2 if fallback_title and title != fallback_title else 1,
                    "page_start": None,
                    "page_end": None,
                    "element_types": ["MarkdownComposite"],
                    "token_count": _count_tokens(text),
                },
            }
        )

    return _deduplicate_chunks(chunks)


def _chunk_markdown_with_heading(
    page_chunks: list[dict[str, Any]],
    fallback_title: str,
    metadata_seed: dict[str, Any],
) -> list[dict[str, Any]]:
    sections = _split_biblio_sections(page_chunks, fallback_title)
    chunks: list[dict[str, Any]] = []
    for section in sections:
        text = str(section.get("text") or "").strip()
        if not text:
            continue
        label = str(section.get("label") or fallback_title or "Documento").strip()
        pages = [page for page in section.get("pages", []) if page is not None]
        for piece in _split_with_overlap(text):
            chunks.append(
                {
                    "text": piece,
                    "title": label,
                    "metadata": {
                        **metadata_seed,
                        "heading": label,
                        "heading_path": list(section.get("heading_path") or [label]),
                        "heading_level": len(section.get("heading_path") or [label]),
                        "page_start": min(pages) if pages else None,
                        "page_end": max(pages) if pages else None,
                        "element_types": ["NarrativeText"],
                        "token_count": _count_tokens(piece),
                    },
                }
            )

    return _deduplicate_chunks(chunks)


def _split_biblio_sections(
    page_chunks: list[dict[str, Any]],
    fallback_title: str,
) -> list[dict[str, Any]]:
    sections: list[dict[str, Any]] = []
    current_heading = fallback_title
    current_heading_path = [fallback_title] if fallback_title else []
    current_lines: list[str] = []
    current_pages: list[int] = []

    def _flush() -> None:
        nonlocal current_lines, current_pages
        payload = "\n".join(line for line in current_lines if line.strip()).strip()
        if payload:
            sections.append(
                {
                    "label": current_heading or fallback_title or "Documento",
                    "text": payload,
                    "heading_path": list(current_heading_path) or [fallback_title or "Documento"],
                    "pages": list(current_pages),
                }
            )
        current_lines = []
        current_pages = []

    for page in page_chunks:
        page_number = page.get("page_number")
        for raw_line in str(page.get("text") or "").splitlines():
            line = raw_line.strip()
            if not line:
                if current_lines:
                    current_lines.append("")
                continue
            if _looks_like_heading(line):
                _flush()
                heading = _clean_heading(line)
                if heading:
                    current_heading = heading
                    current_heading_path = [fallback_title, heading] if fallback_title else [heading]
                continue
            current_lines.append(line)
            if page_number is not None:
                current_pages.append(page_number)

    _flush()

    if not sections:
        joined = "\n\n".join(page["text"] for page in page_chunks if page.get("text")).strip()
        if not joined:
            return []
        return [
            {
                "label": fallback_title or "Documento",
                "text": joined,
                "heading_path": [fallback_title or "Documento"],
                "pages": [page.get("page_number") for page in page_chunks if page.get("page_number") is not None],
            }
        ]

    return sections


def _looks_like_heading(line: str) -> bool:
    stripped = line.strip()
    if not stripped:
        return False
    if BIBLIO_HEADING_RE.match(stripped):
        return True
    if BIBLIO_ALL_CAPS_RE.match(stripped) and len(stripped.split()) <= 16:
        return True
    if BIBLIO_NUMERIC_HEADING_RE.match(stripped) and len(stripped.split()) <= 18:
        return True
    return False


def _clean_heading(line: str) -> str:
    heading = re.sub(r"^#+\s*", "", line.strip())
    return re.sub(r"\s+", " ", heading).strip(":-").strip()


def _split_with_overlap(text: str) -> list[str]:
    text = text.strip()
    if not text:
        return []
    if _count_tokens(text) <= BIBLIO_MAX_CHUNK_TOKENS:
        return [text]

    paragraphs = [part.strip() for part in re.split(r"\n{2,}", text) if part.strip()]
    chunks: list[str] = []
    current_parts: list[str] = []

    def _flush() -> None:
        if current_parts:
            chunks.append("\n\n".join(current_parts).strip())

    for paragraph in paragraphs or [text]:
        tentative = "\n\n".join(current_parts + [paragraph]).strip()
        if current_parts and _count_tokens(tentative) > BIBLIO_MAX_CHUNK_TOKENS:
            _flush()
            overlap = _tail_for_overlap(chunks[-1], BIBLIO_BALANCED_OVERLAP) if chunks else None
            current_parts = [overlap, paragraph] if overlap else [paragraph]
        else:
            current_parts.append(paragraph)

    _flush()
    return [chunk for chunk in chunks if chunk]


def _tail_for_overlap(text: str, target_tokens: int) -> str | None:
    tokens = TOKEN_RE.findall(text or "")
    if len(tokens) <= target_tokens:
        return text.strip() or None
    return " ".join(tokens[-target_tokens:]).strip() or None


def _deduplicate_chunks(chunks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduplicated: list[dict[str, Any]] = []
    seen: set[str] = set()
    for chunk in chunks:
        text = str(chunk.get("text") or "").strip()
        normalized = re.sub(r"\s+", " ", text.lower())
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        deduplicated.append(chunk)
    return deduplicated


def _best_chunk_title(text: str, fallback_title: str) -> str:
    for line in text.splitlines():
        stripped = line.strip().lstrip("#").strip()
        if stripped:
            return stripped[:180]
    return fallback_title or text[:180]


def _count_tokens(text: str) -> int:
    try:
        import tiktoken

        encoder = tiktoken.get_encoding("cl100k_base")
        return len(encoder.encode(text or ""))
    except Exception:
        return max(1, int(len((text or "").split()) / 0.75))
