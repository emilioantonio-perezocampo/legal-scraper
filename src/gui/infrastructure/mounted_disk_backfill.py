"""
Mounted-disk backfill for scraper source files and RAG indexing.

This module connects the existing corpus stored under the mounted data volume
to the same durable Supabase Storage + Temporal embedding path used by the
live scraper pipelines.
"""
from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import re
import sqlite3
import time
import unicodedata
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

from src.gui.infrastructure.temporal_client import TemporalConfig, TemporalScraperClient
from src.infrastructure.adapters.supabase_storage import SupabaseStorageAdapter

logger = logging.getLogger(__name__)

SUPPORTED_BACKFILL_SOURCES = ("bjv", "cas", "dof", "orden", "scjn")
BACKFILL_SELECT = (
    "id, external_id, title, publication_date, source_type, storage_path, "
    "embedding_status, chunk_count, created_at, updated_at"
)
BOOTSTRAP_IDENTITY_SELECT = (
    "id, external_id, title, publication_date, storage_path, embedding_status, chunk_count"
)
FETCH_PAGE_SIZE = 200
DEFAULT_DATA_ROOT = "/app/mounted_data"
TERMINAL_EMBEDDING_STATUS = "completed"
AWAITING_SOURCE_STATUS = "awaiting_source_file"
SOURCE_FILE_PENDING_STATUS = "pending"
MOUNTED_STORAGE_PREFIX = "mounted://"
SCRAPER_DOCUMENT_ALIASES_TABLE = "scraper_document_aliases"
SCJN_MOUNTED_FAMILIES = ("tesis", "ejecutorias", "votos", "acuerdos", "otros")
SCJN_BOOTSTRAP_FAMILY_ORDER = ("ejecutorias", "votos", "acuerdos", "otros", "tesis")


@dataclass
class LocalSourceMatch:
    path: Path
    content_type: str
    detail: str


@dataclass(frozen=True)
class DocumentAlias:
    alias_type: str
    alias_value: str


@dataclass(frozen=True)
class MountedCorpusRecord:
    external_id: str
    title: str
    publication_date: Optional[str] = None
    aliases: tuple[DocumentAlias, ...] = ()


@dataclass
class MountedBackfillResult:
    source: str
    scanned: int = 0
    eligible: int = 0
    already_completed: int = 0
    missing_local_file: int = 0
    uploaded: int = 0
    reused_storage_object: int = 0
    queued_existing_storage: int = 0
    queued_for_embedding: int = 0
    updated_rows: int = 0
    registered_rows: int = 0
    existing_registered_rows: int = 0
    alias_rows_upserted: int = 0
    trigger_job_id: Optional[str] = None
    trigger_run_id: Optional[str] = None
    workflow_result: Optional[dict] = None
    errors: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "scanned": self.scanned,
            "eligible": self.eligible,
            "already_completed": self.already_completed,
            "missing_local_file": self.missing_local_file,
            "uploaded": self.uploaded,
            "reused_storage_object": self.reused_storage_object,
            "queued_existing_storage": self.queued_existing_storage,
            "queued_for_embedding": self.queued_for_embedding,
            "updated_rows": self.updated_rows,
            "registered_rows": self.registered_rows,
            "existing_registered_rows": self.existing_registered_rows,
            "alias_rows_upserted": self.alias_rows_upserted,
            "trigger_job_id": self.trigger_job_id,
            "trigger_run_id": self.trigger_run_id,
            "workflow_result": self.workflow_result,
            "errors": list(self.errors),
        }


@dataclass
class MountedBackfillDrainResult:
    source: str
    passes: int = 0
    hydrated_documents: int = 0
    remaining_missing_storage: int = 0
    registered_documents: int = 0
    completed: bool = False
    stalled: bool = False
    pass_results: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "passes": self.passes,
            "hydrated_documents": self.hydrated_documents,
            "remaining_missing_storage": self.remaining_missing_storage,
            "registered_documents": self.registered_documents,
            "completed": self.completed,
            "stalled": self.stalled,
            "pass_results": list(self.pass_results),
        }


def _coerce_chunk_count(value: Any) -> int:
    try:
        return max(int(value or 0), 0)
    except Exception:
        return 0


def _document_index_completed(doc: dict[str, Any]) -> bool:
    status = str(doc.get("embedding_status") or "").strip().lower()
    return status == TERMINAL_EMBEDDING_STATUS and _coerce_chunk_count(doc.get("chunk_count")) > 0


def _mounted_data_root(data_root: Optional[str | Path] = None) -> Path:
    configured = data_root or os.environ.get("SCRAPER_MOUNTED_DATA_ROOT") or DEFAULT_DATA_ROOT
    return Path(configured)


def _content_type_for_path(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix == ".pdf":
        return "application/pdf"
    if suffix == ".json":
        return "application/json"
    if suffix == ".docx":
        return "application/pdf"
    if suffix == ".doc":
        return "application/pdf"
    if suffix in {".html", ".htm"}:
        return "text/html"
    if suffix == ".md":
        return "text/markdown"
    return "text/plain"


def _normalize_match_text(value: Optional[str]) -> str:
    normalized = unicodedata.normalize("NFKD", str(value or ""))
    normalized = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    normalized = normalized.lower().replace("_", " ").replace("-", " ")
    normalized = re.sub(r"[^a-z0-9]+", " ", normalized)
    return re.sub(r"\s+", " ", normalized).strip()


def _normalize_alias_value(value: Optional[str]) -> Optional[str]:
    alias = str(value or "").strip()
    return alias or None


def _iter_unique_aliases(
    external_id: str,
    aliases: Iterable[DocumentAlias],
) -> Iterable[DocumentAlias]:
    seen: set[str] = set()
    canonical = _normalize_alias_value(external_id)
    if canonical:
        seen.add(canonical)
    for alias in aliases:
        value = _normalize_alias_value(alias.alias_value)
        if not value or value in seen:
            continue
        seen.add(value)
        yield DocumentAlias(alias.alias_type, value)


def _preferred_local_match(existing: Optional[LocalSourceMatch], candidate: LocalSourceMatch) -> LocalSourceMatch:
    if existing is None:
        return candidate
    priority = {".json": 0, ".txt": 1, ".md": 2, ".html": 3, ".docx": 4, ".doc": 5, ".pdf": 6}
    existing_rank = priority.get(existing.path.suffix.lower(), 99)
    candidate_rank = priority.get(candidate.path.suffix.lower(), 99)
    if candidate_rank < existing_rank:
        return candidate
    return existing


def _book_chapter_external_id(book_id: str, chapter_id: str) -> str:
    return f"book-{book_id}-ch{chapter_id}"


def _scjn_family_external_id(family: str, doc_id: str) -> str:
    return f"{family}-{doc_id}"


def _record_title(source: str, external_id: str) -> str:
    if source == "bjv":
        if external_id.startswith("book-") and "-ch" in external_id:
            return external_id.replace("book-", "Book ").replace("-ch", " Chapter ")
        return f"Book {external_id}"
    if source == "cas":
        return f"CAS {external_id}"
    if source == "dof":
        return f"DOF {external_id}"
    if source == "orden":
        return external_id
    if source == "scjn":
        family, _, doc_id = external_id.partition("-")
        return f"{family.title()} {doc_id}".strip()
    return external_id


def _extract_dof_legacy_slug(external_id: str) -> str:
    normalized = str(external_id or "").strip()
    if normalized.startswith("dof-"):
        return normalized[4:]
    return ""


def _standard_upload_max_bytes() -> int:
    raw_value = os.environ.get("SCRAPER_STANDARD_UPLOAD_MAX_BYTES", "").strip()
    if raw_value:
        try:
            return max(int(raw_value), 0)
        except Exception:
            logger.warning("Invalid SCRAPER_STANDARD_UPLOAD_MAX_BYTES=%s", raw_value)
    return 6 * 1024 * 1024


def _prefer_mounted_path_for_large_files(file_size_bytes: int) -> bool:
    enabled = os.environ.get("SCRAPER_DIRECT_MOUNTED_FALLBACK_FOR_LARGE_FILES", "true").lower()
    if enabled in {"0", "false", "no"}:
        return False
    threshold = _standard_upload_max_bytes()
    return threshold > 0 and file_size_bytes > threshold


def _prefer_direct_mounted_backfill_for_source(source: str) -> bool:
    raw_value = os.environ.get(
        "SCRAPER_DIRECT_MOUNTED_BACKFILL_SOURCES",
        "bjv,orden",
    ).strip()
    if not raw_value:
        return False
    lowered = {part.strip().lower() for part in raw_value.split(",") if part.strip()}
    return "*" in lowered or source.strip().lower() in lowered


def _extract_last_numeric_token(value: str) -> Optional[str]:
    match = re.search(r"(\d+)(?!.*\d)", value or "")
    return match.group(1) if match else None


def _append_unique_case_key(candidates: list[str], seen: set[str], raw_value: str) -> None:
    normalized = re.sub(r"\s+", "", (raw_value or "").upper())
    if not normalized:
        return
    if normalized.isdigit():
        normalized = str(int(normalized))
    if normalized not in seen:
        seen.add(normalized)
        candidates.append(normalized)


def _extract_cas_case_keys(external_id: str, title: Optional[str] = None) -> list[str]:
    candidates: list[str] = []
    seen: set[str] = set()

    for raw_value in (title or "", external_id or ""):
        normalized = " ".join(raw_value.upper().split())
        if not normalized:
            continue

        for match in re.finditer(r"(?:CAS|TAS)\s+([A-Z]\d(?:\s*&\s*[A-Z]\d)+)/(\d{2,4})", normalized):
            year = match.group(2)
            for part in re.split(r"\s*&\s*", match.group(1)):
                _append_unique_case_key(candidates, seen, f"{part}-{year}")

        for code, year in re.findall(r"(?:CAS|TAS)\s+([A-Z]\d)/(\d{2,4})", normalized):
            _append_unique_case_key(candidates, seen, f"{code}-{year}")

        for procedure, case_number in re.findall(
            r"(?:CAS|TAS)\s+\d{2,4}/([A-Z]{1,3})/(\d{1,5})",
            normalized,
        ):
            number = str(int(case_number))
            if procedure in {"O", "IA"}:
                _append_unique_case_key(candidates, seen, f"{number}-{procedure}")
            _append_unique_case_key(candidates, seen, number)

        for _games_code, case_number in re.findall(
            r"(?:CAS|TAS)\s+(OG|JO)\s+\d{2}/(\d{1,5})",
            normalized,
        ):
            _append_unique_case_key(candidates, seen, case_number)

        last_numeric = _extract_last_numeric_token(normalized)
        if last_numeric:
            _append_unique_case_key(candidates, seen, last_numeric)

    return candidates


def _resolve_bjv_file(data_root: Path, external_id: str) -> Optional[LocalSourceMatch]:
    book_dir = data_root / "books" / external_id
    whole_book_pdf = book_dir / "book.pdf"
    if whole_book_pdf.exists():
        return LocalSourceMatch(
            path=whole_book_pdf,
            content_type="application/pdf",
            detail="books/<id>/book.pdf",
        )

    chapter_match = re.fullmatch(r"book-(\d+)-ch([0-9a-zA-Z]+)", external_id or "")
    if chapter_match:
        book_id, chapter_id = chapter_match.groups()
        chapters_dir = data_root / "books" / book_id / "chapters"
        chapter_candidates = (
            chapters_dir / f"{chapter_id}_{book_id}.pdf",
            chapters_dir / f"{book_id}_{chapter_id}.pdf",
        )
        for candidate in chapter_candidates:
            if candidate.exists():
                return LocalSourceMatch(
                    path=candidate,
                    content_type="application/pdf",
                    detail="books/<id>/chapters/<chapter>.pdf",
                )
    return None


def _resolve_cas_file(
    data_root: Path,
    external_id: str,
    title: Optional[str] = None,
) -> Optional[LocalSourceMatch]:
    cases_dir = data_root / "cas_pdfs"
    converted_dir = data_root / "cas" / "converted"

    for case_key in _extract_cas_case_keys(external_id, title):
        for converted_name in ("canonical.json", "output.md", "output.txt"):
            converted_candidate = converted_dir / case_key / converted_name
            if converted_candidate.exists():
                return LocalSourceMatch(
                    path=converted_candidate,
                    content_type=_content_type_for_path(converted_candidate),
                    detail=f"cas/converted/{case_key}/{converted_name}",
                )

        pdf_candidates: list[Path] = [cases_dir / f"{case_key}.pdf"]
        if case_key.endswith("-O"):
            base_key = case_key[:-2]
            pdf_candidates.extend(sorted(cases_dir.glob(f"{base_key}-O*.pdf")))
            pdf_candidates.append(cases_dir / f"{base_key}.pdf")
        elif case_key.endswith("-IA"):
            base_key = case_key[:-3]
            pdf_candidates.extend(sorted(cases_dir.glob(f"{base_key}-IA*.pdf")))
            pdf_candidates.append(cases_dir / f"{base_key}.pdf")

        seen_candidates: set[Path] = set()
        for candidate in pdf_candidates:
            if candidate in seen_candidates:
                continue
            seen_candidates.add(candidate)
            if candidate.exists():
                return LocalSourceMatch(
                    path=candidate,
                    content_type="application/pdf",
                    detail="cas_pdfs/<award>.pdf",
                )
    return None


def _resolve_dof_file_by_external_id(data_root: Path, external_id: str) -> Optional[LocalSourceMatch]:
    dof_root = data_root / "dof"
    converted_txt = dof_root / "converted" / external_id / "output.txt"
    if converted_txt.exists():
        return LocalSourceMatch(
            path=converted_txt,
            content_type="text/plain",
            detail="dof/converted/<id>/output.txt",
        )

    for suffix in (".docx", ".doc", ".pdf", ".html", ".txt"):
        candidate = dof_root / f"{external_id}{suffix}"
        if candidate.exists():
            return LocalSourceMatch(
                path=candidate,
                content_type=_content_type_for_path(candidate),
                detail=f"dof/<id>{suffix}",
            )

    return None


def _iter_dof_progress_candidates(
    data_root: Path,
    publication_date: Optional[str],
) -> list[dict[str, str]]:
    db_path = data_root / "dof_progress.db"
    if not db_path.exists():
        return []

    conn = sqlite3.connect(str(db_path), timeout=10)
    conn.row_factory = sqlite3.Row
    try:
        if publication_date:
            rows = conn.execute(
                "SELECT external_id, title, publication_date "
                "FROM documents WHERE publication_date = ?",
                (publication_date,),
            ).fetchall()
            if rows:
                return [dict(row) for row in rows]

            year = publication_date[:4]
            if year.isdigit():
                rows = conn.execute(
                    "SELECT external_id, title, publication_date "
                    "FROM documents WHERE substr(publication_date, 1, 4) = ?",
                    (year,),
                ).fetchall()
                if rows:
                    return [dict(row) for row in rows]
        return []
    finally:
        conn.close()


def _score_dof_candidate(
    candidate_title: Optional[str],
    *,
    target_title_norm: str,
    legacy_slug_norm: str,
) -> int:
    candidate_norm = _normalize_match_text(candidate_title)
    if not candidate_norm:
        return -1

    scores: list[int] = []
    if target_title_norm and candidate_norm == target_title_norm:
        scores.append(100)
    if legacy_slug_norm and candidate_norm == legacy_slug_norm:
        scores.append(99)

    if target_title_norm and len(target_title_norm) >= 12:
        if candidate_norm.startswith(target_title_norm) or target_title_norm.startswith(candidate_norm):
            scores.append(95)
        elif target_title_norm in candidate_norm or candidate_norm in target_title_norm:
            scores.append(90)

    if legacy_slug_norm and len(legacy_slug_norm) >= 12:
        if candidate_norm.startswith(legacy_slug_norm) or legacy_slug_norm.startswith(candidate_norm):
            scores.append(94)
        elif legacy_slug_norm in candidate_norm or candidate_norm in legacy_slug_norm:
            scores.append(89)

    return max(scores) if scores else -1


def _resolve_dof_legacy_external_id(
    data_root: Path,
    external_id: str,
    *,
    title: Optional[str] = None,
    publication_date: Optional[str] = None,
) -> Optional[str]:
    legacy_slug_norm = _normalize_match_text(_extract_dof_legacy_slug(external_id))
    target_title_norm = _normalize_match_text(title)
    if not legacy_slug_norm and not target_title_norm:
        return None

    best_external_id: Optional[str] = None
    best_score = -1
    duplicate_best = False

    for row in _iter_dof_progress_candidates(data_root, publication_date):
        candidate_external_id = str(row.get("external_id") or "").strip()
        if not candidate_external_id:
            continue

        score = _score_dof_candidate(
            row.get("title"),
            target_title_norm=target_title_norm,
            legacy_slug_norm=legacy_slug_norm,
        )
        if score < 0:
            continue
        if score > best_score:
            best_score = score
            best_external_id = candidate_external_id
            duplicate_best = False
        elif score == best_score:
            duplicate_best = True

    if duplicate_best or best_score < 0:
        return None
    return best_external_id


def _resolve_dof_file(
    data_root: Path,
    external_id: str,
    *,
    title: Optional[str] = None,
    publication_date: Optional[str] = None,
) -> Optional[LocalSourceMatch]:
    direct_match = _resolve_dof_file_by_external_id(data_root, external_id)
    if direct_match is not None:
        return direct_match

    legacy_external_id = _resolve_dof_legacy_external_id(
        data_root,
        external_id,
        title=title,
        publication_date=publication_date,
    )
    if legacy_external_id:
        return _resolve_dof_file_by_external_id(data_root, legacy_external_id)
    return None


def _iter_orden_candidates(data_root: Path, external_id: str) -> Iterable[Path]:
    converted_candidates = (
        data_root / "federal" / "converted" / external_id / "output.txt",
        data_root / "federal" / "converted" / external_id / "output.md",
    )
    for candidate in converted_candidates:
        if candidate.exists():
            yield candidate

    glob_patterns = (
        ("federal", "*/pdf", ".pdf"),
        ("federal", "*/doc", ".doc"),
        ("federal", "*/doc", ".docx"),
        ("federal", "*/html", ".html"),
        ("estatal", "*/pdf", ".pdf"),
        ("estatal", "*/doc", ".doc"),
        ("estatal", "*/doc", ".docx"),
        ("estatal", "*/html", ".html"),
        ("internacional", "pdf", ".pdf"),
        ("internacional", "doc", ".doc"),
        ("internacional", "doc", ".docx"),
        ("internacional", "html", ".html"),
    )
    for root_name, subdir_pattern, suffix in glob_patterns:
        yield from sorted((data_root / root_name).glob(f"{subdir_pattern}/{external_id}{suffix}"))


def _resolve_orden_file(data_root: Path, external_id: str) -> Optional[LocalSourceMatch]:
    for candidate in _iter_orden_candidates(data_root, external_id):
        return LocalSourceMatch(
            path=candidate,
            content_type=_content_type_for_path(candidate),
            detail="orden mounted-data match",
        )
    return None


def _resolve_scjn_file(data_root: Path, external_id: str) -> Optional[LocalSourceMatch]:
    normalized = (external_id or "").strip()
    for family in SCJN_MOUNTED_FAMILIES:
        prefix = f"{family}-"
        if not normalized.startswith(prefix):
            continue
        doc_id = normalized.split("-", 1)[1]
        for converted_name in ("output.txt", "output.md"):
            converted_candidate = data_root / family / "converted" / doc_id / converted_name
            if converted_candidate.exists():
                return LocalSourceMatch(
                    path=converted_candidate,
                    content_type=_content_type_for_path(converted_candidate),
                    detail=f"{family}/converted/<id>/{converted_name}",
                )
        api_json_candidate = data_root / family / "api_json" / f"{doc_id}.json"
        if api_json_candidate.exists():
            return LocalSourceMatch(
                path=api_json_candidate,
                content_type="application/json",
                detail=f"{family}/api_json/<id>.json",
            )
        pdf_candidate = data_root / family / "pdf" / f"{doc_id}.pdf"
        if pdf_candidate.exists():
            return LocalSourceMatch(
                path=pdf_candidate,
                content_type="application/pdf",
                detail=f"{family}/pdf/<id>.pdf",
            )
    return None


def resolve_local_source_file(
    source: str,
    external_id: str,
    *,
    title: Optional[str] = None,
    publication_date: Optional[str] = None,
    data_root: Optional[str | Path] = None,
) -> Optional[LocalSourceMatch]:
    root = _mounted_data_root(data_root)
    if source == "cas":
        return _resolve_cas_file(root, external_id, title=title)
    if source == "dof":
        return _resolve_dof_file(
            root,
            external_id,
            title=title,
            publication_date=publication_date,
        )

    resolvers = {
        "bjv": _resolve_bjv_file,
        "orden": _resolve_orden_file,
        "scjn": _resolve_scjn_file,
    }
    resolver = resolvers.get(source)
    if resolver is None:
        raise ValueError(f"Unsupported mounted-disk source: {source}")
    return resolver(root, external_id)


def _build_mounted_record(
    source: str,
    external_id: str,
    *,
    aliases: Iterable[DocumentAlias] = (),
    title: Optional[str] = None,
    publication_date: Optional[str] = None,
) -> MountedCorpusRecord:
    return MountedCorpusRecord(
        external_id=external_id,
        title=title or _record_title(source, external_id),
        publication_date=publication_date,
        aliases=tuple(_iter_unique_aliases(external_id, aliases)),
    )


def _iter_bjv_mounted_records(root: Path) -> Iterable[MountedCorpusRecord]:
    books_root = root / "books"
    if not books_root.exists():
        return

    for book_dir in sorted(books_root.iterdir()):
        if not book_dir.is_dir():
            continue
        book_id = book_dir.name.strip()
        if not book_id:
            continue
        if (book_dir / "book.pdf").exists():
            yield _build_mounted_record(
                "bjv",
                book_id,
                aliases=(DocumentAlias("book_id", f"book:{book_id}"),),
                title=f"Book {book_id}",
            )

        chapters_dir = book_dir / "chapters"
        if not chapters_dir.exists():
            continue
        for candidate in sorted(chapters_dir.glob("*.pdf")):
            stem = candidate.stem
            chapter_id = None
            match = re.fullmatch(rf"(.+)_{re.escape(book_id)}", stem)
            if match:
                chapter_id = match.group(1)
            else:
                match = re.fullmatch(rf"{re.escape(book_id)}_(.+)", stem)
                if match:
                    chapter_id = match.group(1)
            if not chapter_id:
                continue
            external_id = _book_chapter_external_id(book_id, chapter_id)
            yield _build_mounted_record(
                "bjv",
                external_id,
                aliases=(DocumentAlias("mounted_file", stem),),
                title=f"Book {book_id} Chapter {chapter_id}",
            )


def _iter_cas_mounted_records(root: Path) -> Iterable[MountedCorpusRecord]:
    converted_root = root / "cas" / "converted"
    if converted_root.exists():
        for case_dir in sorted(converted_root.iterdir()):
            if not case_dir.is_dir():
                continue
            case_key = case_dir.name.strip()
            if not case_key:
                continue
            yield _build_mounted_record("cas", case_key, title=f"CAS {case_key}")

    cases_root = root / "cas_pdfs"
    if not cases_root.exists():
        return
    for candidate in sorted(cases_root.glob("*.pdf")):
        case_key = candidate.stem.strip()
        if not case_key:
            continue
        yield _build_mounted_record(
            "cas",
            case_key,
            aliases=(DocumentAlias("mounted_file", candidate.stem),),
            title=f"CAS {case_key}",
        )


def _iter_dof_mounted_records(root: Path) -> Iterable[MountedCorpusRecord]:
    dof_root = root / "dof"
    converted_root = dof_root / "converted"
    if converted_root.exists():
        for doc_dir in sorted(converted_root.iterdir()):
            if doc_dir.is_dir() and doc_dir.name.strip():
                yield _build_mounted_record("dof", doc_dir.name.strip(), title=f"DOF {doc_dir.name.strip()}")

    if not dof_root.exists():
        return
    for candidate in sorted(dof_root.glob("*")):
        if candidate.is_dir():
            continue
        if candidate.suffix.lower() not in {".docx", ".doc", ".pdf", ".html", ".txt"}:
            continue
        external_id = candidate.stem.strip()
        if not external_id:
            continue
        yield _build_mounted_record(
            "dof",
            external_id,
            aliases=(DocumentAlias("mounted_file", candidate.stem),),
            title=f"DOF {external_id}",
        )


def _iter_orden_mounted_records(root: Path) -> Iterable[MountedCorpusRecord]:
    converted_root = root / "federal" / "converted"
    if converted_root.exists():
        for doc_dir in sorted(converted_root.iterdir()):
            if doc_dir.is_dir() and doc_dir.name.strip():
                yield _build_mounted_record("orden", doc_dir.name.strip(), title=doc_dir.name.strip())

    glob_patterns = (
        "federal/*/pdf/*.*",
        "federal/*/doc/*.*",
        "federal/*/html/*.*",
        "estatal/*/pdf/*.*",
        "estatal/*/doc/*.*",
        "estatal/*/html/*.*",
        "internacional/pdf/*.*",
        "internacional/doc/*.*",
        "internacional/html/*.*",
    )
    for pattern in glob_patterns:
        for candidate in sorted(root.glob(pattern)):
            if candidate.suffix.lower() not in {".docx", ".doc", ".pdf", ".html", ".txt"}:
                continue
            external_id = candidate.stem.strip()
            if not external_id:
                continue
            yield _build_mounted_record(
                "orden",
                external_id,
                aliases=(DocumentAlias("mounted_file", candidate.stem),),
                title=external_id,
            )


def _iter_scjn_mounted_records(root: Path) -> Iterable[MountedCorpusRecord]:
    for family in SCJN_BOOTSTRAP_FAMILY_ORDER:
        converted_root = root / family / "converted"
        if converted_root.exists():
            for doc_dir in sorted(converted_root.iterdir()):
                if not doc_dir.is_dir() or not doc_dir.name.strip():
                    continue
                external_id = _scjn_family_external_id(family, doc_dir.name.strip())
                yield _build_mounted_record("scjn", external_id, title=f"{family.title()} {doc_dir.name.strip()}")

        api_json_root = root / family / "api_json"
        if api_json_root.exists():
            for candidate in sorted(api_json_root.glob("*.json")):
                doc_id = candidate.stem.strip()
                if not doc_id:
                    continue
                external_id = _scjn_family_external_id(family, doc_id)
                yield _build_mounted_record(
                    "scjn",
                    external_id,
                    aliases=(DocumentAlias("mounted_file", candidate.stem),),
                    title=f"{family.title()} {doc_id}",
                )

        pdf_root = root / family / "pdf"
        if not pdf_root.exists():
            continue
        for candidate in sorted(pdf_root.glob("*.pdf")):
            doc_id = candidate.stem.strip()
            if not doc_id:
                continue
            external_id = _scjn_family_external_id(family, doc_id)
            yield _build_mounted_record(
                "scjn",
                external_id,
                aliases=(DocumentAlias("mounted_file", candidate.stem),),
                title=f"{family.title()} {doc_id}",
            )


def _iter_mounted_corpus_records(source: str, root: Path) -> Iterable[MountedCorpusRecord]:
    iterators = {
        "bjv": _iter_bjv_mounted_records,
        "cas": _iter_cas_mounted_records,
        "dof": _iter_dof_mounted_records,
        "orden": _iter_orden_mounted_records,
        "scjn": _iter_scjn_mounted_records,
    }
    iterator = iterators.get(source)
    if iterator is None:
        raise ValueError(f"Unsupported source: {source}")

    seen: set[str] = set()
    for record in iterator(root):
        if record.external_id in seen:
            continue
        seen.add(record.external_id)
        yield record


def _build_source_documents_query(
    client: Any,
    source: str,
    *,
    has_storage: Optional[bool] = None,
    select_columns: str = BACKFILL_SELECT,
    count: Optional[str] = None,
    head: bool = False,
) -> Any:
    query = client.table("scraper_documents").select(
        select_columns,
        count=count,
        head=head,
    ).eq("source_type", source)
    if has_storage is True:
        query = query.not_.is_("storage_path", "null")
    elif has_storage is False:
        query = query.is_("storage_path", "null")
    return query


def _iter_source_documents(
    client: Any,
    source: str,
    *,
    has_storage: Optional[bool] = None,
    select_columns: str = BACKFILL_SELECT,
) -> Iterable[dict[str, Any]]:
    offset = 0
    while True:
        response = (
            _build_source_documents_query(
                client,
                source,
                has_storage=has_storage,
                select_columns=select_columns,
            )
            .order("created_at", desc=True)
            .range(offset, offset + FETCH_PAGE_SIZE - 1)
            .execute()
        )
        rows = getattr(response, "data", None) or []
        if not rows:
            break
        for row in rows:
            yield row
        if len(rows) < FETCH_PAGE_SIZE:
            break
        offset += FETCH_PAGE_SIZE


def _lookup_document_by_external_id(
    client: Any,
    source: str,
    external_id: str,
    *,
    select_columns: str = BACKFILL_SELECT,
) -> Optional[dict[str, Any]]:
    if not external_id:
        return None
    response = (
        client.table("scraper_documents")
        .select(select_columns)
        .eq("source_type", source)
        .eq("external_id", external_id)
        .limit(1)
        .execute()
    )
    rows = getattr(response, "data", None) or []
    return rows[0] if rows else None


def _lookup_document_by_alias(
    client: Any,
    source: str,
    aliases: Iterable[DocumentAlias],
    *,
    select_columns: str = BACKFILL_SELECT,
) -> Optional[dict[str, Any]]:
    for alias in aliases:
        try:
            response = (
                client.table(SCRAPER_DOCUMENT_ALIASES_TABLE)
                .select("document_id")
                .eq("source_type", source)
                .eq("alias_value", alias.alias_value)
                .limit(1)
                .execute()
            )
        except Exception:
            return None
        rows = getattr(response, "data", None) or []
        if not rows:
            continue
        document_id = rows[0].get("document_id")
        if not document_id:
            continue
        response = (
            client.table("scraper_documents")
            .select(select_columns)
            .eq("id", document_id)
            .limit(1)
            .execute()
        )
        doc_rows = getattr(response, "data", None) or []
        if doc_rows:
            return doc_rows[0]
    return None


def _find_document_by_identity(
    client: Any,
    source: str,
    external_id: str,
    aliases: Iterable[DocumentAlias],
    *,
    select_columns: str = BACKFILL_SELECT,
) -> Optional[dict[str, Any]]:
    document = _lookup_document_by_external_id(
        client,
        source,
        external_id,
        select_columns=select_columns,
    )
    if document:
        return document
    for alias in _iter_unique_aliases(external_id, aliases):
        document = _lookup_document_by_external_id(
            client,
            source,
            alias.alias_value,
            select_columns=select_columns,
        )
        if document:
            return document
    return _lookup_document_by_alias(
        client,
        source,
        _iter_unique_aliases(external_id, aliases),
        select_columns=select_columns,
    )


def _iter_source_alias_rows(
    client: Any,
    source: str,
) -> Iterable[dict[str, Any]]:
    offset = 0
    while True:
        response = (
            client.table(SCRAPER_DOCUMENT_ALIASES_TABLE)
            .select("document_id,alias_value")
            .eq("source_type", source)
            .range(offset, offset + FETCH_PAGE_SIZE - 1)
            .execute()
        )
        rows = getattr(response, "data", None) or []
        if not rows:
            break
        for row in rows:
            yield row
        if len(rows) < FETCH_PAGE_SIZE:
            break
        offset += FETCH_PAGE_SIZE


def _load_source_identity_index(
    client: Any,
    source: str,
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    documents_by_external_id: dict[str, dict[str, Any]] = {}
    documents_by_id: dict[str, dict[str, Any]] = {}
    documents_by_alias: dict[str, dict[str, Any]] = {}

    for row in _iter_source_documents(
        client,
        source,
        select_columns=BOOTSTRAP_IDENTITY_SELECT,
    ):
        document_id = str(row.get("id") or "").strip()
        external_id = str(row.get("external_id") or "").strip()
        if document_id:
            documents_by_id[document_id] = row
        if not external_id:
            continue
        documents_by_external_id[external_id] = row

    for alias_row in _iter_source_alias_rows(client, source):
        alias_value = str(alias_row.get("alias_value") or "").strip()
        document_id = str(alias_row.get("document_id") or "").strip()
        if not alias_value or not document_id:
            continue
        document = documents_by_id.get(document_id)
        if document is not None:
            documents_by_alias[alias_value] = document

    return documents_by_external_id, documents_by_alias


def _find_document_in_identity_index(
    documents_by_external_id: dict[str, dict[str, Any]],
    documents_by_alias: dict[str, dict[str, Any]],
    external_id: str,
    aliases: Iterable[DocumentAlias],
) -> Optional[dict[str, Any]]:
    document = documents_by_external_id.get(external_id)
    if document is not None:
        return document
    for alias in _iter_unique_aliases(external_id, aliases):
        document = documents_by_external_id.get(alias.alias_value)
        if document is not None:
            return document
    for alias in _iter_unique_aliases(external_id, aliases):
        document = documents_by_alias.get(alias.alias_value)
        if document is not None:
            return document
    return None


def _build_registered_document_row(
    source: str,
    record: MountedCorpusRecord,
    existing_row: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    row = {
        "source_type": source,
        "external_id": record.external_id,
        "title": (record.title or _record_title(source, record.external_id))[:500],
        "publication_date": record.publication_date,
        "storage_path": (existing_row or {}).get("storage_path"),
    }
    existing_status = str((existing_row or {}).get("embedding_status") or "").strip().lower()
    existing_chunk_count = _coerce_chunk_count((existing_row or {}).get("chunk_count"))
    if existing_row is None:
        row["embedding_status"] = SOURCE_FILE_PENDING_STATUS
    elif row["storage_path"] and existing_chunk_count == 0 and existing_status in {
        "",
        "pending",
        "awaiting_source_file",
        "failed",
    }:
        row["embedding_status"] = SOURCE_FILE_PENDING_STATUS
    elif not row["storage_path"] and existing_status in {"", "pending", "awaiting_source_file"}:
        row["embedding_status"] = SOURCE_FILE_PENDING_STATUS
    return row


def _lookup_document_id(client: Any, source: str, external_id: str) -> Optional[str]:
    response = (
        client.table("scraper_documents")
        .select("id")
        .eq("source_type", source)
        .eq("external_id", external_id)
        .limit(1)
        .execute()
    )
    rows = getattr(response, "data", None) or []
    if rows:
        return rows[0].get("id")
    return None


def _upsert_document_aliases(
    client: Any,
    source: str,
    document_id: str,
    external_id: str,
    aliases: Iterable[DocumentAlias],
) -> int:
    rows = [
        {
            "document_id": document_id,
            "source_type": source,
            "alias_type": alias.alias_type,
            "alias_value": alias.alias_value,
        }
        for alias in _iter_unique_aliases(external_id, aliases)
    ]
    if not rows:
        return 0
    try:
        client.table(SCRAPER_DOCUMENT_ALIASES_TABLE).upsert(
            rows,
            on_conflict="source_type,alias_value",
        ).execute()
    except Exception:
        return 0
    return len(rows)


def _filter_missing_aliases(
    documents_by_external_id: dict[str, dict[str, Any]],
    documents_by_alias: dict[str, dict[str, Any]],
    external_id: str,
    aliases: Iterable[DocumentAlias],
) -> list[DocumentAlias]:
    missing: list[DocumentAlias] = []
    seen: set[str] = set()
    for alias in _iter_unique_aliases(external_id, aliases):
        if alias.alias_value in seen:
            continue
        seen.add(alias.alias_value)
        if alias.alias_value in documents_by_external_id or alias.alias_value in documents_by_alias:
            continue
        missing.append(alias)
    return missing


def _build_registration_upsert_row(
    source: str,
    record: MountedCorpusRecord,
    existing_row: Optional[dict[str, Any]],
) -> Optional[dict[str, Any]]:
    desired = _build_registered_document_row(source, record, existing_row)
    if existing_row is None:
        return desired

    update_fields: dict[str, Any] = {
        "source_type": source,
        "external_id": record.external_id,
    }

    if not str(existing_row.get("title") or "").strip() and desired.get("title"):
        update_fields["title"] = desired["title"]

    if not str(existing_row.get("publication_date") or "").strip() and desired.get("publication_date"):
        update_fields["publication_date"] = desired["publication_date"]

    desired_storage = desired.get("storage_path")
    if desired_storage and desired_storage != existing_row.get("storage_path"):
        update_fields["storage_path"] = desired_storage

    desired_status = desired.get("embedding_status")
    if desired_status and desired_status != existing_row.get("embedding_status"):
        update_fields["embedding_status"] = desired_status

    if len(update_fields) == 2:
        return None
    return update_fields


def bootstrap_source_documents_from_mounted_disk(
    source: str,
    *,
    data_root: Optional[str | Path] = None,
    supabase_client: Any | None = None,
    limit: Optional[int] = None,
) -> dict[str, Any]:
    if source not in SUPPORTED_BACKFILL_SOURCES:
        raise ValueError(f"Unsupported source: {source}")

    root = _mounted_data_root(data_root)
    if not root.exists():
        raise FileNotFoundError(f"Mounted data root not found: {root}")

    client = supabase_client or _get_supabase_client_from_env()
    documents_by_external_id, documents_by_alias = _load_source_identity_index(client, source)
    result = {
        "source": source,
        "scanned_records": 0,
        "registered_rows": 0,
        "existing_registered_rows": 0,
        "alias_rows_upserted": 0,
        "errors": [],
    }

    for record in _iter_mounted_corpus_records(source, root):
        if limit is not None and result["registered_rows"] >= limit:
            break
        result["scanned_records"] += 1
        try:
            existing_row = _find_document_in_identity_index(
                documents_by_external_id,
                documents_by_alias,
                record.external_id,
                record.aliases,
            )
            row = _build_registration_upsert_row(source, record, existing_row)
            document_id = str((existing_row or {}).get("id") or "").strip() or None

            if row is not None:
                response = client.table("scraper_documents").upsert(
                    row,
                    on_conflict="source_type,external_id",
                ).execute()
                rows = getattr(response, "data", None) or []
                if rows:
                    document_id = rows[0].get("id")
                if not document_id:
                    document_id = _lookup_document_id(client, source, record.external_id)

            alias_rows = _filter_missing_aliases(
                documents_by_external_id,
                documents_by_alias,
                record.external_id,
                record.aliases,
            )
            if document_id and alias_rows:
                result["alias_rows_upserted"] += _upsert_document_aliases(
                    client,
                    source,
                    document_id,
                    record.external_id,
                    alias_rows,
                )

            if document_id:
                refreshed_row = dict(existing_row or {})
                if row is not None:
                    refreshed_row.update(row)
                refreshed_row["id"] = document_id
                documents_by_external_id[record.external_id] = refreshed_row
                for alias in _iter_unique_aliases(record.external_id, record.aliases):
                    documents_by_alias[alias.alias_value] = refreshed_row
            if existing_row is None:
                result["registered_rows"] += 1
            else:
                result["existing_registered_rows"] += 1
        except Exception as exc:
            result["errors"].append(f"{record.external_id}: {exc}")

    return result


def _document_has_storage_path(doc: dict[str, Any]) -> bool:
    return bool(str(doc.get("storage_path") or "").strip())


def _storage_suffix(storage_path: str) -> str:
    return Path(str(storage_path or "")).suffix.lower()


def _duplicate_storage_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return "already exists" in message or "duplicate" in message


def _storage_payload_too_large_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return "payload too large" in message or "statuscode': 413" in message


def _storage_timeout_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return any(
        token in message
        for token in (
            "timed out",
            "timeout",
            "read timeout",
            "write timeout",
        )
    )


def _awaiting_source_retry_seconds() -> int:
    raw_value = os.environ.get("SCRAPER_AWAITING_SOURCE_RETRY_SECONDS", "60").strip()
    if not raw_value:
        return 60
    try:
        return max(int(raw_value), 0)
    except Exception:
        logger.warning("Invalid SCRAPER_AWAITING_SOURCE_RETRY_SECONDS=%s", raw_value)
        return 60


def _parse_iso_datetime(value: Any) -> Optional[datetime]:
    if value in (None, "", "None", "null"):
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except Exception:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _should_retry_awaiting_source(doc: dict[str, Any]) -> bool:
    status = str(doc.get("embedding_status") or "").strip().lower()
    if status != AWAITING_SOURCE_STATUS:
        return True
    retry_after_seconds = _awaiting_source_retry_seconds()
    if retry_after_seconds <= 0:
        return True
    last_touched_at = _parse_iso_datetime(doc.get("updated_at") or doc.get("created_at"))
    if last_touched_at is None:
        return True
    age_seconds = (datetime.now(timezone.utc) - last_touched_at).total_seconds()
    return age_seconds >= retry_after_seconds


def _sha256_for_path(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        while True:
            chunk = fh.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _build_mounted_storage_path(root: Path, local_match: LocalSourceMatch) -> str:
    relative_path = local_match.path.relative_to(root).as_posix()
    return f"{MOUNTED_STORAGE_PREFIX}{relative_path}"


@dataclass
class PersistedMountedFile:
    storage_path: str
    reused_existing: bool
    file_size_bytes: int
    content_hash: str


async def _persist_local_match(
    storage_adapter: SupabaseStorageAdapter,
    source: str,
    external_id: str,
    local_match: LocalSourceMatch,
    *,
    root: Path,
) -> PersistedMountedFile:
    file_size_bytes = local_match.path.stat().st_size
    if _prefer_direct_mounted_backfill_for_source(source):
        return PersistedMountedFile(
            storage_path=_build_mounted_storage_path(root, local_match),
            reused_existing=True,
            file_size_bytes=file_size_bytes,
            content_hash=_sha256_for_path(local_match.path),
        )

    if local_match.path.suffix.lower() == ".json":
        return PersistedMountedFile(
            storage_path=_build_mounted_storage_path(root, local_match),
            reused_existing=True,
            file_size_bytes=file_size_bytes,
            content_hash=_sha256_for_path(local_match.path),
        )

    if _prefer_mounted_path_for_large_files(file_size_bytes):
        return PersistedMountedFile(
            storage_path=_build_mounted_storage_path(root, local_match),
            reused_existing=True,
            file_size_bytes=file_size_bytes,
            content_hash=_sha256_for_path(local_match.path),
        )

    try:
        storage_path, reused_existing, content = await _upload_or_reuse_local_file(
            storage_adapter,
            source,
            external_id,
            local_match,
        )
        return PersistedMountedFile(
            storage_path=storage_path,
            reused_existing=reused_existing,
            file_size_bytes=len(content),
            content_hash=hashlib.sha256(content).hexdigest(),
        )
    except Exception as exc:
        if not (_storage_payload_too_large_error(exc) or _storage_timeout_error(exc)):
            raise
        return PersistedMountedFile(
            storage_path=_build_mounted_storage_path(root, local_match),
            reused_existing=True,
            file_size_bytes=file_size_bytes,
            content_hash=_sha256_for_path(local_match.path),
        )


async def _upload_or_reuse_local_file(
    storage_adapter: SupabaseStorageAdapter,
    source: str,
    external_id: str,
    local_match: LocalSourceMatch,
) -> tuple[str, bool, bytes]:
    content = local_match.path.read_bytes()
    try:
        storage_path = await storage_adapter.upload(
            content=content,
            source_type=source,
            doc_id=external_id,
            content_type=local_match.content_type,
            file_extension=local_match.path.suffix.lower(),
        )
        return storage_path, False, content
    except Exception as exc:
        if not _duplicate_storage_error(exc):
            raise
        storage_path = storage_adapter.build_path(
            source_type=source,
            doc_id=external_id,
            content_type=local_match.content_type,
            file_extension=local_match.path.suffix.lower(),
        )
        return storage_path, True, content


async def _storage_path_exists(storage_adapter: SupabaseStorageAdapter, storage_path: str) -> bool:
    try:
        return await storage_adapter.exists(storage_path)
    except Exception as exc:
        logger.warning("Failed to check storage path %s: %s", storage_path, exc)
        return False


def _update_document_row(client: Any, document_id: str, fields: dict[str, Any]) -> None:
    if not document_id or not fields:
        return
    payload = {**fields, "updated_at": datetime.now(timezone.utc).isoformat()}
    status = str(fields.get("embedding_status") or "").strip().lower()
    if status in {"pending", AWAITING_SOURCE_STATUS}:
        payload["processing_started_at"] = None
        payload["processed_at"] = None
    client.table("scraper_documents").update(payload).eq("id", document_id).execute()


def _count_documents_missing_storage(client: Any, source: str) -> int:
    try:
        response = _build_source_documents_query(
            client,
            source,
            has_storage=False,
            select_columns="id",
            count="exact",
            head=True,
        ).execute()
        exact_count = getattr(response, "count", None)
        if isinstance(exact_count, int):
            return exact_count
    except Exception as exc:
        logger.warning(
            "Falling back to client-side missing-storage count for %s: %s",
            source,
            exc,
        )
    return sum(1 for _ in _iter_source_documents(client, source, has_storage=False))


def _pending_embedding_document_ids(
    client: Any,
    source: str,
    *,
    limit: int,
) -> list[str]:
    if limit <= 0:
        return []
    try:
        response = (
            _build_source_documents_query(
                client,
                source,
                has_storage=True,
                select_columns="id, embedding_status, chunk_count",
            )
            .eq("embedding_status", "pending")
            .order("updated_at", desc=False)
            .limit(limit)
            .execute()
        )
    except Exception as exc:
        logger.warning("Failed to fetch pending embedding docs for %s: %s", source, exc)
        return []

    rows = getattr(response, "data", None) or []
    document_ids: list[str] = []
    for row in rows:
        document_id = str(row.get("id") or "").strip()
        if not document_id:
            continue
        if _document_index_completed(row):
            continue
        document_ids.append(document_id)
    return list(dict.fromkeys(document_ids))


async def _wait_for_workflow_result(job_id: str) -> Optional[dict[str, Any]]:
    config = TemporalConfig.from_env()
    if not config.enabled or not config.address:
        return None

    from temporalio.client import Client

    client = await Client.connect(config.address, namespace=config.namespace)
    workflow_id = f"{TemporalScraperClient.WORKFLOW_ID_PREFIX}{job_id}"
    handle = client.get_workflow_handle(workflow_id)
    return await handle.result()


def _get_supabase_client_from_env() -> Any:
    from supabase import create_client

    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        raise RuntimeError("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be configured")
    return create_client(url, key)


def _get_storage_adapter_from_env(client: Any) -> SupabaseStorageAdapter:
    bucket_name = os.environ.get("SCRAPER_PDF_BUCKET", SupabaseStorageAdapter.DEFAULT_BUCKET)
    return SupabaseStorageAdapter(client=client, bucket_name=bucket_name)


async def backfill_source_from_mounted_disk(
    source: str,
    *,
    limit: int = 100,
    include_existing_storage: bool = True,
    bootstrap_missing_documents: bool = True,
    trigger_embedding: bool = True,
    wait_for_workflow: bool = False,
    data_root: Optional[str | Path] = None,
    supabase_client: Any | None = None,
    storage_adapter: SupabaseStorageAdapter | None = None,
    temporal_client: TemporalScraperClient | Any | None = None,
) -> MountedBackfillResult:
    if source not in SUPPORTED_BACKFILL_SOURCES:
        raise ValueError(f"Unsupported source: {source}")

    if limit <= 0:
        raise ValueError("limit must be greater than zero")

    root = _mounted_data_root(data_root)
    if not root.exists():
        raise FileNotFoundError(f"Mounted data root not found: {root}")

    client = supabase_client or _get_supabase_client_from_env()
    storage = storage_adapter or _get_storage_adapter_from_env(client)
    temporal = temporal_client or TemporalScraperClient()

    result = MountedBackfillResult(source=source)
    embedding_document_ids: list[str] = []
    stage_filters = [True, False] if include_existing_storage else [False]
    seen_document_ids: set[str] = set()

    for require_storage in stage_filters:
        for doc in _iter_source_documents(client, source, has_storage=require_storage):
            document_id = doc.get("id")
            if not document_id or document_id in seen_document_ids:
                continue

            seen_document_ids.add(document_id)
            result.scanned += 1
            if _document_index_completed(doc):
                result.already_completed += 1
                continue

            storage_path = str(doc.get("storage_path") or "").strip()
            current_status = str(doc.get("embedding_status") or "").strip().lower()

            try:
                if storage_path:
                    if result.eligible >= limit:
                        break
                    result.eligible += 1
                    local_match = resolve_local_source_file(
                        source,
                        str(doc.get("external_id") or ""),
                        title=str(doc.get("title") or ""),
                        publication_date=str(doc.get("publication_date") or ""),
                        data_root=root,
                    )
                    if local_match is not None and _storage_suffix(storage_path) != local_match.path.suffix.lower():
                        persisted = await _persist_local_match(
                            storage,
                            source,
                            str(doc.get("external_id") or ""),
                            local_match,
                            root=root,
                        )
                        update_fields = {
                            "storage_path": persisted.storage_path,
                            "file_size_bytes": persisted.file_size_bytes,
                            "content_hash": persisted.content_hash,
                            "embedding_status": "pending",
                        }
                        _update_document_row(client, document_id, update_fields)
                        result.updated_rows += 1
                        if persisted.reused_existing:
                            result.reused_storage_object += 1
                        else:
                            result.uploaded += 1
                        embedding_document_ids.append(document_id)
                        continue

                    if await _storage_path_exists(storage, storage_path):
                        result.queued_existing_storage += 1
                        _update_document_row(
                            client,
                            document_id,
                            {"embedding_status": "pending"},
                        )
                        result.updated_rows += 1
                        embedding_document_ids.append(document_id)
                        continue

                    if local_match is None:
                        result.missing_local_file += 1
                        _update_document_row(
                            client,
                            document_id,
                            {"embedding_status": AWAITING_SOURCE_STATUS},
                        )
                        result.updated_rows += 1
                        continue

                    persisted = await _persist_local_match(
                        storage,
                        source,
                        str(doc.get("external_id") or ""),
                        local_match,
                        root=root,
                    )
                    update_fields = {
                        "storage_path": persisted.storage_path,
                        "file_size_bytes": persisted.file_size_bytes,
                        "content_hash": persisted.content_hash,
                        "embedding_status": "pending",
                    }
                    _update_document_row(client, document_id, update_fields)
                    result.updated_rows += 1
                    if persisted.reused_existing:
                        result.reused_storage_object += 1
                    else:
                        result.uploaded += 1
                    embedding_document_ids.append(document_id)
                    continue

                if not _should_retry_awaiting_source(doc):
                    continue

                local_match = resolve_local_source_file(
                    source,
                    str(doc.get("external_id") or ""),
                    title=str(doc.get("title") or ""),
                    publication_date=str(doc.get("publication_date") or ""),
                    data_root=root,
                )
                if local_match is None:
                    result.missing_local_file += 1
                    _update_document_row(
                        client,
                        document_id,
                        {"embedding_status": AWAITING_SOURCE_STATUS},
                    )
                    result.updated_rows += 1
                    continue

                if result.eligible >= limit:
                    break
                result.eligible += 1

                persisted = await _persist_local_match(
                    storage,
                    source,
                    str(doc.get("external_id") or ""),
                    local_match,
                    root=root,
                )
                update_fields = {
                    "storage_path": persisted.storage_path,
                    "file_size_bytes": persisted.file_size_bytes,
                    "content_hash": persisted.content_hash,
                    "embedding_status": "pending",
                }
                _update_document_row(client, document_id, update_fields)
                result.updated_rows += 1
                if persisted.reused_existing:
                    result.reused_storage_object += 1
                else:
                    result.uploaded += 1
                embedding_document_ids.append(document_id)

            except Exception as exc:
                message = f"{doc.get('external_id', '?')}: {exc}"
                logger.warning("Mounted-disk backfill failed for %s: %s", doc.get("external_id"), exc)
                result.errors.append(message)

        if result.eligible >= limit:
            break

    embedding_document_ids = list(dict.fromkeys(filter(None, embedding_document_ids)))
    if trigger_embedding and not embedding_document_ids:
        embedding_document_ids = _pending_embedding_document_ids(
            client,
            source,
            limit=limit,
        )
    result.queued_for_embedding = len(embedding_document_ids)

    if bootstrap_missing_documents:
        bootstrap_result = bootstrap_source_documents_from_mounted_disk(
            source,
            data_root=root,
            supabase_client=client,
            limit=max(limit, FETCH_PAGE_SIZE),
        )
        result.registered_rows += int(bootstrap_result.get("registered_rows", 0) or 0)
        result.existing_registered_rows += int(
            bootstrap_result.get("existing_registered_rows", 0) or 0
        )
        result.alias_rows_upserted += int(bootstrap_result.get("alias_rows_upserted", 0) or 0)
        result.errors.extend(bootstrap_result.get("errors", [])[:25])

    if trigger_embedding and embedding_document_ids:
        job_id = f"mounted-backfill-{source}-{int(time.time())}-{os.getpid()}"
        run_id = await temporal.trigger_embedding_workflow(
            job_id=job_id,
            source_type=source,
            document_count=len(embedding_document_ids),
            document_ids=embedding_document_ids,
            batch_size=min(25, max(len(embedding_document_ids), 1)),
            max_documents=len(embedding_document_ids),
        )
        result.trigger_job_id = job_id
        result.trigger_run_id = run_id
        if wait_for_workflow and run_id:
            result.workflow_result = await _wait_for_workflow_result(job_id)

    return result


async def backfill_source_from_mounted_disk_until_exhausted(
    source: str,
    *,
    limit: int = 100,
    include_existing_storage: bool = False,
    bootstrap_missing_documents: bool = True,
    trigger_embedding: bool = True,
    wait_for_workflow: bool = False,
    data_root: Optional[str | Path] = None,
    max_passes: Optional[int] = None,
    sleep_between_passes: float = 0.0,
    supabase_client: Any | None = None,
    storage_adapter: SupabaseStorageAdapter | None = None,
    temporal_client: TemporalScraperClient | Any | None = None,
) -> MountedBackfillDrainResult:
    client = supabase_client or _get_supabase_client_from_env()
    storage = storage_adapter or _get_storage_adapter_from_env(client)
    temporal = temporal_client or TemporalScraperClient()

    summary = MountedBackfillDrainResult(source=source)

    while True:
        remaining_before = _count_documents_missing_storage(client, source)
        summary.remaining_missing_storage = remaining_before
        if remaining_before <= 0:
            summary.completed = True
            return summary

        if max_passes is not None and summary.passes >= max_passes:
            return summary

        result = await backfill_source_from_mounted_disk(
            source,
            limit=limit,
            include_existing_storage=include_existing_storage,
            bootstrap_missing_documents=bootstrap_missing_documents,
            trigger_embedding=trigger_embedding,
            wait_for_workflow=wait_for_workflow,
            data_root=data_root,
            supabase_client=client,
            storage_adapter=storage,
            temporal_client=temporal,
        )
        remaining_after = _count_documents_missing_storage(client, source)
        hydrated_this_pass = max(0, remaining_before - remaining_after)

        pass_data = result.to_dict()
        pass_data["remaining_before"] = remaining_before
        pass_data["remaining_after"] = remaining_after
        pass_data["hydrated_this_pass"] = hydrated_this_pass
        summary.pass_results.append(pass_data)
        summary.passes += 1
        summary.hydrated_documents += hydrated_this_pass
        summary.registered_documents += int(result.registered_rows or 0)
        summary.remaining_missing_storage = remaining_after

        if remaining_after <= 0:
            summary.completed = True
            return summary

        if hydrated_this_pass <= 0 and int(result.registered_rows or 0) <= 0:
            summary.stalled = True
            return summary

        if sleep_between_passes > 0:
            await asyncio.sleep(sleep_between_passes)
