"""
Benchmark harness for CAS/TAS extraction and chunking strategies.

The CAS corpus already includes raw PDFs plus converted artifacts generated from
the current self-hosted pipeline. This module benchmarks chunking candidates on
top of those artifacts and records structure, retrieval, and operational
metrics without mutating production data.
"""
from __future__ import annotations

import json
import io
import logging
import math
import os
import random
import re
import shutil
import statistics
import subprocess
import sys
import time
import warnings
from collections import Counter, defaultdict
from contextlib import contextmanager, redirect_stderr, redirect_stdout
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator, Sequence

STOPWORDS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "award",
    "by",
    "court",
    "de",
    "del",
    "des",
    "du",
    "el",
    "en",
    "for",
    "i",
    "ii",
    "iii",
    "iv",
    "in",
    "is",
    "la",
    "las",
    "le",
    "les",
    "los",
    "of",
    "or",
    "sport",
    "the",
    "to",
    "tribunal",
    "v",
    "vs",
    "y",
}
TOKEN_RE = re.compile(r"[A-Za-zÀ-ÿ0-9][A-Za-zÀ-ÿ0-9'/_-]*")
SENTENCE_BOUNDARY_RE = re.compile(r"(?<=[.!?])\s+(?=[A-Z0-9ÁÉÍÓÚÑ])")
PDFINFO_PAGES_RE = re.compile(r"^Pages:\s+(\d+)$", re.MULTILINE)
FRONTMATTER_RE = re.compile(r"^---\n(.*?)\n---\n?", re.DOTALL)
NUMERIC_MARKER_RE = re.compile(r"^(?:\d+|[IVXLCDM]+)\.$", re.IGNORECASE)
CAS_FACTS_RE = re.compile(r"^(?:I{0,3}\.?\s*)?(?:FACTS?|HECHOS|FAITS)\s*$", re.IGNORECASE)
CAS_REASONS_RE = re.compile(
    r"^(?:I{0,3}\.?\s*)?(?:REASONS?|MERITS?|FUNDAMENTOS?|MOTIFS?)\s*$",
    re.IGNORECASE,
)
CAS_DECISION_RE = re.compile(
    r"^(?:I{0,3}\.?\s*)?(?:DECISION|AWARD|RULING|DISPOSITIVO|DISPOSITIF)\s*$",
    re.IGNORECASE,
)
DEFAULT_CORE_CANDIDATES = (
    "pdftext_floor_v1",
    "worker_prod_current_v1",
    "unified_current_v1",
    "canonical_section_v1",
    "markdown_title_v1",
    "markdown_cas_v1",
)
OPTIONAL_CANDIDATES = (
    "docling_cpu_v1",
    "unstructured_hires_subset_v1",
    "marker_cpu_subset_v1",
)
DEFAULT_ARTIFACT_CANDIDATES = (
    "canonical_section_v1",
    "unstructured_md_balanced_v1",
    "unstructured_md_dense_v1",
    "markdown_title_v1",
    "markdown_cas_v1",
    "unified_current_v1",
    "pdftext_floor_v1",
)
DEFAULT_RAW_PDF_CANDIDATES = (
    "unstructured_pdf_fast_v1",
    "pymupdf_unstructured_v1",
    "docling_cpu_hybrid_v1",
    "unstructured_hires_subset_v1",
    "marker_cpu_subset_v1",
)
DEFAULT_CANDIDATE_SETS = {
    "control": (
        "canonical_section_v1",
        "markdown_title_v1",
        "markdown_cas_v1",
        "unified_current_v1",
        "pdftext_floor_v1",
    ),
    "unstructured": (
        "canonical_section_v1",
        "unstructured_md_balanced_v1",
        "unstructured_md_dense_v1",
        "unstructured_pdf_fast_v1",
        "unstructured_hires_subset_v1",
    ),
    "hybrid": (
        "canonical_section_v1",
        "pymupdf_unstructured_v1",
        "docling_cpu_hybrid_v1",
        "marker_cpu_subset_v1",
    ),
    "all": tuple(dict.fromkeys(DEFAULT_CORE_CANDIDATES + DEFAULT_ARTIFACT_CANDIDATES + DEFAULT_RAW_PDF_CANDIDATES)),
}
QUERY_SET_VERSION = "cas_benchmark_v1"
DEFAULT_DATA_ROOT = Path(os.environ.get("SCRAPER_MOUNTED_DATA_ROOT", "/app/mounted_data"))
DEFAULT_DOCLING_ARTIFACTS_PATH = os.environ.get("SCRAPER_DOCLING_ARTIFACTS_PATH")
CAS_RUNTIME_GATES_HOURS = {
    "artifact_chunking": 4.0,
    "raw_pdf_extraction": 12.0,
}
_NOISY_PDF_LOGGERS = (
    "RapidOCR",
    "docling.models.stages.ocr.rapid_ocr_model",
    "pdfminer",
    "pdfminer.pdfinterp",
    "pypdf",
    "pypdf._reader",
)
_UNSTRUCTURED_NOISY_PDF_LOGGERS = _NOISY_PDF_LOGGERS + ("unstructured.partition.pdf",)
_DEFAULT_UNSTRUCTURED_CAS_LANGUAGES = ("eng", "fra")


def _configure_local_model_runtime() -> None:
    # Benchmark runs should stay local-first, but strict offline mode remains
    # opt-in because some optional challengers still fetch artifacts on first
    # use and we want the benchmark to execute end-to-end by default.
    force_offline = os.environ.get("SCRAPER_BENCHMARK_FORCE_OFFLINE", "").lower() in {
        "1",
        "true",
        "yes",
    }
    if force_offline:
        os.environ.setdefault("HF_HUB_OFFLINE", "1")
        os.environ.setdefault("TRANSFORMERS_OFFLINE", "1")
    os.environ.setdefault("HF_HUB_DISABLE_TELEMETRY", "1")


def _docling_self_hosted_status() -> str:
    if DEFAULT_DOCLING_ARTIFACTS_PATH:
        return "local_only"
    return "local_with_optional_artifacts"


@contextmanager
def _suppress_noisy_pdf_logs(include_unstructured: bool = False) -> Iterator[None]:
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
def _docling_offline_runtime() -> Iterator[None]:
    if not DEFAULT_DOCLING_ARTIFACTS_PATH:
        yield
        return

    previous = {
        "HF_HUB_OFFLINE": os.environ.get("HF_HUB_OFFLINE"),
        "TRANSFORMERS_OFFLINE": os.environ.get("TRANSFORMERS_OFFLINE"),
    }
    os.environ["HF_HUB_OFFLINE"] = "1"
    os.environ["TRANSFORMERS_OFFLINE"] = "1"
    try:
        yield
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def _within_runtime_gate(lane: str, projected_hours: float | None) -> bool:
    if projected_hours is None:
        return True
    max_hours = CAS_RUNTIME_GATES_HOURS.get(lane)
    if max_hours is None:
        return True
    return projected_hours <= max_hours


def _winner_eligible(report: AggregateReport) -> bool:
    return report.decision_status != "blocked" and _within_runtime_gate(
        report.lane, report.projected_full_corpus_runtime_hours
    )


def _normalize_runtime_status(report: AggregateReport) -> None:
    if report.decision_status == "blocked":
        return
    if not _within_runtime_gate(report.lane, report.projected_full_corpus_runtime_hours):
        report.decision_status = "over_budget"


@dataclass(frozen=True)
class DocumentBundle:
    document_id: str
    pdf_path: Path
    converted_dir: Path
    canonical_json_path: Path | None
    output_md_path: Path | None
    output_txt_path: Path | None
    meta_json_path: Path | None


@dataclass
class InventoryRow:
    document_id: str
    pdf_path: str
    converted_dir: str
    title: str | None
    publication_date: str | None
    language: str | None
    procedure: str | None
    sport: str | None
    subject_keywords: list[str]
    page_count: int | None
    pdf_size_bytes: int
    output_txt_bytes: int
    text_density: float | None
    year_bucket: str
    encrypted_pdf: bool = False
    page_bucket: str = "unknown"
    density_bucket: str = "unknown"
    stratum: str = "unknown"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class BenchmarkChunk:
    text: str
    label: str | None
    token_count: int
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "text": self.text,
            "label": self.label,
            "token_count": self.token_count,
            "metadata": self.metadata,
        }


@dataclass
class BenchmarkQuery:
    query_id: str
    query_type: str
    text: str
    target_label: str | None
    target_anchor: str | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class CandidateRun:
    candidate_id: str
    input_source: str
    extraction_method: str
    chunking_method: str
    chunks: list[BenchmarkChunk]
    warnings: list[str] = field(default_factory=list)
    failed: bool = False
    skipped: bool = False
    error: str | None = None
    processing_time_ms: int = 0
    rss_mb: float | None = None


@dataclass
class PerDocumentResult:
    candidate_id: str
    lane: str
    document_id: str
    input_source: str
    extraction_method: str
    chunking_method: str
    self_hosted_status: str
    requires_remote_service: bool
    ocr_languages: list[str]
    chunk_count: int
    total_tokens: int
    token_stats: dict[str, Any]
    structure_metrics: dict[str, Any]
    retrieval_metrics: dict[str, Any]
    resource_metrics: dict[str, Any]
    warnings: list[str]
    failed: bool
    skipped: bool
    error: str | None = None
    chunks: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class AggregateReport:
    candidate_id: str
    lane: str
    documents_run: int
    success_rate: float
    self_hosted_status: str
    requires_remote_service: bool
    ocr_languages: list[str]
    manual_scores: dict[str, Any]
    structure_metrics: dict[str, Any]
    retrieval_metrics: dict[str, Any]
    ops_metrics: dict[str, Any]
    projected_full_corpus_runtime_hours: float | None
    decision_status: str
    weighted_score: float = 0.0
    skipped_documents: int = 0
    failed_documents: int = 0

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class CandidateSpec:
    candidate_id: str
    lane: str
    self_hosted_status: str
    requires_remote_service: bool = False
    ocr_languages: tuple[str, ...] = ()
    subset_only: bool = False
    description: str = ""


@dataclass
class BenchmarkManifest:
    split_seed: int
    sample_sets: dict[str, list[str]]
    candidate_ids: list[str]
    mode: str
    candidate_set: str
    resource_limits: dict[str, Any]
    acceptance_thresholds: dict[str, Any]
    query_set_version: str = QUERY_SET_VERSION

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def discover_cas_bundles(data_root: Path = DEFAULT_DATA_ROOT) -> list[DocumentBundle]:
    pdf_root = data_root / "cas_pdfs"
    converted_root = data_root / "cas" / "converted"
    bundles: list[DocumentBundle] = []
    if not pdf_root.exists():
        return bundles

    for pdf_path in sorted(pdf_root.glob("*.pdf")):
        document_id = pdf_path.stem
        converted_dir = converted_root / document_id
        bundles.append(
            DocumentBundle(
                document_id=document_id,
                pdf_path=pdf_path,
                converted_dir=converted_dir,
                canonical_json_path=_existing_path(converted_dir / "canonical.json"),
                output_md_path=_existing_path(converted_dir / "output.md"),
                output_txt_path=_existing_path(converted_dir / "output.txt"),
                meta_json_path=_existing_path(converted_dir / "meta.json"),
            )
        )
    return bundles


def _pdf_profile(pdf_path: Path) -> tuple[int | None, bool]:
    result = subprocess.run(
        ["pdfinfo", str(pdf_path)],
        capture_output=True,
        check=False,
        text=True,
    )
    if result.returncode != 0:
        return None, False

    page_count = None
    encrypted = False
    for line in result.stdout.splitlines():
        if line.startswith("Pages:"):
            try:
                page_count = int(line.split(":", 1)[1].strip())
            except Exception:
                page_count = None
        elif line.startswith("Encrypted:"):
            encrypted = "yes" in line.lower()
    return page_count, encrypted


def build_inventory(
    bundles: Sequence[DocumentBundle],
    include_pdf_info: bool = True,
) -> list[InventoryRow]:
    rows: list[InventoryRow] = []
    for bundle in bundles:
        canonical = load_canonical_document(bundle)
        metadata = canonical.get("metadata", {}) if canonical else {}
        extra = metadata.get("extra", {}) if isinstance(metadata.get("extra"), dict) else {}
        txt_bytes = bundle.output_txt_path.stat().st_size if bundle.output_txt_path else 0
        if include_pdf_info:
            page_count, encrypted_pdf = _pdf_profile(bundle.pdf_path)
        else:
            page_count, encrypted_pdf = None, False
        density = (txt_bytes / page_count) if page_count else None
        row = InventoryRow(
            document_id=bundle.document_id,
            pdf_path=str(bundle.pdf_path),
            converted_dir=str(bundle.converted_dir),
            title=_coerce_text(metadata.get("title")),
            publication_date=_coerce_text(metadata.get("publication_date")),
            language=_coerce_text(extra.get("language")),
            procedure=_coerce_text(extra.get("procedure")),
            sport=_first_subject(metadata),
            subject_keywords=[str(k) for k in extra.get("keywords", []) if str(k).strip()],
            page_count=page_count,
            pdf_size_bytes=bundle.pdf_path.stat().st_size,
            output_txt_bytes=txt_bytes,
            text_density=density,
            year_bucket=_year_bucket(_coerce_text(metadata.get("publication_date"))),
            encrypted_pdf=encrypted_pdf,
        )
        rows.append(row)

    _assign_quantile_buckets(rows, "page_count", "page_bucket")
    _assign_quantile_buckets(rows, "text_density", "density_bucket")
    for row in rows:
        row.stratum = "|".join(
            [
                row.year_bucket,
                row.procedure or "unknown",
                row.language or "unknown",
                row.page_bucket,
                row.density_bucket,
            ]
        )
    return rows


def build_stratified_splits(
    inventory_rows: Sequence[InventoryRow],
    seed: int,
    smoke_size: int,
    tuning_size: int,
    holdout_size: int,
) -> dict[str, list[str]]:
    rng = random.Random(seed)
    groups: dict[str, list[str]] = defaultdict(list)
    for row in inventory_rows:
        groups[row.stratum].append(row.document_id)

    for ids in groups.values():
        rng.shuffle(ids)

    ordered = _round_robin_groups(groups)
    return {
        "smoke": ordered[:smoke_size],
        "tuning": ordered[smoke_size : smoke_size + tuning_size],
        "holdout": ordered[smoke_size + tuning_size : smoke_size + tuning_size + holdout_size],
    }


def build_difficult_splits(
    inventory_rows: Sequence[InventoryRow],
    seed: int,
    smoke_size: int,
    tuning_size: int,
    holdout_size: int,
    difficult_subset_size: int | None = None,
) -> dict[str, list[str]]:
    total_requested = smoke_size + tuning_size + holdout_size
    if difficult_subset_size is None:
        difficult_subset_size = total_requested
    difficult_subset_size = max(difficult_subset_size, total_requested)

    ranked_rows = sorted(
        inventory_rows,
        key=lambda row: (
            -_difficulty_score(row),
            row.language != "fr",
            row.density_bucket != "q1",
            row.page_bucket != "q4",
            row.document_id,
        ),
    )
    selected_rows = ranked_rows[:difficult_subset_size]

    rng = random.Random(seed)
    groups: dict[str, list[str]] = defaultdict(list)
    for row in selected_rows:
        groups[_difficulty_group(row)].append(row.document_id)
    for ids in groups.values():
        rng.shuffle(ids)

    ordered = _round_robin_groups(groups)
    return {
        "smoke": ordered[:smoke_size],
        "tuning": ordered[smoke_size : smoke_size + tuning_size],
        "holdout": ordered[smoke_size + tuning_size : smoke_size + tuning_size + holdout_size],
    }


def _difficulty_group(row: InventoryRow) -> str:
    return "|".join(
        [
            "enc" if row.encrypted_pdf else "plain",
            row.language or "unknown",
            row.page_bucket,
            row.density_bucket,
        ]
    )


def _difficulty_score(row: InventoryRow) -> int:
    score = 0
    if row.encrypted_pdf:
        score += 5
    if row.language == "fr":
        score += 4
    if row.density_bucket == "q1":
        score += 3
    if row.page_bucket == "q4":
        score += 2
    if (row.pdf_size_bytes or 0) > 1_500_000:
        score += 1
    return score


def _normalize_mode(mode: str) -> str:
    normalized = (mode or "artifact_chunking").strip().lower()
    allowed = {"artifact_chunking", "raw_pdf_extraction", "full_decision"}
    if normalized not in allowed:
        raise ValueError(f"Unsupported benchmark mode: {mode}")
    return normalized


def _normalize_candidate_set(candidate_set: str) -> str:
    normalized = (candidate_set or "all").strip().lower()
    if normalized not in DEFAULT_CANDIDATE_SETS:
        raise ValueError(f"Unsupported candidate set: {candidate_set}")
    return normalized


def _resolve_requested_candidates(
    candidate_ids: Sequence[str] | None,
    candidate_set: str,
) -> list[str]:
    if candidate_ids:
        return list(dict.fromkeys(candidate_ids))
    return list(DEFAULT_CANDIDATE_SETS[candidate_set])


def build_candidate_specs() -> dict[str, CandidateSpec]:
    return {
        "pdftext_floor_v1": CandidateSpec(
            candidate_id="pdftext_floor_v1",
            lane="artifact_chunking",
            self_hosted_status="local_only",
            description="Plain pdftotext floor baseline",
        ),
        "worker_prod_current_v1": CandidateSpec(
            candidate_id="worker_prod_current_v1",
            lane="raw_pdf_extraction",
            self_hosted_status="local_only",
            description="Current worker extraction baseline",
        ),
        "unified_current_v1": CandidateSpec(
            candidate_id="unified_current_v1",
            lane="artifact_chunking",
            self_hosted_status="local_only",
            description="Current shared section-aware text chunking",
        ),
        "canonical_section_v1": CandidateSpec(
            candidate_id="canonical_section_v1",
            lane="artifact_chunking",
            self_hosted_status="local_only",
            description="Current production CAS canonical chunker",
        ),
        "markdown_title_v1": CandidateSpec(
            candidate_id="markdown_title_v1",
            lane="artifact_chunking",
            self_hosted_status="local_only",
            description="Markdown AST by-title fallback",
        ),
        "markdown_cas_v1": CandidateSpec(
            candidate_id="markdown_cas_v1",
            lane="artifact_chunking",
            self_hosted_status="local_only",
            description="Markdown AST CAS-aware chunking",
        ),
        "unstructured_md_balanced_v1": CandidateSpec(
            candidate_id="unstructured_md_balanced_v1",
            lane="artifact_chunking",
            self_hosted_status="local_only",
            description="Unstructured by-title over output.md, balanced settings",
        ),
        "unstructured_md_dense_v1": CandidateSpec(
            candidate_id="unstructured_md_dense_v1",
            lane="artifact_chunking",
            self_hosted_status="local_only",
            description="Unstructured by-title over output.md, dense settings",
        ),
        "unstructured_pdf_fast_v1": CandidateSpec(
            candidate_id="unstructured_pdf_fast_v1",
            lane="raw_pdf_extraction",
            self_hosted_status="local_only",
            description="Raw PDF through Unstructured fast partitioning",
        ),
        "pymupdf_unstructured_v1": CandidateSpec(
            candidate_id="pymupdf_unstructured_v1",
            lane="raw_pdf_extraction",
            self_hosted_status="local_only",
            ocr_languages=("eng", "fra"),
            description="PyMuPDF4LLM extraction followed by Unstructured title chunking",
        ),
        "docling_cpu_v1": CandidateSpec(
            candidate_id="docling_cpu_v1",
            lane="raw_pdf_extraction",
            self_hosted_status=_docling_self_hosted_status(),
            description="Legacy Docling benchmark adapter",
        ),
        "docling_cpu_hybrid_v1": CandidateSpec(
            candidate_id="docling_cpu_hybrid_v1",
            lane="raw_pdf_extraction",
            self_hosted_status=_docling_self_hosted_status(),
            description="Docling HybridChunker on CPU with remote services disabled",
        ),
        "unstructured_hires_subset_v1": CandidateSpec(
            candidate_id="unstructured_hires_subset_v1",
            lane="raw_pdf_extraction",
            self_hosted_status="local_with_optional_artifacts",
            subset_only=True,
            description="Subset-only Unstructured hi_res challenger",
        ),
        "marker_cpu_subset_v1": CandidateSpec(
            candidate_id="marker_cpu_subset_v1",
            lane="raw_pdf_extraction",
            self_hosted_status="local_with_optional_artifacts",
            subset_only=True,
            description="Subset-only Marker CPU challenger",
        ),
    }


def run_benchmark(
    *,
    output_dir: Path,
    data_root: Path = DEFAULT_DATA_ROOT,
    smoke_size: int = 40,
    tuning_size: int = 120,
    holdout_size: int = 60,
    seed: int = 20260412,
    candidate_ids: Sequence[str] | None = None,
    doc_ids: Sequence[str] | None = None,
    mode: str = "artifact_chunking",
    candidate_set: str = "all",
    difficult_subset_size: int | None = None,
    include_optional: bool = False,
    include_pdf_info: bool = True,
) -> dict[str, Any]:
    _reset_benchmark_output_dir(output_dir)
    _configure_local_model_runtime()
    mode = _normalize_mode(mode)
    candidate_set = _normalize_candidate_set(candidate_set)
    _write_progress_status(
        output_dir / "progress.json",
        phase="initializing",
        completed=0,
        total=0,
        extra={
            "mode": mode,
            "candidate_set": candidate_set,
            "status": "building_inventory",
        },
    )
    bundles = discover_cas_bundles(data_root)
    if not bundles:
        raise FileNotFoundError(f"No CAS PDFs found under {data_root / 'cas_pdfs'}")

    bundle_map = {bundle.document_id: bundle for bundle in bundles}
    inventory = build_inventory(bundles, include_pdf_info=include_pdf_info)
    inventory_map = {row.document_id: row for row in inventory}
    candidate_registry = build_candidate_registry()
    candidate_specs = build_candidate_specs()

    if doc_ids:
        selected_ids = [doc_id for doc_id in doc_ids if doc_id in bundle_map]
        sample_sets = {"selected": selected_ids}
        difficult_sets = {"selected": selected_ids}
    else:
        sample_sets = build_stratified_splits(
            inventory,
            seed=seed,
            smoke_size=smoke_size,
            tuning_size=tuning_size,
            holdout_size=holdout_size,
        )
        difficult_sets = build_difficult_splits(
            inventory,
            seed=seed,
            smoke_size=smoke_size,
            tuning_size=tuning_size,
            holdout_size=holdout_size,
            difficult_subset_size=difficult_subset_size,
        )
        selected_ids = [doc_id for ids in sample_sets.values() for doc_id in ids]

    requested_candidates = _resolve_requested_candidates(candidate_ids, candidate_set)
    if include_optional:
        requested_candidates.extend(candidate_id for candidate_id in OPTIONAL_CANDIDATES if candidate_id not in requested_candidates)
    requested_candidates = [cid for cid in requested_candidates if cid in candidate_registry]
    if mode == "artifact_chunking":
        requested_candidates = [cid for cid in requested_candidates if candidate_specs[cid].lane == "artifact_chunking"]
    elif mode == "raw_pdf_extraction":
        requested_candidates = [cid for cid in requested_candidates if candidate_specs[cid].lane == "raw_pdf_extraction"]
    if not requested_candidates:
        raise ValueError("No benchmark candidates selected for the requested mode/candidate-set")

    manifest_sample_sets = dict(sample_sets)
    if mode == "full_decision":
        manifest_sample_sets = {
            **{f"artifact_{name}": ids for name, ids in sample_sets.items()},
            **{f"raw_pdf_{name}": ids for name, ids in difficult_sets.items()},
        }
    elif mode == "raw_pdf_extraction":
        manifest_sample_sets = difficult_sets
    _write_json(
        output_dir / "manifest.json",
        _default_manifest(
            manifest_sample_sets,
            requested_candidates,
            seed,
            mode=mode,
            candidate_set=candidate_set,
        ).to_dict(),
    )
    _write_jsonl(output_dir / "inventory.jsonl", [row.to_dict() for row in inventory])
    _write_json(
        output_dir / "splits.json",
        {
            "artifact_chunking": sample_sets,
            "raw_pdf_extraction": difficult_sets,
        }
        if mode == "full_decision"
        else {mode: difficult_sets if mode == "raw_pdf_extraction" else sample_sets},
    )

    queries: list[dict[str, Any]] = []
    results_by_candidate: dict[str, list[PerDocumentResult]] = defaultdict(list)
    lane_ids = {
        "artifact_chunking": [doc_id for ids in sample_sets.values() for doc_id in ids],
        "raw_pdf_extraction": [doc_id for ids in difficult_sets.values() for doc_id in ids],
    }
    if doc_ids:
        lane_ids = {"artifact_chunking": selected_ids, "raw_pdf_extraction": selected_ids}
    candidates_dir = output_dir / "candidates"
    candidates_dir.mkdir(exist_ok=True)
    selected_candidate_ids = [
        candidate_id
        for candidate_id in requested_candidates
        if mode == "full_decision" or candidate_specs[candidate_id].lane == mode
    ]
    query_cache: dict[str, list[BenchmarkQuery]] = {}
    query_warnings: dict[str, str] = {}
    for document_id in dict.fromkeys(selected_ids):
        bundle = bundle_map[document_id]
        try:
            document_queries = build_document_queries(bundle)
        except Exception as exc:
            document_queries = []
            query_warnings[document_id] = f"query_generation_failed: {exc}"
        query_cache[document_id] = document_queries
    seen_queries: set[tuple[str, str, str]] = set()
    for lane, lane_document_ids in lane_ids.items():
        for document_id in lane_document_ids:
            for query in query_cache.get(document_id, []):
                key = (lane, document_id, query.query_id)
                if key in seen_queries:
                    continue
                seen_queries.add(key)
                queries.append({"document_id": document_id, "lane": lane, **query.to_dict()})
    _write_jsonl(output_dir / "queries.jsonl", queries)
    total_candidate_runs = sum(len(lane_ids[candidate_specs[candidate_id].lane]) for candidate_id in selected_candidate_ids)
    completed_candidate_runs = 0
    _write_progress_status(
        output_dir / "progress.json",
        phase="running_candidates",
        completed=0,
        total=total_candidate_runs,
        extra={
            "mode": mode,
            "candidate_count": len(selected_candidate_ids),
            "candidate_runs_completed": 0,
            "candidate_runs_total": total_candidate_runs,
        },
    )

    for candidate_id in requested_candidates:
        spec = candidate_specs[candidate_id]
        if mode != "full_decision" and spec.lane != mode:
            continue
        candidate_dir = candidates_dir / candidate_id
        candidate_dir.mkdir(exist_ok=True)
        candidate_total = len(lane_ids[spec.lane])
        _write_progress_status(
            candidate_dir / "status.json",
            phase="running",
            completed=0,
            total=candidate_total,
            current_candidate_id=candidate_id,
            extra={"lane": spec.lane},
        )
        for document_id in lane_ids[spec.lane]:
            bundle = bundle_map[document_id]
            inventory_row = inventory_map[document_id]
            document_queries = query_cache.get(document_id, [])
            query_warning = query_warnings.get(document_id)
            runner = candidate_registry[candidate_id]
            try:
                run = runner(bundle)
                result = analyze_candidate_run(bundle, inventory_row, run, document_queries, spec)
            except Exception as exc:
                result = analyze_candidate_run(
                    bundle,
                    inventory_row,
                    _failed_candidate_run(candidate_id, exc),
                    document_queries,
                    spec,
                )
            if query_warning:
                result.warnings.append(query_warning)
            results_by_candidate[candidate_id].append(result)
            completed_candidate_runs += 1
            _append_jsonl(candidate_dir / "results.jsonl", result.to_dict())
            _write_progress_status(
                candidate_dir / "status.json",
                phase="running",
                completed=len(results_by_candidate[candidate_id]),
                total=candidate_total,
                current_candidate_id=candidate_id,
                current_item_id=document_id,
                extra={"lane": spec.lane},
            )
            _write_progress_status(
                output_dir / "progress.json",
                phase="running_candidates",
                completed=completed_candidate_runs,
                total=total_candidate_runs,
                current_candidate_id=candidate_id,
                current_item_id=document_id,
                extra={
                    "mode": mode,
                    "candidate_count": len(selected_candidate_ids),
                    "candidate_runs_completed": completed_candidate_runs,
                    "candidate_runs_total": total_candidate_runs,
                },
            )

    aggregates: list[AggregateReport] = []
    for candidate_id, results in results_by_candidate.items():
        candidate_dir = candidates_dir / candidate_id
        _write_jsonl(candidate_dir / "results.jsonl", [result.to_dict() for result in results])
        aggregate = aggregate_candidate_results(candidate_id, results, len(bundles), candidate_specs[candidate_id])
        aggregates.append(aggregate)
        _write_json(candidate_dir / "aggregate.json", aggregate.to_dict())
        _write_progress_status(
            candidate_dir / "status.json",
            phase="completed",
            completed=len(results),
            total=len(results),
            current_candidate_id=candidate_id,
            extra={"lane": candidate_specs[candidate_id].lane},
        )

    ranked = rank_aggregates(aggregates)
    _write_json(output_dir / "aggregate.json", [aggregate.to_dict() for aggregate in ranked])
    summary = render_summary_markdown(
        ranked,
        sample_sets if mode != "raw_pdf_extraction" else difficult_sets,
        inventory,
        mode=mode,
        difficult_sets=difficult_sets,
    )
    (output_dir / "summary.md").write_text(summary, encoding="utf-8")
    review_doc = render_holdout_review(
        (
            list(dict.fromkeys(sample_sets.get("holdout", []) + difficult_sets.get("holdout", [])))
            if mode == "full_decision"
            else (difficult_sets if mode == "raw_pdf_extraction" else sample_sets).get("holdout", [])
        ),
        bundle_map,
        results_by_candidate,
        ranked[:3],
    )
    (output_dir / "review_holdout.md").write_text(review_doc, encoding="utf-8")
    _write_progress_status(
        output_dir / "progress.json",
        phase="completed",
        completed=total_candidate_runs,
        total=total_candidate_runs,
        extra={
            "mode": mode,
            "candidate_count": len(selected_candidate_ids),
            "candidate_runs_completed": total_candidate_runs,
            "candidate_runs_total": total_candidate_runs,
        },
    )

    return {
        "output_dir": str(output_dir),
        "mode": mode,
        "candidate_set": candidate_set,
        "candidate_ids": requested_candidates,
        "sample_sets": (
            {"artifact_chunking": sample_sets, "raw_pdf_extraction": difficult_sets}
            if mode == "full_decision"
            else (difficult_sets if mode == "raw_pdf_extraction" else sample_sets)
        ),
        "aggregates": [aggregate.to_dict() for aggregate in ranked],
    }


def build_candidate_registry() -> dict[str, Callable[[DocumentBundle], CandidateRun]]:
    return {
        "pdftext_floor_v1": run_pdftext_floor_v1,
        "worker_prod_current_v1": run_worker_prod_current_v1,
        "unified_current_v1": run_unified_current_v1,
        "canonical_section_v1": run_canonical_section_v1,
        "markdown_title_v1": run_markdown_title_v1,
        "markdown_cas_v1": run_markdown_cas_v1,
        "unstructured_md_balanced_v1": run_unstructured_md_balanced_v1,
        "unstructured_md_dense_v1": run_unstructured_md_dense_v1,
        "unstructured_pdf_fast_v1": run_unstructured_pdf_fast_v1,
        "pymupdf_unstructured_v1": run_pymupdf_unstructured_v1,
        "docling_cpu_v1": run_docling_cpu_v1,
        "docling_cpu_hybrid_v1": run_docling_cpu_hybrid_v1,
        "unstructured_hires_subset_v1": run_unstructured_hires_subset_v1,
        "marker_cpu_subset_v1": run_marker_cpu_subset_v1,
    }


def run_pdftext_floor_v1(bundle: DocumentBundle) -> CandidateRun:
    def _runner() -> CandidateRun:
        text = _pdftotext(bundle.pdf_path)
        chunks = [
            BenchmarkChunk(
                text=piece,
                label=f"Párrafo {index + 1}",
                token_count=_count_tokens(piece),
                metadata={"source": "pdftotext_floor"},
            )
            for index, piece in enumerate(_paragraph_split(text))
        ]
        return CandidateRun(
            candidate_id="pdftext_floor_v1",
            input_source="pdf",
            extraction_method="pdftotext",
            chunking_method="paragraph_floor",
            chunks=chunks,
        )

    return _timed_candidate(_runner, candidate_id="pdftext_floor_v1")


def run_worker_prod_current_v1(bundle: DocumentBundle) -> CandidateRun:
    def _runner() -> CandidateRun:
        chunk_dicts = _run_worker_extract_chunks(bundle.pdf_path, bundle.document_id)
        chunks = [
            BenchmarkChunk(
                text=chunk["text"],
                label=chunk.get("title"),
                token_count=_coerce_int(chunk.get("metadata", {}).get("token_count")) or _count_tokens(chunk["text"]),
                metadata=chunk.get("metadata", {}),
            )
            for chunk in chunk_dicts
            if str(chunk.get("text") or "").strip()
        ]
        return CandidateRun(
            candidate_id="worker_prod_current_v1",
            input_source="pdf",
            extraction_method="unstructured_fast_pdf",
            chunking_method="worker_current",
            chunks=chunks,
        )

    return _timed_candidate(_runner, candidate_id="worker_prod_current_v1")


def run_unified_current_v1(bundle: DocumentBundle) -> CandidateRun:
    def _runner() -> CandidateRun:
        source = _read_text(bundle.output_txt_path)
        nodes = _detect_cas_structure_nodes(source)
        chunk_dicts = _group_nodes_into_chunks(nodes, target_min_tokens=200, target_max_tokens=600, max_chunk_tokens=800)
        chunks = [
            BenchmarkChunk(
                text=chunk["text"],
                label=chunk["label"],
                token_count=chunk["token_count"],
                metadata={"node_types": chunk["node_types"], "source": "output_txt"},
            )
            for chunk in chunk_dicts
        ]
        return CandidateRun(
            candidate_id="unified_current_v1",
            input_source="output_txt",
            extraction_method="converted_text",
            chunking_method="section_aware_port",
            chunks=chunks,
        )

    return _timed_candidate(_runner, candidate_id="unified_current_v1")


def run_canonical_section_v1(bundle: DocumentBundle) -> CandidateRun:
    def _runner() -> CandidateRun:
        from src.gui.infrastructure.cas_chunking import extract_cas_canonical_chunks

        if not bundle.canonical_json_path:
            raise FileNotFoundError(f"Missing canonical.json for {bundle.document_id}")
        chunk_dicts = extract_cas_canonical_chunks(
            bundle.canonical_json_path,
            bundle.document_id,
        )
        chunks = [
            BenchmarkChunk(
                text=chunk["text"],
                label=chunk["title"],
                token_count=_coerce_int(chunk.get("metadata", {}).get("token_count"))
                or _count_tokens(chunk["text"]),
                metadata=chunk.get("metadata", {}),
            )
            for chunk in chunk_dicts
            if chunk.get("text")
        ]
        return CandidateRun(
            candidate_id="canonical_section_v1",
            input_source="canonical_json",
            extraction_method="canonical_sections",
            chunking_method="canonical_section",
            chunks=chunks,
        )

    return _timed_candidate(_runner, candidate_id="canonical_section_v1")


def run_unstructured_md_balanced_v1(bundle: DocumentBundle) -> CandidateRun:
    return _run_unstructured_markdown_candidate(
        bundle,
        candidate_id="unstructured_md_balanced_v1",
        max_characters=2600,
        new_after_n_chars=1800,
        combine_text_under_n_chars=900,
        overlap_tokens=120,
    )


def run_unstructured_md_dense_v1(bundle: DocumentBundle) -> CandidateRun:
    return _run_unstructured_markdown_candidate(
        bundle,
        candidate_id="unstructured_md_dense_v1",
        max_characters=3000,
        new_after_n_chars=2200,
        combine_text_under_n_chars=600,
        overlap_tokens=80,
    )


def _run_unstructured_markdown_candidate(
    bundle: DocumentBundle,
    *,
    candidate_id: str,
    max_characters: int,
    new_after_n_chars: int,
    combine_text_under_n_chars: int,
    overlap_tokens: int,
) -> CandidateRun:
    def _runner() -> CandidateRun:
        body = _read_text(bundle.output_md_path)
        if not body:
            raise FileNotFoundError(f"Missing output.md for {bundle.document_id}")

        chunk_dicts, extraction_method = _chunk_markdown_with_unstructured(
            body,
            max_characters=max_characters,
            new_after_n_chars=new_after_n_chars,
            combine_text_under_n_chars=combine_text_under_n_chars,
            overlap_tokens=overlap_tokens,
        )
        chunks = [
            BenchmarkChunk(
                text=chunk["text"],
                label=chunk.get("title"),
                token_count=_coerce_int(chunk.get("metadata", {}).get("token_count")) or _count_tokens(chunk["text"]),
                metadata=chunk.get("metadata", {}),
            )
            for chunk in chunk_dicts
            if str(chunk.get("text") or "").strip()
        ]
        return CandidateRun(
            candidate_id=candidate_id,
            input_source="output_md",
            extraction_method=extraction_method,
            chunking_method="unstructured_by_title",
            chunks=chunks,
        )

    return _timed_candidate(_runner, candidate_id=candidate_id)


def run_markdown_title_v1(bundle: DocumentBundle) -> CandidateRun:
    def _runner() -> CandidateRun:
        sections = _markdown_sections(bundle)
        chunks = _chunk_markdown_sections(
            sections,
            max_characters=2600,
            new_after_n_chars=1800,
            combine_text_under_n_chars=900,
            overlap_tokens=120,
            cas_mode=False,
        )
        return CandidateRun(
            candidate_id="markdown_title_v1",
            input_source="output_md",
            extraction_method="markdown_ast",
            chunking_method="by_title_fallback",
            chunks=chunks,
        )

    return _timed_candidate(_runner, candidate_id="markdown_title_v1")


def run_markdown_cas_v1(bundle: DocumentBundle) -> CandidateRun:
    def _runner() -> CandidateRun:
        sections = _markdown_sections(bundle)
        chunks = _chunk_markdown_sections(
            sections,
            max_characters=3000,
            new_after_n_chars=2200,
            combine_text_under_n_chars=600,
            overlap_tokens=80,
            cas_mode=True,
        )
        return CandidateRun(
            candidate_id="markdown_cas_v1",
            input_source="output_md",
            extraction_method="markdown_ast",
            chunking_method="markdown_cas",
            chunks=chunks,
        )

    return _timed_candidate(_runner, candidate_id="markdown_cas_v1")


def run_unstructured_pdf_fast_v1(bundle: DocumentBundle) -> CandidateRun:
    def _runner() -> CandidateRun:
        try:
            from unstructured.partition.pdf import partition_pdf
        except Exception as exc:
            return CandidateRun(
                candidate_id="unstructured_pdf_fast_v1",
                input_source="pdf",
                extraction_method="unstructured_fast_pdf",
                chunking_method="worker_current",
                chunks=[],
                skipped=True,
                error=f"Unstructured fast PDF unavailable: {exc}",
            )

        try:
            with _suppress_noisy_pdf_logs(include_unstructured=True):
                elements = partition_pdf(
                    filename=str(bundle.pdf_path),
                    strategy="fast",
                    languages=list(_DEFAULT_UNSTRUCTURED_CAS_LANGUAGES),
                )
            chunks = _extract_candidate_chunks_from_elements(elements, bundle.document_id)
        except Exception as exc:
            return CandidateRun(
                candidate_id="unstructured_pdf_fast_v1",
                input_source="pdf",
                extraction_method="unstructured_fast_pdf",
                chunking_method="worker_current",
                chunks=[],
                failed=True,
                error=str(exc),
            )

        return CandidateRun(
            candidate_id="unstructured_pdf_fast_v1",
            input_source="pdf",
            extraction_method="unstructured_fast_pdf",
            chunking_method="worker_current",
            chunks=chunks,
        )

    return _timed_candidate(_runner, candidate_id="unstructured_pdf_fast_v1")


def run_pymupdf_unstructured_v1(bundle: DocumentBundle) -> CandidateRun:
    def _runner() -> CandidateRun:
        try:
            import pymupdf4llm  # type: ignore
        except Exception as exc:
            return CandidateRun(
                candidate_id="pymupdf_unstructured_v1",
                input_source="pdf",
                extraction_method="pymupdf4llm",
                chunking_method="unstructured_by_title",
                chunks=[],
                skipped=True,
                error=f"PyMuPDF4LLM unavailable: {exc}",
            )

        markdown = _call_quietly(
            pymupdf4llm.to_markdown,
            str(bundle.pdf_path),
            page_chunks=False,
            header=False,
            footer=False,
            ignore_code=True,
            use_ocr=True,
            force_ocr=False,
            ocr_language="eng+fra",
        )
        if isinstance(markdown, list):
            markdown = "\n\n".join(str(item.get("text") or "") if isinstance(item, dict) else str(item) for item in markdown)
        markdown_text = str(markdown or "").strip()
        if not markdown_text:
            raise RuntimeError(f"No markdown extracted from {bundle.pdf_path.name}")

        chunk_dicts, extraction_method = _chunk_markdown_with_unstructured(
            markdown_text,
            max_characters=2600,
            new_after_n_chars=1800,
            combine_text_under_n_chars=900,
            overlap_tokens=120,
        )
        chunks = [
            BenchmarkChunk(
                text=chunk["text"],
                label=chunk.get("title"),
                token_count=_coerce_int(chunk.get("metadata", {}).get("token_count")) or _count_tokens(chunk["text"]),
                metadata=chunk.get("metadata", {}),
            )
            for chunk in chunk_dicts
            if str(chunk.get("text") or "").strip()
        ]
        return CandidateRun(
            candidate_id="pymupdf_unstructured_v1",
            input_source="pdf",
            extraction_method=f"pymupdf4llm->{extraction_method}",
            chunking_method="unstructured_by_title",
            chunks=chunks,
        )

    return _timed_candidate(_runner, candidate_id="pymupdf_unstructured_v1")


def run_docling_cpu_v1(bundle: DocumentBundle) -> CandidateRun:
    def _runner() -> CandidateRun:
        try:
            import tiktoken
            from docling_core.transforms.chunker.tokenizer.openai import OpenAITokenizer
        except Exception as exc:
            return CandidateRun(
                candidate_id="docling_cpu_v1",
                input_source="pdf",
                extraction_method="docling",
                chunking_method="docling_hybrid",
                chunks=[],
                skipped=True,
                error=f"Docling unavailable: {exc}",
            )

        try:
            with _suppress_noisy_pdf_logs():
                with _docling_offline_runtime():
                    converter = _build_docling_converter(enable_remote_services=False)
                    doc = _call_quietly(converter.convert, source=str(bundle.pdf_path)).document
                    chunker = _build_docling_chunker()
                    converted_chunks = []
                    for index, chunk in enumerate(_call_quietly(lambda: list(chunker.chunk(doc)))):
                        text = getattr(chunk, "text", None) or getattr(chunk, "content", None) or str(chunk)
                        if not str(text).strip():
                            continue
                        label = getattr(chunk, "heading", None) or getattr(chunk, "title", None) or f"Chunk {index + 1}"
                        converted_chunks.append(
                            BenchmarkChunk(
                                text=str(text).strip(),
                                label=str(label).strip(),
                                token_count=_count_tokens(str(text)),
                                metadata={"source": "docling"},
                            )
                        )
        except Exception as exc:
            return CandidateRun(
                candidate_id="docling_cpu_v1",
                input_source="pdf",
                extraction_method="docling",
                chunking_method="docling_hybrid",
                chunks=[],
                failed=True,
                error=str(exc),
            )

        return CandidateRun(
            candidate_id="docling_cpu_v1",
            input_source="pdf",
            extraction_method="docling",
            chunking_method="docling_hybrid",
            chunks=converted_chunks,
        )

    return _timed_candidate(_runner, candidate_id="docling_cpu_v1")


def run_docling_cpu_hybrid_v1(bundle: DocumentBundle) -> CandidateRun:
    def _runner() -> CandidateRun:
        try:
            import tiktoken
            from docling_core.transforms.chunker.tokenizer.openai import OpenAITokenizer
        except Exception as exc:
            return CandidateRun(
                candidate_id="docling_cpu_hybrid_v1",
                input_source="pdf",
                extraction_method="docling",
                chunking_method="docling_hybrid",
                chunks=[],
                skipped=True,
                error=f"Docling unavailable: {exc}",
            )

        try:
            with _suppress_noisy_pdf_logs():
                with _docling_offline_runtime():
                    converter = _build_docling_converter(enable_remote_services=False)
                    doc = _call_quietly(converter.convert, source=str(bundle.pdf_path)).document
                    chunker = _build_docling_chunker()
                    converted_chunks = []
                    for index, chunk in enumerate(_call_quietly(lambda: list(chunker.chunk(doc)))):
                        text = getattr(chunk, "text", None) or getattr(chunk, "content", None) or str(chunk)
                        if not str(text).strip():
                            continue
                        label = getattr(chunk, "heading", None) or getattr(chunk, "title", None) or f"Chunk {index + 1}"
                        converted_chunks.append(
                            BenchmarkChunk(
                                text=str(text).strip(),
                                label=str(label).strip(),
                                token_count=_count_tokens(str(text)),
                                metadata={"source": "docling_hybrid"},
                            )
                        )
        except Exception as exc:
            return CandidateRun(
                candidate_id="docling_cpu_hybrid_v1",
                input_source="pdf",
                extraction_method="docling",
                chunking_method="docling_hybrid",
                chunks=[],
                failed=True,
                error=str(exc),
            )

        return CandidateRun(
            candidate_id="docling_cpu_hybrid_v1",
            input_source="pdf",
            extraction_method="docling",
            chunking_method="docling_hybrid",
            chunks=converted_chunks,
        )

    return _timed_candidate(_runner, candidate_id="docling_cpu_hybrid_v1")


def _build_docling_converter(enable_remote_services: bool):
    from docling.datamodel.base_models import InputFormat
    from docling.datamodel.pipeline_options import PdfPipelineOptions
    from docling.document_converter import DocumentConverter, PdfFormatOption

    pipeline_kwargs: dict[str, Any] = {"enable_remote_services": enable_remote_services}
    if DEFAULT_DOCLING_ARTIFACTS_PATH:
        artifacts_path = Path(DEFAULT_DOCLING_ARTIFACTS_PATH)
        artifacts_path.mkdir(parents=True, exist_ok=True)
        pipeline_kwargs["artifacts_path"] = str(artifacts_path)
    pipeline_options = PdfPipelineOptions(**pipeline_kwargs)
    return DocumentConverter(
        format_options={
            InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_options)
        }
    )


def _build_docling_chunker():
    import tiktoken
    from docling.chunking import HybridChunker
    from docling_core.transforms.chunker.tokenizer.openai import OpenAITokenizer

    tokenizer = OpenAITokenizer(
        tokenizer=tiktoken.get_encoding("cl100k_base"),
        max_tokens=768,
    )
    return HybridChunker(tokenizer=tokenizer)


def run_unstructured_hires_subset_v1(bundle: DocumentBundle) -> CandidateRun:
    def _runner() -> CandidateRun:
        if not _is_difficult_bundle(bundle):
            return CandidateRun(
                candidate_id="unstructured_hires_subset_v1",
                input_source="pdf",
                extraction_method="unstructured_hi_res",
                chunking_method="worker_current",
                chunks=[],
                skipped=True,
                error="Candidate only runs on difficult-file subset",
            )

        try:
            from unstructured.partition.pdf import partition_pdf
        except Exception as exc:
            return CandidateRun(
                candidate_id="unstructured_hires_subset_v1",
                input_source="pdf",
                extraction_method="unstructured_hi_res",
                chunking_method="worker_current",
                chunks=[],
                skipped=True,
                error=f"Unstructured hi_res unavailable: {exc}",
            )

        try:
            with _suppress_noisy_pdf_logs(include_unstructured=True):
                elements = partition_pdf(
                    filename=str(bundle.pdf_path),
                    strategy="hi_res",
                    languages=list(_DEFAULT_UNSTRUCTURED_CAS_LANGUAGES),
                )
            chunk_dicts = _extract_candidate_chunks_from_elements(elements, bundle.document_id)
        except Exception as exc:
            return CandidateRun(
                candidate_id="unstructured_hires_subset_v1",
                input_source="pdf",
                extraction_method="unstructured_hi_res",
                chunking_method="worker_current",
                chunks=[],
                failed=True,
                error=str(exc),
            )
        return CandidateRun(
            candidate_id="unstructured_hires_subset_v1",
            input_source="pdf",
            extraction_method="unstructured_hi_res",
            chunking_method="worker_current",
            chunks=chunk_dicts,
        )

    return _timed_candidate(_runner, candidate_id="unstructured_hires_subset_v1")


def run_marker_cpu_subset_v1(bundle: DocumentBundle) -> CandidateRun:
    def _runner() -> CandidateRun:
        if not _is_difficult_bundle(bundle):
            return CandidateRun(
                candidate_id="marker_cpu_subset_v1",
                input_source="pdf",
                extraction_method="marker",
                chunking_method="marker_json",
                chunks=[],
                skipped=True,
                error="Candidate only runs on difficult-file subset",
            )

        return CandidateRun(
            candidate_id="marker_cpu_subset_v1",
            input_source="pdf",
            extraction_method="marker",
            chunking_method="marker_json",
            chunks=[],
            skipped=True,
            error="Marker CPU adapter is a benchmark placeholder until marker is installed locally",
        )

    return _timed_candidate(_runner, candidate_id="marker_cpu_subset_v1")


def analyze_candidate_run(
    bundle: DocumentBundle,
    inventory_row: InventoryRow,
    run: CandidateRun,
    document_queries: Sequence[BenchmarkQuery],
    spec: CandidateSpec,
) -> PerDocumentResult:
    chunk_dicts = [chunk.to_dict() for chunk in run.chunks]
    token_values = [chunk.token_count for chunk in run.chunks]
    token_stats = _summarize_numbers(token_values)
    structure_metrics = evaluate_structure_metrics(bundle, run.chunks)
    retrieval_metrics = evaluate_retrieval_metrics(run.chunks, document_queries)
    total_tokens = sum(token_values)
    return PerDocumentResult(
        candidate_id=run.candidate_id,
        lane=spec.lane,
        document_id=bundle.document_id,
        input_source=run.input_source,
        extraction_method=run.extraction_method,
        chunking_method=run.chunking_method,
        self_hosted_status=spec.self_hosted_status,
        requires_remote_service=spec.requires_remote_service,
        ocr_languages=list(spec.ocr_languages),
        chunk_count=len(run.chunks),
        total_tokens=total_tokens,
        token_stats=token_stats,
        structure_metrics=structure_metrics,
        retrieval_metrics=retrieval_metrics,
        resource_metrics={
            "processing_time_ms": run.processing_time_ms,
            "rss_mb": run.rss_mb,
            "page_count": inventory_row.page_count,
            "pdf_size_bytes": inventory_row.pdf_size_bytes,
        },
        warnings=run.warnings,
        failed=run.failed,
        skipped=run.skipped,
        error=run.error,
        chunks=chunk_dicts,
    )


def evaluate_structure_metrics(
    bundle: DocumentBundle,
    chunks: Sequence[BenchmarkChunk],
) -> dict[str, Any]:
    if not chunks:
        return {
            "heading_recall": 0.0,
            "section_boundary_recall": 0.0,
            "tiny_chunk_ratio": 0.0,
            "oversized_chunk_ratio": 0.0,
            "sentence_fracture_rate": 1.0,
            "repeated_header_ratio": 0.0,
            "numbered_paragraph_integrity": 0.0,
            "structure_score": 0.0,
        }

    canonical = load_canonical_document(bundle)
    reference_sections = _merge_numbered_markers(_canonical_sections(canonical))
    canonical_labels = [section["label"] for section in reference_sections if section.get("label")]
    canonical_headings = [label for label in canonical_labels if not NUMERIC_MARKER_RE.match(label)]
    chunk_labels = [chunk.label or "" for chunk in chunks]
    chunk_starts = [chunk.text[:200] for chunk in chunks]

    heading_hits = sum(
        1
        for label in canonical_headings
        if _label_recovered(label, chunk_labels, chunk_starts)
    )
    boundary_hits = sum(
        1
        for label in canonical_labels
        if _label_recovered(label, chunk_labels, chunk_starts)
    )
    tiny_chunk_ratio = _ratio(sum(1 for chunk in chunks if chunk.token_count < 120), len(chunks))
    oversized_chunk_ratio = _ratio(sum(1 for chunk in chunks if chunk.token_count > 900), len(chunks))
    sentence_fracture_rate = _ratio(
        sum(1 for chunk in chunks if _appears_sentence_fractured(chunk.text)),
        len(chunks),
    )
    repeated_header_ratio = _repeated_header_ratio(chunks)
    numbered_integrity = _numbered_paragraph_integrity(chunks)
    heading_recall = _ratio(heading_hits, len(canonical_headings)) if canonical_headings else 1.0
    boundary_recall = _ratio(boundary_hits, len(canonical_labels)) if canonical_labels else 1.0
    structure_score = round(
        (
            heading_recall * 0.25
            + boundary_recall * 0.25
            + numbered_integrity * 0.2
            + (1.0 - sentence_fracture_rate) * 0.15
            + (1.0 - repeated_header_ratio) * 0.1
            + (1.0 - min(1.0, tiny_chunk_ratio + oversized_chunk_ratio)) * 0.05
        ),
        4,
    )
    return {
        "heading_recall": round(heading_recall, 4),
        "section_boundary_recall": round(boundary_recall, 4),
        "tiny_chunk_ratio": round(tiny_chunk_ratio, 4),
        "oversized_chunk_ratio": round(oversized_chunk_ratio, 4),
        "sentence_fracture_rate": round(sentence_fracture_rate, 4),
        "repeated_header_ratio": round(repeated_header_ratio, 4),
        "numbered_paragraph_integrity": round(numbered_integrity, 4),
        "structure_score": structure_score,
    }


def build_document_queries(bundle: DocumentBundle) -> list[BenchmarkQuery]:
    canonical = load_canonical_document(bundle)
    metadata = canonical.get("metadata", {}) if canonical else {}
    sections = _merge_numbered_markers(_canonical_sections(canonical))
    substantial = [section for section in sections if len(section.get("text", "")) >= 80]
    queries: list[BenchmarkQuery] = []
    if not substantial:
        return queries

    thesis_key = _coerce_text(metadata.get("thesis_key")) or bundle.document_id
    sport = _first_subject(metadata) or "sport"
    keywords = metadata.get("extra", {}).get("keywords", []) if isinstance(metadata.get("extra"), dict) else []
    keyword_terms = " ".join(str(keyword) for keyword in keywords[:2])

    intro = substantial[0]
    queries.append(
        BenchmarkQuery(
            query_id=f"{bundle.document_id}-case",
            query_type="case_identification",
            text=f"{thesis_key} {sport} {keyword_terms}".strip(),
            target_label=intro.get("label"),
            target_anchor=_anchor_from_text(intro.get("text", "")),
        )
    )

    heading_section = next((section for section in substantial if section.get("type") == "heading"), None)
    if heading_section:
        queries.append(
            BenchmarkQuery(
                query_id=f"{bundle.document_id}-section",
                query_type="section_lookup",
                text=heading_section.get("label") or heading_section.get("text", "")[:80],
                target_label=heading_section.get("label"),
                target_anchor=_anchor_from_text(heading_section.get("text", "")),
            )
        )

    decision = _pick_decision_section(substantial)
    if decision:
        text_terms = _salient_query_terms(decision.get("text", ""))
        queries.append(
            BenchmarkQuery(
                query_id=f"{bundle.document_id}-decision",
                query_type="decision_holding",
                text=text_terms,
                target_label=decision.get("label"),
                target_anchor=_anchor_from_text(decision.get("text", "")),
            )
        )
    return queries


def evaluate_retrieval_metrics(
    chunks: Sequence[BenchmarkChunk],
    queries: Sequence[BenchmarkQuery],
) -> dict[str, Any]:
    if not chunks or not queries:
        return {
            "query_count": 0,
            "recall_at_1": 0.0,
            "recall_at_3": 0.0,
            "recall_at_5": 0.0,
            "mrr_at_5": 0.0,
        }

    bm25 = _SimpleBM25([chunk.text for chunk in chunks])
    hits_at_1 = hits_at_3 = hits_at_5 = 0
    reciprocal_rank = 0.0

    for query in queries:
        scores = bm25.score(query.text)
        ranked_indices = sorted(range(len(chunks)), key=lambda index: scores[index], reverse=True)
        relevant = {
            index
            for index, chunk in enumerate(chunks)
            if _chunk_matches_query_target(chunk, query)
        }
        if not relevant:
            continue
        top_1 = set(ranked_indices[:1])
        top_3 = set(ranked_indices[:3])
        top_5 = set(ranked_indices[:5])
        hits_at_1 += int(bool(relevant & top_1))
        hits_at_3 += int(bool(relevant & top_3))
        hits_at_5 += int(bool(relevant & top_5))
        for rank, index in enumerate(ranked_indices[:5], start=1):
            if index in relevant:
                reciprocal_rank += 1.0 / rank
                break

    query_count = len(queries)
    return {
        "query_count": query_count,
        "recall_at_1": round(_ratio(hits_at_1, query_count), 4),
        "recall_at_3": round(_ratio(hits_at_3, query_count), 4),
        "recall_at_5": round(_ratio(hits_at_5, query_count), 4),
        "mrr_at_5": round(reciprocal_rank / query_count, 4),
    }


def aggregate_candidate_results(
    candidate_id: str,
    results: Sequence[PerDocumentResult],
    corpus_size: int,
    spec: CandidateSpec,
) -> AggregateReport:
    documents_run = len(results)
    non_skipped = [result for result in results if not result.skipped]
    successful = [result for result in non_skipped if not result.failed]
    success_rate = _ratio(len(successful), len(non_skipped)) if non_skipped else 0.0
    structure_metrics = _average_metric_dict(
        [result.structure_metrics for result in successful],
        ("heading_recall", "section_boundary_recall", "tiny_chunk_ratio", "oversized_chunk_ratio", "sentence_fracture_rate", "repeated_header_ratio", "numbered_paragraph_integrity", "structure_score"),
    )
    retrieval_metrics = _average_metric_dict(
        [result.retrieval_metrics for result in successful],
        ("recall_at_1", "recall_at_3", "recall_at_5", "mrr_at_5"),
    )
    ops_metrics = _average_metric_dict(
        [result.resource_metrics for result in successful],
        ("processing_time_ms", "rss_mb", "page_count", "pdf_size_bytes"),
    )
    projected_hours = None
    if ops_metrics.get("processing_time_ms"):
        projected_hours = round((ops_metrics["processing_time_ms"] * corpus_size) / 3_600_000, 3)

    weighted_score = 0.0
    if successful:
        weighted_score = round(
            (
                structure_metrics.get("structure_score", 0.0) * 0.45
                + retrieval_metrics.get("recall_at_5", 0.0) * 0.2
                + retrieval_metrics.get("recall_at_3", 0.0) * 0.1
                + retrieval_metrics.get("mrr_at_5", 0.0) * 0.05
                + (1.0 - min(1.0, (ops_metrics.get("processing_time_ms", 0.0) or 0.0) / 5_000.0)) * 0.2
            ),
            4,
        )

    decision_status = "candidate"
    if not successful:
        decision_status = "blocked"
    elif not _within_runtime_gate(spec.lane, projected_hours):
        decision_status = "over_budget"
    elif (
        success_rate == 1.0
        and structure_metrics.get("structure_score", 0.0) >= 0.7
        and retrieval_metrics.get("recall_at_5", 0.0) >= 0.8
    ):
        decision_status = "finalist"

    return AggregateReport(
        candidate_id=candidate_id,
        lane=spec.lane,
        documents_run=documents_run,
        success_rate=round(success_rate, 4),
        self_hosted_status=spec.self_hosted_status,
        requires_remote_service=spec.requires_remote_service,
        ocr_languages=list(spec.ocr_languages),
        manual_scores={},
        structure_metrics=structure_metrics,
        retrieval_metrics=retrieval_metrics,
        ops_metrics=ops_metrics,
        projected_full_corpus_runtime_hours=projected_hours,
        decision_status=decision_status,
        weighted_score=weighted_score,
        skipped_documents=sum(1 for result in results if result.skipped),
        failed_documents=sum(1 for result in results if result.failed),
    )


def rank_aggregates(aggregates: Sequence[AggregateReport]) -> list[AggregateReport]:
    for aggregate in aggregates:
        _normalize_runtime_status(aggregate)
    ranked = sorted(
        aggregates,
        key=lambda aggregate: (
            _winner_eligible(aggregate),
            aggregate.decision_status != "blocked",
            aggregate.success_rate,
            aggregate.weighted_score,
            aggregate.structure_metrics.get("structure_score", 0.0),
            aggregate.retrieval_metrics.get("recall_at_5", 0.0),
            -aggregate.ops_metrics.get("processing_time_ms", float("inf")),
        ),
        reverse=True,
    )
    winners_by_lane: set[str] = set()
    for aggregate in ranked:
        if not _winner_eligible(aggregate):
            continue
        if aggregate.lane in winners_by_lane:
            continue
        aggregate.decision_status = "winner"
        winners_by_lane.add(aggregate.lane)
    return ranked


def render_summary_markdown(
    ranked: Sequence[AggregateReport],
    sample_sets: dict[str, list[str]],
    inventory: Sequence[InventoryRow],
    *,
    mode: str,
    difficult_sets: dict[str, list[str]] | None = None,
) -> str:
    lines = [
        "# CAS/TAS Chunk Benchmark Summary",
        "",
        f"- Corpus inventory rows: {len(inventory)}",
        f"- Mode: `{mode}`",
        f"- Runtime gates: artifact_chunking <= {CAS_RUNTIME_GATES_HOURS['artifact_chunking']}h, raw_pdf_extraction <= {CAS_RUNTIME_GATES_HOURS['raw_pdf_extraction']}h",
        f"- Artifact smoke sample: {len(sample_sets.get('smoke', [])) if mode != 'raw_pdf_extraction' else 0}",
        f"- Artifact tuning sample: {len(sample_sets.get('tuning', [])) if mode != 'raw_pdf_extraction' else 0}",
        f"- Artifact holdout sample: {len(sample_sets.get('holdout', [])) if mode != 'raw_pdf_extraction' else 0}",
        f"- Raw-PDF smoke sample: {len((difficult_sets or {}).get('smoke', [])) if mode == 'full_decision' else (len(sample_sets.get('smoke', [])) if mode == 'raw_pdf_extraction' else 0)}",
        f"- Raw-PDF tuning sample: {len((difficult_sets or {}).get('tuning', [])) if mode == 'full_decision' else (len(sample_sets.get('tuning', [])) if mode == 'raw_pdf_extraction' else 0)}",
        f"- Raw-PDF holdout sample: {len((difficult_sets or {}).get('holdout', [])) if mode == 'full_decision' else (len(sample_sets.get('holdout', [])) if mode == 'raw_pdf_extraction' else 0)}",
        "",
        "| Candidate | Lane | Status | Success | Structure | Recall@5 | Score | Avg ms/doc | Projected hours |",
        "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for aggregate in ranked:
        lines.append(
            "| {candidate} | {lane} | {status} | {success:.2f} | {structure:.2f} | {recall:.2f} | {score:.2f} | {time:.0f} | {hours} |".format(
                candidate=aggregate.candidate_id,
                lane=aggregate.lane,
                status=aggregate.decision_status,
                success=aggregate.success_rate,
                structure=aggregate.structure_metrics.get("structure_score", 0.0),
                recall=aggregate.retrieval_metrics.get("recall_at_5", 0.0),
                score=aggregate.weighted_score,
                time=aggregate.ops_metrics.get("processing_time_ms", 0.0),
                hours=aggregate.projected_full_corpus_runtime_hours if aggregate.projected_full_corpus_runtime_hours is not None else "n/a",
            )
        )
    return "\n".join(lines) + "\n"


def render_holdout_review(
    holdout_ids: Sequence[str],
    bundle_map: dict[str, DocumentBundle],
    results_by_candidate: dict[str, list[PerDocumentResult]],
    top_aggregates: Sequence[AggregateReport],
) -> str:
    lines = [
        "# Holdout Review Sheet",
        "",
        "Use this sheet for manual review of the top candidates on the holdout set.",
        "",
    ]
    top_ids = {aggregate.candidate_id for aggregate in top_aggregates}
    results_index = {
        (result.candidate_id, result.document_id): result
        for results in results_by_candidate.values()
        for result in results
        if result.candidate_id in top_ids
    }
    for document_id in holdout_ids:
        bundle = bundle_map[document_id]
        lines.extend([f"## {document_id}", f"- PDF: `{bundle.pdf_path}`", ""])
        for candidate_id in top_ids:
            result = results_index.get((candidate_id, document_id))
            if result is None:
                continue
            lines.append(f"### {candidate_id}")
            lines.append(
                f"- Structure score: {result.structure_metrics.get('structure_score', 0.0):.2f}"
            )
            lines.append(f"- Recall@5: {result.retrieval_metrics.get('recall_at_5', 0.0):.2f}")
            preview = result.chunks[0]["text"][:500] if result.chunks else ""
            lines.append(f"- First chunk preview: `{preview}`")
            lines.append("")
    return "\n".join(lines) + "\n"


def load_canonical_document(bundle: DocumentBundle) -> dict[str, Any]:
    if not bundle.canonical_json_path or not bundle.canonical_json_path.exists():
        return {}
    return json.loads(bundle.canonical_json_path.read_text(encoding="utf-8"))


def _canonical_sections(canonical: dict[str, Any]) -> list[dict[str, Any]]:
    sections = canonical.get("sections", []) if isinstance(canonical, dict) else []
    normalized = []
    for section in sections:
        if not isinstance(section, dict):
            continue
        text = _coerce_text(section.get("text"))
        if not text:
            continue
        label = section.get("heading_path", [])[-1] if section.get("heading_path") else _coerce_text(section.get("text"))
        pages = [
            block.get("page")
            for block in section.get("blocks", [])
            if isinstance(block, dict) and block.get("page") is not None
        ]
        normalized.append(
            {
                "label": label or text[:80],
                "text": text,
                "type": _coerce_text(section.get("type")) or "paragraph",
                "heading_path": list(section.get("heading_path", [])),
                "page_start": min(pages) if pages else None,
                "page_end": max(pages) if pages else None,
            }
        )
    return normalized


def _markdown_sections(bundle: DocumentBundle) -> list[dict[str, Any]]:
    body = _read_text(bundle.output_md_path)
    if not body:
        return []
    frontmatter, content = _split_frontmatter(body)
    current_heading_path: list[str] = []
    current_level = 0
    current_lines: list[str] = []
    sections: list[dict[str, Any]] = []

    def flush_paragraph() -> None:
        nonlocal current_lines
        text = "\n".join(line for line in current_lines if line.strip()).strip()
        if text:
            label = current_heading_path[-1] if current_heading_path else (frontmatter.get("title") or bundle.document_id)
            sections.append(
                {
                    "label": label,
                    "text": text,
                    "type": "paragraph",
                    "heading_path": list(current_heading_path),
                    "level": current_level or (len(current_heading_path) or 0),
                }
            )
        current_lines = []

    for raw_line in content.splitlines():
        line = raw_line.rstrip()
        if line.startswith("#"):
            flush_paragraph()
            level = len(line) - len(line.lstrip("#"))
            heading_text = line[level:].strip()
            while len(current_heading_path) >= level:
                current_heading_path.pop()
            current_heading_path.append(heading_text)
            current_level = level
            sections.append(
                {
                    "label": heading_text,
                    "text": heading_text,
                    "type": "heading",
                    "heading_path": list(current_heading_path),
                    "level": level,
                }
            )
            continue
        if not line.strip():
            flush_paragraph()
            continue
        current_lines.append(line)

    flush_paragraph()
    return _merge_numbered_markers(sections)


def _chunk_markdown_with_unstructured(
    markdown: str,
    *,
    max_characters: int,
    new_after_n_chars: int,
    combine_text_under_n_chars: int,
    overlap_tokens: int,
) -> tuple[list[dict[str, Any]], str]:
    try:
        from unstructured.chunking.title import chunk_by_title
        from unstructured.partition.md import partition_md
    except Exception:
        sections = _markdown_sections_from_text(markdown)
        fallback_chunks = _chunk_markdown_sections(
            sections,
            max_characters=max_characters,
            new_after_n_chars=new_after_n_chars,
            combine_text_under_n_chars=combine_text_under_n_chars,
            overlap_tokens=overlap_tokens,
            cas_mode=False,
        )
        return (
            [
                {
                    "text": chunk.text,
                    "title": chunk.label,
                    "metadata": dict(chunk.metadata),
                }
                for chunk in fallback_chunks
            ],
            "markdown_ast_fallback",
        )

    elements = partition_md(text=markdown)
    composites = chunk_by_title(
        elements,
        max_characters=max_characters,
        new_after_n_chars=new_after_n_chars,
        combine_text_under_n_chars=combine_text_under_n_chars,
        overlap=overlap_tokens,
        overlap_all=False,
        multipage_sections=True,
        include_orig_elements=True,
    )
    chunks: list[dict[str, Any]] = []
    for element in composites:
        text = str(getattr(element, "text", "") or "").strip()
        if not text:
            continue
        metadata = getattr(element, "metadata", None)
        heading = None
        if metadata is not None:
            heading = getattr(metadata, "section", None) or getattr(metadata, "filename", None)
        chunks.append(
            {
                "text": text,
                "title": heading or text.splitlines()[0][:180],
                "metadata": {
                    "heading": heading,
                    "token_count": _count_tokens(text),
                    "source": "output_md",
                },
            }
        )
    return chunks, "unstructured_md"


def _markdown_sections_from_text(markdown: str) -> list[dict[str, Any]]:
    temp_bundle = DocumentBundle(
        document_id="inline",
        pdf_path=Path("/tmp/inline.pdf"),
        converted_dir=Path("/tmp"),
        canonical_json_path=None,
        output_md_path=None,
        output_txt_path=None,
        meta_json_path=None,
    )
    frontmatter, content = _split_frontmatter(markdown)
    current_heading_path: list[str] = []
    current_level = 0
    current_lines: list[str] = []
    sections: list[dict[str, Any]] = []

    def flush_paragraph() -> None:
        nonlocal current_lines
        text = "\n".join(line for line in current_lines if line.strip()).strip()
        if text:
            label = current_heading_path[-1] if current_heading_path else (frontmatter.get("title") or temp_bundle.document_id)
            sections.append(
                {
                    "label": label,
                    "text": text,
                    "type": "paragraph",
                    "heading_path": list(current_heading_path),
                    "level": current_level or (len(current_heading_path) or 0),
                }
            )
        current_lines = []

    for raw_line in content.splitlines():
        line = raw_line.rstrip()
        if line.startswith("#"):
            flush_paragraph()
            level = len(line) - len(line.lstrip("#"))
            heading_text = line[level:].strip()
            while len(current_heading_path) >= level:
                current_heading_path.pop()
            current_heading_path.append(heading_text)
            current_level = level
            sections.append(
                {
                    "label": heading_text,
                    "text": heading_text,
                    "type": "heading",
                    "heading_path": list(current_heading_path),
                    "level": level,
                }
            )
            continue
        if not line.strip():
            flush_paragraph()
            continue
        current_lines.append(line)

    flush_paragraph()
    return _merge_numbered_markers(sections)


def _chunk_markdown_sections(
    sections: Sequence[dict[str, Any]],
    *,
    max_characters: int,
    new_after_n_chars: int,
    combine_text_under_n_chars: int,
    overlap_tokens: int,
    cas_mode: bool,
) -> list[BenchmarkChunk]:
    chunks: list[BenchmarkChunk] = []
    buffer: list[dict[str, Any]] = []
    buffer_chars = 0

    def flush() -> None:
        nonlocal buffer, buffer_chars
        if not buffer:
            return
        text = "\n\n".join(section["text"] for section in buffer if section.get("text")).strip()
        if not text:
            buffer = []
            buffer_chars = 0
            return
        label = buffer[0]["label"] if len(buffer) == 1 else f"{buffer[0]['label']} – {buffer[-1]['label']}"
        chunks.extend(
            _split_with_overlap(
                text,
                label=label,
                metadata={
                    "heading_path": buffer[-1].get("heading_path", []),
                    "section_types": sorted({section.get("type", "paragraph") for section in buffer}),
                    "source": "output_md",
                },
                target_max_tokens=max(450, new_after_n_chars // 4),
                max_chunk_tokens=max(650, max_characters // 4),
                overlap_tokens=overlap_tokens,
            )
        )
        buffer = []
        buffer_chars = 0

    for section in sections:
        text = section.get("text", "").strip()
        if not text:
            continue
        is_heading = section.get("type") == "heading"
        is_cas_boundary = cas_mode and _looks_like_cas_boundary(section.get("label", ""))
        if is_heading and buffer and (buffer_chars >= combine_text_under_n_chars or is_cas_boundary):
            flush()
        candidate_chars = buffer_chars + len(text)
        if buffer and candidate_chars > max_characters:
            flush()
        buffer.append(section)
        buffer_chars += len(text)
        if buffer_chars >= new_after_n_chars and not is_heading:
            flush()
    flush()
    return chunks


def _chunk_structured_sections(
    sections: Sequence[dict[str, Any]],
    *,
    target_min_tokens: int,
    target_max_tokens: int,
    max_chunk_tokens: int,
    overlap_tokens: int,
    input_source: str,
    chunking_method: str,
) -> list[BenchmarkChunk]:
    chunks: list[BenchmarkChunk] = []
    buffer: list[dict[str, Any]] = []
    buffer_tokens = 0

    def flush() -> None:
        nonlocal buffer, buffer_tokens
        if not buffer:
            return
        text = "\n\n".join(section["text"] for section in buffer if section.get("text")).strip()
        label = buffer[0]["label"] if len(buffer) == 1 else f"{buffer[0]['label']} – {buffer[-1]['label']}"
        pages = [section.get("page_start") for section in buffer if section.get("page_start") is not None]
        pages += [section.get("page_end") for section in buffer if section.get("page_end") is not None]
        chunks.extend(
            _split_with_overlap(
                text,
                label=label,
                metadata={
                    "heading_path": buffer[-1].get("heading_path", []),
                    "section_types": sorted({section.get("type", "paragraph") for section in buffer}),
                    "page_start": min(pages) if pages else None,
                    "page_end": max(pages) if pages else None,
                    "source": input_source,
                },
                target_max_tokens=target_max_tokens,
                max_chunk_tokens=max_chunk_tokens,
                overlap_tokens=overlap_tokens,
            )
        )
        buffer = []
        buffer_tokens = 0

    for section in sections:
        token_count = _count_tokens(section.get("text", ""))
        if token_count > max_chunk_tokens:
            flush()
            chunks.extend(
                _split_with_overlap(
                    section.get("text", ""),
                    label=section.get("label"),
                    metadata={
                        "heading_path": section.get("heading_path", []),
                        "section_types": [section.get("type", "paragraph")],
                        "page_start": section.get("page_start"),
                        "page_end": section.get("page_end"),
                        "source": input_source,
                    },
                    target_max_tokens=target_max_tokens,
                    max_chunk_tokens=max_chunk_tokens,
                    overlap_tokens=overlap_tokens,
                )
            )
            continue
        if buffer and buffer_tokens + token_count > target_max_tokens:
            flush()
        buffer.append(section)
        buffer_tokens += token_count
        if buffer_tokens >= target_min_tokens:
            flush()
    flush()
    return chunks


def _timed_candidate(
    runner: Callable[[], CandidateRun],
    *,
    candidate_id: str | None = None,
) -> CandidateRun:
    start = time.perf_counter()
    rss_before = _current_rss_mb()
    try:
        result = runner()
    except Exception as exc:
        result = CandidateRun(
            candidate_id=candidate_id or "unknown",
            input_source="unknown",
            extraction_method="unknown",
            chunking_method="unknown",
            chunks=[],
            failed=True,
            error=str(exc),
        )
    result.processing_time_ms = int((time.perf_counter() - start) * 1000)
    rss_after = _current_rss_mb()
    if rss_before is not None and rss_after is not None:
        result.rss_mb = round(max(rss_after, rss_before), 2)
    return result


def _failed_candidate_run(
    candidate_id: str,
    error: Exception | str,
    *,
    input_source: str = "unknown",
    extraction_method: str = "unknown",
    chunking_method: str = "unknown",
) -> CandidateRun:
    return CandidateRun(
        candidate_id=candidate_id,
        input_source=input_source,
        extraction_method=extraction_method,
        chunking_method=chunking_method,
        chunks=[],
        failed=True,
        error=str(error),
    )


def _detect_cas_structure_nodes(text: str) -> list[dict[str, Any]]:
    lines = text.splitlines()
    offsets = _line_offsets(lines)
    sections: list[tuple[str, str, int]] = []
    for index, line in enumerate(lines):
        stripped = line.strip()
        if not stripped:
            continue
        if CAS_FACTS_RE.match(stripped):
            sections.append(("cas_hechos", stripped, index))
        elif CAS_REASONS_RE.match(stripped):
            sections.append(("cas_fundamentos", stripped, index))
        elif CAS_DECISION_RE.match(stripped):
            sections.append(("cas_dispositivo", stripped, index))
    if len(sections) < 2:
        return [
            {
                "type": "paragraph",
                "label": f"Párrafo {index + 1}",
                "text": paragraph,
                "estimated_tokens": _count_tokens(paragraph),
            }
            for index, paragraph in enumerate(_paragraph_split(text))
        ]
    nodes: list[dict[str, Any]] = []
    first_start = offsets[sections[0][2]]
    if first_start > 50:
        preamble = text[:first_start].strip()
        if preamble:
            nodes.append(
                {
                    "type": "titulo_ley",
                    "label": "Preámbulo",
                    "text": preamble,
                    "estimated_tokens": _count_tokens(preamble),
                }
            )
    for index, (node_type, label, line_start) in enumerate(sections):
        start_offset = offsets[line_start]
        end_offset = offsets[sections[index + 1][2]] if index + 1 < len(sections) else len(text)
        node_text = text[start_offset:end_offset].strip()
        nodes.append(
            {
                "type": node_type,
                "label": label,
                "text": node_text,
                "estimated_tokens": _count_tokens(node_text),
            }
        )
    return nodes


def _group_nodes_into_chunks(
    nodes: Sequence[dict[str, Any]],
    *,
    target_min_tokens: int,
    target_max_tokens: int,
    max_chunk_tokens: int,
) -> list[dict[str, Any]]:
    chunks: list[dict[str, Any]] = []
    buffer: list[dict[str, Any]] = []
    buffer_tokens = 0

    def flush() -> None:
        nonlocal buffer, buffer_tokens
        if not buffer:
            return
        text = "\n\n".join(node["text"] for node in buffer).strip()
        label = buffer[0]["label"] if len(buffer) == 1 else f"{buffer[0]['label']} – {buffer[-1]['label']}"
        chunks.append(
            {
                "text": text,
                "label": label,
                "token_count": _count_tokens(text),
                "node_types": sorted({node["type"] for node in buffer}),
            }
        )
        buffer = []
        buffer_tokens = 0

    for node in nodes:
        if node["estimated_tokens"] > max_chunk_tokens:
            flush()
            chunks.extend(
                {
                    "text": part.text,
                    "label": part.label,
                    "token_count": part.token_count,
                    "node_types": [node["type"]],
                }
                for part in _split_with_overlap(
                    node["text"],
                    label=node["label"],
                    metadata={},
                    target_max_tokens=target_max_tokens,
                    max_chunk_tokens=max_chunk_tokens,
                    overlap_tokens=0,
                )
            )
            continue
        if buffer and buffer_tokens + node["estimated_tokens"] > target_max_tokens:
            flush()
        buffer.append(node)
        buffer_tokens += node["estimated_tokens"]
        if buffer_tokens >= target_min_tokens:
            flush()
    flush()
    return chunks


def _split_with_overlap(
    text: str,
    *,
    label: str | None,
    metadata: dict[str, Any],
    target_max_tokens: int,
    max_chunk_tokens: int,
    overlap_tokens: int,
) -> list[BenchmarkChunk]:
    if not text.strip():
        return []
    sentences = [piece.strip() for piece in SENTENCE_BOUNDARY_RE.split(text.strip()) if piece.strip()]
    if not sentences:
        sentences = [text.strip()]
    chunks: list[BenchmarkChunk] = []
    current: list[str] = []
    current_tokens = 0

    def flush() -> None:
        nonlocal current, current_tokens
        if not current:
            return
        chunk_text = " ".join(current).strip()
        chunks.append(
            BenchmarkChunk(
                text=chunk_text,
                label=label,
                token_count=_count_tokens(chunk_text),
                metadata=dict(metadata),
            )
        )
        if overlap_tokens > 0:
            current = _tail_for_overlap(current, overlap_tokens)
            current_tokens = _count_tokens(" ".join(current)) if current else 0
        else:
            current = []
            current_tokens = 0

    for sentence in sentences:
        sentence_tokens = _count_tokens(sentence)
        if current and current_tokens + sentence_tokens > target_max_tokens:
            flush()
        if sentence_tokens > max_chunk_tokens:
            for piece in _hard_wrap_sentence(sentence, max_chunk_tokens):
                piece_text = piece.strip()
                if not piece_text:
                    continue
                current.append(piece_text)
                current_tokens += _count_tokens(piece_text)
                flush()
            continue
        current.append(sentence)
        current_tokens += sentence_tokens
    flush()
    return chunks


def _extract_candidate_chunks_from_elements(elements: Sequence[Any], fallback_title: str) -> list[BenchmarkChunk]:
    chunks = []
    for chunk in _extract_document_chunks_from_elements(elements, fallback_title):
        text = chunk.get("text", "").strip()
        if not text:
            continue
        chunks.append(
            BenchmarkChunk(
                text=text,
                label=chunk.get("title"),
                token_count=_coerce_int(chunk.get("metadata", {}).get("token_count")) or _count_tokens(text),
                metadata=chunk.get("metadata", {}),
            )
        )
    return chunks


def _extract_document_chunks_from_elements(elements: Sequence[Any], fallback_title: str) -> list[dict[str, Any]]:
    # Reuse the current worker logic through a private function boundary.
    from src.gui.infrastructure.scraper_document_workflow import _chunk_unstructured_elements

    return _chunk_unstructured_elements(list(elements), fallback_title)


def _merge_numbered_markers(sections: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    skip_next = False
    for index, section in enumerate(sections):
        if skip_next:
            skip_next = False
            continue
        text = section.get("text", "").strip()
        if NUMERIC_MARKER_RE.match(text) and index + 1 < len(sections):
            next_section = dict(sections[index + 1])
            next_text = next_section.get("text", "").strip()
            combined = f"{text} {next_text}".strip()
            next_section["text"] = combined
            next_section["label"] = next_section.get("label") or section.get("label") or text
            merged.append(next_section)
            skip_next = True
            continue
        merged.append(dict(section))
    return merged


def _pick_decision_section(sections: Sequence[dict[str, Any]]) -> dict[str, Any] | None:
    for section in reversed(sections):
        label = (section.get("label") or "").strip()
        text = (section.get("text") or "").strip()
        if any(term in label.lower() for term in ("decision", "award", "ruling", "dispositivo", "dispositif")):
            return section
        if any(term in text.lower() for term in ("upheld", "dismissed", "ordered to pay", "appeal")) and len(text) > 120:
            return section
    return sections[-1] if sections else None


def _looks_like_cas_boundary(label: str) -> bool:
    stripped = (label or "").strip()
    return bool(
        stripped
        and (
            CAS_FACTS_RE.match(stripped)
            or CAS_REASONS_RE.match(stripped)
            or CAS_DECISION_RE.match(stripped)
            or re.match(r"^[IVXLCDM]+\.$", stripped, re.IGNORECASE)
        )
    )


def _ratio(numerator: int | float, denominator: int | float) -> float:
    if not denominator:
        return 0.0
    return float(numerator) / float(denominator)


def _first_subject(metadata: dict[str, Any]) -> str | None:
    subjects = metadata.get("subject_matters", [])
    if isinstance(subjects, list) and subjects:
        return _coerce_text(subjects[0])
    return None


def _coerce_text(value: Any) -> str | None:
    text = str(value).strip() if value is not None else ""
    return text or None


def _existing_path(path: Path) -> Path | None:
    return path if path.exists() else None


def _year_bucket(publication_date: str | None) -> str:
    if not publication_date:
        return "unknown"
    try:
        year = int(publication_date[:4])
    except Exception:
        return "unknown"
    if year < 2000:
        return "pre_2000"
    if year < 2010:
        return "2000s"
    if year < 2020:
        return "2010s"
    return "2020s"


def _assign_quantile_buckets(
    rows: Sequence[InventoryRow],
    attr_name: str,
    bucket_name: str,
) -> None:
    values = sorted(
        float(getattr(row, attr_name))
        for row in rows
        if getattr(row, attr_name) is not None
    )
    if not values:
        return
    quartiles = [values[math.floor((len(values) - 1) * ratio)] for ratio in (0.25, 0.5, 0.75)]
    for row in rows:
        value = getattr(row, attr_name)
        if value is None:
            setattr(row, bucket_name, "unknown")
            continue
        if value <= quartiles[0]:
            setattr(row, bucket_name, "q1")
        elif value <= quartiles[1]:
            setattr(row, bucket_name, "q2")
        elif value <= quartiles[2]:
            setattr(row, bucket_name, "q3")
        else:
            setattr(row, bucket_name, "q4")


def _round_robin_groups(groups: dict[str, list[str]]) -> list[str]:
    ordered: list[str] = []
    active = {key: list(values) for key, values in sorted(groups.items()) if values}
    while active:
        for key in list(active.keys()):
            values = active[key]
            if not values:
                del active[key]
                continue
            ordered.append(values.pop())
            if not values:
                del active[key]
    return ordered


def _default_manifest(
    sample_sets: dict[str, list[str]],
    candidate_ids: Sequence[str],
    seed: int,
    *,
    mode: str,
    candidate_set: str,
) -> BenchmarkManifest:
    return BenchmarkManifest(
        split_seed=seed,
        sample_sets=sample_sets,
        candidate_ids=list(candidate_ids),
        mode=mode,
        candidate_set=candidate_set,
        resource_limits={
            "max_rss_mb": 24 * 1024,
            "max_projected_hours_artifact": 8,
            "max_projected_hours_raw_pdf": 12,
        },
        acceptance_thresholds={
            "structure_score_min": 0.70,
            "section_boundary_recall_min": 0.90,
            "sentence_fracture_rate_max": 0.05,
            "repeated_header_ratio_max": 0.10,
            "recall_at_5_min": 0.80,
        },
    )


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: Sequence[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows),
        encoding="utf-8",
    )


def _append_jsonl(path: Path, row: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def _reset_benchmark_output_dir(output_dir: Path, *, extra_dirs: Sequence[str] = ()) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    for filename in (
        "manifest.json",
        "inventory.jsonl",
        "splits.json",
        "queries.jsonl",
        "aggregate.json",
        "summary.md",
        "review_holdout.md",
        "classification_review.md",
        "progress.json",
    ):
        artifact_path = output_dir / filename
        if artifact_path.exists():
            artifact_path.unlink()
    for dirname in ("candidates", *extra_dirs):
        artifact_dir = output_dir / dirname
        if artifact_dir.exists():
            shutil.rmtree(artifact_dir)


def _write_progress_status(
    path: Path,
    *,
    phase: str,
    completed: int,
    total: int,
    current_candidate_id: str | None = None,
    current_item_id: str | None = None,
    extra: dict[str, Any] | None = None,
) -> None:
    payload: dict[str, Any] = {
        "phase": phase,
        "completed": completed,
        "total": total,
        "percent": round((completed / total) * 100, 2) if total else 100.0,
        "current_candidate_id": current_candidate_id,
        "current_item_id": current_item_id,
        "updated_at_epoch": round(time.time(), 3),
    }
    if extra:
        payload.update(extra)
    _write_json(path, payload)


def _pdf_page_count(pdf_path: Path) -> int | None:
    result = subprocess.run(
        ["pdfinfo", str(pdf_path)],
        capture_output=True,
        check=False,
        text=True,
    )
    if result.returncode != 0:
        return None
    match = PDFINFO_PAGES_RE.search(result.stdout)
    return int(match.group(1)) if match else None


def _pdftotext(pdf_path: Path) -> str:
    result = subprocess.run(
        ["pdftotext", str(pdf_path), "-"],
        capture_output=True,
        check=False,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or f"pdftotext failed for {pdf_path.name}")
    return result.stdout


def _paragraph_split(text: str) -> list[str]:
    paragraphs = [paragraph.strip() for paragraph in re.split(r"\n\s*\n", text) if paragraph.strip()]
    return paragraphs


def _split_frontmatter(text: str) -> tuple[dict[str, Any], str]:
    match = FRONTMATTER_RE.match(text)
    if not match:
        return {}, text
    frontmatter_text = match.group(1)
    metadata: dict[str, Any] = {}
    for line in frontmatter_text.splitlines():
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        cleaned = value.strip().strip("[]")
        if "," in cleaned:
            metadata[key.strip()] = [part.strip() for part in cleaned.split(",") if part.strip()]
        else:
            metadata[key.strip()] = cleaned
    return metadata, text[match.end() :]


def _read_text(path: Path | None) -> str:
    if not path or not path.exists():
        return ""
    return path.read_text(encoding="utf-8", errors="ignore")


def _line_offsets(lines: Sequence[str]) -> list[int]:
    offsets: list[int] = []
    cursor = 0
    for line in lines:
        offsets.append(cursor)
        cursor += len(line) + 1
    return offsets


def _summarize_numbers(values: Sequence[int | float]) -> dict[str, Any]:
    if not values:
        return {"count": 0, "min": 0, "max": 0, "mean": 0, "median": 0, "total": 0}
    numeric = [float(value) for value in values]
    return {
        "count": len(numeric),
        "min": round(min(numeric), 4),
        "max": round(max(numeric), 4),
        "mean": round(statistics.mean(numeric), 4),
        "median": round(statistics.median(numeric), 4),
        "total": round(sum(numeric), 4),
    }


def _average_metric_dict(
    metrics: Sequence[dict[str, Any]],
    keys: Sequence[str],
) -> dict[str, Any]:
    if not metrics:
        return {key: 0.0 for key in keys}
    averaged = {}
    for key in keys:
        values = [float(metric.get(key, 0.0) or 0.0) for metric in metrics]
        averaged[key] = round(statistics.mean(values), 4) if values else 0.0
    return averaged


def _label_recovered(label: str, chunk_labels: Sequence[str], chunk_starts: Sequence[str]) -> bool:
    normalized = _normalize_text(label)
    if not normalized:
        return False
    for candidate in chunk_labels:
        if normalized in _normalize_text(candidate):
            return True
    for candidate in chunk_starts:
        if normalized in _normalize_text(candidate):
            return True
    return False


def _normalize_text(text: str) -> str:
    cleaned = re.sub(r"\s+", " ", (text or "").strip().lower())
    return re.sub(r"[^a-z0-9à-ÿ ]+", "", cleaned)


def _appears_sentence_fractured(text: str) -> bool:
    stripped = text.strip()
    if len(stripped) < 40:
        return False
    return not stripped.endswith((".", "!", "?", "”", '"', ")", "]"))


def _repeated_header_ratio(chunks: Sequence[BenchmarkChunk]) -> float:
    line_counts: Counter[str] = Counter()
    for chunk in chunks:
        for line in chunk.text.splitlines()[:3]:
            normalized = _normalize_text(line)
            if len(normalized) >= 20:
                line_counts[normalized] += 1
    repeated = sum(count for count in line_counts.values() if count >= max(2, len(chunks) // 5))
    return _ratio(repeated, len(chunks))


def _numbered_paragraph_integrity(chunks: Sequence[BenchmarkChunk]) -> float:
    broken = 0
    for chunk in chunks:
        lines = [line.strip() for line in chunk.text.splitlines() if line.strip()]
        if not lines:
            continue
        if NUMERIC_MARKER_RE.match(lines[-1]):
            broken += 1
        for first, second in zip(lines, lines[1:]):
            if NUMERIC_MARKER_RE.match(first) and len(second) < 20:
                broken += 1
                break
    return max(0.0, 1.0 - _ratio(broken, len(chunks)))


def _anchor_from_text(text: str) -> str:
    sentences = [sentence.strip() for sentence in SENTENCE_BOUNDARY_RE.split(text) if sentence.strip()]
    candidate = sentences[0] if sentences else text.strip()
    return candidate[:140]


def _salient_query_terms(text: str, limit: int = 8) -> str:
    tokens = [token.lower() for token in TOKEN_RE.findall(text)]
    filtered = [token for token in tokens if token not in STOPWORDS and len(token) > 2]
    return " ".join(filtered[:limit])


def _chunk_matches_query_target(chunk: BenchmarkChunk, query: BenchmarkQuery) -> bool:
    if query.target_label and _normalize_text(query.target_label) in _normalize_text(chunk.label or ""):
        return True
    if query.target_anchor and _normalize_text(query.target_anchor) in _normalize_text(chunk.text):
        return True
    return False


class _SimpleBM25:
    def __init__(self, documents: Sequence[str]):
        self.documents = [self._tokenize(document) for document in documents]
        self.doc_lengths = [len(document) for document in self.documents]
        self.avgdl = statistics.mean(self.doc_lengths) if self.doc_lengths else 0.0
        self.term_frequencies = [Counter(document) for document in self.documents]
        self.document_frequency: Counter[str] = Counter()
        for document in self.documents:
            for term in set(document):
                self.document_frequency[term] += 1

    def score(self, query: str, k1: float = 1.2, b: float = 0.75) -> list[float]:
        terms = self._tokenize(query)
        total_documents = len(self.documents) or 1
        scores: list[float] = []
        for index, document in enumerate(self.documents):
            doc_length = self.doc_lengths[index] or 1
            tf = self.term_frequencies[index]
            score = 0.0
            for term in terms:
                df = self.document_frequency.get(term, 0)
                if df == 0:
                    continue
                idf = math.log(1 + ((total_documents - df + 0.5) / (df + 0.5)))
                freq = tf.get(term, 0)
                if not freq:
                    continue
                denom = freq + k1 * (1 - b + b * (doc_length / (self.avgdl or 1)))
                score += idf * ((freq * (k1 + 1)) / denom)
            scores.append(score)
        return scores

    @staticmethod
    def _tokenize(text: str) -> list[str]:
        return [token.lower() for token in TOKEN_RE.findall(text) if token.lower() not in STOPWORDS]


def _tail_for_overlap(sentences: Sequence[str], overlap_tokens: int) -> list[str]:
    tail: list[str] = []
    total = 0
    for sentence in reversed(sentences):
        tail.insert(0, sentence)
        total += _count_tokens(sentence)
        if total >= overlap_tokens:
            break
    return tail


def _hard_wrap_sentence(sentence: str, max_chunk_tokens: int) -> Iterator[str]:
    words = sentence.split()
    current: list[str] = []
    for word in words:
        tentative = " ".join(current + [word]).strip()
        if current and _count_tokens(tentative) > max_chunk_tokens:
            yield " ".join(current).strip()
            current = [word]
            continue
        current.append(word)
    if current:
        yield " ".join(current).strip()


def _coerce_int(value: Any) -> int:
    try:
        return int(value or 0)
    except Exception:
        return 0


def _is_difficult_bundle(bundle: DocumentBundle) -> bool:
    text = _read_text(bundle.output_txt_path)
    canonical = load_canonical_document(bundle)
    language = (
        (
            (canonical.get("metadata") or {}).get("extra") or {}
        ).get("language")
        if canonical
        else None
    )
    page_count, encrypted = _pdf_profile(bundle.pdf_path)
    return (
        len(text) > 80_000
        or bundle.pdf_path.stat().st_size > 1_500_000
        or (page_count or 0) >= 150
        or str(language or "").strip().lower() == "fr"
        or encrypted
    )


def _current_rss_mb() -> float | None:
    try:
        import psutil

        return round(psutil.Process(os.getpid()).memory_info().rss / (1024 * 1024), 2)
    except Exception:
        return None


def _count_tokens(text: str) -> int:
    try:
        from src.gui.infrastructure.scraper_document_workflow import _count_tokens as worker_count_tokens
    except Exception:
        worker_count_tokens = None
    if worker_count_tokens is not None:
        return worker_count_tokens(text)
    if not text:
        return 0
    return max(1, int(len(text.split()) / 0.75))


def _run_worker_extract_chunks(file_path: Path, fallback_title: str) -> list[dict[str, Any]]:
    from src.gui.infrastructure.scraper_document_workflow import _extract_document_chunks

    with _suppress_noisy_pdf_logs(include_unstructured=True):
        return _call_quietly(_extract_document_chunks, file_path, fallback_title)
