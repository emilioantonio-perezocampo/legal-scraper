"""
Benchmark harness for Biblio UNAM extraction, chunking, deduplication, and
document-level legal matter classification.

Unlike CAS/TAS, the Biblio corpus is not standardized:
- some families only have chapter PDFs
- some have a whole-book PDF
- many have both whole-book and chapter PDFs
- some files are text-extractable while others are scanned / copy-protected

This harness evaluates family-aware strategies so we can choose a repeatable
production path before changing the live pipeline.
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
import tempfile
import time
import sys
import urllib.error
import urllib.request
import warnings
from collections import Counter, defaultdict
from contextlib import contextmanager, redirect_stderr, redirect_stdout
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator, Sequence

from src.gui.infrastructure.cas_chunk_benchmark import (
    AggregateReport,
    BenchmarkChunk,
    BenchmarkManifest,
    BenchmarkQuery,
    CandidateSpec,
    CandidateRun,
    PerDocumentResult,
    _SimpleBM25,
    _average_metric_dict,
    _anchor_from_text,
    _coerce_int,
    _count_tokens,
    _current_rss_mb,
    _hard_wrap_sentence,
    _paragraph_split,
    _ratio,
    _round_robin_groups,
    _run_worker_extract_chunks,
    _salient_query_terms,
    _split_with_overlap,
    _summarize_numbers,
    _tail_for_overlap,
    _failed_candidate_run,
    _timed_candidate,
    _write_json,
    _write_jsonl,
    _append_jsonl,
    _reset_benchmark_output_dir,
    _write_progress_status,
)

QUERY_SET_VERSION = "biblio_benchmark_v1"
DEFAULT_DATA_ROOT = Path(os.environ.get("SCRAPER_MOUNTED_DATA_ROOT", "/app/mounted_data"))
DEFAULT_DOCLING_ARTIFACTS_PATH = os.environ.get("SCRAPER_DOCLING_ARTIFACTS_PATH")
BIBLIO_RUNTIME_GATE_HOURS = 36.0
_NOISY_PDF_LOGGERS = (
    "RapidOCR",
    "docling.models.stages.ocr.rapid_ocr_model",
    "pdfminer",
    "pdfminer.pdfinterp",
    "pypdf",
    "pypdf._reader",
)
_UNSTRUCTURED_NOISY_PDF_LOGGERS = _NOISY_PDF_LOGGERS + ("unstructured.partition.pdf",)


def _configure_local_model_runtime() -> None:
    force_offline = os.environ.get("SCRAPER_BENCHMARK_FORCE_OFFLINE", "").lower() in {
        "1",
        "true",
        "yes",
    }
    if force_offline:
        os.environ.setdefault("HF_HUB_OFFLINE", "1")
        os.environ.setdefault("TRANSFORMERS_OFFLINE", "1")
    os.environ.setdefault("HF_HUB_DISABLE_TELEMETRY", "1")


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


def _within_runtime_gate(projected_hours: float | None) -> bool:
    if projected_hours is None:
        return True
    return projected_hours <= BIBLIO_RUNTIME_GATE_HOURS


def _winner_eligible(report: AggregateReport) -> bool:
    return report.decision_status != "blocked" and _within_runtime_gate(
        report.projected_full_corpus_runtime_hours
    )


def _normalize_runtime_status(report: AggregateReport) -> None:
    if report.decision_status == "blocked":
        return
    if not _within_runtime_gate(report.projected_full_corpus_runtime_hours):
        report.decision_status = "over_budget"
DEFAULT_CORE_CANDIDATES = (
    "prod_current_v1",
    "pdftotext_floor_v1",
    "chapter_first_heading_v1",
    "book_only_heading_v1",
    "unstructured_title_text_v1",
    "cal_pymupdf_unstructured_balanced_v1",
)
CALIBRATED_CANDIDATES = (
    "cal_pymupdf_autoocr_heading_v1",
    "cal_pymupdf_routed_heading_v1",
    "cal_pymupdf_unstructured_balanced_v1",
    "cal_pymupdf_unstructured_dense_v1",
)
OPTIONAL_CANDIDATES = (
    "pymupdf4llm_md_v1",
    "ocrmypdf_pymupdf_v1",
    "ocrmypdf_unstructured_md_v1",
    "docling_cpu_v1",
    "marker_cpu_subset_v1",
)


def _docling_self_hosted_status() -> str:
    if DEFAULT_DOCLING_ARTIFACTS_PATH:
        return "local_only"
    return "local_with_optional_artifacts"


def build_candidate_specs() -> dict[str, CandidateSpec]:
    return {
        "prod_current_v1": CandidateSpec(
            candidate_id="prod_current_v1",
            lane="biblio",
            self_hosted_status="local_only",
            description="Current worker/path baseline",
        ),
        "pdftotext_floor_v1": CandidateSpec(
            candidate_id="pdftotext_floor_v1",
            lane="biblio",
            self_hosted_status="local_only",
            description="pdftotext floor baseline",
        ),
        "chapter_first_heading_v1": CandidateSpec(
            candidate_id="chapter_first_heading_v1",
            lane="biblio",
            self_hosted_status="local_only",
            description="Chapter-first heading-aware chunking",
        ),
        "book_only_heading_v1": CandidateSpec(
            candidate_id="book_only_heading_v1",
            lane="biblio",
            self_hosted_status="local_only",
            description="Whole-book heading-aware chunking",
        ),
        "unstructured_title_text_v1": CandidateSpec(
            candidate_id="unstructured_title_text_v1",
            lane="biblio",
            self_hosted_status="local_only",
            description="Unstructured title chunking over extracted text",
        ),
        "cal_pymupdf_autoocr_heading_v1": CandidateSpec(
            candidate_id="cal_pymupdf_autoocr_heading_v1",
            lane="biblio",
            self_hosted_status="local_only",
            ocr_languages=("spa", "eng"),
            description="PyMuPDF4LLM auto-OCR with heading chunking",
        ),
        "cal_pymupdf_routed_heading_v1": CandidateSpec(
            candidate_id="cal_pymupdf_routed_heading_v1",
            lane="biblio",
            self_hosted_status="local_only",
            ocr_languages=("spa", "eng"),
            description="PyMuPDF4LLM routed OCR with heading chunking",
        ),
        "cal_pymupdf_unstructured_balanced_v1": CandidateSpec(
            candidate_id="cal_pymupdf_unstructured_balanced_v1",
            lane="biblio",
            self_hosted_status="local_only",
            ocr_languages=("spa", "eng"),
            description="Recommended PyMuPDF4LLM plus balanced Unstructured title chunking",
        ),
        "cal_pymupdf_unstructured_dense_v1": CandidateSpec(
            candidate_id="cal_pymupdf_unstructured_dense_v1",
            lane="biblio",
            self_hosted_status="local_only",
            ocr_languages=("spa", "eng"),
            description="PyMuPDF4LLM plus dense Unstructured title chunking",
        ),
        "pymupdf4llm_md_v1": CandidateSpec(
            candidate_id="pymupdf4llm_md_v1",
            lane="biblio",
            self_hosted_status="local_only",
            ocr_languages=("spa", "eng"),
            description="PyMuPDF4LLM markdown output",
        ),
        "pymupdf_unstructured_md_v1": CandidateSpec(
            candidate_id="pymupdf_unstructured_md_v1",
            lane="biblio",
            self_hosted_status="local_only",
            ocr_languages=("spa", "eng"),
            description="Legacy PyMuPDF4LLM plus Unstructured markdown chunking baseline",
        ),
        "ocrmypdf_pymupdf_v1": CandidateSpec(
            candidate_id="ocrmypdf_pymupdf_v1",
            lane="biblio",
            self_hosted_status="local_only",
            ocr_languages=("spa", "eng"),
            description="OCRmyPDF preprocessing plus PyMuPDF4LLM chunking",
        ),
        "ocrmypdf_unstructured_md_v1": CandidateSpec(
            candidate_id="ocrmypdf_unstructured_md_v1",
            lane="biblio",
            self_hosted_status="local_only",
            ocr_languages=("spa", "eng"),
            description="OCRmyPDF preprocessing plus Unstructured markdown chunking",
        ),
        "docling_cpu_v1": CandidateSpec(
            candidate_id="docling_cpu_v1",
            lane="biblio",
            self_hosted_status=_docling_self_hosted_status(),
            description="Docling HybridChunker on CPU",
        ),
        "marker_cpu_subset_v1": CandidateSpec(
            candidate_id="marker_cpu_subset_v1",
            lane="biblio",
            self_hosted_status="local_with_optional_artifacts",
            subset_only=True,
            description="Subset-only Marker CPU challenger",
        ),
    }
DEFAULT_CLASSIFIERS = (
    "metadata_rule_v1",
    "hybrid_classifier_v1",
    "ollama_agentic_v1",
)
OPTIONAL_CLASSIFIERS = ("setfit_zero_shot_v1",)
PDFINFO_PAGES_RE = re.compile(r"^Pages:\s+(\d+)$", re.MULTILINE)
PDFINFO_ENCRYPTED_RE = re.compile(r"^Encrypted:\s+(.+)$", re.MULTILINE)
PDFINFO_PRODUCER_RE = re.compile(r"^Producer:\s+(.+)$", re.MULTILINE)
TOKEN_RE = re.compile(r"[A-Za-zÀ-ÿ0-9][A-Za-zÀ-ÿ0-9'/_-]*")
ROMAN_RE = re.compile(r"^[IVXLCDM]+(?:\.)?$", re.IGNORECASE)
SECTION_HEADING_RE = re.compile(
    r"^(?:CAP[IÍ]TULO|T[IÍ]TULO|SECCI[ÓO]N|LIBRO|PARTE|ART[IÍ]CULO)\b",
    re.IGNORECASE,
)
ALL_CAPS_RE = re.compile(r"^[A-ZÁÉÍÓÚÑ0-9][A-ZÁÉÍÓÚÑ0-9 ,.;:()/-]{5,}$")
NUMERIC_SECTION_RE = re.compile(r"^(?:\d+(?:\.\d+)*|[IVXLCDM]+)[.)-]?\s+\S", re.IGNORECASE)
STOPWORDS = {
    "a",
    "al",
    "and",
    "art",
    "articulo",
    "artículo",
    "book",
    "capitulo",
    "capítulo",
    "con",
    "de",
    "del",
    "derecho",
    "el",
    "en",
    "for",
    "la",
    "las",
    "libro",
    "los",
    "para",
    "por",
    "seccion",
    "sección",
    "the",
    "title",
    "titulo",
    "título",
    "un",
    "una",
    "y",
}
LEGAL_MATERIAS = (
    "civil",
    "laboral",
    "penal",
    "mercantil",
    "administrativo",
    "constitucional",
    "fiscal",
    "familiar",
    "agrario",
    "ambiental",
    "aduanero",
    "electoral",
    "seguridad_social",
    "propiedad_intelectual",
)
MATERIA_KEYWORDS: dict[str, tuple[str, ...]] = {
    "civil": (
        "arrendamiento",
        "compraventa",
        "contrato civil",
        "obligaciones",
        "propiedad",
        "posesión",
        "sucesión",
        "testamento",
        "usufructo",
    ),
    "laboral": (
        "despido",
        "trabajador",
        "patrón",
        "salario",
        "aguinaldo",
        "vacaciones",
        "prestaciones",
        "sindicato",
        "subordinación",
        "laudo",
    ),
    "penal": (
        "delito",
        "penal",
        "ministerio público",
        "sentencia penal",
        "robo",
        "fraude",
        "homicidio",
        "acusado",
        "imputado",
    ),
    "mercantil": (
        "acto de comercio",
        "sociedad mercantil",
        "comerciante",
        "título de crédito",
        "concurso mercantil",
        "quiebra",
        "pagaré",
        "cheque",
    ),
    "administrativo": (
        "administrativo",
        "administración pública",
        "permiso",
        "licencia",
        "concesión",
        "migración",
        "visas",
        "inm",
        "licitación",
    ),
    "constitucional": (
        "amparo",
        "constitucional",
        "derechos humanos",
        "suprema corte",
        "scjn",
        "garantías",
    ),
    "fiscal": (
        "impuestos",
        "fiscal",
        "tributario",
        "sat",
        "isr",
        "iva",
        "contribuciones",
    ),
    "familiar": (
        "matrimonio",
        "divorcio",
        "custodia",
        "pensión alimenticia",
        "adopción",
        "patria potestad",
        "violencia familiar",
    ),
    "agrario": (
        "ejido",
        "agrario",
        "comunidad agraria",
        "ejidatario",
        "parcela",
        "tribunal agrario",
    ),
    "ambiental": (
        "medio ambiente",
        "ambiental",
        "contaminación",
        "semarnat",
        "profepa",
        "impacto ambiental",
    ),
    "aduanero": (
        "aduana",
        "aduanero",
        "importación",
        "exportación",
        "pedimento",
        "comercio exterior",
    ),
    "electoral": (
        "electoral",
        "ine",
        "tepjf",
        "voto",
        "elección",
        "partido político",
    ),
    "seguridad_social": (
        "seguridad social",
        "pensión",
        "jubilación",
        "issste",
        "afore",
        "imss",
        "infonavit",
    ),
    "propiedad_intelectual": (
        "derechos de autor",
        "marca",
        "patente",
        "propiedad intelectual",
        "impi",
        "copyright",
        "secreto industrial",
    ),
}


@dataclass(frozen=True)
class BiblioAsset:
    path: Path
    asset_role: str
    label: str
    chapter_number: str | None = None


@dataclass(frozen=True)
class BiblioFamilyBundle:
    family_id: str
    family_dir: Path
    book_pdf_path: Path | None
    chapter_pdf_paths: tuple[Path, ...]
    cover_path: Path | None
    topology: str
    total_pdf_size_bytes: int
    total_chapter_bytes: int
    chapter_count: int


@dataclass
class BiblioInventoryRow:
    family_id: str
    family_dir: str
    topology: str
    has_book_pdf: bool
    has_chapters: bool
    chapter_count: int
    total_pdf_size_bytes: int
    book_pdf_size_bytes: int
    total_chapter_bytes: int
    sample_extractable_chars: int
    sample_page_count: int | None
    sample_encrypted: str | None
    sample_producer: str | None
    size_bucket: str = "unknown"
    chapter_bucket: str = "unknown"
    extractability_bucket: str = "unknown"
    stratum: str = "unknown"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class ClassificationPrediction:
    classifier_id: str
    family_id: str
    primary_materia: str | None
    secondary_materias: list[str]
    subject_tags: list[str]
    confidence: float
    method: str
    skipped: bool = False
    error: str | None = None
    reasoning: str | None = None
    evidence_excerpt: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class ClassificationAggregate:
    classifier_id: str
    families_run: int
    success_rate: float
    high_confidence_rate: float
    avg_confidence: float
    agreement_with_silver: float | None
    decision_status: str
    skipped_families: int = 0
    failed_families: int = 0

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def discover_biblio_families(data_root: Path = DEFAULT_DATA_ROOT) -> list[BiblioFamilyBundle]:
    books_root = data_root / "books"
    families: list[BiblioFamilyBundle] = []
    if not books_root.exists():
        return families

    for family_dir in sorted(path for path in books_root.iterdir() if path.is_dir()):
        book_pdf = family_dir / "book.pdf"
        chapters_dir = family_dir / "chapters"
        chapter_pdfs = tuple(sorted(chapters_dir.glob("*.pdf"), key=_chapter_path_sort_key)) if chapters_dir.exists() else tuple()
        has_book = book_pdf.exists()
        has_chapters = bool(chapter_pdfs)
        if has_book and has_chapters:
            topology = "both"
        elif has_book:
            topology = "book_only"
        elif has_chapters:
            topology = "chapter_only"
        else:
            topology = "incomplete"
        total_chapter_bytes = sum(path.stat().st_size for path in chapter_pdfs)
        total_pdf_size_bytes = total_chapter_bytes + (book_pdf.stat().st_size if has_book else 0)
        families.append(
            BiblioFamilyBundle(
                family_id=family_dir.name,
                family_dir=family_dir,
                book_pdf_path=book_pdf if has_book else None,
                chapter_pdf_paths=chapter_pdfs,
                cover_path=(family_dir / "cover.jpg") if (family_dir / "cover.jpg").exists() else None,
                topology=topology,
                total_pdf_size_bytes=total_pdf_size_bytes,
                total_chapter_bytes=total_chapter_bytes,
                chapter_count=len(chapter_pdfs),
            )
        )
    return families


def build_inventory(
    families: Sequence[BiblioFamilyBundle],
    include_pdf_info: bool = True,
) -> list[BiblioInventoryRow]:
    rows: list[BiblioInventoryRow] = []
    for family in families:
        sample_asset = _sample_asset(family)
        pdfinfo = _pdfinfo(sample_asset.path) if include_pdf_info and sample_asset else {}
        row = BiblioInventoryRow(
            family_id=family.family_id,
            family_dir=str(family.family_dir),
            topology=family.topology,
            has_book_pdf=family.book_pdf_path is not None,
            has_chapters=bool(family.chapter_pdf_paths),
            chapter_count=family.chapter_count,
            total_pdf_size_bytes=family.total_pdf_size_bytes,
            book_pdf_size_bytes=family.book_pdf_path.stat().st_size if family.book_pdf_path else 0,
            total_chapter_bytes=family.total_chapter_bytes,
            sample_extractable_chars=_sample_extractable_chars(sample_asset.path) if sample_asset else 0,
            sample_page_count=_coerce_int(pdfinfo.get("Pages")) if pdfinfo.get("Pages") else None,
            sample_encrypted=_coerce_text(pdfinfo.get("Encrypted")),
            sample_producer=_coerce_text(pdfinfo.get("Producer")),
        )
        rows.append(row)

    _assign_bucket(
        rows,
        lambda row: row.total_pdf_size_bytes / (1024 * 1024),
        "size_bucket",
        [1, 10, 50, 200],
        ["xs", "sm", "md", "lg", "xl"],
    )
    _assign_bucket(
        rows,
        lambda row: row.chapter_count,
        "chapter_bucket",
        [0, 4, 10, 20, 40],
        ["none", "few", "medium", "many", "very_many", "massive"],
    )
    _assign_bucket(
        rows,
        lambda row: row.sample_extractable_chars,
        "extractability_bucket",
        [0, 200, 1000, 4000],
        ["none", "low", "medium", "high", "very_high"],
    )
    for row in rows:
        row.stratum = "|".join(
            [row.topology, row.size_bucket, row.chapter_bucket, row.extractability_bucket]
        )
    return rows


def build_stratified_splits(
    inventory_rows: Sequence[BiblioInventoryRow],
    seed: int,
    smoke_size: int,
    tuning_size: int,
    holdout_size: int,
) -> dict[str, list[str]]:
    rng = random.Random(seed)
    groups: dict[str, list[str]] = defaultdict(list)
    for row in inventory_rows:
        if row.topology == "incomplete":
            continue
        groups[row.stratum].append(row.family_id)
    for ids in groups.values():
        rng.shuffle(ids)
    ordered = _round_robin_groups(groups)
    return {
        "smoke": ordered[:smoke_size],
        "tuning": ordered[smoke_size : smoke_size + tuning_size],
        "holdout": ordered[smoke_size + tuning_size : smoke_size + tuning_size + holdout_size],
    }


def run_benchmark(
    *,
    output_dir: Path,
    data_root: Path = DEFAULT_DATA_ROOT,
    smoke_size: int = 60,
    tuning_size: int = 140,
    holdout_size: int = 60,
    seed: int = 20260412,
    candidate_ids: Sequence[str] | None = None,
    classifier_ids: Sequence[str] | None = None,
    family_ids: Sequence[str] | None = None,
    include_optional: bool = False,
    include_pdf_info: bool = True,
) -> dict[str, Any]:
    _reset_benchmark_output_dir(output_dir, extra_dirs=("classifiers",))
    _configure_local_model_runtime()
    _write_progress_status(
        output_dir / "progress.json",
        phase="initializing",
        completed=0,
        total=0,
        extra={
            "mode": "biblio",
            "status": "building_inventory",
        },
    )
    candidate_specs = build_candidate_specs()
    families = discover_biblio_families(data_root)
    if not families:
        raise FileNotFoundError(f"No Biblio families found under {data_root / 'books'}")

    family_map = {family.family_id: family for family in families}
    if family_ids:
        selected_ids = [family_id for family_id in family_ids if family_id in family_map]
        selected_families = [family_map[family_id] for family_id in selected_ids]
        sample_sets = {"selected": selected_ids}
    else:
        selected_families = list(families)
    inventory = build_inventory(selected_families, include_pdf_info=include_pdf_info)
    inventory_map = {row.family_id: row for row in inventory}

    if not family_ids:
        sample_sets = build_stratified_splits(
            inventory,
            seed=seed,
            smoke_size=smoke_size,
            tuning_size=tuning_size,
            holdout_size=holdout_size,
        )
        selected_ids = [family_id for ids in sample_sets.values() for family_id in ids]

    requested_candidates = list(candidate_ids or DEFAULT_CORE_CANDIDATES)
    if include_optional:
        requested_candidates.extend(
            candidate_id
            for candidate_id in OPTIONAL_CANDIDATES
            if candidate_id not in requested_candidates
        )
    candidate_registry = build_candidate_registry()
    requested_candidates = [
        cid for cid in requested_candidates if cid in candidate_registry and cid in candidate_specs
    ]

    requested_classifiers = list(classifier_ids or DEFAULT_CLASSIFIERS)
    if include_optional:
        requested_classifiers.extend(
            classifier_id
            for classifier_id in OPTIONAL_CLASSIFIERS
            if classifier_id not in requested_classifiers
        )
    classifier_registry = build_classifier_registry()
    requested_classifiers = [cid for cid in requested_classifiers if cid in classifier_registry]

    _write_json(
        output_dir / "manifest.json",
        _default_manifest(sample_sets, requested_candidates, requested_classifiers, seed).to_dict(),
    )
    _write_jsonl(output_dir / "inventory.jsonl", [row.to_dict() for row in inventory])
    _write_json(output_dir / "splits.json", sample_sets)

    queries: list[dict[str, Any]] = []
    results_by_candidate: dict[str, list[PerDocumentResult]] = defaultdict(list)
    classification_predictions: dict[str, list[ClassificationPrediction]] = defaultdict(list)
    silver_labels: dict[str, str] = {}
    family_query_cache: dict[str, list[BenchmarkQuery]] = {}
    query_warnings: dict[str, str] = {}
    for family_id in selected_ids:
        family = family_map[family_id]
        try:
            family_queries = build_family_queries(family)
        except Exception as exc:
            family_queries = []
            query_warnings[family_id] = f"query_generation_failed: {exc}"
        family_query_cache[family_id] = family_queries
        for query in family_queries:
            queries.append({"family_id": family_id, **query.to_dict()})
    _write_jsonl(output_dir / "queries.jsonl", queries)
    candidates_dir = output_dir / "candidates"
    candidates_dir.mkdir(exist_ok=True)
    classifier_dir = output_dir / "classifiers"
    classifier_dir.mkdir(exist_ok=True)
    total_candidate_runs = len(selected_ids) * len(requested_candidates)
    total_classifier_runs = len(selected_ids) * len(requested_classifiers)
    completed_candidate_runs = 0
    completed_classifier_runs = 0
    for candidate_id in requested_candidates:
        (candidates_dir / candidate_id).mkdir(exist_ok=True)
        _write_progress_status(
            candidates_dir / candidate_id / "status.json",
            phase="running",
            completed=0,
            total=len(selected_ids),
            current_candidate_id=candidate_id,
        )
    for classifier_id in requested_classifiers:
        classifier_status_dir = classifier_dir / classifier_id
        classifier_status_dir.mkdir(exist_ok=True)
        _write_progress_status(
            classifier_status_dir / "status.json",
            phase="running",
            completed=0,
            total=len(selected_ids),
            current_candidate_id=classifier_id,
        )
    _write_progress_status(
        output_dir / "progress.json",
        phase="running_candidates",
        completed=0,
        total=total_candidate_runs + total_classifier_runs,
        extra={
            "candidate_runs_completed": 0,
            "candidate_runs_total": total_candidate_runs,
            "classifier_runs_completed": 0,
            "classifier_runs_total": total_classifier_runs,
        },
    )

    for family_id in selected_ids:
        family = family_map[family_id]
        inventory_row = inventory_map[family_id]
        family_queries = family_query_cache.get(family_id, [])
        query_warning = query_warnings.get(family_id)

        for candidate_id in requested_candidates:
            runner = candidate_registry[candidate_id]
            spec = candidate_specs[candidate_id]
            try:
                run = runner(family)
                result = analyze_candidate_run(family, inventory_row, run, family_queries, spec)
            except Exception as exc:
                result = analyze_candidate_run(
                    family,
                    inventory_row,
                    _failed_candidate_run(candidate_id, exc),
                    family_queries,
                    spec,
                )
            if query_warning:
                result.warnings.append(query_warning)
            results_by_candidate[candidate_id].append(result)
            completed_candidate_runs += 1
            candidate_dir = candidates_dir / candidate_id
            _append_jsonl(candidate_dir / "results.jsonl", result.to_dict())
            _write_progress_status(
                candidate_dir / "status.json",
                phase="running",
                completed=len(results_by_candidate[candidate_id]),
                total=len(selected_ids),
                current_candidate_id=candidate_id,
                current_item_id=family_id,
            )
            _write_progress_status(
                output_dir / "progress.json",
                phase="running_candidates",
                completed=completed_candidate_runs + completed_classifier_runs,
                total=total_candidate_runs + total_classifier_runs,
                current_candidate_id=candidate_id,
                current_item_id=family_id,
                extra={
                    "candidate_runs_completed": completed_candidate_runs,
                    "candidate_runs_total": total_candidate_runs,
                    "classifier_runs_completed": completed_classifier_runs,
                    "classifier_runs_total": total_classifier_runs,
                },
            )

        classifier_context = ""
        classifier_context_error: str | None = None
        try:
            classifier_context = build_classifier_context(family)
        except Exception as exc:
            classifier_context_error = f"classifier_context_failed: {exc}"
        for classifier_id in requested_classifiers:
            predictor = classifier_registry[classifier_id]
            if classifier_context_error:
                prediction = ClassificationPrediction(
                    classifier_id=classifier_id,
                    family_id=family.family_id,
                    primary_materia=None,
                    secondary_materias=[],
                    subject_tags=[],
                    confidence=0.0,
                    method="context_error",
                    error=classifier_context_error,
                )
            else:
                try:
                    prediction = predictor(family, classifier_context)
                except Exception as exc:
                    prediction = ClassificationPrediction(
                        classifier_id=classifier_id,
                        family_id=family.family_id,
                        primary_materia=None,
                        secondary_materias=[],
                        subject_tags=[],
                        confidence=0.0,
                        method="exception",
                        error=str(exc),
                    )
            classification_predictions[classifier_id].append(prediction)
            completed_classifier_runs += 1
            classifier_status_dir = classifier_dir / classifier_id
            _append_jsonl(
                classifier_dir / f"{classifier_id}.jsonl",
                prediction.to_dict(),
            )
            _write_progress_status(
                classifier_status_dir / "status.json",
                phase="running",
                completed=len(classification_predictions[classifier_id]),
                total=len(selected_ids),
                current_candidate_id=classifier_id,
                current_item_id=family_id,
            )
            _write_progress_status(
                output_dir / "progress.json",
                phase="running_classifiers",
                completed=completed_candidate_runs + completed_classifier_runs,
                total=total_candidate_runs + total_classifier_runs,
                current_candidate_id=classifier_id,
                current_item_id=family_id,
                extra={
                    "candidate_runs_completed": completed_candidate_runs,
                    "candidate_runs_total": total_candidate_runs,
                    "classifier_runs_completed": completed_classifier_runs,
                    "classifier_runs_total": total_classifier_runs,
                },
            )

        silver = _derive_silver_label(classification_predictions, family_id)
        if silver:
            silver_labels[family_id] = silver

    aggregates: list[AggregateReport] = []
    for candidate_id, results in results_by_candidate.items():
        candidate_dir = candidates_dir / candidate_id
        _write_jsonl(candidate_dir / "results.jsonl", [result.to_dict() for result in results])
        aggregate = aggregate_candidate_results(candidate_id, results, len(families), candidate_specs[candidate_id])
        aggregates.append(aggregate)
        _write_json(candidate_dir / "aggregate.json", aggregate.to_dict())
        _write_progress_status(
            candidate_dir / "status.json",
            phase="completed",
            completed=len(results),
            total=len(selected_ids),
            current_candidate_id=candidate_id,
        )

    ranked = rank_aggregates(aggregates)
    _write_json(output_dir / "aggregate.json", [aggregate.to_dict() for aggregate in ranked])
    summary = render_summary_markdown(ranked, sample_sets, inventory)
    (output_dir / "summary.md").write_text(summary, encoding="utf-8")
    review_doc = render_holdout_review(
        sample_sets.get("holdout", []),
        family_map,
        results_by_candidate,
        ranked[:3],
    )
    (output_dir / "review_holdout.md").write_text(review_doc, encoding="utf-8")

    classifier_aggregates: list[ClassificationAggregate] = []
    for classifier_id, predictions in classification_predictions.items():
        _write_jsonl(
            classifier_dir / f"{classifier_id}.jsonl",
            [prediction.to_dict() for prediction in predictions],
        )
        aggregate = aggregate_classifier_predictions(classifier_id, predictions, silver_labels)
        classifier_aggregates.append(aggregate)
        _write_progress_status(
            classifier_dir / classifier_id / "status.json",
            phase="completed",
            completed=len(predictions),
            total=len(selected_ids),
            current_candidate_id=classifier_id,
        )
    classifier_aggregates = rank_classifier_aggregates(classifier_aggregates)
    _write_json(
        classifier_dir / "aggregate.json",
        [aggregate.to_dict() for aggregate in classifier_aggregates],
    )
    _write_jsonl(
        classifier_dir / "silver_labels.jsonl",
        [{"family_id": family_id, "silver_primary_materia": materia} for family_id, materia in silver_labels.items()],
    )
    (output_dir / "classification_review.md").write_text(
        render_classification_review(sample_sets.get("holdout", []), family_map, classification_predictions),
        encoding="utf-8",
    )
    _write_progress_status(
        output_dir / "progress.json",
        phase="completed",
        completed=total_candidate_runs + total_classifier_runs,
        total=total_candidate_runs + total_classifier_runs,
        extra={
            "candidate_runs_completed": total_candidate_runs,
            "candidate_runs_total": total_candidate_runs,
            "classifier_runs_completed": total_classifier_runs,
            "classifier_runs_total": total_classifier_runs,
        },
    )

    return {
        "output_dir": str(output_dir),
        "candidate_ids": requested_candidates,
        "classifier_ids": requested_classifiers,
        "sample_sets": sample_sets,
        "aggregates": [aggregate.to_dict() for aggregate in ranked],
        "classifier_aggregates": [aggregate.to_dict() for aggregate in classifier_aggregates],
    }


def build_candidate_registry() -> dict[str, Callable[[BiblioFamilyBundle], CandidateRun]]:
    return {
        "prod_current_v1": run_prod_current_v1,
        "pdftotext_floor_v1": run_pdftotext_floor_v1,
        "chapter_first_heading_v1": run_chapter_first_heading_v1,
        "book_only_heading_v1": run_book_only_heading_v1,
        "unstructured_title_text_v1": run_unstructured_title_text_v1,
        "cal_pymupdf_autoocr_heading_v1": run_cal_pymupdf_autoocr_heading_v1,
        "cal_pymupdf_routed_heading_v1": run_cal_pymupdf_routed_heading_v1,
        "cal_pymupdf_unstructured_balanced_v1": run_cal_pymupdf_unstructured_balanced_v1,
        "cal_pymupdf_unstructured_dense_v1": run_cal_pymupdf_unstructured_dense_v1,
        "pymupdf4llm_md_v1": run_pymupdf4llm_md_v1,
        "pymupdf_unstructured_md_v1": run_pymupdf_unstructured_md_v1,
        "ocrmypdf_pymupdf_v1": run_ocrmypdf_pymupdf_v1,
        "ocrmypdf_unstructured_md_v1": run_ocrmypdf_unstructured_md_v1,
        "docling_cpu_v1": run_docling_cpu_v1,
        "marker_cpu_subset_v1": run_marker_cpu_subset_v1,
    }


def build_classifier_registry() -> dict[str, Callable[[BiblioFamilyBundle, str], ClassificationPrediction]]:
    return {
        "metadata_rule_v1": run_metadata_rule_classifier,
        "hybrid_classifier_v1": run_hybrid_classifier,
        "ollama_agentic_v1": run_ollama_agentic_classifier,
        "setfit_zero_shot_v1": run_setfit_zero_shot_classifier,
    }


def run_prod_current_v1(family: BiblioFamilyBundle) -> CandidateRun:
    def _runner() -> CandidateRun:
        chunks: list[BenchmarkChunk] = []
        for asset in _chapter_first_assets(family):
            chunk_dicts = _run_worker_extract_chunks(asset.path, asset.label)
            for chunk in chunk_dicts:
                text = str(chunk.get("text") or "").strip()
                if not text:
                    continue
                metadata = dict(chunk.get("metadata", {}))
                metadata.update(
                    {
                        "family_id": family.family_id,
                        "asset_role": asset.asset_role,
                        "chapter_number": asset.chapter_number,
                    }
                )
                chunks.append(
                    BenchmarkChunk(
                        text=text,
                        label=(chunk.get("title") or asset.label),
                        token_count=_coerce_int(metadata.get("token_count")) or _count_tokens(text),
                        metadata=metadata,
                    )
                )
        return CandidateRun(
            candidate_id="prod_current_v1",
            input_source="chapter_first_pdf",
            extraction_method="worker_current",
            chunking_method="worker_current",
            chunks=chunks,
        )

    return _timed_candidate(_runner, candidate_id="prod_current_v1")


def run_pdftotext_floor_v1(family: BiblioFamilyBundle) -> CandidateRun:
    def _runner() -> CandidateRun:
        chunks: list[BenchmarkChunk] = []
        for asset in _chapter_first_assets(family):
            text = _extract_pdf_text(asset.path)
            paragraphs = _paragraph_split(text)
            for index, paragraph in enumerate(paragraphs):
                chunks.append(
                    BenchmarkChunk(
                        text=paragraph,
                        label=f"{asset.label} · Párrafo {index + 1}",
                        token_count=_count_tokens(paragraph),
                        metadata={
                            "family_id": family.family_id,
                            "asset_role": asset.asset_role,
                            "chapter_number": asset.chapter_number,
                        },
                    )
                )
        return CandidateRun(
            candidate_id="pdftotext_floor_v1",
            input_source="chapter_first_pdf",
            extraction_method="pdftotext",
            chunking_method="paragraph_floor",
            chunks=chunks,
        )

    return _timed_candidate(_runner, candidate_id="pdftotext_floor_v1")


def run_chapter_first_heading_v1(family: BiblioFamilyBundle) -> CandidateRun:
    def _runner() -> CandidateRun:
        chunks = _run_heading_candidate(
            family,
            assets=_chapter_first_assets(family),
            candidate_id="chapter_first_heading_v1",
        )
        return CandidateRun(
            candidate_id="chapter_first_heading_v1",
            input_source="chapter_first_pdf",
            extraction_method="pdftotext",
            chunking_method="biblio_heading",
            chunks=chunks,
        )

    return _timed_candidate(_runner, candidate_id="chapter_first_heading_v1")


def run_book_only_heading_v1(family: BiblioFamilyBundle) -> CandidateRun:
    def _runner() -> CandidateRun:
        chunks = _run_heading_candidate(
            family,
            assets=_book_priority_assets(family),
            candidate_id="book_only_heading_v1",
        )
        return CandidateRun(
            candidate_id="book_only_heading_v1",
            input_source="book_priority_pdf",
            extraction_method="pdftotext",
            chunking_method="biblio_heading",
            chunks=chunks,
        )

    return _timed_candidate(_runner, candidate_id="book_only_heading_v1")


def run_unstructured_title_text_v1(family: BiblioFamilyBundle) -> CandidateRun:
    def _runner() -> CandidateRun:
        try:
            from unstructured.chunking.title import chunk_by_title
            from unstructured.partition.text import partition_text
        except Exception as exc:
            return CandidateRun(
                candidate_id="unstructured_title_text_v1",
                input_source="chapter_first_text",
                extraction_method="pdftotext",
                chunking_method="unstructured_by_title",
                chunks=[],
                skipped=True,
                error=f"Unstructured unavailable: {exc}",
            )

        chunks: list[BenchmarkChunk] = []
        for asset in _chapter_first_assets(family):
            text = _extract_pdf_text(asset.path)
            if not text.strip():
                continue
            with _suppress_noisy_pdf_logs(include_unstructured=True):
                elements = partition_text(text=text)
                composites = chunk_by_title(
                    elements,
                    max_characters=2600,
                    new_after_n_chars=1800,
                    combine_text_under_n_chars=900,
                    overlap=120,
                    overlap_all=False,
                    multipage_sections=True,
                    include_orig_elements=True,
                )
            for index, element in enumerate(composites):
                chunk_text = str(getattr(element, "text", "") or "").strip()
                if not chunk_text:
                    continue
                metadata = {
                    "family_id": family.family_id,
                    "asset_role": asset.asset_role,
                    "chapter_number": asset.chapter_number,
                    "source": "unstructured_text",
                }
                chunks.append(
                    BenchmarkChunk(
                        text=chunk_text,
                        label=f"{asset.label} · Chunk {index + 1}",
                        token_count=_count_tokens(chunk_text),
                        metadata=metadata,
                    )
                )
        return CandidateRun(
            candidate_id="unstructured_title_text_v1",
            input_source="chapter_first_text",
            extraction_method="pdftotext",
            chunking_method="unstructured_by_title",
            chunks=chunks,
        )

    return _timed_candidate(_runner, candidate_id="unstructured_title_text_v1")


def run_cal_pymupdf_autoocr_heading_v1(family: BiblioFamilyBundle) -> CandidateRun:
    return _timed_candidate(
        lambda: _run_pymupdf_heading_candidate(
            family,
            candidate_id="cal_pymupdf_autoocr_heading_v1",
            extraction_method="pymupdf4llm_autoocr",
            settings={
                "page_chunks": True,
                "header": False,
                "footer": False,
                "ignore_code": True,
                "use_ocr": True,
                "force_ocr": False,
                "ocr_language": "spa+eng",
            },
        ),
        candidate_id="cal_pymupdf_autoocr_heading_v1",
    )


def run_cal_pymupdf_routed_heading_v1(family: BiblioFamilyBundle) -> CandidateRun:
    return _timed_candidate(
        lambda: _run_pymupdf_heading_candidate(
            family,
            candidate_id="cal_pymupdf_routed_heading_v1",
            extraction_method="pymupdf4llm_routed_ocr",
            settings={
                "page_chunks": True,
                "header": False,
                "footer": False,
                "ignore_code": True,
                "ocr_language": "spa+eng",
            },
            routed_ocr=True,
        ),
        candidate_id="cal_pymupdf_routed_heading_v1",
    )


def run_cal_pymupdf_unstructured_balanced_v1(family: BiblioFamilyBundle) -> CandidateRun:
    return _timed_candidate(
        lambda: _run_pymupdf_unstructured_candidate(
            family,
            candidate_id="cal_pymupdf_unstructured_balanced_v1",
            settings={
                "page_chunks": True,
                "header": False,
                "footer": False,
                "ignore_code": True,
                "ocr_language": "spa+eng",
            },
            chunking_config={
                "max_characters": 3000,
                "new_after_n_chars": 2200,
                "combine_text_under_n_chars": 1000,
                "overlap": 80,
                "overlap_all": False,
                "multipage_sections": True,
                "include_orig_elements": True,
            },
            routed_ocr=True,
        ),
        candidate_id="cal_pymupdf_unstructured_balanced_v1",
    )


def run_cal_pymupdf_unstructured_dense_v1(family: BiblioFamilyBundle) -> CandidateRun:
    return _timed_candidate(
        lambda: _run_pymupdf_unstructured_candidate(
            family,
            candidate_id="cal_pymupdf_unstructured_dense_v1",
            settings={
                "page_chunks": True,
                "header": False,
                "footer": False,
                "ignore_code": True,
                "ocr_language": "spa+eng",
            },
            chunking_config={
                "max_characters": 2200,
                "new_after_n_chars": 1500,
                "combine_text_under_n_chars": 700,
                "overlap": 100,
                "overlap_all": False,
                "multipage_sections": True,
                "include_orig_elements": True,
            },
            routed_ocr=True,
        ),
        candidate_id="cal_pymupdf_unstructured_dense_v1",
    )


def run_pymupdf4llm_md_v1(family: BiblioFamilyBundle) -> CandidateRun:
    def _runner() -> CandidateRun:
        try:
            import pymupdf4llm  # type: ignore
        except Exception as exc:
            return CandidateRun(
                candidate_id="pymupdf4llm_md_v1",
                input_source="chapter_first_pdf",
                extraction_method="pymupdf4llm",
                chunking_method="biblio_heading",
                chunks=[],
                skipped=True,
                error=f"PyMuPDF4LLM unavailable: {exc}",
            )

        chunks: list[BenchmarkChunk] = []
        for asset in _chapter_first_assets(family):
            try:
                markdown = _call_quietly(pymupdf4llm.to_markdown, str(asset.path))
            except Exception as exc:
                return CandidateRun(
                    candidate_id="pymupdf4llm_md_v1",
                    input_source="chapter_first_pdf",
                    extraction_method="pymupdf4llm",
                    chunking_method="biblio_heading",
                    chunks=[],
                    failed=True,
                    error=str(exc),
                )
            sections = _split_biblio_sections(markdown, asset.label)
            chunks.extend(_sections_to_chunks(sections, family, asset))
        return CandidateRun(
            candidate_id="pymupdf4llm_md_v1",
            input_source="chapter_first_pdf",
            extraction_method="pymupdf4llm",
            chunking_method="biblio_heading",
            chunks=chunks,
        )

    return _timed_candidate(_runner, candidate_id="pymupdf4llm_md_v1")


def run_pymupdf_unstructured_md_v1(family: BiblioFamilyBundle) -> CandidateRun:
    def _runner() -> CandidateRun:
        try:
            import pymupdf4llm  # type: ignore
            from unstructured.chunking.title import chunk_by_title
            from unstructured.partition.md import partition_md
        except Exception as exc:
            return CandidateRun(
                candidate_id="pymupdf_unstructured_md_v1",
                input_source="chapter_first_pdf",
                extraction_method="pymupdf4llm",
                chunking_method="unstructured_by_title",
                chunks=[],
                skipped=True,
                error=f"PyMuPDF4LLM/Unstructured unavailable: {exc}",
            )

        chunks: list[BenchmarkChunk] = []
        for asset in _chapter_first_assets(family):
            markdown = _call_quietly(pymupdf4llm.to_markdown, str(asset.path))
            with _suppress_noisy_pdf_logs(include_unstructured=True):
                elements = partition_md(text=markdown)
                composites = chunk_by_title(
                    elements,
                    max_characters=2600,
                    new_after_n_chars=1800,
                    combine_text_under_n_chars=900,
                    overlap=120,
                    overlap_all=False,
                    multipage_sections=True,
                    include_orig_elements=True,
                )
            for index, element in enumerate(composites):
                chunk_text = str(getattr(element, "text", "") or "").strip()
                if not chunk_text:
                    continue
                chunks.append(
                    BenchmarkChunk(
                        text=chunk_text,
                        label=f"{asset.label} · Chunk {index + 1}",
                        token_count=_count_tokens(chunk_text),
                        metadata={
                            "family_id": family.family_id,
                            "asset_role": asset.asset_role,
                            "chapter_number": asset.chapter_number,
                            "source": "pymupdf4llm_markdown",
                        },
                    )
                )
        return CandidateRun(
            candidate_id="pymupdf_unstructured_md_v1",
            input_source="chapter_first_pdf",
            extraction_method="pymupdf4llm",
            chunking_method="unstructured_by_title",
            chunks=chunks,
        )

    return _timed_candidate(_runner, candidate_id="pymupdf_unstructured_md_v1")


def run_ocrmypdf_pymupdf_v1(family: BiblioFamilyBundle) -> CandidateRun:
    def _runner() -> CandidateRun:
        if not _is_difficult_family(family):
            return CandidateRun(
                candidate_id="ocrmypdf_pymupdf_v1",
                input_source="chapter_first_pdf",
                extraction_method="ocrmypdf+pymupdf4llm",
                chunking_method="biblio_heading",
                chunks=[],
                skipped=True,
                error="Candidate only runs on difficult families",
            )
        try:
            import pymupdf4llm  # type: ignore
        except Exception as exc:
            return CandidateRun(
                candidate_id="ocrmypdf_pymupdf_v1",
                input_source="chapter_first_pdf",
                extraction_method="ocrmypdf+pymupdf4llm",
                chunking_method="biblio_heading",
                chunks=[],
                skipped=True,
                error=f"PyMuPDF4LLM unavailable: {exc}",
            )
        chunks: list[BenchmarkChunk] = []
        for asset in _chapter_first_assets(family):
            with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
                tmp_path = Path(tmp.name)
            try:
                _run_ocrmypdf(asset.path, tmp_path)
                markdown = _call_quietly(pymupdf4llm.to_markdown, str(tmp_path))
                sections = _split_biblio_sections(markdown, asset.label)
                chunks.extend(_sections_to_chunks(sections, family, asset))
            finally:
                if tmp_path.exists():
                    tmp_path.unlink()
        return CandidateRun(
            candidate_id="ocrmypdf_pymupdf_v1",
            input_source="chapter_first_pdf",
            extraction_method="ocrmypdf+pymupdf4llm",
            chunking_method="biblio_heading",
            chunks=chunks,
        )

    return _timed_candidate(_runner, candidate_id="ocrmypdf_pymupdf_v1")


def run_ocrmypdf_unstructured_md_v1(family: BiblioFamilyBundle) -> CandidateRun:
    def _runner() -> CandidateRun:
        if not _is_difficult_family(family):
            return CandidateRun(
                candidate_id="ocrmypdf_unstructured_md_v1",
                input_source="chapter_first_pdf",
                extraction_method="ocrmypdf+pymupdf4llm",
                chunking_method="unstructured_by_title",
                chunks=[],
                skipped=True,
                error="Candidate only runs on difficult families",
            )
        try:
            import pymupdf4llm  # type: ignore
            from unstructured.chunking.title import chunk_by_title
            from unstructured.partition.md import partition_md
        except Exception as exc:
            return CandidateRun(
                candidate_id="ocrmypdf_unstructured_md_v1",
                input_source="chapter_first_pdf",
                extraction_method="ocrmypdf+pymupdf4llm",
                chunking_method="unstructured_by_title",
                chunks=[],
                skipped=True,
                error=f"OCR/Unstructured stack unavailable: {exc}",
            )
        chunks: list[BenchmarkChunk] = []
        for asset in _chapter_first_assets(family):
            with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
                tmp_path = Path(tmp.name)
            try:
                _run_ocrmypdf(asset.path, tmp_path)
                markdown = _call_quietly(pymupdf4llm.to_markdown, str(tmp_path))
                with _suppress_noisy_pdf_logs(include_unstructured=True):
                    elements = partition_md(text=markdown)
                    composites = chunk_by_title(
                        elements,
                        max_characters=2600,
                        new_after_n_chars=1800,
                        combine_text_under_n_chars=900,
                        overlap=120,
                        overlap_all=False,
                        multipage_sections=True,
                        include_orig_elements=True,
                    )
                for index, element in enumerate(composites):
                    chunk_text = str(getattr(element, "text", "") or "").strip()
                    if not chunk_text:
                        continue
                    chunks.append(
                        BenchmarkChunk(
                            text=chunk_text,
                            label=f"{asset.label} · Chunk {index + 1}",
                            token_count=_count_tokens(chunk_text),
                            metadata={
                                "family_id": family.family_id,
                                "asset_role": asset.asset_role,
                                "chapter_number": asset.chapter_number,
                                "source": "ocrmypdf_markdown",
                            },
                        )
                    )
            finally:
                if tmp_path.exists():
                    tmp_path.unlink()
        return CandidateRun(
            candidate_id="ocrmypdf_unstructured_md_v1",
            input_source="chapter_first_pdf",
            extraction_method="ocrmypdf+pymupdf4llm",
            chunking_method="unstructured_by_title",
            chunks=chunks,
        )

    return _timed_candidate(_runner, candidate_id="ocrmypdf_unstructured_md_v1")


def run_docling_cpu_v1(family: BiblioFamilyBundle) -> CandidateRun:
    def _runner() -> CandidateRun:
        try:
            import tiktoken
            from docling_core.transforms.chunker.tokenizer.openai import OpenAITokenizer
        except Exception as exc:
            return CandidateRun(
                candidate_id="docling_cpu_v1",
                input_source="chapter_first_pdf",
                extraction_method="docling",
                chunking_method="docling_hybrid",
                chunks=[],
                skipped=True,
                error=f"Docling unavailable: {exc}",
            )
        chunks: list[BenchmarkChunk] = []
        with _suppress_noisy_pdf_logs():
            with _docling_offline_runtime():
                converter = _build_docling_converter(enable_remote_services=False)
                chunker = _build_docling_chunker()
                for asset in _chapter_first_assets(family):
                    doc = _call_quietly(converter.convert, source=str(asset.path)).document
                    for index, chunk in enumerate(_call_quietly(lambda: list(chunker.chunk(doc)))):
                        text = getattr(chunk, "text", None) or getattr(chunk, "content", None) or str(chunk)
                        if not str(text).strip():
                            continue
                        label = getattr(chunk, "heading", None) or getattr(chunk, "title", None) or f"{asset.label} · Chunk {index + 1}"
                        chunks.append(
                            BenchmarkChunk(
                                text=str(text).strip(),
                                label=str(label).strip(),
                                token_count=_count_tokens(str(text)),
                                metadata={
                                    "family_id": family.family_id,
                                    "asset_role": asset.asset_role,
                                    "chapter_number": asset.chapter_number,
                                    "source": "docling",
                                },
                            )
                        )
        return CandidateRun(
            candidate_id="docling_cpu_v1",
            input_source="chapter_first_pdf",
            extraction_method="docling",
            chunking_method="docling_hybrid",
            chunks=chunks,
        )

    return _timed_candidate(_runner, candidate_id="docling_cpu_v1")


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


def run_marker_cpu_subset_v1(family: BiblioFamilyBundle) -> CandidateRun:
    def _runner() -> CandidateRun:
        if not _is_difficult_family(family):
            return CandidateRun(
                candidate_id="marker_cpu_subset_v1",
                input_source="chapter_first_pdf",
                extraction_method="marker",
                chunking_method="marker_json",
                chunks=[],
                skipped=True,
                error="Candidate only runs on difficult families",
            )
        return CandidateRun(
            candidate_id="marker_cpu_subset_v1",
            input_source="chapter_first_pdf",
            extraction_method="marker",
            chunking_method="marker_json",
            chunks=[],
            skipped=True,
            error="Marker adapter is a benchmark placeholder until marker is installed locally",
        )

    return _timed_candidate(_runner, candidate_id="marker_cpu_subset_v1")


def build_classifier_context(family: BiblioFamilyBundle) -> str:
    parts: list[str] = [f"book_id={family.family_id}"]
    for asset in _classifier_assets(family):
        text = _extract_pdf_text(asset.path)
        excerpt = text.strip()[:4000]
        if excerpt:
            parts.append(f"[{asset.label}]\n{excerpt}")
    return "\n\n".join(parts).strip()


def run_metadata_rule_classifier(
    family: BiblioFamilyBundle,
    context: str,
) -> ClassificationPrediction:
    scores = _keyword_scores(context)
    if not scores:
        return ClassificationPrediction(
            classifier_id="metadata_rule_v1",
            family_id=family.family_id,
            primary_materia=None,
            secondary_materias=[],
            subject_tags=[],
            confidence=0.0,
            method="keyword_rules",
            reasoning="No legal-matter keywords found in extracted context",
            evidence_excerpt=context[:320] or None,
        )
    ranked = sorted(scores.items(), key=lambda item: item[1], reverse=True)
    primary = ranked[0][0]
    max_score = ranked[0][1]
    secondaries = [materia for materia, score in ranked[1:] if score >= max_score * 0.5][:3]
    confidence = min(0.95, 0.45 + (max_score / max(1, sum(scores.values()))) * 0.5)
    return ClassificationPrediction(
        classifier_id="metadata_rule_v1",
        family_id=family.family_id,
        primary_materia=primary,
        secondary_materias=secondaries,
        subject_tags=_subject_tags_from_context(context),
        confidence=round(confidence, 4),
        method="keyword_rules",
        reasoning=f"Top materia by keyword score: {primary} ({max_score})",
        evidence_excerpt=context[:320] or None,
    )


def run_hybrid_classifier(
    family: BiblioFamilyBundle,
    context: str,
) -> ClassificationPrediction:
    rule_prediction = run_metadata_rule_classifier(family, context)
    if rule_prediction.confidence >= 0.8 or not context.strip():
        rule_prediction.classifier_id = "hybrid_classifier_v1"
        rule_prediction.method = "hybrid_rule_fastpath"
        return rule_prediction

    agentic = run_ollama_agentic_classifier(family, context)
    if agentic.skipped or agentic.error:
        rule_prediction.classifier_id = "hybrid_classifier_v1"
        rule_prediction.method = "hybrid_rule_fallback"
        return rule_prediction

    primary = agentic.primary_materia or rule_prediction.primary_materia
    secondary_pool = list(dict.fromkeys(rule_prediction.secondary_materias + agentic.secondary_materias))
    confidence = max(rule_prediction.confidence, agentic.confidence)
    if primary and rule_prediction.primary_materia == primary:
        confidence = min(0.98, confidence + 0.08)
    return ClassificationPrediction(
        classifier_id="hybrid_classifier_v1",
        family_id=family.family_id,
        primary_materia=primary,
        secondary_materias=secondary_pool[:4],
        subject_tags=list(dict.fromkeys(rule_prediction.subject_tags + agentic.subject_tags))[:8],
        confidence=round(confidence, 4),
        method="hybrid_rule_plus_ollama",
        reasoning=f"Rule={rule_prediction.primary_materia} / Agentic={agentic.primary_materia}",
        evidence_excerpt=context[:320] or None,
    )


def run_ollama_agentic_classifier(
    family: BiblioFamilyBundle,
    context: str,
) -> ClassificationPrediction:
    model = os.environ.get("BIBLIO_BENCHMARK_OLLAMA_MODEL", "qwen2.5:7b-instruct")
    host = os.environ.get("OLLAMA_HOST", "http://127.0.0.1:11434").rstrip("/")
    prompt = _build_ollama_prompt(context)
    payload = {
        "model": model,
        "prompt": prompt,
        "stream": False,
        "format": {
            "type": "object",
            "properties": {
                "primary_materia": {"type": ["string", "null"]},
                "secondary_materias": {"type": "array", "items": {"type": "string"}},
                "subject_tags": {"type": "array", "items": {"type": "string"}},
                "confidence": {"type": "number"},
                "reasoning": {"type": "string"},
            },
            "required": ["primary_materia", "secondary_materias", "subject_tags", "confidence", "reasoning"],
        },
    }
    request = urllib.request.Request(
        f"{host}/api/generate",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            raw = json.loads(response.read().decode("utf-8"))
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        return ClassificationPrediction(
            classifier_id="ollama_agentic_v1",
            family_id=family.family_id,
            primary_materia=None,
            secondary_materias=[],
            subject_tags=[],
            confidence=0.0,
            method="ollama_local",
            skipped=True,
            error=f"Ollama unavailable: {exc}",
        )
    except Exception as exc:
        return ClassificationPrediction(
            classifier_id="ollama_agentic_v1",
            family_id=family.family_id,
            primary_materia=None,
            secondary_materias=[],
            subject_tags=[],
            confidence=0.0,
            method="ollama_local",
            error=str(exc),
        )

    try:
        parsed = json.loads(raw.get("response", "{}"))
    except Exception as exc:
        return ClassificationPrediction(
            classifier_id="ollama_agentic_v1",
            family_id=family.family_id,
            primary_materia=None,
            secondary_materias=[],
            subject_tags=[],
            confidence=0.0,
            method="ollama_local",
            error=f"Failed to parse Ollama JSON: {exc}",
        )

    primary = parsed.get("primary_materia")
    if primary not in LEGAL_MATERIAS:
        primary = None
    secondaries = [item for item in parsed.get("secondary_materias", []) if item in LEGAL_MATERIAS and item != primary]
    confidence = float(parsed.get("confidence") or 0.0)
    return ClassificationPrediction(
        classifier_id="ollama_agentic_v1",
        family_id=family.family_id,
        primary_materia=primary,
        secondary_materias=secondaries[:4],
        subject_tags=[str(item).strip() for item in parsed.get("subject_tags", []) if str(item).strip()][:8],
        confidence=max(0.0, min(confidence, 1.0)),
        method="ollama_local",
        reasoning=str(parsed.get("reasoning") or "").strip() or None,
        evidence_excerpt=context[:320] or None,
    )


def run_setfit_zero_shot_classifier(
    family: BiblioFamilyBundle,
    context: str,
) -> ClassificationPrediction:
    model_name = os.environ.get("BIBLIO_BENCHMARK_SETFIT_MODEL", "").strip()
    if not model_name:
        return ClassificationPrediction(
            classifier_id="setfit_zero_shot_v1",
            family_id=family.family_id,
            primary_materia=None,
            secondary_materias=[],
            subject_tags=[],
            confidence=0.0,
            method="setfit_zero_shot",
            skipped=True,
            error="BIBLIO_BENCHMARK_SETFIT_MODEL is not configured",
        )
    try:
        from setfit import SetFitModel  # type: ignore
    except Exception as exc:
        return ClassificationPrediction(
            classifier_id="setfit_zero_shot_v1",
            family_id=family.family_id,
            primary_materia=None,
            secondary_materias=[],
            subject_tags=[],
            confidence=0.0,
            method="setfit_zero_shot",
            skipped=True,
            error=f"SetFit unavailable: {exc}",
        )

    try:
        model = SetFitModel.from_pretrained(model_name)
        probabilities = model.predict_proba([context], as_numpy=True)[0].tolist()
        labels = list(getattr(model.model_head, "classes_", LEGAL_MATERIAS))
    except Exception as exc:
        return ClassificationPrediction(
            classifier_id="setfit_zero_shot_v1",
            family_id=family.family_id,
            primary_materia=None,
            secondary_materias=[],
            subject_tags=[],
            confidence=0.0,
            method="setfit_zero_shot",
            error=str(exc),
        )

    scored = sorted(
        [(label, float(score)) for label, score in zip(labels, probabilities) if label in LEGAL_MATERIAS],
        key=lambda item: item[1],
        reverse=True,
    )
    if not scored:
        return ClassificationPrediction(
            classifier_id="setfit_zero_shot_v1",
            family_id=family.family_id,
            primary_materia=None,
            secondary_materias=[],
            subject_tags=[],
            confidence=0.0,
            method="setfit_zero_shot",
        )
    primary, confidence = scored[0]
    secondaries = [label for label, score in scored[1:] if score >= 0.3][:4]
    return ClassificationPrediction(
        classifier_id="setfit_zero_shot_v1",
        family_id=family.family_id,
        primary_materia=primary,
        secondary_materias=secondaries,
        subject_tags=_subject_tags_from_context(context),
        confidence=round(confidence, 4),
        method="setfit_zero_shot",
        reasoning=f"Top SetFit probability: {primary}",
        evidence_excerpt=context[:320] or None,
    )


def build_family_queries(family: BiblioFamilyBundle) -> list[BenchmarkQuery]:
    sections = _reference_sections_for_family(family)
    substantial = [section for section in sections if len(section.get("text", "")) >= 80]
    queries: list[BenchmarkQuery] = []
    if not substantial:
        return queries

    intro = substantial[0]
    queries.append(
        BenchmarkQuery(
            query_id=f"{family.family_id}-family",
            query_type="family_identification",
            text=f"{family.family_id} {_salient_query_terms(intro.get('text', ''), limit=10)}".strip(),
            target_label=intro.get("label"),
            target_anchor=_anchor_from_text(intro.get("text", "")),
        )
    )

    if family.chapter_count:
        chapter_sections = [section for section in substantial if "Capítulo" in (section.get("label") or "")]
        target = chapter_sections[0] if chapter_sections else substantial[min(len(substantial) - 1, 1)]
        queries.append(
            BenchmarkQuery(
                query_id=f"{family.family_id}-chapter",
                query_type="chapter_lookup",
                text=(target.get("label") or "") + " " + _salient_query_terms(target.get("text", ""), limit=8),
                target_label=target.get("label"),
                target_anchor=_anchor_from_text(target.get("text", "")),
            )
        )

    longest = max(substantial, key=lambda section: len(section.get("text", "")))
    queries.append(
        BenchmarkQuery(
            query_id=f"{family.family_id}-doctrine",
            query_type="doctrinal_section",
            text=_salient_query_terms(longest.get("text", ""), limit=12),
            target_label=longest.get("label"),
            target_anchor=_anchor_from_text(longest.get("text", "")),
        )
    )
    return queries


def analyze_candidate_run(
    family: BiblioFamilyBundle,
    inventory_row: BiblioInventoryRow,
    run: CandidateRun,
    queries: Sequence[BenchmarkQuery],
    spec: CandidateSpec,
) -> PerDocumentResult:
    chunk_dicts = [chunk.to_dict() for chunk in run.chunks]
    token_values = [chunk.token_count for chunk in run.chunks]
    token_stats = _summarize_numbers(token_values)
    structure_metrics = evaluate_structure_metrics(family, run.chunks)
    retrieval_metrics = evaluate_retrieval_metrics(run.chunks, queries)
    total_tokens = sum(token_values)
    return PerDocumentResult(
        candidate_id=run.candidate_id,
        lane="biblio",
        document_id=family.family_id,
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
            "chapter_count": inventory_row.chapter_count,
            "pdf_size_bytes": inventory_row.total_pdf_size_bytes,
            "sample_extractable_chars": inventory_row.sample_extractable_chars,
        },
        warnings=run.warnings,
        failed=run.failed,
        skipped=run.skipped,
        error=run.error,
        chunks=chunk_dicts,
    )


def evaluate_structure_metrics(
    family: BiblioFamilyBundle,
    chunks: Sequence[BenchmarkChunk],
) -> dict[str, Any]:
    if not chunks:
        return {
            "chapter_coverage": 0.0,
            "duplicate_ratio": 0.0,
            "exact_duplicate_ratio": 0.0,
            "sentence_fracture_rate": 1.0,
            "repeated_header_ratio": 0.0,
            "tiny_chunk_ratio": 0.0,
            "oversized_chunk_ratio": 0.0,
            "structure_score": 0.0,
        }

    chapter_numbers = {asset.chapter_number for asset in _chapter_assets(family) if asset.chapter_number}
    recovered = {
        chunk.metadata.get("chapter_number")
        for chunk in chunks
        if chunk.metadata.get("chapter_number")
    }
    chapter_coverage = _ratio(len(chapter_numbers & recovered), len(chapter_numbers)) if chapter_numbers else 1.0
    duplicate_metrics = _duplicate_metrics(chunks)
    tiny_chunk_ratio = _ratio(sum(1 for chunk in chunks if chunk.token_count < 120), len(chunks))
    oversized_chunk_ratio = _ratio(sum(1 for chunk in chunks if chunk.token_count > 900), len(chunks))
    sentence_fracture_rate = _ratio(sum(1 for chunk in chunks if _appears_sentence_fractured(chunk.text)), len(chunks))
    repeated_header_ratio = _repeated_header_ratio(chunks)
    structure_score = round(
        (
            chapter_coverage * 0.2
            + (1.0 - duplicate_metrics["duplicate_ratio"]) * 0.25
            + (1.0 - duplicate_metrics["exact_duplicate_ratio"]) * 0.15
            + (1.0 - sentence_fracture_rate) * 0.15
            + (1.0 - repeated_header_ratio) * 0.1
            + (1.0 - min(1.0, tiny_chunk_ratio + oversized_chunk_ratio)) * 0.15
        ),
        4,
    )
    return {
        "chapter_coverage": round(chapter_coverage, 4),
        "duplicate_ratio": round(duplicate_metrics["duplicate_ratio"], 4),
        "exact_duplicate_ratio": round(duplicate_metrics["exact_duplicate_ratio"], 4),
        "sentence_fracture_rate": round(sentence_fracture_rate, 4),
        "repeated_header_ratio": round(repeated_header_ratio, 4),
        "tiny_chunk_ratio": round(tiny_chunk_ratio, 4),
        "oversized_chunk_ratio": round(oversized_chunk_ratio, 4),
        "structure_score": structure_score,
    }


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
        (
            "chapter_coverage",
            "duplicate_ratio",
            "exact_duplicate_ratio",
            "sentence_fracture_rate",
            "repeated_header_ratio",
            "tiny_chunk_ratio",
            "oversized_chunk_ratio",
            "structure_score",
        ),
    )
    retrieval_metrics = _average_metric_dict(
        [result.retrieval_metrics for result in successful],
        ("recall_at_1", "recall_at_3", "recall_at_5", "mrr_at_5"),
    )
    ops_metrics = _average_metric_dict(
        [result.resource_metrics for result in successful],
        ("processing_time_ms", "rss_mb", "chapter_count", "pdf_size_bytes", "sample_extractable_chars"),
    )
    projected_hours = None
    if ops_metrics.get("processing_time_ms"):
        projected_hours = round((ops_metrics["processing_time_ms"] * corpus_size) / 3_600_000, 3)

    weighted_score = round(
        (
            structure_metrics.get("structure_score", 0.0) * 0.45
            + retrieval_metrics.get("recall_at_5", 0.0) * 0.35
            + success_rate * 0.20
        ),
        4,
    )

    decision_status = "candidate"
    if not successful:
        decision_status = "blocked"
    elif not _within_runtime_gate(projected_hours):
        decision_status = "over_budget"
    elif (
        success_rate == 1.0
        and structure_metrics.get("structure_score", 0.0) >= 0.75
    ):
        decision_status = "finalist"

    return AggregateReport(
        candidate_id=candidate_id,
        lane="biblio",
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


def aggregate_classifier_predictions(
    classifier_id: str,
    predictions: Sequence[ClassificationPrediction],
    silver_labels: dict[str, str],
) -> ClassificationAggregate:
    non_skipped = [prediction for prediction in predictions if not prediction.skipped]
    successful = [prediction for prediction in non_skipped if not prediction.error]
    success_rate = _ratio(len(successful), len(non_skipped)) if non_skipped else 0.0
    confidences = [prediction.confidence for prediction in successful]
    avg_confidence = round(statistics.mean(confidences), 4) if confidences else 0.0
    high_confidence_rate = round(_ratio(sum(1 for value in confidences if value >= 0.75), len(confidences)), 4) if confidences else 0.0

    agreement_samples = [
        prediction
        for prediction in successful
        if prediction.family_id in silver_labels and prediction.primary_materia
    ]
    agreement = None
    if agreement_samples:
        agreement = round(
            _ratio(
                sum(1 for prediction in agreement_samples if prediction.primary_materia == silver_labels[prediction.family_id]),
                len(agreement_samples),
            ),
            4,
        )

    decision_status = "candidate"
    if success_rate == 1.0 and high_confidence_rate >= 0.6:
        decision_status = "finalist"
    if not successful:
        decision_status = "blocked"

    return ClassificationAggregate(
        classifier_id=classifier_id,
        families_run=len(predictions),
        success_rate=round(success_rate, 4),
        high_confidence_rate=high_confidence_rate,
        avg_confidence=avg_confidence,
        agreement_with_silver=agreement,
        decision_status=decision_status,
        skipped_families=sum(1 for prediction in predictions if prediction.skipped),
        failed_families=sum(1 for prediction in predictions if prediction.error),
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
            aggregate.structure_metrics.get("structure_score", 0.0),
            aggregate.retrieval_metrics.get("recall_at_5", 0.0),
            -aggregate.structure_metrics.get("duplicate_ratio", 1.0),
            -aggregate.ops_metrics.get("processing_time_ms", float("inf")),
        ),
        reverse=True,
    )
    for aggregate in ranked:
        if not _winner_eligible(aggregate):
            continue
        aggregate.decision_status = "winner"
        break
    return ranked


def rank_classifier_aggregates(aggregates: Sequence[ClassificationAggregate]) -> list[ClassificationAggregate]:
    ranked = sorted(
        aggregates,
        key=lambda aggregate: (
            aggregate.decision_status != "blocked",
            aggregate.success_rate,
            aggregate.high_confidence_rate,
            aggregate.agreement_with_silver if aggregate.agreement_with_silver is not None else -1.0,
            aggregate.avg_confidence,
        ),
        reverse=True,
    )
    if ranked and ranked[0].decision_status != "blocked":
        ranked[0].decision_status = "winner"
    return ranked


def render_summary_markdown(
    ranked: Sequence[AggregateReport],
    sample_sets: dict[str, list[str]],
    inventory: Sequence[BiblioInventoryRow],
) -> str:
    lines = [
        "# Biblio Chunk Benchmark Summary",
        "",
        f"- Family inventory rows: {len(inventory)}",
        f"- Runtime gate: <= {BIBLIO_RUNTIME_GATE_HOURS}h",
        f"- Smoke sample: {len(sample_sets.get('smoke', []))}",
        f"- Tuning sample: {len(sample_sets.get('tuning', []))}",
        f"- Holdout sample: {len(sample_sets.get('holdout', []))}",
        "",
        "| Candidate | Status | Success | Structure | Recall@5 | Dup Ratio | Avg ms/family | Projected hours |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for aggregate in ranked:
        lines.append(
            "| {candidate} | {status} | {success:.2f} | {structure:.2f} | {recall:.2f} | {dup:.2f} | {time:.0f} | {hours} |".format(
                candidate=aggregate.candidate_id,
                status=aggregate.decision_status,
                success=aggregate.success_rate,
                structure=aggregate.structure_metrics.get("structure_score", 0.0),
                recall=aggregate.retrieval_metrics.get("recall_at_5", 0.0),
                dup=aggregate.structure_metrics.get("duplicate_ratio", 0.0),
                time=aggregate.ops_metrics.get("processing_time_ms", 0.0),
                hours=aggregate.projected_full_corpus_runtime_hours if aggregate.projected_full_corpus_runtime_hours is not None else "n/a",
            )
        )
    return "\n".join(lines) + "\n"


def render_holdout_review(
    holdout_ids: Sequence[str],
    family_map: dict[str, BiblioFamilyBundle],
    results_by_candidate: dict[str, list[PerDocumentResult]],
    top_aggregates: Sequence[AggregateReport],
) -> str:
    lines = [
        "# Biblio Holdout Review Sheet",
        "",
        "Use this sheet to manually compare the top chunking candidates.",
        "",
    ]
    top_ids = {aggregate.candidate_id for aggregate in top_aggregates}
    results_index = {
        (result.candidate_id, result.document_id): result
        for results in results_by_candidate.values()
        for result in results
        if result.candidate_id in top_ids
    }
    for family_id in holdout_ids:
        family = family_map[family_id]
        lines.extend(
            [
                f"## Family {family_id}",
                f"- Topology: `{family.topology}`",
                f"- Book PDF: `{family.book_pdf_path}`" if family.book_pdf_path else "- Book PDF: none",
                f"- Chapters: {family.chapter_count}",
                "",
            ]
        )
        for candidate_id in top_ids:
            result = results_index.get((candidate_id, family_id))
            if result is None:
                continue
            lines.append(f"### {candidate_id}")
            lines.append(
                f"- Structure score: {result.structure_metrics.get('structure_score', 0.0):.2f}"
            )
            lines.append(
                f"- Duplicate ratio: {result.structure_metrics.get('duplicate_ratio', 0.0):.2f}"
            )
            lines.append(f"- Recall@5: {result.retrieval_metrics.get('recall_at_5', 0.0):.2f}")
            preview = result.chunks[0]["text"][:500] if result.chunks else ""
            lines.append(f"- First chunk preview: `{preview}`")
            lines.append("")
    return "\n".join(lines) + "\n"


def render_classification_review(
    holdout_ids: Sequence[str],
    family_map: dict[str, BiblioFamilyBundle],
    predictions_by_classifier: dict[str, list[ClassificationPrediction]],
) -> str:
    lines = [
        "# Biblio Classification Review Sheet",
        "",
        "Use this sheet to compare classifier outputs on the holdout families.",
        "",
    ]
    index = {
        (prediction.classifier_id, prediction.family_id): prediction
        for predictions in predictions_by_classifier.values()
        for prediction in predictions
    }
    classifier_ids = sorted(predictions_by_classifier.keys())
    for family_id in holdout_ids:
        family = family_map[family_id]
        lines.extend(
            [
                f"## Family {family_id}",
                f"- Topology: `{family.topology}`",
                f"- Chapters: {family.chapter_count}",
                "",
            ]
        )
        for classifier_id in classifier_ids:
            prediction = index.get((classifier_id, family_id))
            if prediction is None:
                continue
            lines.append(f"### {classifier_id}")
            lines.append(f"- Primary materia: `{prediction.primary_materia}`")
            lines.append(f"- Secondary materias: `{', '.join(prediction.secondary_materias)}`")
            lines.append(f"- Subject tags: `{', '.join(prediction.subject_tags)}`")
            lines.append(f"- Confidence: {prediction.confidence:.2f}")
            if prediction.reasoning:
                lines.append(f"- Reasoning: {prediction.reasoning}")
            if prediction.error:
                lines.append(f"- Error: {prediction.error}")
            lines.append("")
    return "\n".join(lines) + "\n"


def _chapter_assets(family: BiblioFamilyBundle) -> list[BiblioAsset]:
    assets: list[BiblioAsset] = []
    for path in family.chapter_pdf_paths:
        chapter_number = _chapter_number_from_path(path)
        assets.append(
            BiblioAsset(
                path=path,
                asset_role="chapter",
                label=f"Capítulo {chapter_number or path.stem}",
                chapter_number=chapter_number,
            )
        )
    return assets


def _chapter_first_assets(family: BiblioFamilyBundle) -> list[BiblioAsset]:
    assets = _chapter_assets(family)
    if assets:
        return assets
    if family.book_pdf_path:
        return [BiblioAsset(path=family.book_pdf_path, asset_role="book", label=f"Libro {family.family_id}")]
    return []


def _book_priority_assets(family: BiblioFamilyBundle) -> list[BiblioAsset]:
    if family.book_pdf_path:
        return [BiblioAsset(path=family.book_pdf_path, asset_role="book", label=f"Libro {family.family_id}")]
    return _chapter_assets(family)


def _classifier_assets(family: BiblioFamilyBundle) -> list[BiblioAsset]:
    assets = _chapter_first_assets(family)
    if len(assets) > 4:
        middle = assets[len(assets) // 2]
        assets = [assets[0], middle, assets[-1]]
    return assets


def _sample_asset(family: BiblioFamilyBundle) -> BiblioAsset | None:
    assets = _chapter_first_assets(family)
    return assets[0] if assets else None


def _chapter_number_from_path(path: Path) -> str | None:
    stem = path.stem
    match = re.match(r"(\d+[a-z]?)", stem, re.IGNORECASE)
    if match:
        return match.group(1)
    match = re.search(r"(\d+[a-z]?)_(\d+)$", stem, re.IGNORECASE)
    if match:
        return match.group(1)
    return None


def _chapter_path_sort_key(path: Path) -> tuple[int, str]:
    chapter = _chapter_number_from_path(path)
    if chapter:
        numeric = re.match(r"(\d+)", chapter)
        if numeric:
            return (int(numeric.group(1)), chapter)
    return (10_000, path.name)


def _pymupdf_extract_markdown(
    asset: BiblioAsset,
    *,
    settings: dict[str, Any],
    routed_ocr: bool = False,
) -> str:
    import pymupdf4llm  # type: ignore

    effective_settings = dict(settings)
    if routed_ocr:
        effective_settings.update(_routed_ocr_settings(asset.path))
    markdown = _call_quietly(pymupdf4llm.to_markdown, str(asset.path), **effective_settings)
    return _coerce_markdown_payload(markdown)


def _coerce_markdown_payload(payload: Any) -> str:
    if isinstance(payload, str):
        return payload
    if isinstance(payload, list):
        texts: list[str] = []
        for item in payload:
            if isinstance(item, dict):
                text = str(item.get("text") or "").strip()
            else:
                text = str(item).strip()
            if text:
                texts.append(text)
        return "\n\n".join(texts).strip()
    return str(payload or "").strip()


def _routed_ocr_settings(pdf_path: Path) -> dict[str, Any]:
    extractable_chars = _sample_extractable_chars(pdf_path)
    encrypted = (_pdfinfo(pdf_path).get("Encrypted") or "").lower()
    if extractable_chars >= 1200 and not encrypted.startswith("yes"):
        return {"use_ocr": False, "force_ocr": False}
    if extractable_chars == 0 or encrypted.startswith("yes"):
        return {"use_ocr": True, "force_ocr": True}
    return {"use_ocr": True, "force_ocr": False}


def _run_pymupdf_heading_candidate(
    family: BiblioFamilyBundle,
    *,
    candidate_id: str,
    extraction_method: str,
    settings: dict[str, Any],
    routed_ocr: bool = False,
) -> CandidateRun:
    try:
        import pymupdf4llm  # type: ignore  # noqa: F401
    except Exception as exc:
        return CandidateRun(
            candidate_id=candidate_id,
            input_source="chapter_first_pdf",
            extraction_method=extraction_method,
            chunking_method="biblio_heading",
            chunks=[],
            skipped=True,
            error=f"PyMuPDF4LLM unavailable: {exc}",
        )

    chunks: list[BenchmarkChunk] = []
    for asset in _chapter_first_assets(family):
        try:
            markdown = _pymupdf_extract_markdown(asset, settings=settings, routed_ocr=routed_ocr)
        except Exception as exc:
            return CandidateRun(
                candidate_id=candidate_id,
                input_source="chapter_first_pdf",
                extraction_method=extraction_method,
                chunking_method="biblio_heading",
                chunks=[],
                failed=True,
                error=str(exc),
            )
        sections = _split_biblio_sections(markdown, asset.label)
        chunks.extend(_sections_to_chunks(sections, family, asset))
    return CandidateRun(
        candidate_id=candidate_id,
        input_source="chapter_first_pdf",
        extraction_method=extraction_method,
        chunking_method="biblio_heading",
        chunks=chunks,
    )


def _run_pymupdf_unstructured_candidate(
    family: BiblioFamilyBundle,
    *,
    candidate_id: str,
    settings: dict[str, Any],
    chunking_config: dict[str, Any],
    routed_ocr: bool = False,
) -> CandidateRun:
    try:
        import pymupdf4llm  # type: ignore  # noqa: F401
        from unstructured.chunking.title import chunk_by_title
        from unstructured.partition.md import partition_md
    except Exception as exc:
        return CandidateRun(
            candidate_id=candidate_id,
            input_source="chapter_first_pdf",
            extraction_method="pymupdf4llm",
            chunking_method="unstructured_by_title",
            chunks=[],
            skipped=True,
            error=f"PyMuPDF4LLM/Unstructured unavailable: {exc}",
        )

    chunks: list[BenchmarkChunk] = []
    for asset in _chapter_first_assets(family):
        try:
            markdown = _pymupdf_extract_markdown(asset, settings=settings, routed_ocr=routed_ocr)
        except Exception as exc:
            return CandidateRun(
                candidate_id=candidate_id,
                input_source="chapter_first_pdf",
                extraction_method="pymupdf4llm",
                chunking_method="unstructured_by_title",
                chunks=[],
                failed=True,
                error=str(exc),
            )
        if not markdown.strip():
            continue
        elements = partition_md(text=markdown)
        composites = chunk_by_title(elements, **chunking_config)
        for index, element in enumerate(composites):
            chunk_text = str(getattr(element, "text", "") or "").strip()
            if not chunk_text:
                continue
            chunks.append(
                BenchmarkChunk(
                    text=chunk_text,
                    label=f"{asset.label} · Chunk {index + 1}",
                    token_count=_count_tokens(chunk_text),
                    metadata={
                        "family_id": family.family_id,
                        "asset_role": asset.asset_role,
                        "chapter_number": asset.chapter_number,
                        "source": "pymupdf4llm_markdown",
                    },
                )
            )
    return CandidateRun(
        candidate_id=candidate_id,
        input_source="chapter_first_pdf",
        extraction_method="pymupdf4llm",
        chunking_method="unstructured_by_title",
        chunks=chunks,
    )


def _extract_pdf_text(pdf_path: Path) -> str:
    result = subprocess.run(
        ["pdftotext", str(pdf_path), "-"],
        capture_output=True,
        check=False,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or f"pdftotext failed for {pdf_path.name}")
    return result.stdout


def _sample_extractable_chars(pdf_path: Path) -> int:
    result = subprocess.run(
        ["pdftotext", "-f", "1", "-l", "3", str(pdf_path), "-"],
        capture_output=True,
        check=False,
        text=True,
    )
    if result.returncode != 0:
        return 0
    return len(result.stdout.strip())


def _pdfinfo(pdf_path: Path) -> dict[str, str]:
    result = subprocess.run(
        ["pdfinfo", str(pdf_path)],
        capture_output=True,
        check=False,
        text=True,
    )
    if result.returncode != 0:
        return {}
    info: dict[str, str] = {}
    for line in result.stdout.splitlines():
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        info[key.strip()] = value.strip()
    return info


def _assign_bucket(
    rows: Sequence[BiblioInventoryRow],
    getter: Callable[[BiblioInventoryRow], int | float],
    attr_name: str,
    thresholds: Sequence[int | float],
    labels: Sequence[str],
) -> None:
    for row in rows:
        value = getter(row)
        bucket = labels[-1]
        for index, threshold in enumerate(thresholds):
            if value <= threshold:
                bucket = labels[index]
                break
        setattr(row, attr_name, bucket)


def _coerce_text(value: Any) -> str | None:
    text = str(value).strip() if value is not None else ""
    return text or None


def _normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").strip().lower())


def _tokenize_text(value: str) -> list[str]:
    return [token.lower() for token in TOKEN_RE.findall(value or "") if token.lower() not in STOPWORDS]


def _keyword_scores(context: str) -> dict[str, int]:
    text = _normalize_text(context)
    scores: dict[str, int] = {}
    for materia, keywords in MATERIA_KEYWORDS.items():
        score = sum(text.count(keyword.lower()) for keyword in keywords)
        if score:
            scores[materia] = score
    return scores


def _subject_tags_from_context(context: str, limit: int = 8) -> list[str]:
    tokens = _tokenize_text(context)
    counts = Counter(token for token in tokens if len(token) > 4)
    return [token for token, _ in counts.most_common(limit)]


def _derive_silver_label(
    predictions_by_classifier: dict[str, list[ClassificationPrediction]],
    family_id: str,
) -> str | None:
    rule_predictions = {
        prediction.classifier_id: prediction
        for predictions in predictions_by_classifier.values()
        for prediction in predictions
        if prediction.family_id == family_id and prediction.primary_materia
    }
    rule = rule_predictions.get("metadata_rule_v1")
    hybrid = rule_predictions.get("hybrid_classifier_v1")
    if rule and hybrid and rule.primary_materia == hybrid.primary_materia and min(rule.confidence, hybrid.confidence) >= 0.75:
        return rule.primary_materia
    if rule and rule.confidence >= 0.85:
        return rule.primary_materia
    return None


def _build_ollama_prompt(context: str) -> str:
    materias = ", ".join(LEGAL_MATERIAS)
    return (
        "Clasifica este libro jurídico en una o más materias legales mexicanas.\n"
        f"Materias válidas: {materias}.\n"
        "Responde solo JSON con primary_materia, secondary_materias, subject_tags, confidence y reasoning.\n"
        "Usa confidence baja si el extracto no alcanza.\n\n"
        f"CONTEXTO:\n{context[:12000]}"
    )


def _run_heading_candidate(
    family: BiblioFamilyBundle,
    *,
    assets: Sequence[BiblioAsset],
    candidate_id: str,
) -> list[BenchmarkChunk]:
    chunks: list[BenchmarkChunk] = []
    for asset in assets:
        text = _extract_pdf_text(asset.path)
        sections = _split_biblio_sections(text, asset.label)
        chunks.extend(_sections_to_chunks(sections, family, asset))
    return chunks


def _sections_to_chunks(
    sections: Sequence[dict[str, Any]],
    family: BiblioFamilyBundle,
    asset: BiblioAsset,
) -> list[BenchmarkChunk]:
    chunks: list[BenchmarkChunk] = []
    for section in sections:
        label = section.get("label") or asset.label
        text = section.get("text", "").strip()
        if not text:
            continue
        chunks.extend(
            _split_with_overlap(
                text,
                label=label,
                metadata={
                    "family_id": family.family_id,
                    "asset_role": asset.asset_role,
                    "chapter_number": asset.chapter_number,
                    "heading_path": list(section.get("heading_path", [])),
                },
                target_max_tokens=650,
                max_chunk_tokens=800,
                overlap_tokens=80,
            )
        )
    return chunks


def _split_biblio_sections(text: str, fallback_label: str) -> list[dict[str, Any]]:
    lines = text.splitlines()
    sections: list[dict[str, Any]] = []
    current_heading = fallback_label
    current_heading_path = [fallback_label]
    current_lines: list[str] = []

    def flush() -> None:
        nonlocal current_lines
        payload = "\n".join(line for line in current_lines if line.strip()).strip()
        if payload:
            sections.append(
                {
                    "label": current_heading,
                    "text": payload,
                    "heading_path": list(current_heading_path),
                }
            )
        current_lines = []

    for raw_line in lines:
        line = raw_line.strip()
        if not line:
            flush()
            continue
        if _looks_like_heading(line):
            flush()
            heading = _clean_heading(line)
            current_heading = heading
            current_heading_path = [fallback_label, heading]
            continue
        current_lines.append(line)
    flush()
    if not sections:
        paragraphs = _paragraph_split(text)
        return [{"label": fallback_label, "text": paragraph, "heading_path": [fallback_label]} for paragraph in paragraphs]
    return sections


def _looks_like_heading(line: str) -> bool:
    stripped = line.strip()
    if not stripped:
        return False
    if SECTION_HEADING_RE.match(stripped):
        return True
    if ALL_CAPS_RE.match(stripped) and len(stripped.split()) <= 16:
        return True
    if NUMERIC_SECTION_RE.match(stripped) and len(stripped.split()) <= 18:
        return True
    return False


def _clean_heading(line: str) -> str:
    return re.sub(r"\s+", " ", line.strip()).strip(":-")


def _reference_sections_for_family(family: BiblioFamilyBundle) -> list[dict[str, Any]]:
    sections: list[dict[str, Any]] = []
    for asset in _classifier_assets(family):
        text = _extract_pdf_text(asset.path)
        if not text.strip():
            continue
        asset_sections = _split_biblio_sections(text[:12_000], asset.label)
        sections.extend(asset_sections[:4])
    if not sections:
        return [{"label": f"Libro {family.family_id}", "text": family.family_id}]
    return sections


def _duplicate_metrics(chunks: Sequence[BenchmarkChunk]) -> dict[str, float]:
    if not chunks:
        return {"duplicate_ratio": 0.0, "exact_duplicate_ratio": 0.0}
    normalized = [_normalize_text(chunk.text) for chunk in chunks if chunk.text.strip()]
    if not normalized:
        return {"duplicate_ratio": 0.0, "exact_duplicate_ratio": 0.0}
    exact_duplicates = len(normalized) - len(set(normalized))
    near_duplicate_hits = 0
    fingerprints: list[set[str]] = []
    for text in normalized:
        shingles = _text_shingles(text)
        if any(_jaccard(shingles, prior) >= 0.9 for prior in fingerprints):
            near_duplicate_hits += 1
        fingerprints.append(shingles)
    return {
        "duplicate_ratio": _ratio(near_duplicate_hits, len(normalized)),
        "exact_duplicate_ratio": _ratio(exact_duplicates, len(normalized)),
    }


def _text_shingles(text: str, n: int = 5) -> set[str]:
    tokens = _tokenize_text(text)
    if len(tokens) < n:
        return {" ".join(tokens)} if tokens else set()
    return {" ".join(tokens[index : index + n]) for index in range(len(tokens) - n + 1)}


def _jaccard(left: set[str], right: set[str]) -> float:
    if not left and not right:
        return 1.0
    if not left or not right:
        return 0.0
    return len(left & right) / len(left | right)


def _appears_sentence_fractured(text: str) -> bool:
    stripped = text.strip()
    if not stripped:
        return False
    return stripped[-1] not in ".!?:;”\"')" and len(stripped.split()) > 12


def _repeated_header_ratio(chunks: Sequence[BenchmarkChunk]) -> float:
    starts = [chunk.text[:80].strip().lower() for chunk in chunks if chunk.text.strip()]
    if not starts:
        return 0.0
    duplicates = sum(count - 1 for count in Counter(starts).values() if count > 1)
    return _ratio(duplicates, len(starts))


def _chunk_matches_query_target(chunk: BenchmarkChunk, query: BenchmarkQuery) -> bool:
    if query.target_label and _normalize_text(query.target_label) in _normalize_text(chunk.label or ""):
        return True
    if query.target_anchor and _normalize_text(query.target_anchor) in _normalize_text(chunk.text):
        return True
    return False


def _is_difficult_family(family: BiblioFamilyBundle) -> bool:
    sample_asset = _sample_asset(family)
    if sample_asset is None:
        return False
    extractable_chars = _sample_extractable_chars(sample_asset.path)
    if extractable_chars == 0:
        return True
    if family.total_pdf_size_bytes > 50 * 1024 * 1024:
        return True
    encrypted = (_pdfinfo(sample_asset.path).get("Encrypted") or "").lower()
    return encrypted.startswith("yes")


def _run_ocrmypdf(source_path: Path, target_path: Path) -> None:
    executable = shutil.which("ocrmypdf")
    if not executable:
        raise RuntimeError("ocrmypdf binary is not installed")
    result = subprocess.run(
        [
            executable,
            "--skip-text",
            "--optimize",
            "0",
            str(source_path),
            str(target_path),
        ],
        capture_output=True,
        check=False,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "ocrmypdf failed")


def _default_manifest(
    sample_sets: dict[str, list[str]],
    candidate_ids: Sequence[str],
    classifier_ids: Sequence[str],
    seed: int,
) -> BenchmarkManifest:
    manifest = BenchmarkManifest(
        mode="biblio",
        candidate_set="custom",
        split_seed=seed,
        sample_sets=sample_sets,
        candidate_ids=list(candidate_ids),
        resource_limits={
            "max_rss_mb": 24 * 1024,
            "max_projected_hours": 36,
        },
        acceptance_thresholds={
            "structure_score_min": 0.75,
            "duplicate_ratio_max": 0.015,
            "sentence_fracture_rate_max": 0.05,
            "repeated_header_ratio_max": 0.03,
            "recall_at_5_min": 0.75,
            "classifier_high_confidence_rate_min": 0.60,
        },
        query_set_version=QUERY_SET_VERSION,
    )
    payload = manifest.to_dict()
    payload["classifier_ids"] = list(classifier_ids)
    payload["benchmark_type"] = "biblio_family"
    return _ManifestCompat(payload)


@dataclass
class _ManifestCompat:
    payload: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return self.payload
