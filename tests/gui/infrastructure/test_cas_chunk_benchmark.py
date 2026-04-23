import json
import logging
from pathlib import Path

import src.gui.infrastructure.cas_chunk_benchmark as cas_bench
from src.gui.infrastructure.cas_chunk_benchmark import (
    AggregateReport,
    _markdown_sections,
    CandidateSpec,
    InventoryRow,
    build_candidate_specs,
    build_difficult_splits,
    build_document_queries,
    discover_cas_bundles,
    evaluate_retrieval_metrics,
    rank_aggregates,
    run_benchmark,
    run_canonical_section_v1,
    run_markdown_cas_v1,
    run_unified_current_v1,
    _suppress_noisy_pdf_logs,
)


def _write_cas_bundle(root: Path, document_id: str, heading: str = "INTRODUCTION") -> None:
    pdf_root = root / "cas_pdfs"
    pdf_root.mkdir(parents=True, exist_ok=True)
    (pdf_root / f"{document_id}.pdf").write_bytes(b"%PDF-1.4 test\n")

    converted = root / "cas" / "converted" / document_id
    converted.mkdir(parents=True, exist_ok=True)
    canonical = {
        "metadata": {
            "title": f"CAS {document_id} Example Award",
            "publication_date": "2021-05-18",
            "subject_matters": ["Football"],
            "thesis_key": f"2019/A/{document_id}",
            "extra": {
                "language": "en",
                "procedure": "A",
                "keywords": ["disciplinary", "appeal"],
            },
        },
        "sections": [
            {
                "type": "heading",
                "heading_path": ["Court of Arbitration for Sport"],
                "text": "Court of Arbitration for Sport",
                "blocks": [],
            },
            {
                "type": "paragraph",
                "heading_path": [],
                "text": "Arbitration CAS 2019/A/{0} Club A v. Federation B, award of 18 May 2021".format(
                    document_id
                ),
                "blocks": [{"page": 1}],
            },
            {
                "type": "paragraph",
                "heading_path": [],
                "text": "1.",
                "blocks": [{"page": 1}],
            },
            {
                "type": "paragraph",
                "heading_path": [],
                "text": "The appeal concerns a disciplinary sanction imposed after a match incident.",
                "blocks": [{"page": 1}],
            },
            {
                "type": "heading",
                "heading_path": [heading],
                "text": heading,
                "blocks": [],
            },
            {
                "type": "paragraph",
                "heading_path": [heading],
                "text": "The Panel finds that the sanction must be partially upheld based on proportionality.",
                "blocks": [{"page": 2}],
            },
            {
                "type": "heading",
                "heading_path": ["DECISION"],
                "text": "DECISION",
                "blocks": [],
            },
            {
                "type": "paragraph",
                "heading_path": ["DECISION"],
                "text": "The appeal is partially upheld and the federation is ordered to amend the sanction.",
                "blocks": [{"page": 3}],
            },
        ],
    }
    (converted / "canonical.json").write_text(json.dumps(canonical), encoding="utf-8")
    (converted / "meta.json").write_text(
        json.dumps({"status": "success", "conversion_time_seconds": 1.23}),
        encoding="utf-8",
    )
    markdown = """---
document_id: {doc}
document_type: tesis
title: CAS {doc} Example Award
publication_date: 2021-05-18
subjects: [Football]
thesis_key: 2019/A/{doc}
---

# Court of Arbitration for Sport

Arbitration CAS 2019/A/{doc} Club A v. Federation B, award of 18 May 2021

1.

The appeal concerns a disciplinary sanction imposed after a match incident.

# {heading}

The Panel finds that the sanction must be partially upheld based on proportionality.

# DECISION

The appeal is partially upheld and the federation is ordered to amend the sanction.
""".format(doc=document_id, heading=heading)
    (converted / "output.md").write_text(markdown, encoding="utf-8")
    (converted / "output.txt").write_text(
        "\n".join(
            [
                "Court of Arbitration for Sport",
                "Arbitration CAS 2019/A/{0} Club A v. Federation B, award of 18 May 2021".format(document_id),
                "FACTS",
                "The appeal concerns a disciplinary sanction imposed after a match incident.",
                "REASONS",
                "The Panel finds that the sanction must be partially upheld based on proportionality.",
                "DECISION",
                "The appeal is partially upheld and the federation is ordered to amend the sanction.",
            ]
        ),
        encoding="utf-8",
    )


def test_discover_cas_bundles_reads_pdf_and_converted_layout(tmp_path):
    _write_cas_bundle(tmp_path, "6547")

    bundles = discover_cas_bundles(tmp_path)

    assert [bundle.document_id for bundle in bundles] == ["6547"]
    assert bundles[0].canonical_json_path is not None
    assert bundles[0].output_md_path is not None
    assert bundles[0].output_txt_path is not None


def test_markdown_sections_merge_number_markers(tmp_path):
    _write_cas_bundle(tmp_path, "6547")
    bundle = discover_cas_bundles(tmp_path)[0]

    sections = _markdown_sections(bundle)

    assert any(section["type"] == "heading" and section["label"] == "DECISION" for section in sections)
    assert any(
        section["type"] == "paragraph"
        and section["text"].startswith("1. The appeal concerns")
        for section in sections
    )


def test_candidate_runners_use_converted_artifacts(tmp_path):
    _write_cas_bundle(tmp_path, "6547")
    bundle = discover_cas_bundles(tmp_path)[0]

    canonical_run = run_canonical_section_v1(bundle)
    markdown_run = run_markdown_cas_v1(bundle)
    unified_run = run_unified_current_v1(bundle)

    assert canonical_run.failed is False
    assert markdown_run.failed is False
    assert unified_run.failed is False
    assert any("partially upheld" in chunk.text for chunk in canonical_run.chunks)
    assert any("DECISION" in (chunk.label or "") for chunk in markdown_run.chunks)
    assert any("DECISION" in (chunk.label or "") for chunk in unified_run.chunks)


def test_query_generation_and_retrieval_metrics(tmp_path):
    _write_cas_bundle(tmp_path, "6547")
    bundle = discover_cas_bundles(tmp_path)[0]
    candidate_run = run_canonical_section_v1(bundle)
    queries = build_document_queries(bundle)

    metrics = evaluate_retrieval_metrics(candidate_run.chunks, queries)

    assert metrics["query_count"] >= 2
    assert metrics["recall_at_5"] > 0


def test_run_benchmark_writes_artifacts(tmp_path):
    data_root = tmp_path / "data"
    _write_cas_bundle(data_root, "6547")
    _write_cas_bundle(data_root, "3580", heading="FACTUAL BACKGROUND")
    output_dir = tmp_path / "benchmark"

    results = run_benchmark(
        output_dir=output_dir,
        data_root=data_root,
        smoke_size=1,
        tuning_size=1,
        holdout_size=0,
        candidate_ids=["canonical_section_v1", "markdown_cas_v1", "unified_current_v1"],
        include_pdf_info=False,
    )

    assert Path(results["output_dir"]) == output_dir
    assert (output_dir / "manifest.json").exists()
    assert (output_dir / "inventory.jsonl").exists()
    assert (output_dir / "queries.jsonl").exists()
    assert (output_dir / "progress.json").exists()
    assert (output_dir / "summary.md").exists()
    assert (output_dir / "candidates" / "canonical_section_v1" / "results.jsonl").exists()
    assert (output_dir / "candidates" / "canonical_section_v1" / "status.json").exists()

    manifest = json.loads((output_dir / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["mode"] == "artifact_chunking"
    assert manifest["candidate_set"] == "all"
    progress = json.loads((output_dir / "progress.json").read_text(encoding="utf-8"))
    assert progress["phase"] == "completed"


def test_run_benchmark_resets_artifacts_before_rerun(tmp_path):
    data_root = tmp_path / "data"
    _write_cas_bundle(data_root, "6547")
    output_dir = tmp_path / "benchmark"

    run_benchmark(
        output_dir=output_dir,
        data_root=data_root,
        candidate_ids=["canonical_section_v1"],
        doc_ids=["6547"],
        include_pdf_info=False,
    )
    run_benchmark(
        output_dir=output_dir,
        data_root=data_root,
        candidate_ids=["canonical_section_v1"],
        doc_ids=["6547"],
        include_pdf_info=False,
    )

    results_path = output_dir / "candidates" / "canonical_section_v1" / "results.jsonl"
    lines = [line for line in results_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert len(lines) == 1


def test_run_benchmark_records_failed_candidate_exception(monkeypatch, tmp_path):
    data_root = tmp_path / "data"
    _write_cas_bundle(data_root, "6547")
    output_dir = tmp_path / "benchmark"

    def _boom(_bundle):
        raise RuntimeError("candidate boom")

    monkeypatch.setattr(
        cas_bench,
        "build_candidate_registry",
        lambda: {"canonical_section_v1": _boom},
    )

    results = cas_bench.run_benchmark(
        output_dir=output_dir,
        data_root=data_root,
        candidate_ids=["canonical_section_v1"],
        doc_ids=["6547"],
        include_pdf_info=False,
    )

    rows = [
        json.loads(line)
        for line in (
            output_dir / "candidates" / "canonical_section_v1" / "results.jsonl"
        ).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert len(rows) == 1
    assert rows[0]["failed"] is True
    assert "candidate boom" in (rows[0]["error"] or "")
    assert results["aggregates"][0]["decision_status"] == "blocked"


def test_run_benchmark_survives_query_generation_exception(monkeypatch, tmp_path):
    data_root = tmp_path / "data"
    _write_cas_bundle(data_root, "6547")
    output_dir = tmp_path / "benchmark"

    monkeypatch.setattr(cas_bench, "build_document_queries", lambda _bundle: (_ for _ in ()).throw(RuntimeError("query boom")))

    results = cas_bench.run_benchmark(
        output_dir=output_dir,
        data_root=data_root,
        candidate_ids=["canonical_section_v1"],
        doc_ids=["6547"],
        include_pdf_info=False,
    )

    rows = [
        json.loads(line)
        for line in (
            output_dir / "candidates" / "canonical_section_v1" / "results.jsonl"
        ).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert len(rows) == 1
    assert any("query_generation_failed: query boom" in warning for warning in rows[0]["warnings"])
    assert results["aggregates"][0]["documents_run"] == 1


def test_run_benchmark_caches_queries_and_writes_them_before_candidate_runs(monkeypatch, tmp_path):
    data_root = tmp_path / "data"
    _write_cas_bundle(data_root, "6547")
    output_dir = tmp_path / "benchmark"
    call_count = {"count": 0}

    def _build_queries(_bundle):
        call_count["count"] += 1
        return [
            cas_bench.BenchmarkQuery(
                query_id="q1",
                query_type="case_identification",
                text="disciplinary sanction",
                target_label="INTRODUCTION",
                target_anchor="disciplinary",
            )
        ]

    def _runner(_bundle):
        query_lines = [
            line for line in (output_dir / "queries.jsonl").read_text(encoding="utf-8").splitlines() if line.strip()
        ]
        assert len(query_lines) == 1
        return cas_bench.CandidateRun(
            candidate_id="canonical_section_v1",
            input_source="canonical_json",
            extraction_method="stub",
            chunking_method="stub",
            chunks=[
                cas_bench.BenchmarkChunk(
                    text="The appeal concerns a disciplinary sanction.",
                    label="INTRODUCTION",
                    token_count=6,
                )
            ],
        )

    monkeypatch.setattr(cas_bench, "build_document_queries", _build_queries)
    monkeypatch.setattr(
        cas_bench,
        "build_candidate_registry",
        lambda: {
            "canonical_section_v1": _runner,
            "markdown_cas_v1": _runner,
        },
    )

    cas_bench.run_benchmark(
        output_dir=output_dir,
        data_root=data_root,
        candidate_ids=["canonical_section_v1", "markdown_cas_v1"],
        doc_ids=["6547"],
        include_pdf_info=False,
    )

    assert call_count["count"] == 1


def test_build_candidate_specs_exposes_lane_and_hosting_metadata():
    specs = build_candidate_specs()

    assert isinstance(specs["canonical_section_v1"], CandidateSpec)
    assert specs["canonical_section_v1"].lane == "artifact_chunking"
    assert specs["pymupdf_unstructured_v1"].lane == "raw_pdf_extraction"
    assert specs["pymupdf_unstructured_v1"].ocr_languages == ("eng", "fra")


def test_suppress_noisy_pdf_logs_restores_logger_levels():
    pdfminer_logger = logging.getLogger("pdfminer.pdfinterp")
    original_level = pdfminer_logger.level

    with _suppress_noisy_pdf_logs():
        assert pdfminer_logger.level == logging.ERROR

    assert pdfminer_logger.level == original_level


def test_build_difficult_splits_prioritizes_french_and_low_density():
    rows = [
        InventoryRow(
            document_id="easy-en",
            pdf_path="/tmp/easy-en.pdf",
            converted_dir="/tmp/easy-en",
            title="Easy EN",
            publication_date="2021-01-01",
            language="en",
            procedure="A",
            sport="Football",
            subject_keywords=[],
            page_count=10,
            pdf_size_bytes=1000,
            output_txt_bytes=9000,
            text_density=900.0,
            year_bucket="2020s",
            encrypted_pdf=False,
            page_bucket="q1",
            density_bucket="q4",
        ),
        InventoryRow(
            document_id="fr-hard",
            pdf_path="/tmp/fr-hard.pdf",
            converted_dir="/tmp/fr-hard",
            title="FR hard",
            publication_date="2021-01-01",
            language="fr",
            procedure="A",
            sport="Football",
            subject_keywords=[],
            page_count=200,
            pdf_size_bytes=2_000_000,
            output_txt_bytes=2000,
            text_density=10.0,
            year_bucket="2020s",
            encrypted_pdf=False,
            page_bucket="q4",
            density_bucket="q1",
        ),
        InventoryRow(
            document_id="enc-hard",
            pdf_path="/tmp/enc-hard.pdf",
            converted_dir="/tmp/enc-hard",
            title="Encrypted",
            publication_date="2021-01-01",
            language="en",
            procedure="A",
            sport="Football",
            subject_keywords=[],
            page_count=150,
            pdf_size_bytes=1_800_000,
            output_txt_bytes=3000,
            text_density=20.0,
            year_bucket="2020s",
            encrypted_pdf=True,
            page_bucket="q4",
            density_bucket="q1",
        ),
    ]

    splits = build_difficult_splits(rows, seed=42, smoke_size=1, tuning_size=1, holdout_size=0)
    selected = splits["smoke"] + splits["tuning"]

    assert "fr-hard" in selected
    assert "enc-hard" in selected


def test_run_benchmark_filters_candidates_by_mode(tmp_path):
    data_root = tmp_path / "data"
    _write_cas_bundle(data_root, "6547")
    _write_cas_bundle(data_root, "3580", heading="FACTUAL BACKGROUND")
    output_dir = tmp_path / "benchmark_raw"

    results = run_benchmark(
        output_dir=output_dir,
        data_root=data_root,
        smoke_size=1,
        tuning_size=0,
        holdout_size=0,
        mode="raw_pdf_extraction",
        candidate_ids=["canonical_section_v1", "unstructured_pdf_fast_v1"],
        include_pdf_info=False,
    )

    assert results["mode"] == "raw_pdf_extraction"
    assert results["candidate_ids"] == ["unstructured_pdf_fast_v1"]


def test_rank_aggregates_skips_over_budget_winner_assignment():
    reports = [
        AggregateReport(
            candidate_id="canonical_section_v1",
            lane="artifact_chunking",
            documents_run=2,
            success_rate=1.0,
            self_hosted_status="local_only",
            requires_remote_service=False,
            ocr_languages=[],
            manual_scores={},
            structure_metrics={"structure_score": 0.8},
            retrieval_metrics={"recall_at_5": 0.9},
            ops_metrics={"processing_time_ms": 1000},
            projected_full_corpus_runtime_hours=2.0,
            decision_status="finalist",
            weighted_score=0.8,
        ),
        AggregateReport(
            candidate_id="docling_cpu_hybrid_v1",
            lane="raw_pdf_extraction",
            documents_run=2,
            success_rate=1.0,
            self_hosted_status="local_only",
            requires_remote_service=False,
            ocr_languages=[],
            manual_scores={},
            structure_metrics={"structure_score": 0.81},
            retrieval_metrics={"recall_at_5": 0.91},
            ops_metrics={"processing_time_ms": 1000},
            projected_full_corpus_runtime_hours=30.0,
            decision_status="finalist",
            weighted_score=0.95,
        ),
    ]

    ranked = rank_aggregates(reports)

    status_by_candidate = {report.candidate_id: report.decision_status for report in ranked}
    assert status_by_candidate["canonical_section_v1"] == "winner"
    assert status_by_candidate["docling_cpu_hybrid_v1"] == "over_budget"
