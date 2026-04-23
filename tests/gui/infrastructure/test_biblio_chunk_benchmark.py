import json
from pathlib import Path

from src.gui.infrastructure import biblio_chunk_benchmark as bench
from src.gui.infrastructure.cas_chunk_benchmark import AggregateReport


def _write_biblio_family(
    root: Path,
    family_id: str,
    *,
    include_book: bool = False,
    chapters: tuple[str, ...] = (),
    include_cover: bool = True,
) -> None:
    family_dir = root / "books" / family_id
    family_dir.mkdir(parents=True, exist_ok=True)
    if include_book:
        (family_dir / "book.pdf").write_bytes(b"%PDF-1.4 test\n")
    if chapters:
        chapters_dir = family_dir / "chapters"
        chapters_dir.mkdir(parents=True, exist_ok=True)
        for chapter in chapters:
            (chapters_dir / chapter).write_bytes(b"%PDF-1.4 chapter\n")
    if include_cover:
        (family_dir / "cover.jpg").write_bytes(b"jpeg")


def _fake_extract_text(pdf_path: Path) -> str:
    rel = str(pdf_path).replace("\\", "/")
    if rel.endswith("/100/book.pdf"):
        return (
            "LIBRO 100\n"
            "CAPÍTULO I\n"
            "Arrendamiento, obligaciones y propiedad civil.\n\n"
            "CAPÍTULO II\n"
            "Contratos civiles, posesión y compraventa.\n"
        )
    if rel.endswith("/100/chapters/1.pdf"):
        return "CAPÍTULO I\nArrendamiento, obligaciones y propiedad civil.\n"
    if rel.endswith("/100/chapters/2.pdf"):
        return "CAPÍTULO II\nContratos civiles, posesión y compraventa.\n"
    if rel.endswith("/200/book.pdf"):
        return "LIBRO 200\nDespido, trabajador, salario, prestaciones, IMSS e Infonavit.\n"
    if rel.endswith("/300/chapters/1.pdf"):
        return "CAPÍTULO I\nDelito, penal, ministerio público y sentencia penal.\n"
    return "CAPÍTULO ÚNICO\nTexto genérico de prueba.\n"


def test_discover_biblio_families_detects_topologies(tmp_path):
    _write_biblio_family(tmp_path, "100", include_book=True, chapters=("1.pdf", "2.pdf"))
    _write_biblio_family(tmp_path, "200", include_book=True)
    _write_biblio_family(tmp_path, "300", chapters=("1.pdf",))
    _write_biblio_family(tmp_path, "400", include_cover=True)

    families = bench.discover_biblio_families(tmp_path)
    topology = {family.family_id: family.topology for family in families}

    assert topology["100"] == "both"
    assert topology["200"] == "book_only"
    assert topology["300"] == "chapter_only"
    assert topology["400"] == "incomplete"


def test_chapter_first_heading_candidate_prefers_chapters(monkeypatch, tmp_path):
    _write_biblio_family(tmp_path, "100", include_book=True, chapters=("1.pdf", "2.pdf"))
    family = bench.discover_biblio_families(tmp_path)[0]

    monkeypatch.setattr(bench, "_extract_pdf_text", _fake_extract_text)

    run = bench.run_chapter_first_heading_v1(family)

    assert run.failed is False
    assert run.skipped is False
    assert run.chunks
    assert all(chunk.metadata["asset_role"] == "chapter" for chunk in run.chunks)
    assert {chunk.metadata["chapter_number"] for chunk in run.chunks} == {"1", "2"}


def test_metadata_rule_classifier_detects_laboral(tmp_path):
    _write_biblio_family(tmp_path, "200", include_book=True)
    family = bench.discover_biblio_families(tmp_path)[0]
    context = (
        "Despido injustificado, trabajador, salario, prestaciones, IMSS e Infonavit. "
        "Relación laboral, subordinación y vacaciones."
    )

    prediction = bench.run_metadata_rule_classifier(family, context)

    assert prediction.primary_materia == "laboral"
    assert prediction.confidence > 0.5


def test_candidate_registry_includes_calibrated_variants():
    registry = bench.build_candidate_registry()

    for candidate_id in (
        "cal_pymupdf_autoocr_heading_v1",
        "cal_pymupdf_routed_heading_v1",
        "cal_pymupdf_unstructured_balanced_v1",
        "cal_pymupdf_unstructured_dense_v1",
    ):
        assert candidate_id in registry


def test_candidate_specs_expose_metadata():
    specs = bench.build_candidate_specs()

    assert specs["chapter_first_heading_v1"].self_hosted_status == "local_only"
    assert bench.DEFAULT_CORE_CANDIDATES[-1] == "cal_pymupdf_unstructured_balanced_v1"
    assert specs["pymupdf_unstructured_md_v1"].ocr_languages == ("spa", "eng")
    assert "Legacy" in specs["pymupdf_unstructured_md_v1"].description
    assert "Recommended" in specs["cal_pymupdf_unstructured_balanced_v1"].description
    assert specs["marker_cpu_subset_v1"].subset_only is True


def test_run_benchmark_writes_artifacts(monkeypatch, tmp_path):
    data_root = tmp_path / "data"
    _write_biblio_family(data_root, "100", include_book=True, chapters=("1.pdf", "2.pdf"))
    _write_biblio_family(data_root, "200", include_book=True)
    output_dir = tmp_path / "benchmark"

    monkeypatch.setattr(bench, "_extract_pdf_text", _fake_extract_text)
    monkeypatch.setattr(bench, "_sample_extractable_chars", lambda _path: 2400)
    monkeypatch.setattr(
        bench,
        "_pdfinfo",
        lambda _path: {"Pages": "12", "Encrypted": "no", "Producer": "pytest"},
    )
    monkeypatch.setattr(
        bench,
        "run_ollama_agentic_classifier",
        lambda family, context: bench.ClassificationPrediction(
            classifier_id="ollama_agentic_v1",
            family_id=family.family_id,
            primary_materia="civil" if family.family_id == "100" else "laboral",
            secondary_materias=[],
            subject_tags=["prueba"],
            confidence=0.72,
            method="ollama_local",
            reasoning="stubbed",
            evidence_excerpt=context[:120],
        ),
    )

    results = bench.run_benchmark(
        output_dir=output_dir,
        data_root=data_root,
        smoke_size=1,
        tuning_size=1,
        holdout_size=0,
        candidate_ids=["chapter_first_heading_v1", "book_only_heading_v1"],
        classifier_ids=["metadata_rule_v1", "hybrid_classifier_v1"],
        include_pdf_info=True,
    )

    assert Path(results["output_dir"]) == output_dir
    assert (output_dir / "manifest.json").exists()
    assert (output_dir / "inventory.jsonl").exists()
    assert (output_dir / "queries.jsonl").exists()
    assert (output_dir / "progress.json").exists()
    assert (output_dir / "summary.md").exists()
    assert (output_dir / "review_holdout.md").exists()
    assert (output_dir / "classifiers" / "aggregate.json").exists()
    assert (output_dir / "classifiers" / "metadata_rule_v1.jsonl").exists()
    assert (output_dir / "candidates" / "chapter_first_heading_v1" / "results.jsonl").exists()
    assert (output_dir / "candidates" / "chapter_first_heading_v1" / "status.json").exists()
    assert (output_dir / "classifiers" / "metadata_rule_v1" / "status.json").exists()
    assert results["aggregates"]
    assert results["classifier_aggregates"]

    aggregate = json.loads((output_dir / "classifiers" / "aggregate.json").read_text(encoding="utf-8"))
    assert aggregate[0]["classifier_id"] in {"metadata_rule_v1", "hybrid_classifier_v1"}
    progress = json.loads((output_dir / "progress.json").read_text(encoding="utf-8"))
    assert progress["phase"] == "completed"


def test_run_benchmark_family_ids_short_circuits_inventory_scope(monkeypatch, tmp_path):
    data_root = tmp_path / "data"
    _write_biblio_family(data_root, "100", include_book=True, chapters=("1.pdf",))
    _write_biblio_family(data_root, "200", include_book=True)
    output_dir = tmp_path / "benchmark"

    monkeypatch.setattr(bench, "_extract_pdf_text", _fake_extract_text)
    monkeypatch.setattr(bench, "_sample_extractable_chars", lambda _path: 2400)
    monkeypatch.setattr(
        bench,
        "_pdfinfo",
        lambda _path: {"Pages": "12", "Encrypted": "no", "Producer": "pytest"},
    )
    monkeypatch.setattr(
        bench,
        "run_ollama_agentic_classifier",
        lambda family, context: bench.ClassificationPrediction(
            classifier_id="ollama_agentic_v1",
            family_id=family.family_id,
            primary_materia="civil",
            secondary_materias=[],
            subject_tags=["prueba"],
            confidence=0.72,
            method="ollama_local",
            reasoning="stubbed",
            evidence_excerpt=context[:120],
        ),
    )

    results = bench.run_benchmark(
        output_dir=output_dir,
        data_root=data_root,
        candidate_ids=["chapter_first_heading_v1"],
        classifier_ids=["metadata_rule_v1"],
        family_ids=["100"],
        include_pdf_info=True,
    )

    inventory_rows = [
        json.loads(line)
        for line in (output_dir / "inventory.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert results["sample_sets"] == {"selected": ["100"]}
    assert [row["family_id"] for row in inventory_rows] == ["100"]


def test_run_benchmark_resets_artifacts_before_rerun(monkeypatch, tmp_path):
    data_root = tmp_path / "data"
    _write_biblio_family(data_root, "100", include_book=True, chapters=("1.pdf",))
    output_dir = tmp_path / "benchmark"

    monkeypatch.setattr(bench, "_extract_pdf_text", _fake_extract_text)
    monkeypatch.setattr(bench, "_sample_extractable_chars", lambda _path: 2400)
    monkeypatch.setattr(
        bench,
        "_pdfinfo",
        lambda _path: {"Pages": "12", "Encrypted": "no", "Producer": "pytest"},
    )

    for _ in range(2):
        bench.run_benchmark(
            output_dir=output_dir,
            data_root=data_root,
            candidate_ids=["chapter_first_heading_v1"],
            classifier_ids=["metadata_rule_v1"],
            family_ids=["100"],
            include_pdf_info=True,
        )

    candidate_lines = [
        line
        for line in (
            output_dir / "candidates" / "chapter_first_heading_v1" / "results.jsonl"
        ).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    classifier_lines = [
        line
        for line in (output_dir / "classifiers" / "metadata_rule_v1.jsonl")
        .read_text(encoding="utf-8")
        .splitlines()
        if line.strip()
    ]
    assert len(candidate_lines) == 1
    assert len(classifier_lines) == 1


def test_run_benchmark_records_failed_candidate_and_classifier_exceptions(monkeypatch, tmp_path):
    data_root = tmp_path / "data"
    _write_biblio_family(data_root, "100", include_book=True, chapters=("1.pdf",))
    output_dir = tmp_path / "benchmark"

    monkeypatch.setattr(bench, "_extract_pdf_text", _fake_extract_text)
    monkeypatch.setattr(bench, "_sample_extractable_chars", lambda _path: 2400)
    monkeypatch.setattr(
        bench,
        "_pdfinfo",
        lambda _path: {"Pages": "12", "Encrypted": "no", "Producer": "pytest"},
    )
    monkeypatch.setattr(
        bench,
        "build_candidate_registry",
        lambda: {
            "chapter_first_heading_v1": lambda _family: (_ for _ in ()).throw(
                RuntimeError("candidate boom")
            )
        },
    )
    monkeypatch.setattr(
        bench,
        "build_classifier_registry",
        lambda: {
            "metadata_rule_v1": lambda _family, _context: (_ for _ in ()).throw(
                RuntimeError("classifier boom")
            )
        },
    )

    results = bench.run_benchmark(
        output_dir=output_dir,
        data_root=data_root,
        candidate_ids=["chapter_first_heading_v1"],
        classifier_ids=["metadata_rule_v1"],
        family_ids=["100"],
        include_pdf_info=True,
    )

    candidate_rows = [
        json.loads(line)
        for line in (
            output_dir / "candidates" / "chapter_first_heading_v1" / "results.jsonl"
        ).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    classifier_rows = [
        json.loads(line)
        for line in (output_dir / "classifiers" / "metadata_rule_v1.jsonl")
        .read_text(encoding="utf-8")
        .splitlines()
        if line.strip()
    ]
    assert len(candidate_rows) == 1
    assert candidate_rows[0]["failed"] is True
    assert "candidate boom" in (candidate_rows[0]["error"] or "")
    assert len(classifier_rows) == 1
    assert "classifier boom" in (classifier_rows[0]["error"] or "")
    assert results["aggregates"][0]["decision_status"] == "blocked"
    assert results["classifier_aggregates"][0]["decision_status"] == "blocked"


def test_run_benchmark_survives_query_generation_exception(monkeypatch, tmp_path):
    data_root = tmp_path / "data"
    _write_biblio_family(data_root, "100", include_book=True, chapters=("1.pdf",))
    output_dir = tmp_path / "benchmark"

    monkeypatch.setattr(bench, "_extract_pdf_text", _fake_extract_text)
    monkeypatch.setattr(bench, "_sample_extractable_chars", lambda _path: 2400)
    monkeypatch.setattr(
        bench,
        "_pdfinfo",
        lambda _path: {"Pages": "12", "Encrypted": "no", "Producer": "pytest"},
    )
    monkeypatch.setattr(
        bench,
        "build_family_queries",
        lambda _family: (_ for _ in ()).throw(RuntimeError("query boom")),
    )

    results = bench.run_benchmark(
        output_dir=output_dir,
        data_root=data_root,
        candidate_ids=["chapter_first_heading_v1"],
        classifier_ids=["metadata_rule_v1"],
        family_ids=["100"],
        include_pdf_info=True,
    )

    candidate_rows = [
        json.loads(line)
        for line in (
            output_dir / "candidates" / "chapter_first_heading_v1" / "results.jsonl"
        ).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert len(candidate_rows) == 1
    assert any(
        "query_generation_failed: query boom" in warning
        for warning in candidate_rows[0]["warnings"]
    )
    assert results["aggregates"][0]["documents_run"] == 1


def test_run_benchmark_writes_queries_before_candidate_runs(monkeypatch, tmp_path):
    data_root = tmp_path / "data"
    _write_biblio_family(data_root, "100", include_book=True, chapters=("1.pdf",))
    output_dir = tmp_path / "benchmark"

    monkeypatch.setattr(bench, "_extract_pdf_text", _fake_extract_text)
    monkeypatch.setattr(bench, "_sample_extractable_chars", lambda _path: 2400)
    monkeypatch.setattr(
        bench,
        "_pdfinfo",
        lambda _path: {"Pages": "12", "Encrypted": "no", "Producer": "pytest"},
    )
    monkeypatch.setattr(
        bench,
        "build_family_queries",
        lambda _family: [
            bench.BenchmarkQuery(
                query_id="q1",
                query_type="family_identification",
                text="arrendamiento obligaciones",
                target_label="CAPÍTULO I",
                target_anchor="arrendamiento",
            )
        ],
    )

    def _runner(_family):
        query_lines = [
            line for line in (output_dir / "queries.jsonl").read_text(encoding="utf-8").splitlines() if line.strip()
        ]
        assert len(query_lines) == 1
        return bench.CandidateRun(
            candidate_id="chapter_first_heading_v1",
            input_source="chapter_first_pdf",
            extraction_method="stub",
            chunking_method="stub",
            chunks=[
                bench.BenchmarkChunk(
                    text="Arrendamiento, obligaciones y propiedad civil.",
                    label="CAPÍTULO I",
                    token_count=5,
                )
            ],
        )

    monkeypatch.setattr(
        bench,
        "build_candidate_registry",
        lambda: {"chapter_first_heading_v1": _runner},
    )
    monkeypatch.setattr(
        bench,
        "build_classifier_registry",
        lambda: {
            "metadata_rule_v1": lambda family, _context: bench.ClassificationPrediction(
                classifier_id="metadata_rule_v1",
                family_id=family.family_id,
                primary_materia="civil",
                secondary_materias=[],
                subject_tags=[],
                confidence=0.8,
                method="stub",
            )
        },
    )

    bench.run_benchmark(
        output_dir=output_dir,
        data_root=data_root,
        candidate_ids=["chapter_first_heading_v1"],
        classifier_ids=["metadata_rule_v1"],
        family_ids=["100"],
        include_pdf_info=True,
    )


def test_run_benchmark_records_classifier_context_failure(monkeypatch, tmp_path):
    data_root = tmp_path / "data"
    _write_biblio_family(data_root, "100", include_book=True, chapters=("1.pdf",))
    output_dir = tmp_path / "benchmark"

    monkeypatch.setattr(bench, "_extract_pdf_text", _fake_extract_text)
    monkeypatch.setattr(bench, "_sample_extractable_chars", lambda _path: 2400)
    monkeypatch.setattr(
        bench,
        "_pdfinfo",
        lambda _path: {"Pages": "12", "Encrypted": "no", "Producer": "pytest"},
    )
    monkeypatch.setattr(
        bench,
        "build_classifier_context",
        lambda _family: (_ for _ in ()).throw(RuntimeError("context boom")),
    )

    results = bench.run_benchmark(
        output_dir=output_dir,
        data_root=data_root,
        candidate_ids=["chapter_first_heading_v1"],
        classifier_ids=["metadata_rule_v1"],
        family_ids=["100"],
        include_pdf_info=True,
    )

    classifier_rows = [
        json.loads(line)
        for line in (output_dir / "classifiers" / "metadata_rule_v1.jsonl")
        .read_text(encoding="utf-8")
        .splitlines()
        if line.strip()
    ]
    assert len(classifier_rows) == 1
    assert classifier_rows[0]["method"] == "context_error"
    assert "classifier_context_failed: context boom" in (classifier_rows[0]["error"] or "")
    assert results["classifier_aggregates"][0]["decision_status"] == "blocked"


def test_rank_aggregates_does_not_mark_over_budget_candidate_as_winner():
    reports = [
        AggregateReport(
            candidate_id="chapter_first_heading_v1",
            lane="biblio",
            documents_run=1,
            success_rate=1.0,
            self_hosted_status="local_only",
            requires_remote_service=False,
            ocr_languages=[],
            manual_scores={},
            structure_metrics={"structure_score": 0.82, "duplicate_ratio": 0.0},
            retrieval_metrics={"recall_at_5": 0.7},
            ops_metrics={"processing_time_ms": 1000},
            projected_full_corpus_runtime_hours=12.0,
            decision_status="finalist",
            weighted_score=0.75,
        ),
        AggregateReport(
            candidate_id="docling_cpu_v1",
            lane="biblio",
            documents_run=1,
            success_rate=1.0,
            self_hosted_status="local_only",
            requires_remote_service=False,
            ocr_languages=[],
            manual_scores={},
            structure_metrics={"structure_score": 0.93, "duplicate_ratio": 0.0},
            retrieval_metrics={"recall_at_5": 0.8},
            ops_metrics={"processing_time_ms": 1000},
            projected_full_corpus_runtime_hours=300.0,
            decision_status="finalist",
            weighted_score=0.9,
        ),
    ]

    ranked = bench.rank_aggregates(reports)

    status_by_candidate = {report.candidate_id: report.decision_status for report in ranked}
    assert status_by_candidate["chapter_first_heading_v1"] == "winner"
    assert status_by_candidate["docling_cpu_v1"] == "over_budget"
