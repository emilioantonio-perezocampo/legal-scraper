import logging
from pathlib import Path

import src.gui.infrastructure.biblio_chunking as biblio_chunking


def test_decide_biblio_chunking_strategy_prefers_balanced_for_extractable_pdf(monkeypatch, tmp_path):
    pdf_path = tmp_path / "chapter.pdf"
    pdf_path.write_bytes(b"%PDF-1.4")

    monkeypatch.setattr(biblio_chunking, "_sample_extractable_chars", lambda _path: 3200)
    monkeypatch.setattr(biblio_chunking, "_pdf_is_encrypted", lambda _path: False)

    strategy = biblio_chunking.decide_biblio_chunking_strategy(pdf_path)

    assert strategy.chunking_method == "biblio_pymupdf_unstructured_balanced_v1"
    assert strategy.use_ocr is False
    assert strategy.force_ocr is False
    assert strategy.reason == "extractable_text"


def test_decide_biblio_chunking_strategy_forces_ocr_for_unextractable_pdf(monkeypatch, tmp_path):
    pdf_path = tmp_path / "chapter.pdf"
    pdf_path.write_bytes(b"%PDF-1.4")

    monkeypatch.setattr(biblio_chunking, "_sample_extractable_chars", lambda _path: 0)
    monkeypatch.setattr(biblio_chunking, "_pdf_is_encrypted", lambda _path: False)

    strategy = biblio_chunking.decide_biblio_chunking_strategy(pdf_path)

    assert strategy.chunking_method == "biblio_pymupdf_routed_heading_v1"
    assert strategy.use_ocr is True
    assert strategy.force_ocr is True
    assert strategy.reason == "copy_protected_or_no_text_layer"


def test_extract_biblio_chunks_falls_back_to_heading_chunker(monkeypatch, tmp_path):
    pdf_path = tmp_path / "books" / "100" / "chapters" / "1.pdf"
    pdf_path.parent.mkdir(parents=True)
    pdf_path.write_bytes(b"%PDF-1.4")

    monkeypatch.setattr(
        biblio_chunking,
        "decide_biblio_chunking_strategy",
        lambda _path: biblio_chunking.BiblioChunkingStrategy(
            chunking_method="biblio_pymupdf_unstructured_balanced_v1",
            extraction_method="pymupdf4llm_text_only",
            use_ocr=False,
            force_ocr=False,
            extractable_chars=2500,
            encrypted=False,
            reason="extractable_text",
        ),
    )
    monkeypatch.setattr(
        biblio_chunking,
        "_extract_pymupdf_page_chunks",
        lambda _path, strategy: [{"text": "# Capítulo I\nTexto útil", "page_number": 1}],
    )
    monkeypatch.setattr(
        biblio_chunking,
        "_chunk_markdown_with_unstructured",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
    )

    chunks = biblio_chunking.extract_biblio_chunks(pdf_path, "Libro 100")

    assert len(chunks) == 1
    assert chunks[0]["metadata"]["asset_role"] == "chapter"
    assert chunks[0]["metadata"]["chapter_number"] == "1"
    assert chunks[0]["metadata"]["chunking_method"] == "biblio_pymupdf_unstructured_balanced_v1"


def test_call_quietly_suppresses_parser_noise(capsys):
    def noisy():
        print("=== Document parser messages ===\x00\x00Using Tesseract")
        return "payload"

    result = biblio_chunking._call_quietly(noisy)

    captured = capsys.readouterr()
    assert result == "payload"
    assert captured.out == ""
    assert captured.err == ""


def test_suppress_noisy_pdf_logs_restores_logger_levels():
    pdfminer_logger = logging.getLogger("pdfminer.pdfinterp")
    original_level = pdfminer_logger.level

    with biblio_chunking._suppress_noisy_pdf_logs():
        assert pdfminer_logger.level == logging.ERROR

    assert pdfminer_logger.level == original_level
