"""
Production CAS/TAS chunking helpers.

This module turns the converted ``canonical.json`` artifacts in the mounted
CAS/TAS corpus into semantically structured chunks using the benchmark-winning
``canonical_section_v1`` strategy.
"""
from __future__ import annotations

import json
import re
from functools import lru_cache
from pathlib import Path
from typing import Any

CHUNK_TARGET_MIN_TOKENS = 250
CHUNK_TARGET_MAX_TOKENS = 650
CHUNK_MAX_TOKENS = 800
CHUNK_OVERLAP_TOKENS = 80
SENTENCE_BOUNDARY_RE = re.compile(r"(?<=[.!?])\s+(?=[A-Z0-9ÁÉÍÓÚÑ])")
NUMERIC_MARKER_RE = re.compile(r"^(?:\d+|[IVXLCDM]+)\.$", re.IGNORECASE)


@lru_cache(maxsize=1)
def _get_token_encoder():
    try:
        import tiktoken

        return tiktoken.get_encoding("cl100k_base")
    except Exception:
        return None


def count_tokens(text: str) -> int:
    text = str(text or "").strip()
    if not text:
        return 0

    encoder = _get_token_encoder()
    if encoder is not None:
        try:
            return len(encoder.encode(text))
        except Exception:
            pass

    return max(1, int(len(text.split()) / 0.75))


def is_cas_canonical_json(path: Path) -> bool:
    return path.suffix.lower() == ".json" and path.name.lower() == "canonical.json"


def load_cas_canonical_document(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"CAS canonical file not found: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def extract_cas_canonical_chunks(path: Path, fallback_title: str) -> list[dict[str, Any]]:
    canonical = load_cas_canonical_document(path)
    sections = _merge_numbered_markers(_canonical_sections(canonical, fallback_title))
    return _chunk_canonical_sections(sections, fallback_title=fallback_title)


def _canonical_sections(canonical: dict[str, Any], fallback_title: str) -> list[dict[str, Any]]:
    raw_sections = canonical.get("sections", []) if isinstance(canonical, dict) else []
    sections: list[dict[str, Any]] = []

    for section in raw_sections:
        if not isinstance(section, dict):
            continue
        text = str(section.get("text") or "").strip()
        if not text:
            continue

        heading_path = [
            str(item).strip()
            for item in section.get("heading_path", [])
            if str(item or "").strip()
        ]
        label = heading_path[-1] if heading_path else (str(section.get("title") or "").strip() or fallback_title or text[:80])
        pages = [
            block.get("page")
            for block in section.get("blocks", [])
            if isinstance(block, dict) and isinstance(block.get("page"), int)
        ]
        sections.append(
            {
                "label": label,
                "text": text,
                "type": str(section.get("type") or "paragraph").strip() or "paragraph",
                "heading_path": heading_path,
                "page_start": min(pages) if pages else None,
                "page_end": max(pages) if pages else None,
            }
        )

    return sections


def _merge_numbered_markers(sections: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    skip_next = False

    for index, section in enumerate(sections):
        if skip_next:
            skip_next = False
            continue

        text = str(section.get("text") or "").strip()
        if NUMERIC_MARKER_RE.match(text) and index + 1 < len(sections):
            next_section = dict(sections[index + 1])
            next_text = str(next_section.get("text") or "").strip()
            next_section["text"] = f"{text} {next_text}".strip()
            next_section["label"] = next_section.get("label") or section.get("label") or text
            merged.append(next_section)
            skip_next = True
            continue

        merged.append(dict(section))

    return merged


def _chunk_canonical_sections(
    sections: list[dict[str, Any]],
    *,
    fallback_title: str,
) -> list[dict[str, Any]]:
    chunks: list[dict[str, Any]] = []
    buffer: list[dict[str, Any]] = []
    buffer_tokens = 0

    def flush() -> None:
        nonlocal buffer, buffer_tokens
        if not buffer:
            return

        text = "\n\n".join(section["text"] for section in buffer if section.get("text")).strip()
        if not text:
            buffer = []
            buffer_tokens = 0
            return

        label = (
            buffer[0]["label"]
            if len(buffer) == 1
            else f"{buffer[0]['label']} – {buffer[-1]['label']}"
        )
        heading_path = list(buffer[-1].get("heading_path", []))
        pages = [section.get("page_start") for section in buffer if section.get("page_start") is not None]
        pages += [section.get("page_end") for section in buffer if section.get("page_end") is not None]
        section_types = sorted({section.get("type", "paragraph") for section in buffer})

        chunks.extend(
            _split_with_overlap(
                text=text,
                title=label or fallback_title or text[:120],
                metadata={
                    "heading": heading_path[-1] if heading_path else (label or fallback_title or text[:120]),
                    "heading_path": heading_path,
                    "heading_level": len(heading_path) if heading_path else None,
                    "page_start": min(pages) if pages else None,
                    "page_end": max(pages) if pages else None,
                    "section_types": section_types,
                    "chunking_method": "canonical_section_v1",
                    "source_format": "canonical_json",
                },
            )
        )

        buffer = []
        buffer_tokens = 0

    for section in sections:
        section_text = str(section.get("text") or "").strip()
        token_count = count_tokens(section_text)
        if token_count > CHUNK_MAX_TOKENS:
            flush()
            chunks.extend(
                _split_with_overlap(
                    text=section_text,
                    title=section.get("label") or fallback_title or section_text[:120],
                    metadata={
                        "heading": section.get("heading_path", [])[-1]
                        if section.get("heading_path")
                        else (section.get("label") or fallback_title or section_text[:120]),
                        "heading_path": list(section.get("heading_path", [])),
                        "heading_level": len(section.get("heading_path", [])) or None,
                        "page_start": section.get("page_start"),
                        "page_end": section.get("page_end"),
                        "section_types": [section.get("type", "paragraph")],
                        "chunking_method": "canonical_section_v1",
                        "source_format": "canonical_json",
                    },
                )
            )
            continue

        if buffer and buffer_tokens + token_count > CHUNK_TARGET_MAX_TOKENS:
            flush()

        buffer.append(section)
        buffer_tokens += token_count

        if buffer_tokens >= CHUNK_TARGET_MIN_TOKENS:
            flush()

    flush()
    return chunks


def _split_with_overlap(
    *,
    text: str,
    title: str,
    metadata: dict[str, Any],
) -> list[dict[str, Any]]:
    text = str(text or "").strip()
    if not text:
        return []

    sentences = [piece.strip() for piece in SENTENCE_BOUNDARY_RE.split(text) if piece.strip()]
    if not sentences:
        sentences = [text]

    chunks: list[dict[str, Any]] = []
    current: list[str] = []
    current_tokens = 0

    def flush() -> None:
        nonlocal current, current_tokens
        if not current:
            return

        chunk_text = " ".join(current).strip()
        chunk_metadata = dict(metadata)
        chunk_metadata["token_count"] = count_tokens(chunk_text)
        chunks.append(
            {
                "text": chunk_text,
                "title": title,
                "metadata": chunk_metadata,
            }
        )

        current = _tail_for_overlap(current, CHUNK_OVERLAP_TOKENS)
        current_tokens = count_tokens(" ".join(current)) if current else 0

    for sentence in sentences:
        sentence_tokens = count_tokens(sentence)
        if current and current_tokens + sentence_tokens > CHUNK_TARGET_MAX_TOKENS:
            flush()

        if sentence_tokens > CHUNK_MAX_TOKENS:
            for piece in _hard_wrap_sentence(sentence):
                piece_text = piece.strip()
                if not piece_text:
                    continue
                current.append(piece_text)
                current_tokens += count_tokens(piece_text)
                flush()
            continue

        current.append(sentence)
        current_tokens += sentence_tokens

    flush()
    return chunks


def _tail_for_overlap(sentences: list[str], overlap_tokens: int) -> list[str]:
    if overlap_tokens <= 0 or not sentences:
        return []

    tail: list[str] = []
    running_tokens = 0
    for sentence in reversed(sentences):
        sentence_tokens = count_tokens(sentence)
        if tail and running_tokens + sentence_tokens > overlap_tokens:
            break
        tail.insert(0, sentence)
        running_tokens += sentence_tokens
    return tail


def _hard_wrap_sentence(sentence: str) -> list[str]:
    words = str(sentence or "").split()
    if not words:
        return []

    parts: list[str] = []
    current: list[str] = []

    for word in words:
        tentative = " ".join(current + [word]).strip()
        if current and count_tokens(tentative) > CHUNK_MAX_TOKENS:
            parts.append(" ".join(current).strip())
            current = [word]
        else:
            current.append(word)

    if current:
        parts.append(" ".join(current).strip())

    return parts
