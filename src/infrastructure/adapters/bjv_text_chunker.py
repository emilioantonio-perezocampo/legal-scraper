"""
BJV text chunker.

Token-aware text chunking with legal structure preservation
for embedding generation.
"""
import re
from dataclasses import dataclass
from typing import Tuple, List, Optional

try:
    import tiktoken
except ImportError:
    tiktoken = None

from src.domain.bjv_value_objects import IdentificadorLibro
from src.domain.bjv_entities import FragmentoTexto
from src.infrastructure.adapters.bjv_errors import ChunkingError


@dataclass(frozen=True)
class ChunkingConfig:
    """Configuration for text chunking."""

    max_tokens: int = 512
    overlap_tokens: int = 50
    modelo_tokenizer: str = "cl100k_base"
    separadores_primarios: Tuple[str, ...] = (
        "\n\nArtículo",
        "\n\nCAPÍTULO",
        "\n\nSECCIÓN",
        "\n\nTÍTULO",
        "\n\n",
    )
    separadores_secundarios: Tuple[str, ...] = (
        "\n",
        ". ",
        "; ",
        ", ",
    )


class BJVTextChunker:
    """
    Token-aware text chunker for BJV legal documents.

    Features:
    - Respects token limits for embedding models
    - Preserves legal structure (articles, chapters, sections)
    - Adds configurable overlap between chunks
    - Tracks fragment position and metadata
    """

    def __init__(self, config: Optional[ChunkingConfig] = None):
        """
        Initialize the text chunker.

        Args:
            config: Chunking configuration (uses defaults if not provided)
        """
        self._config = config or ChunkingConfig()
        self._tokenizer = self._get_tokenizer()

    def _get_tokenizer(self):
        """Get the tiktoken tokenizer."""
        if tiktoken is None:
            return None
        try:
            return tiktoken.get_encoding(self._config.modelo_tokenizer)
        except Exception:
            # Fallback to cl100k_base if model name doesn't work
            try:
                return tiktoken.get_encoding("cl100k_base")
            except Exception:
                return None

    def _count_tokens(self, text: str) -> int:
        """Count tokens in text."""
        if self._tokenizer is None:
            # Rough estimate: ~4 chars per token for Spanish
            return len(text) // 4
        return len(self._tokenizer.encode(text))

    def fragmentar(
        self,
        texto: str,
        libro_id: str,
        numero_pagina: int = 1,
        numero_capitulo: Optional[int] = None,
    ) -> Tuple[FragmentoTexto, ...]:
        """
        Split text into token-aware fragments.

        Args:
            texto: Text to split
            libro_id: Book identifier
            numero_pagina: Page number for metadata
            numero_capitulo: Optional chapter number

        Returns:
            Tuple of FragmentoTexto entities
        """
        if not texto or not texto.strip():
            return tuple()

        texto = texto.strip()

        # If text fits in one chunk, return single fragment
        if self._count_tokens(texto) <= self._config.max_tokens:
            return (
                FragmentoTexto(
                    id=f"{libro_id}_frag_0",
                    libro_id=IdentificadorLibro(bjv_id=libro_id),
                    contenido=texto,
                    numero_pagina=numero_pagina,
                    posicion=0,
                    numero_capitulo=numero_capitulo,
                ),
            )

        # Split into chunks
        chunks = self._split_into_chunks(texto)

        # Create fragments
        fragmentos = []
        for i, chunk in enumerate(chunks):
            fragmento = FragmentoTexto(
                id=f"{libro_id}_frag_{i}",
                libro_id=IdentificadorLibro(bjv_id=libro_id),
                contenido=chunk,
                numero_pagina=numero_pagina,
                posicion=i,
                numero_capitulo=numero_capitulo,
            )
            fragmentos.append(fragmento)

        return tuple(fragmentos)

    def _split_into_chunks(self, texto: str) -> List[str]:
        """
        Split text into chunks respecting token limits.

        Uses hierarchical splitting:
        1. Try primary separators (legal structures)
        2. Fall back to secondary separators
        3. Hard split if necessary
        """
        chunks = []
        current_chunk = ""
        current_tokens = 0

        # First, try splitting by primary separators
        segments = self._split_by_separators(texto, self._config.separadores_primarios)

        for segment in segments:
            segment_tokens = self._count_tokens(segment)

            # If segment alone exceeds max tokens, split it further
            if segment_tokens > self._config.max_tokens:
                # Save current chunk if any
                if current_chunk.strip():
                    chunks.append(current_chunk.strip())
                    current_chunk = ""
                    current_tokens = 0

                # Split large segment
                sub_chunks = self._split_large_segment(segment)
                chunks.extend(sub_chunks)
                continue

            # Check if adding segment would exceed limit
            if current_tokens + segment_tokens > self._config.max_tokens:
                # Save current chunk
                if current_chunk.strip():
                    chunks.append(current_chunk.strip())

                # Start new chunk with overlap
                overlap = self._get_overlap_text(current_chunk)
                current_chunk = overlap + segment
                current_tokens = self._count_tokens(current_chunk)
            else:
                current_chunk += segment
                current_tokens += segment_tokens

        # Don't forget the last chunk
        if current_chunk.strip():
            chunks.append(current_chunk.strip())

        return chunks

    def _split_by_separators(self, texto: str, separadores: Tuple[str, ...]) -> List[str]:
        """Split text by separators, keeping separators with following content."""
        if not separadores:
            return [texto]

        # Build regex pattern
        pattern = "|".join(re.escape(sep) for sep in separadores)
        parts = re.split(f"({pattern})", texto)

        # Recombine parts to keep separator with following text
        segments = []
        i = 0
        while i < len(parts):
            segment = parts[i]
            # If next part is separator, combine with following content
            if i + 1 < len(parts) and parts[i + 1].strip() in [s.strip() for s in separadores]:
                if i + 2 < len(parts):
                    segment = parts[i] + parts[i + 1] + parts[i + 2]
                    i += 2
                else:
                    segment = parts[i] + parts[i + 1]
                    i += 1
            segments.append(segment)
            i += 1

        return [s for s in segments if s.strip()]

    def _split_large_segment(self, segment: str) -> List[str]:
        """Split a segment that exceeds max tokens."""
        chunks = []
        current = ""

        # Try secondary separators
        parts = self._split_by_separators(segment, self._config.separadores_secundarios)

        for part in parts:
            part_tokens = self._count_tokens(part)

            if part_tokens > self._config.max_tokens:
                # Save current if any
                if current.strip():
                    chunks.append(current.strip())
                    current = ""

                # Hard split by words
                word_chunks = self._hard_split(part)
                chunks.extend(word_chunks)
            elif self._count_tokens(current + part) > self._config.max_tokens:
                if current.strip():
                    chunks.append(current.strip())
                overlap = self._get_overlap_text(current)
                current = overlap + part
            else:
                current += part

        if current.strip():
            chunks.append(current.strip())

        return chunks

    def _hard_split(self, text: str) -> List[str]:
        """Hard split text by words when all else fails."""
        words = text.split()
        chunks = []
        current = ""

        for word in words:
            test_chunk = current + " " + word if current else word
            if self._count_tokens(test_chunk) > self._config.max_tokens:
                if current.strip():
                    chunks.append(current.strip())
                overlap = self._get_overlap_text(current)
                current = overlap + word
            else:
                current = test_chunk

        if current.strip():
            chunks.append(current.strip())

        return chunks

    def _get_overlap_text(self, text: str) -> str:
        """Get overlap text from end of chunk."""
        if not text or self._config.overlap_tokens <= 0:
            return ""

        words = text.split()
        if not words:
            return ""

        # Take words from end up to overlap token count
        overlap_words = []
        token_count = 0

        for word in reversed(words):
            word_tokens = self._count_tokens(word)
            if token_count + word_tokens > self._config.overlap_tokens:
                break
            overlap_words.insert(0, word)
            token_count += word_tokens

        return " ".join(overlap_words) + " " if overlap_words else ""
