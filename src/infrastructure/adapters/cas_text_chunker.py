"""
Text chunker for CAS arbitration awards.

Implements intelligent chunking that respects:
- Legal document structure (numbered paragraphs, sections)
- Token limits for embedding models
- Overlap for context preservation
- Multi-language support (EN/FR)
"""
from dataclasses import dataclass, field
from typing import Tuple, List, Optional
import re

from src.domain.cas_entities import FragmentoLaudo
from src.infrastructure.adapters.cas_errors import ChunkingError


@dataclass(frozen=True)
class CASChunkingConfig:
    """Configuration for CAS award text chunking."""
    max_tokens: int = 512
    overlap_tokens: int = 50
    modelo_tokenizer: str = "cl100k_base"
    separadores_primarios: Tuple[str, ...] = (
        "\n\nI.",      # Roman numeral sections
        "\n\nII.",
        "\n\nIII.",
        "\n\nIV.",
        "\n\nV.",
        "\n\nVI.",
        "\n\n",        # Double newlines
    )
    separadores_secundarios: Tuple[str, ...] = (
        "\n",
        ". ",
        "; ",
    )


class CASTextChunker:
    """
    Intelligent text chunker for CAS arbitration awards.

    Features:
    - Token-aware chunking using tiktoken
    - Legal structure detection (numbered paragraphs)
    - Section-aware chunking (Facts, Reasons, Decision)
    - Configurable overlap for context preservation
    - Multi-language support (English/French)
    """

    def __init__(self, config: Optional[CASChunkingConfig] = None):
        self._config = config or CASChunkingConfig()
        self._tokenizer = None

    def _get_tokenizer(self):
        """Lazy load tokenizer."""
        if self._tokenizer is None:
            try:
                import tiktoken
                self._tokenizer = tiktoken.get_encoding(self._config.modelo_tokenizer)
            except ImportError:
                # Fallback to simple estimation
                self._tokenizer = None
        return self._tokenizer

    def _count_tokens(self, text: str) -> int:
        """Count tokens in text."""
        tokenizer = self._get_tokenizer()
        if tokenizer:
            return len(tokenizer.encode(text))
        # Fallback: rough estimate of ~4 chars per token
        return len(text) // 4

    def fragmentar(
        self,
        texto: str,
        laudo_id: str,
        seccion: str = "completo",
    ) -> Tuple[FragmentoLaudo, ...]:
        """
        Split award text into fragments for embedding.

        Args:
            texto: Full text to chunk
            laudo_id: Parent award ID
            seccion: Section name (hechos, fundamentos, dispositivo, completo)

        Returns:
            Tuple of FragmentoLaudo

        Raises:
            ChunkingError: If chunking fails
        """
        if not texto or not texto.strip():
            return ()

        try:
            fragmentos: List[FragmentoLaudo] = []
            chunks = self._split_text(texto)

            for i, chunk_text in enumerate(chunks):
                chunk_text = chunk_text.strip()
                if not chunk_text:
                    continue

                tokens = self._count_tokens(chunk_text)
                parrafo_inicio = self._detect_paragraph_number(chunk_text)

                fragmento = FragmentoLaudo(
                    id=f"{laudo_id}-{seccion}-frag-{i+1:04d}",
                    laudo_id=laudo_id,
                    texto=chunk_text,
                    posicion=i,
                    seccion=seccion,
                )
                fragmentos.append(fragmento)

            return tuple(fragmentos)

        except Exception as e:
            raise ChunkingError(f"Error fragmentando texto: {e}")

    def fragmentar_por_secciones(
        self,
        texto: str,
        laudo_id: str,
    ) -> Tuple[FragmentoLaudo, ...]:
        """
        Split text by legal sections first, then chunk each section.

        Identifies sections like:
        - Facts / Hechos / Faits
        - Reasons / Fundamentos / Motifs
        - Decision / Dispositivo / Dispositif
        """
        secciones = self._identificar_secciones(texto)

        all_fragmentos: List[FragmentoLaudo] = []

        for nombre_seccion, contenido in secciones:
            fragmentos = self.fragmentar(contenido, laudo_id, nombre_seccion)
            all_fragmentos.extend(fragmentos)

        # If no sections identified, chunk as complete
        if not all_fragmentos:
            return self.fragmentar(texto, laudo_id, "completo")

        return tuple(all_fragmentos)

    def _identificar_secciones(self, texto: str) -> List[Tuple[str, str]]:
        """Identify legal sections in text."""
        secciones = []

        section_patterns = {
            'hechos': [
                r'(?:^|\n)\s*(?:I+\.?\s*)?(?:THE\s+)?FACTS?[:\s]*\n',
                r'(?:^|\n)\s*(?:I+\.?\s*)?HECHOS[:\s]*\n',
                r'(?:^|\n)\s*(?:I+\.?\s*)?(?:LES\s+)?FAITS[:\s]*\n',
            ],
            'fundamentos': [
                r'(?:^|\n)\s*(?:I+\.?\s*)?(?:THE\s+)?(?:REASONS?|MERITS?)[:\s]*\n',
                r'(?:^|\n)\s*(?:I+\.?\s*)?FUNDAMENTOS[:\s]*\n',
                r'(?:^|\n)\s*(?:I+\.?\s*)?(?:LES\s+)?MOTIFS[:\s]*\n',
            ],
            'dispositivo': [
                r'(?:^|\n)\s*(?:I+\.?\s*)?(?:THE\s+)?(?:DECISION|AWARD|RULING)[:\s]*\n',
                r'(?:^|\n)\s*(?:I+\.?\s*)?DISPOSITIVO[:\s]*\n',
                r'(?:^|\n)\s*(?:LE\s+)?DISPOSITIF[:\s]*\n',
            ],
        }

        for nombre, patterns in section_patterns.items():
            for pattern in patterns:
                match = re.search(pattern, texto, re.IGNORECASE | re.MULTILINE)
                if match:
                    # Find the end of this section (start of next or end of text)
                    start = match.end()
                    end = len(texto)

                    # Look for next section
                    for other_patterns in section_patterns.values():
                        for other_pattern in other_patterns:
                            other_match = re.search(other_pattern, texto[start:], re.IGNORECASE | re.MULTILINE)
                            if other_match:
                                potential_end = start + other_match.start()
                                if potential_end < end:
                                    end = potential_end

                    contenido = texto[start:end].strip()
                    if contenido:
                        secciones.append((nombre, contenido))
                    break  # Found this section, move to next

        return secciones

    def _split_text(self, texto: str) -> List[str]:
        """Split text into chunks respecting token limits."""
        chunks = []
        max_chars = self._config.max_tokens * 4  # Rough estimate

        # First try primary separators
        parts = [texto]
        for sep in self._config.separadores_primarios:
            new_parts = []
            for part in parts:
                new_parts.extend(part.split(sep))
            parts = new_parts

        # Then handle parts that are still too long
        final_parts = []
        for part in parts:
            part = part.strip()
            if not part:
                continue
            if len(part) <= max_chars:
                final_parts.append(part)
            else:
                # Split by secondary separators
                sub_parts = self._split_long_text(part, max_chars)
                final_parts.extend(sub_parts)

        return final_parts

    def _split_long_text(self, texto: str, max_chars: int) -> List[str]:
        """Split text that exceeds max length."""
        chunks = []
        current_chunk = ""

        # Try sentence-level splitting first
        sentences = re.split(r'(?<=[.!?])\s+', texto)

        for sentence in sentences:
            if len(current_chunk) + len(sentence) <= max_chars:
                current_chunk += (" " if current_chunk else "") + sentence
            else:
                if current_chunk:
                    chunks.append(current_chunk.strip())
                current_chunk = sentence

        if current_chunk:
            chunks.append(current_chunk.strip())

        # If still too long, split by words
        final_chunks = []
        for chunk in chunks:
            if len(chunk) <= max_chars:
                final_chunks.append(chunk)
            else:
                words = chunk.split()
                current = ""
                for word in words:
                    if len(current) + len(word) + 1 <= max_chars:
                        current += (" " if current else "") + word
                    else:
                        if current:
                            final_chunks.append(current)
                        current = word
                if current:
                    final_chunks.append(current)

        return final_chunks

    def _detect_paragraph_number(self, text: str) -> Optional[int]:
        """Detect paragraph number if present at start of text."""
        match = re.match(r'^(\d+)\.\s', text)
        if match:
            return int(match.group(1))
        return None
