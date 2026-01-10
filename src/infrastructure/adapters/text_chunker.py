"""
Text Chunker Adapter

Splits text into overlapping chunks suitable for embedding generation.
Respects legal document structure (articles, chapters, sections).
"""
import re
from dataclasses import dataclass, field
from typing import List, Iterator, Optional

from .errors import ChunkingError, TokenizationError


@dataclass(frozen=True)
class ChunkConfig:
    """
    Configuration for text chunking.

    Attributes:
        max_tokens: Maximum tokens per chunk (default 512)
        overlap_tokens: Tokens to overlap between chunks (default 50)
        min_chunk_tokens: Minimum tokens per chunk (default 100)
        respect_boundaries: Try to break at natural boundaries (default True)
    """
    max_tokens: int = 512
    overlap_tokens: int = 50
    min_chunk_tokens: int = 100
    respect_boundaries: bool = True


@dataclass(frozen=True)
class TextChunkResult:
    """
    A chunk of text with metadata.

    Attributes:
        content: The chunk text content
        token_count: Number of tokens in chunk
        chunk_index: Zero-based index of this chunk
        start_char: Starting character position in original text
        end_char: Ending character position in original text
        boundary_type: Type of boundary used ("article", "paragraph", "sentence", "forced")
    """
    content: str
    token_count: int
    chunk_index: int
    start_char: int
    end_char: int
    boundary_type: str


class TextChunker:
    """
    Chunks text for embedding generation.

    Splits text into overlapping chunks while respecting natural boundaries
    in legal documents (articles, paragraphs, sentences).
    """

    # Legal document patterns (Mexican legal structure)
    ARTICLE_PATTERN = re.compile(
        r'(?:^|\n)\s*(?:Art[ií]culo|ARTÍCULO|ARTICULO)\s+\d+[\w\s]*[\.\-]',
        re.IGNORECASE | re.MULTILINE
    )
    TRANSITORY_PATTERN = re.compile(
        r'(?:^|\n)\s*(?:TRANSITORIOS?|Transitorios?)',
        re.MULTILINE
    )
    CHAPTER_PATTERN = re.compile(
        r'(?:^|\n)\s*(?:CAP[IÍ]TULO|CAPÍTULO)\s+[IVXLCDM\d]+',
        re.IGNORECASE | re.MULTILINE
    )
    TITLE_PATTERN = re.compile(
        r'(?:^|\n)\s*(?:T[IÍ]TULO|TÍTULO)\s+(?:PRIMERO|SEGUNDO|TERCERO|[IVXLCDM\d]+)',
        re.IGNORECASE | re.MULTILINE
    )
    PARAGRAPH_PATTERN = re.compile(r'\n\s*\n')
    SENTENCE_PATTERN = re.compile(r'[.!?]\s+')

    def __init__(self, config: ChunkConfig = None):
        """
        Initialize the text chunker.

        Args:
            config: Chunking configuration (uses defaults if not provided)
        """
        self.config = config or ChunkConfig()
        self._encoder = None

    def _get_encoder(self):
        """Lazy load tiktoken encoder."""
        if self._encoder is None:
            try:
                import tiktoken
                self._encoder = tiktoken.get_encoding("cl100k_base")
            except ImportError:
                # Fallback: approximate token count as words * 1.3
                self._encoder = None
        return self._encoder

    def _count_tokens(self, text: str) -> int:
        """
        Count tokens in text using tiktoken.

        Args:
            text: Text to count tokens for

        Returns:
            Number of tokens
        """
        if not text:
            return 0

        encoder = self._get_encoder()
        if encoder is not None:
            try:
                return len(encoder.encode(text))
            except Exception:
                pass

        # Fallback: approximate with word count * 1.3
        return int(len(text.split()) * 1.3)

    def chunk(self, text: str) -> List[TextChunkResult]:
        """
        Split text into chunks.

        Args:
            text: Text to chunk

        Returns:
            List of TextChunkResult objects
        """
        return list(self.chunk_streaming(text))

    def chunk_streaming(self, text: str) -> Iterator[TextChunkResult]:
        """
        Stream chunks for memory efficiency.

        Args:
            text: Text to chunk

        Yields:
            TextChunkResult objects
        """
        # Handle empty/whitespace text
        if not text or not text.strip():
            return

        text = text.strip()
        total_tokens = self._count_tokens(text)

        # If text fits in one chunk, return it
        if total_tokens <= self.config.max_tokens:
            yield TextChunkResult(
                content=text,
                token_count=total_tokens,
                chunk_index=0,
                start_char=0,
                end_char=len(text),
                boundary_type="paragraph" if "\n" in text else "sentence",
            )
            return

        # Find all potential boundaries
        boundaries = self._detect_all_boundaries(text)

        # Chunk the text
        chunk_index = 0
        current_pos = 0

        while current_pos < len(text):
            # Determine chunk end position
            chunk_end, boundary_type = self._find_chunk_end(
                text, current_pos, boundaries
            )

            # Extract chunk content
            chunk_content = text[current_pos:chunk_end].strip()

            if chunk_content:
                token_count = self._count_tokens(chunk_content)

                yield TextChunkResult(
                    content=chunk_content,
                    token_count=token_count,
                    chunk_index=chunk_index,
                    start_char=current_pos,
                    end_char=chunk_end,
                    boundary_type=boundary_type,
                )
                chunk_index += 1

            # Move to next position with overlap
            if self.config.overlap_tokens > 0 and chunk_end < len(text):
                # Calculate overlap in characters (approximate)
                overlap_chars = self._tokens_to_chars(
                    chunk_content, self.config.overlap_tokens
                )
                current_pos = max(current_pos + 1, chunk_end - overlap_chars)
            else:
                current_pos = chunk_end

            # Safety: ensure progress
            if current_pos <= chunk_end - len(chunk_content):
                current_pos = chunk_end

    def _detect_all_boundaries(self, text: str) -> List[tuple]:
        """
        Detect all potential boundaries in text.

        Returns list of (position, type) tuples sorted by position.
        """
        boundaries = []

        # Article boundaries (highest priority)
        for match in self.ARTICLE_PATTERN.finditer(text):
            boundaries.append((match.start(), "article"))

        # Transitory section
        for match in self.TRANSITORY_PATTERN.finditer(text):
            boundaries.append((match.start(), "article"))

        # Chapter boundaries
        for match in self.CHAPTER_PATTERN.finditer(text):
            boundaries.append((match.start(), "article"))

        # Title boundaries
        for match in self.TITLE_PATTERN.finditer(text):
            boundaries.append((match.start(), "article"))

        # Paragraph boundaries
        for match in self.PARAGRAPH_PATTERN.finditer(text):
            boundaries.append((match.start(), "paragraph"))

        # Sentence boundaries
        for match in self.SENTENCE_PATTERN.finditer(text):
            boundaries.append((match.end(), "sentence"))

        # Sort by position
        boundaries.sort(key=lambda x: x[0])

        return boundaries

    def _find_chunk_end(
        self,
        text: str,
        start_pos: int,
        boundaries: List[tuple]
    ) -> tuple:
        """
        Find the best end position for a chunk.

        Returns (end_position, boundary_type).
        """
        # Calculate target end based on max tokens
        remaining_text = text[start_pos:]
        target_chars = self._tokens_to_chars(
            remaining_text[:self.config.max_tokens * 4],  # Approximate
            self.config.max_tokens
        )
        target_end = min(start_pos + target_chars, len(text))

        if not self.config.respect_boundaries:
            return target_end, "forced"

        # Find the best boundary before target_end
        best_boundary = None
        best_type = "forced"

        # Priority: article > paragraph > sentence
        priority = {"article": 3, "paragraph": 2, "sentence": 1}

        for pos, btype in boundaries:
            if pos <= start_pos:
                continue
            if pos > target_end:
                break

            # Check if chunk would be too small
            chunk_text = text[start_pos:pos].strip()
            token_count = self._count_tokens(chunk_text)

            if token_count >= self.config.min_chunk_tokens or pos >= target_end - 10:
                if best_boundary is None or priority.get(btype, 0) > priority.get(best_type, 0):
                    best_boundary = pos
                    best_type = btype
                elif pos > best_boundary and priority.get(btype, 0) >= priority.get(best_type, 0):
                    best_boundary = pos
                    best_type = btype

        if best_boundary is not None:
            return best_boundary, best_type

        # No good boundary found, force split
        return target_end, "forced"

    def _tokens_to_chars(self, text: str, num_tokens: int) -> int:
        """
        Estimate character count for given number of tokens.

        Uses binary search to find approximate character position.
        """
        if not text:
            return 0

        encoder = self._get_encoder()

        if encoder is not None:
            try:
                tokens = encoder.encode(text)
                if len(tokens) <= num_tokens:
                    return len(text)

                # Binary search for approximate position
                low, high = 0, len(text)
                while low < high:
                    mid = (low + high) // 2
                    mid_tokens = len(encoder.encode(text[:mid]))
                    if mid_tokens < num_tokens:
                        low = mid + 1
                    else:
                        high = mid
                return low
            except Exception:
                pass

        # Fallback: estimate ~4 chars per token
        return min(num_tokens * 4, len(text))

    def _detect_article_boundaries(self, text: str) -> List[int]:
        """
        Find article markers in legal text.

        Returns list of positions where articles begin.
        """
        positions = []
        for match in self.ARTICLE_PATTERN.finditer(text):
            positions.append(match.start())
        return positions
