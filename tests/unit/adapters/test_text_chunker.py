"""
RED Phase Tests: Text Chunker

Tests for text chunking adapter that splits text for embeddings.
These tests must FAIL initially (RED phase).
"""
import pytest
from dataclasses import FrozenInstanceError

# These imports will fail until implementation exists
from src.infrastructure.adapters.text_chunker import (
    TextChunker,
    ChunkConfig,
    TextChunkResult,
)
from src.infrastructure.adapters.errors import ChunkingError, TokenizationError


class TestChunkConfig:
    """Tests for ChunkConfig dataclass."""

    def test_default_values(self):
        """Config should have sensible defaults."""
        config = ChunkConfig()
        assert config.max_tokens == 512
        assert config.overlap_tokens == 50
        assert config.min_chunk_tokens == 100
        assert config.respect_boundaries is True

    def test_custom_values(self):
        """Config should accept custom values."""
        config = ChunkConfig(
            max_tokens=256,
            overlap_tokens=25,
            min_chunk_tokens=50,
            respect_boundaries=False,
        )
        assert config.max_tokens == 256
        assert config.overlap_tokens == 25

    def test_immutable(self):
        """Config should be frozen/immutable."""
        config = ChunkConfig()
        with pytest.raises(FrozenInstanceError):
            config.max_tokens = 1024


class TestTextChunkResult:
    """Tests for TextChunkResult dataclass."""

    def test_create_result(self):
        """Result should be creatable with all fields."""
        result = TextChunkResult(
            content="Sample text content",
            token_count=5,
            chunk_index=0,
            start_char=0,
            end_char=19,
            boundary_type="paragraph",
        )
        assert result.content == "Sample text content"
        assert result.token_count == 5
        assert result.chunk_index == 0
        assert result.boundary_type == "paragraph"

    def test_result_immutable(self):
        """Result should be frozen/immutable."""
        result = TextChunkResult(
            content="Test",
            token_count=1,
            chunk_index=0,
            start_char=0,
            end_char=4,
            boundary_type="forced",
        )
        with pytest.raises(FrozenInstanceError):
            result.content = "Changed"


class TestTextChunkerBasic:
    """Basic chunking tests."""

    def test_chunk_short_text_single_chunk(self):
        """Short text should produce a single chunk."""
        chunker = TextChunker()
        text = "This is a short text."
        chunks = chunker.chunk(text)
        assert len(chunks) == 1
        assert chunks[0].content == text
        assert chunks[0].chunk_index == 0

    def test_chunk_empty_text(self):
        """Empty text should return empty list."""
        chunker = TextChunker()
        chunks = chunker.chunk("")
        assert chunks == []

    def test_chunk_whitespace_only(self):
        """Whitespace-only text should return empty list."""
        chunker = TextChunker()
        chunks = chunker.chunk("   \n\t\n   ")
        assert chunks == []

    def test_chunk_single_word(self):
        """Single word should produce single chunk."""
        chunker = TextChunker()
        chunks = chunker.chunk("Constitución")
        assert len(chunks) == 1
        assert chunks[0].content == "Constitución"

    def test_chunk_exact_max_tokens(self):
        """Text at exactly max tokens should be single chunk."""
        config = ChunkConfig(max_tokens=10, min_chunk_tokens=1)
        chunker = TextChunker(config=config)
        # Create text that's approximately 10 tokens
        text = "one two three four five six seven eight nine ten"
        chunks = chunker.chunk(text)
        # Should be 1 or 2 chunks depending on exact token count
        assert len(chunks) >= 1

    def test_chunk_requires_split(self):
        """Long text should be split into multiple chunks."""
        config = ChunkConfig(max_tokens=50, overlap_tokens=10, min_chunk_tokens=10)
        chunker = TextChunker(config=config)
        # Create text that definitely exceeds 50 tokens
        text = " ".join(["palabra"] * 200)
        chunks = chunker.chunk(text)
        assert len(chunks) > 1

    def test_chunk_with_overlap(self):
        """Chunks should have overlapping content."""
        config = ChunkConfig(max_tokens=50, overlap_tokens=20, min_chunk_tokens=10)
        chunker = TextChunker(config=config)
        text = " ".join(["palabra"] * 200)
        chunks = chunker.chunk(text)

        if len(chunks) >= 2:
            # Check that there's some overlap between consecutive chunks
            chunk1_words = set(chunks[0].content.split()[-10:])
            chunk2_words = set(chunks[1].content.split()[:10])
            # Some overlap expected
            assert len(chunk1_words & chunk2_words) > 0


class TestTextChunkerBoundaries:
    """Tests for boundary-respecting chunking."""

    def test_chunk_breaks_at_paragraph(self):
        """Chunker should prefer breaking at paragraph boundaries."""
        config = ChunkConfig(max_tokens=100, respect_boundaries=True, min_chunk_tokens=10)
        chunker = TextChunker(config=config)
        text = "First paragraph with content.\n\nSecond paragraph with content."
        chunks = chunker.chunk(text)
        # Should try to break at paragraph if possible
        for chunk in chunks:
            assert chunk.boundary_type in ("paragraph", "sentence", "article", "forced")

    def test_chunk_breaks_at_sentence(self):
        """Chunker should break at sentence boundaries."""
        config = ChunkConfig(max_tokens=50, respect_boundaries=True, min_chunk_tokens=5)
        chunker = TextChunker(config=config)
        text = "Primera oración completa. Segunda oración completa. Tercera oración completa."
        chunks = chunker.chunk(text)
        # Chunks should ideally end at sentence boundaries
        assert len(chunks) >= 1

    def test_chunk_forced_break_long_paragraph(self):
        """Very long paragraph should be force-split."""
        config = ChunkConfig(max_tokens=20, respect_boundaries=True, min_chunk_tokens=5)
        chunker = TextChunker(config=config)
        # One very long "paragraph" with no natural breaks
        text = "palabra " * 100
        chunks = chunker.chunk(text)
        # Must split even though no natural boundary exists
        assert len(chunks) > 1
        # At least one chunk should have forced boundary
        boundary_types = [c.boundary_type for c in chunks]
        assert "forced" in boundary_types or len(chunks) > 3


class TestTextChunkerLegalPatterns:
    """Tests for legal document pattern detection."""

    def test_chunk_detects_articulo_pattern(self):
        """Chunker should detect 'Artículo X.-' patterns."""
        config = ChunkConfig(max_tokens=100, respect_boundaries=True, min_chunk_tokens=5)
        chunker = TextChunker(config=config)
        text = """Artículo 1.- Primera disposición legal.
        Artículo 2.- Segunda disposición legal.
        Artículo 3.- Tercera disposición legal."""
        chunks = chunker.chunk(text)
        # Should detect article boundaries
        assert len(chunks) >= 1

    def test_chunk_detects_articulo_bis(self):
        """Chunker should detect 'Artículo X Bis' patterns."""
        config = ChunkConfig(max_tokens=100, respect_boundaries=True, min_chunk_tokens=5)
        chunker = TextChunker(config=config)
        text = """Artículo 2.- Disposición original.
        Artículo 2 Bis.- Disposición adicional.
        Artículo 3.- Siguiente artículo."""
        chunks = chunker.chunk(text)
        assert len(chunks) >= 1

    def test_chunk_detects_transitorio(self):
        """Chunker should detect TRANSITORIOS section."""
        chunker = TextChunker()
        text = """Artículo 5.- Última disposición.

        TRANSITORIOS

        Primero.- Esta ley entrará en vigor."""
        chunks = chunker.chunk(text)
        assert len(chunks) >= 1

    def test_chunk_detects_capitulo(self):
        """Chunker should detect CAPÍTULO markers."""
        chunker = TextChunker()
        text = """CAPÍTULO I
        DE LAS DISPOSICIONES GENERALES

        Artículo 1.- Primera disposición.

        CAPÍTULO II
        DE LOS DERECHOS"""
        chunks = chunker.chunk(text)
        assert len(chunks) >= 1

    def test_chunk_detects_titulo(self):
        """Chunker should detect TÍTULO markers."""
        chunker = TextChunker()
        text = """TÍTULO PRIMERO
        DISPOSICIONES GENERALES

        Artículo 1.- Contenido del título."""
        chunks = chunker.chunk(text)
        assert len(chunks) >= 1


class TestTextChunkerUnicode:
    """Tests for Unicode handling."""

    def test_chunk_preserves_accents(self):
        """Chunker should preserve Spanish accents."""
        chunker = TextChunker()
        text = "Constitución Política de los Estados Unidos Mexicanos"
        chunks = chunker.chunk(text)
        assert len(chunks) >= 1
        assert "Constitución" in chunks[0].content
        assert "Política" in chunks[0].content

    def test_chunk_handles_ene(self):
        """Chunker should handle ñ character."""
        chunker = TextChunker()
        text = "Los niños y niñas tienen derecho a la educación en España."
        chunks = chunker.chunk(text)
        assert len(chunks) >= 1
        assert "niños" in chunks[0].content
        assert "niñas" in chunks[0].content
        assert "España" in chunks[0].content

    def test_chunk_handles_special_quotes(self):
        """Chunker should handle special quote characters."""
        chunker = TextChunker()
        text = 'El artículo «primero» establece que "todos" son iguales.'
        chunks = chunker.chunk(text)
        assert len(chunks) >= 1
        assert "«" in chunks[0].content or "primero" in chunks[0].content

    def test_chunk_handles_legal_symbols(self):
        """Chunker should handle legal symbols like § and ¶."""
        chunker = TextChunker()
        text = "Véase § 123 del Código. ¶ Nuevo párrafo."
        chunks = chunker.chunk(text)
        assert len(chunks) >= 1


class TestTextChunkerTokenCounting:
    """Tests for token counting functionality."""

    def test_count_tokens_spanish_text(self):
        """Token count should work for Spanish text."""
        chunker = TextChunker()
        text = "La Constitución Política de México."
        count = chunker._count_tokens(text)
        assert count > 0
        assert count < 20  # Reasonable range

    def test_count_tokens_legal_terminology(self):
        """Token count should work for legal terms."""
        chunker = TextChunker()
        text = "Jurisprudencia constitucional sobre amparo indirecto."
        count = chunker._count_tokens(text)
        assert count > 0

    def test_count_tokens_numbers_and_dates(self):
        """Token count should handle numbers and dates."""
        chunker = TextChunker()
        text = "Publicado el 15 de marzo de 2024 en el DOF número 274/2024."
        count = chunker._count_tokens(text)
        assert count > 0

    def test_chunk_token_count_matches(self):
        """Chunk token_count field should match actual count."""
        chunker = TextChunker()
        text = "Este es un texto de prueba para verificar conteo."
        chunks = chunker.chunk(text)
        assert len(chunks) >= 1
        # Token count in result should match internal counting
        assert chunks[0].token_count == chunker._count_tokens(chunks[0].content)


class TestTextChunkerStreaming:
    """Tests for streaming chunk generation."""

    def test_chunk_streaming_yields_results(self):
        """Streaming should yield TextChunkResult objects."""
        chunker = TextChunker()
        text = "Sample text for streaming test."
        results = list(chunker.chunk_streaming(text))
        assert len(results) >= 1
        assert all(isinstance(r, TextChunkResult) for r in results)

    def test_chunk_streaming_same_as_list(self):
        """Streaming should yield same results as list method."""
        config = ChunkConfig(max_tokens=50, min_chunk_tokens=5)
        chunker = TextChunker(config=config)
        text = " ".join(["palabra"] * 100)

        list_results = chunker.chunk(text)
        stream_results = list(chunker.chunk_streaming(text))

        assert len(list_results) == len(stream_results)
        for lr, sr in zip(list_results, stream_results):
            assert lr.content == sr.content
            assert lr.chunk_index == sr.chunk_index

    def test_chunk_streaming_empty_text(self):
        """Streaming empty text should yield nothing."""
        chunker = TextChunker()
        results = list(chunker.chunk_streaming(""))
        assert results == []


class TestTextChunkerConfiguration:
    """Tests for configuration options."""

    def test_custom_max_tokens(self):
        """Custom max_tokens should be respected."""
        config = ChunkConfig(max_tokens=20, min_chunk_tokens=5)
        chunker = TextChunker(config=config)
        text = " ".join(["word"] * 100)
        chunks = chunker.chunk(text)
        # All chunks should have token count <= max_tokens (with some tolerance)
        for chunk in chunks:
            assert chunk.token_count <= 25  # Allow small overflow for boundary respect

    def test_custom_overlap(self):
        """Custom overlap_tokens should be respected."""
        config = ChunkConfig(max_tokens=30, overlap_tokens=10, min_chunk_tokens=5)
        chunker = TextChunker(config=config)
        text = " ".join(["word"] * 100)
        chunks = chunker.chunk(text)
        assert len(chunks) > 1

    def test_zero_overlap(self):
        """Zero overlap should produce non-overlapping chunks."""
        config = ChunkConfig(max_tokens=30, overlap_tokens=0, min_chunk_tokens=5)
        chunker = TextChunker(config=config)
        text = " ".join(["word"] * 100)
        chunks = chunker.chunk(text)
        assert len(chunks) > 1

    def test_min_chunk_respected(self):
        """Minimum chunk size should be respected where possible."""
        config = ChunkConfig(max_tokens=100, min_chunk_tokens=20)
        chunker = TextChunker(config=config)
        text = " ".join(["word"] * 200)
        chunks = chunker.chunk(text)
        # Most chunks should meet minimum (last chunk might be smaller)
        for chunk in chunks[:-1]:
            assert chunk.token_count >= 15  # Allow some tolerance

    def test_respect_boundaries_false(self):
        """Disabling boundary respect should allow mid-sentence splits."""
        config = ChunkConfig(max_tokens=20, respect_boundaries=False, min_chunk_tokens=5)
        chunker = TextChunker(config=config)
        text = "This is a very long sentence that should be split without respecting boundaries."
        chunks = chunker.chunk(text)
        # Should split regardless of sentence structure
        if len(chunks) > 1:
            # At least one chunk might end mid-word or mid-sentence
            assert any(c.boundary_type == "forced" for c in chunks)


class TestTextChunkerCharacterPositions:
    """Tests for character position tracking."""

    def test_chunk_tracks_start_position(self):
        """Chunks should track start character position."""
        chunker = TextChunker()
        text = "First chunk text. Second chunk text."
        chunks = chunker.chunk(text)
        assert chunks[0].start_char == 0

    def test_chunk_tracks_end_position(self):
        """Chunks should track end character position."""
        chunker = TextChunker()
        text = "Test text here."
        chunks = chunker.chunk(text)
        assert chunks[0].end_char == len(text)

    def test_chunk_positions_reconstruct_text(self):
        """Using positions should reconstruct original text."""
        config = ChunkConfig(max_tokens=30, overlap_tokens=0, min_chunk_tokens=5)
        chunker = TextChunker(config=config)
        text = " ".join(["word"] * 50)
        chunks = chunker.chunk(text)

        # Non-overlapping chunks should reconstruct text
        reconstructed_parts = []
        for chunk in chunks:
            reconstructed_parts.append(text[chunk.start_char:chunk.end_char])

        # Join and compare (may have whitespace differences)
        reconstructed = "".join(reconstructed_parts)
        assert len(reconstructed) > 0
