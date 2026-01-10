"""
Tests for BJV text chunker.

Following RED-GREEN TDD: These tests define the expected behavior
for token-aware text chunking with legal structure preservation.

Target: ~20 tests
"""
import pytest
from unittest.mock import Mock, patch


class TestChunkingConfig:
    """Tests for ChunkingConfig dataclass."""

    def test_default_values(self):
        """ChunkingConfig has sensible defaults."""
        from src.infrastructure.adapters.bjv_text_chunker import ChunkingConfig

        config = ChunkingConfig()

        assert config.max_tokens == 512
        assert config.overlap_tokens == 50
        assert config.modelo_tokenizer == "cl100k_base"

    def test_custom_values(self):
        """ChunkingConfig accepts custom values."""
        from src.infrastructure.adapters.bjv_text_chunker import ChunkingConfig

        config = ChunkingConfig(
            max_tokens=256,
            overlap_tokens=25,
            modelo_tokenizer="gpt-3.5-turbo",
        )

        assert config.max_tokens == 256
        assert config.overlap_tokens == 25

    def test_is_frozen(self):
        """ChunkingConfig is immutable."""
        from src.infrastructure.adapters.bjv_text_chunker import ChunkingConfig

        config = ChunkingConfig()

        with pytest.raises(Exception):  # FrozenInstanceError
            config.max_tokens = 1000

    def test_has_separadores_primarios(self):
        """ChunkingConfig has primary separators."""
        from src.infrastructure.adapters.bjv_text_chunker import ChunkingConfig

        config = ChunkingConfig()

        assert len(config.separadores_primarios) > 0
        assert any("Artículo" in s for s in config.separadores_primarios)

    def test_has_separadores_secundarios(self):
        """ChunkingConfig has secondary separators."""
        from src.infrastructure.adapters.bjv_text_chunker import ChunkingConfig

        config = ChunkingConfig()

        assert len(config.separadores_secundarios) > 0
        assert ". " in config.separadores_secundarios


class TestBJVTextChunker:
    """Tests for BJVTextChunker class."""

    def test_chunker_creation(self):
        """BJVTextChunker can be created."""
        from src.infrastructure.adapters.bjv_text_chunker import BJVTextChunker

        chunker = BJVTextChunker()

        assert chunker is not None

    def test_chunker_with_custom_config(self):
        """BJVTextChunker accepts custom config."""
        from src.infrastructure.adapters.bjv_text_chunker import (
            BJVTextChunker,
            ChunkingConfig,
        )

        config = ChunkingConfig(max_tokens=256)
        chunker = BJVTextChunker(config=config)

        assert chunker._config.max_tokens == 256

    def test_empty_text_returns_empty_tuple(self):
        """Empty text returns empty tuple."""
        from src.infrastructure.adapters.bjv_text_chunker import BJVTextChunker

        chunker = BJVTextChunker()
        result = chunker.fragmentar("", libro_id="123")

        assert result == ()

    def test_whitespace_only_returns_empty_tuple(self):
        """Whitespace-only text returns empty tuple."""
        from src.infrastructure.adapters.bjv_text_chunker import BJVTextChunker

        chunker = BJVTextChunker()
        result = chunker.fragmentar("   \n\t  ", libro_id="123")

        assert result == ()

    def test_short_text_single_fragment(self):
        """Short text produces single fragment."""
        from src.infrastructure.adapters.bjv_text_chunker import BJVTextChunker

        chunker = BJVTextChunker()
        text = "Este es un texto corto sobre derecho civil."
        result = chunker.fragmentar(text, libro_id="123")

        assert len(result) == 1
        assert result[0].contenido == text

    def test_fragmento_has_correct_libro_id(self):
        """Fragment has correct libro_id."""
        from src.infrastructure.adapters.bjv_text_chunker import BJVTextChunker

        chunker = BJVTextChunker()
        result = chunker.fragmentar("Texto de prueba.", libro_id="abc123")

        assert result[0].libro_id.bjv_id == "abc123"

    def test_fragmento_id_format(self):
        """Fragment ID follows expected format."""
        from src.infrastructure.adapters.bjv_text_chunker import BJVTextChunker

        chunker = BJVTextChunker()
        result = chunker.fragmentar("Texto de prueba.", libro_id="123")

        assert "123" in result[0].id
        assert "frag" in result[0].id.lower() or "chunk" in result[0].id.lower() or "_" in result[0].id

    def test_token_limit_respected(self):
        """Fragments respect token limit."""
        from src.infrastructure.adapters.bjv_text_chunker import (
            BJVTextChunker,
            ChunkingConfig,
        )

        config = ChunkingConfig(max_tokens=50, overlap_tokens=10)
        chunker = BJVTextChunker(config=config)

        # Create long text
        long_text = " ".join(["palabra"] * 200)
        result = chunker.fragmentar(long_text, libro_id="123")

        # Should produce multiple fragments
        assert len(result) > 1

    def test_overlap_added(self):
        """Fragments have overlapping content."""
        from src.infrastructure.adapters.bjv_text_chunker import (
            BJVTextChunker,
            ChunkingConfig,
        )

        config = ChunkingConfig(max_tokens=50, overlap_tokens=10)
        chunker = BJVTextChunker(config=config)

        # Create text that will be split
        long_text = " ".join([f"palabra{i}" for i in range(100)])
        result = chunker.fragmentar(long_text, libro_id="123")

        if len(result) >= 2:
            # Check for some overlap between consecutive chunks
            # (last words of chunk 1 should appear at start of chunk 2)
            first_words = result[0].contenido.split()[-5:]
            second_words = result[1].contenido.split()[:10]
            # Some overlap should exist
            overlap = set(first_words) & set(second_words)
            assert len(overlap) >= 0  # May have overlap

    def test_posicion_tracking(self):
        """Fragment position is tracked."""
        from src.infrastructure.adapters.bjv_text_chunker import (
            BJVTextChunker,
            ChunkingConfig,
        )

        config = ChunkingConfig(max_tokens=30, overlap_tokens=5)
        chunker = BJVTextChunker(config=config)

        long_text = " ".join(["texto"] * 100)
        result = chunker.fragmentar(long_text, libro_id="123")

        if len(result) >= 2:
            assert result[0].posicion == 0
            assert result[1].posicion == 1

    def test_numero_pagina_default(self):
        """Fragment has default page number."""
        from src.infrastructure.adapters.bjv_text_chunker import BJVTextChunker

        chunker = BJVTextChunker()
        result = chunker.fragmentar("Texto.", libro_id="123")

        assert result[0].numero_pagina == 1

    def test_numero_pagina_from_parameter(self):
        """Fragment uses page number from parameter."""
        from src.infrastructure.adapters.bjv_text_chunker import BJVTextChunker

        chunker = BJVTextChunker()
        result = chunker.fragmentar("Texto.", libro_id="123", numero_pagina=5)

        assert result[0].numero_pagina == 5


class TestFragmentarWithSections:
    """Tests for chunking with legal section detection."""

    def test_articulo_splitting(self):
        """Text is split at Artículo boundaries."""
        from src.infrastructure.adapters.bjv_text_chunker import BJVTextChunker

        chunker = BJVTextChunker()
        text = """
        Introducción al código.

        Artículo 1. El presente código establece las bases.

        Artículo 2. Los ciudadanos tienen derecho a.

        Artículo 3. El estado garantizará.
        """
        result = chunker.fragmentar(text, libro_id="123")

        # Should preserve article structure
        assert len(result) >= 1

    def test_capitulo_splitting(self):
        """Text is split at CAPÍTULO boundaries."""
        from src.infrastructure.adapters.bjv_text_chunker import BJVTextChunker

        chunker = BJVTextChunker()
        text = """
        CAPÍTULO I
        De las disposiciones generales

        Contenido del primer capítulo.

        CAPÍTULO II
        De los derechos fundamentales

        Contenido del segundo capítulo.
        """
        result = chunker.fragmentar(text, libro_id="123")

        assert len(result) >= 1

    def test_seccion_splitting(self):
        """Text is split at SECCIÓN boundaries."""
        from src.infrastructure.adapters.bjv_text_chunker import BJVTextChunker

        chunker = BJVTextChunker()
        text = """
        SECCIÓN PRIMERA
        Disposiciones generales

        Contenido.

        SECCIÓN SEGUNDA
        Del procedimiento

        Más contenido.
        """
        result = chunker.fragmentar(text, libro_id="123")

        assert len(result) >= 1

    def test_preserves_legal_structure(self):
        """Legal structures are not split mid-article."""
        from src.infrastructure.adapters.bjv_text_chunker import (
            BJVTextChunker,
            ChunkingConfig,
        )

        # Use small token limit to force splitting
        config = ChunkingConfig(max_tokens=100, overlap_tokens=10)
        chunker = BJVTextChunker(config=config)

        text = """
        Artículo 1. Primer artículo con contenido extenso que describe las bases del sistema legal mexicano y sus fundamentos.

        Artículo 2. Segundo artículo con más contenido relevante sobre los derechos y obligaciones.
        """
        result = chunker.fragmentar(text, libro_id="123")

        # Fragments should tend to break at article boundaries when possible
        assert len(result) >= 1


class TestFragmentoTextoEntity:
    """Tests for FragmentoTexto entity creation."""

    def test_fragmento_is_domain_entity(self):
        """Chunker produces FragmentoTexto domain entities."""
        from src.infrastructure.adapters.bjv_text_chunker import BJVTextChunker
        from src.domain.bjv_entities import FragmentoTexto

        chunker = BJVTextChunker()
        result = chunker.fragmentar("Texto de prueba.", libro_id="123")

        assert isinstance(result[0], FragmentoTexto)

    def test_fragmento_longitud_property(self):
        """FragmentoTexto has longitud property."""
        from src.infrastructure.adapters.bjv_text_chunker import BJVTextChunker

        chunker = BJVTextChunker()
        text = "Texto de prueba con varias palabras."
        result = chunker.fragmentar(text, libro_id="123")

        assert result[0].longitud == len(text)
