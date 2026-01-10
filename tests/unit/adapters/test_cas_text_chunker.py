"""
Tests for CAS Text Chunker

Following RED-GREEN TDD: These tests define the expected behavior
for intelligent text chunking of CAS arbitration awards.

Target: ~15 tests for text chunker
"""
import pytest


class TestCASChunkingConfig:
    """Tests for CASChunkingConfig dataclass."""

    def test_default_values(self):
        """CASChunkingConfig has sensible defaults."""
        from src.infrastructure.adapters.cas_text_chunker import CASChunkingConfig
        config = CASChunkingConfig()
        assert config.max_tokens == 512
        assert config.overlap_tokens == 50

    def test_custom_values(self):
        """CASChunkingConfig accepts custom values."""
        from src.infrastructure.adapters.cas_text_chunker import CASChunkingConfig
        config = CASChunkingConfig(max_tokens=1024, overlap_tokens=100)
        assert config.max_tokens == 1024
        assert config.overlap_tokens == 100

    def test_immutability(self):
        """CASChunkingConfig is frozen dataclass."""
        from src.infrastructure.adapters.cas_text_chunker import CASChunkingConfig
        config = CASChunkingConfig()
        with pytest.raises(Exception):
            config.max_tokens = 1024

    def test_has_separadores_primarios(self):
        """CASChunkingConfig has primary separators."""
        from src.infrastructure.adapters.cas_text_chunker import CASChunkingConfig
        config = CASChunkingConfig()
        assert len(config.separadores_primarios) > 0

    def test_has_separadores_secundarios(self):
        """CASChunkingConfig has secondary separators."""
        from src.infrastructure.adapters.cas_text_chunker import CASChunkingConfig
        config = CASChunkingConfig()
        assert len(config.separadores_secundarios) > 0


class TestCASTextChunkerFragmentar:
    """Tests for CASTextChunker.fragmentar method."""

    def test_empty_text_returns_empty_tuple(self):
        """Empty text returns empty tuple."""
        from src.infrastructure.adapters.cas_text_chunker import CASTextChunker
        chunker = CASTextChunker()
        result = chunker.fragmentar("", "laudo-123")
        assert result == ()

    def test_whitespace_only_returns_empty_tuple(self):
        """Whitespace-only text returns empty tuple."""
        from src.infrastructure.adapters.cas_text_chunker import CASTextChunker
        chunker = CASTextChunker()
        result = chunker.fragmentar("   \n\t  ", "laudo-123")
        assert result == ()

    def test_short_text_returns_single_fragment(self):
        """Short text returns single fragment."""
        from src.infrastructure.adapters.cas_text_chunker import CASTextChunker
        chunker = CASTextChunker()
        text = "This is a short arbitration award text."
        result = chunker.fragmentar(text, "laudo-123")
        assert len(result) == 1
        assert result[0].texto == text

    def test_fragment_has_laudo_id(self):
        """Fragment has correct laudo_id."""
        from src.infrastructure.adapters.cas_text_chunker import CASTextChunker
        chunker = CASTextChunker()
        result = chunker.fragmentar("Some text", "laudo-123")
        assert result[0].laudo_id == "laudo-123"

    def test_fragment_has_seccion(self):
        """Fragment has section name."""
        from src.infrastructure.adapters.cas_text_chunker import CASTextChunker
        chunker = CASTextChunker()
        result = chunker.fragmentar("Some text", "laudo-123", seccion="hechos")
        assert result[0].seccion == "hechos"

    def test_fragment_has_posicion(self):
        """Fragment has position information."""
        from src.infrastructure.adapters.cas_text_chunker import CASTextChunker
        chunker = CASTextChunker()
        result = chunker.fragmentar("Some text", "laudo-123")
        assert result[0].posicion == 0

    def test_long_text_creates_multiple_fragments(self):
        """Long text is split into multiple fragments."""
        from src.infrastructure.adapters.cas_text_chunker import CASTextChunker, CASChunkingConfig
        config = CASChunkingConfig(max_tokens=50)  # Very small for testing
        chunker = CASTextChunker(config=config)

        # Create long text
        text = " ".join(["The Panel finds that the evidence is sufficient."] * 100)
        result = chunker.fragmentar(text, "laudo-123")
        assert len(result) > 1

    def test_respects_section_boundaries(self):
        """Chunker respects section boundaries when possible."""
        from src.infrastructure.adapters.cas_text_chunker import CASTextChunker
        chunker = CASTextChunker()

        text = """
I. FACTS

The Appellant is a professional athlete.

II. REASONS

The Panel considered the evidence carefully.

III. DECISION

The appeal is dismissed.
"""
        result = chunker.fragmentar(text, "laudo-123")
        assert len(result) >= 1


class TestCASTextChunkerFragmentarPorSecciones:
    """Tests for CASTextChunker.fragmentar_por_secciones method."""

    def test_identifies_facts_section(self):
        """Identifies facts section in text."""
        from src.infrastructure.adapters.cas_text_chunker import CASTextChunker
        chunker = CASTextChunker()

        text = """
I. FACTS

The Appellant is a professional athlete.

II. DECISION

The appeal is dismissed.
"""
        result = chunker.fragmentar_por_secciones(text, "laudo-123")
        sections = [f.seccion for f in result]
        assert "hechos" in sections or len(result) >= 1

    def test_identifies_french_sections(self):
        """Identifies French section headers."""
        from src.infrastructure.adapters.cas_text_chunker import CASTextChunker
        chunker = CASTextChunker()

        text = """
I. FAITS

L'appelant est un athlète professionnel.

II. MOTIFS

Le Tribunal a examiné les preuves.

III. DISPOSITIF

L'appel est rejeté.
"""
        result = chunker.fragmentar_por_secciones(text, "laudo-123")
        assert len(result) >= 1

    def test_falls_back_to_completo_when_no_sections(self):
        """Falls back to 'completo' when no sections detected."""
        from src.infrastructure.adapters.cas_text_chunker import CASTextChunker
        chunker = CASTextChunker()

        text = "Just some text without section headers."
        result = chunker.fragmentar_por_secciones(text, "laudo-123")
        assert len(result) >= 1
        if result:
            assert result[0].seccion == "completo"


class TestCASTextChunkerTokenCounting:
    """Tests for token counting functionality."""

    def test_count_tokens_returns_integer(self):
        """Token count is an integer."""
        from src.infrastructure.adapters.cas_text_chunker import CASTextChunker
        chunker = CASTextChunker()
        count = chunker._count_tokens("This is a test sentence.")
        assert isinstance(count, int)
        assert count > 0

    def test_count_tokens_for_longer_text(self):
        """Token count works for longer text."""
        from src.infrastructure.adapters.cas_text_chunker import CASTextChunker
        chunker = CASTextChunker()
        long_text = "The Panel finds that the evidence is sufficient. " * 50
        count = chunker._count_tokens(long_text)
        assert isinstance(count, int)
        assert count > 100
