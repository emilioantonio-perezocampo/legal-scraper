"""
Tests for BJV Fragmentador Actor.

Following RED-GREEN TDD: These tests define the expected behavior
for text chunking using the BJVTextChunker.

Target: ~12 tests
"""
import pytest
from unittest.mock import Mock, MagicMock, AsyncMock, patch


class TestBJVFragmentadorActorInit:
    """Tests for BJVFragmentadorActor initialization."""

    def test_inherits_from_base_actor(self):
        """BJVFragmentadorActor inherits from BJVBaseActor."""
        from src.infrastructure.actors.bjv_fragmentador_actor import BJVFragmentadorActor
        from src.infrastructure.actors.bjv_base_actor import BJVBaseActor

        mock_coordinator = Mock()

        actor = BJVFragmentadorActor(coordinator=mock_coordinator)

        assert isinstance(actor, BJVBaseActor)

    def test_creates_with_coordinator(self):
        """BJVFragmentadorActor stores coordinator reference."""
        from src.infrastructure.actors.bjv_fragmentador_actor import BJVFragmentadorActor

        mock_coordinator = Mock()

        actor = BJVFragmentadorActor(coordinator=mock_coordinator)

        assert actor._coordinator is mock_coordinator

    def test_has_nombre(self):
        """BJVFragmentadorActor has descriptive nombre."""
        from src.infrastructure.actors.bjv_fragmentador_actor import BJVFragmentadorActor

        mock_coordinator = Mock()

        actor = BJVFragmentadorActor(coordinator=mock_coordinator)

        assert "Fragmentador" in actor.nombre

    def test_creates_chunker(self):
        """BJVFragmentadorActor creates text chunker."""
        from src.infrastructure.actors.bjv_fragmentador_actor import BJVFragmentadorActor
        from src.infrastructure.adapters.bjv_text_chunker import BJVTextChunker

        mock_coordinator = Mock()

        actor = BJVFragmentadorActor(coordinator=mock_coordinator)

        assert isinstance(actor._chunker, BJVTextChunker)


class TestBJVFragmentadorActorProcessing:
    """Tests for text fragmenting."""

    @pytest.mark.asyncio
    async def test_handle_fragmentar_texto(self):
        """Actor handles FragmentarTexto message."""
        from src.infrastructure.actors.bjv_fragmentador_actor import BJVFragmentadorActor
        from src.infrastructure.actors.bjv_messages import FragmentarTexto

        mock_coordinator = Mock()
        mock_coordinator.tell = AsyncMock()

        actor = BJVFragmentadorActor(coordinator=mock_coordinator)

        msg = FragmentarTexto(
            correlation_id="corr-123",
            libro_id="libro-456",
            capitulo_id=None,
            texto="Este es un texto de prueba para fragmentar.",
        )

        await actor.handle_message(msg)

        mock_coordinator.tell.assert_called_once()

    @pytest.mark.asyncio
    async def test_texto_fragmentado_returned(self):
        """Actor returns TextoFragmentado with fragments."""
        from src.infrastructure.actors.bjv_fragmentador_actor import BJVFragmentadorActor
        from src.infrastructure.actors.bjv_messages import FragmentarTexto, TextoFragmentado

        mock_coordinator = Mock()
        mock_coordinator.tell = AsyncMock()

        actor = BJVFragmentadorActor(coordinator=mock_coordinator)

        msg = FragmentarTexto(
            correlation_id="corr-123",
            libro_id="libro-456",
            capitulo_id=None,
            texto="Este es un texto de prueba para fragmentar.",
        )

        await actor.handle_message(msg)

        call_args = mock_coordinator.tell.call_args[0][0]
        assert isinstance(call_args, TextoFragmentado)
        assert call_args.correlation_id == "corr-123"
        assert call_args.libro_id == "libro-456"

    @pytest.mark.asyncio
    async def test_fragments_are_domain_entities(self):
        """Fragments are FragmentoTexto domain entities."""
        from src.infrastructure.actors.bjv_fragmentador_actor import BJVFragmentadorActor
        from src.infrastructure.actors.bjv_messages import FragmentarTexto, TextoFragmentado
        from src.domain.bjv_entities import FragmentoTexto as FragmentoTextoEntity

        mock_coordinator = Mock()
        mock_coordinator.tell = AsyncMock()

        actor = BJVFragmentadorActor(coordinator=mock_coordinator)

        msg = FragmentarTexto(
            correlation_id="corr-123",
            libro_id="libro-456",
            capitulo_id=None,
            texto="Este es un texto de prueba para fragmentar.",
        )

        await actor.handle_message(msg)

        call_args = mock_coordinator.tell.call_args[0][0]
        assert call_args.total_fragmentos >= 1
        if call_args.total_fragmentos > 0:
            assert isinstance(call_args.fragmentos[0], FragmentoTextoEntity)

    @pytest.mark.asyncio
    async def test_empty_text_produces_empty_fragments(self):
        """Empty text produces zero fragments."""
        from src.infrastructure.actors.bjv_fragmentador_actor import BJVFragmentadorActor
        from src.infrastructure.actors.bjv_messages import FragmentarTexto, TextoFragmentado

        mock_coordinator = Mock()
        mock_coordinator.tell = AsyncMock()

        actor = BJVFragmentadorActor(coordinator=mock_coordinator)

        msg = FragmentarTexto(
            correlation_id="corr-123",
            libro_id="libro-456",
            capitulo_id=None,
            texto="",
        )

        await actor.handle_message(msg)

        call_args = mock_coordinator.tell.call_args[0][0]
        assert isinstance(call_args, TextoFragmentado)
        assert call_args.total_fragmentos == 0
        assert call_args.fragmentos == ()

    @pytest.mark.asyncio
    async def test_long_text_produces_multiple_fragments(self):
        """Long text produces multiple fragments."""
        from src.infrastructure.actors.bjv_fragmentador_actor import BJVFragmentadorActor
        from src.infrastructure.actors.bjv_messages import FragmentarTexto, TextoFragmentado
        from src.infrastructure.adapters.bjv_text_chunker import ChunkingConfig

        mock_coordinator = Mock()
        mock_coordinator.tell = AsyncMock()

        # Use small token limit to force multiple fragments
        config = ChunkingConfig(max_tokens=50, overlap_tokens=10)
        actor = BJVFragmentadorActor(
            coordinator=mock_coordinator,
            chunking_config=config,
        )

        # Create long text
        long_text = " ".join(["palabra"] * 200)

        msg = FragmentarTexto(
            correlation_id="corr-123",
            libro_id="libro-456",
            capitulo_id=None,
            texto=long_text,
        )

        await actor.handle_message(msg)

        call_args = mock_coordinator.tell.call_args[0][0]
        assert call_args.total_fragmentos > 1


class TestBJVFragmentadorActorErrorHandling:
    """Tests for error handling."""

    @pytest.mark.asyncio
    async def test_chunking_error_emits_error(self):
        """Chunking error emits ErrorProcesamiento."""
        from src.infrastructure.actors.bjv_fragmentador_actor import BJVFragmentadorActor
        from src.infrastructure.actors.bjv_messages import FragmentarTexto, ErrorProcesamiento

        mock_coordinator = Mock()
        mock_coordinator.tell = AsyncMock()

        actor = BJVFragmentadorActor(coordinator=mock_coordinator)

        # Mock chunker to fail
        mock_chunker = Mock()
        mock_chunker.fragmentar = Mock(side_effect=Exception("Chunking failed"))
        actor._chunker = mock_chunker

        msg = FragmentarTexto(
            correlation_id="corr-123",
            libro_id="libro-456",
            capitulo_id=None,
            texto="Texto que causará error.",
        )

        await actor.handle_message(msg)

        call_args = mock_coordinator.tell.call_args[0][0]
        assert isinstance(call_args, ErrorProcesamiento)
        assert call_args.libro_id == "libro-456"

    @pytest.mark.asyncio
    async def test_forwards_to_coordinator(self):
        """All results forwarded to coordinator."""
        from src.infrastructure.actors.bjv_fragmentador_actor import BJVFragmentadorActor
        from src.infrastructure.actors.bjv_messages import FragmentarTexto

        mock_coordinator = Mock()
        mock_coordinator.tell = AsyncMock()

        actor = BJVFragmentadorActor(coordinator=mock_coordinator)

        msg = FragmentarTexto(
            correlation_id="corr-123",
            libro_id="libro-456",
            capitulo_id=None,
            texto="Texto de prueba.",
        )

        await actor.handle_message(msg)

        mock_coordinator.tell.assert_called_once()
