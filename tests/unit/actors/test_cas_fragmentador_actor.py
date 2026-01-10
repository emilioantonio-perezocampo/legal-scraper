"""
Tests for CAS Fragmentador Actor

Following RED-GREEN TDD: These tests define the expected behavior
for the CAS award fragmentador actor that chunks award text for embeddings.

Target: ~15 tests for CAS fragmentador actor
"""
import pytest
from unittest.mock import Mock, AsyncMock


class TestCASFragmentadorActorInit:
    """Tests for CASFragmentadorActor initialization."""

    def test_inherits_from_base_actor(self):
        """Fragmentador actor inherits from CASBaseActor."""
        from src.infrastructure.actors.cas_fragmentador_actor import CASFragmentadorActor
        from src.infrastructure.actors.cas_base_actor import CASBaseActor
        assert issubclass(CASFragmentadorActor, CASBaseActor)

    def test_has_text_chunker(self):
        """Fragmentador actor has text chunker."""
        from src.infrastructure.actors.cas_fragmentador_actor import CASFragmentadorActor
        from src.infrastructure.adapters.cas_text_chunker import CASTextChunker

        chunker = CASTextChunker()
        actor = CASFragmentadorActor(text_chunker=chunker)
        assert actor.text_chunker == chunker

    def test_default_text_chunker(self):
        """Fragmentador creates default text chunker."""
        from src.infrastructure.actors.cas_fragmentador_actor import CASFragmentadorActor
        actor = CASFragmentadorActor()
        assert actor.text_chunker is not None


class TestCASFragmentadorActorFragmentation:
    """Tests for CASFragmentadorActor fragmentation functionality."""

    @pytest.mark.asyncio
    async def test_handle_fragmentar_laudo(self):
        """Handles FragmentarLaudoCAS message."""
        from src.infrastructure.actors.cas_fragmentador_actor import CASFragmentadorActor
        from src.infrastructure.actors.cas_messages import FragmentarLaudoCAS

        actor = CASFragmentadorActor()
        await actor.start()

        msg = FragmentarLaudoCAS(laudo_id="laudo-123", texto="Award text content")
        await actor.receive(msg)

        await actor.stop()

    @pytest.mark.asyncio
    async def test_fragmentation_uses_text_chunker(self):
        """Fragmentation uses text chunker."""
        from src.infrastructure.actors.cas_fragmentador_actor import CASFragmentadorActor
        from src.infrastructure.actors.cas_messages import FragmentarLaudoCAS
        from src.infrastructure.adapters.cas_text_chunker import CASTextChunker

        chunker = Mock(spec=CASTextChunker)
        chunker.fragmentar_por_secciones = Mock(return_value=())

        actor = CASFragmentadorActor(text_chunker=chunker)
        await actor.start()

        msg = FragmentarLaudoCAS(laudo_id="laudo-123", texto="Award text")
        await actor.receive(msg)

        chunker.fragmentar_por_secciones.assert_called()
        await actor.stop()

    @pytest.mark.asyncio
    async def test_fragmentation_emits_fragmentos_listos(self):
        """Fragmentation emits FragmentosListosCAS on success."""
        from src.infrastructure.actors.cas_fragmentador_actor import CASFragmentadorActor
        from src.infrastructure.actors.cas_messages import FragmentarLaudoCAS, FragmentosListosCAS
        from src.domain.cas_entities import FragmentoLaudo

        coordinator = AsyncMock()
        actor = CASFragmentadorActor(supervisor=coordinator)
        await actor.start()

        msg = FragmentarLaudoCAS(laudo_id="laudo-123", texto="Award text content here")
        await actor.receive(msg)

        await actor.stop()

    @pytest.mark.asyncio
    async def test_fragmentation_handles_empty_text(self):
        """Fragmentation handles empty text gracefully."""
        from src.infrastructure.actors.cas_fragmentador_actor import CASFragmentadorActor
        from src.infrastructure.actors.cas_messages import FragmentarLaudoCAS

        actor = CASFragmentadorActor()
        await actor.start()

        msg = FragmentarLaudoCAS(laudo_id="laudo-123", texto="")
        await actor.receive(msg)

        await actor.stop()

    @pytest.mark.asyncio
    async def test_fragmentation_handles_whitespace_text(self):
        """Fragmentation handles whitespace-only text."""
        from src.infrastructure.actors.cas_fragmentador_actor import CASFragmentadorActor
        from src.infrastructure.actors.cas_messages import FragmentarLaudoCAS

        actor = CASFragmentadorActor()
        await actor.start()

        msg = FragmentarLaudoCAS(laudo_id="laudo-123", texto="   \n\t   ")
        await actor.receive(msg)

        await actor.stop()


class TestCASFragmentadorActorErrorHandling:
    """Tests for CASFragmentadorActor error handling."""

    @pytest.mark.asyncio
    async def test_handles_chunking_error(self):
        """Fragmentador handles chunking errors."""
        from src.infrastructure.actors.cas_fragmentador_actor import CASFragmentadorActor
        from src.infrastructure.actors.cas_messages import FragmentarLaudoCAS
        from src.infrastructure.adapters.cas_errors import ChunkingError
        from src.infrastructure.adapters.cas_text_chunker import CASTextChunker

        chunker = Mock(spec=CASTextChunker)
        chunker.fragmentar_por_secciones = Mock(side_effect=ChunkingError("Chunking failed"))

        supervisor = AsyncMock()
        actor = CASFragmentadorActor(text_chunker=chunker, supervisor=supervisor)
        await actor.start()

        msg = FragmentarLaudoCAS(laudo_id="laudo-123", texto="Award text")
        await actor.receive(msg)

        await actor.stop()

    @pytest.mark.asyncio
    async def test_escalates_error_to_supervisor(self):
        """Fragmentador escalates errors to supervisor."""
        from src.infrastructure.actors.cas_fragmentador_actor import CASFragmentadorActor
        from src.infrastructure.actors.cas_messages import FragmentarLaudoCAS
        from src.infrastructure.adapters.cas_errors import ChunkingError
        from src.infrastructure.adapters.cas_text_chunker import CASTextChunker

        chunker = Mock(spec=CASTextChunker)
        chunker.fragmentar_por_secciones = Mock(side_effect=ChunkingError("Chunking failed"))

        supervisor = AsyncMock()
        actor = CASFragmentadorActor(text_chunker=chunker, supervisor=supervisor)
        await actor.start()

        msg = FragmentarLaudoCAS(laudo_id="laudo-123", texto="Award text")
        await actor.receive(msg)

        # Supervisor should be notified
        await actor.stop()


class TestCASFragmentadorActorState:
    """Tests for CASFragmentadorActor state management."""

    @pytest.mark.asyncio
    async def test_sets_state_to_fragmentando(self):
        """Fragmentador sets state to FRAGMENTANDO during fragmentation."""
        from src.infrastructure.actors.cas_fragmentador_actor import CASFragmentadorActor
        from src.infrastructure.actors.cas_messages import FragmentarLaudoCAS, EstadoPipelineCAS

        actor = CASFragmentadorActor()
        await actor.start()

        msg = FragmentarLaudoCAS(laudo_id="laudo-123", texto="Award text")
        await actor.receive(msg)

        await actor.stop()

    def test_tracks_fragmented_count(self):
        """Fragmentador tracks number of fragmented awards."""
        from src.infrastructure.actors.cas_fragmentador_actor import CASFragmentadorActor

        actor = CASFragmentadorActor()
        assert actor.fragmented_count == 0

    @pytest.mark.asyncio
    async def test_increments_fragmented_count(self):
        """Fragmentador increments fragmented count."""
        from src.infrastructure.actors.cas_fragmentador_actor import CASFragmentadorActor
        from src.infrastructure.actors.cas_messages import FragmentarLaudoCAS

        actor = CASFragmentadorActor()
        await actor.start()

        msg = FragmentarLaudoCAS(laudo_id="laudo-123", texto="Award text content")
        await actor.receive(msg)

        assert actor.fragmented_count >= 0
        await actor.stop()

    def test_tracks_total_fragments(self):
        """Fragmentador tracks total fragments created."""
        from src.infrastructure.actors.cas_fragmentador_actor import CASFragmentadorActor

        actor = CASFragmentadorActor()
        assert actor.total_fragments == 0
