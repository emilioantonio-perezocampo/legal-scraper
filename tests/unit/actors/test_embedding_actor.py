"""
RED Phase Tests: EmbeddingActor

Tests for embedding generation actor.
These tests must FAIL initially (RED phase).
"""
import pytest
import asyncio
from unittest.mock import MagicMock, AsyncMock

# These imports will fail until implementation exists
from src.infrastructure.actors.embedding_actor import EmbeddingActor
from src.infrastructure.actors.messages import (
    GenerarEmbeddings,
    EmbeddingsGenerados,
    ErrorDeActor,
)
from src.domain.scjn_entities import TextChunk, DocumentEmbedding


@pytest.fixture
def sample_chunks():
    """Create sample text chunks for testing."""
    return (
        TextChunk(
            id="chunk-001",
            document_id="doc-123",
            content="Artículo 1.- La presente Ley es de observancia general.",
            token_count=50,
            chunk_index=0,
        ),
        TextChunk(
            id="chunk-002",
            document_id="doc-123",
            content="Artículo 2.- Las normas del trabajo tienden al equilibrio.",
            token_count=45,
            chunk_index=1,
        ),
        TextChunk(
            id="chunk-003",
            document_id="doc-123",
            content="Artículo 3.- El trabajo es un derecho y un deber sociales.",
            token_count=48,
            chunk_index=2,
        ),
    )


@pytest.fixture
def mock_model():
    """Create a mock embedding model."""
    model = MagicMock()
    model.encode.return_value = [[0.1] * 384, [0.2] * 384, [0.3] * 384]
    return model


class TestEmbeddingActorLifecycle:
    """Tests for EmbeddingActor lifecycle."""

    @pytest.mark.asyncio
    async def test_start_loads_model(self):
        """Start should initialize embedding model."""
        actor = EmbeddingActor()
        await actor.start()

        assert actor._model is not None

        await actor.stop()

    @pytest.mark.asyncio
    async def test_stop_is_clean(self):
        """Stop should cleanly terminate the actor."""
        actor = EmbeddingActor()
        await actor.start()
        await actor.stop()

        assert not actor._running

    @pytest.mark.asyncio
    async def test_custom_model_name(self):
        """Should accept custom model name."""
        actor = EmbeddingActor(model_name="custom-model")
        assert actor._model_name == "custom-model"


class TestEmbeddingActorGeneration:
    """Tests for embedding generation."""

    @pytest.mark.asyncio
    async def test_generate_single_embedding(self, sample_chunks):
        """Should generate embedding for single chunk."""
        actor = EmbeddingActor()
        await actor.start()

        # Use only first chunk
        single_chunk = (sample_chunks[0],)
        result = await actor.ask(
            GenerarEmbeddings(document_id="doc-123", chunks=single_chunk)
        )

        assert isinstance(result, EmbeddingsGenerados)
        assert result.embedding_count == 1
        assert result.document_id == "doc-123"

        await actor.stop()

    @pytest.mark.asyncio
    async def test_generate_multiple_embeddings(self, sample_chunks):
        """Should generate embeddings for multiple chunks."""
        actor = EmbeddingActor()
        await actor.start()

        result = await actor.ask(
            GenerarEmbeddings(document_id="doc-123", chunks=sample_chunks)
        )

        assert isinstance(result, EmbeddingsGenerados)
        assert result.embedding_count == 3
        assert result.document_id == "doc-123"

        await actor.stop()

    @pytest.mark.asyncio
    async def test_generate_returns_embeddings(self, sample_chunks):
        """Generated embeddings should be retrievable."""
        actor = EmbeddingActor()
        await actor.start()

        result = await actor.ask(
            GenerarEmbeddings(document_id="doc-123", chunks=sample_chunks)
        )

        # Get the generated embeddings
        embeddings = await actor.ask(("GET_EMBEDDINGS", "doc-123"))

        assert len(embeddings) == 3
        for emb in embeddings:
            assert isinstance(emb, DocumentEmbedding)
            assert len(emb.vector) == 384  # Default dimension

        await actor.stop()

    @pytest.mark.asyncio
    async def test_embeddings_have_correct_chunk_ids(self, sample_chunks):
        """Embeddings should reference correct chunk IDs."""
        actor = EmbeddingActor()
        await actor.start()

        await actor.ask(GenerarEmbeddings(document_id="doc-123", chunks=sample_chunks))
        embeddings = await actor.ask(("GET_EMBEDDINGS", "doc-123"))

        chunk_ids = {e.chunk_id for e in embeddings}
        expected_ids = {"chunk-001", "chunk-002", "chunk-003"}
        assert chunk_ids == expected_ids

        await actor.stop()


class TestEmbeddingActorEmptyInput:
    """Tests for empty/edge case inputs."""

    @pytest.mark.asyncio
    async def test_empty_chunks_returns_zero(self):
        """Empty chunks should return 0 embeddings."""
        actor = EmbeddingActor()
        await actor.start()

        result = await actor.ask(GenerarEmbeddings(document_id="doc-123", chunks=()))

        assert isinstance(result, EmbeddingsGenerados)
        assert result.embedding_count == 0

        await actor.stop()


class TestEmbeddingActorBatching:
    """Tests for batch processing."""

    @pytest.mark.asyncio
    async def test_large_batch_processed(self):
        """Should handle large batches of chunks."""
        actor = EmbeddingActor()
        await actor.start()

        # Create many chunks
        chunks = tuple(
            TextChunk(
                id=f"chunk-{i:03d}",
                document_id="doc-large",
                content=f"Artículo {i}.- Contenido del artículo {i}.",
                token_count=30,
                chunk_index=i,
            )
            for i in range(50)
        )

        result = await actor.ask(
            GenerarEmbeddings(document_id="doc-large", chunks=chunks)
        )

        assert result.embedding_count == 50

        await actor.stop()


class TestEmbeddingActorCorrelation:
    """Tests for message correlation."""

    @pytest.mark.asyncio
    async def test_preserves_correlation_id(self, sample_chunks):
        """Result should preserve correlation_id."""
        actor = EmbeddingActor()
        await actor.start()

        cmd = GenerarEmbeddings(
            document_id="doc-123",
            chunks=sample_chunks,
            correlation_id="test-correlation-123",
        )
        result = await actor.ask(cmd)

        assert result.correlation_id == "test-correlation-123"

        await actor.stop()


class TestEmbeddingActorWithMock:
    """Tests using mock model."""

    @pytest.mark.asyncio
    async def test_with_injected_model(self, sample_chunks, mock_model):
        """Should work with injected mock model."""
        actor = EmbeddingActor()
        actor._model = mock_model  # Inject mock
        await actor.start()

        result = await actor.ask(
            GenerarEmbeddings(document_id="doc-123", chunks=sample_chunks)
        )

        assert result.embedding_count == 3
        mock_model.encode.assert_called()

        await actor.stop()


class TestEmbeddingActorErrorHandling:
    """Tests for error handling."""

    @pytest.mark.asyncio
    async def test_handles_encoding_error(self):
        """Should handle encoding errors gracefully."""
        actor = EmbeddingActor()
        await actor.start()

        # Create chunk with problematic content
        bad_chunk = TextChunk(
            id="bad-chunk",
            document_id="doc-bad",
            content="",  # Empty content
            token_count=0,
            chunk_index=0,
        )

        result = await actor.ask(
            GenerarEmbeddings(document_id="doc-bad", chunks=(bad_chunk,))
        )

        # Should handle gracefully (either skip or return 0)
        assert isinstance(result, (EmbeddingsGenerados, ErrorDeActor))

        await actor.stop()
