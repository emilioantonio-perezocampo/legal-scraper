"""
Tests for FAISS Vector Store Actor.
"""
import pytest
import pytest_asyncio
import asyncio
from unittest.mock import AsyncMock, patch, MagicMock

from src.infrastructure.actors.vector_store_actor import (
    FAISSVectorStoreActor,
    VectorEntry,
    _MockFAISSIndex,
)
from src.infrastructure.actors.messages import (
    GuardarEmbedding,
    GuardarEmbeddingsBatch,
    BuscarSimilares,
    ObtenerEstadisticasVectorStore,
    EmbeddingsGuardados,
    ResultadosBusqueda,
    EstadisticasVectorStore,
)
from src.domain.scjn_entities import DocumentEmbedding


class TestVectorEntry:
    """Tests for VectorEntry dataclass."""

    def test_vector_entry_creation(self):
        """VectorEntry should be creatable with required fields."""
        entry = VectorEntry(
            chunk_id="chunk-1",
            index_position=0,
            document_id="doc-1",
        )

        assert entry.chunk_id == "chunk-1"
        assert entry.index_position == 0
        assert entry.document_id == "doc-1"

    def test_vector_entry_is_frozen(self):
        """VectorEntry should be immutable."""
        entry = VectorEntry(
            chunk_id="chunk-1",
            index_position=0,
            document_id="doc-1",
        )

        with pytest.raises(Exception):
            entry.chunk_id = "modified"


class TestMockFAISSIndex:
    """Tests for _MockFAISSIndex fallback."""

    def test_mock_index_creation(self):
        """Mock index should initialize with dimension."""
        index = _MockFAISSIndex(dimension=384)

        assert index.dimension == 384
        assert index.ntotal == 0

    def test_mock_index_add_vectors(self):
        """Mock index should add vectors."""
        import numpy as np

        index = _MockFAISSIndex(dimension=4)
        vectors = np.array([[1.0, 2.0, 3.0, 4.0], [5.0, 6.0, 7.0, 8.0]], dtype=np.float32)

        index.add(vectors)

        assert index.ntotal == 2

    def test_mock_index_search(self):
        """Mock index should return nearest neighbors."""
        import numpy as np

        index = _MockFAISSIndex(dimension=4)
        vectors = np.array([
            [1.0, 0.0, 0.0, 0.0],
            [0.0, 1.0, 0.0, 0.0],
            [0.0, 0.0, 1.0, 0.0],
        ], dtype=np.float32)
        index.add(vectors)

        query = np.array([[1.0, 0.0, 0.0, 0.0]], dtype=np.float32)
        distances, indices = index.search(query, k=2)

        # First result should be exact match (index 0)
        assert indices[0][0] == 0
        assert distances[0][0] < 0.1  # Very close distance

    def test_mock_index_search_empty(self):
        """Mock index should handle empty search gracefully."""
        import numpy as np

        index = _MockFAISSIndex(dimension=4)
        query = np.array([[1.0, 0.0, 0.0, 0.0]], dtype=np.float32)

        distances, indices = index.search(query, k=5)

        # Should return -1 indices and inf distances for missing results
        assert indices[0][0] == -1
        assert distances[0][0] == float('inf')


class TestFAISSVectorStoreActorCreation:
    """Tests for FAISSVectorStoreActor initialization."""

    def test_actor_creation_with_defaults(self):
        """Actor should create with default settings."""
        actor = FAISSVectorStoreActor()

        assert actor.dimension == 384  # MiniLM default
        assert actor._index_type == "Flat"
        assert actor._storage_path is None

    def test_actor_creation_with_custom_dimension(self):
        """Actor should accept custom dimension."""
        actor = FAISSVectorStoreActor(dimension=768)

        assert actor.dimension == 768

    def test_actor_creation_with_storage_path(self):
        """Actor should accept storage path."""
        actor = FAISSVectorStoreActor(storage_path="/tmp/vectors.index")

        assert actor._storage_path is not None

    def test_actor_creation_with_index_type(self):
        """Actor should accept index type."""
        actor = FAISSVectorStoreActor(index_type="IVF")

        assert actor._index_type == "IVF"


class TestFAISSVectorStoreActorLifecycle:
    """Tests for actor start/stop lifecycle."""

    @pytest.mark.asyncio
    async def test_actor_starts_successfully(self):
        """Actor should start and initialize index."""
        actor = FAISSVectorStoreActor()

        await actor.start()

        assert actor._running
        assert actor._index is not None

        await actor.stop()

    @pytest.mark.asyncio
    async def test_actor_stops_cleanly(self):
        """Actor should stop without errors."""
        actor = FAISSVectorStoreActor()

        await actor.start()
        await actor.stop()

        assert not actor._running

    @pytest.mark.asyncio
    async def test_actor_uses_mock_when_faiss_unavailable(self):
        """Actor should fallback to mock index if FAISS not installed."""
        actor = FAISSVectorStoreActor()

        # Force mock by making import fail
        with patch.dict('sys.modules', {'faiss': None}):
            await actor.start()

        # Should still work with mock
        assert actor._index is not None

        await actor.stop()


class TestFAISSVectorStoreActorSave:
    """Tests for saving embeddings."""

    @pytest_asyncio.fixture
    async def actor(self):
        """Create and start actor for tests."""
        actor = FAISSVectorStoreActor(dimension=4)
        await actor.start()
        yield actor
        await actor.stop()

    @pytest.mark.asyncio
    async def test_save_single_embedding(self, actor):
        """Should save a single embedding."""
        embedding = DocumentEmbedding(
            chunk_id="chunk-1",
            vector=(0.1, 0.2, 0.3, 0.4),
            model_name="test-model",
        )

        result = await actor.ask(GuardarEmbedding(embedding=embedding))

        assert isinstance(result, EmbeddingsGuardados)
        assert result.count == 1
        assert actor.total_vectors == 1

    @pytest.mark.asyncio
    async def test_save_single_embedding_none(self, actor):
        """Should handle None embedding gracefully."""
        result = await actor.ask(GuardarEmbedding(embedding=None))

        assert isinstance(result, EmbeddingsGuardados)
        assert result.count == 0

    @pytest.mark.asyncio
    async def test_save_batch_embeddings(self, actor):
        """Should save batch of embeddings."""
        embeddings = (
            DocumentEmbedding(
                chunk_id="chunk-1",
                vector=(0.1, 0.2, 0.3, 0.4),
            ),
            DocumentEmbedding(
                chunk_id="chunk-2",
                vector=(0.5, 0.6, 0.7, 0.8),
            ),
            DocumentEmbedding(
                chunk_id="chunk-3",
                vector=(0.9, 1.0, 1.1, 1.2),
            ),
        )

        result = await actor.ask(GuardarEmbeddingsBatch(
            embeddings=embeddings,
            document_id="doc-1",
        ))

        assert isinstance(result, EmbeddingsGuardados)
        assert result.count == 3
        assert result.document_id == "doc-1"
        assert actor.total_vectors == 3

    @pytest.mark.asyncio
    async def test_save_empty_batch(self, actor):
        """Should handle empty batch."""
        result = await actor.ask(GuardarEmbeddingsBatch(
            embeddings=(),
            document_id="doc-1",
        ))

        assert result.count == 0

    @pytest.mark.asyncio
    async def test_save_tracks_document_mapping(self, actor):
        """Should track document to chunks mapping."""
        embeddings = (
            DocumentEmbedding(chunk_id="c1", vector=(0.1, 0.2, 0.3, 0.4)),
            DocumentEmbedding(chunk_id="c2", vector=(0.5, 0.6, 0.7, 0.8)),
        )

        await actor.ask(GuardarEmbeddingsBatch(
            embeddings=embeddings,
            document_id="doc-1",
        ))

        assert "doc-1" in actor._document_chunks
        assert actor._document_chunks["doc-1"] == {"c1", "c2"}


class TestFAISSVectorStoreActorSearch:
    """Tests for similarity search."""

    @pytest_asyncio.fixture
    async def actor_with_data(self):
        """Create actor with pre-loaded data."""
        actor = FAISSVectorStoreActor(dimension=4)
        await actor.start()

        # Add test vectors
        embeddings = (
            DocumentEmbedding(chunk_id="c1", vector=(1.0, 0.0, 0.0, 0.0)),
            DocumentEmbedding(chunk_id="c2", vector=(0.0, 1.0, 0.0, 0.0)),
            DocumentEmbedding(chunk_id="c3", vector=(0.0, 0.0, 1.0, 0.0)),
            DocumentEmbedding(chunk_id="c4", vector=(0.0, 0.0, 0.0, 1.0)),
        )

        await actor.ask(GuardarEmbeddingsBatch(
            embeddings=embeddings,
            document_id="doc-1",
        ))

        yield actor
        await actor.stop()

    @pytest.mark.asyncio
    async def test_search_returns_results(self, actor_with_data):
        """Should return similar vectors."""
        query = (1.0, 0.0, 0.0, 0.0)  # Should match c1

        result = await actor_with_data.ask(BuscarSimilares(
            query_vector=query,
            top_k=2,
        ))

        assert isinstance(result, ResultadosBusqueda)
        assert len(result.results) > 0
        # First result should be c1 (exact match)
        assert result.results[0][0] == "c1"

    @pytest.mark.asyncio
    async def test_search_respects_top_k(self, actor_with_data):
        """Should respect top_k limit."""
        result = await actor_with_data.ask(BuscarSimilares(
            query_vector=(0.5, 0.5, 0.0, 0.0),
            top_k=2,
        ))

        assert len(result.results) <= 2

    @pytest.mark.asyncio
    async def test_search_includes_timing(self, actor_with_data):
        """Should include search timing."""
        result = await actor_with_data.ask(BuscarSimilares(
            query_vector=(1.0, 0.0, 0.0, 0.0),
            top_k=2,
        ))

        assert result.search_time_ms >= 0

    @pytest.mark.asyncio
    async def test_search_empty_query(self, actor_with_data):
        """Should handle empty query."""
        result = await actor_with_data.ask(BuscarSimilares(
            query_vector=(),
            top_k=2,
        ))

        assert result.results == ()

    @pytest.mark.asyncio
    async def test_search_empty_index(self):
        """Should handle search on empty index."""
        actor = FAISSVectorStoreActor(dimension=4)
        await actor.start()

        result = await actor.ask(BuscarSimilares(
            query_vector=(1.0, 0.0, 0.0, 0.0),
            top_k=2,
        ))

        assert result.results == ()

        await actor.stop()

    @pytest.mark.asyncio
    async def test_search_with_document_filter(self):
        """Should filter results by document ID."""
        actor = FAISSVectorStoreActor(dimension=4)
        await actor.start()

        # Add vectors from different documents
        await actor.ask(GuardarEmbeddingsBatch(
            embeddings=(
                DocumentEmbedding(chunk_id="d1c1", vector=(1.0, 0.0, 0.0, 0.0)),
            ),
            document_id="doc-1",
        ))
        await actor.ask(GuardarEmbeddingsBatch(
            embeddings=(
                DocumentEmbedding(chunk_id="d2c1", vector=(0.9, 0.1, 0.0, 0.0)),
            ),
            document_id="doc-2",
        ))

        # Search with filter for doc-2 only
        result = await actor.ask(BuscarSimilares(
            query_vector=(1.0, 0.0, 0.0, 0.0),
            top_k=5,
            filter_document_ids=("doc-2",),
        ))

        # Should only return results from doc-2
        for chunk_id, _ in result.results:
            assert chunk_id.startswith("d2")

        await actor.stop()


class TestFAISSVectorStoreActorStats:
    """Tests for statistics retrieval."""

    @pytest.mark.asyncio
    async def test_stats_on_empty_store(self):
        """Should return stats for empty store."""
        actor = FAISSVectorStoreActor(dimension=384)
        await actor.start()

        result = await actor.ask(ObtenerEstadisticasVectorStore())

        assert isinstance(result, EstadisticasVectorStore)
        assert result.total_vectors == 0
        assert result.dimension == 384
        assert result.document_count == 0

        await actor.stop()

    @pytest.mark.asyncio
    async def test_stats_with_data(self):
        """Should return accurate stats with data."""
        actor = FAISSVectorStoreActor(dimension=4)
        await actor.start()

        embeddings = (
            DocumentEmbedding(chunk_id="c1", vector=(0.1, 0.2, 0.3, 0.4)),
            DocumentEmbedding(chunk_id="c2", vector=(0.5, 0.6, 0.7, 0.8)),
        )

        await actor.ask(GuardarEmbeddingsBatch(
            embeddings=embeddings,
            document_id="doc-1",
        ))

        result = await actor.ask(ObtenerEstadisticasVectorStore())

        assert result.total_vectors == 2
        assert result.document_count == 1
        assert result.memory_usage_bytes > 0

        await actor.stop()

    @pytest.mark.asyncio
    async def test_stats_includes_index_type(self):
        """Should report index type."""
        actor = FAISSVectorStoreActor(index_type="Flat")
        await actor.start()

        result = await actor.ask(ObtenerEstadisticasVectorStore())

        # Will be "Mock" or "Flat" depending on FAISS availability
        assert result.index_type in ("Mock", "Flat")

        await actor.stop()


class TestFAISSVectorStoreActorMultipleDocuments:
    """Tests for handling multiple documents."""

    @pytest.mark.asyncio
    async def test_multiple_documents_tracked(self):
        """Should track multiple documents separately."""
        actor = FAISSVectorStoreActor(dimension=4)
        await actor.start()

        # Add vectors from doc-1
        await actor.ask(GuardarEmbeddingsBatch(
            embeddings=(
                DocumentEmbedding(chunk_id="d1c1", vector=(0.1, 0.2, 0.3, 0.4)),
                DocumentEmbedding(chunk_id="d1c2", vector=(0.5, 0.6, 0.7, 0.8)),
            ),
            document_id="doc-1",
        ))

        # Add vectors from doc-2
        await actor.ask(GuardarEmbeddingsBatch(
            embeddings=(
                DocumentEmbedding(chunk_id="d2c1", vector=(1.0, 1.1, 1.2, 1.3)),
            ),
            document_id="doc-2",
        ))

        stats = await actor.ask(ObtenerEstadisticasVectorStore())

        assert stats.total_vectors == 3
        assert stats.document_count == 2
        assert "doc-1" in actor._document_chunks
        assert "doc-2" in actor._document_chunks
        assert len(actor._document_chunks["doc-1"]) == 2
        assert len(actor._document_chunks["doc-2"]) == 1

        await actor.stop()


class TestFAISSVectorStoreActorCorrelationId:
    """Tests for correlation ID propagation."""

    @pytest.mark.asyncio
    async def test_save_preserves_correlation_id(self):
        """Save result should preserve correlation ID."""
        actor = FAISSVectorStoreActor(dimension=4)
        await actor.start()

        embedding = DocumentEmbedding(
            chunk_id="c1",
            vector=(0.1, 0.2, 0.3, 0.4),
        )

        result = await actor.ask(GuardarEmbedding(
            embedding=embedding,
            correlation_id="test-corr-123",
        ))

        assert result.correlation_id == "test-corr-123"

        await actor.stop()

    @pytest.mark.asyncio
    async def test_search_preserves_correlation_id(self):
        """Search result should preserve correlation ID."""
        actor = FAISSVectorStoreActor(dimension=4)
        await actor.start()

        result = await actor.ask(BuscarSimilares(
            query_vector=(0.1, 0.2, 0.3, 0.4),
            correlation_id="search-corr-456",
        ))

        assert result.correlation_id == "search-corr-456"

        await actor.stop()

    @pytest.mark.asyncio
    async def test_stats_preserves_correlation_id(self):
        """Stats result should preserve correlation ID."""
        actor = FAISSVectorStoreActor(dimension=4)
        await actor.start()

        result = await actor.ask(ObtenerEstadisticasVectorStore(
            correlation_id="stats-corr-789",
        ))

        assert result.correlation_id == "stats-corr-789"

        await actor.stop()
