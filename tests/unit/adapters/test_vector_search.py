"""
RED Phase Tests: VectorSearchAdapter with pgvectorscale

Tests for semantic search using pgvectorscale's StreamingDiskANN index.
These tests must FAIL initially (RED phase).

The adapter should:
- Execute vector similarity searches against scraper_chunks table
- Support label-based filtering for source type (SCJN, BJV, CAS, DOF)
- Set query tuning parameters (diskann.query_search_list_size)
- Return results with similarity scores between 0 and 1
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch, call
from typing import List

from src.infrastructure.adapters.vector_search import VectorSearchAdapter


@pytest.fixture
def mock_pool():
    """Create a mock asyncpg connection pool."""
    pool = MagicMock()
    conn = AsyncMock()

    # Create a proper async context manager for acquire()
    async_cm = AsyncMock()
    async_cm.__aenter__.return_value = conn
    async_cm.__aexit__.return_value = None

    pool.acquire.return_value = async_cm

    return pool, conn


@pytest.fixture
def sample_embedding():
    """Create a sample 4000-dimension embedding vector."""
    return [0.1] * 4000


@pytest.fixture
def sample_search_results():
    """Create sample search results from database."""
    return [
        {
            "chunk_id": "chunk-001",
            "document_id": "doc-123",
            "text": "Artículo 1.- La presente Ley es de observancia general.",
            "source_type": "scjn",
            "similarity": 0.95,
        },
        {
            "chunk_id": "chunk-002",
            "document_id": "doc-124",
            "text": "Artículo 2.- Esta ley tiene por objeto regular...",
            "source_type": "scjn",
            "similarity": 0.87,
        },
        {
            "chunk_id": "chunk-003",
            "document_id": "doc-200",
            "text": "Capítulo I. Disposiciones generales del derecho laboral.",
            "source_type": "bjv",
            "similarity": 0.82,
        },
    ]


class TestVectorSearchAdapterBasicSearch:
    """Tests for basic semantic search without filtering."""

    @pytest.mark.asyncio
    async def test_search_returns_similar_chunks(
        self, mock_pool, sample_embedding, sample_search_results
    ):
        """Given query embedding, returns chunks ordered by similarity."""
        pool, conn = mock_pool
        conn.fetch.return_value = sample_search_results

        adapter = VectorSearchAdapter(pool=pool)
        results = await adapter.search(query_embedding=sample_embedding, limit=10)

        assert len(results) == 3
        assert results[0]["chunk_id"] == "chunk-001"
        assert results[0]["similarity"] == 0.95

    @pytest.mark.asyncio
    async def test_search_respects_limit(
        self, mock_pool, sample_embedding, sample_search_results
    ):
        """Given limit=2, returns at most 2 results."""
        pool, conn = mock_pool
        conn.fetch.return_value = sample_search_results[:2]

        adapter = VectorSearchAdapter(pool=pool)
        results = await adapter.search(query_embedding=sample_embedding, limit=2)

        assert len(results) == 2

    @pytest.mark.asyncio
    async def test_search_returns_empty_when_no_matches(self, mock_pool, sample_embedding):
        """Given no matching chunks, returns empty list."""
        pool, conn = mock_pool
        conn.fetch.return_value = []

        adapter = VectorSearchAdapter(pool=pool)
        results = await adapter.search(query_embedding=sample_embedding)

        assert results == []

    @pytest.mark.asyncio
    async def test_search_uses_cosine_distance_operator(
        self, mock_pool, sample_embedding
    ):
        """Query should use <=> operator for cosine distance."""
        pool, conn = mock_pool
        conn.fetch.return_value = []

        adapter = VectorSearchAdapter(pool=pool)
        await adapter.search(query_embedding=sample_embedding)

        # Verify the SQL query uses cosine distance operator
        fetch_call = conn.fetch.call_args
        sql_query = fetch_call[0][0]
        assert "<=>" in sql_query


class TestVectorSearchAdapterFilteredSearch:
    """Tests for label-filtered vector search."""

    @pytest.mark.asyncio
    async def test_filtered_search_by_single_source_type(
        self, mock_pool, sample_embedding, sample_search_results
    ):
        """Given source_types=['scjn'], only returns SCJN chunks."""
        pool, conn = mock_pool
        scjn_results = [r for r in sample_search_results if r["source_type"] == "scjn"]
        conn.fetch.return_value = scjn_results

        adapter = VectorSearchAdapter(pool=pool)
        results = await adapter.search(
            query_embedding=sample_embedding,
            source_types=["scjn"],
        )

        assert len(results) == 2
        assert all(r["source_type"] == "scjn" for r in results)

    @pytest.mark.asyncio
    async def test_filtered_search_by_multiple_sources(
        self, mock_pool, sample_embedding, sample_search_results
    ):
        """Given source_types=['scjn', 'bjv'], returns chunks from both."""
        pool, conn = mock_pool
        conn.fetch.return_value = sample_search_results

        adapter = VectorSearchAdapter(pool=pool)
        results = await adapter.search(
            query_embedding=sample_embedding,
            source_types=["scjn", "bjv"],
        )

        assert len(results) == 3

    @pytest.mark.asyncio
    async def test_filtered_search_uses_array_overlap_operator(
        self, mock_pool, sample_embedding
    ):
        """Query should use && operator for label array overlap."""
        pool, conn = mock_pool
        conn.fetch.return_value = []

        adapter = VectorSearchAdapter(pool=pool)
        await adapter.search(
            query_embedding=sample_embedding,
            source_types=["scjn"],
        )

        # Verify the SQL query uses array overlap operator
        fetch_call = conn.fetch.call_args
        sql_query = fetch_call[0][0]
        assert "&&" in sql_query or "source_labels" in sql_query

    @pytest.mark.asyncio
    async def test_source_type_to_label_mapping(self, mock_pool, sample_embedding):
        """Source types should map to correct smallint labels."""
        pool, conn = mock_pool
        conn.fetch.return_value = []

        adapter = VectorSearchAdapter(pool=pool)

        # Verify label mapping
        assert adapter.SOURCE_LABELS == {
            "scjn": 1,
            "bjv": 2,
            "cas": 3,
            "dof": 4,
        }


class TestVectorSearchAdapterQueryTuning:
    """Tests for pgvectorscale query tuning parameters."""

    @pytest.mark.asyncio
    async def test_sets_search_list_size_before_query(
        self, mock_pool, sample_embedding
    ):
        """diskann.query_search_list_size should be set before the search query."""
        pool, conn = mock_pool
        conn.fetch.return_value = []

        adapter = VectorSearchAdapter(pool=pool, search_list_size=150)
        await adapter.search(query_embedding=sample_embedding)

        # Verify SET command was executed
        conn.execute.assert_called()
        set_call = conn.execute.call_args
        assert "diskann.query_search_list_size" in set_call[0][0]
        assert "150" in set_call[0][0]

    @pytest.mark.asyncio
    async def test_default_search_list_size_is_100(self, mock_pool, sample_embedding):
        """Default search_list_size should be 100."""
        pool, conn = mock_pool
        conn.fetch.return_value = []

        adapter = VectorSearchAdapter(pool=pool)
        await adapter.search(query_embedding=sample_embedding)

        set_call = conn.execute.call_args
        assert "100" in set_call[0][0]

    @pytest.mark.asyncio
    async def test_custom_search_list_size(self, mock_pool, sample_embedding):
        """Custom search_list_size should be used."""
        pool, conn = mock_pool
        conn.fetch.return_value = []

        adapter = VectorSearchAdapter(pool=pool, search_list_size=200)
        assert adapter._search_list_size == 200


class TestVectorSearchAdapterSimilarityScores:
    """Tests for similarity score calculation."""

    @pytest.mark.asyncio
    async def test_similarity_scores_in_valid_range(
        self, mock_pool, sample_embedding, sample_search_results
    ):
        """Similarity scores should be between 0 and 1."""
        pool, conn = mock_pool
        conn.fetch.return_value = sample_search_results

        adapter = VectorSearchAdapter(pool=pool)
        results = await adapter.search(query_embedding=sample_embedding)

        for result in results:
            assert 0 <= result["similarity"] <= 1

    @pytest.mark.asyncio
    async def test_similarity_calculated_from_cosine_distance(
        self, mock_pool, sample_embedding
    ):
        """Similarity should be 1 - cosine_distance."""
        pool, conn = mock_pool
        conn.fetch.return_value = []

        adapter = VectorSearchAdapter(pool=pool)
        await adapter.search(query_embedding=sample_embedding)

        # Verify query calculates similarity as 1 - distance
        fetch_call = conn.fetch.call_args
        sql_query = fetch_call[0][0]
        assert "1 -" in sql_query or "1-" in sql_query


class TestVectorSearchAdapterResultFormat:
    """Tests for result format and structure."""

    @pytest.mark.asyncio
    async def test_returns_list_of_dicts(
        self, mock_pool, sample_embedding, sample_search_results
    ):
        """Results should be a list of dictionaries."""
        pool, conn = mock_pool
        conn.fetch.return_value = sample_search_results

        adapter = VectorSearchAdapter(pool=pool)
        results = await adapter.search(query_embedding=sample_embedding)

        assert isinstance(results, list)
        assert all(isinstance(r, dict) for r in results)

    @pytest.mark.asyncio
    async def test_result_contains_required_fields(
        self, mock_pool, sample_embedding, sample_search_results
    ):
        """Each result should contain chunk_id, document_id, text, similarity."""
        pool, conn = mock_pool
        conn.fetch.return_value = sample_search_results

        adapter = VectorSearchAdapter(pool=pool)
        results = await adapter.search(query_embedding=sample_embedding)

        for result in results:
            assert "chunk_id" in result
            assert "document_id" in result
            assert "text" in result
            assert "similarity" in result


class TestVectorSearchAdapterConnectionHandling:
    """Tests for connection pool usage."""

    @pytest.mark.asyncio
    async def test_acquires_and_releases_connection(
        self, mock_pool, sample_embedding
    ):
        """Connection should be properly acquired and released."""
        pool, conn = mock_pool
        conn.fetch.return_value = []

        adapter = VectorSearchAdapter(pool=pool)
        await adapter.search(query_embedding=sample_embedding)

        # Verify acquire was called
        pool.acquire.assert_called_once()


class TestVectorSearchAdapterEdgeCases:
    """Tests for edge cases and error handling."""

    @pytest.mark.asyncio
    async def test_handles_empty_embedding(self, mock_pool):
        """Should handle empty embedding gracefully."""
        pool, conn = mock_pool
        conn.fetch.return_value = []

        adapter = VectorSearchAdapter(pool=pool)
        results = await adapter.search(query_embedding=[], limit=10)

        assert results == []

    @pytest.mark.asyncio
    async def test_handles_unknown_source_type(self, mock_pool, sample_embedding):
        """Should raise error for unknown source type."""
        pool, conn = mock_pool

        adapter = VectorSearchAdapter(pool=pool)

        with pytest.raises(KeyError):
            await adapter.search(
                query_embedding=sample_embedding,
                source_types=["unknown_source"],
            )

    @pytest.mark.asyncio
    async def test_default_limit_is_10(self, mock_pool, sample_embedding):
        """Default limit should be 10."""
        pool, conn = mock_pool
        conn.fetch.return_value = []

        adapter = VectorSearchAdapter(pool=pool)
        await adapter.search(query_embedding=sample_embedding)

        # Check limit in query args
        fetch_call = conn.fetch.call_args
        # The limit should be passed as one of the parameters
        assert 10 in fetch_call[0] or "10" in str(fetch_call)
