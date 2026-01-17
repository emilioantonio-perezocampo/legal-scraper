"""
Vector Search Adapter using pgvectorscale StreamingDiskANN.

Provides high-performance semantic search for document chunks using
Timescale's pgvectorscale extension with StreamingDiskANN indexes.

Features:
- 28x lower p95 latency compared to Pinecone (storage-optimized)
- Native label-based filtering for source type (SCJN, BJV, CAS, DOF)
- Configurable query tuning parameters

Usage:
    import asyncpg
    pool = await asyncpg.create_pool(db_url)
    adapter = VectorSearchAdapter(pool=pool, search_list_size=100)

    # Basic search
    results = await adapter.search(query_embedding, limit=10)

    # Filtered search (only SCJN documents)
    results = await adapter.search(query_embedding, source_types=["scjn"])
"""
from __future__ import annotations

from typing import Any, Optional

try:
    import asyncpg
except ImportError:
    asyncpg = None  # type: ignore


class VectorSearchAdapter:
    """
    Semantic search adapter using pgvectorscale StreamingDiskANN index.

    This adapter executes vector similarity searches against the scraper_chunks
    table using pgvectorscale's disk-based ANN algorithm for efficient search
    at scale.

    Attributes:
        SOURCE_LABELS: Mapping of source type names to smallint label IDs
                       used for native label-filtered search.
    """

    # Label mapping for source types (used in filtered search)
    # Labels must be smallint for pgvectorscale's filtered DiskANN
    SOURCE_LABELS: dict[str, int] = {
        "scjn": 1,  # Supreme Court legislation
        "bjv": 2,   # UNAM Legal Library books
        "cas": 3,   # Court of Arbitration for Sport
        "dof": 4,   # Official Gazette (Diario Oficial)
    }

    def __init__(
        self,
        pool: "asyncpg.Pool",
        search_list_size: int = 100,
    ) -> None:
        """
        Initialize the vector search adapter.

        Args:
            pool: asyncpg connection pool for database access
            search_list_size: pgvectorscale query tuning parameter.
                             Higher values increase accuracy but reduce speed.
                             Default: 100, Range: 10-1000
        """
        self._pool = pool
        self._search_list_size = search_list_size

    async def search(
        self,
        query_embedding: list[float],
        limit: int = 10,
        source_types: Optional[list[str]] = None,
    ) -> list[dict[str, Any]]:
        """
        Search for similar document chunks using vector similarity.

        Uses pgvectorscale's StreamingDiskANN index for efficient approximate
        nearest neighbor search with optional source type filtering.

        Args:
            query_embedding: Query vector (4000 dimensions for this project)
            limit: Maximum number of results to return (default: 10)
            source_types: Optional list of source types to filter by.
                         Valid values: 'scjn', 'bjv', 'cas', 'dof'
                         If None, searches across all sources.

        Returns:
            List of dictionaries containing:
            - chunk_id: Unique identifier for the chunk
            - document_id: UUID of the parent document
            - text: The chunk text content
            - source_type: Source type ('scjn', 'bjv', 'cas', 'dof')
            - similarity: Cosine similarity score (0 to 1, higher is better)

        Raises:
            KeyError: If an unknown source type is provided

        Example:
            >>> results = await adapter.search(embedding, limit=5)
            >>> for r in results:
            ...     print(f"{r['similarity']:.2f}: {r['text'][:50]}...")
        """
        # Handle empty embedding edge case
        if not query_embedding:
            return []

        async with self._pool.acquire() as conn:
            # Set pgvectorscale query tuning parameters
            await conn.execute(
                f"SET diskann.query_search_list_size = {self._search_list_size}"
            )

            if source_types:
                # Convert source types to label IDs for filtered search
                labels = [self.SOURCE_LABELS[s] for s in source_types]
                results = await conn.fetch(
                    """
                    SELECT chunk_id, document_id, text, source_type,
                           1 - (embedding <=> $1::vector) as similarity
                    FROM scraper_chunks
                    WHERE source_labels && $2::smallint[]
                    ORDER BY embedding <=> $1::vector
                    LIMIT $3
                    """,
                    query_embedding,
                    labels,
                    limit,
                )
            else:
                # Search across all sources
                results = await conn.fetch(
                    """
                    SELECT chunk_id, document_id, text, source_type,
                           1 - (embedding <=> $1::vector) as similarity
                    FROM scraper_chunks
                    ORDER BY embedding <=> $1::vector
                    LIMIT $2
                    """,
                    query_embedding,
                    limit,
                )

            return [dict(r) for r in results]

    async def search_hybrid(
        self,
        query_embedding: list[float],
        keyword: Optional[str] = None,
        limit: int = 10,
        source_types: Optional[list[str]] = None,
    ) -> list[dict[str, Any]]:
        """
        Hybrid search combining vector similarity with keyword matching.

        Performs vector search first, then filters/reranks based on keyword
        presence in the text content.

        Args:
            query_embedding: Query vector
            keyword: Optional keyword to boost relevance
            limit: Maximum results to return
            source_types: Optional source type filter

        Returns:
            List of matching chunks with similarity scores

        Note:
            This is a simple hybrid implementation. For production use,
            consider using PostgreSQL's full-text search with ts_rank
            for better keyword scoring.
        """
        # Get more results for post-filtering
        fetch_limit = limit * 3 if keyword else limit

        results = await self.search(
            query_embedding=query_embedding,
            limit=fetch_limit,
            source_types=source_types,
        )

        if keyword and results:
            # Boost results containing the keyword
            keyword_lower = keyword.lower()
            for result in results:
                if keyword_lower in result.get("text", "").lower():
                    result["similarity"] = min(1.0, result["similarity"] * 1.1)

            # Re-sort by boosted similarity
            results.sort(key=lambda x: x["similarity"], reverse=True)

        return results[:limit]
