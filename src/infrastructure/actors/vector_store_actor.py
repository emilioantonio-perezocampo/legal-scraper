"""
FAISS Vector Store Actor

Manages vector storage and similarity search using FAISS.
Provides in-memory and persistent vector indexing.
"""
import asyncio
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

from .base import BaseActor
from .messages import (
    GuardarEmbedding,
    GuardarEmbeddingsBatch,
    BuscarSimilares,
    ObtenerEstadisticasVectorStore,
    EmbeddingsGuardados,
    ResultadosBusqueda,
    EstadisticasVectorStore,
    ErrorDeActor,
)
from src.domain.scjn_entities import DocumentEmbedding


@dataclass(frozen=True)
class VectorEntry:
    """
    Internal entry for vector storage.

    Maps chunk_id to its index position and metadata.
    """
    chunk_id: str
    index_position: int
    document_id: str


class FAISSVectorStoreActor(BaseActor):
    """
    Actor that manages vector storage using FAISS.

    Provides:
    - Add vectors (single or batch)
    - Similarity search (k-NN)
    - Persistence to disk
    - Statistics and monitoring

    Handles:
    - GuardarEmbedding: Add single embedding
    - GuardarEmbeddingsBatch: Add batch of embeddings
    - BuscarSimilares: Similarity search
    - ObtenerEstadisticasVectorStore: Get statistics
    """

    DEFAULT_DIMENSION = 384  # MiniLM-L6 dimension
    DEFAULT_INDEX_TYPE = "Flat"  # L2 distance, exact search

    def __init__(
        self,
        dimension: int = None,
        index_type: str = None,
        storage_path: Optional[str] = None,
        max_workers: int = 2,
    ):
        """
        Initialize the vector store actor.

        Args:
            dimension: Vector dimension (default: 384 for MiniLM)
            index_type: FAISS index type (Flat, IVF, HNSW)
            storage_path: Path for persistent storage
            max_workers: Thread pool workers for FAISS operations
        """
        super().__init__()
        self._dimension = dimension or self.DEFAULT_DIMENSION
        self._index_type = index_type or self.DEFAULT_INDEX_TYPE
        self._storage_path = Path(storage_path) if storage_path else None
        self._max_workers = max_workers

        # FAISS index and metadata
        self._index = None
        self._faiss_available = False
        self._executor: Optional[ThreadPoolExecutor] = None

        # In-memory metadata mapping
        self._entries: List[VectorEntry] = []
        self._chunk_to_index: Dict[str, int] = {}
        self._document_chunks: Dict[str, Set[str]] = {}

    async def start(self):
        """Start the actor and initialize FAISS index."""
        await super().start()
        self._executor = ThreadPoolExecutor(max_workers=self._max_workers)
        await self._initialize_index()

    async def stop(self):
        """Stop the actor and cleanup."""
        if self._storage_path:
            await self._persist_index()
        if self._executor:
            self._executor.shutdown(wait=False)
        await super().stop()

    async def _initialize_index(self):
        """Initialize the FAISS index."""
        loop = asyncio.get_event_loop()
        try:
            self._index = await loop.run_in_executor(
                self._executor,
                self._create_index,
            )
            self._faiss_available = True
        except Exception:
            # Fallback to mock index if FAISS not available
            self._index = _MockFAISSIndex(self._dimension)
            self._faiss_available = False

    def _create_index(self):
        """Create FAISS index (runs in executor)."""
        try:
            import faiss

            if self._index_type == "Flat":
                return faiss.IndexFlatL2(self._dimension)
            elif self._index_type == "IVF":
                quantizer = faiss.IndexFlatL2(self._dimension)
                return faiss.IndexIVFFlat(quantizer, self._dimension, 100)
            else:
                return faiss.IndexFlatL2(self._dimension)
        except ImportError:
            # FAISS not installed, use mock
            return _MockFAISSIndex(self._dimension)

    async def handle_message(self, message):
        """Handle incoming messages."""
        if isinstance(message, GuardarEmbedding):
            return await self._handle_save_single(message)

        elif isinstance(message, GuardarEmbeddingsBatch):
            return await self._handle_save_batch(message)

        elif isinstance(message, BuscarSimilares):
            return await self._handle_search(message)

        elif isinstance(message, ObtenerEstadisticasVectorStore):
            return await self._handle_get_stats(message)

        return None

    async def _handle_save_single(
        self, message: GuardarEmbedding
    ) -> EmbeddingsGuardados:
        """Save a single embedding."""
        embedding = message.embedding
        if embedding is None:
            return EmbeddingsGuardados(
                correlation_id=message.correlation_id,
                document_id="",
                count=0,
            )

        # Add to index
        await self._add_vectors(
            vectors=[embedding.vector],
            chunk_ids=[embedding.chunk_id],
            document_id="",  # Unknown for single save
        )

        return EmbeddingsGuardados(
            correlation_id=message.correlation_id,
            document_id="",
            count=1,
        )

    async def _handle_save_batch(
        self, message: GuardarEmbeddingsBatch
    ) -> EmbeddingsGuardados:
        """Save batch of embeddings."""
        embeddings = message.embeddings
        document_id = message.document_id

        if not embeddings:
            return EmbeddingsGuardados(
                correlation_id=message.correlation_id,
                document_id=document_id,
                count=0,
            )

        vectors = [e.vector for e in embeddings]
        chunk_ids = [e.chunk_id for e in embeddings]

        await self._add_vectors(
            vectors=vectors,
            chunk_ids=chunk_ids,
            document_id=document_id,
        )

        return EmbeddingsGuardados(
            correlation_id=message.correlation_id,
            document_id=document_id,
            count=len(embeddings),
        )

    async def _add_vectors(
        self,
        vectors: List[tuple],
        chunk_ids: List[str],
        document_id: str,
    ) -> None:
        """Add vectors to the index."""
        if not vectors:
            return

        loop = asyncio.get_event_loop()

        # Convert tuples to numpy array
        import numpy as np
        vectors_array = np.array(vectors, dtype=np.float32)

        # Add to FAISS index
        start_idx = len(self._entries)

        await loop.run_in_executor(
            self._executor,
            lambda: self._index.add(vectors_array),
        )

        # Update metadata
        for i, chunk_id in enumerate(chunk_ids):
            idx = start_idx + i
            entry = VectorEntry(
                chunk_id=chunk_id,
                index_position=idx,
                document_id=document_id,
            )
            self._entries.append(entry)
            self._chunk_to_index[chunk_id] = idx

            # Track document -> chunks mapping
            if document_id:
                if document_id not in self._document_chunks:
                    self._document_chunks[document_id] = set()
                self._document_chunks[document_id].add(chunk_id)

    async def _handle_search(
        self, message: BuscarSimilares
    ) -> ResultadosBusqueda:
        """Search for similar vectors."""
        query_vector = message.query_vector
        top_k = message.top_k
        filter_document_ids = message.filter_document_ids

        if not query_vector:
            return ResultadosBusqueda(
                correlation_id=message.correlation_id,
                results=(),
                query_dimension=0,
                search_time_ms=0.0,
            )

        if len(self._entries) == 0:
            return ResultadosBusqueda(
                correlation_id=message.correlation_id,
                results=(),
                query_dimension=len(query_vector),
                search_time_ms=0.0,
            )

        loop = asyncio.get_event_loop()
        start_time = time.time()

        # Convert query to numpy
        import numpy as np
        query_array = np.array([query_vector], dtype=np.float32)

        # Search in FAISS
        distances, indices = await loop.run_in_executor(
            self._executor,
            lambda: self._index.search(query_array, min(top_k * 2, len(self._entries))),
        )

        search_time = (time.time() - start_time) * 1000  # ms

        # Build results with optional filtering
        results = []
        for dist, idx in zip(distances[0], indices[0]):
            if idx < 0 or idx >= len(self._entries):
                continue

            entry = self._entries[idx]

            # Apply document filter if specified
            if filter_document_ids:
                if entry.document_id not in filter_document_ids:
                    continue

            # Convert L2 distance to similarity score (0-1)
            similarity = 1.0 / (1.0 + float(dist))
            results.append((entry.chunk_id, similarity))

            if len(results) >= top_k:
                break

        return ResultadosBusqueda(
            correlation_id=message.correlation_id,
            results=tuple(results),
            query_dimension=len(query_vector),
            search_time_ms=search_time,
        )

    async def _handle_get_stats(
        self, message: ObtenerEstadisticasVectorStore
    ) -> EstadisticasVectorStore:
        """Get vector store statistics."""
        total_vectors = len(self._entries)
        document_count = len(self._document_chunks)

        # Estimate memory usage (rough approximation)
        memory_bytes = total_vectors * self._dimension * 4  # float32 = 4 bytes

        return EstadisticasVectorStore(
            correlation_id=message.correlation_id,
            total_vectors=total_vectors,
            dimension=self._dimension,
            index_type=self._index_type if self._faiss_available else "Mock",
            memory_usage_bytes=memory_bytes,
            document_count=document_count,
        )

    async def _persist_index(self) -> None:
        """Persist the index to disk."""
        if not self._storage_path or not self._faiss_available:
            return

        loop = asyncio.get_event_loop()

        try:
            import faiss

            self._storage_path.parent.mkdir(parents=True, exist_ok=True)
            await loop.run_in_executor(
                self._executor,
                lambda: faiss.write_index(self._index, str(self._storage_path)),
            )
        except (ImportError, Exception):
            pass

    # Properties for testing/inspection
    @property
    def total_vectors(self) -> int:
        """Get total number of vectors in the store."""
        return len(self._entries)

    @property
    def dimension(self) -> int:
        """Get vector dimension."""
        return self._dimension

    @property
    def is_faiss_available(self) -> bool:
        """Check if real FAISS is being used."""
        return self._faiss_available


class _MockFAISSIndex:
    """
    Mock FAISS index for testing when FAISS isn't available.

    Provides basic exact search using numpy.
    """

    def __init__(self, dimension: int):
        self.dimension = dimension
        self._vectors: List[List[float]] = []

    def add(self, vectors) -> None:
        """Add vectors to the index."""
        import numpy as np
        if hasattr(vectors, 'tolist'):
            vectors = vectors.tolist()
        self._vectors.extend(vectors)

    def search(self, query, k: int):
        """Search for k nearest neighbors.

        Returns:
            Tuple of (distances, indices) matching FAISS API.
        """
        import numpy as np

        if not self._vectors:
            return np.array([[float('inf')] * k]), np.array([[-1] * k])

        query_np = np.array(query)
        vectors_np = np.array(self._vectors)

        # Compute L2 distances
        distances = np.linalg.norm(vectors_np - query_np, axis=1)

        # Get top-k indices
        k = min(k, len(self._vectors))
        indices = np.argsort(distances)[:k]
        top_distances = distances[indices]

        # Pad if needed
        if len(indices) < k:
            pad_size = k - len(indices)
            indices = np.pad(indices, (0, pad_size), constant_values=-1)
            top_distances = np.pad(top_distances, (0, pad_size), constant_values=float('inf'))

        return np.array([top_distances]), np.array([indices])

    @property
    def ntotal(self) -> int:
        """Total number of vectors in index."""
        return len(self._vectors)
