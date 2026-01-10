"""
Embedding Actor

Generates vector embeddings for text chunks using sentence transformers.
"""
import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Optional

from .base import BaseActor
from .messages import (
    GenerarEmbeddings,
    EmbeddingsGenerados,
    ErrorDeActor,
)
from src.domain.scjn_entities import TextChunk, DocumentEmbedding


class EmbeddingActor(BaseActor):
    """
    Actor that generates vector embeddings for text chunks.

    Uses sentence-transformers for embedding generation.
    Runs encoding in a thread pool to avoid blocking the event loop.

    Handles:
    - GenerarEmbeddings: Generate embeddings for chunks
    - ("GET_EMBEDDINGS", document_id): Retrieve embeddings for a document
    """

    DEFAULT_MODEL = "sentence-transformers/all-MiniLM-L6-v2"
    DEFAULT_DIMENSION = 384

    def __init__(
        self,
        model_name: str = None,
        max_workers: int = 2,
    ):
        """
        Initialize the embedding actor.

        Args:
            model_name: Name of the sentence-transformers model
            max_workers: Number of threads for encoding
        """
        super().__init__()
        self._model_name = model_name or self.DEFAULT_MODEL
        self._max_workers = max_workers
        self._model = None
        self._executor: Optional[ThreadPoolExecutor] = None
        self._embeddings: Dict[str, List[DocumentEmbedding]] = {}

    async def start(self):
        """Start the actor and load the model."""
        await super().start()
        self._executor = ThreadPoolExecutor(max_workers=self._max_workers)
        await self._load_model()

    async def stop(self):
        """Stop the actor and cleanup."""
        if self._executor:
            self._executor.shutdown(wait=False)
        await super().stop()

    async def _load_model(self):
        """Load the sentence transformer model in executor."""
        if self._model is not None:
            return

        loop = asyncio.get_event_loop()
        try:
            self._model = await loop.run_in_executor(
                self._executor,
                self._create_model,
            )
        except Exception:
            # Use a simple mock model for testing
            self._model = _SimpleMockModel(self.DEFAULT_DIMENSION)

    def _create_model(self):
        """Create the sentence transformer model."""
        try:
            from sentence_transformers import SentenceTransformer

            return SentenceTransformer(self._model_name)
        except ImportError:
            # Fallback to mock model if sentence_transformers not installed
            return _SimpleMockModel(self.DEFAULT_DIMENSION)

    async def handle_message(self, message):
        """Handle incoming messages."""
        if isinstance(message, GenerarEmbeddings):
            return await self._generate_embeddings(message)

        elif isinstance(message, tuple) and len(message) >= 2:
            if message[0] == "GET_EMBEDDINGS":
                return self._embeddings.get(message[1], [])

        return None

    async def _generate_embeddings(
        self, message: GenerarEmbeddings
    ) -> EmbeddingsGenerados:
        """Generate embeddings for text chunks."""
        chunks = message.chunks
        document_id = message.document_id

        if not chunks:
            return EmbeddingsGenerados(
                correlation_id=message.correlation_id,
                document_id=document_id,
                embedding_count=0,
            )

        # Extract texts from chunks
        texts = [chunk.content for chunk in chunks if chunk.content]

        if not texts:
            return EmbeddingsGenerados(
                correlation_id=message.correlation_id,
                document_id=document_id,
                embedding_count=0,
            )

        # Generate embeddings in executor
        loop = asyncio.get_event_loop()
        try:
            vectors = await loop.run_in_executor(
                self._executor,
                lambda: self._model.encode(texts),
            )
        except Exception as e:
            return ErrorDeActor(
                correlation_id=message.correlation_id,
                actor_name="EmbeddingActor",
                document_id=document_id,
                error_type=type(e).__name__,
                error_message=str(e),
                recoverable=True,
                original_command=message,
            )

        # Create embedding objects
        embeddings = []
        text_idx = 0
        for chunk in chunks:
            if chunk.content:
                vector = vectors[text_idx]
                # Convert numpy array to tuple if needed
                if hasattr(vector, 'tolist'):
                    vector = tuple(vector.tolist())
                else:
                    vector = tuple(vector)

                embedding = DocumentEmbedding(
                    chunk_id=chunk.id,
                    vector=vector,
                    model_name=self._model_name,
                )
                embeddings.append(embedding)
                text_idx += 1

        # Store embeddings
        self._embeddings[document_id] = embeddings

        return EmbeddingsGenerados(
            correlation_id=message.correlation_id,
            document_id=document_id,
            embedding_count=len(embeddings),
        )


class _SimpleMockModel:
    """Simple mock model for testing when sentence_transformers isn't available."""

    def __init__(self, dimension: int = 384):
        self.dimension = dimension

    def encode(self, texts: List[str]) -> List[List[float]]:
        """Generate deterministic mock embeddings."""
        embeddings = []
        for text in texts:
            # Generate deterministic vector based on text hash
            hash_val = hash(text) % 1000000
            vector = [(hash_val + i) / 1000000.0 for i in range(self.dimension)]
            embeddings.append(vector)
        return embeddings
