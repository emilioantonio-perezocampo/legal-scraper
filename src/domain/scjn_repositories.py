"""
SCJN Repository Interfaces (Ports)

Abstract interfaces defining the contracts for persistence.
Following Hexagonal Architecture / Ports and Adapters pattern.
These are the ports that infrastructure adapters must implement.
"""
from abc import ABC, abstractmethod
from typing import Optional, List

from .scjn_entities import SCJNDocument, DocumentEmbedding


class ISCJNDocumentRepository(ABC):
    """
    Port for SCJN document persistence.

    Defines the contract for storing and retrieving SCJN documents.
    Implementations may use JSON files, SQLite, PostgreSQL, etc.
    """

    @abstractmethod
    async def save(self, document: SCJNDocument) -> None:
        """
        Save a document to the repository.

        Args:
            document: The SCJNDocument to persist.
        """
        pass

    @abstractmethod
    async def find_by_id(self, document_id: str) -> Optional[SCJNDocument]:
        """
        Find a document by its ID.

        Args:
            document_id: The unique identifier of the document.

        Returns:
            The document if found, None otherwise.
        """
        pass

    @abstractmethod
    async def find_by_q_param(self, q_param: str) -> Optional[SCJNDocument]:
        """
        Find a document by its SCJN q_param.

        Args:
            q_param: The encrypted URL parameter from SCJN.

        Returns:
            The document if found, None otherwise.
        """
        pass

    @abstractmethod
    async def exists(self, q_param: str) -> bool:
        """
        Check if a document exists by its q_param.

        Args:
            q_param: The encrypted URL parameter from SCJN.

        Returns:
            True if document exists, False otherwise.
        """
        pass


class IEmbeddingRepository(ABC):
    """
    Port for embedding persistence.

    Defines the contract for storing and retrieving vector embeddings.
    Implementations may use FAISS, ChromaDB, PostgreSQL pgvector, etc.
    """

    @abstractmethod
    async def save(self, embedding: DocumentEmbedding) -> None:
        """
        Save a single embedding to the repository.

        Args:
            embedding: The DocumentEmbedding to persist.
        """
        pass

    @abstractmethod
    async def save_batch(self, embeddings: List[DocumentEmbedding]) -> None:
        """
        Save multiple embeddings in a batch.

        Args:
            embeddings: List of DocumentEmbedding objects to persist.
        """
        pass

    @abstractmethod
    async def find_similar(
        self,
        vector: tuple,
        top_k: int = 10
    ) -> List[DocumentEmbedding]:
        """
        Find similar embeddings using vector similarity search.

        Args:
            vector: Query vector to search for similar embeddings.
            top_k: Maximum number of results to return.

        Returns:
            List of similar embeddings, ordered by similarity.
        """
        pass
