"""
Supabase Document Repository

Repository adapter for persisting scraped legal documents to Supabase.
Implements the repository pattern for document storage and retrieval.

Usage:
    from supabase import create_client
    client = create_client(url, key)
    repo = SupabaseDocumentRepository(client=client)
    await repo.save_document(document, source_type="scjn")
"""
from __future__ import annotations

from dataclasses import asdict
from datetime import date
from typing import Any, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from src.domain.scjn_entities import (
        SCJNDocument,
        SCJNArticle,
        TextChunk,
        DocumentEmbedding,
    )


def _convert_articles_to_json(articles: tuple) -> list[dict[str, Any]]:
    """
    Convert a tuple of SCJNArticle entities to JSON-serializable list.

    Args:
        articles: Tuple of SCJNArticle dataclass instances

    Returns:
        List of dictionaries suitable for JSONB storage
    """
    result = []
    for article in articles:
        article_dict = {
            "number": article.number,
            "title": getattr(article, "title", ""),
            "content": getattr(article, "content", ""),
            "reform_dates": list(getattr(article, "reform_dates", ())),
        }
        result.append(article_dict)
    return result


def _convert_reforms_to_json(reforms: tuple) -> list[dict[str, Any]]:
    """
    Convert a tuple of SCJNReform entities to JSON-serializable list.

    Args:
        reforms: Tuple of SCJNReform dataclass instances

    Returns:
        List of dictionaries suitable for JSONB storage
    """
    result = []
    for reform in reforms:
        reform_dict = {
            "id": reform.id,
            "q_param": reform.q_param,
            "publication_date": (
                reform.publication_date.isoformat()
                if reform.publication_date
                else None
            ),
            "publication_number": reform.publication_number,
            "gazette_section": reform.gazette_section,
        }
        result.append(reform_dict)
    return result


def _prepare_scjn_data(
    document: "SCJNDocument",
) -> tuple[dict[str, Any], dict[str, Any]]:
    """
    Prepare SCJN document data for database insertion.

    Splits the document into parent table data and child table data.

    Args:
        document: SCJNDocument domain entity

    Returns:
        Tuple of (parent_data, scjn_data) dictionaries
    """
    # Parent table data (scraper_documents)
    parent_data = {
        "id": document.id,
        "source_type": "scjn",
        "external_id": document.q_param,
        "title": document.title,
        "publication_date": (
            document.publication_date.isoformat()
            if document.publication_date
            else None
        ),
    }

    # Child table data (scjn_documents)
    scjn_data = {
        "id": document.id,
        "q_param": document.q_param,
        "short_title": document.short_title,
        "category": document.category.value if hasattr(document.category, "value") else str(document.category),
        "scope": document.scope.value if hasattr(document.scope, "value") else str(document.scope),
        "status": document.status.value if hasattr(document.status, "value") else str(document.status),
        "subject_matters": list(document.subject_matters),
        "articles": _convert_articles_to_json(document.articles),
        "reforms": _convert_reforms_to_json(document.reforms),
        "source_url": document.source_url,
        "chunk_count": 0,
        "embedding_status": "pending",
    }

    return parent_data, scjn_data


class SupabaseDocumentRepository:
    """
    Repository for persisting legal documents to Supabase.

    Handles CRUD operations for scraped documents including:
    - SCJN documents (Supreme Court legislation)
    - BJV books (UNAM legal library)
    - CAS awards (sports arbitration)
    - DOF publications (official gazette)

    Attributes:
        client: Supabase client instance
    """

    def __init__(self, client: Any) -> None:
        """
        Initialize repository with Supabase client.

        Args:
            client: Supabase client from create_client()
        """
        self._client = client

    async def save_document(
        self,
        document: Any,
        source_type: str,
        tenant_id: Optional[str] = None,
    ) -> str:
        """
        Save a document to Supabase.

        Inserts into both the parent scraper_documents table and the
        appropriate child table based on source_type.

        Args:
            document: Domain entity (SCJNDocument, LibroBJV, etc.)
            source_type: One of 'scjn', 'bjv', 'cas', 'dof'
            tenant_id: Optional tenant UUID for multi-tenancy

        Returns:
            The document ID

        Raises:
            ValueError: If source_type is not supported
        """
        if source_type == "scjn":
            parent_data, child_data = _prepare_scjn_data(document)
        else:
            raise ValueError(f"Unsupported source_type: {source_type}")

        # Add tenant_id if provided
        if tenant_id:
            parent_data["tenant_id"] = tenant_id

        # Insert into parent table
        self._client.table("scraper_documents").insert(parent_data).execute()

        # Insert into child table
        self._client.table(f"{source_type}_documents").insert(child_data).execute()

        return document.id

    async def exists(
        self,
        external_id: str,
        source_type: str,
    ) -> bool:
        """
        Check if a document already exists.

        Args:
            external_id: The external identifier (q_param, libro_id, etc.)
            source_type: One of 'scjn', 'bjv', 'cas', 'dof'

        Returns:
            True if document exists, False otherwise
        """
        result = (
            self._client.table("scraper_documents")
            .select("id")
            .eq("external_id", external_id)
            .eq("source_type", source_type)
            .execute()
        )

        return len(result.data) > 0

    async def find_by_id(self, document_id: str) -> Optional[dict[str, Any]]:
        """
        Find a document by its UUID.

        Args:
            document_id: The document UUID

        Returns:
            Document data as dictionary, or None if not found
        """
        result = (
            self._client.table("scraper_documents")
            .select("*")
            .eq("id", document_id)
            .execute()
        )

        if result.data:
            return result.data[0]
        return None

    async def find_by_external_id(
        self,
        external_id: str,
        source_type: str,
    ) -> Optional[dict[str, Any]]:
        """
        Find a document by its external identifier.

        Args:
            external_id: The external identifier (q_param, libro_id, etc.)
            source_type: One of 'scjn', 'bjv', 'cas', 'dof'

        Returns:
            Document data as dictionary, or None if not found
        """
        result = (
            self._client.table("scraper_documents")
            .select("*")
            .eq("external_id", external_id)
            .eq("source_type", source_type)
            .execute()
        )

        if result.data:
            return result.data[0]
        return None

    async def save_chunk(
        self,
        chunk: "TextChunk",
        source_type: str,
        embedding: Optional["DocumentEmbedding"] = None,
        tenant_id: Optional[str] = None,
    ) -> str:
        """
        Save a text chunk to Supabase.

        Args:
            chunk: TextChunk domain entity
            source_type: One of 'scjn', 'bjv', 'cas', 'dof'
            embedding: Optional DocumentEmbedding with vector
            tenant_id: Optional tenant UUID for multi-tenancy

        Returns:
            The chunk ID
        """
        chunk_data = {
            "chunk_id": chunk.id,
            "document_id": chunk.document_id,
            "text": chunk.content,
            "chunk_index": chunk.chunk_index,
            "source_type": source_type,
            "metadata": dict(chunk.metadata) if chunk.metadata else {},
        }

        if tenant_id:
            chunk_data["tenant_id"] = tenant_id

        if embedding:
            chunk_data["embedding"] = list(embedding.vector)

        self._client.table("scraper_chunks").insert(chunk_data).execute()

        return chunk.id

    async def save_chunks_batch(
        self,
        chunks: list["TextChunk"],
        source_type: str,
        embeddings: Optional[list["DocumentEmbedding"]] = None,
        tenant_id: Optional[str] = None,
    ) -> list[str]:
        """
        Save multiple chunks in a single batch operation.

        Args:
            chunks: List of TextChunk entities
            source_type: One of 'scjn', 'bjv', 'cas', 'dof'
            embeddings: Optional list of DocumentEmbedding matching chunks
            tenant_id: Optional tenant UUID for multi-tenancy

        Returns:
            List of chunk IDs
        """
        embedding_map = {}
        if embeddings:
            embedding_map = {e.chunk_id: e for e in embeddings}

        batch_data = []
        for chunk in chunks:
            chunk_data = {
                "chunk_id": chunk.id,
                "document_id": chunk.document_id,
                "text": chunk.content,
                "chunk_index": chunk.chunk_index,
                "source_type": source_type,
                "metadata": dict(chunk.metadata) if chunk.metadata else {},
            }

            if tenant_id:
                chunk_data["tenant_id"] = tenant_id

            if chunk.id in embedding_map:
                chunk_data["embedding"] = list(embedding_map[chunk.id].vector)

            batch_data.append(chunk_data)

        self._client.table("scraper_chunks").upsert(batch_data).execute()

        return [c.id for c in chunks]

    async def update_embedding_status(
        self,
        document_id: str,
        source_type: str,
        status: str,
    ) -> None:
        """
        Update the embedding status of a document.

        Args:
            document_id: The document UUID
            source_type: One of 'scjn', 'bjv', 'cas', 'dof'
            status: New status ('pending', 'processing', 'completed', 'failed')
        """
        table_name = self._get_child_table_name(source_type)

        (
            self._client.table(table_name)
            .update({"embedding_status": status})
            .eq("id", document_id)
            .execute()
        )

    async def update_chunk_count(
        self,
        document_id: str,
        source_type: str,
        count: int,
    ) -> None:
        """
        Update the chunk count for a document.

        Args:
            document_id: The document UUID
            source_type: One of 'scjn', 'bjv', 'cas', 'dof'
            count: Number of chunks
        """
        table_name = self._get_child_table_name(source_type)

        (
            self._client.table(table_name)
            .update({"chunk_count": count})
            .eq("id", document_id)
            .execute()
        )

    async def delete(self, document_id: str) -> None:
        """
        Delete a document and its associated data.

        Child table records and chunks are deleted via CASCADE.

        Args:
            document_id: The document UUID to delete
        """
        (
            self._client.table("scraper_documents")
            .delete()
            .eq("id", document_id)
            .execute()
        )

    async def list_by_source_type(
        self,
        source_type: str,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        """
        List documents by source type with pagination.

        Args:
            source_type: One of 'scjn', 'bjv', 'cas', 'dof'
            limit: Maximum number of results
            offset: Number of results to skip

        Returns:
            List of document dictionaries
        """
        result = (
            self._client.table("scraper_documents")
            .select("*")
            .eq("source_type", source_type)
            .range(offset, offset + limit - 1)
            .execute()
        )

        return result.data

    def _get_child_table_name(self, source_type: str) -> str:
        """
        Get the child table name for a source type.

        Args:
            source_type: One of 'scjn', 'bjv', 'cas', 'dof'

        Returns:
            Table name string
        """
        table_map = {
            "scjn": "scjn_documents",
            "bjv": "bjv_libros",
            "cas": "cas_laudos",
            "dof": "dof_publicaciones",
        }

        if source_type not in table_map:
            raise ValueError(f"Unknown source_type: {source_type}")

        return table_map[source_type]
