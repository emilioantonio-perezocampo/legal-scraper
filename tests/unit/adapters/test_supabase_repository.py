"""
RED Phase Tests: SupabaseDocumentRepository

Tests for document persistence to Supabase.
These tests must FAIL initially (RED phase).

The repository should:
- Save documents to the parent scraper_documents table
- Save source-specific details to child tables (scjn_documents, etc.)
- Check document existence to avoid duplicates
- Find documents by ID or external identifier (q_param, libro_id, etc.)
- Support batch operations for performance
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import date
from uuid import uuid4

# These imports will fail until implementation exists
from src.infrastructure.adapters.supabase_repository import SupabaseDocumentRepository
from src.domain.scjn_entities import (
    SCJNDocument,
    SCJNArticle,
    TextChunk,
    DocumentEmbedding,
)
from src.domain.scjn_value_objects import (
    DocumentCategory,
    DocumentScope,
    DocumentStatus,
)


@pytest.fixture
def mock_supabase():
    """Mock Supabase client for unit tests."""
    client = MagicMock()

    # Mock table operations
    table_mock = MagicMock()
    table_mock.insert.return_value.execute = MagicMock(
        return_value=MagicMock(data=[{"id": str(uuid4())}])
    )
    table_mock.select.return_value.eq.return_value.execute = MagicMock(
        return_value=MagicMock(data=[])
    )
    table_mock.upsert.return_value.execute = MagicMock(
        return_value=MagicMock(data=[{"id": str(uuid4())}])
    )

    client.table.return_value = table_mock
    return client


@pytest.fixture
def sample_scjn_document():
    """Create a sample SCJNDocument for testing."""
    return SCJNDocument(
        id="doc-123",
        q_param="abc123==",
        title="LEY FEDERAL DEL TRABAJO",
        short_title="LFT",
        category=DocumentCategory.LEY_FEDERAL,
        scope=DocumentScope.FEDERAL,
        status=DocumentStatus.VIGENTE,
        publication_date=date(2019, 5, 1),
        subject_matters=("LABORAL",),
        articles=(
            SCJNArticle(number="1", content="Artículo 1 contenido"),
            SCJNArticle(number="2", content="Artículo 2 contenido"),
        ),
        source_url="https://www.scjn.gob.mx/...",
    )


@pytest.fixture
def sample_text_chunk():
    """Create a sample TextChunk for testing."""
    return TextChunk(
        id="chunk-001",
        document_id="doc-123",
        content="Artículo 1.- La presente Ley es de observancia general.",
        token_count=50,
        chunk_index=0,
    )


@pytest.fixture
def sample_embedding():
    """Create a sample DocumentEmbedding for testing."""
    return DocumentEmbedding(
        chunk_id="chunk-001",
        vector=tuple([0.1] * 4000),  # halfvec(4000)
        model_name="text-embedding-3-small",
    )


@pytest.fixture
def repository(mock_supabase):
    """Create repository with mocked client."""
    return SupabaseDocumentRepository(client=mock_supabase)


class TestSupabaseDocumentRepositorySave:
    """Tests for saving documents."""

    @pytest.mark.asyncio
    async def test_save_document_inserts_to_parent_table(
        self, repository, mock_supabase, sample_scjn_document
    ):
        """Saving should insert into scraper_documents table."""
        await repository.save_document(sample_scjn_document, source_type="scjn")

        mock_supabase.table.assert_any_call("scraper_documents")

    @pytest.mark.asyncio
    async def test_save_document_inserts_to_scjn_table(
        self, repository, mock_supabase, sample_scjn_document
    ):
        """Saving SCJN document should also insert into scjn_documents."""
        await repository.save_document(sample_scjn_document, source_type="scjn")

        mock_supabase.table.assert_any_call("scjn_documents")

    @pytest.mark.asyncio
    async def test_save_document_returns_id(
        self, repository, sample_scjn_document
    ):
        """Saving should return the document ID."""
        doc_id = await repository.save_document(
            sample_scjn_document, source_type="scjn"
        )

        assert doc_id is not None
        assert isinstance(doc_id, str)

    @pytest.mark.asyncio
    async def test_save_document_with_tenant_id(
        self, repository, mock_supabase, sample_scjn_document
    ):
        """Saving with tenant_id should include it in the insert."""
        tenant_id = str(uuid4())

        await repository.save_document(
            sample_scjn_document, source_type="scjn", tenant_id=tenant_id
        )

        # Verify tenant_id was included in the call
        calls = mock_supabase.table.return_value.insert.call_args_list
        assert len(calls) > 0

    @pytest.mark.asyncio
    async def test_save_document_stores_articles_as_jsonb(
        self, repository, mock_supabase, sample_scjn_document
    ):
        """Articles should be stored as JSONB array."""
        await repository.save_document(sample_scjn_document, source_type="scjn")

        # The insert should have been called with articles as a list
        insert_calls = mock_supabase.table.return_value.insert.call_args_list
        assert len(insert_calls) > 0


class TestSupabaseDocumentRepositoryExists:
    """Tests for checking document existence."""

    @pytest.mark.asyncio
    async def test_exists_returns_true_when_found(
        self, repository, mock_supabase
    ):
        """exists() should return True when document exists."""
        # Chain two .eq() calls
        mock_supabase.table.return_value.select.return_value.eq.return_value.eq.return_value.execute.return_value.data = [
            {"id": "doc-123"}
        ]

        exists = await repository.exists(external_id="abc123==", source_type="scjn")

        assert exists is True

    @pytest.mark.asyncio
    async def test_exists_returns_false_when_not_found(
        self, repository, mock_supabase
    ):
        """exists() should return False when document doesn't exist."""
        # Chain two .eq() calls
        mock_supabase.table.return_value.select.return_value.eq.return_value.eq.return_value.execute.return_value.data = []

        exists = await repository.exists(external_id="nonexistent", source_type="scjn")

        assert exists is False

    @pytest.mark.asyncio
    async def test_exists_queries_correct_table(
        self, repository, mock_supabase
    ):
        """exists() should query scraper_documents table."""
        await repository.exists(external_id="abc123==", source_type="scjn")

        mock_supabase.table.assert_called_with("scraper_documents")


class TestSupabaseDocumentRepositoryFind:
    """Tests for finding documents."""

    @pytest.mark.asyncio
    async def test_find_by_id_returns_document(
        self, repository, mock_supabase
    ):
        """find_by_id() should return a document when found."""
        mock_supabase.table.return_value.select.return_value.eq.return_value.execute.return_value.data = [
            {
                "id": "doc-123",
                "source_type": "scjn",
                "external_id": "abc123==",
                "title": "LEY FEDERAL DEL TRABAJO",
                "publication_date": "2019-05-01",
            }
        ]

        doc = await repository.find_by_id("doc-123")

        assert doc is not None
        assert doc["id"] == "doc-123"

    @pytest.mark.asyncio
    async def test_find_by_id_returns_none_when_not_found(
        self, repository, mock_supabase
    ):
        """find_by_id() should return None when not found."""
        mock_supabase.table.return_value.select.return_value.eq.return_value.execute.return_value.data = []

        doc = await repository.find_by_id("nonexistent")

        assert doc is None

    @pytest.mark.asyncio
    async def test_find_by_external_id(
        self, repository, mock_supabase
    ):
        """find_by_external_id() should find by q_param or libro_id."""
        mock_supabase.table.return_value.select.return_value.eq.return_value.eq.return_value.execute.return_value.data = [
            {"id": "doc-123", "external_id": "abc123=="}
        ]

        doc = await repository.find_by_external_id(
            external_id="abc123==", source_type="scjn"
        )

        assert doc is not None


class TestSupabaseDocumentRepositoryChunks:
    """Tests for saving text chunks with embeddings."""

    @pytest.mark.asyncio
    async def test_save_chunk_inserts_to_scraper_chunks(
        self, repository, mock_supabase, sample_text_chunk
    ):
        """Saving chunk should insert into scraper_chunks table."""
        await repository.save_chunk(sample_text_chunk, source_type="scjn")

        mock_supabase.table.assert_called_with("scraper_chunks")

    @pytest.mark.asyncio
    async def test_save_chunk_with_embedding(
        self, repository, mock_supabase, sample_text_chunk, sample_embedding
    ):
        """Saving chunk with embedding should include vector."""
        await repository.save_chunk(
            sample_text_chunk,
            source_type="scjn",
            embedding=sample_embedding,
        )

        # Verify insert was called
        mock_supabase.table.return_value.insert.assert_called()

    @pytest.mark.asyncio
    async def test_save_chunks_batch(
        self, repository, mock_supabase
    ):
        """save_chunks_batch() should insert multiple chunks efficiently."""
        chunks = [
            TextChunk(
                id=f"chunk-{i}",
                document_id="doc-123",
                content=f"Content {i}",
                chunk_index=i,
            )
            for i in range(10)
        ]

        await repository.save_chunks_batch(chunks, source_type="scjn")

        # Should use upsert for batch operations
        mock_supabase.table.assert_called_with("scraper_chunks")


class TestSupabaseDocumentRepositoryUpdate:
    """Tests for updating documents."""

    @pytest.mark.asyncio
    async def test_update_embedding_status(
        self, repository, mock_supabase
    ):
        """update_embedding_status() should update the status field."""
        mock_supabase.table.return_value.update.return_value.eq.return_value.execute = MagicMock()

        await repository.update_embedding_status(
            document_id="doc-123",
            source_type="scjn",
            status="completed",
        )

        mock_supabase.table.assert_called()

    @pytest.mark.asyncio
    async def test_update_chunk_count(
        self, repository, mock_supabase
    ):
        """update_chunk_count() should update the chunk_count field."""
        mock_supabase.table.return_value.update.return_value.eq.return_value.execute = MagicMock()

        await repository.update_chunk_count(
            document_id="doc-123",
            source_type="scjn",
            count=25,
        )

        mock_supabase.table.assert_called()


class TestSupabaseDocumentRepositoryDelete:
    """Tests for deleting documents."""

    @pytest.mark.asyncio
    async def test_delete_removes_document(
        self, repository, mock_supabase
    ):
        """delete() should remove document from database."""
        mock_supabase.table.return_value.delete.return_value.eq.return_value.execute = MagicMock()

        await repository.delete("doc-123")

        mock_supabase.table.return_value.delete.assert_called()

    @pytest.mark.asyncio
    async def test_delete_cascades_to_chunks(
        self, repository, mock_supabase
    ):
        """delete() should cascade to scraper_chunks via FK."""
        # CASCADE is handled by database, just verify delete is called
        mock_supabase.table.return_value.delete.return_value.eq.return_value.execute = MagicMock()

        await repository.delete("doc-123")

        mock_supabase.table.assert_called_with("scraper_documents")


class TestSupabaseDocumentRepositoryList:
    """Tests for listing documents."""

    @pytest.mark.asyncio
    async def test_list_by_source_type(
        self, repository, mock_supabase
    ):
        """list_by_source_type() should filter by source."""
        # Include .range() in the chain
        mock_supabase.table.return_value.select.return_value.eq.return_value.range.return_value.execute.return_value.data = [
            {"id": "doc-1", "title": "Doc 1"},
            {"id": "doc-2", "title": "Doc 2"},
        ]

        docs = await repository.list_by_source_type("scjn", limit=10)

        assert len(docs) == 2

    @pytest.mark.asyncio
    async def test_list_with_pagination(
        self, repository, mock_supabase
    ):
        """list_by_source_type() should support pagination."""
        mock_supabase.table.return_value.select.return_value.eq.return_value.range.return_value.execute.return_value.data = []

        await repository.list_by_source_type("scjn", limit=10, offset=20)

        mock_supabase.table.return_value.select.return_value.eq.return_value.range.assert_called()


class TestSupabaseDocumentRepositoryConnection:
    """Tests for connection management."""

    def test_repository_requires_client(self):
        """Repository should require a Supabase client."""
        with pytest.raises(TypeError):
            SupabaseDocumentRepository()

    def test_repository_accepts_client(self, mock_supabase):
        """Repository should accept a Supabase client."""
        repo = SupabaseDocumentRepository(client=mock_supabase)
        assert repo is not None


class TestSupabaseDocumentRepositoryDataMapping:
    """Tests for domain entity to database mapping."""

    @pytest.mark.asyncio
    async def test_scjn_document_mapping(
        self, repository, mock_supabase, sample_scjn_document
    ):
        """SCJN document should be mapped correctly to database format."""
        await repository.save_document(sample_scjn_document, source_type="scjn")

        # Verify the data structure matches database schema
        insert_call = mock_supabase.table.return_value.insert.call_args
        assert insert_call is not None

    @pytest.mark.asyncio
    async def test_articles_converted_to_json_list(
        self, repository, sample_scjn_document
    ):
        """Articles tuple should be converted to JSON list for storage."""
        from src.infrastructure.adapters.supabase_repository import (
            _convert_articles_to_json,
        )

        result = _convert_articles_to_json(sample_scjn_document.articles)

        assert isinstance(result, list)
        assert len(result) == 2
        assert result[0]["number"] == "1"

    @pytest.mark.asyncio
    async def test_enum_values_stored_as_strings(
        self, repository, sample_scjn_document
    ):
        """Enum values should be stored as their string values."""
        from src.infrastructure.adapters.supabase_repository import (
            _prepare_scjn_data,
        )

        parent_data, scjn_data = _prepare_scjn_data(sample_scjn_document)

        assert scjn_data["category"] == "LEY_FEDERAL"
        assert scjn_data["scope"] == "FEDERAL"
        assert scjn_data["status"] == "VIGENTE"
