"""
RED Phase Tests: SCJNPersistenceActor

Tests for document and embedding persistence actor.
These tests must FAIL initially (RED phase).
"""
import pytest
import asyncio
from pathlib import Path

# These imports will fail until implementation exists
from src.infrastructure.actors.persistence_actor import SCJNPersistenceActor
from src.infrastructure.actors.messages import (
    GuardarDocumento,
    GuardarEmbedding,
    DocumentoGuardado,
)
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
def temp_storage_dir(tmp_path):
    """Create a temporary directory for storage."""
    return tmp_path / "storage"


@pytest.fixture
def sample_document():
    """Create a sample SCJNDocument for testing."""
    return SCJNDocument(
        id="doc-123",
        q_param="abc123==",
        title="LEY FEDERAL DEL TRABAJO",
        short_title="LFT",
        category=DocumentCategory.LEY_FEDERAL,
        scope=DocumentScope.FEDERAL,
        status=DocumentStatus.VIGENTE,
        articles=(
            SCJNArticle(number="1", content="Artículo 1 contenido"),
            SCJNArticle(number="2", content="Artículo 2 contenido"),
        ),
    )


@pytest.fixture
def another_document():
    """Create another sample document."""
    return SCJNDocument(
        id="doc-456",
        q_param="xyz789==",
        title="CÓDIGO CIVIL FEDERAL",
        short_title="CCF",
        category=DocumentCategory.CODIGO,
        scope=DocumentScope.FEDERAL,
        status=DocumentStatus.VIGENTE,
    )


@pytest.fixture
def sample_embedding():
    """Create a sample DocumentEmbedding for testing."""
    return DocumentEmbedding(
        chunk_id="chunk-001",
        vector=tuple([0.1, 0.2, 0.3, 0.4] * 96),  # 384 dimensions
        model_name="sentence-transformers/all-MiniLM-L6-v2",
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


class TestPersistenceActorLifecycle:
    """Tests for SCJNPersistenceActor lifecycle."""

    @pytest.mark.asyncio
    async def test_start_creates_directories(self, temp_storage_dir):
        """Start should create storage directories."""
        actor = SCJNPersistenceActor(storage_dir=str(temp_storage_dir))
        await actor.start()

        assert temp_storage_dir.exists()
        assert (temp_storage_dir / "documents").exists()
        assert (temp_storage_dir / "embeddings").exists()

        await actor.stop()

    @pytest.mark.asyncio
    async def test_stop_is_clean(self, temp_storage_dir):
        """Stop should cleanly terminate the actor."""
        actor = SCJNPersistenceActor(storage_dir=str(temp_storage_dir))
        await actor.start()
        await actor.stop()

        assert not actor._running


class TestPersistenceActorDocumentSave:
    """Tests for saving documents."""

    @pytest.mark.asyncio
    async def test_save_document_creates_file(self, temp_storage_dir, sample_document):
        """Saving document should create a file."""
        actor = SCJNPersistenceActor(storage_dir=str(temp_storage_dir))
        await actor.start()

        await actor.tell(GuardarDocumento(document=sample_document))
        await asyncio.sleep(0.1)

        doc_file = temp_storage_dir / "documents" / f"{sample_document.id}.json"
        assert doc_file.exists()

        await actor.stop()

    @pytest.mark.asyncio
    async def test_save_document_returns_event(self, temp_storage_dir, sample_document):
        """Saving document should return DocumentoGuardado event."""
        actor = SCJNPersistenceActor(storage_dir=str(temp_storage_dir))
        await actor.start()

        result = await actor.ask(GuardarDocumento(document=sample_document))

        assert isinstance(result, DocumentoGuardado)
        assert result.document_id == sample_document.id

        await actor.stop()

    @pytest.mark.asyncio
    async def test_save_document_preserves_data(self, temp_storage_dir, sample_document):
        """Saved document should preserve all data."""
        actor = SCJNPersistenceActor(storage_dir=str(temp_storage_dir))
        await actor.start()

        await actor.ask(GuardarDocumento(document=sample_document))

        # Load it back
        result = await actor.ask(("LOAD_DOCUMENT", sample_document.id))

        assert result is not None
        assert result.id == sample_document.id
        assert result.title == sample_document.title
        assert result.q_param == sample_document.q_param

        await actor.stop()


class TestPersistenceActorDocumentLoad:
    """Tests for loading documents."""

    @pytest.mark.asyncio
    async def test_load_document_by_id(self, temp_storage_dir, sample_document):
        """Should load document by ID."""
        actor = SCJNPersistenceActor(storage_dir=str(temp_storage_dir))
        await actor.start()

        await actor.ask(GuardarDocumento(document=sample_document))
        result = await actor.ask(("LOAD_DOCUMENT", sample_document.id))

        assert result is not None
        assert result.id == sample_document.id

        await actor.stop()

    @pytest.mark.asyncio
    async def test_load_document_by_q_param(self, temp_storage_dir, sample_document):
        """Should load document by q_param."""
        actor = SCJNPersistenceActor(storage_dir=str(temp_storage_dir))
        await actor.start()

        await actor.ask(GuardarDocumento(document=sample_document))
        result = await actor.ask(("FIND_BY_Q_PARAM", sample_document.q_param))

        assert result is not None
        assert result.q_param == sample_document.q_param

        await actor.stop()

    @pytest.mark.asyncio
    async def test_load_nonexistent_returns_none(self, temp_storage_dir):
        """Loading non-existent document should return None."""
        actor = SCJNPersistenceActor(storage_dir=str(temp_storage_dir))
        await actor.start()

        result = await actor.ask(("LOAD_DOCUMENT", "nonexistent-id"))
        assert result is None

        await actor.stop()

    @pytest.mark.asyncio
    async def test_exists_check(self, temp_storage_dir, sample_document):
        """Should check if document exists by q_param."""
        actor = SCJNPersistenceActor(storage_dir=str(temp_storage_dir))
        await actor.start()

        # Before saving
        exists_before = await actor.ask(("EXISTS", sample_document.q_param))
        assert exists_before is False

        # After saving
        await actor.ask(GuardarDocumento(document=sample_document))
        exists_after = await actor.ask(("EXISTS", sample_document.q_param))
        assert exists_after is True

        await actor.stop()


class TestPersistenceActorDocumentList:
    """Tests for listing documents."""

    @pytest.mark.asyncio
    async def test_list_documents_empty(self, temp_storage_dir):
        """Listing when empty should return empty tuple."""
        actor = SCJNPersistenceActor(storage_dir=str(temp_storage_dir))
        await actor.start()

        result = await actor.ask(("LIST_DOCUMENTS",))
        assert result == ()

        await actor.stop()

    @pytest.mark.asyncio
    async def test_list_documents_multiple(
        self, temp_storage_dir, sample_document, another_document
    ):
        """Listing should return all document IDs."""
        actor = SCJNPersistenceActor(storage_dir=str(temp_storage_dir))
        await actor.start()

        await actor.ask(GuardarDocumento(document=sample_document))
        await actor.ask(GuardarDocumento(document=another_document))

        result = await actor.ask(("LIST_DOCUMENTS",))

        assert sample_document.id in result
        assert another_document.id in result

        await actor.stop()


class TestPersistenceActorEmbeddingSave:
    """Tests for saving embeddings."""

    @pytest.mark.asyncio
    async def test_save_embedding(self, temp_storage_dir, sample_embedding):
        """Saving embedding should store it."""
        actor = SCJNPersistenceActor(storage_dir=str(temp_storage_dir))
        await actor.start()

        result = await actor.ask(GuardarEmbedding(embedding=sample_embedding))
        assert result is not None

        await actor.stop()

    @pytest.mark.asyncio
    async def test_save_multiple_embeddings(self, temp_storage_dir):
        """Should save multiple embeddings."""
        actor = SCJNPersistenceActor(storage_dir=str(temp_storage_dir))
        await actor.start()

        embeddings = [
            DocumentEmbedding(
                chunk_id=f"chunk-{i}",
                vector=tuple([float(i)] * 384),
            )
            for i in range(5)
        ]

        for emb in embeddings:
            await actor.ask(GuardarEmbedding(embedding=emb))

        # Verify count
        count = await actor.ask(("EMBEDDING_COUNT",))
        assert count == 5

        await actor.stop()


class TestPersistenceActorPersistence:
    """Tests for data persistence across restarts."""

    @pytest.mark.asyncio
    async def test_document_survives_restart(self, temp_storage_dir, sample_document):
        """Document should survive actor restart."""
        # First instance - save
        actor1 = SCJNPersistenceActor(storage_dir=str(temp_storage_dir))
        await actor1.start()
        await actor1.ask(GuardarDocumento(document=sample_document))
        await actor1.stop()

        # Second instance - load
        actor2 = SCJNPersistenceActor(storage_dir=str(temp_storage_dir))
        await actor2.start()
        result = await actor2.ask(("LOAD_DOCUMENT", sample_document.id))
        await actor2.stop()

        assert result is not None
        assert result.id == sample_document.id


class TestPersistenceActorErrorHandling:
    """Tests for error handling."""

    @pytest.mark.asyncio
    async def test_handles_corrupted_document_file(self, temp_storage_dir):
        """Should handle corrupted document files gracefully."""
        (temp_storage_dir / "documents").mkdir(parents=True)
        corrupted = temp_storage_dir / "documents" / "corrupted.json"
        corrupted.write_text("invalid json {{{")

        actor = SCJNPersistenceActor(storage_dir=str(temp_storage_dir))
        await actor.start()

        # Should not crash
        result = await actor.ask(("LOAD_DOCUMENT", "corrupted"))
        assert result is None

        await actor.stop()
