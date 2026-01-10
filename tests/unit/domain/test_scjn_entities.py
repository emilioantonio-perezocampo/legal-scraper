"""
RED Phase Tests: SCJN Domain Entities

Tests for SCJN-specific domain entities following DDD principles.
These tests must FAIL initially (RED phase).
"""
import pytest
from datetime import date, datetime
from dataclasses import FrozenInstanceError

# These imports will fail until implementation exists
from src.domain.scjn_entities import (
    SCJNDocument,
    SCJNReform,
    SCJNArticle,
    TextChunk,
    DocumentEmbedding,
    ScrapingCheckpoint,
)
from src.domain.scjn_value_objects import (
    DocumentCategory,
    DocumentScope,
    DocumentStatus,
    SubjectMatter,
)


class TestSCJNArticle:
    """Tests for SCJNArticle value object."""

    def test_create_article_with_required_fields(self):
        """Article should be creatable with number only."""
        article = SCJNArticle(number="1")
        assert article.number == "1"

    def test_create_article_with_all_fields(self):
        """Article should accept all fields."""
        article = SCJNArticle(
            number="42 Bis",
            title="Del derecho al trabajo",
            content="Toda persona tiene derecho al trabajo digno...",
            reform_dates=("2021-03-01", "2019-05-15"),
        )
        assert article.number == "42 Bis"
        assert article.title == "Del derecho al trabajo"
        assert "trabajo digno" in article.content
        assert len(article.reform_dates) == 2

    def test_article_is_immutable(self):
        """Article should be frozen/immutable."""
        article = SCJNArticle(number="1", content="Original")
        with pytest.raises(FrozenInstanceError):
            article.content = "Modified"

    def test_article_default_values(self):
        """Article should have sensible defaults."""
        article = SCJNArticle(number="1")
        assert article.title == ""
        assert article.content == ""
        assert article.reform_dates == ()

    def test_article_with_transitory_number(self):
        """Article should handle transitory articles."""
        article = SCJNArticle(number="Transitorio Primero")
        assert article.number == "Transitorio Primero"

    def test_article_with_unicode_content(self):
        """Article should handle Spanish unicode characters."""
        article = SCJNArticle(
            number="1",
            content="La Constitución garantiza los derechos de niños y niñas",
        )
        assert "niños" in article.content
        assert "niñas" in article.content


class TestSCJNReform:
    """Tests for SCJNReform entity."""

    def test_create_reform_with_defaults(self):
        """Reform should be creatable with defaults."""
        reform = SCJNReform()
        assert reform.id is not None
        assert len(reform.id) > 0

    def test_create_reform_with_all_fields(self):
        """Reform should accept all fields."""
        reform = SCJNReform(
            q_param="abc123encrypted",
            publication_date=date(2021, 3, 1),
            publication_number="274/2021",
            gazette_section="Primera Sección",
            text_content="Contenido del decreto...",
            pdf_path="/data/reforms/274_2021.pdf",
        )
        assert reform.q_param == "abc123encrypted"
        assert reform.publication_date == date(2021, 3, 1)
        assert reform.publication_number == "274/2021"
        assert reform.gazette_section == "Primera Sección"

    def test_reform_is_immutable(self):
        """Reform should be frozen/immutable."""
        reform = SCJNReform(publication_number="1/2021")
        with pytest.raises(FrozenInstanceError):
            reform.publication_number = "2/2021"

    def test_reform_generates_unique_ids(self):
        """Each reform should get a unique ID."""
        reform1 = SCJNReform()
        reform2 = SCJNReform()
        assert reform1.id != reform2.id

    def test_reform_default_values(self):
        """Reform should have sensible defaults."""
        reform = SCJNReform()
        assert reform.q_param == ""
        assert reform.publication_date is None
        assert reform.publication_number == ""
        assert reform.gazette_section == ""
        assert reform.text_content is None
        assert reform.pdf_path is None


class TestSCJNDocument:
    """Tests for SCJNDocument aggregate root."""

    def test_create_document_with_defaults(self):
        """Document should be creatable with defaults."""
        doc = SCJNDocument()
        assert doc.id is not None
        assert doc.category == DocumentCategory.LEY
        assert doc.scope == DocumentScope.FEDERAL
        assert doc.status == DocumentStatus.VIGENTE

    def test_create_document_with_all_fields(self):
        """Document should accept all fields."""
        article = SCJNArticle(number="1", content="Artículo primero...")
        reform = SCJNReform(publication_date=date(2021, 3, 1))

        doc = SCJNDocument(
            q_param="xyz789encrypted",
            title="Ley Federal del Trabajo",
            short_title="LFT",
            category=DocumentCategory.LEY_FEDERAL,
            scope=DocumentScope.FEDERAL,
            status=DocumentStatus.VIGENTE,
            publication_date=date(1970, 4, 1),
            expedition_date=date(1970, 3, 15),
            state=None,
            subject_matters=(SubjectMatter.LABORAL,),
            articles=(article,),
            reforms=(reform,),
            source_url="https://legislacion.scjn.gob.mx/...",
        )

        assert doc.title == "Ley Federal del Trabajo"
        assert doc.short_title == "LFT"
        assert doc.category == DocumentCategory.LEY_FEDERAL
        assert len(doc.articles) == 1
        assert len(doc.reforms) == 1

    def test_document_is_immutable(self):
        """Document should be frozen/immutable."""
        doc = SCJNDocument(title="Original Title")
        with pytest.raises(FrozenInstanceError):
            doc.title = "New Title"

    def test_document_articles_is_tuple(self):
        """Document articles must be a tuple (immutable)."""
        doc = SCJNDocument()
        assert isinstance(doc.articles, tuple)

    def test_document_reforms_is_tuple(self):
        """Document reforms must be a tuple (immutable)."""
        doc = SCJNDocument()
        assert isinstance(doc.reforms, tuple)

    def test_document_subject_matters_is_tuple(self):
        """Document subject_matters must be a tuple (immutable)."""
        doc = SCJNDocument()
        assert isinstance(doc.subject_matters, tuple)

    def test_document_article_count_property(self):
        """Document should calculate article count."""
        articles = (
            SCJNArticle(number="1"),
            SCJNArticle(number="2"),
            SCJNArticle(number="3"),
        )
        doc = SCJNDocument(articles=articles)
        assert doc.article_count == 3

    def test_document_reform_count_property(self):
        """Document should calculate reform count."""
        reforms = (
            SCJNReform(publication_number="1/2021"),
            SCJNReform(publication_number="2/2021"),
        )
        doc = SCJNDocument(reforms=reforms)
        assert doc.reform_count == 2

    def test_document_empty_counts(self):
        """Document with no articles/reforms should have zero counts."""
        doc = SCJNDocument()
        assert doc.article_count == 0
        assert doc.reform_count == 0

    def test_document_with_state_level_scope(self):
        """Document should handle state-level documents."""
        doc = SCJNDocument(
            title="Constitución Política del Estado de Jalisco",
            scope=DocumentScope.ESTATAL,
            state="Jalisco",
        )
        assert doc.scope == DocumentScope.ESTATAL
        assert doc.state == "Jalisco"

    def test_document_with_international_scope(self):
        """Document should handle international treaties."""
        doc = SCJNDocument(
            title="Tratado de Libre Comercio de América del Norte",
            category=DocumentCategory.TRATADO,
            scope=DocumentScope.INTERNACIONAL,
        )
        assert doc.category == DocumentCategory.TRATADO
        assert doc.scope == DocumentScope.INTERNACIONAL

    def test_document_with_multiple_subjects(self):
        """Document can have multiple subject matters."""
        doc = SCJNDocument(
            subject_matters=(
                SubjectMatter.CIVIL,
                SubjectMatter.MERCANTIL,
                SubjectMatter.PROCESAL,
            )
        )
        assert len(doc.subject_matters) == 3
        assert SubjectMatter.CIVIL in doc.subject_matters


class TestTextChunk:
    """Tests for TextChunk entity."""

    def test_create_chunk_with_defaults(self):
        """Chunk should be creatable with defaults."""
        chunk = TextChunk()
        assert chunk.id is not None
        assert chunk.document_id == ""
        assert chunk.content == ""
        assert chunk.token_count == 0
        assert chunk.chunk_index == 0

    def test_create_chunk_with_all_fields(self):
        """Chunk should accept all fields."""
        chunk = TextChunk(
            document_id="doc-123",
            content="Este es el contenido del chunk...",
            token_count=128,
            chunk_index=5,
            metadata=(("article", "42"), ("section", "Primera")),
        )
        assert chunk.document_id == "doc-123"
        assert chunk.token_count == 128
        assert chunk.chunk_index == 5

    def test_chunk_is_immutable(self):
        """Chunk should be frozen/immutable."""
        chunk = TextChunk(content="Original")
        with pytest.raises(FrozenInstanceError):
            chunk.content = "Modified"

    def test_chunk_metadata_is_tuple(self):
        """Chunk metadata must be a tuple (immutable)."""
        chunk = TextChunk()
        assert isinstance(chunk.metadata, tuple)

    def test_chunk_metadata_with_key_value_pairs(self):
        """Chunk metadata should store key-value pairs as tuples."""
        chunk = TextChunk(
            metadata=(("key1", "value1"), ("key2", "value2"))
        )
        assert len(chunk.metadata) == 2
        assert chunk.metadata[0] == ("key1", "value1")

    def test_chunk_generates_unique_ids(self):
        """Each chunk should get a unique ID."""
        chunk1 = TextChunk()
        chunk2 = TextChunk()
        assert chunk1.id != chunk2.id


class TestDocumentEmbedding:
    """Tests for DocumentEmbedding entity."""

    def test_create_embedding_with_required_fields(self):
        """Embedding requires chunk_id and vector."""
        embedding = DocumentEmbedding(
            chunk_id="chunk-123",
            vector=(0.1, 0.2, 0.3, 0.4),
        )
        assert embedding.chunk_id == "chunk-123"
        assert len(embedding.vector) == 4

    def test_embedding_default_model(self):
        """Embedding should have default model name."""
        embedding = DocumentEmbedding(
            chunk_id="chunk-123",
            vector=(0.1,),
        )
        assert "sentence-transformers" in embedding.model_name

    def test_embedding_custom_model(self):
        """Embedding should accept custom model name."""
        embedding = DocumentEmbedding(
            chunk_id="chunk-123",
            vector=(0.1,),
            model_name="custom-model/v1",
        )
        assert embedding.model_name == "custom-model/v1"

    def test_embedding_is_immutable(self):
        """Embedding should be frozen/immutable."""
        embedding = DocumentEmbedding(
            chunk_id="chunk-123",
            vector=(0.1,),
        )
        with pytest.raises(FrozenInstanceError):
            embedding.chunk_id = "different-id"

    def test_embedding_vector_is_tuple(self):
        """Embedding vector must be a tuple (immutable)."""
        embedding = DocumentEmbedding(
            chunk_id="chunk-123",
            vector=(0.1, 0.2),
        )
        assert isinstance(embedding.vector, tuple)

    def test_embedding_preserves_vector_precision(self):
        """Embedding should preserve float precision."""
        vector = (0.12345678901234567, -0.98765432109876543)
        embedding = DocumentEmbedding(
            chunk_id="chunk-123",
            vector=vector,
        )
        assert embedding.vector[0] == pytest.approx(0.12345678901234567)
        assert embedding.vector[1] == pytest.approx(-0.98765432109876543)


class TestScrapingCheckpoint:
    """Tests for ScrapingCheckpoint entity."""

    def test_create_checkpoint_with_required_fields(self):
        """Checkpoint requires session_id, last_processed_q_param, processed_count."""
        checkpoint = ScrapingCheckpoint(
            session_id="session-abc",
            last_processed_q_param="xyz789",
            processed_count=42,
        )
        assert checkpoint.session_id == "session-abc"
        assert checkpoint.last_processed_q_param == "xyz789"
        assert checkpoint.processed_count == 42

    def test_checkpoint_default_values(self):
        """Checkpoint should have sensible defaults."""
        checkpoint = ScrapingCheckpoint(
            session_id="session-abc",
            last_processed_q_param="xyz",
            processed_count=0,
        )
        assert checkpoint.failed_q_params == ()
        assert checkpoint.created_at is not None

    def test_checkpoint_with_failed_params(self):
        """Checkpoint should track failed q_params."""
        checkpoint = ScrapingCheckpoint(
            session_id="session-abc",
            last_processed_q_param="xyz",
            processed_count=10,
            failed_q_params=("failed1", "failed2", "failed3"),
        )
        assert len(checkpoint.failed_q_params) == 3
        assert "failed1" in checkpoint.failed_q_params

    def test_checkpoint_is_immutable(self):
        """Checkpoint should be frozen/immutable."""
        checkpoint = ScrapingCheckpoint(
            session_id="session-abc",
            last_processed_q_param="xyz",
            processed_count=0,
        )
        with pytest.raises(FrozenInstanceError):
            checkpoint.processed_count = 100

    def test_checkpoint_failed_params_is_tuple(self):
        """Checkpoint failed_q_params must be a tuple (immutable)."""
        checkpoint = ScrapingCheckpoint(
            session_id="session-abc",
            last_processed_q_param="xyz",
            processed_count=0,
        )
        assert isinstance(checkpoint.failed_q_params, tuple)

    def test_checkpoint_created_at_is_datetime(self):
        """Checkpoint created_at should be a datetime."""
        checkpoint = ScrapingCheckpoint(
            session_id="session-abc",
            last_processed_q_param="xyz",
            processed_count=0,
        )
        assert isinstance(checkpoint.created_at, datetime)
