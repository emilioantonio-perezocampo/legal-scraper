"""
RED Phase Tests: Actor Messages

Tests for typed actor message definitions.
These tests must FAIL initially (RED phase).
"""
import pytest
from dataclasses import FrozenInstanceError
from datetime import datetime

# These imports will fail until implementation exists
from src.infrastructure.actors.messages import (
    # Base
    ActorMessage,
    # Commands
    DescubrirDocumentos,
    DescargarDocumento,
    ProcesarPDF,
    GenerarEmbeddings,
    GuardarDocumento,
    GuardarEmbedding,
    GuardarCheckpoint,
    CargarCheckpoint,
    PausarPipeline,
    ReanudarPipeline,
    ObtenerEstado,
    # Events
    DocumentoDescubierto,
    DocumentoDescargado,
    PDFProcesado,
    EmbeddingsGenerados,
    DocumentoGuardado,
    CheckpointGuardado,
    # Errors
    ErrorDeActor,
)


class TestActorMessageBase:
    """Tests for ActorMessage base class."""

    def test_base_message_has_correlation_id(self):
        """Base message should auto-generate correlation_id."""
        msg = ActorMessage()
        assert msg.correlation_id is not None
        assert len(msg.correlation_id) > 0

    def test_base_message_has_timestamp(self):
        """Base message should auto-generate timestamp."""
        msg = ActorMessage()
        assert msg.timestamp is not None
        assert isinstance(msg.timestamp, datetime)

    def test_base_message_unique_correlation_ids(self):
        """Each message should have unique correlation_id."""
        msg1 = ActorMessage()
        msg2 = ActorMessage()
        assert msg1.correlation_id != msg2.correlation_id

    def test_base_message_immutable(self):
        """Base message should be frozen."""
        msg = ActorMessage()
        with pytest.raises(FrozenInstanceError):
            msg.correlation_id = "changed"


class TestCommandMessages:
    """Tests for command messages."""

    def test_descubrir_documentos_defaults(self):
        """DescubrirDocumentos should have sensible defaults."""
        cmd = DescubrirDocumentos()
        assert cmd.category is None
        assert cmd.scope is None
        assert cmd.status is None
        assert cmd.max_results == 100

    def test_descubrir_documentos_custom_values(self):
        """DescubrirDocumentos should accept custom values."""
        cmd = DescubrirDocumentos(
            category="LEY",
            scope="FEDERAL",
            status="VIGENTE",
            max_results=50,
        )
        assert cmd.category == "LEY"
        assert cmd.scope == "FEDERAL"
        assert cmd.status == "VIGENTE"
        assert cmd.max_results == 50

    def test_descubrir_documentos_immutable(self):
        """DescubrirDocumentos should be frozen."""
        cmd = DescubrirDocumentos()
        with pytest.raises(FrozenInstanceError):
            cmd.max_results = 200

    def test_descargar_documento_defaults(self):
        """DescargarDocumento should have defaults."""
        cmd = DescargarDocumento()
        assert cmd.q_param == ""
        assert cmd.include_pdf is True
        assert cmd.include_reforms is True

    def test_descargar_documento_with_q_param(self):
        """DescargarDocumento should accept q_param."""
        cmd = DescargarDocumento(q_param="abc123==")
        assert cmd.q_param == "abc123=="

    def test_procesar_pdf_defaults(self):
        """ProcesarPDF should have defaults."""
        cmd = ProcesarPDF()
        assert cmd.document_id == ""
        assert cmd.pdf_bytes == b""
        assert cmd.source_url == ""

    def test_procesar_pdf_with_bytes(self):
        """ProcesarPDF should accept bytes."""
        cmd = ProcesarPDF(
            document_id="doc-123",
            pdf_bytes=b"%PDF-1.4",
            source_url="https://example.com/doc.pdf",
        )
        assert cmd.document_id == "doc-123"
        assert cmd.pdf_bytes == b"%PDF-1.4"

    def test_generar_embeddings_defaults(self):
        """GenerarEmbeddings should have defaults."""
        cmd = GenerarEmbeddings()
        assert cmd.document_id == ""
        assert cmd.chunks == ()

    def test_generar_embeddings_with_chunks(self):
        """GenerarEmbeddings should accept tuple of chunks."""
        chunks = ("chunk1", "chunk2", "chunk3")
        cmd = GenerarEmbeddings(document_id="doc-123", chunks=chunks)
        assert cmd.chunks == chunks
        assert len(cmd.chunks) == 3

    def test_guardar_documento_defaults(self):
        """GuardarDocumento should have defaults."""
        cmd = GuardarDocumento()
        assert cmd.document is None

    def test_guardar_embedding_defaults(self):
        """GuardarEmbedding should have defaults."""
        cmd = GuardarEmbedding()
        assert cmd.embedding is None

    def test_guardar_checkpoint_defaults(self):
        """GuardarCheckpoint should have defaults."""
        cmd = GuardarCheckpoint()
        assert cmd.checkpoint is None

    def test_cargar_checkpoint_defaults(self):
        """CargarCheckpoint should have defaults."""
        cmd = CargarCheckpoint()
        assert cmd.session_id == ""

    def test_cargar_checkpoint_with_session_id(self):
        """CargarCheckpoint should accept session_id."""
        cmd = CargarCheckpoint(session_id="session-abc-123")
        assert cmd.session_id == "session-abc-123"

    def test_pausar_pipeline_has_correlation_id(self):
        """PausarPipeline should have correlation_id."""
        cmd = PausarPipeline()
        assert cmd.correlation_id is not None

    def test_reanudar_pipeline_has_correlation_id(self):
        """ReanudarPipeline should have correlation_id."""
        cmd = ReanudarPipeline()
        assert cmd.correlation_id is not None

    def test_obtener_estado_has_correlation_id(self):
        """ObtenerEstado should have correlation_id."""
        cmd = ObtenerEstado()
        assert cmd.correlation_id is not None


class TestEventMessages:
    """Tests for event messages."""

    def test_documento_descubierto_defaults(self):
        """DocumentoDescubierto should have defaults."""
        event = DocumentoDescubierto()
        assert event.q_param == ""
        assert event.title == ""
        assert event.category == ""

    def test_documento_descubierto_with_values(self):
        """DocumentoDescubierto should accept values."""
        event = DocumentoDescubierto(
            q_param="abc123",
            title="LEY FEDERAL DEL TRABAJO",
            category="LEY FEDERAL",
        )
        assert event.q_param == "abc123"
        assert event.title == "LEY FEDERAL DEL TRABAJO"
        assert event.category == "LEY FEDERAL"

    def test_documento_descubierto_immutable(self):
        """DocumentoDescubierto should be frozen."""
        event = DocumentoDescubierto()
        with pytest.raises(FrozenInstanceError):
            event.title = "Changed"

    def test_documento_descargado_defaults(self):
        """DocumentoDescargado should have defaults."""
        event = DocumentoDescargado()
        assert event.document_id == ""
        assert event.q_param == ""
        assert event.has_pdf is False
        assert event.pdf_size_bytes == 0

    def test_documento_descargado_with_pdf(self):
        """DocumentoDescargado should track PDF info."""
        event = DocumentoDescargado(
            document_id="doc-123",
            q_param="abc123",
            has_pdf=True,
            pdf_size_bytes=1024000,
        )
        assert event.has_pdf is True
        assert event.pdf_size_bytes == 1024000

    def test_pdf_procesado_defaults(self):
        """PDFProcesado should have defaults."""
        event = PDFProcesado()
        assert event.document_id == ""
        assert event.chunk_count == 0
        assert event.total_tokens == 0
        assert event.extraction_confidence == 0.0

    def test_pdf_procesado_with_metrics(self):
        """PDFProcesado should track extraction metrics."""
        event = PDFProcesado(
            document_id="doc-123",
            chunk_count=15,
            total_tokens=5000,
            extraction_confidence=0.95,
        )
        assert event.chunk_count == 15
        assert event.total_tokens == 5000
        assert event.extraction_confidence == 0.95

    def test_embeddings_generados_defaults(self):
        """EmbeddingsGenerados should have defaults."""
        event = EmbeddingsGenerados()
        assert event.document_id == ""
        assert event.embedding_count == 0

    def test_embeddings_generados_with_count(self):
        """EmbeddingsGenerados should track count."""
        event = EmbeddingsGenerados(
            document_id="doc-123",
            embedding_count=15,
        )
        assert event.embedding_count == 15

    def test_documento_guardado_defaults(self):
        """DocumentoGuardado should have defaults."""
        event = DocumentoGuardado()
        assert event.document_id == ""

    def test_checkpoint_guardado_defaults(self):
        """CheckpointGuardado should have defaults."""
        event = CheckpointGuardado()
        assert event.session_id == ""
        assert event.processed_count == 0

    def test_checkpoint_guardado_with_progress(self):
        """CheckpointGuardado should track progress."""
        event = CheckpointGuardado(
            session_id="session-123",
            processed_count=50,
        )
        assert event.session_id == "session-123"
        assert event.processed_count == 50


class TestErrorMessages:
    """Tests for error messages."""

    def test_error_de_actor_defaults(self):
        """ErrorDeActor should have defaults."""
        error = ErrorDeActor()
        assert error.actor_name == ""
        assert error.document_id is None
        assert error.error_type == ""
        assert error.error_message == ""
        assert error.recoverable is True
        assert error.original_command is None

    def test_error_de_actor_with_details(self):
        """ErrorDeActor should accept error details."""
        original = DescargarDocumento(q_param="abc123")
        error = ErrorDeActor(
            actor_name="SCJNScraperActor",
            document_id="doc-123",
            error_type="HTTPError",
            error_message="Connection timeout",
            recoverable=True,
            original_command=original,
        )
        assert error.actor_name == "SCJNScraperActor"
        assert error.document_id == "doc-123"
        assert error.error_type == "HTTPError"
        assert error.error_message == "Connection timeout"
        assert error.recoverable is True
        assert error.original_command == original

    def test_error_de_actor_non_recoverable(self):
        """ErrorDeActor should support non-recoverable errors."""
        error = ErrorDeActor(
            actor_name="PDFProcessorActor",
            error_type="PDFCorruptedError",
            error_message="Invalid PDF structure",
            recoverable=False,
        )
        assert error.recoverable is False

    def test_error_de_actor_immutable(self):
        """ErrorDeActor should be frozen."""
        error = ErrorDeActor()
        with pytest.raises(FrozenInstanceError):
            error.recoverable = False


class TestMessageCorrelation:
    """Tests for message correlation."""

    def test_command_preserves_correlation_id(self):
        """Commands should preserve correlation_id when explicitly set."""
        cmd = DescubrirDocumentos(correlation_id="custom-123")
        assert cmd.correlation_id == "custom-123"

    def test_event_preserves_correlation_id(self):
        """Events should preserve correlation_id when explicitly set."""
        event = DocumentoDescubierto(correlation_id="custom-456")
        assert event.correlation_id == "custom-456"

    def test_error_preserves_correlation_id(self):
        """Errors should preserve correlation_id when explicitly set."""
        error = ErrorDeActor(correlation_id="custom-789")
        assert error.correlation_id == "custom-789"

    def test_correlation_id_for_request_response(self):
        """Correlation ID should link request to response."""
        cmd = DescargarDocumento(q_param="abc123")
        event = DocumentoDescargado(
            correlation_id=cmd.correlation_id,
            document_id="doc-123",
            q_param="abc123",
        )
        assert cmd.correlation_id == event.correlation_id
