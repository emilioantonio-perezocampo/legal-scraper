"""
Typed actor messages for SCJN scraping pipeline.

Convention:
- Commands: Imperative Spanish verbs (DescubrirDocumentos, DescargarDocumento)
- Events: Past participle (DocumentoDescubierto, PDFProcesado)
- All messages are frozen dataclasses
"""
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Any
from uuid import uuid4


def _generate_correlation_id() -> str:
    """Generate a unique correlation ID for message tracking."""
    return str(uuid4())


def _now() -> datetime:
    """Generate current timestamp."""
    return datetime.now()


# ============ BASE ============

@dataclass(frozen=True)
class ActorMessage:
    """
    Base class for all typed actor messages.

    Provides automatic correlation_id and timestamp for tracking.
    """
    correlation_id: str = field(default_factory=_generate_correlation_id)
    timestamp: datetime = field(default_factory=_now)


# ============ COMMANDS ============

@dataclass(frozen=True)
class DescubrirDocumentos(ActorMessage):
    """
    Command: Start document discovery.

    Initiates search for documents matching the given criteria.
    """
    category: Optional[str] = None
    scope: Optional[str] = None
    status: Optional[str] = None
    max_results: int = 100
    discover_all_pages: bool = False


@dataclass(frozen=True)
class DescubrirPagina(ActorMessage):
    """
    Command: Discover documents on a specific page.

    Used for paginated discovery.
    """
    category: Optional[str] = None
    scope: Optional[str] = None
    status: Optional[str] = None
    page_number: int = 1


@dataclass(frozen=True)
class DescargarDocumento(ActorMessage):
    """
    Command: Download document by q_param.

    Fetches document content from SCJN website.
    """
    q_param: str = ""
    include_pdf: bool = True
    include_reforms: bool = True


@dataclass(frozen=True)
class ProcesarPDF(ActorMessage):
    """
    Command: Extract and chunk PDF.

    Processes PDF bytes into text chunks for embedding.
    """
    document_id: str = ""
    pdf_bytes: bytes = b""
    source_url: str = ""


@dataclass(frozen=True)
class GenerarEmbeddings(ActorMessage):
    """
    Command: Generate embeddings for chunks.

    Creates vector embeddings from text chunks.
    """
    document_id: str = ""
    chunks: tuple = ()  # tuple[TextChunk, ...]


@dataclass(frozen=True)
class GuardarDocumento(ActorMessage):
    """
    Command: Persist document.

    Saves document to storage.
    """
    document: Any = None  # SCJNDocument


@dataclass(frozen=True)
class GuardarEmbedding(ActorMessage):
    """
    Command: Persist embedding.

    Saves embedding to vector store.
    """
    embedding: Any = None  # DocumentEmbedding


@dataclass(frozen=True)
class GuardarEmbeddingsBatch(ActorMessage):
    """
    Command: Persist multiple embeddings.

    Saves batch of embeddings to vector store.
    """
    embeddings: tuple = ()  # tuple[DocumentEmbedding, ...]
    document_id: str = ""


@dataclass(frozen=True)
class BuscarSimilares(ActorMessage):
    """
    Command: Search for similar embeddings.

    Performs vector similarity search in the store.
    """
    query_vector: tuple = ()  # tuple[float, ...]
    top_k: int = 10
    filter_document_ids: Optional[tuple] = None  # Optional filter by document


@dataclass(frozen=True)
class ObtenerEstadisticasVectorStore(ActorMessage):
    """
    Command: Get vector store statistics.

    Returns metrics about the vector store.
    """
    pass


@dataclass(frozen=True)
class GuardarCheckpoint(ActorMessage):
    """
    Command: Save scraping progress.

    Persists checkpoint for resume capability.
    """
    checkpoint: Any = None  # ScrapingCheckpoint


@dataclass(frozen=True)
class CargarCheckpoint(ActorMessage):
    """
    Command: Load checkpoint by session_id.

    Retrieves checkpoint for resuming scraping.
    """
    session_id: str = ""


@dataclass(frozen=True)
class PausarPipeline(ActorMessage):
    """
    Command: Pause scraping.

    Signals the pipeline to pause processing.
    """
    pass


@dataclass(frozen=True)
class ReanudarPipeline(ActorMessage):
    """
    Command: Resume scraping.

    Signals the pipeline to resume processing.
    """
    pass


@dataclass(frozen=True)
class ObtenerEstado(ActorMessage):
    """
    Command: Get pipeline status (ask pattern).

    Requests current status of the scraping pipeline.
    """
    pass


# ============ EVENTS ============

@dataclass(frozen=True)
class DocumentoDescubierto(ActorMessage):
    """
    Event: Document URL discovered.

    Raised when a new document is found during discovery.
    """
    q_param: str = ""
    title: str = ""
    category: str = ""


@dataclass(frozen=True)
class PaginaDescubierta(ActorMessage):
    """
    Event: Discovery page processed.

    Raised when a search results page has been processed.
    """
    documents_found: int = 0
    current_page: int = 1
    total_pages: int = 1
    has_more_pages: bool = False


@dataclass(frozen=True)
class DocumentoDescargado(ActorMessage):
    """
    Event: Document downloaded.

    Raised when document content has been fetched.
    """
    document_id: str = ""
    q_param: str = ""
    has_pdf: bool = False
    pdf_size_bytes: int = 0


@dataclass(frozen=True)
class PDFProcesado(ActorMessage):
    """
    Event: PDF processed into chunks.

    Raised when PDF text has been extracted and chunked.
    """
    document_id: str = ""
    chunk_count: int = 0
    total_tokens: int = 0
    extraction_confidence: float = 0.0


@dataclass(frozen=True)
class EmbeddingsGenerados(ActorMessage):
    """
    Event: Embeddings created.

    Raised when vector embeddings have been generated.
    """
    document_id: str = ""
    embedding_count: int = 0


@dataclass(frozen=True)
class DocumentoGuardado(ActorMessage):
    """
    Event: Document persisted.

    Raised when document has been saved to storage.
    """
    document_id: str = ""


@dataclass(frozen=True)
class CheckpointGuardado(ActorMessage):
    """
    Event: Checkpoint saved.

    Raised when scraping progress has been persisted.
    """
    session_id: str = ""
    processed_count: int = 0


@dataclass(frozen=True)
class EmbeddingsGuardados(ActorMessage):
    """
    Event: Embeddings persisted to vector store.

    Raised when embeddings have been saved.
    """
    document_id: str = ""
    count: int = 0


@dataclass(frozen=True)
class ResultadosBusqueda(ActorMessage):
    """
    Event: Search results returned.

    Contains similar embeddings found in vector store.
    """
    results: tuple = ()  # tuple[tuple[str, float], ...]  # (chunk_id, score)
    query_dimension: int = 0
    search_time_ms: float = 0.0


@dataclass(frozen=True)
class EstadisticasVectorStore(ActorMessage):
    """
    Event: Vector store statistics.

    Contains metrics about the vector store state.
    """
    total_vectors: int = 0
    dimension: int = 0
    index_type: str = ""
    memory_usage_bytes: int = 0
    document_count: int = 0


# ============ ERRORS ============

@dataclass(frozen=True)
class ErrorDeActor(ActorMessage):
    """
    Event: Error occurred in actor.

    Raised when an error occurs during processing.
    Includes information for retry/recovery decisions.
    """
    actor_name: str = ""
    document_id: Optional[str] = None
    error_type: str = ""
    error_message: str = ""
    recoverable: bool = True
    original_command: Optional[ActorMessage] = None
