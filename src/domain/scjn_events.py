"""
SCJN Domain Events

Domain events for the SCJN legislation scraping bounded context.
Events are immutable records of things that have happened.
"""
from dataclasses import dataclass, field
from datetime import datetime


@dataclass(frozen=True)
class DocumentoDescubierto:
    """
    Event: A new document URL was discovered.

    Raised when the discovery actor finds a new document to scrape.
    """
    q_param: str
    title: str
    discovered_at: datetime = field(default_factory=datetime.now)


@dataclass(frozen=True)
class DocumentoDescargado:
    """
    Event: Document content was successfully downloaded.

    Raised when document HTML or PDF has been fetched from SCJN.
    """
    document_id: str
    source: str  # 'html' | 'pdf'


@dataclass(frozen=True)
class DocumentoProcesado:
    """
    Event: Document was parsed and chunked.

    Raised when document text has been extracted and split into chunks.
    """
    document_id: str
    chunk_count: int


@dataclass(frozen=True)
class EmbeddingGenerado:
    """
    Event: Embedding was created for a chunk.

    Raised when a vector embedding has been generated for a text chunk.
    """
    chunk_id: str
    document_id: str


@dataclass(frozen=True)
class ErrorDeScraping:
    """
    Event: An error occurred during scraping.

    Raised when any error occurs during the scraping process.
    Includes information about whether the error is recoverable.
    """
    document_id: str
    error_type: str
    error_message: str
    recoverable: bool
