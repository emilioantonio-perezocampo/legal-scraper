"""
SCJN Domain Entities

Entities for SCJN legislation domain following DDD principles.
All entities are immutable (frozen dataclasses) with tuple-based collections.
"""
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Optional
from uuid import uuid4

from .scjn_value_objects import (
    DocumentCategory,
    DocumentScope,
    DocumentStatus,
)


@dataclass(frozen=True)
class SCJNArticle:
    """
    An individual article within a legal document.

    Value Object: Identity is defined by all its attributes.
    """
    number: str  # "1", "2 Bis", "Transitorio Primero"
    title: str = ""
    content: str = ""
    reform_dates: tuple = field(default_factory=tuple)  # tuple[str, ...]


@dataclass(frozen=True)
class SCJNReform:
    """
    A specific reform/amendment to a document.

    Entity: Has unique identity via id field.
    Represents a single modification to a legal instrument.
    """
    id: str = field(default_factory=lambda: str(uuid4()))
    q_param: str = ""  # Encrypted URL parameter from SCJN
    publication_date: Optional[date] = None
    publication_number: str = ""
    gazette_section: str = ""
    text_content: Optional[str] = None
    pdf_path: Optional[str] = None


@dataclass(frozen=True)
class SCJNDocument:
    """
    Aggregate Root for SCJN legislation documents.

    Represents a complete legal instrument with its articles and reform history.
    All collections are tuples to maintain immutability.
    """
    id: str = field(default_factory=lambda: str(uuid4()))
    q_param: str = ""  # Encrypted URL parameter from SCJN
    title: str = ""
    short_title: str = ""
    category: DocumentCategory = DocumentCategory.LEY
    scope: DocumentScope = DocumentScope.FEDERAL
    status: DocumentStatus = DocumentStatus.VIGENTE
    publication_date: Optional[date] = None
    expedition_date: Optional[date] = None
    state: Optional[str] = None  # For state-level documents
    subject_matters: tuple = field(default_factory=tuple)  # tuple[SubjectMatter, ...]
    articles: tuple = field(default_factory=tuple)  # tuple[SCJNArticle, ...]
    reforms: tuple = field(default_factory=tuple)  # tuple[SCJNReform, ...]
    source_url: str = ""

    @property
    def article_count(self) -> int:
        """Returns the number of articles in the document."""
        return len(self.articles)

    @property
    def reform_count(self) -> int:
        """Returns the number of reforms/amendments."""
        return len(self.reforms)


@dataclass(frozen=True)
class TextChunk:
    """
    A chunk of text prepared for embedding.

    Represents a segment of document text suitable for vector embedding.
    Metadata stored as tuple of key-value tuple pairs for immutability.
    """
    id: str = field(default_factory=lambda: str(uuid4()))
    document_id: str = ""
    content: str = ""
    token_count: int = 0
    chunk_index: int = 0
    metadata: tuple = field(default_factory=tuple)  # tuple[tuple[str, str], ...]


@dataclass(frozen=True)
class DocumentEmbedding:
    """
    Vector embedding for a text chunk.

    Stores the vector representation of a text chunk.
    Vector is a tuple of floats for immutability.
    """
    chunk_id: str
    vector: tuple  # tuple[float, ...]
    model_name: str = "sentence-transformers/all-MiniLM-L6-v2"


@dataclass(frozen=True)
class ScrapingCheckpoint:
    """
    Tracks progress for resume capability.

    Allows scraping sessions to be paused and resumed.
    Stores information about where processing stopped.
    """
    session_id: str
    last_processed_q_param: str
    processed_count: int
    failed_q_params: tuple = field(default_factory=tuple)  # tuple[str, ...]
    created_at: datetime = field(default_factory=datetime.now)
