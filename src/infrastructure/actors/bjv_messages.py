"""
BJV actor messages.

Commands (imperative) and queries for actor communication.
All messages are immutable frozen dataclasses with Spanish naming.
"""
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Tuple, Any


# ============================================================================
# Discovery Commands
# ============================================================================

@dataclass(frozen=True)
class IniciarBusqueda:
    """Start search/discovery."""
    correlation_id: str
    query: Optional[str] = None
    area_derecho: Optional[str] = None
    anio_desde: Optional[int] = None
    anio_hasta: Optional[int] = None
    max_resultados: int = 100
    timestamp: datetime = field(default_factory=datetime.utcnow)


@dataclass(frozen=True)
class ProcesarPaginaResultados:
    """Process a page of search results."""
    correlation_id: str
    numero_pagina: int
    url_pagina: str
    timestamp: datetime = field(default_factory=datetime.utcnow)


@dataclass(frozen=True)
class BusquedaCompletada:
    """Search completed response."""
    correlation_id: str
    total_descubiertos: int
    total_paginas: int
    timestamp: datetime = field(default_factory=datetime.utcnow)


# ============================================================================
# Download Commands
# ============================================================================

@dataclass(frozen=True)
class DescargarLibro:
    """Download book details."""
    correlation_id: str
    libro_id: str
    url_detalle: str
    incluir_capitulos: bool = True
    timestamp: datetime = field(default_factory=datetime.utcnow)


@dataclass(frozen=True)
class DescargarPDF:
    """Download a PDF file."""
    correlation_id: str
    libro_id: str
    capitulo_id: Optional[str]
    url_pdf: str
    timestamp: datetime = field(default_factory=datetime.utcnow)


@dataclass(frozen=True)
class LibroListo:
    """Book ready for processing."""
    correlation_id: str
    libro_id: str
    libro: Any  # LibroBJV
    timestamp: datetime = field(default_factory=datetime.utcnow)


@dataclass(frozen=True)
class PDFListo:
    """PDF ready for text extraction."""
    correlation_id: str
    libro_id: str
    capitulo_id: Optional[str]
    ruta_pdf: str
    timestamp: datetime = field(default_factory=datetime.utcnow)


# ============================================================================
# Processing Commands
# ============================================================================

@dataclass(frozen=True)
class ExtraerTextoPDF:
    """Extract text from PDF."""
    correlation_id: str
    libro_id: str
    capitulo_id: Optional[str]
    ruta_pdf: str
    timestamp: datetime = field(default_factory=datetime.utcnow)


@dataclass(frozen=True)
class TextoExtraido:
    """Text extracted from PDF."""
    correlation_id: str
    libro_id: str
    capitulo_id: Optional[str]
    texto: str
    total_paginas: int
    confianza: float
    timestamp: datetime = field(default_factory=datetime.utcnow)


@dataclass(frozen=True)
class FragmentarTexto:
    """Chunk text into fragments."""
    correlation_id: str
    libro_id: str
    capitulo_id: Optional[str]
    texto: str
    timestamp: datetime = field(default_factory=datetime.utcnow)


@dataclass(frozen=True)
class TextoFragmentado:
    """Text chunked response."""
    correlation_id: str
    libro_id: str
    total_fragmentos: int
    fragmentos: Tuple  # Tuple[FragmentoTexto, ...]
    timestamp: datetime = field(default_factory=datetime.utcnow)


@dataclass(frozen=True)
class GenerarEmbeddings:
    """Generate embeddings for fragments."""
    correlation_id: str
    libro_id: str
    fragmentos: Tuple  # Tuple[FragmentoTexto, ...]
    timestamp: datetime = field(default_factory=datetime.utcnow)


@dataclass(frozen=True)
class EmbeddingsGenerados:
    """Embeddings generated response."""
    correlation_id: str
    libro_id: str
    total_embeddings: int
    timestamp: datetime = field(default_factory=datetime.utcnow)


# ============================================================================
# Persistence Commands
# ============================================================================

@dataclass(frozen=True)
class GuardarLibro:
    """Persist book to storage."""
    correlation_id: str
    libro: Any  # LibroBJV
    timestamp: datetime = field(default_factory=datetime.utcnow)


@dataclass(frozen=True)
class GuardarFragmentos:
    """Persist fragments to storage."""
    correlation_id: str
    libro_id: str
    fragmentos: Tuple  # Tuple[FragmentoTexto, ...]
    timestamp: datetime = field(default_factory=datetime.utcnow)


@dataclass(frozen=True)
class GuardarEmbeddings:
    """Persist embeddings to vector store."""
    correlation_id: str
    libro_id: str
    embeddings: Tuple  # Tuple[EmbeddingFragmento, ...]
    timestamp: datetime = field(default_factory=datetime.utcnow)


@dataclass(frozen=True)
class GuardarCheckpoint:
    """Save scraping checkpoint."""
    correlation_id: str
    checkpoint: Any  # PuntoControlScraping
    timestamp: datetime = field(default_factory=datetime.utcnow)


# ============================================================================
# Pipeline Control Commands
# ============================================================================

@dataclass(frozen=True)
class IniciarPipeline:
    """Start the scraping pipeline."""
    session_id: str
    max_resultados: int = 100
    query: Optional[str] = None
    timestamp: datetime = field(default_factory=datetime.utcnow)


@dataclass(frozen=True)
class PausarPipeline:
    """Pause the scraping pipeline."""
    correlation_id: str = ""
    timestamp: datetime = field(default_factory=datetime.utcnow)


@dataclass(frozen=True)
class ReanudarPipeline:
    """Resume the scraping pipeline."""
    correlation_id: str = ""
    session_id: Optional[str] = None
    timestamp: datetime = field(default_factory=datetime.utcnow)


@dataclass(frozen=True)
class DetenerPipeline:
    """Stop the pipeline gracefully."""
    correlation_id: str = ""
    guardar_checkpoint: bool = True
    timestamp: datetime = field(default_factory=datetime.utcnow)


@dataclass(frozen=True)
class ObtenerEstado:
    """Query current pipeline state."""
    timestamp: datetime = field(default_factory=datetime.utcnow)


# ============================================================================
# Error Messages
# ============================================================================

@dataclass(frozen=True)
class ErrorProcesamiento:
    """Processing error notification."""
    correlation_id: str
    libro_id: Optional[str]
    actor_origen: str
    tipo_error: str
    mensaje_error: str
    recuperable: bool = True
    timestamp: datetime = field(default_factory=datetime.utcnow)
