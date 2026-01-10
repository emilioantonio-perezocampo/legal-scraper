"""
SCJN Scraping Actor System

Actor-based architecture for document scraping pipeline.
"""
from .base import BaseActor
from .messages import (
    # Commands
    DescubrirDocumentos,
    DescubrirPagina,
    DescargarDocumento,
    ProcesarPDF,
    GenerarEmbeddings,
    GuardarDocumento,
    GuardarEmbedding,
    GuardarEmbeddingsBatch,
    BuscarSimilares,
    ObtenerEstadisticasVectorStore,
    GuardarCheckpoint,
    CargarCheckpoint,
    PausarPipeline,
    ReanudarPipeline,
    ObtenerEstado,
    # Events
    DocumentoDescubierto,
    PaginaDescubierta,
    DocumentoDescargado,
    PDFProcesado,
    EmbeddingsGenerados,
    EmbeddingsGuardados,
    ResultadosBusqueda,
    EstadisticasVectorStore,
    DocumentoGuardado,
    CheckpointGuardado,
    ErrorDeActor,
)
from .rate_limiter import RateLimiter, NoOpRateLimiter
from .checkpoint_actor import CheckpointActor
from .persistence_actor import SCJNPersistenceActor
from .embedding_actor import EmbeddingActor
from .pdf_processor_actor import PDFProcessorActor
from .scjn_scraper_actor import SCJNScraperActor
from .scjn_discovery_actor import SCJNDiscoveryActor
from .scjn_coordinator_actor import SCJNCoordinatorActor, PipelineState
from .vector_store_actor import FAISSVectorStoreActor

# CAS Actor System
from .cas_messages import (
    EstadoPipelineCAS,
    IniciarDescubrimientoCAS,
    LaudoDescubiertoCAS,
    ScrapearLaudoCAS,
    LaudoScrapeadoCAS,
    FragmentarLaudoCAS,
    FragmentosListosCAS,
    ErrorCASPipeline,
    EstadisticasPipelineCAS,
    PausarPipelineCAS,
    ReanudarPipelineCAS,
    DetenerPipelineCAS,
    ObtenerEstadoCAS,
)
from .cas_base_actor import CASBaseActor
from .cas_discovery_actor import CASDiscoveryActor
from .cas_scraper_actor import CASScraperActor
from .cas_fragmentador_actor import CASFragmentadorActor
from .cas_coordinator_actor import CASCoordinatorActor

__all__ = [
    # Base
    "BaseActor",
    # Commands
    "DescubrirDocumentos",
    "DescubrirPagina",
    "DescargarDocumento",
    "ProcesarPDF",
    "GenerarEmbeddings",
    "GuardarDocumento",
    "GuardarEmbedding",
    "GuardarEmbeddingsBatch",
    "BuscarSimilares",
    "ObtenerEstadisticasVectorStore",
    "GuardarCheckpoint",
    "CargarCheckpoint",
    "PausarPipeline",
    "ReanudarPipeline",
    "ObtenerEstado",
    # Events
    "DocumentoDescubierto",
    "PaginaDescubierta",
    "DocumentoDescargado",
    "PDFProcesado",
    "EmbeddingsGenerados",
    "EmbeddingsGuardados",
    "ResultadosBusqueda",
    "EstadisticasVectorStore",
    "DocumentoGuardado",
    "CheckpointGuardado",
    "ErrorDeActor",
    # Utilities
    "RateLimiter",
    "NoOpRateLimiter",
    # Actors
    "CheckpointActor",
    "SCJNPersistenceActor",
    "EmbeddingActor",
    "PDFProcessorActor",
    "SCJNScraperActor",
    "SCJNDiscoveryActor",
    "SCJNCoordinatorActor",
    "FAISSVectorStoreActor",
    "PipelineState",
    # CAS Actor System
    "EstadoPipelineCAS",
    "IniciarDescubrimientoCAS",
    "LaudoDescubiertoCAS",
    "ScrapearLaudoCAS",
    "LaudoScrapeadoCAS",
    "FragmentarLaudoCAS",
    "FragmentosListosCAS",
    "ErrorCASPipeline",
    "EstadisticasPipelineCAS",
    "PausarPipelineCAS",
    "ReanudarPipelineCAS",
    "DetenerPipelineCAS",
    "ObtenerEstadoCAS",
    "CASBaseActor",
    "CASDiscoveryActor",
    "CASScraperActor",
    "CASFragmentadorActor",
    "CASCoordinatorActor",
]
