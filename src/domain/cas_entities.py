"""
CAS Domain Entities

Entities for the Court of Arbitration for Sport (CAS/TAS) scraper.
All entities are immutable (frozen dataclasses) following DDD patterns.

LaudoArbitral is the aggregate root.
"""
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional, Tuple, Dict, Any

from src.domain.cas_value_objects import (
    NumeroCaso,
    FechaLaudo,
    URLLaudo,
    TipoProcedimiento,
    CategoriaDeporte,
    TipoMateria,
    IdiomaLaudo,
    EstadoLaudo,
)


class TipoParte(Enum):
    """Type of party in CAS proceedings."""
    APELANTE = "appellant"
    APELADO = "respondent"
    DEMANDANTE = "claimant"
    DEMANDADO = "defendant"


@dataclass(frozen=True)
class Arbitro:
    """
    Arbitrator entity.

    Represents a member of the CAS arbitration panel.
    """
    nombre: str
    nacionalidad: str
    rol: Optional[str] = None


@dataclass(frozen=True)
class Parte:
    """
    Party entity.

    Represents a party (appellant, respondent, etc.) in CAS proceedings.
    """
    nombre: str
    tipo: TipoParte
    pais: Optional[str] = None


@dataclass(frozen=True)
class Federacion:
    """
    Federation entity.

    Represents a sports federation involved in CAS proceedings.
    """
    nombre: str
    acronimo: str
    deporte: Optional[CategoriaDeporte] = None


@dataclass(frozen=True)
class LaudoArbitral:
    """
    Arbitral Award aggregate root.

    Represents a CAS/TAS arbitration decision with all associated metadata.
    This is the main entity for the CAS scraper domain.
    """
    id: str
    numero_caso: NumeroCaso
    fecha: FechaLaudo
    titulo: str
    tipo_procedimiento: Optional[TipoProcedimiento] = None
    categoria_deporte: Optional[CategoriaDeporte] = None
    materia: Optional[TipoMateria] = None
    partes: Tuple[Parte, ...] = field(default_factory=tuple)
    arbitros: Tuple[Arbitro, ...] = field(default_factory=tuple)
    federaciones: Tuple[Federacion, ...] = field(default_factory=tuple)
    url: Optional[URLLaudo] = None
    idioma: Optional[IdiomaLaudo] = None
    resumen: Optional[str] = None
    texto_completo: Optional[str] = None
    estado: Optional[EstadoLaudo] = None
    palabras_clave: Tuple[str, ...] = field(default_factory=tuple)
    fecha_creacion: datetime = field(default_factory=datetime.now)


@dataclass(frozen=True)
class FragmentoLaudo:
    """
    Text fragment entity for RAG.

    Represents a chunk of text from an arbitral award for vector search.
    """
    id: str
    laudo_id: str
    texto: str
    posicion: int
    seccion: Optional[str] = None


@dataclass(frozen=True)
class EmbeddingLaudo:
    """
    Embedding entity for vector search.

    Represents a vector embedding of a text fragment.
    """
    id: str
    fragmento_id: str
    vector: Tuple[float, ...]
    modelo: str


@dataclass(frozen=True)
class ResultadoBusquedaCAS:
    """
    Search result entity.

    Represents a search result with relevance score.
    """
    numero_caso: NumeroCaso
    titulo: str
    relevancia: float
    fragmento_relevante: Optional[str] = None


@dataclass(frozen=True)
class PuntoControlScrapingCAS:
    """
    Scraping checkpoint entity.

    Enables resume capability for long-running scraping jobs.
    """
    id: str
    pagina_actual: int
    laudos_procesados: int
    filtros_activos: Optional[Dict[str, Any]] = None
    laudos_pendientes: Tuple[str, ...] = field(default_factory=tuple)
    fecha_checkpoint: datetime = field(default_factory=datetime.now)
