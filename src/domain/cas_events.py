"""
CAS Domain Events

Domain events for the Court of Arbitration for Sport (CAS/TAS) scraper.
All events are immutable (frozen dataclasses) following DDD patterns.

Events represent significant occurrences during the scraping process.
"""
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from src.domain.cas_value_objects import (
    NumeroCaso,
    URLLaudo,
    CategoriaDeporte,
)


@dataclass(frozen=True)
class BusquedaCASIniciada:
    """
    Event: CAS search initiated.

    Emitted when a new search operation begins.
    """
    termino_busqueda: str
    deporte: Optional[CategoriaDeporte] = None
    anio: Optional[int] = None
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass(frozen=True)
class LaudoDescubierto:
    """
    Event: Arbitral award discovered.

    Emitted when a new award is found during scraping.
    """
    numero_caso: NumeroCaso
    titulo: str
    url: Optional[URLLaudo] = None
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass(frozen=True)
class LaudoDescargado:
    """
    Event: Arbitral award downloaded.

    Emitted when an award PDF/document is successfully downloaded.
    """
    numero_caso: NumeroCaso
    ruta_archivo: str
    tamano_bytes: Optional[int] = None
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass(frozen=True)
class LaudoProcesado:
    """
    Event: Arbitral award processed.

    Emitted when text extraction and embedding generation complete.
    """
    numero_caso: NumeroCaso
    fragmentos_generados: int
    embeddings_generados: Optional[int] = None
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass(frozen=True)
class ErrorScrapingCAS:
    """
    Event: Scraping error occurred.

    Emitted when an error occurs during the scraping process.
    """
    mensaje: str
    tipo_error: str
    numero_caso: Optional[NumeroCaso] = None
    reintentable: bool = False
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass(frozen=True)
class ScrapingCASCompletado:
    """
    Event: CAS scraping completed.

    Emitted when the entire scraping job finishes.
    """
    total_laudos_descubiertos: int
    total_laudos_descargados: int
    total_errores: int
    duracion_segundos: Optional[float] = None
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass(frozen=True)
class ScrapingCASPausado:
    """
    Event: CAS scraping paused.

    Emitted when scraping is paused (user request or checkpoint).
    """
    razon: str
    laudos_pendientes: int
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass(frozen=True)
class ScrapingCASReanudado:
    """
    Event: CAS scraping resumed.

    Emitted when scraping resumes from a checkpoint.
    """
    checkpoint_id: str
    laudos_restantes: int
    timestamp: datetime = field(default_factory=datetime.now)
