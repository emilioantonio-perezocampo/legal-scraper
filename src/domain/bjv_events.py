"""
BJV Domain Events

Domain events in Spanish following DDD patterns.
All events are immutable frozen dataclasses with automatic timestamps.
"""
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from src.domain.bjv_value_objects import IdentificadorLibro, AreaDerecho


@dataclass(frozen=True)
class BusquedaIniciada:
    """Event: Search operation started."""
    busqueda_id: str
    termino_busqueda: str
    max_resultados: int = 100
    area_derecho: Optional[AreaDerecho] = None
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass(frozen=True)
class LibroDescubierto:
    """Event: Book discovered during scraping."""
    libro_id: IdentificadorLibro
    titulo: str
    url: str
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass(frozen=True)
class LibroDescargado:
    """Event: Book PDF downloaded successfully."""
    libro_id: IdentificadorLibro
    ruta_archivo: str
    tamano_bytes: int
    timestamp: datetime = field(default_factory=datetime.now)

    @property
    def tamano_mb(self) -> float:
        """Get file size in megabytes."""
        return self.tamano_bytes / (1024 * 1024)


@dataclass(frozen=True)
class LibroProcesado:
    """Event: Book processed and parsed."""
    libro_id: IdentificadorLibro
    total_paginas: int
    total_fragmentos: int
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass(frozen=True)
class FragmentoIndexado:
    """Event: Text fragment indexed with embedding."""
    fragmento_id: str
    libro_id: IdentificadorLibro
    modelo_embedding: str
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass(frozen=True)
class ErrorScraping:
    """Event: Error occurred during scraping."""
    libro_id: IdentificadorLibro
    mensaje_error: str
    codigo_error: str
    es_recuperable: bool = False
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass(frozen=True)
class ScrapingCompletado:
    """Event: Scraping operation completed."""
    total_libros_descubiertos: int
    total_libros_descargados: int
    total_errores: int
    duracion_segundos: float
    timestamp: datetime = field(default_factory=datetime.now)

    @property
    def tasa_exito(self) -> float:
        """Calculate success rate percentage."""
        if self.total_libros_descubiertos == 0:
            return 0.0
        return (self.total_libros_descargados / self.total_libros_descubiertos) * 100

    @property
    def duracion_formateada(self) -> str:
        """Format duration as HH:MM:SS."""
        total_seconds = int(self.duracion_segundos)
        hours = total_seconds // 3600
        minutes = (total_seconds % 3600) // 60
        seconds = total_seconds % 60
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


@dataclass(frozen=True)
class PuntoControlCreado:
    """Event: Checkpoint created for resume capability."""
    checkpoint_id: str
    pagina_actual: int
    libros_procesados: int
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass(frozen=True)
class ScrapingPausado:
    """Event: Scraping operation paused."""
    razon: str
    libros_pendientes: int
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass(frozen=True)
class ScrapingReanudado:
    """Event: Scraping operation resumed from checkpoint."""
    checkpoint_id: str
    libros_restantes: int
    timestamp: datetime = field(default_factory=datetime.now)
