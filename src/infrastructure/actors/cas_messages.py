"""
CAS Pipeline Actor Messages

Message types for the CAS scraper actor system.
All messages are frozen dataclasses for immutability.
"""
from dataclasses import dataclass, field, replace
from typing import Tuple, Optional, Dict, Any
from enum import Enum, auto

from src.domain.cas_entities import LaudoArbitral, FragmentoLaudo


class EstadoPipelineCAS(Enum):
    """Pipeline execution states."""
    INICIANDO = auto()
    DESCUBRIENDO = auto()
    SCRAPEANDO = auto()
    FRAGMENTANDO = auto()
    COMPLETADO = auto()
    ERROR = auto()
    PAUSADO = auto()


@dataclass(frozen=True)
class IniciarDescubrimientoCAS:
    """Message to start CAS award discovery."""
    max_paginas: Optional[int] = None
    filtros: Optional[Dict[str, Any]] = None


@dataclass(frozen=True)
class LaudoDescubiertoCAS:
    """Message when an award is discovered in search results."""
    numero_caso: str
    url: str
    titulo: str = ""


@dataclass(frozen=True)
class ScrapearLaudoCAS:
    """Message to scrape a specific award."""
    numero_caso: str
    url: str
    reintentos: int = 0


@dataclass(frozen=True)
class LaudoScrapeadoCAS:
    """Message when an award is successfully scraped."""
    laudo: LaudoArbitral


@dataclass(frozen=True)
class FragmentarLaudoCAS:
    """Message to fragment award text for embeddings."""
    laudo_id: str
    texto: str


@dataclass(frozen=True)
class FragmentosListosCAS:
    """Message when fragments are ready."""
    laudo_id: str
    fragmentos: Tuple[FragmentoLaudo, ...]


@dataclass(frozen=True)
class ErrorCASPipeline:
    """Error message for the CAS pipeline."""
    mensaje: str
    tipo_error: str
    recuperable: bool = True
    contexto: Optional[Dict[str, Any]] = None


@dataclass(frozen=True)
class EstadisticasPipelineCAS:
    """Pipeline execution statistics."""
    laudos_descubiertos: int = 0
    laudos_scrapeados: int = 0
    laudos_fragmentados: int = 0
    errores: int = 0

    @property
    def tasa_exito(self) -> float:
        """Calculate success rate."""
        if self.laudos_descubiertos == 0:
            return 0.0
        return self.laudos_scrapeados / self.laudos_descubiertos

    def con_incremento(self, campo: str) -> "EstadisticasPipelineCAS":
        """Return new instance with incremented field."""
        current_value = getattr(self, campo)
        return replace(self, **{campo: current_value + 1})


@dataclass(frozen=True)
class PausarPipelineCAS:
    """Message to pause the pipeline."""
    razon: str = "usuario"


@dataclass(frozen=True)
class ReanudarPipelineCAS:
    """Message to resume the pipeline."""
    desde_checkpoint: bool = False


@dataclass(frozen=True)
class DetenerPipelineCAS:
    """Message to stop the pipeline."""
    guardar_progreso: bool = True


@dataclass(frozen=True)
class ObtenerEstadoCAS:
    """Message to query pipeline state."""
    incluir_estadisticas: bool = True
