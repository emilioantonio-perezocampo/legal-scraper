"""
BJV Domain Value Objects

Value objects are immutable and defined by their attributes.
They have no identity beyond their values.
All use frozen dataclasses with Spanish domain terminology.
"""
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Optional


class TipoContenido(Enum):
    """
    Represents content types available in BJV.
    """
    LIBRO = "libro"
    CAPITULO = "capitulo"
    ARTICULO = "articulo"
    REVISTA = "revista"


class AreaDerecho(Enum):
    """
    Legal practice areas in Mexican law.
    """
    CIVIL = "civil"
    PENAL = "penal"
    CONSTITUCIONAL = "constitucional"
    ADMINISTRATIVO = "administrativo"
    MERCANTIL = "mercantil"
    LABORAL = "laboral"
    FISCAL = "fiscal"
    INTERNACIONAL = "internacional"
    GENERAL = "general"


@dataclass(frozen=True)
class ISBN:
    """
    International Standard Book Number.

    Immutable value object for book identification.
    """
    valor: str

    @property
    def normalizado(self) -> str:
        """Return ISBN without dashes for comparison."""
        return self.valor.replace("-", "")


@dataclass(frozen=True)
class AnioPublicacion:
    """
    Publication year value object.
    """
    valor: int

    @property
    def es_reciente(self) -> bool:
        """Check if publication is from last 5 years."""
        current_year = datetime.now().year
        return (current_year - self.valor) <= 5


@dataclass(frozen=True)
class URLArchivo:
    """
    File URL with format information.
    """
    url: str
    formato: str

    @property
    def es_pdf(self) -> bool:
        """Check if file is a PDF."""
        return self.formato.lower() == "pdf"

    @property
    def nombre_archivo(self) -> str:
        """Extract filename from URL."""
        return self.url.split("/")[-1]


@dataclass(frozen=True)
class IdentificadorLibro:
    """
    Book identifier in the BJV system.

    Primary identity for books in the Biblioteca Jurídica Virtual.
    """
    bjv_id: str

    @property
    def url(self) -> str:
        """Generate BJV detail page URL."""
        return f"https://biblio.juridicas.unam.mx/bjv/detalle/{self.bjv_id}"
