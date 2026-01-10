"""
BJV Domain Entities

Entities have identity and lifecycle.
All entities are immutable (frozen dataclasses) with Spanish domain terminology.
LibroBJV is the aggregate root for the BJV bounded context.
"""
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Tuple

from src.domain.bjv_value_objects import (
    IdentificadorLibro,
    ISBN,
    AnioPublicacion,
    URLArchivo,
    AreaDerecho,
)


@dataclass(frozen=True)
class Autor:
    """
    Book author entity.
    """
    nombre: str
    afiliacion: Optional[str] = None

    @property
    def nombre_completo(self) -> str:
        """Return full name."""
        return self.nombre


@dataclass(frozen=True)
class Editorial:
    """
    Publisher entity.
    """
    nombre: str
    pais: Optional[str] = None

    @property
    def es_unam(self) -> bool:
        """Check if publisher is UNAM."""
        return "unam" in self.nombre.lower()


@dataclass(frozen=True)
class CapituloLibro:
    """
    Book chapter entity.
    """
    numero: int
    titulo: str
    pagina_inicio: int
    pagina_fin: int
    url_archivo: Optional[URLArchivo] = None

    @property
    def total_paginas(self) -> int:
        """Calculate total pages in chapter (inclusive)."""
        return self.pagina_fin - self.pagina_inicio + 1


@dataclass(frozen=True)
class LibroBJV:
    """
    Book aggregate root for BJV bounded context.

    This is the main entity representing a book in the
    Biblioteca Jurídica Virtual system.
    """
    id: IdentificadorLibro
    titulo: str
    subtitulo: Optional[str] = None
    autores: Tuple[Autor, ...] = field(default_factory=tuple)
    editores: Optional[Tuple[Autor, ...]] = None
    editorial: Optional[Editorial] = None
    isbn: Optional[ISBN] = None
    anio_publicacion: Optional[AnioPublicacion] = None
    area_derecho: Optional[AreaDerecho] = None
    capitulos: Tuple[CapituloLibro, ...] = field(default_factory=tuple)
    url_pdf: Optional[URLArchivo] = None
    portada_url: Optional[str] = None
    total_paginas: Optional[int] = None
    resumen: Optional[str] = None
    palabras_clave: Optional[Tuple[str, ...]] = None

    @property
    def url(self) -> str:
        """Get book URL via its identifier."""
        return self.id.url


@dataclass(frozen=True)
class FragmentoTexto:
    """
    Text fragment entity for embedding generation.
    """
    id: str
    libro_id: IdentificadorLibro
    contenido: str
    numero_pagina: int
    posicion: int = 0
    numero_capitulo: Optional[int] = None

    @property
    def longitud(self) -> int:
        """Get character length of content."""
        return len(self.contenido)


@dataclass(frozen=True)
class EmbeddingFragmento:
    """
    Vector embedding for a text fragment.
    """
    fragmento_id: str
    vector: Tuple[float, ...]
    modelo: str

    @property
    def dimension(self) -> int:
        """Get vector dimension."""
        return len(self.vector)


@dataclass(frozen=True)
class ResultadoBusqueda:
    """
    Search result entity.
    """
    libro_id: IdentificadorLibro
    titulo: str
    relevancia: float
    fragmento_coincidente: Optional[str] = None

    @property
    def es_muy_relevante(self) -> bool:
        """Check if result is highly relevant (>= 0.8)."""
        return self.relevancia >= 0.8


@dataclass(frozen=True)
class PuntoControlScraping:
    """
    Scraping checkpoint entity for resume capability.
    """
    id: str
    ultima_pagina_procesada: int
    ultimo_libro_id: Optional[str]
    total_libros_descubiertos: int
    total_libros_descargados: int
    timestamp: Optional[datetime] = None
    total_errores: int = 0

    @property
    def progreso_porcentaje(self) -> float:
        """Calculate progress percentage."""
        if self.total_libros_descubiertos == 0:
            return 0.0
        return (self.total_libros_descargados / self.total_libros_descubiertos) * 100

    @property
    def libros_pendientes(self) -> int:
        """Calculate pending books."""
        return self.total_libros_descubiertos - self.total_libros_descargados
