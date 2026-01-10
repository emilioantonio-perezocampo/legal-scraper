"""
BJV Repository Ports (Interfaces)

Repository ports define the contract for persistence/retrieval.
These are Protocol classes following the Ports & Adapters pattern.
Implementations will be in the infrastructure layer.
"""
from typing import Protocol, Optional, Tuple, Sequence, runtime_checkable

from src.domain.bjv_value_objects import IdentificadorLibro, AreaDerecho
from src.domain.bjv_entities import (
    LibroBJV,
    FragmentoTexto,
    EmbeddingFragmento,
    ResultadoBusqueda,
    PuntoControlScraping,
)


@runtime_checkable
class LibroRepositorio(Protocol):
    """
    Repository port for book persistence.

    Defines the contract for storing and retrieving books.
    """

    async def guardar(self, libro: LibroBJV) -> None:
        """Save a book to the repository."""
        ...

    async def obtener_por_id(self, libro_id: IdentificadorLibro) -> Optional[LibroBJV]:
        """Get a book by its identifier."""
        ...

    async def buscar(
        self,
        termino: str,
        area_derecho: Optional[AreaDerecho] = None,
        limite: int = 100
    ) -> Tuple[ResultadoBusqueda, ...]:
        """Search books by term and optional area filter."""
        ...

    async def listar_todos(self, limite: int = 1000, offset: int = 0) -> Tuple[LibroBJV, ...]:
        """List all books with pagination."""
        ...

    async def existe(self, libro_id: IdentificadorLibro) -> bool:
        """Check if a book exists in the repository."""
        ...


@runtime_checkable
class FragmentoRepositorio(Protocol):
    """
    Repository port for text fragment persistence.

    Defines the contract for storing and retrieving text fragments
    and their embeddings.
    """

    async def guardar(self, fragmento: FragmentoTexto, embedding: Optional[EmbeddingFragmento] = None) -> None:
        """Save a text fragment with optional embedding."""
        ...

    async def obtener_por_libro(self, libro_id: IdentificadorLibro) -> Tuple[FragmentoTexto, ...]:
        """Get all fragments for a book."""
        ...

    async def buscar_similares(
        self,
        vector: Tuple[float, ...],
        limite: int = 10
    ) -> Tuple[ResultadoBusqueda, ...]:
        """Search for similar fragments by vector similarity."""
        ...


@runtime_checkable
class CheckpointRepositorio(Protocol):
    """
    Repository port for scraping checkpoint persistence.

    Defines the contract for storing and retrieving checkpoints
    to enable resume capability.
    """

    async def guardar(self, checkpoint: PuntoControlScraping) -> None:
        """Save a checkpoint."""
        ...

    async def obtener_ultimo(self) -> Optional[PuntoControlScraping]:
        """Get the most recent checkpoint."""
        ...

    async def eliminar(self, checkpoint_id: str) -> bool:
        """Delete a checkpoint by ID. Returns True if deleted."""
        ...
