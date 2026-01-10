"""
CAS Repository Ports (Interfaces)

Repository ports define the contract for persistence/retrieval.
These are Protocol classes following the Ports & Adapters pattern.
Implementations will be in the infrastructure layer.
"""
from typing import Protocol, Optional, Tuple, runtime_checkable

from src.domain.cas_value_objects import NumeroCaso, CategoriaDeporte, TipoMateria
from src.domain.cas_entities import (
    LaudoArbitral,
    FragmentoLaudo,
    EmbeddingLaudo,
    ResultadoBusquedaCAS,
    PuntoControlScrapingCAS,
)


@runtime_checkable
class LaudoRepositorio(Protocol):
    """
    Repository port for arbitral award persistence.

    Defines the contract for storing and retrieving CAS awards.
    """

    async def guardar(self, laudo: LaudoArbitral) -> None:
        """Save an arbitral award to the repository."""
        ...

    async def obtener_por_numero(self, numero: NumeroCaso) -> Optional[LaudoArbitral]:
        """Get an award by its case number."""
        ...

    async def buscar(
        self,
        termino: str,
        deporte: Optional[CategoriaDeporte] = None,
        materia: Optional[TipoMateria] = None,
        limite: int = 100
    ) -> Tuple[ResultadoBusquedaCAS, ...]:
        """Search awards by term and optional filters."""
        ...

    async def listar_todos(self, limite: int = 1000, offset: int = 0) -> Tuple[LaudoArbitral, ...]:
        """List all awards with pagination."""
        ...

    async def existe(self, numero: NumeroCaso) -> bool:
        """Check if an award exists in the repository."""
        ...


@runtime_checkable
class FragmentoLaudoRepositorio(Protocol):
    """
    Repository port for text fragment persistence.

    Defines the contract for storing and retrieving text fragments
    for RAG operations.
    """

    async def guardar(self, fragmento: FragmentoLaudo) -> None:
        """Save a text fragment to the repository."""
        ...

    async def obtener_por_laudo(self, laudo_id: str) -> Tuple[FragmentoLaudo, ...]:
        """Get all fragments for an award."""
        ...

    async def buscar_similares(
        self,
        vector: Tuple[float, ...],
        limite: int = 10
    ) -> Tuple[ResultadoBusquedaCAS, ...]:
        """Search for similar fragments by vector similarity."""
        ...


@runtime_checkable
class EmbeddingLaudoRepositorio(Protocol):
    """
    Repository port for embedding persistence.

    Defines the contract for storing and retrieving vector embeddings.
    """

    async def guardar(self, embedding: EmbeddingLaudo) -> None:
        """Save an embedding to the repository."""
        ...

    async def obtener_por_fragmento(self, fragmento_id: str) -> Optional[EmbeddingLaudo]:
        """Get the embedding for a fragment."""
        ...


@runtime_checkable
class CheckpointCASRepositorio(Protocol):
    """
    Repository port for scraping checkpoint persistence.

    Defines the contract for storing and retrieving checkpoints
    to enable resume capability.
    """

    async def guardar(self, checkpoint: PuntoControlScrapingCAS) -> None:
        """Save a checkpoint."""
        ...

    async def obtener_ultimo(self) -> Optional[PuntoControlScrapingCAS]:
        """Get the most recent checkpoint."""
        ...

    async def eliminar(self, checkpoint_id: str) -> bool:
        """Delete a checkpoint by ID. Returns True if deleted."""
        ...
