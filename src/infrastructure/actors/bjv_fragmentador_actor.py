"""
BJV Fragmentador Actor.

Chunks text into fragments using the BJVTextChunker.
"""
from typing import Optional

from src.infrastructure.actors.bjv_base_actor import BJVBaseActor
from src.infrastructure.actors.bjv_messages import (
    FragmentarTexto,
    TextoFragmentado,
    ErrorProcesamiento,
)
from src.infrastructure.adapters.bjv_text_chunker import (
    BJVTextChunker,
    ChunkingConfig,
)


class BJVFragmentadorActor(BJVBaseActor):
    """
    Actor for chunking text into fragments.

    Responsibilities:
    - Chunk text using BJVTextChunker
    - Preserve legal structure (articles, sections)
    - Forward TextoFragmentado to coordinator
    """

    def __init__(
        self,
        coordinator: BJVBaseActor,
        chunking_config: Optional[ChunkingConfig] = None,
    ):
        """
        Initialize the fragmentador actor.

        Args:
            coordinator: Parent coordinator actor
            chunking_config: Optional chunking configuration
        """
        super().__init__(nombre="BJVFragmentador")
        self._coordinator = coordinator
        self._chunker = BJVTextChunker(config=chunking_config)

    async def handle_message(self, message) -> None:
        """
        Handle incoming messages.

        Args:
            message: Message to process
        """
        if isinstance(message, FragmentarTexto):
            await self._process_fragmentar(
                message.correlation_id,
                message.libro_id,
                message.capitulo_id,
                message.texto,
            )

    async def _process_fragmentar(
        self,
        correlation_id: str,
        libro_id: str,
        capitulo_id: Optional[str],
        texto: str,
    ) -> None:
        """Process a text fragmentation request."""
        try:
            fragmentos = self._chunker.fragmentar(
                texto=texto,
                libro_id=libro_id,
                numero_capitulo=int(capitulo_id) if capitulo_id and capitulo_id.isdigit() else None,
            )

            msg = TextoFragmentado(
                correlation_id=correlation_id,
                libro_id=libro_id,
                total_fragmentos=len(fragmentos),
                fragmentos=fragmentos,
            )
            await self._coordinator.tell(msg)

        except Exception as e:
            error_msg = ErrorProcesamiento(
                correlation_id=correlation_id,
                libro_id=libro_id,
                actor_origen=self.nombre,
                tipo_error=type(e).__name__,
                mensaje_error=str(e),
                recuperable=False,
            )
            await self._coordinator.tell(error_msg)
