"""
CAS Fragmentador Actor

Actor responsible for fragmenting (chunking) CAS award text for embeddings.
"""
import logging
from typing import Optional

from src.infrastructure.actors.cas_base_actor import CASBaseActor
from src.infrastructure.actors.cas_messages import (
    EstadoPipelineCAS,
    FragmentarLaudoCAS,
    FragmentosListosCAS,
    ErrorCASPipeline,
)
from src.infrastructure.adapters.cas_text_chunker import CASTextChunker
from src.infrastructure.adapters.cas_errors import ChunkingError


logger = logging.getLogger(__name__)


class CASFragmentadorActor(CASBaseActor):
    """
    Actor for fragmenting CAS award text.

    Chunks award text into fragments suitable for
    embedding and semantic search.
    """

    def __init__(
        self,
        text_chunker: Optional[CASTextChunker] = None,
        actor_id: Optional[str] = None,
        supervisor: Optional[CASBaseActor] = None,
    ):
        super().__init__(actor_id=actor_id, supervisor=supervisor)
        self._text_chunker = text_chunker or CASTextChunker()
        self._fragmented_count = 0
        self._total_fragments = 0

    @property
    def text_chunker(self) -> CASTextChunker:
        """Get text chunker."""
        return self._text_chunker

    @property
    def fragmented_count(self) -> int:
        """Get count of fragmented awards."""
        return self._fragmented_count

    @property
    def total_fragments(self) -> int:
        """Get total fragments created."""
        return self._total_fragments

    async def handle_fragmentar_laudo_cas(self, msg: FragmentarLaudoCAS) -> None:
        """
        Handle fragment award message.

        Args:
            msg: Fragment request with laudo_id and texto
        """
        self._set_estado(EstadoPipelineCAS.FRAGMENTANDO)
        logger.info(f"Fragmenting: {msg.laudo_id}")

        try:
            fragmentos = await self._fragment_text(msg)

            self._fragmented_count += 1
            self._total_fragments += len(fragmentos)

            result_msg = FragmentosListosCAS(
                laudo_id=msg.laudo_id,
                fragmentos=fragmentos,
            )

            if self._supervisor:
                await self._supervisor.tell(result_msg)

            logger.debug(f"Created {len(fragmentos)} fragments for {msg.laudo_id}")

        except ChunkingError as e:
            error = ErrorCASPipeline(
                mensaje=str(e),
                tipo_error="ChunkingError",
                recuperable=True,
                contexto={"laudo_id": msg.laudo_id},
            )
            await self.escalate(error)
        except Exception as e:
            error = self._handle_error(e)
            await self.escalate(error)

    async def _fragment_text(self, msg: FragmentarLaudoCAS) -> tuple:
        """
        Fragment text into chunks.

        Args:
            msg: Fragment request message

        Returns:
            Tuple of FragmentoLaudo
        """
        # Handle empty text
        if not msg.texto or not msg.texto.strip():
            logger.debug(f"Empty text for {msg.laudo_id}")
            return ()

        # Fragment by sections for better semantic coherence
        fragmentos = self._text_chunker.fragmentar_por_secciones(
            msg.texto,
            msg.laudo_id,
        )

        return fragmentos
