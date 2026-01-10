"""
PDF Processor Actor

Processes PDF documents into text chunks for embedding.
Integrates PDFExtractor and TextChunker adapters.
"""
import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Optional

from .base import BaseActor
from .messages import (
    ProcesarPDF,
    PDFProcesado,
    ErrorDeActor,
)
from src.infrastructure.adapters.pdf_extractor import PDFExtractor
from src.infrastructure.adapters.text_chunker import TextChunker, ChunkConfig
from src.infrastructure.adapters.errors import (
    PDFExtractionError,
    PDFCorruptedError,
    PDFPasswordProtectedError,
    PDFEmptyError,
)
from src.domain.scjn_entities import TextChunk


class PDFProcessorActor(BaseActor):
    """
    Actor that processes PDFs into text chunks.

    Integrates:
    - PDFExtractor: For text extraction from PDF bytes
    - TextChunker: For splitting text into embedding-ready chunks

    Handles:
    - ProcesarPDF: Extract and chunk PDF
    - ("GET_CHUNKS", document_id): Retrieve chunks for a document
    """

    def __init__(
        self,
        max_tokens: int = 512,
        overlap_tokens: int = 50,
        ocr_enabled: bool = False,
    ):
        """
        Initialize the PDF processor actor.

        Args:
            max_tokens: Maximum tokens per chunk
            overlap_tokens: Token overlap between chunks
            ocr_enabled: Whether to enable OCR for scanned PDFs
        """
        super().__init__()
        self._max_tokens = max_tokens
        self._overlap_tokens = overlap_tokens
        self._ocr_enabled = ocr_enabled
        self._pdf_extractor: Optional[PDFExtractor] = None
        self._text_chunker: Optional[TextChunker] = None
        self._executor: Optional[ThreadPoolExecutor] = None
        self._chunks: Dict[str, List[TextChunk]] = {}

    async def start(self):
        """Start the actor and initialize adapters."""
        await super().start()
        self._executor = ThreadPoolExecutor(max_workers=2)
        self._initialize_adapters()

    async def stop(self):
        """Stop the actor and cleanup."""
        if self._executor:
            self._executor.shutdown(wait=False)
        await super().stop()

    def _initialize_adapters(self):
        """Initialize PDF extractor and text chunker."""
        if self._pdf_extractor is None:
            self._pdf_extractor = PDFExtractor(ocr_enabled=self._ocr_enabled)

        if self._text_chunker is None:
            config = ChunkConfig(
                max_tokens=self._max_tokens,
                overlap_tokens=self._overlap_tokens,
            )
            self._text_chunker = TextChunker(config)

    async def handle_message(self, message):
        """Handle incoming messages."""
        if isinstance(message, ProcesarPDF):
            return await self._process_pdf(message)

        elif isinstance(message, tuple) and len(message) >= 2:
            if message[0] == "GET_CHUNKS":
                return self._chunks.get(message[1], [])

        return None

    async def _process_pdf(self, message: ProcesarPDF) -> PDFProcesado:
        """Process PDF bytes into chunks."""
        document_id = message.document_id
        pdf_bytes = message.pdf_bytes

        # Validate input
        if not pdf_bytes:
            return ErrorDeActor(
                correlation_id=message.correlation_id,
                actor_name="PDFProcessorActor",
                document_id=document_id,
                error_type="PDFExtractionError",
                error_message="Empty PDF bytes provided",
                recoverable=False,
                original_command=message,
            )

        # Extract text from PDF
        try:
            loop = asyncio.get_event_loop()
            extraction_result = await loop.run_in_executor(
                self._executor,
                lambda: self._pdf_extractor.extract_from_bytes(pdf_bytes),
            )
        except (PDFCorruptedError, PDFPasswordProtectedError, PDFEmptyError) as e:
            return ErrorDeActor(
                correlation_id=message.correlation_id,
                actor_name="PDFProcessorActor",
                document_id=document_id,
                error_type=type(e).__name__,
                error_message=str(e),
                recoverable=isinstance(e, PDFEmptyError),
                original_command=message,
            )
        except Exception as e:
            return ErrorDeActor(
                correlation_id=message.correlation_id,
                actor_name="PDFProcessorActor",
                document_id=document_id,
                error_type=type(e).__name__,
                error_message=str(e),
                recoverable=True,
                original_command=message,
            )

        # Chunk the extracted text
        try:
            chunk_results = await loop.run_in_executor(
                self._executor,
                lambda: self._text_chunker.chunk(extraction_result.text),
            )
        except Exception as e:
            return ErrorDeActor(
                correlation_id=message.correlation_id,
                actor_name="PDFProcessorActor",
                document_id=document_id,
                error_type=type(e).__name__,
                error_message=f"Chunking failed: {e}",
                recoverable=True,
                original_command=message,
            )

        # Convert to TextChunk domain objects
        chunks = []
        total_tokens = 0
        for i, result in enumerate(chunk_results):
            chunk = TextChunk(
                id=f"{document_id}-chunk-{i:04d}",
                document_id=document_id,
                content=result.content,
                token_count=result.token_count,
                chunk_index=i,
                metadata=(
                    ("source_url", message.source_url),
                    ("start_char", str(result.start_char)),
                    ("end_char", str(result.end_char)),
                ),
            )
            chunks.append(chunk)
            total_tokens += result.token_count

        # Store chunks
        self._chunks[document_id] = chunks

        return PDFProcesado(
            correlation_id=message.correlation_id,
            document_id=document_id,
            chunk_count=len(chunks),
            total_tokens=total_tokens,
            extraction_confidence=extraction_result.confidence,
        )
