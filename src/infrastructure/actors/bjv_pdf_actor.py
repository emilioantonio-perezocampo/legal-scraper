"""
BJV PDF Processor Actor.

Downloads PDF files and saves them to disk.
"""
import aiohttp
import os
import tempfile
from typing import Optional

from src.infrastructure.actors.bjv_base_actor import BJVBaseActor
from src.infrastructure.actors.bjv_messages import (
    DescargarPDF,
    PDFListo,
    ErrorProcesamiento,
)
from src.infrastructure.actors.bjv_rate_limiter import BJVRateLimiter


USER_AGENT = "BJVScraper/1.0 (Educational Research)"


class BJVPDFActor(BJVBaseActor):
    """
    Actor for downloading PDF files.

    Responsibilities:
    - Download PDF files
    - Save to disk
    - Retry on transient failures
    - Forward PDFListo with file path to coordinator
    """

    def __init__(
        self,
        coordinator: BJVBaseActor,
        rate_limiter: BJVRateLimiter,
        max_retries: int = 3,
        download_dir: Optional[str] = None,
    ):
        """
        Initialize the PDF actor.

        Args:
            coordinator: Parent coordinator actor
            rate_limiter: Rate limiter for requests
            max_retries: Maximum retry attempts
            download_dir: Directory to save PDFs (default: temp dir)
        """
        super().__init__(nombre="BJVPDFProcessor")
        self._coordinator = coordinator
        self._rate_limiter = rate_limiter
        self._max_retries = max_retries
        self._download_dir = download_dir or tempfile.gettempdir()
        self._session: Optional[aiohttp.ClientSession] = None

    async def start(self) -> None:
        """Start the actor and create HTTP session."""
        await super().start()
        timeout = aiohttp.ClientTimeout(total=60)  # Longer timeout for PDFs
        self._session = aiohttp.ClientSession(
            timeout=timeout,
            headers={"User-Agent": USER_AGENT},
        )

    async def stop(self) -> None:
        """Stop the actor and close HTTP session."""
        if self._session:
            await self._session.close()
        await super().stop()

    async def handle_message(self, message) -> None:
        """
        Handle incoming messages.

        Args:
            message: Message to process
        """
        if isinstance(message, DescargarPDF):
            await self._process_pdf(
                message.correlation_id,
                message.libro_id,
                message.capitulo_id,
                message.url_pdf,
            )

    async def _process_pdf(
        self,
        correlation_id: str,
        libro_id: str,
        capitulo_id: Optional[str],
        url_pdf: str,
    ) -> None:
        """Process a PDF download request."""
        last_error = None
        pdf_bytes = None

        # Download phase with retries
        for attempt in range(self._max_retries):
            try:
                pdf_bytes = await self._fetch_pdf(url_pdf)
                break
            except Exception as e:
                last_error = e
                if not self._is_retryable(e):
                    break

        if pdf_bytes is None:
            # Download failed
            error_msg = self._create_error_message(
                correlation_id, libro_id, last_error
            )
            await self._coordinator.tell(error_msg)
            return

        # Save to disk
        try:
            ruta_pdf = self._save_pdf(libro_id, capitulo_id, pdf_bytes)

            msg = PDFListo(
                correlation_id=correlation_id,
                libro_id=libro_id,
                capitulo_id=capitulo_id,
                ruta_pdf=ruta_pdf,
            )
            await self._coordinator.tell(msg)

        except Exception as e:
            error_msg = self._create_error_message(correlation_id, libro_id, e)
            await self._coordinator.tell(error_msg)

    async def _fetch_pdf(self, url: str) -> bytes:
        """
        Fetch a PDF file with rate limiting.

        Args:
            url: URL to fetch

        Returns:
            PDF bytes
        """
        await self._rate_limiter.acquire()

        async with self._session.get(url) as response:
            response.raise_for_status()
            return await response.read()

    def _save_pdf(
        self, libro_id: str, capitulo_id: Optional[str], pdf_bytes: bytes
    ) -> str:
        """
        Save PDF bytes to disk.

        Args:
            libro_id: Book ID
            capitulo_id: Chapter ID (optional)
            pdf_bytes: PDF content

        Returns:
            Path to saved file
        """
        # Create filename
        if capitulo_id:
            filename = f"{libro_id}_{capitulo_id}.pdf"
        else:
            filename = f"{libro_id}.pdf"

        filepath = os.path.join(self._download_dir, filename)

        with open(filepath, "wb") as f:
            f.write(pdf_bytes)

        return filepath

    def _is_retryable(self, error: Exception) -> bool:
        """Check if error is retryable."""
        return isinstance(error, (aiohttp.ClientError, TimeoutError))

    def _create_error_message(
        self, correlation_id: str, libro_id: str, error: Exception
    ) -> ErrorProcesamiento:
        """Create error message from exception."""
        recoverable = self._is_retryable(error)
        return ErrorProcesamiento(
            correlation_id=correlation_id,
            libro_id=libro_id,
            actor_origen=self.nombre,
            tipo_error=type(error).__name__,
            mensaje_error=str(error),
            recuperable=recoverable,
        )
