"""
BJV GUI Bridge Actor

Bridges the GUI to the BJV scraper actor system.
Translates GUI commands into BJV-specific actor messages and
forwards scraper events back to the GUI.
"""
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Any, Callable, Dict, Tuple
from uuid import uuid4
import asyncio

from src.infrastructure.actors.base import BaseActor
from src.domain.bjv_value_objects import AreaDerecho
from src.domain.bjv_events import (
    BusquedaIniciada,
    LibroDescubierto,
    LibroDescargado,
    LibroProcesado,
    ErrorScraping,
    ScrapingCompletado,
    ScrapingPausado,
    ScrapingReanudado,
)


@dataclass(frozen=True)
class BJVSearchConfig:
    """
    Configuration for BJV book search.

    Immutable configuration for a search operation.
    """
    termino_busqueda: Optional[str] = None
    area_derecho: Optional[AreaDerecho] = None
    max_resultados: int = 100
    incluir_capitulos: bool = True
    descargar_pdfs: bool = True
    rate_limit: float = 1.0
    concurrency: int = 2


@dataclass(frozen=True)
class BJVProgress:
    """
    Progress information for BJV scraping.

    Immutable progress snapshot.
    """
    libros_descubiertos: int = 0
    libros_descargados: int = 0
    libros_pendientes: int = 0
    descargas_activas: int = 0
    errores: int = 0
    estado: str = "IDLE"


@dataclass(frozen=True)
class BJVJobStarted:
    """Event: BJV scraping job started."""
    job_id: str
    config: BJVSearchConfig
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass(frozen=True)
class BJVJobProgress:
    """Event: BJV scraping progress update."""
    job_id: str
    progress: BJVProgress
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass(frozen=True)
class BJVJobCompleted:
    """Event: BJV scraping job completed."""
    job_id: str
    total_libros_descubiertos: int
    total_libros_descargados: int
    total_errores: int
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass(frozen=True)
class BJVJobFailed:
    """Event: BJV scraping job failed."""
    job_id: str
    mensaje_error: str
    timestamp: datetime = field(default_factory=datetime.now)


class BJVGuiBridgeActor(BaseActor):
    """
    Actor that bridges the GUI with the BJV scraper actor system.

    Responsibilities:
    - Connect to BJV coordinator actor (when implemented)
    - Translate GUI commands into BJV-specific messages
    - Poll for progress updates
    - Forward events to GUI event handler

    Messages:
    - ("START_SEARCH", config) -> Starts BJV search
    - "STOP_SEARCH" -> Stops current search
    - "PAUSE_SEARCH" -> Pauses search
    - "RESUME_SEARCH" -> Resumes search
    - "GET_STATUS" -> Returns current status
    - "GET_PROGRESS" -> Returns progress info
    """

    def __init__(
        self,
        coordinator_actor: Any = None,
        event_handler: Optional[Callable] = None,
        poll_interval: float = 2.0,
    ):
        super().__init__()
        self._coordinator = coordinator_actor
        self._event_handler = event_handler
        self._poll_interval = poll_interval
        self._current_job_id: Optional[str] = None
        self._is_polling = False
        self._poll_task: Optional[asyncio.Task] = None
        self._current_progress = BJVProgress()
        self._is_paused = False
        # Simulated state for demo (will be replaced with real coordinator)
        self._simulated_discovered = 0
        self._simulated_downloaded = 0

    @property
    def is_connected(self) -> bool:
        """Check if bridge is connected to coordinator."""
        # For now, always return True for demo purposes
        # Will check coordinator when implemented
        return True

    @property
    def current_job_id(self) -> Optional[str]:
        """Get current job ID if any."""
        return self._current_job_id

    async def start(self):
        """Start the bridge actor."""
        await super().start()

    async def stop(self):
        """Stop the bridge actor and cleanup."""
        await self._stop_polling()
        await super().stop()

    async def handle_message(self, message):
        """Process incoming messages."""
        # String messages
        if isinstance(message, str):
            if message == "STOP_SEARCH":
                return await self._handle_stop_search()
            elif message == "PAUSE_SEARCH":
                return await self._handle_pause_search()
            elif message == "RESUME_SEARCH":
                return await self._handle_resume_search()
            elif message == "GET_STATUS":
                return await self._handle_get_status()
            elif message == "GET_PROGRESS":
                return await self._handle_get_progress()

        # Tuple messages
        elif isinstance(message, tuple):
            command = message[0]

            if command == "START_SEARCH":
                config = message[1]
                return await self._handle_start_search(config)
            elif command == "SET_COORDINATOR":
                self._coordinator = message[1]
                return {"success": True}

        return None

    async def _handle_start_search(self, config: BJVSearchConfig) -> Dict[str, Any]:
        """Handle START_SEARCH command."""
        if self._current_job_id:
            return {"success": False, "error": "Search already in progress"}

        # Generate job ID
        self._current_job_id = str(uuid4())
        self._is_paused = False
        self._simulated_discovered = 0
        self._simulated_downloaded = 0

        # Emit job started event
        if self._event_handler:
            await self._emit_event(BJVJobStarted(
                job_id=self._current_job_id,
                config=config,
            ))

        # Start polling for progress (simulated for now)
        await self._start_polling(config)
        return {"success": True, "job_id": self._current_job_id}

    async def _handle_stop_search(self) -> Dict[str, Any]:
        """Handle STOP_SEARCH command."""
        await self._stop_polling()

        job_id = self._current_job_id
        self._current_job_id = None
        self._is_paused = False

        return {"success": True, "stopped_job_id": job_id}

    async def _handle_pause_search(self) -> Dict[str, Any]:
        """Handle PAUSE_SEARCH command."""
        if not self._current_job_id:
            return {"success": False, "error": "No active search to pause"}

        self._is_paused = True

        if self._event_handler:
            await self._emit_event(ScrapingPausado(
                razon="User requested pause",
                libros_pendientes=self._current_progress.libros_pendientes,
            ))

        return {"success": True, "paused": True}

    async def _handle_resume_search(self) -> Dict[str, Any]:
        """Handle RESUME_SEARCH command."""
        if not self._current_job_id:
            return {"success": False, "error": "No paused search to resume"}

        self._is_paused = False

        if self._event_handler:
            await self._emit_event(ScrapingReanudado(
                checkpoint_id=self._current_job_id,
                libros_restantes=self._current_progress.libros_pendientes,
            ))

        return {"success": True, "resumed": True}

    async def _handle_get_status(self) -> Dict[str, Any]:
        """Handle GET_STATUS command."""
        state = "idle"
        if self._current_job_id:
            state = "paused" if self._is_paused else "running"

        return {
            "connected": self.is_connected,
            "current_job_id": self._current_job_id,
            "is_polling": self._is_polling,
            "state": state,
        }

    async def _handle_get_progress(self) -> Optional[BJVProgress]:
        """Handle GET_PROGRESS command."""
        return self._current_progress

    async def _start_polling(self, config: BJVSearchConfig):
        """Start polling for progress updates."""
        if self._is_polling:
            return

        self._is_polling = True
        self._poll_task = asyncio.create_task(self._poll_loop(config))

    async def _stop_polling(self):
        """Stop polling for progress updates."""
        self._is_polling = False
        if self._poll_task:
            self._poll_task.cancel()
            try:
                await self._poll_task
            except asyncio.CancelledError:
                pass
            self._poll_task = None

    async def _poll_loop(self, config: BJVSearchConfig):
        """Real BJV scraping using Playwright to render JS-heavy pages."""
        import os
        import re
        logger = __import__('logging').getLogger(__name__)

        max_books = config.max_resultados
        total_saved = 0
        total_errors = 0

        # Supabase client
        supabase_client = None
        sb_url = os.environ.get("SUPABASE_URL")
        sb_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
        if sb_url and sb_key:
            try:
                from supabase import create_client
                supabase_client = create_client(sb_url, sb_key)
            except Exception:
                pass

        # Legal areas to search across
        areas = ["civil", "penal", "constitucional", "administrativo",
                 "mercantil", "laboral", "fiscal", "internacional"]
        if config.area:
            areas = [config.area]

        query = config.query or "derecho"

        try:
            from playwright.async_api import async_playwright

            async with async_playwright() as p:
                browser = await p.chromium.launch(
                    headless=True,
                    args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"]
                )
                context = await browser.new_context(
                    viewport={"width": 1920, "height": 1080},
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                )

                for area in areas:
                    if total_saved >= max_books:
                        break

                    page_num = 1
                    while page_num <= 50 and total_saved < max_books:
                        if not self._is_polling:
                            break

                        url = f"https://biblio.juridicas.unam.mx/bjv/resultados?ti={query}&area={area}&pagina={page_num}"
                        page = await context.new_page()
                        try:
                            await page.goto(url, timeout=30000)
                            await page.wait_for_load_state("networkidle", timeout=15000)
                            await asyncio.sleep(3)

                            # Extract book data from rendered page
                            books = await page.evaluate("""
                                () => {
                                    const results = [];
                                    // BJV renders book cards with links
                                    const items = document.querySelectorAll('.resultado-item, .libro-card, .card, article, .item-resultado, [class*="result"]');
                                    for (const item of items) {
                                        const link = item.querySelector('a[href*="libro"], a[href*="detalle"]');
                                        const title = item.querySelector('h2, h3, h4, .titulo, .title');
                                        if (link || title) {
                                            results.push({
                                                href: link ? link.getAttribute('href') : '',
                                                title: (title || link || item).innerText.trim().substring(0, 300),
                                            });
                                        }
                                    }
                                    // Fallback: any links with libro/detalle
                                    if (results.length === 0) {
                                        const links = document.querySelectorAll('a');
                                        for (const l of links) {
                                            const href = l.getAttribute('href') || '';
                                            const text = l.innerText.trim();
                                            if (text.length > 10 && (href.includes('libro') || href.includes('detalle'))) {
                                                results.push({href, title: text.substring(0, 300)});
                                            }
                                        }
                                    }
                                    // Check for next page
                                    const hasNext = !!document.querySelector('a[href*="pagina=' + (arguments[0] + 1) + '"], .pagination .next, [class*="next"]');
                                    return {books: results, hasNext};
                                }
                            """)

                            page_books = books.get("books", [])
                            has_next = books.get("hasNext", False)

                            for book in page_books:
                                if total_saved >= max_books:
                                    break
                                title = book.get("title", "").strip()
                                href = book.get("href", "")
                                ext_id = re.findall(r'/(\d+)', href)
                                ext_id = ext_id[0] if ext_id else f"bjv-{area}-{page_num}-{total_saved}"

                                if supabase_client and title:
                                    try:
                                        supabase_client.table("scraper_documents").upsert({
                                            "source_type": "bjv",
                                            "external_id": str(ext_id),
                                            "title": title[:500],
                                        }, on_conflict="source_type,external_id").execute()
                                        total_saved += 1
                                    except Exception:
                                        total_errors += 1

                            logger.info(f"BJV {area} p{page_num}: {len(page_books)} books (total: {total_saved})")

                            # Update progress
                            self._simulated_discovered = total_saved
                            self._simulated_downloaded = total_saved
                            self._current_progress = BJVProgress(
                                libros_descubiertos=total_saved,
                                libros_descargados=total_saved,
                                libros_pendientes=0,
                                descargas_activas=1,
                                errores=total_errors,
                                estado="RUNNING",
                            )
                            if self._event_handler:
                                await self._emit_event(BJVJobProgress(
                                    job_id=self._current_job_id,
                                    progress=self._current_progress,
                                ))

                            if not page_books or not has_next:
                                break
                            page_num += 1

                        except Exception as e:
                            logger.warning(f"BJV page error {area} p{page_num}: {e}")
                            total_errors += 1
                            break
                        finally:
                            await page.close()

                        await asyncio.sleep(2)  # Rate limit

                await browser.close()

        except ImportError:
            logger.error("Playwright not available for BJV scraping")
        except Exception as e:
            logger.error(f"BJV scrape failed: {e}")

        # Emit completion
        if self._event_handler and self._current_job_id:
            await self._emit_event(BJVJobCompleted(
                job_id=self._current_job_id,
                total_libros_descubiertos=total_saved,
                total_libros_descargados=total_saved,
                total_errores=total_errors,
            ))
        self._current_job_id = None
        self._is_polling = False
        logger.info(f"BJV scrape complete: {total_saved} books, {total_errors} errors")
        return

    async def _emit_event(self, event: Any):
        """Emit event to handler."""
        if self._event_handler:
            try:
                if asyncio.iscoroutinefunction(self._event_handler):
                    await self._event_handler(event)
                else:
                    self._event_handler(event)
            except Exception:
                pass  # Don't let handler errors break the bridge
