"""
CAS GUI Bridge Actor.

Connects the GUI layer to the CAS coordinator actor,
providing a clean interface for starting, monitoring,
and controlling CAS scraping jobs.
"""
from dataclasses import dataclass, field, asdict
from typing import Optional, Callable, Awaitable, Dict, Any
from datetime import datetime, timezone
import asyncio
import logging
import uuid

logger = logging.getLogger(__name__)

from src.infrastructure.actors.cas_base_actor import CASBaseActor


@dataclass
class CASGuiConfig:
    """Configuration for CAS GUI scraping jobs."""
    year_from: Optional[int] = None
    year_to: Optional[int] = None
    sport: Optional[str] = None
    matter: Optional[str] = None
    max_results: int = 100

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return asdict(self)


@dataclass
class CASJobStarted:
    """Event emitted when a CAS job starts."""
    job_id: str
    timestamp: str
    filters: Dict[str, Any]


@dataclass
class CASJobProgress:
    """Event emitted for CAS job progress updates."""
    job_id: str
    estado: str
    descubiertos: int
    descargados: int
    procesados: int
    errores: int
    porcentaje: float


@dataclass
class CASJobCompleted:
    """Event emitted when a CAS job completes."""
    job_id: str
    timestamp: str
    estadisticas: Dict[str, Any]
    exito: bool


@dataclass
class CASJobError:
    """Event emitted when a CAS job encounters an error."""
    job_id: str
    timestamp: str
    mensaje: str
    recuperable: bool


class CASGuiBridgeActor(CASBaseActor):
    """
    Bridge actor connecting GUI to CAS coordinator.

    Provides a simple interface for:
    - Starting scraping jobs with filters
    - Monitoring job progress
    - Pausing, resuming, and stopping jobs
    - Receiving event callbacks for UI updates
    """

    def __init__(
        self,
        on_event: Optional[Callable[[Any], Awaitable[None]]] = None,
        output_dir: str = "cas_data",
    ):
        """
        Initialize the CAS GUI bridge actor.

        Args:
            on_event: Async callback for job events
            output_dir: Directory for output files
        """
        super().__init__()
        self._on_event = on_event
        self._output_dir = output_dir
        self._current_job_id: Optional[str] = None
        self._coordinator = None

    async def _emit_event(self, event: Any) -> None:
        """Emit an event to the callback if registered."""
        if self._on_event:
            await self._on_event(event)

    async def start_job(self, config: CASGuiConfig) -> str:
        """
        Start a new CAS scraping job.

        Args:
            config: Configuration for the job

        Returns:
            Job ID for tracking
        """
        # Generate unique job ID
        job_id = f"cas-gui-{uuid.uuid4().hex[:8]}"
        self._current_job_id = job_id

        # Build filters from config
        filters = {}
        if config.year_from:
            filters["year_from"] = config.year_from
        if config.year_to:
            filters["year_to"] = config.year_to
        if config.sport:
            filters["sport"] = config.sport
        if config.matter:
            filters["matter"] = config.matter
        filters["max_results"] = config.max_results

        # Emit started event
        event = CASJobStarted(
            job_id=job_id,
            timestamp=datetime.now(timezone.utc).isoformat(),
            filters=filters,
        )
        await self._emit_event(event)

        # Run CAS scraping directly (coordinator was never connected)
        asyncio.create_task(self._run_cas_scrape(config, job_id))

        return job_id

    async def _run_cas_scrape(self, config: CASGuiConfig, job_id: str):
        """Run CAS scraping using Playwright to render SharePoint page."""
        import os
        import re

        total_saved = 0
        total_errors = 0
        max_results = config.max_results

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
                page = await context.new_page()

                try:
                    cas_url = "https://jurisprudence.tas-cas.org/Shared%20Documents/Forms/AllItems.aspx"
                    await page.goto(cas_url, timeout=60000)
                    await page.wait_for_load_state("networkidle", timeout=30000)
                    await asyncio.sleep(5)

                    # Extract case data from SharePoint document library
                    cases = await page.evaluate("""
                        () => {
                            const results = [];
                            // SharePoint list/library items
                            const rows = document.querySelectorAll('tr');
                            for (const row of rows) {
                                const cells = row.querySelectorAll('td');
                                if (cells.length >= 3) {
                                    const text = row.innerText.trim();
                                    // CAS cases have format like "CAS 2024/A/1234"
                                    const caseMatch = text.match(/CAS\\s*(\\d{4})[\\s/]*(\\w)[\\s/]*(\\d+)/i);
                                    if (caseMatch) {
                                        results.push({
                                            caseNumber: `CAS ${caseMatch[1]}/${caseMatch[2]}/${caseMatch[3]}`,
                                            year: caseMatch[1],
                                            fullText: text.substring(0, 500)
                                        });
                                    }
                                }
                            }
                            // Also try links with PDF references
                            const links = document.querySelectorAll('a[href*=".pdf"], a[href*="CAS"]');
                            for (const link of links) {
                                const text = link.innerText.trim();
                                const href = link.getAttribute('href') || '';
                                const caseMatch = (text + ' ' + href).match(/CAS\\s*(\\d{4})[\\s/]*(\\w)[\\s/]*(\\d+)/i);
                                if (caseMatch) {
                                    const cn = `CAS ${caseMatch[1]}/${caseMatch[2]}/${caseMatch[3]}`;
                                    if (!results.find(r => r.caseNumber === cn)) {
                                        results.push({
                                            caseNumber: cn,
                                            year: caseMatch[1],
                                            fullText: text.substring(0, 500),
                                            url: href
                                        });
                                    }
                                }
                            }
                            return results;
                        }
                    """)

                    logger.info(f"CAS Playwright found {len(cases)} cases on page")

                    for case in cases:
                        if total_saved >= max_results:
                            break
                        case_num = case.get("caseNumber", "")
                        year = case.get("year", "")
                        title = case.get("fullText", case_num)[:500]

                        if supabase_client and case_num:
                            try:
                                supabase_client.table("scraper_documents").upsert({
                                    "source_type": "cas",
                                    "external_id": case_num,
                                    "title": title,
                                    "publication_date": f"{year}-01-01" if year else None,
                                }, on_conflict="source_type,external_id").execute()
                                total_saved += 1
                            except Exception:
                                total_errors += 1

                except Exception as e:
                    logger.error(f"CAS Playwright error: {e}")
                    total_errors += 1
                finally:
                    await context.close()
                    await browser.close()

        except ImportError:
            logger.error("Playwright not available for CAS scraping")
        except Exception as e:
            logger.error(f"CAS scrape failed: {e}")

        # Emit completion
        await self._emit_event(CASJobCompleted(
            job_id=job_id,
            timestamp=datetime.now(timezone.utc).isoformat(),
            estadisticas={"documents_saved": total_saved, "errors": total_errors},
            exito=total_errors == 0,
        ))
        self._current_job_id = None
        logger.info(f"CAS scrape complete: {total_saved} cases, {total_errors} errors")

    async def pause_job(self) -> None:
        """Pause the current job."""
        if self._coordinator and self._current_job_id:
            await self._coordinator.tell({
                "type": "pause_job",
                "job_id": self._current_job_id,
            })

    async def resume_job(self) -> None:
        """Resume the current job."""
        if self._coordinator and self._current_job_id:
            await self._coordinator.tell({
                "type": "resume_job",
                "job_id": self._current_job_id,
            })

    async def stop_job(self) -> None:
        """Stop the current job."""
        if self._coordinator and self._current_job_id:
            await self._coordinator.tell({
                "type": "stop_job",
                "job_id": self._current_job_id,
            })

    async def handle_coordinator_event(self, event: Dict[str, Any]) -> None:
        """
        Handle events from the coordinator.

        Translates coordinator events to GUI events.
        """
        event_type = event.get("type")
        job_id = event.get("job_id", self._current_job_id)

        if event_type == "progress":
            gui_event = CASJobProgress(
                job_id=job_id,
                estado=event.get("estado", "DESCONOCIDO"),
                descubiertos=event.get("descubiertos", 0),
                descargados=event.get("descargados", 0),
                procesados=event.get("procesados", 0),
                errores=event.get("errores", 0),
                porcentaje=event.get("porcentaje", 0.0),
            )
            await self._emit_event(gui_event)

        elif event_type == "completed":
            gui_event = CASJobCompleted(
                job_id=job_id,
                timestamp=datetime.now(timezone.utc).isoformat(),
                estadisticas=event.get("estadisticas", {}),
                exito=event.get("exito", True),
            )
            await self._emit_event(gui_event)

        elif event_type == "error":
            gui_event = CASJobError(
                job_id=job_id,
                timestamp=datetime.now(timezone.utc).isoformat(),
                mensaje=event.get("mensaje", "Error desconocido"),
                recuperable=event.get("recuperable", False),
            )
            await self._emit_event(gui_event)
