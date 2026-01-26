"""
CAS LLM-based parser using Crawl4AI.

Extracts arbitration awards from the Court of Arbitration for Sport (CAS/TAS)
jurisprudence database using LLM-powered semantic extraction.
"""
import logging
from datetime import date
from typing import List, Optional

from pydantic import BaseModel, Field

from src.domain.cas_value_objects import (
    NumeroCaso,
    FechaLaudo,
    URLLaudo,
    TipoProcedimiento,
    CategoriaDeporte,
)
from src.infrastructure.adapters.crawl4ai_adapter import Crawl4AIAdapter

logger = logging.getLogger(__name__)


# --- Pydantic Schemas for LLM Extraction ---

class ExtractedCASCase(BaseModel):
    """Schema for a single CAS case extracted by LLM."""

    case_number: str = Field(
        description="CAS case number in format: CAS YYYY/A/NNNN or TAS YYYY/A/NNNN"
    )
    title: Optional[str] = Field(
        default=None,
        description="Case title or parties (e.g., 'A. v. B.')"
    )
    parties: Optional[str] = Field(
        default=None,
        description="Names of parties involved, comma separated"
    )
    date: Optional[str] = Field(
        default=None,
        description="Award date in YYYY-MM-DD format"
    )
    sport: Optional[str] = Field(
        default=None,
        description="Sport category: football, athletics, cycling, swimming, basketball, tennis, skiing, other"
    )
    procedure_type: Optional[str] = Field(
        default=None,
        description="Type: ordinary, appeal, anti-doping, mediation, advisory"
    )
    subject_matter: Optional[str] = Field(
        default=None,
        description="Subject: doping, transfer, eligibility, disciplinary, contractual, governance"
    )
    summary: Optional[str] = Field(
        default=None,
        description="Brief summary or description of the case"
    )
    detail_url: Optional[str] = Field(
        default=None,
        description="URL to view the full award"
    )
    language: Optional[str] = Field(
        default=None,
        description="Language: en, fr, es"
    )


class ExtractedCASSearchResults(BaseModel):
    """Schema for CAS search results page."""

    cases: List[ExtractedCASCase] = Field(
        default_factory=list,
        description="List of all CAS cases found on the page"
    )
    total_results: Optional[int] = Field(
        default=None,
        description="Total number of search results"
    )
    current_page: Optional[int] = Field(
        default=None,
        description="Current page number"
    )
    total_pages: Optional[int] = Field(
        default=None,
        description="Total number of pages"
    )
    has_next_page: bool = Field(
        default=False,
        description="Whether there is a next page"
    )


# --- Result Data Class ---

class CASSearchResult:
    """Search result from CAS with domain-compatible fields."""

    def __init__(
        self,
        numero_caso: NumeroCaso,
        titulo: str,
        fecha: Optional[FechaLaudo] = None,
        categoria_deporte: Optional[CategoriaDeporte] = None,
        tipo_procedimiento: Optional[TipoProcedimiento] = None,
        partes: Optional[str] = None,
        resumen: Optional[str] = None,
        url: Optional[URLLaudo] = None,
    ):
        self.numero_caso = numero_caso
        self.titulo = titulo
        self.fecha = fecha
        self.categoria_deporte = categoria_deporte
        self.tipo_procedimiento = tipo_procedimiento
        self.partes = partes
        self.resumen = resumen
        self.url = url


# --- Main Parser Class ---

class CASLLMParser:
    """
    LLM-powered parser for CAS jurisprudence.

    Uses Crawl4AI with JavaScript rendering to handle
    the dynamically-loaded content on jurisprudence.tas-cas.org.
    """

    BASE_URL = "https://jurisprudence.tas-cas.org"
    SEARCH_URL = BASE_URL

    EXTRACTION_INSTRUCTION = """
You are extracting arbitration awards from the Court of Arbitration for Sport (CAS/TAS) jurisprudence database.

Extract ALL cases visible on this search results page. For each case found:

1. **case_number**: The CAS case number. Format is usually:
   - CAS YYYY/A/NNNN (Appeal)
   - CAS YYYY/O/NNNN (Ordinary)
   - TAS YYYY/A/NNNN (French version)
   Example: "CAS 2024/A/9876"

2. **title**: The case title, usually the parties (e.g., "A. v. B." or "Club X v. Player Y")

3. **parties**: Names of parties involved in the case, comma separated

4. **date**: The date of the award in YYYY-MM-DD format

5. **sport**: Sport category - classify as one of:
   - football, athletics, cycling, swimming, basketball, tennis, skiing, other

6. **procedure_type**: Type of CAS procedure:
   - ordinary (first instance arbitration)
   - appeal (appeals against federation decisions)
   - anti-doping (anti-doping disputes)
   - mediation
   - advisory (advisory opinions)

7. **subject_matter**: Main legal subject:
   - doping (substance violations)
   - transfer (player transfers, training compensation)
   - eligibility (nationality, age, registration)
   - disciplinary (sanctions, suspensions)
   - contractual (employment disputes)
   - governance (federation rules)

8. **summary**: Any brief summary or description shown

9. **detail_url**: URL to view the full award document

10. **language**: Document language (en, fr, or es)

Also extract pagination:
- **total_results**: Total count of cases if shown
- **current_page**: Current page number
- **total_pages**: Total pages
- **has_next_page**: True if there's pagination showing more pages

Be thorough - extract every case listing visible on the page.
If a field is not visible or cannot be determined, use null.
"""

    def __init__(
        self,
        model: str = "openrouter/google/gemini-3-flash-preview",
        api_token: Optional[str] = None,
    ):
        """
        Initialize the CAS LLM parser.

        Args:
            model: LLM model to use (LiteLLM format)
            api_token: API token for the model provider
        """
        self.adapter = Crawl4AIAdapter(
            provider=model,
            api_token=api_token,
        )

    async def search(
        self,
        year: Optional[int] = None,
        sport: Optional[str] = None,
        query: Optional[str] = None,
        page: int = 1,
    ) -> tuple[List[CASSearchResult], bool]:
        """
        Search CAS jurisprudence and extract case results.

        CAS uses a SharePoint-based search interface that requires browser
        interaction to trigger searches. We use Playwright to click on filters.

        Args:
            year: Filter by year
            sport: Filter by sport
            query: Free text search query
            page: Page number (1-indexed)

        Returns:
            Tuple of (list of CASSearchResult, has_more_pages)
        """
        logger.info(f"Searching CAS: sport={sport}, year={year}")

        # Try Playwright-based extraction first
        try:
            return await self._search_with_playwright(sport=sport, year=year)
        except Exception as e:
            logger.warning(f"Playwright search failed: {e}")
            # Fall back to basic LLM extraction
            return await self._search_basic()

    async def _search_with_playwright(
        self,
        sport: Optional[str] = None,
        year: Optional[int] = None,
    ) -> tuple[List[CASSearchResult], bool]:
        """
        Use Playwright directly to interact with CAS Angular SPA.

        The CAS site uses Bootstrap dropdowns for filters:
        1. Find the Sport filter button by index
        2. Click to open dropdown
        3. Use search input to filter sport options
        4. Click the sport option
        5. Click Apply filters
        6. Extract cases from the results table
        """
        try:
            from playwright.async_api import async_playwright
        except ImportError:
            logger.warning("Playwright not available for CAS search")
            return [], False

        sport_filter = sport or "Football"

        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-setuid-sandbox",
                    "--disable-dev-shm-usage",
                ]
            )
            context = await browser.new_context(
                viewport={"width": 1920, "height": 1080},
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            )
            page = await context.new_page()

            try:
                await page.goto(self.SEARCH_URL, timeout=30000)
                await page.wait_for_load_state("networkidle", timeout=30000)
                await page.wait_for_timeout(3000)

                # Step 1: Find Sport button index
                buttons_info = await page.evaluate("""
                    (() => {
                        const btns = document.querySelectorAll("button.filter-button");
                        return Array.from(btns).map((btn, i) => ({
                            index: i,
                            text: btn.innerText.trim().substring(0, 30)
                        }));
                    })()
                """)

                sport_idx = None
                for b in buttons_info:
                    if "Sport" in b["text"]:
                        sport_idx = b["index"]
                        break

                if sport_idx is None:
                    logger.warning("Sport filter button not found")
                    return [], False

                # Step 2: Click Sport dropdown
                await page.click("body", position={"x": 10, "y": 10})
                await page.wait_for_timeout(500)
                sport_buttons = page.locator("button.filter-button")
                await sport_buttons.nth(sport_idx).click()
                await page.wait_for_timeout(1500)

                # Step 3: Type sport name in search to filter
                await page.evaluate(f"""
                    (() => {{
                        const btns = document.querySelectorAll("button.filter-button");
                        const sportBtn = btns[{sport_idx}];
                        const parent = sportBtn.parentElement;
                        const dropdown = parent.querySelector(".dropdown-menu");
                        if (!dropdown) return;
                        const searchInput = dropdown.querySelector("input.input-search, input[type='text']");
                        if (searchInput) {{
                            searchInput.focus();
                            searchInput.value = "{sport_filter}";
                            searchInput.dispatchEvent(new Event('input', {{ bubbles: true }}));
                        }}
                    }})()
                """)
                await page.wait_for_timeout(1000)

                # Step 4: Click the sport option
                await page.evaluate(f"""
                    (() => {{
                        const btns = document.querySelectorAll("button.filter-button");
                        const sportBtn = btns[{sport_idx}];
                        const parent = sportBtn.parentElement;
                        const dropdown = parent.querySelector(".dropdown-menu");
                        if (!dropdown) return;
                        const items = dropdown.querySelectorAll(".suggestion-button");
                        for (const item of items) {{
                            const txt = item.innerText.toLowerCase().trim();
                            if (txt === "{sport_filter.lower()}") {{
                                item.click();
                                return;
                            }}
                        }}
                        // Fallback to partial match
                        for (const item of items) {{
                            const txt = item.innerText.toLowerCase().trim();
                            if (txt.includes("{sport_filter.lower()}") && !txt.includes("american") && !txt.includes("australian")) {{
                                item.click();
                                return;
                            }}
                        }}
                    }})()
                """)
                await page.wait_for_timeout(1000)

                # Step 5: Click Apply filters
                await page.evaluate("""
                    (() => {
                        const btns = document.querySelectorAll("button");
                        for (const btn of btns) {
                            if (btn.innerText.toLowerCase().includes("apply filter")) {
                                btn.click();
                                return;
                            }
                        }
                    })()
                """)
                await page.wait_for_timeout(8000)

                try:
                    await page.wait_for_load_state("networkidle", timeout=20000)
                except:
                    pass

                # Step 6: Extract cases from table
                cases = await page.evaluate("""
                    (() => {
                        const rows = document.querySelectorAll("tr");
                        const results = [];
                        for (const row of rows) {
                            const cells = row.querySelectorAll("td");
                            if (cells.length >= 6) {
                                // Format: Lang, Year, Proc, Case number, Appellant, Respondent, Sport, Matter, Date, Outcome
                                const lang = cells[0]?.innerText?.trim();
                                const year = cells[1]?.innerText?.trim();
                                const proc = cells[2]?.innerText?.trim();
                                const caseNum = cells[3]?.innerText?.trim();
                                const appellant = cells[4]?.innerText?.trim();
                                const respondent = cells[5]?.innerText?.trim();
                                const sport = cells[6]?.innerText?.trim();
                                const matter = cells[7]?.innerText?.trim();
                                const decisionDate = cells[8]?.innerText?.trim();
                                const outcome = cells[9]?.innerText?.trim();

                                if (year && /^\\d{4}$/.test(year) && proc && /^[A-Z]$/.test(proc) && caseNum) {
                                    results.push({
                                        caseNumber: `CAS ${year}/${proc}/${caseNum}`,
                                        year: year,
                                        proc: proc,
                                        num: caseNum,
                                        appellant: appellant,
                                        respondent: respondent,
                                        sport: sport,
                                        matter: matter,
                                        decisionDate: decisionDate,
                                        outcome: outcome
                                    });
                                }
                            }
                        }
                        return results;
                    })()
                """)

                logger.info(f"CAS Playwright extraction: Found {len(cases)} cases from table")

                # Convert to domain objects
                search_results = []
                seen_cases = set()

                for case in cases:
                    case_number = case.get("caseNumber", "")
                    if not case_number or case_number in seen_cases:
                        continue
                    seen_cases.add(case_number)

                    # Apply year filter if specified
                    if year and case.get("year") != str(year):
                        continue

                    proc_type = case.get("proc", "")
                    appellant = case.get("appellant", "")
                    respondent = case.get("respondent", "")
                    parties = f"{appellant} v. {respondent}" if appellant and respondent else None

                    # Parse decision date
                    fecha = None
                    date_str = case.get("decisionDate", "")
                    if date_str:
                        try:
                            # Format: DD/MM/YYYY
                            parts = date_str.split("/")
                            if len(parts) == 3:
                                fecha = FechaLaudo(
                                    valor=date(int(parts[2]), int(parts[1]), int(parts[0]))
                                )
                        except (ValueError, IndexError):
                            pass

                    search_results.append(CASSearchResult(
                        numero_caso=NumeroCaso(valor=case_number),
                        titulo=parties or case_number,
                        fecha=fecha,
                        categoria_deporte=self._parse_sport(case.get("sport", sport_filter)),
                        tipo_procedimiento=self._parse_procedure_type(proc_type),
                        partes=parties,
                        resumen=None,
                        url=None,
                    ))

                return search_results, len(search_results) >= 20

            except Exception as e:
                logger.error(f"CAS Playwright interaction error: {e}")
                import traceback
                traceback.print_exc()
                return [], False

            finally:
                await context.close()
                await browser.close()

    def _parse_sport(self, sport: str) -> Optional[CategoriaDeporte]:
        """Parse sport string to enum."""
        sport_map = {
            "football": CategoriaDeporte.FUTBOL,
            "athletics": CategoriaDeporte.ATLETISMO,
            "cycling": CategoriaDeporte.CICLISMO,
            "swimming": CategoriaDeporte.NATACION,
            "basketball": CategoriaDeporte.BALONCESTO,
            "tennis": CategoriaDeporte.TENIS,
            "skiing": CategoriaDeporte.ESQUI,
        }
        return sport_map.get(sport.lower(), CategoriaDeporte.OTRO)

    def _parse_procedure_type(self, case_type: str) -> Optional[TipoProcedimiento]:
        """Parse case type letter to procedure type."""
        type_map = {
            "A": TipoProcedimiento.ARBITRAJE_APELACION,
            "O": TipoProcedimiento.ARBITRAJE_ORDINARIO,
        }
        return type_map.get(case_type.upper())

    async def _search_basic(self) -> tuple[List[CASSearchResult], bool]:
        """
        Basic LLM extraction fallback (likely won't find cases but serves as fallback).
        """
        result = await self.adapter.extract_structured(
            url=self.SEARCH_URL,
            schema=ExtractedCASSearchResults,
            instruction=self.EXTRACTION_INSTRUCTION,
            wait_for_js=True,
            delay_seconds=5.0,
        )

        if not result or not result.cases:
            logger.warning("Basic CAS search found no cases")
            return [], False

        search_results = []
        for case in result.cases:
            try:
                domain_result = self._to_domain_result(case)
                if domain_result:
                    search_results.append(domain_result)
            except Exception as e:
                logger.warning(f"Error converting case: {e}")

        return search_results, result.has_next_page

    async def search_multiple_pages(
        self,
        year: Optional[int] = None,
        sport: Optional[str] = None,
        query: Optional[str] = None,
        max_pages: int = 5,
        max_results: Optional[int] = None,
    ) -> List[CASSearchResult]:
        """
        Search CAS across multiple pages.

        Args:
            year: Filter by year
            sport: Filter by sport
            query: Free text search query
            max_pages: Maximum pages to fetch
            max_results: Maximum total results to return

        Returns:
            List of CASSearchResult objects
        """
        all_results = []

        for page in range(1, max_pages + 1):
            results, has_next = await self.search(
                year=year,
                sport=sport,
                query=query,
                page=page,
            )

            all_results.extend(results)

            # Check limits
            if max_results and len(all_results) >= max_results:
                all_results = all_results[:max_results]
                break

            if not has_next or not results:
                break

        return all_results

    def _to_domain_result(self, case: ExtractedCASCase) -> Optional[CASSearchResult]:
        """Convert extracted case to domain result."""
        if not case.case_number:
            return None

        # Parse case number
        numero_caso = NumeroCaso(valor=case.case_number)

        # Parse date
        fecha = None
        if case.date:
            try:
                parts = case.date.split("-")
                if len(parts) == 3:
                    fecha = FechaLaudo(
                        valor=date(int(parts[0]), int(parts[1]), int(parts[2]))
                    )
            except (ValueError, IndexError):
                pass

        # Map sport category
        categoria_deporte = None
        if case.sport:
            sport_map = {
                "football": CategoriaDeporte.FUTBOL,
                "futbol": CategoriaDeporte.FUTBOL,
                "athletics": CategoriaDeporte.ATLETISMO,
                "cycling": CategoriaDeporte.CICLISMO,
                "swimming": CategoriaDeporte.NATACION,
                "basketball": CategoriaDeporte.BALONCESTO,
                "tennis": CategoriaDeporte.TENIS,
                "skiing": CategoriaDeporte.ESQUI,
            }
            categoria_deporte = sport_map.get(
                case.sport.lower(), CategoriaDeporte.OTRO
            )

        # Map procedure type
        tipo_procedimiento = None
        if case.procedure_type:
            proc_map = {
                "ordinary": TipoProcedimiento.ARBITRAJE_ORDINARIO,
                "appeal": TipoProcedimiento.ARBITRAJE_APELACION,
                "anti-doping": TipoProcedimiento.ARBITRAJE_ANTIDOPAJE,
                "mediation": TipoProcedimiento.MEDIACION,
                "advisory": TipoProcedimiento.OPINION_CONSULTIVA,
            }
            tipo_procedimiento = proc_map.get(case.procedure_type.lower())

        # Parse URL
        url = None
        if case.detail_url:
            url = URLLaudo(valor=case.detail_url)

        # Title fallback to case number if not provided
        titulo = case.title or case.parties or case.case_number

        return CASSearchResult(
            numero_caso=numero_caso,
            titulo=titulo,
            fecha=fecha,
            categoria_deporte=categoria_deporte,
            tipo_procedimiento=tipo_procedimiento,
            partes=case.parties,
            resumen=case.summary,
            url=url,
        )
