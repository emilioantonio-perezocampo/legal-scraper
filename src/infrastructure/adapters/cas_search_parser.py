"""
Parser for CAS search results.

Extracts award listings from rendered HTML.
Works with the JavaScript-rendered content from CASBrowserAdapter.
"""
from dataclasses import dataclass
from typing import Tuple, Optional, List
from bs4 import BeautifulSoup
import re

from src.domain.cas_value_objects import NumeroCaso
from src.domain.cas_entities import ResultadoBusquedaCAS
from src.infrastructure.adapters.cas_errors import ParseError


@dataclass(frozen=True)
class PaginacionInfoCAS:
    """Pagination information from CAS search results."""
    pagina_actual: int
    total_paginas: int
    total_resultados: int
    resultados_por_pagina: int

    @property
    def tiene_siguiente(self) -> bool:
        return self.pagina_actual < self.total_paginas

    @property
    def tiene_anterior(self) -> bool:
        return self.pagina_actual > 1


def parse_search_results(html: str) -> Tuple[ResultadoBusquedaCAS, ...]:
    """
    Parse CAS search results page.

    Args:
        html: Rendered HTML from browser adapter

    Returns:
        Tuple of ResultadoBusquedaCAS

    Raises:
        ParseError: If HTML is empty
    """
    if not html or not html.strip():
        raise ParseError("HTML vacío recibido")

    soup = BeautifulSoup(html, 'html.parser')
    resultados: List[ResultadoBusquedaCAS] = []

    # Try multiple selector strategies for the SPA
    items = _find_result_items(soup)

    for item in items:
        try:
            resultado = _parse_resultado_item(item)
            if resultado:
                resultados.append(resultado)
        except (ValueError, AttributeError):
            # Skip malformed items
            continue

    return tuple(resultados)


def _find_result_items(soup: BeautifulSoup) -> List:
    """Find result items using multiple selector strategies."""
    # Strategy 1: Common class names
    selectors = [
        '.case-item',
        '.award-item',
        '.result-item',
        '[data-case-id]',
        '[data-award-id]',
        'tr.case-row',
        'div.case-card',
        'article.case',
    ]

    for selector in selectors:
        items = soup.select(selector)
        if items:
            return items

    # Strategy 2: Look for case number patterns in any element
    all_elements = soup.find_all(['div', 'tr', 'li', 'article'])
    case_pattern = re.compile(r'(CAS|TAS)\s*\d{4}/[A-Z]+/\d+', re.IGNORECASE)

    items_with_cases = []
    for elem in all_elements:
        text = elem.get_text()
        if case_pattern.search(text):
            # Avoid duplicates from nested elements
            if not any(elem in parent.descendants for parent in items_with_cases):
                items_with_cases.append(elem)

    return items_with_cases[:50]  # Limit to avoid performance issues


def _parse_resultado_item(item) -> Optional[ResultadoBusquedaCAS]:
    """Parse a single search result item."""
    text = item.get_text(separator=" ", strip=True)

    # Extract case number (required)
    case_pattern = re.compile(r'(CAS|TAS)\s*(\d{4})/([A-Z]+)/(\d+)', re.IGNORECASE)
    match = case_pattern.search(text)

    if not match:
        return None

    numero_caso_str = f"{match.group(1).upper()} {match.group(2)}/{match.group(3).upper()}/{match.group(4)}"
    numero_caso = NumeroCaso(valor=numero_caso_str)

    # Extract title/parties
    titulo = _extract_titulo(item, text, numero_caso_str)

    return ResultadoBusquedaCAS(
        numero_caso=numero_caso,
        titulo=titulo,
        relevancia=1.0,  # Default relevance
    )


def _extract_titulo(item, text: str, numero_caso: str) -> str:
    """Extract title (usually parties: X v. Y)."""
    # Look for "v." or "vs" pattern
    vs_pattern = re.compile(r'([A-Za-z][^v]{2,50})\s+v\.?\s+([A-Za-z][^v]{2,50})', re.IGNORECASE)

    # Try to find in link text first
    link = item.select_one('a')
    if link:
        link_text = link.get_text(strip=True)
        match = vs_pattern.search(link_text)
        if match:
            return f"{match.group(1).strip()} v. {match.group(2).strip()}"

    # Try to find in full text
    match = vs_pattern.search(text)
    if match:
        # Clean up the match
        party1 = match.group(1).strip()[:100]
        party2 = match.group(2).strip()[:100]
        return f"{party1} v. {party2}"

    # Fallback to case number
    return numero_caso


def extract_pagination_info(html: str) -> PaginacionInfoCAS:
    """
    Extract pagination information from search results.

    Args:
        html: Rendered HTML from browser adapter

    Returns:
        PaginacionInfoCAS with pagination details
    """
    soup = BeautifulSoup(html, 'html.parser')

    pagina_actual = 1
    total_paginas = 1
    total_resultados = 0
    resultados_por_pagina = 20

    # Look for pagination container
    paginacion = soup.select_one('.pagination, .pager, nav[aria-label*="pagination"]')

    if paginacion:
        # Current page
        activa = paginacion.select_one('.active, .current, [aria-current="page"]')
        if activa:
            try:
                pagina_actual = int(activa.get_text(strip=True))
            except ValueError:
                pass

        # Total pages from last page link
        page_links = paginacion.select('a[href], button')
        for link in page_links:
            try:
                num = int(link.get_text(strip=True))
                total_paginas = max(total_paginas, num)
            except ValueError:
                continue

    # Total results count
    count_selectors = [
        '.results-count',
        '.total-results',
        '[data-total]',
        '.showing',
    ]

    for selector in count_selectors:
        elem = soup.select_one(selector)
        if elem:
            text = elem.get_text()
            numbers = re.findall(r'\d+', text)
            if numbers:
                total_resultados = int(numbers[-1])  # Usually last number is total
                break

    return PaginacionInfoCAS(
        pagina_actual=pagina_actual,
        total_paginas=total_paginas,
        total_resultados=total_resultados,
        resultados_por_pagina=resultados_por_pagina,
    )


def extract_case_numbers_from_text(text: str) -> Tuple[str, ...]:
    """
    Extract all CAS case numbers from text.

    Useful for finding cited cases within award text.

    Args:
        text: Any text that might contain case references

    Returns:
        Tuple of case number strings
    """
    if not text:
        return ()

    pattern = re.compile(r'(CAS|TAS)\s*\d{4}/[A-Z]+/\d+', re.IGNORECASE)

    # Normalize and deduplicate
    seen = set()
    result = []

    for match in re.finditer(pattern, text):
        caso = match.group(0).upper()
        # Normalize spacing
        caso = re.sub(r'\s+', ' ', caso)
        if caso not in seen:
            seen.add(caso)
            result.append(caso)

    return tuple(result)
