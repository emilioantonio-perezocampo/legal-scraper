"""
SCJN Search Parser Adapter

Parses search result HTML from SCJN legislacion.scjn.gob.mx.
Extracts document listings with metadata and pagination info.
"""
import re
from dataclasses import dataclass
from typing import List, Optional, Tuple
from urllib.parse import parse_qs, urlparse

from bs4 import BeautifulSoup

from .errors import ParseError


@dataclass(frozen=True)
class SearchResultItem:
    """
    Parsed search result row.

    Represents a single document found in search results.
    """
    q_param: str
    title: str
    category: str
    publication_date: Optional[str]  # Raw string, domain will parse
    expedition_date: Optional[str]
    status: str
    scope: str
    has_pdf: bool
    has_extract: bool


def parse_search_results(html: str) -> List[SearchResultItem]:
    """
    Parse SCJN search results page HTML.

    Args:
        html: Raw HTML from Buscar.aspx

    Returns:
        List of SearchResultItem, empty list if no results

    Raises:
        ParseError: If HTML structure is unrecognizable
    """
    soup = BeautifulSoup(html, 'lxml')

    # Find the results grid
    grid = soup.find(id='gridResultados')
    if grid is None:
        raise ParseError(
            "Could not find results grid (id='gridResultados')",
            html[:200] if html else ""
        )

    # Check for "no results" message
    empty_msg = grid.find(class_='dxgvEmptyDataRow')
    if empty_msg and 'no se encontraron' in empty_msg.get_text().lower():
        return []

    # Find all data rows
    rows = grid.find_all('tr', class_='dxgvDataRow')
    if not rows:
        # Try alternative selectors
        table = grid.find('table', class_='dxgvTable')
        if table:
            rows = table.find_all('tr', class_='dxgvDataRow')

    if not rows:
        # No results found, but structure is valid
        return []

    results = []
    for row in rows:
        try:
            item = _parse_result_row(row)
            if item:
                results.append(item)
        except Exception as e:
            # Log but continue parsing other rows
            continue

    return results


def _parse_result_row(row) -> Optional[SearchResultItem]:
    """
    Parse a single result row.

    Args:
        row: BeautifulSoup tr element

    Returns:
        SearchResultItem or None if parsing fails
    """
    cells = row.find_all('td')
    if len(cells) < 6:
        return None

    # Extract title and q_param from link
    title_link = cells[0].find('a', href=lambda h: h and 'wfOrdenamientoDetalle' in h)
    if not title_link:
        return None

    title = title_link.get_text(strip=True)
    q_param = _extract_q_param(title_link.get('href', ''))

    if not q_param:
        return None

    # Extract dates
    publication_date = cells[1].get_text(strip=True) if len(cells) > 1 else None
    expedition_date = cells[2].get_text(strip=True) if len(cells) > 2 else None

    # Extract status
    status = cells[3].get_text(strip=True) if len(cells) > 3 else "UNKNOWN"

    # Extract category
    category = cells[4].get_text(strip=True) if len(cells) > 4 else "UNKNOWN"

    # Extract scope
    scope = cells[5].get_text(strip=True) if len(cells) > 5 else "FEDERAL"

    # Check for extract and PDF links
    has_extract = _has_link(row, 'wfExtracto')
    has_pdf = _has_link(row, 'AbrirDocReforma')

    return SearchResultItem(
        q_param=q_param,
        title=title,
        category=category,
        publication_date=publication_date if publication_date else None,
        expedition_date=expedition_date if expedition_date else None,
        status=status,
        scope=scope,
        has_pdf=has_pdf,
        has_extract=has_extract,
    )


def _extract_q_param(href: str) -> Optional[str]:
    """
    Extract q parameter from URL.

    Args:
        href: URL string like "wfOrdenamientoDetalle.aspx?q=abc123"

    Returns:
        q parameter value or None
    """
    if not href:
        return None

    # Try parsing as URL
    try:
        parsed = urlparse(href)
        params = parse_qs(parsed.query)
        if 'q' in params:
            return params['q'][0]
    except Exception:
        pass

    # Fallback: regex extraction
    match = re.search(r'[?&]q=([^&]+)', href)
    if match:
        return match.group(1)

    return None


def _has_link(element, pattern: str) -> bool:
    """
    Check if element contains a link matching pattern.

    Args:
        element: BeautifulSoup element
        pattern: Pattern to search for in href

    Returns:
        True if matching link found
    """
    links = element.find_all('a', href=lambda h: h and pattern in h)
    return len(links) > 0


def extract_pagination_info(html: str) -> Tuple[int, int, Optional[str]]:
    """
    Extract pagination details from search results.

    Args:
        html: Raw HTML from search results page

    Returns:
        Tuple of (current_page, total_pages, next_page_callback)
        next_page_callback is None if on last page
    """
    soup = BeautifulSoup(html, 'lxml')

    current_page = 1
    total_pages = 1
    next_callback = None

    # Look for pagination info
    grid = soup.find(id='gridResultados')
    if not grid:
        return current_page, total_pages, next_callback

    # Find "Página X de Y" text
    pager_total = grid.find(class_='dxpPagerTotal')
    if pager_total:
        text = pager_total.get_text(strip=True)
        match = re.search(r'P[aá]gina\s+(\d+)\s+de\s+(\d+)', text, re.IGNORECASE)
        if match:
            current_page = int(match.group(1))
            total_pages = int(match.group(2))

    # Determine if there's a next page
    if current_page < total_pages:
        # Look for next page link/callback
        pager_items = grid.find_all(class_='dxpPagerItem')
        for item in pager_items:
            onclick = item.get('onclick', '')
            if onclick and str(current_page + 1) in item.get_text():
                next_callback = onclick
                break

    return current_page, total_pages, next_callback
