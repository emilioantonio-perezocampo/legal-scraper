"""
SCJN Document Parser Adapter

Parses document detail HTML from SCJN legislacion.scjn.gob.mx.
Extracts document metadata, articles, and reform history.
"""
import re
from dataclasses import dataclass
from typing import List, Optional
from urllib.parse import parse_qs, urlparse

from bs4 import BeautifulSoup

from .errors import ParseError


@dataclass(frozen=True)
class ArticleResult:
    """
    Parsed article from a legal document.

    Represents a single article within a document.
    """
    number: str
    title: str
    content: str
    is_transitory: bool = False


@dataclass(frozen=True)
class ReformResult:
    """
    Parsed reform/amendment reference.

    Represents a modification to a legal document.
    """
    q_param: str
    title: str
    publication_date: Optional[str]
    gazette_reference: str
    has_pdf: bool


@dataclass(frozen=True)
class DocumentDetailResult:
    """
    Parsed document detail page result.

    Contains all extracted information from a document detail page.
    """
    title: str
    short_title: str
    category: str
    scope: str
    status: str
    publication_date: Optional[str]
    expedition_date: Optional[str]
    full_text: str
    article_count: int


def parse_document_detail(html: str) -> DocumentDetailResult:
    """
    Parse SCJN document detail page HTML.

    Args:
        html: Raw HTML from wfOrdenamientoDetalle.aspx

    Returns:
        DocumentDetailResult with extracted metadata

    Raises:
        ParseError: If HTML structure is unrecognizable
    """
    soup = BeautifulSoup(html, 'lxml')

    # Find the main container
    container = soup.find(id='contenedor')
    if container is None:
        raise ParseError(
            "Could not find document container (id='contenedor')",
            html[:200] if html else ""
        )

    # Extract title
    title_elem = container.find(class_='titulo-ordenamiento')
    title = title_elem.get_text(strip=True) if title_elem else ""

    # Extract metadata from datos-ordenamiento
    datos = container.find(class_='datos-ordenamiento')
    metadata = _extract_metadata(datos) if datos else {}

    # Extract full text content
    contenido = container.find(id='contenido-ordenamiento')
    full_text = contenido.get_text(separator='\n', strip=True) if contenido else ""

    # Count articles
    articles = parse_articles(str(contenido)) if contenido else []
    article_count = len(articles)

    # Generate short title from title
    short_title = _generate_short_title(title)

    return DocumentDetailResult(
        title=title,
        short_title=short_title,
        category=metadata.get('category', ''),
        scope=metadata.get('scope', ''),
        status=metadata.get('status', ''),
        publication_date=metadata.get('publication_date'),
        expedition_date=metadata.get('expedition_date'),
        full_text=full_text,
        article_count=article_count,
    )


def _extract_metadata(datos_elem) -> dict:
    """
    Extract metadata fields from datos-ordenamiento element.

    Args:
        datos_elem: BeautifulSoup element containing metadata

    Returns:
        Dictionary with extracted metadata
    """
    metadata = {}

    # Map Spanish labels to internal keys
    label_map = {
        'tipo de ordenamiento': 'category',
        'ámbito': 'scope',
        'ambito': 'scope',
        'estatus': 'status',
        'fecha de publicación': 'publication_date',
        'fecha de publicacion': 'publication_date',
        'fecha de expedición': 'expedition_date',
        'fecha de expedicion': 'expedition_date',
    }

    # Find all dato elements
    datos = datos_elem.find_all(class_='dato')
    for dato in datos:
        etiqueta = dato.find(class_='etiqueta')
        valor = dato.find(class_='valor')

        if etiqueta and valor:
            label = etiqueta.get_text(strip=True).lower().rstrip(':')
            value = valor.get_text(strip=True)

            # Map to internal key
            for spanish_label, key in label_map.items():
                if spanish_label in label:
                    metadata[key] = value if value else None
                    break

    return metadata


def _generate_short_title(title: str) -> str:
    """
    Generate a short title/abbreviation from full title.

    Args:
        title: Full document title

    Returns:
        Abbreviated title (e.g., "LFT" for "LEY FEDERAL DEL TRABAJO")
    """
    if not title:
        return ""

    # Remove common prefixes and get initials
    words = title.upper().split()
    # Filter out common Spanish articles and prepositions
    skip_words = {'DE', 'DEL', 'LA', 'LAS', 'LOS', 'EL', 'EN', 'Y', 'A', 'PARA', 'POR', 'CON'}

    initials = []
    for word in words:
        # Clean the word
        clean_word = re.sub(r'[^A-ZÁÉÍÓÚÑÜ]', '', word)
        if clean_word and clean_word not in skip_words:
            initials.append(clean_word[0])

    return ''.join(initials)


def parse_articles(html: str) -> List[ArticleResult]:
    """
    Parse articles from document content HTML.

    Args:
        html: HTML containing article elements

    Returns:
        List of ArticleResult, empty if no articles found
    """
    soup = BeautifulSoup(html, 'lxml')

    # Find all article elements
    article_elems = soup.find_all(class_='articulo')
    if not article_elems:
        # Try alternative: find divs with id starting with 'art'
        article_elems = soup.find_all('div', id=re.compile(r'^art', re.IGNORECASE))

    articles = []
    for elem in article_elems:
        article = _parse_article_element(elem)
        if article:
            articles.append(article)

    return articles


def _parse_article_element(elem) -> Optional[ArticleResult]:
    """
    Parse a single article element.

    Args:
        elem: BeautifulSoup element for an article

    Returns:
        ArticleResult or None if parsing fails
    """
    # Check if it's a transitory article
    classes = elem.get('class', [])
    is_transitory = 'transitorio' in classes or 'transitorio' in str(classes).lower()

    # Also check the text content for "TRANSITORIO"
    text_content = elem.get_text()
    if 'TRANSITORIO' in text_content.upper():
        is_transitory = True

    # Extract title (usually in h3 or h4)
    title_elem = elem.find(['h3', 'h4', 'h2'])
    title = title_elem.get_text(strip=True) if title_elem else ""

    # Extract article number from title
    number = _extract_article_number(title, is_transitory)

    # Extract content (paragraph text)
    content_parts = []
    for p in elem.find_all('p'):
        text = p.get_text(strip=True)
        if text:
            content_parts.append(text)

    # If no p tags, get all text except title
    if not content_parts:
        content = elem.get_text(strip=True)
        if title and content.startswith(title):
            content = content[len(title):].strip()
        content_parts = [content] if content else []

    content = '\n'.join(content_parts)

    if not number and not content:
        return None

    return ArticleResult(
        number=number,
        title=title,
        content=content,
        is_transitory=is_transitory,
    )


def _extract_article_number(title: str, is_transitory: bool) -> str:
    """
    Extract article number from title.

    Args:
        title: Article title string
        is_transitory: Whether this is a transitory article

    Returns:
        Article number string (e.g., "1", "2 Bis", "PRIMERO")
    """
    if not title:
        return ""

    # Handle transitory articles
    if is_transitory:
        # Look for ordinal numbers
        ordinal_match = re.search(
            r'(?:TRANSITORIO\s+)?(PRIMERO|SEGUNDO|TERCERO|CUARTO|QUINTO|'
            r'SEXTO|SÉPTIMO|SEPTIMO|OCTAVO|NOVENO|DÉCIMO|DECIMO|\d+)',
            title.upper()
        )
        if ordinal_match:
            return ordinal_match.group(1)

    # Regular articles - look for "Artículo X" pattern
    # Handle various formats: "1", "1°", "1 Bis", "123-A"
    match = re.search(
        r'[Aa]rt[ií]culo\s+(\d+(?:\s*[°º])?(?:\s+[Bb]is)?(?:\s+[A-Z])?|\d+-[A-Z])',
        title
    )
    if match:
        return match.group(1).strip()

    # Fallback: try to find any number
    num_match = re.search(r'(\d+)', title)
    if num_match:
        return num_match.group(1)

    return ""


def parse_reforms(html: str) -> List[ReformResult]:
    """
    Parse reform/amendment references from document HTML.

    Args:
        html: HTML containing reform table

    Returns:
        List of ReformResult, empty if no reforms found
    """
    soup = BeautifulSoup(html, 'lxml')

    # Find reforms section
    reforms_section = soup.find(id='reformas')
    if not reforms_section:
        return []

    # Find reform rows
    rows = reforms_section.find_all('tr', class_='reforma-row')
    if not rows:
        # Try alternative: find any tr in reforma table
        tabla = reforms_section.find(class_='tabla-reformas')
        if tabla:
            rows = tabla.find_all('tr')

    reforms = []
    for row in rows:
        reform = _parse_reform_row(row)
        if reform:
            reforms.append(reform)

    return reforms


def _parse_reform_row(row) -> Optional[ReformResult]:
    """
    Parse a single reform row.

    Args:
        row: BeautifulSoup tr element

    Returns:
        ReformResult or None if parsing fails
    """
    cells = row.find_all('td')
    if not cells:
        return None

    # Extract q_param and title from first cell link
    link = row.find('a', href=lambda h: h and 'wfOrdenamientoDetalle' in h)
    if not link:
        return None

    title = link.get_text(strip=True)
    q_param = _extract_q_param(link.get('href', ''))

    if not q_param:
        return None

    # Extract date from second cell
    publication_date = cells[1].get_text(strip=True) if len(cells) > 1 else None
    if publication_date == "":
        publication_date = None

    # Extract gazette reference from third cell
    gazette_reference = cells[2].get_text(strip=True) if len(cells) > 2 else ""

    # Check for PDF link
    has_pdf = _has_link(row, 'AbrirDocReforma')

    return ReformResult(
        q_param=q_param,
        title=title,
        publication_date=publication_date,
        gazette_reference=gazette_reference,
        has_pdf=has_pdf,
    )


def _extract_q_param(href: str) -> Optional[str]:
    """
    Extract q parameter from URL.

    Args:
        href: URL string

    Returns:
        q parameter value or None
    """
    if not href:
        return None

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
