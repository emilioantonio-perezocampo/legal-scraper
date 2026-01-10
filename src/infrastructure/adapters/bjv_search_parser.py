"""
BJV search results parser.

Parses BJV search results HTML to extract book information
and pagination data.
"""
import re
from dataclasses import dataclass
from typing import Optional, Tuple
from bs4 import BeautifulSoup

from src.domain.bjv_value_objects import IdentificadorLibro, TipoContenido
from src.infrastructure.adapters.bjv_errors import ParseError


@dataclass(frozen=True)
class SearchResultadoBusqueda:
    """Search result from BJV search page."""

    libro_id: IdentificadorLibro
    titulo: str
    relevancia: float
    tipo_contenido: TipoContenido
    autores_texto: Optional[str] = None
    anio: Optional[int] = None
    fragmento_coincidente: Optional[str] = None


@dataclass(frozen=True)
class PaginacionInfo:
    """Pagination information from search results."""

    pagina_actual: int
    total_paginas: int
    total_resultados: int


class BJVSearchParser:
    """Parser for BJV search results HTML."""

    BASE_URL = "https://biblio.juridicas.unam.mx"

    # Patterns for extracting libro ID
    LIBRO_ID_PATTERNS = [
        re.compile(r"/detalle-libro/(\d+)"),
        re.compile(r"/detalle/(\d+)"),
        re.compile(r"id-libro=(\d+)"),
    ]

    # Pattern for extracting year from text
    YEAR_PATTERN = re.compile(r"\((\d{4})\)")
    YEAR_STANDALONE_PATTERN = re.compile(r"\b(19\d{2}|20\d{2})\b")

    def parse_search_results(self, html: str) -> Tuple[SearchResultadoBusqueda, ...]:
        """
        Parse search results HTML to extract results.

        Args:
            html: Raw HTML from BJV search page

        Returns:
            Tuple of SearchResultadoBusqueda objects

        Raises:
            ParseError: If HTML is empty or invalid
        """
        if html is None or html.strip() == "":
            raise ParseError("HTML vacío o nulo proporcionado", html_sample=str(html)[:100] if html else "")

        soup = BeautifulSoup(html, "html.parser")

        # Find result items
        result_items = soup.find_all("div", class_="resultado-item")

        results = []
        for item in result_items:
            result = self._parse_result_item(item)
            if result:
                results.append(result)

        return tuple(results)

    def _parse_result_item(self, item) -> Optional[SearchResultadoBusqueda]:
        """Parse a single result item."""
        # Find title link
        title_link = item.find("h3")
        if not title_link:
            return None

        anchor = title_link.find("a")
        if not anchor:
            return None

        titulo = anchor.get_text(strip=True)
        if not titulo:
            return None

        href = anchor.get("href", "")

        # Extract libro ID
        libro_id = self._extract_libro_id(href)
        if not libro_id:
            return None

        # Extract tipo_contenido
        tipo_contenido = self._detect_tipo_contenido(href)

        # Extract autores
        autores_texto = self._extract_autores(item)

        # Extract year
        anio = self._extract_anio(item, titulo)

        # Extract snippet
        fragmento = self._extract_fragmento(item)

        return SearchResultadoBusqueda(
            libro_id=IdentificadorLibro(bjv_id=libro_id),
            titulo=titulo,
            relevancia=1.0,
            tipo_contenido=tipo_contenido,
            autores_texto=autores_texto,
            anio=anio,
            fragmento_coincidente=fragmento,
        )

    def _extract_libro_id(self, href: str) -> Optional[str]:
        """Extract libro ID from URL."""
        for pattern in self.LIBRO_ID_PATTERNS:
            match = pattern.search(href)
            if match:
                return match.group(1)
        return None

    def _detect_tipo_contenido(self, href: str) -> TipoContenido:
        """Detect content type from URL."""
        if "capitulo" in href.lower():
            return TipoContenido.CAPITULO
        if "revista" in href.lower():
            return TipoContenido.REVISTA
        if "articulo" in href.lower():
            return TipoContenido.ARTICULO
        return TipoContenido.LIBRO

    def _extract_autores(self, item) -> Optional[str]:
        """Extract authors text from result item."""
        # Try various author selectors
        for selector in ["autor", "meta-author", "authors"]:
            element = item.find(class_=selector)
            if element:
                text = element.get_text(strip=True)
                if text:
                    return text

        # Try direct p.autor or span.autor
        autor_p = item.find("p", class_="autor")
        if autor_p:
            return autor_p.get_text(strip=True)

        autor_span = item.find("span", class_="meta-author")
        if autor_span:
            return autor_span.get_text(strip=True)

        return None

    def _extract_anio(self, item, titulo: str) -> Optional[int]:
        """Extract publication year from result item or title."""
        # Try anio class
        anio_elem = item.find(class_="anio")
        if anio_elem:
            text = anio_elem.get_text(strip=True)
            try:
                return int(text)
            except ValueError:
                pass

        # Try year class
        year_elem = item.find(class_="year")
        if year_elem:
            text = year_elem.get_text(strip=True)
            try:
                return int(text)
            except ValueError:
                pass

        # Try parenthetical format in title
        match = self.YEAR_PATTERN.search(titulo)
        if match:
            return int(match.group(1))

        # Try standalone year in item text
        item_text = item.get_text()
        match = self.YEAR_STANDALONE_PATTERN.search(item_text)
        if match:
            return int(match.group(1))

        return None

    def _extract_fragmento(self, item) -> Optional[str]:
        """Extract matching text fragment/snippet."""
        for selector in ["snippet", "excerpt", "fragment", "resumen"]:
            element = item.find(class_=selector)
            if element:
                return element.get_text(strip=True)

        snippet = item.find("p", class_="snippet")
        if snippet:
            return snippet.get_text(strip=True)

        return None

    def extract_pagination_info(self, html: str) -> PaginacionInfo:
        """
        Extract pagination information from search results HTML.

        Args:
            html: Raw HTML from BJV search page

        Returns:
            PaginacionInfo with current page, total pages, and total results
        """
        if not html or html.strip() == "":
            return PaginacionInfo(
                pagina_actual=1,
                total_paginas=1,
                total_resultados=0,
            )

        soup = BeautifulSoup(html, "html.parser")

        pagina_actual = self._extract_pagina_actual(soup)
        total_paginas = self._extract_total_paginas(soup)
        total_resultados = self._extract_total_resultados(soup)

        return PaginacionInfo(
            pagina_actual=pagina_actual,
            total_paginas=max(total_paginas, pagina_actual),
            total_resultados=total_resultados,
        )

    def _extract_pagina_actual(self, soup) -> int:
        """Extract current page number."""
        # Try .current class in pagination
        current = soup.find(class_="current")
        if current:
            text = current.get_text(strip=True)
            try:
                return int(text)
            except ValueError:
                pass

        # Try .active class in pagination
        active = soup.find("li", class_="active")
        if active:
            link = active.find("a")
            if link:
                text = link.get_text(strip=True)
                try:
                    return int(text)
                except ValueError:
                    pass

        return 1

    def _extract_total_paginas(self, soup) -> int:
        """Extract total number of pages."""
        # Find all pagination links and get the highest number
        pagination = soup.find(class_="pagination") or soup.find("nav", class_="pagination")
        if not pagination:
            pagination = soup.find("ul", class_="pagination")

        if pagination:
            links = pagination.find_all("a")
            max_page = 1
            for link in links:
                text = link.get_text(strip=True)
                try:
                    page_num = int(text)
                    max_page = max(max_page, page_num)
                except ValueError:
                    pass
            return max_page

        return 1

    def _extract_total_resultados(self, soup) -> int:
        """Extract total number of results."""
        # Try .total-results class
        total_elem = soup.find(class_="total-results")
        if total_elem:
            text = total_elem.get_text()
            match = re.search(r"(\d+)", text)
            if match:
                return int(match.group(1))

        # Try "de X resultados" pattern
        text = soup.get_text()
        match = re.search(r"de\s+(\d+)\s+resultados", text, re.IGNORECASE)
        if match:
            return int(match.group(1))

        # Try "encontraron X resultados" pattern
        match = re.search(r"encontraron\s+(\d+)\s+resultados", text, re.IGNORECASE)
        if match:
            return int(match.group(1))

        return 0
