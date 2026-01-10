"""
BJV libro detail page parser.

Parses BJV book detail HTML to create LibroBJV entities.
"""
import re
from typing import Optional, Tuple
from bs4 import BeautifulSoup

from src.domain.bjv_value_objects import (
    IdentificadorLibro,
    ISBN,
    AnioPublicacion,
    URLArchivo,
    AreaDerecho,
)
from src.domain.bjv_entities import (
    LibroBJV,
    Autor,
    Editorial,
    CapituloLibro,
)
from src.infrastructure.adapters.bjv_errors import ParseError


class BJVLibroParser:
    """Parser for BJV book detail pages."""

    BASE_URL = "https://biblio.juridicas.unam.mx"

    # Patterns for extraction
    YEAR_PATTERN = re.compile(r"\b(19\d{2}|20\d{2})\b")
    PAGE_RANGE_PATTERN = re.compile(r"(\d+)\s*[-–]\s*(\d+)")
    PAGES_TOTAL_PATTERN = re.compile(r"(\d+)\s*páginas?", re.IGNORECASE)
    ISBN_PATTERN = re.compile(r"(?:ISBN[:\s]*)?(\d{3}[-\s]?\d{1,5}[-\s]?\d{2,7}[-\s]?\d{1,7}[-\s]?\d{1})")

    # Area detection mappings
    AREA_KEYWORDS = {
        AreaDerecho.CIVIL: ["civil", "obligaciones", "contratos", "familia"],
        AreaDerecho.PENAL: ["penal", "criminal", "delito", "punitivo"],
        AreaDerecho.CONSTITUCIONAL: ["constitucional", "constitución", "derechos humanos"],
        AreaDerecho.ADMINISTRATIVO: ["administrativo", "administración pública"],
        AreaDerecho.MERCANTIL: ["mercantil", "comercial", "societario"],
        AreaDerecho.LABORAL: ["laboral", "trabajo", "empleo"],
        AreaDerecho.FISCAL: ["fiscal", "tributario", "impuestos"],
        AreaDerecho.INTERNACIONAL: ["internacional", "tratados"],
    }

    def parse_libro_detalle(self, html: str, libro_id: str) -> LibroBJV:
        """
        Parse book detail HTML to create LibroBJV entity.

        Args:
            html: Raw HTML from BJV book detail page
            libro_id: Book ID from URL

        Returns:
            LibroBJV entity with extracted data

        Raises:
            ParseError: If HTML is empty or missing required title
        """
        if html is None or html.strip() == "":
            raise ParseError("HTML vacío o nulo proporcionado", html_sample=str(html)[:100] if html else "")

        soup = BeautifulSoup(html, "html.parser")

        # Extract title (required)
        titulo = self._extract_titulo(soup)
        if not titulo:
            raise ParseError("No se encontró el título del libro", html_sample=html[:500])

        # Extract optional fields
        subtitulo = self._extract_subtitulo(soup)
        autores = self._extract_autores(soup)
        editores = self._extract_editores(soup)
        editorial = self._extract_editorial(soup)
        isbn = self._extract_isbn(soup)
        anio = self._extract_anio(soup)
        area_derecho = self._extract_area_derecho(soup)
        resumen = self._extract_resumen(soup)
        palabras_clave = self._extract_palabras_clave(soup)
        capitulos = self._extract_capitulos(soup)
        url_pdf = self._extract_pdf_url(soup)
        portada_url = self._extract_portada_url(soup)
        total_paginas = self._extract_total_paginas(soup)

        return LibroBJV(
            id=IdentificadorLibro(bjv_id=libro_id),
            titulo=titulo,
            subtitulo=subtitulo,
            autores=autores,
            editores=editores,
            editorial=editorial,
            isbn=isbn,
            anio_publicacion=anio,
            area_derecho=area_derecho,
            capitulos=capitulos,
            url_pdf=url_pdf,
            portada_url=portada_url,
            total_paginas=total_paginas,
            resumen=resumen,
            palabras_clave=palabras_clave,
        )

    def _extract_titulo(self, soup) -> Optional[str]:
        """Extract book title."""
        # Try various selectors
        for selector in ["h1", ".titulo", ".book-title", ".title"]:
            elem = soup.select_one(selector)
            if elem:
                text = elem.get_text(strip=True)
                if text:
                    return text

        # Try h1 directly
        h1 = soup.find("h1")
        if h1:
            return h1.get_text(strip=True)

        return None

    def _extract_subtitulo(self, soup) -> Optional[str]:
        """Extract book subtitle."""
        for selector in [".subtitulo", ".subtitle", "h2.subtitulo"]:
            elem = soup.select_one(selector)
            if elem:
                text = elem.get_text(strip=True)
                if text:
                    return text
        return None

    def _extract_autores(self, soup) -> Tuple[Autor, ...]:
        """Extract authors."""
        autores = []

        # Try finding multiple author elements
        autor_elems = soup.find_all(class_="autor")
        if autor_elems:
            for elem in autor_elems:
                nombre = elem.get_text(strip=True)
                if nombre:
                    autores.append(Autor(nombre=nombre))
            return tuple(autores)

        # Try autores container with comma-separated list
        autores_container = soup.find(class_="autores")
        if autores_container:
            text = autores_container.get_text(strip=True)
            if "," in text:
                for nombre in text.split(","):
                    nombre = nombre.strip()
                    if nombre:
                        autores.append(Autor(nombre=nombre))
                return tuple(autores)
            elif text:
                autores.append(Autor(nombre=text))
                return tuple(autores)

        # Try single autor element
        autor_elem = soup.find(class_="autor")
        if autor_elem:
            nombre = autor_elem.get_text(strip=True)
            if nombre:
                return (Autor(nombre=nombre),)

        return tuple()

    def _extract_editores(self, soup) -> Optional[Tuple[Autor, ...]]:
        """Extract editors."""
        editores = []

        editor_elems = soup.find_all(class_="editor")
        for elem in editor_elems:
            nombre = elem.get_text(strip=True)
            if nombre:
                editores.append(Autor(nombre=nombre))

        # Check editores container
        editores_container = soup.find(class_="editores")
        if editores_container:
            text = editores_container.get_text(strip=True)
            # Remove "Editor:" prefix
            text = re.sub(r"^Editor(?:es)?[:\s]*", "", text, flags=re.IGNORECASE)
            if text:
                editores.append(Autor(nombre=text))

        return tuple(editores) if editores else None

    def _extract_editorial(self, soup) -> Optional[Editorial]:
        """Extract publisher."""
        for selector in [".editorial", ".publisher", ".editor"]:
            elem = soup.select_one(selector)
            if elem:
                text = elem.get_text(strip=True)
                if text:
                    return Editorial(nombre=text)
        return None

    def _extract_isbn(self, soup) -> Optional[ISBN]:
        """Extract ISBN."""
        for selector in [".isbn", "span.isbn", "p.isbn"]:
            elem = soup.select_one(selector)
            if elem:
                text = elem.get_text(strip=True)
                isbn_clean = self._clean_isbn(text)
                if isbn_clean:
                    return ISBN(valor=isbn_clean)

        # Search in full text
        text = soup.get_text()
        match = self.ISBN_PATTERN.search(text)
        if match:
            return ISBN(valor=match.group(1))

        return None

    def _extract_anio(self, soup) -> Optional[AnioPublicacion]:
        """Extract publication year."""
        # Try specific year elements
        for selector in [".anio", ".year", ".fecha-publicacion", ".publication-date"]:
            elem = soup.select_one(selector)
            if elem:
                text = elem.get_text(strip=True)
                year = self._extract_year_from_text(text)
                if year:
                    return AnioPublicacion(valor=year)

        return None

    def _extract_area_derecho(self, soup) -> Optional[AreaDerecho]:
        """Detect legal practice area."""
        # Check area/categoria elements
        for selector in [".area", ".categoria", ".category", ".subject"]:
            elem = soup.select_one(selector)
            if elem:
                text = elem.get_text(strip=True).lower()
                for area, keywords in self.AREA_KEYWORDS.items():
                    for keyword in keywords:
                        if keyword in text:
                            return area

        return None

    def _extract_resumen(self, soup) -> Optional[str]:
        """Extract abstract/summary."""
        for selector in [".resumen", ".abstract", ".summary", ".descripcion"]:
            elem = soup.select_one(selector)
            if elem:
                text = elem.get_text(strip=True)
                if text:
                    return text
        return None

    def _extract_palabras_clave(self, soup) -> Optional[Tuple[str, ...]]:
        """Extract keywords."""
        for selector in [".palabras-clave", ".keywords", ".tags"]:
            elem = soup.select_one(selector)
            if elem:
                text = elem.get_text(strip=True)
                if "," in text:
                    keywords = [k.strip() for k in text.split(",") if k.strip()]
                    return tuple(keywords)
                elif text:
                    return (text,)
        return None

    def _extract_capitulos(self, soup) -> Tuple[CapituloLibro, ...]:
        """Extract chapters."""
        capitulos = []

        # Try various chapter container selectors
        capitulo_elems = soup.find_all(class_="capitulo")

        for i, elem in enumerate(capitulo_elems, start=1):
            # Extract chapter number
            num_elem = elem.find(class_=re.compile(r"num|numero"))
            numero = i
            if num_elem:
                try:
                    numero = int(num_elem.get_text(strip=True))
                except ValueError:
                    pass

            # Extract title
            titulo_elem = elem.find(class_=re.compile(r"titulo|title"))
            titulo = f"Capítulo {numero}"
            if titulo_elem:
                titulo = titulo_elem.get_text(strip=True)
            else:
                # Try anchor
                anchor = elem.find("a")
                if anchor:
                    titulo = anchor.get_text(strip=True)

            # Extract page range
            pagina_inicio = 1
            pagina_fin = 10
            paginas_elem = elem.find(class_=re.compile(r"pagin|pages"))
            if paginas_elem:
                start, end = self._extract_page_range(paginas_elem.get_text())
                if start and end:
                    pagina_inicio = start
                    pagina_fin = end

            # Extract PDF URL
            url_archivo = None
            pdf_link = elem.find("a", href=re.compile(r"\.pdf", re.IGNORECASE))
            if pdf_link:
                href = pdf_link.get("href", "")
                url_archivo = URLArchivo(url=href, formato="pdf")

            capitulos.append(CapituloLibro(
                numero=numero,
                titulo=titulo,
                pagina_inicio=pagina_inicio,
                pagina_fin=pagina_fin,
                url_archivo=url_archivo,
            ))

        return tuple(capitulos)

    def _extract_pdf_url(self, soup) -> Optional[URLArchivo]:
        """Extract main PDF URL."""
        # Try various PDF link selectors
        for selector in [".pdf-link", ".btn-download", "a.pdf", ".download-pdf"]:
            elem = soup.select_one(selector)
            if elem:
                href = elem.get("href", "")
                if href and ".pdf" in href.lower():
                    return URLArchivo(url=href, formato="pdf")

        # Search for any PDF link
        pdf_links = soup.find_all("a", href=re.compile(r"\.pdf", re.IGNORECASE))
        for link in pdf_links:
            href = link.get("href", "")
            if href:
                return URLArchivo(url=href, formato="pdf")

        return None

    def _extract_portada_url(self, soup) -> Optional[str]:
        """Extract cover image URL."""
        for selector in [".portada", ".cover-image", ".book-cover", "img.cover"]:
            elem = soup.select_one(selector)
            if elem:
                src = elem.get("src", "")
                if src:
                    return src
        return None

    def _extract_total_paginas(self, soup) -> Optional[int]:
        """Extract total page count."""
        for selector in [".paginas", ".pages", ".page-count"]:
            elem = soup.select_one(selector)
            if elem:
                text = elem.get_text(strip=True)
                match = self.PAGES_TOTAL_PATTERN.search(text)
                if match:
                    return int(match.group(1))
                # Try just extracting a number
                try:
                    return int(re.search(r"(\d+)", text).group(1))
                except (AttributeError, ValueError):
                    pass
        return None

    def _extract_year_from_text(self, text: str) -> Optional[int]:
        """Extract year from text string."""
        match = self.YEAR_PATTERN.search(text)
        if match:
            return int(match.group(1))
        return None

    def _extract_page_range(self, text: str) -> Tuple[Optional[int], Optional[int]]:
        """Extract page range from text."""
        match = self.PAGE_RANGE_PATTERN.search(text)
        if match:
            return int(match.group(1)), int(match.group(2))
        return None, None

    def _clean_isbn(self, text: str) -> Optional[str]:
        """Clean ISBN string."""
        # Remove "ISBN:" prefix and whitespace
        text = re.sub(r"^ISBN[:\s]*", "", text, flags=re.IGNORECASE)
        text = text.strip()

        # Validate it looks like an ISBN
        if re.match(r"^\d{3}[-\s]?\d", text):
            return text

        return None
