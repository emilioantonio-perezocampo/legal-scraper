"""
Tests for BJV search results parser.

Following RED-GREEN TDD: These tests define the expected behavior
for parsing BJV search results HTML.

Target: ~30 tests
"""
import pytest
from unittest.mock import Mock


class TestParseSearchResults:
    """Tests for parsing search results."""

    def test_empty_html_raises_error(self):
        """Empty HTML raises ParseError."""
        from src.infrastructure.adapters.bjv_search_parser import BJVSearchParser
        from src.infrastructure.adapters.bjv_errors import ParseError

        parser = BJVSearchParser()

        with pytest.raises(ParseError) as exc_info:
            parser.parse_search_results("")

        assert "HTML vacío" in str(exc_info.value)

    def test_none_html_raises_error(self):
        """None HTML raises ParseError."""
        from src.infrastructure.adapters.bjv_search_parser import BJVSearchParser
        from src.infrastructure.adapters.bjv_errors import ParseError

        parser = BJVSearchParser()

        with pytest.raises(ParseError):
            parser.parse_search_results(None)

    def test_no_results_returns_empty_tuple(self):
        """HTML with no results returns empty tuple."""
        from src.infrastructure.adapters.bjv_search_parser import BJVSearchParser

        parser = BJVSearchParser()
        html = """
        <html>
            <body>
                <div class="search-results">
                    <p>No se encontraron resultados</p>
                </div>
            </body>
        </html>
        """

        results = parser.parse_search_results(html)

        assert results == ()

    def test_single_result_extraction(self):
        """Single search result is correctly extracted."""
        from src.infrastructure.adapters.bjv_search_parser import BJVSearchParser

        parser = BJVSearchParser()
        html = """
        <html>
            <body>
                <div class="resultado-item">
                    <h3><a href="/bjv/detalle-libro/4567">Derecho Civil Mexicano</a></h3>
                    <p class="autor">Juan Pérez García</p>
                    <p class="anio">2020</p>
                </div>
            </body>
        </html>
        """

        results = parser.parse_search_results(html)

        assert len(results) == 1
        assert results[0].titulo == "Derecho Civil Mexicano"

    def test_multiple_results_extraction(self):
        """Multiple search results are correctly extracted."""
        from src.infrastructure.adapters.bjv_search_parser import BJVSearchParser

        parser = BJVSearchParser()
        html = """
        <html>
            <body>
                <div class="resultado-item">
                    <h3><a href="/bjv/detalle-libro/4567">Libro Uno</a></h3>
                </div>
                <div class="resultado-item">
                    <h3><a href="/bjv/detalle-libro/4568">Libro Dos</a></h3>
                </div>
                <div class="resultado-item">
                    <h3><a href="/bjv/detalle-libro/4569">Libro Tres</a></h3>
                </div>
            </body>
        </html>
        """

        results = parser.parse_search_results(html)

        assert len(results) == 3
        assert results[0].titulo == "Libro Uno"
        assert results[1].titulo == "Libro Dos"
        assert results[2].titulo == "Libro Tres"

    def test_missing_title_skipped(self):
        """Items without title are skipped."""
        from src.infrastructure.adapters.bjv_search_parser import BJVSearchParser

        parser = BJVSearchParser()
        html = """
        <html>
            <body>
                <div class="resultado-item">
                    <h3><a href="/bjv/detalle-libro/4567">Libro Con Título</a></h3>
                </div>
                <div class="resultado-item">
                    <p class="autor">Sin título</p>
                </div>
            </body>
        </html>
        """

        results = parser.parse_search_results(html)

        assert len(results) == 1
        assert results[0].titulo == "Libro Con Título"

    def test_relative_url_converted_to_absolute(self):
        """Relative URLs are converted to absolute."""
        from src.infrastructure.adapters.bjv_search_parser import BJVSearchParser

        parser = BJVSearchParser()
        html = """
        <html>
            <body>
                <div class="resultado-item">
                    <h3><a href="/bjv/detalle-libro/4567">Libro Test</a></h3>
                </div>
            </body>
        </html>
        """

        results = parser.parse_search_results(html)

        assert "biblio.juridicas.unam.mx" in results[0].libro_id.url

    def test_libro_id_extraction_from_detalle_libro_pattern(self):
        """Libro ID extracted from detalle-libro URL pattern."""
        from src.infrastructure.adapters.bjv_search_parser import BJVSearchParser

        parser = BJVSearchParser()
        html = """
        <html>
            <body>
                <div class="resultado-item">
                    <h3><a href="/bjv/detalle-libro/12345">Libro Test</a></h3>
                </div>
            </body>
        </html>
        """

        results = parser.parse_search_results(html)

        assert results[0].libro_id.bjv_id == "12345"

    def test_libro_id_extraction_from_detalle_pattern(self):
        """Libro ID extracted from detalle URL pattern."""
        from src.infrastructure.adapters.bjv_search_parser import BJVSearchParser

        parser = BJVSearchParser()
        html = """
        <html>
            <body>
                <div class="resultado-item">
                    <h3><a href="/bjv/detalle/67890">Libro Test</a></h3>
                </div>
            </body>
        </html>
        """

        results = parser.parse_search_results(html)

        assert results[0].libro_id.bjv_id == "67890"

    def test_libro_id_extraction_from_id_libro_pattern(self):
        """Libro ID extracted from id-libro query param pattern."""
        from src.infrastructure.adapters.bjv_search_parser import BJVSearchParser

        parser = BJVSearchParser()
        html = """
        <html>
            <body>
                <div class="resultado-item">
                    <h3><a href="/bjv/libro?id-libro=99999">Libro Test</a></h3>
                </div>
            </body>
        </html>
        """

        results = parser.parse_search_results(html)

        assert results[0].libro_id.bjv_id == "99999"

    def test_autores_texto_extraction(self):
        """Author text is extracted."""
        from src.infrastructure.adapters.bjv_search_parser import BJVSearchParser

        parser = BJVSearchParser()
        html = """
        <html>
            <body>
                <div class="resultado-item">
                    <h3><a href="/bjv/detalle-libro/4567">Libro Test</a></h3>
                    <p class="autor">María López, Carlos García</p>
                </div>
            </body>
        </html>
        """

        results = parser.parse_search_results(html)

        assert "María López" in results[0].autores_texto

    def test_autores_extraction_from_meta_author(self):
        """Authors extracted from meta author class."""
        from src.infrastructure.adapters.bjv_search_parser import BJVSearchParser

        parser = BJVSearchParser()
        html = """
        <html>
            <body>
                <div class="resultado-item">
                    <h3><a href="/bjv/detalle-libro/4567">Libro Test</a></h3>
                    <span class="meta-author">Pedro Sánchez</span>
                </div>
            </body>
        </html>
        """

        results = parser.parse_search_results(html)

        assert "Pedro Sánchez" in results[0].autores_texto

    def test_anio_extraction_from_anio_class(self):
        """Year extracted from anio class element."""
        from src.infrastructure.adapters.bjv_search_parser import BJVSearchParser

        parser = BJVSearchParser()
        html = """
        <html>
            <body>
                <div class="resultado-item">
                    <h3><a href="/bjv/detalle-libro/4567">Libro Test</a></h3>
                    <p class="anio">2019</p>
                </div>
            </body>
        </html>
        """

        results = parser.parse_search_results(html)

        assert results[0].anio == 2019

    def test_anio_extraction_from_year_class(self):
        """Year extracted from year class element."""
        from src.infrastructure.adapters.bjv_search_parser import BJVSearchParser

        parser = BJVSearchParser()
        html = """
        <html>
            <body>
                <div class="resultado-item">
                    <h3><a href="/bjv/detalle-libro/4567">Libro Test</a></h3>
                    <span class="year">2021</span>
                </div>
            </body>
        </html>
        """

        results = parser.parse_search_results(html)

        assert results[0].anio == 2021

    def test_anio_extraction_from_parenthetical_format(self):
        """Year extracted from parenthetical format (2015)."""
        from src.infrastructure.adapters.bjv_search_parser import BJVSearchParser

        parser = BJVSearchParser()
        html = """
        <html>
            <body>
                <div class="resultado-item">
                    <h3><a href="/bjv/detalle-libro/4567">Libro Test (2015)</a></h3>
                </div>
            </body>
        </html>
        """

        results = parser.parse_search_results(html)

        assert results[0].anio == 2015

    def test_anio_none_when_not_found(self):
        """Year is None when not found."""
        from src.infrastructure.adapters.bjv_search_parser import BJVSearchParser

        parser = BJVSearchParser()
        html = """
        <html>
            <body>
                <div class="resultado-item">
                    <h3><a href="/bjv/detalle-libro/4567">Libro Sin Año</a></h3>
                </div>
            </body>
        </html>
        """

        results = parser.parse_search_results(html)

        assert results[0].anio is None

    def test_tipo_contenido_libro_detected(self):
        """Content type LIBRO detected from URL pattern."""
        from src.infrastructure.adapters.bjv_search_parser import BJVSearchParser
        from src.domain.bjv_value_objects import TipoContenido

        parser = BJVSearchParser()
        html = """
        <html>
            <body>
                <div class="resultado-item">
                    <h3><a href="/bjv/detalle-libro/4567">Libro Test</a></h3>
                </div>
            </body>
        </html>
        """

        results = parser.parse_search_results(html)

        assert results[0].tipo_contenido == TipoContenido.LIBRO

    def test_tipo_contenido_capitulo_detected(self):
        """Content type CAPITULO detected from URL pattern."""
        from src.infrastructure.adapters.bjv_search_parser import BJVSearchParser
        from src.domain.bjv_value_objects import TipoContenido

        parser = BJVSearchParser()
        html = """
        <html>
            <body>
                <div class="resultado-item">
                    <h3><a href="/bjv/capitulo/detalle/789">Capítulo Test</a></h3>
                </div>
            </body>
        </html>
        """

        results = parser.parse_search_results(html)

        assert results[0].tipo_contenido == TipoContenido.CAPITULO

    def test_tipo_contenido_revista_detected(self):
        """Content type REVISTA detected from URL pattern."""
        from src.infrastructure.adapters.bjv_search_parser import BJVSearchParser
        from src.domain.bjv_value_objects import TipoContenido

        parser = BJVSearchParser()
        html = """
        <html>
            <body>
                <div class="resultado-item">
                    <h3><a href="/bjv/revista/detalle/123">Revista Test</a></h3>
                </div>
            </body>
        </html>
        """

        results = parser.parse_search_results(html)

        assert results[0].tipo_contenido == TipoContenido.REVISTA

    def test_result_has_relevancia_default(self):
        """Search result has default relevancia value."""
        from src.infrastructure.adapters.bjv_search_parser import BJVSearchParser

        parser = BJVSearchParser()
        html = """
        <html>
            <body>
                <div class="resultado-item">
                    <h3><a href="/bjv/detalle-libro/4567">Libro Test</a></h3>
                </div>
            </body>
        </html>
        """

        results = parser.parse_search_results(html)

        assert results[0].relevancia == 1.0

    def test_result_with_fragmento_coincidente(self):
        """Search result extracts matching fragment."""
        from src.infrastructure.adapters.bjv_search_parser import BJVSearchParser

        parser = BJVSearchParser()
        html = """
        <html>
            <body>
                <div class="resultado-item">
                    <h3><a href="/bjv/detalle-libro/4567">Libro Test</a></h3>
                    <p class="snippet">...fragmento con <em>término</em> de búsqueda...</p>
                </div>
            </body>
        </html>
        """

        results = parser.parse_search_results(html)

        assert results[0].fragmento_coincidente is not None

    def test_whitespace_trimmed_from_title(self):
        """Whitespace is trimmed from title."""
        from src.infrastructure.adapters.bjv_search_parser import BJVSearchParser

        parser = BJVSearchParser()
        html = """
        <html>
            <body>
                <div class="resultado-item">
                    <h3><a href="/bjv/detalle-libro/4567">   Libro Con Espacios   </a></h3>
                </div>
            </body>
        </html>
        """

        results = parser.parse_search_results(html)

        assert results[0].titulo == "Libro Con Espacios"

    def test_html_entities_decoded(self):
        """HTML entities are decoded in title."""
        from src.infrastructure.adapters.bjv_search_parser import BJVSearchParser

        parser = BJVSearchParser()
        html = """
        <html>
            <body>
                <div class="resultado-item">
                    <h3><a href="/bjv/detalle-libro/4567">Derecho &amp; Justicia</a></h3>
                </div>
            </body>
        </html>
        """

        results = parser.parse_search_results(html)

        assert results[0].titulo == "Derecho & Justicia"


class TestExtractPaginationInfo:
    """Tests for pagination info extraction."""

    def test_pagination_from_nav(self):
        """Pagination extracted from navigation element."""
        from src.infrastructure.adapters.bjv_search_parser import BJVSearchParser

        parser = BJVSearchParser()
        html = """
        <html>
            <body>
                <nav class="pagination">
                    <span class="current">2</span>
                    <a href="?page=1">1</a>
                    <a href="?page=3">3</a>
                    <a href="?page=10">10</a>
                </nav>
            </body>
        </html>
        """

        info = parser.extract_pagination_info(html)

        assert info.pagina_actual == 2
        assert info.total_paginas == 10

    def test_total_resultados_extraction(self):
        """Total results count is extracted."""
        from src.infrastructure.adapters.bjv_search_parser import BJVSearchParser

        parser = BJVSearchParser()
        html = """
        <html>
            <body>
                <div class="results-header">
                    <span class="total-results">Se encontraron 245 resultados</span>
                </div>
            </body>
        </html>
        """

        info = parser.extract_pagination_info(html)

        assert info.total_resultados == 245

    def test_total_resultados_from_alternative_format(self):
        """Total results extracted from alternative format."""
        from src.infrastructure.adapters.bjv_search_parser import BJVSearchParser

        parser = BJVSearchParser()
        html = """
        <html>
            <body>
                <p>Mostrando 1-20 de 150 resultados</p>
            </body>
        </html>
        """

        info = parser.extract_pagination_info(html)

        assert info.total_resultados == 150

    def test_default_values_when_missing(self):
        """Default pagination values when elements missing."""
        from src.infrastructure.adapters.bjv_search_parser import BJVSearchParser

        parser = BJVSearchParser()
        html = """
        <html>
            <body>
                <div>Sin paginación</div>
            </body>
        </html>
        """

        info = parser.extract_pagination_info(html)

        assert info.pagina_actual == 1
        assert info.total_paginas == 1
        assert info.total_resultados == 0

    def test_empty_html_returns_defaults(self):
        """Empty HTML returns default pagination."""
        from src.infrastructure.adapters.bjv_search_parser import BJVSearchParser

        parser = BJVSearchParser()

        info = parser.extract_pagination_info("")

        assert info.pagina_actual == 1

    def test_pagination_info_is_dataclass(self):
        """PaginationInfo is a dataclass."""
        from src.infrastructure.adapters.bjv_search_parser import PaginacionInfo

        info = PaginacionInfo(pagina_actual=1, total_paginas=5, total_resultados=100)

        assert info.pagina_actual == 1
        assert info.total_paginas == 5
        assert info.total_resultados == 100

    def test_pagina_actual_from_class_active(self):
        """Current page from active class."""
        from src.infrastructure.adapters.bjv_search_parser import BJVSearchParser

        parser = BJVSearchParser()
        html = """
        <html>
            <body>
                <ul class="pagination">
                    <li><a href="?p=1">1</a></li>
                    <li class="active"><a href="?p=5">5</a></li>
                    <li><a href="?p=20">20</a></li>
                </ul>
            </body>
        </html>
        """

        info = parser.extract_pagination_info(html)

        assert info.pagina_actual == 5
        assert info.total_paginas == 20


class TestSearchResultadoBusqueda:
    """Tests for SearchResultadoBusqueda dataclass."""

    def test_search_resultado_creation(self):
        """SearchResultadoBusqueda can be created."""
        from src.infrastructure.adapters.bjv_search_parser import SearchResultadoBusqueda
        from src.domain.bjv_value_objects import IdentificadorLibro, TipoContenido

        result = SearchResultadoBusqueda(
            libro_id=IdentificadorLibro(bjv_id="123"),
            titulo="Test Libro",
            relevancia=0.9,
            autores_texto="Juan Pérez",
            anio=2020,
            tipo_contenido=TipoContenido.LIBRO,
            fragmento_coincidente="fragmento",
        )

        assert result.titulo == "Test Libro"
        assert result.anio == 2020

    def test_search_resultado_optional_fields(self):
        """SearchResultadoBusqueda has optional fields."""
        from src.infrastructure.adapters.bjv_search_parser import SearchResultadoBusqueda
        from src.domain.bjv_value_objects import IdentificadorLibro, TipoContenido

        result = SearchResultadoBusqueda(
            libro_id=IdentificadorLibro(bjv_id="123"),
            titulo="Test Libro",
            relevancia=1.0,
            tipo_contenido=TipoContenido.LIBRO,
        )

        assert result.autores_texto is None
        assert result.anio is None
        assert result.fragmento_coincidente is None
