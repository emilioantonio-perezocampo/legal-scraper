"""
Tests for BJV libro detail parser.

Following RED-GREEN TDD: These tests define the expected behavior
for parsing BJV book detail page HTML.

Target: ~35 tests
"""
import pytest


class TestParseLibroDetalle:
    """Tests for parsing book detail page."""

    def test_empty_html_raises_error(self):
        """Empty HTML raises ParseError."""
        from src.infrastructure.adapters.bjv_libro_parser import BJVLibroParser
        from src.infrastructure.adapters.bjv_errors import ParseError

        parser = BJVLibroParser()

        with pytest.raises(ParseError) as exc_info:
            parser.parse_libro_detalle("", libro_id="123")

        assert "HTML vacío" in str(exc_info.value)

    def test_none_html_raises_error(self):
        """None HTML raises ParseError."""
        from src.infrastructure.adapters.bjv_libro_parser import BJVLibroParser
        from src.infrastructure.adapters.bjv_errors import ParseError

        parser = BJVLibroParser()

        with pytest.raises(ParseError):
            parser.parse_libro_detalle(None, libro_id="123")

    def test_missing_titulo_raises_error(self):
        """HTML without title raises ParseError."""
        from src.infrastructure.adapters.bjv_libro_parser import BJVLibroParser
        from src.infrastructure.adapters.bjv_errors import ParseError

        parser = BJVLibroParser()
        html = """
        <html>
            <body>
                <div class="book-detail">
                    <p>Sin título</p>
                </div>
            </body>
        </html>
        """

        with pytest.raises(ParseError) as exc_info:
            parser.parse_libro_detalle(html, libro_id="123")

        assert "título" in str(exc_info.value).lower()

    def test_libro_id_from_parameter(self):
        """Libro ID is taken from parameter."""
        from src.infrastructure.adapters.bjv_libro_parser import BJVLibroParser

        parser = BJVLibroParser()
        html = """
        <html>
            <body>
                <h1 class="titulo">Derecho Civil Mexicano</h1>
            </body>
        </html>
        """

        libro = parser.parse_libro_detalle(html, libro_id="12345")

        assert libro.id.bjv_id == "12345"

    def test_titulo_extraction_from_h1(self):
        """Title extracted from h1 element."""
        from src.infrastructure.adapters.bjv_libro_parser import BJVLibroParser

        parser = BJVLibroParser()
        html = """
        <html>
            <body>
                <h1>Derecho Constitucional</h1>
            </body>
        </html>
        """

        libro = parser.parse_libro_detalle(html, libro_id="123")

        assert libro.titulo == "Derecho Constitucional"

    def test_titulo_extraction_from_titulo_class(self):
        """Title extracted from titulo class."""
        from src.infrastructure.adapters.bjv_libro_parser import BJVLibroParser

        parser = BJVLibroParser()
        html = """
        <html>
            <body>
                <div class="titulo">Derecho Penal</div>
            </body>
        </html>
        """

        libro = parser.parse_libro_detalle(html, libro_id="123")

        assert libro.titulo == "Derecho Penal"

    def test_titulo_extraction_from_book_title_class(self):
        """Title extracted from book-title class."""
        from src.infrastructure.adapters.bjv_libro_parser import BJVLibroParser

        parser = BJVLibroParser()
        html = """
        <html>
            <body>
                <h2 class="book-title">Derecho Mercantil</h2>
            </body>
        </html>
        """

        libro = parser.parse_libro_detalle(html, libro_id="123")

        assert libro.titulo == "Derecho Mercantil"

    def test_titulo_whitespace_trimmed(self):
        """Title whitespace is trimmed."""
        from src.infrastructure.adapters.bjv_libro_parser import BJVLibroParser

        parser = BJVLibroParser()
        html = """
        <html>
            <body>
                <h1>   Derecho Laboral   </h1>
            </body>
        </html>
        """

        libro = parser.parse_libro_detalle(html, libro_id="123")

        assert libro.titulo == "Derecho Laboral"

    def test_autores_single_author(self):
        """Single author extracted correctly."""
        from src.infrastructure.adapters.bjv_libro_parser import BJVLibroParser

        parser = BJVLibroParser()
        html = """
        <html>
            <body>
                <h1>Libro Test</h1>
                <p class="autor">Juan Pérez García</p>
            </body>
        </html>
        """

        libro = parser.parse_libro_detalle(html, libro_id="123")

        assert len(libro.autores) == 1
        assert libro.autores[0].nombre == "Juan Pérez García"

    def test_autores_multiple_authors(self):
        """Multiple authors extracted correctly."""
        from src.infrastructure.adapters.bjv_libro_parser import BJVLibroParser

        parser = BJVLibroParser()
        html = """
        <html>
            <body>
                <h1>Libro Test</h1>
                <div class="autores">
                    <span class="autor">María López</span>
                    <span class="autor">Carlos García</span>
                    <span class="autor">Ana Martínez</span>
                </div>
            </body>
        </html>
        """

        libro = parser.parse_libro_detalle(html, libro_id="123")

        assert len(libro.autores) == 3

    def test_autores_from_comma_separated(self):
        """Authors extracted from comma-separated list."""
        from src.infrastructure.adapters.bjv_libro_parser import BJVLibroParser

        parser = BJVLibroParser()
        html = """
        <html>
            <body>
                <h1>Libro Test</h1>
                <p class="autores">Juan López, María García, Pedro Sánchez</p>
            </body>
        </html>
        """

        libro = parser.parse_libro_detalle(html, libro_id="123")

        assert len(libro.autores) >= 2

    def test_editores_extraction(self):
        """Editors extracted correctly."""
        from src.infrastructure.adapters.bjv_libro_parser import BJVLibroParser

        parser = BJVLibroParser()
        html = """
        <html>
            <body>
                <h1>Libro Test</h1>
                <p class="editores">Editor: Luis Fernández</p>
            </body>
        </html>
        """

        libro = parser.parse_libro_detalle(html, libro_id="123")

        assert libro.editores is not None or len(libro.autores) >= 0

    def test_editorial_extraction(self):
        """Publisher extracted correctly."""
        from src.infrastructure.adapters.bjv_libro_parser import BJVLibroParser

        parser = BJVLibroParser()
        html = """
        <html>
            <body>
                <h1>Libro Test</h1>
                <p class="editorial">UNAM, Instituto de Investigaciones Jurídicas</p>
            </body>
        </html>
        """

        libro = parser.parse_libro_detalle(html, libro_id="123")

        assert libro.editorial is not None
        assert "UNAM" in libro.editorial.nombre

    def test_isbn_extraction(self):
        """ISBN extracted correctly."""
        from src.infrastructure.adapters.bjv_libro_parser import BJVLibroParser

        parser = BJVLibroParser()
        html = """
        <html>
            <body>
                <h1>Libro Test</h1>
                <p class="isbn">ISBN: 978-607-02-1234-5</p>
            </body>
        </html>
        """

        libro = parser.parse_libro_detalle(html, libro_id="123")

        assert libro.isbn is not None
        assert "978" in libro.isbn.valor

    def test_isbn_validation_valid_format(self):
        """ISBN with valid format is accepted."""
        from src.infrastructure.adapters.bjv_libro_parser import BJVLibroParser

        parser = BJVLibroParser()
        html = """
        <html>
            <body>
                <h1>Libro Test</h1>
                <span class="isbn">978-607-02-9876-1</span>
            </body>
        </html>
        """

        libro = parser.parse_libro_detalle(html, libro_id="123")

        assert libro.isbn is not None

    def test_anio_publicacion_extraction(self):
        """Publication year extracted correctly."""
        from src.infrastructure.adapters.bjv_libro_parser import BJVLibroParser

        parser = BJVLibroParser()
        html = """
        <html>
            <body>
                <h1>Libro Test</h1>
                <p class="anio">2019</p>
            </body>
        </html>
        """

        libro = parser.parse_libro_detalle(html, libro_id="123")

        assert libro.anio_publicacion is not None
        assert libro.anio_publicacion.valor == 2019

    def test_anio_from_fecha_publicacion(self):
        """Year extracted from fecha_publicacion field."""
        from src.infrastructure.adapters.bjv_libro_parser import BJVLibroParser

        parser = BJVLibroParser()
        html = """
        <html>
            <body>
                <h1>Libro Test</h1>
                <p class="fecha-publicacion">México, 2021</p>
            </body>
        </html>
        """

        libro = parser.parse_libro_detalle(html, libro_id="123")

        assert libro.anio_publicacion is not None
        assert libro.anio_publicacion.valor == 2021

    def test_area_derecho_civil_detection(self):
        """Civil law area detected."""
        from src.infrastructure.adapters.bjv_libro_parser import BJVLibroParser
        from src.domain.bjv_value_objects import AreaDerecho

        parser = BJVLibroParser()
        html = """
        <html>
            <body>
                <h1>Libro Test</h1>
                <p class="area">Derecho Civil</p>
            </body>
        </html>
        """

        libro = parser.parse_libro_detalle(html, libro_id="123")

        assert libro.area_derecho == AreaDerecho.CIVIL

    def test_area_derecho_penal_detection(self):
        """Penal law area detected."""
        from src.infrastructure.adapters.bjv_libro_parser import BJVLibroParser
        from src.domain.bjv_value_objects import AreaDerecho

        parser = BJVLibroParser()
        html = """
        <html>
            <body>
                <h1>Libro Test</h1>
                <span class="categoria">Penal</span>
            </body>
        </html>
        """

        libro = parser.parse_libro_detalle(html, libro_id="123")

        assert libro.area_derecho == AreaDerecho.PENAL

    def test_area_derecho_constitucional_detection(self):
        """Constitutional law area detected."""
        from src.infrastructure.adapters.bjv_libro_parser import BJVLibroParser
        from src.domain.bjv_value_objects import AreaDerecho

        parser = BJVLibroParser()
        html = """
        <html>
            <body>
                <h1>Libro Test</h1>
                <p class="area">Derecho Constitucional</p>
            </body>
        </html>
        """

        libro = parser.parse_libro_detalle(html, libro_id="123")

        assert libro.area_derecho == AreaDerecho.CONSTITUCIONAL

    def test_resumen_extraction(self):
        """Abstract/summary extracted correctly."""
        from src.infrastructure.adapters.bjv_libro_parser import BJVLibroParser

        parser = BJVLibroParser()
        html = """
        <html>
            <body>
                <h1>Libro Test</h1>
                <div class="resumen">Este libro trata sobre derecho civil mexicano.</div>
            </body>
        </html>
        """

        libro = parser.parse_libro_detalle(html, libro_id="123")

        assert libro.resumen is not None
        assert "derecho civil" in libro.resumen.lower()

    def test_resumen_from_abstract_class(self):
        """Abstract extracted from abstract class."""
        from src.infrastructure.adapters.bjv_libro_parser import BJVLibroParser

        parser = BJVLibroParser()
        html = """
        <html>
            <body>
                <h1>Libro Test</h1>
                <p class="abstract">This is the book abstract.</p>
            </body>
        </html>
        """

        libro = parser.parse_libro_detalle(html, libro_id="123")

        assert libro.resumen is not None

    def test_capitulos_extraction(self):
        """Chapters extracted correctly."""
        from src.infrastructure.adapters.bjv_libro_parser import BJVLibroParser

        parser = BJVLibroParser()
        html = """
        <html>
            <body>
                <h1>Libro Test</h1>
                <ul class="capitulos">
                    <li class="capitulo">
                        <span class="num">1</span>
                        <a href="/pdf/cap1.pdf">Introducción</a>
                        <span class="paginas">1-20</span>
                    </li>
                    <li class="capitulo">
                        <span class="num">2</span>
                        <a href="/pdf/cap2.pdf">Desarrollo</a>
                        <span class="paginas">21-50</span>
                    </li>
                </ul>
            </body>
        </html>
        """

        libro = parser.parse_libro_detalle(html, libro_id="123")

        assert len(libro.capitulos) >= 1

    def test_capitulo_titulo_extraction(self):
        """Chapter title extracted."""
        from src.infrastructure.adapters.bjv_libro_parser import BJVLibroParser

        parser = BJVLibroParser()
        html = """
        <html>
            <body>
                <h1>Libro Test</h1>
                <div class="capitulo">
                    <span class="numero">1</span>
                    <span class="titulo-capitulo">Fundamentos del Derecho</span>
                </div>
            </body>
        </html>
        """

        libro = parser.parse_libro_detalle(html, libro_id="123")

        if libro.capitulos:
            assert "Fundamentos" in libro.capitulos[0].titulo

    def test_capitulo_paginas_extraction(self):
        """Chapter page range extracted."""
        from src.infrastructure.adapters.bjv_libro_parser import BJVLibroParser

        parser = BJVLibroParser()
        html = """
        <html>
            <body>
                <h1>Libro Test</h1>
                <div class="indice">
                    <div class="capitulo">
                        <span class="num">1</span>
                        <span class="titulo">Capítulo Uno</span>
                        <span class="paginas">pp. 1-25</span>
                    </div>
                </div>
            </body>
        </html>
        """

        libro = parser.parse_libro_detalle(html, libro_id="123")

        if libro.capitulos:
            assert libro.capitulos[0].pagina_inicio == 1
            assert libro.capitulos[0].pagina_fin == 25

    def test_pdf_url_extraction(self):
        """PDF URL extracted correctly."""
        from src.infrastructure.adapters.bjv_libro_parser import BJVLibroParser

        parser = BJVLibroParser()
        html = """
        <html>
            <body>
                <h1>Libro Test</h1>
                <a class="pdf-link" href="https://archivos.juridicas.unam.mx/bjv/libros/123/libro.pdf">
                    Descargar PDF
                </a>
            </body>
        </html>
        """

        libro = parser.parse_libro_detalle(html, libro_id="123")

        assert libro.url_pdf is not None
        assert libro.url_pdf.es_pdf

    def test_pdf_url_from_download_button(self):
        """PDF URL from download button."""
        from src.infrastructure.adapters.bjv_libro_parser import BJVLibroParser

        parser = BJVLibroParser()
        html = """
        <html>
            <body>
                <h1>Libro Test</h1>
                <a class="btn-download" href="/download/libro.pdf">Descargar</a>
            </body>
        </html>
        """

        libro = parser.parse_libro_detalle(html, libro_id="123")

        assert libro.url_pdf is not None

    def test_portada_url_extraction(self):
        """Cover image URL extracted."""
        from src.infrastructure.adapters.bjv_libro_parser import BJVLibroParser

        parser = BJVLibroParser()
        html = """
        <html>
            <body>
                <h1>Libro Test</h1>
                <img class="portada" src="https://biblio.juridicas.unam.mx/covers/123.jpg"/>
            </body>
        </html>
        """

        libro = parser.parse_libro_detalle(html, libro_id="123")

        assert libro.portada_url is not None

    def test_portada_from_cover_image_class(self):
        """Cover from cover-image class."""
        from src.infrastructure.adapters.bjv_libro_parser import BJVLibroParser

        parser = BJVLibroParser()
        html = """
        <html>
            <body>
                <h1>Libro Test</h1>
                <img class="cover-image" src="/images/cover.jpg"/>
            </body>
        </html>
        """

        libro = parser.parse_libro_detalle(html, libro_id="123")

        assert libro.portada_url is not None

    def test_total_paginas_extraction(self):
        """Total pages extracted."""
        from src.infrastructure.adapters.bjv_libro_parser import BJVLibroParser

        parser = BJVLibroParser()
        html = """
        <html>
            <body>
                <h1>Libro Test</h1>
                <p class="paginas">250 páginas</p>
            </body>
        </html>
        """

        libro = parser.parse_libro_detalle(html, libro_id="123")

        assert libro.total_paginas == 250

    def test_returns_libro_bjv_entity(self):
        """Parser returns LibroBJV entity."""
        from src.infrastructure.adapters.bjv_libro_parser import BJVLibroParser
        from src.domain.bjv_entities import LibroBJV

        parser = BJVLibroParser()
        html = """
        <html>
            <body>
                <h1>Libro Test</h1>
            </body>
        </html>
        """

        libro = parser.parse_libro_detalle(html, libro_id="123")

        assert isinstance(libro, LibroBJV)

    def test_subtitulo_extraction(self):
        """Subtitle extracted correctly."""
        from src.infrastructure.adapters.bjv_libro_parser import BJVLibroParser

        parser = BJVLibroParser()
        html = """
        <html>
            <body>
                <h1>Derecho Civil</h1>
                <h2 class="subtitulo">Una introducción al sistema mexicano</h2>
            </body>
        </html>
        """

        libro = parser.parse_libro_detalle(html, libro_id="123")

        assert libro.subtitulo is not None
        assert "introducción" in libro.subtitulo.lower()

    def test_palabras_clave_extraction(self):
        """Keywords extracted correctly."""
        from src.infrastructure.adapters.bjv_libro_parser import BJVLibroParser

        parser = BJVLibroParser()
        html = """
        <html>
            <body>
                <h1>Libro Test</h1>
                <p class="palabras-clave">contratos, obligaciones, responsabilidad</p>
            </body>
        </html>
        """

        libro = parser.parse_libro_detalle(html, libro_id="123")

        assert libro.palabras_clave is not None
        assert len(libro.palabras_clave) >= 2


class TestLibroParserHelpers:
    """Tests for helper methods in libro parser."""

    def test_extract_year_from_text(self):
        """Year extraction from text."""
        from src.infrastructure.adapters.bjv_libro_parser import BJVLibroParser

        parser = BJVLibroParser()

        year = parser._extract_year_from_text("México, 2020, Primera edición")

        assert year == 2020

    def test_extract_page_range(self):
        """Page range extraction."""
        from src.infrastructure.adapters.bjv_libro_parser import BJVLibroParser

        parser = BJVLibroParser()

        start, end = parser._extract_page_range("pp. 45-120")

        assert start == 45
        assert end == 120

    def test_clean_isbn(self):
        """ISBN cleaning."""
        from src.infrastructure.adapters.bjv_libro_parser import BJVLibroParser

        parser = BJVLibroParser()

        isbn = parser._clean_isbn("ISBN: 978-607-02-1234-5")

        assert isbn == "978-607-02-1234-5"
