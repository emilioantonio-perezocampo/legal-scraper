"""
Tests for BJV Domain Entities

RED phase: All tests should fail until implementation is complete.
Tests follow DDD patterns with Spanish domain terminology.
Entities have identity and lifecycle.
"""
import pytest
from dataclasses import FrozenInstanceError
from datetime import datetime
from uuid import UUID


class TestAutor:
    """Tests for Autor entity - book author."""

    def test_autor_creation(self):
        """Autor should store name."""
        from src.domain.bjv_entities import Autor
        autor = Autor(nombre="Juan Pérez García")
        assert autor.nombre == "Juan Pérez García"

    def test_autor_is_immutable(self):
        """Autor should be frozen dataclass."""
        from src.domain.bjv_entities import Autor
        autor = Autor(nombre="Juan Pérez")
        with pytest.raises(FrozenInstanceError):
            autor.nombre = "Pedro López"

    def test_autor_equality(self):
        """Two Autor with same name should be equal."""
        from src.domain.bjv_entities import Autor
        autor1 = Autor(nombre="Juan Pérez")
        autor2 = Autor(nombre="Juan Pérez")
        assert autor1 == autor2

    def test_autor_nombre_completo_property(self):
        """Autor should provide nombre_completo property."""
        from src.domain.bjv_entities import Autor
        autor = Autor(nombre="Juan Pérez García")
        assert autor.nombre_completo == "Juan Pérez García"

    def test_autor_with_afiliacion(self):
        """Autor should optionally store affiliation."""
        from src.domain.bjv_entities import Autor
        autor = Autor(nombre="Juan Pérez", afiliacion="UNAM")
        assert autor.afiliacion == "UNAM"


class TestEditorial:
    """Tests for Editorial entity - publisher."""

    def test_editorial_creation(self):
        """Editorial should store name."""
        from src.domain.bjv_entities import Editorial
        editorial = Editorial(nombre="Instituto de Investigaciones Jurídicas UNAM")
        assert editorial.nombre == "Instituto de Investigaciones Jurídicas UNAM"

    def test_editorial_is_immutable(self):
        """Editorial should be frozen dataclass."""
        from src.domain.bjv_entities import Editorial
        editorial = Editorial(nombre="UNAM")
        with pytest.raises(FrozenInstanceError):
            editorial.nombre = "Porrúa"

    def test_editorial_with_pais(self):
        """Editorial should optionally store country."""
        from src.domain.bjv_entities import Editorial
        editorial = Editorial(nombre="UNAM", pais="México")
        assert editorial.pais == "México"

    def test_editorial_es_unam(self):
        """Editorial should identify UNAM publications."""
        from src.domain.bjv_entities import Editorial
        editorial = Editorial(nombre="Instituto de Investigaciones Jurídicas UNAM")
        assert editorial.es_unam is True

    def test_editorial_es_unam_false(self):
        """Editorial should identify non-UNAM publications."""
        from src.domain.bjv_entities import Editorial
        editorial = Editorial(nombre="Editorial Porrúa")
        assert editorial.es_unam is False


class TestCapituloLibro:
    """Tests for CapituloLibro entity - book chapter."""

    def test_capitulo_libro_creation(self):
        """CapituloLibro should store basic info."""
        from src.domain.bjv_entities import CapituloLibro
        capitulo = CapituloLibro(
            numero=1,
            titulo="Introducción al Derecho Civil",
            pagina_inicio=15,
            pagina_fin=42
        )
        assert capitulo.numero == 1
        assert capitulo.titulo == "Introducción al Derecho Civil"
        assert capitulo.pagina_inicio == 15
        assert capitulo.pagina_fin == 42

    def test_capitulo_libro_is_immutable(self):
        """CapituloLibro should be frozen dataclass."""
        from src.domain.bjv_entities import CapituloLibro
        capitulo = CapituloLibro(
            numero=1,
            titulo="Intro",
            pagina_inicio=1,
            pagina_fin=10
        )
        with pytest.raises(FrozenInstanceError):
            capitulo.titulo = "New Title"

    def test_capitulo_libro_total_paginas(self):
        """CapituloLibro should calculate total pages."""
        from src.domain.bjv_entities import CapituloLibro
        capitulo = CapituloLibro(
            numero=1,
            titulo="Intro",
            pagina_inicio=15,
            pagina_fin=42
        )
        assert capitulo.total_paginas == 28

    def test_capitulo_libro_with_url(self):
        """CapituloLibro should optionally store URL."""
        from src.domain.bjv_entities import CapituloLibro
        from src.domain.bjv_value_objects import URLArchivo
        url = URLArchivo(url="https://example.com/cap1.pdf", formato="pdf")
        capitulo = CapituloLibro(
            numero=1,
            titulo="Intro",
            pagina_inicio=1,
            pagina_fin=10,
            url_archivo=url
        )
        assert capitulo.url_archivo == url


class TestLibroBJV:
    """Tests for LibroBJV aggregate root - the main book entity."""

    def test_libro_bjv_creation_minimal(self):
        """LibroBJV should be creatable with minimal required fields."""
        from src.domain.bjv_entities import LibroBJV
        from src.domain.bjv_value_objects import IdentificadorLibro

        libro = LibroBJV(
            id=IdentificadorLibro(bjv_id="12345"),
            titulo="Derecho Civil Mexicano"
        )
        assert libro.id.bjv_id == "12345"
        assert libro.titulo == "Derecho Civil Mexicano"

    def test_libro_bjv_is_immutable(self):
        """LibroBJV should be frozen dataclass."""
        from src.domain.bjv_entities import LibroBJV
        from src.domain.bjv_value_objects import IdentificadorLibro

        libro = LibroBJV(
            id=IdentificadorLibro(bjv_id="12345"),
            titulo="Derecho Civil"
        )
        with pytest.raises(FrozenInstanceError):
            libro.titulo = "Nuevo Título"

    def test_libro_bjv_with_autores_as_tuple(self):
        """LibroBJV autores should be immutable tuple."""
        from src.domain.bjv_entities import LibroBJV, Autor
        from src.domain.bjv_value_objects import IdentificadorLibro

        libro = LibroBJV(
            id=IdentificadorLibro(bjv_id="12345"),
            titulo="Derecho Civil",
            autores=(Autor(nombre="Juan Pérez"), Autor(nombre="María López"))
        )
        assert len(libro.autores) == 2
        assert isinstance(libro.autores, tuple)

    def test_libro_bjv_with_editorial(self):
        """LibroBJV should store editorial."""
        from src.domain.bjv_entities import LibroBJV, Editorial
        from src.domain.bjv_value_objects import IdentificadorLibro

        libro = LibroBJV(
            id=IdentificadorLibro(bjv_id="12345"),
            titulo="Derecho Civil",
            editorial=Editorial(nombre="UNAM")
        )
        assert libro.editorial.nombre == "UNAM"

    def test_libro_bjv_with_isbn(self):
        """LibroBJV should store ISBN."""
        from src.domain.bjv_entities import LibroBJV
        from src.domain.bjv_value_objects import IdentificadorLibro, ISBN

        libro = LibroBJV(
            id=IdentificadorLibro(bjv_id="12345"),
            titulo="Derecho Civil",
            isbn=ISBN(valor="978-607-02-1234-5")
        )
        assert libro.isbn.valor == "978-607-02-1234-5"

    def test_libro_bjv_with_anio_publicacion(self):
        """LibroBJV should store publication year."""
        from src.domain.bjv_entities import LibroBJV
        from src.domain.bjv_value_objects import IdentificadorLibro, AnioPublicacion

        libro = LibroBJV(
            id=IdentificadorLibro(bjv_id="12345"),
            titulo="Derecho Civil",
            anio_publicacion=AnioPublicacion(valor=2023)
        )
        assert libro.anio_publicacion.valor == 2023

    def test_libro_bjv_with_area_derecho(self):
        """LibroBJV should store legal area."""
        from src.domain.bjv_entities import LibroBJV
        from src.domain.bjv_value_objects import IdentificadorLibro, AreaDerecho

        libro = LibroBJV(
            id=IdentificadorLibro(bjv_id="12345"),
            titulo="Derecho Civil",
            area_derecho=AreaDerecho.CIVIL
        )
        assert libro.area_derecho == AreaDerecho.CIVIL

    def test_libro_bjv_with_capitulos_as_tuple(self):
        """LibroBJV capitulos should be immutable tuple."""
        from src.domain.bjv_entities import LibroBJV, CapituloLibro
        from src.domain.bjv_value_objects import IdentificadorLibro

        libro = LibroBJV(
            id=IdentificadorLibro(bjv_id="12345"),
            titulo="Derecho Civil",
            capitulos=(
                CapituloLibro(numero=1, titulo="Intro", pagina_inicio=1, pagina_fin=20),
                CapituloLibro(numero=2, titulo="Personas", pagina_inicio=21, pagina_fin=50),
            )
        )
        assert len(libro.capitulos) == 2
        assert isinstance(libro.capitulos, tuple)

    def test_libro_bjv_with_url_pdf(self):
        """LibroBJV should store PDF URL."""
        from src.domain.bjv_entities import LibroBJV
        from src.domain.bjv_value_objects import IdentificadorLibro, URLArchivo

        libro = LibroBJV(
            id=IdentificadorLibro(bjv_id="12345"),
            titulo="Derecho Civil",
            url_pdf=URLArchivo(url="https://example.com/libro.pdf", formato="pdf")
        )
        assert libro.url_pdf.es_pdf is True

    def test_libro_bjv_total_paginas(self):
        """LibroBJV should store total pages."""
        from src.domain.bjv_entities import LibroBJV
        from src.domain.bjv_value_objects import IdentificadorLibro

        libro = LibroBJV(
            id=IdentificadorLibro(bjv_id="12345"),
            titulo="Derecho Civil",
            total_paginas=350
        )
        assert libro.total_paginas == 350

    def test_libro_bjv_resumen(self):
        """LibroBJV should store abstract/summary."""
        from src.domain.bjv_entities import LibroBJV
        from src.domain.bjv_value_objects import IdentificadorLibro

        libro = LibroBJV(
            id=IdentificadorLibro(bjv_id="12345"),
            titulo="Derecho Civil",
            resumen="Este libro aborda los fundamentos del derecho civil mexicano."
        )
        assert "fundamentos" in libro.resumen

    def test_libro_bjv_url_property(self):
        """LibroBJV should provide URL via its id."""
        from src.domain.bjv_entities import LibroBJV
        from src.domain.bjv_value_objects import IdentificadorLibro

        libro = LibroBJV(
            id=IdentificadorLibro(bjv_id="12345"),
            titulo="Derecho Civil"
        )
        assert libro.url == "https://biblio.juridicas.unam.mx/bjv/detalle/12345"


class TestFragmentoTexto:
    """Tests for FragmentoTexto entity - text fragment for embedding."""

    def test_fragmento_texto_creation(self):
        """FragmentoTexto should store text content and metadata."""
        from src.domain.bjv_entities import FragmentoTexto
        from src.domain.bjv_value_objects import IdentificadorLibro

        fragmento = FragmentoTexto(
            id="frag-001",
            libro_id=IdentificadorLibro(bjv_id="12345"),
            contenido="El derecho civil regula las relaciones entre particulares.",
            numero_pagina=42,
            numero_capitulo=2
        )
        assert fragmento.id == "frag-001"
        assert fragmento.contenido == "El derecho civil regula las relaciones entre particulares."
        assert fragmento.numero_pagina == 42

    def test_fragmento_texto_is_immutable(self):
        """FragmentoTexto should be frozen dataclass."""
        from src.domain.bjv_entities import FragmentoTexto
        from src.domain.bjv_value_objects import IdentificadorLibro

        fragmento = FragmentoTexto(
            id="frag-001",
            libro_id=IdentificadorLibro(bjv_id="12345"),
            contenido="Texto de prueba",
            numero_pagina=1
        )
        with pytest.raises(FrozenInstanceError):
            fragmento.contenido = "Nuevo contenido"

    def test_fragmento_texto_longitud_property(self):
        """FragmentoTexto should calculate character length."""
        from src.domain.bjv_entities import FragmentoTexto
        from src.domain.bjv_value_objects import IdentificadorLibro

        fragmento = FragmentoTexto(
            id="frag-001",
            libro_id=IdentificadorLibro(bjv_id="12345"),
            contenido="Hola mundo",
            numero_pagina=1
        )
        assert fragmento.longitud == 10


class TestEmbeddingFragmento:
    """Tests for EmbeddingFragmento entity - vector embedding."""

    def test_embedding_fragmento_creation(self):
        """EmbeddingFragmento should store embedding vector."""
        from src.domain.bjv_entities import EmbeddingFragmento

        embedding = EmbeddingFragmento(
            fragmento_id="frag-001",
            vector=(0.1, 0.2, 0.3, 0.4, 0.5),
            modelo="text-embedding-3-small"
        )
        assert embedding.fragmento_id == "frag-001"
        assert len(embedding.vector) == 5
        assert embedding.modelo == "text-embedding-3-small"

    def test_embedding_fragmento_is_immutable(self):
        """EmbeddingFragmento should be frozen dataclass."""
        from src.domain.bjv_entities import EmbeddingFragmento

        embedding = EmbeddingFragmento(
            fragmento_id="frag-001",
            vector=(0.1, 0.2),
            modelo="test"
        )
        with pytest.raises(FrozenInstanceError):
            embedding.modelo = "new-model"

    def test_embedding_fragmento_vector_is_tuple(self):
        """EmbeddingFragmento vector should be immutable tuple."""
        from src.domain.bjv_entities import EmbeddingFragmento

        embedding = EmbeddingFragmento(
            fragmento_id="frag-001",
            vector=(0.1, 0.2, 0.3),
            modelo="test"
        )
        assert isinstance(embedding.vector, tuple)

    def test_embedding_fragmento_dimension_property(self):
        """EmbeddingFragmento should report vector dimension."""
        from src.domain.bjv_entities import EmbeddingFragmento

        embedding = EmbeddingFragmento(
            fragmento_id="frag-001",
            vector=tuple([0.1] * 1536),
            modelo="text-embedding-3-small"
        )
        assert embedding.dimension == 1536


class TestResultadoBusqueda:
    """Tests for ResultadoBusqueda entity - search result."""

    def test_resultado_busqueda_creation(self):
        """ResultadoBusqueda should store search result info."""
        from src.domain.bjv_entities import ResultadoBusqueda
        from src.domain.bjv_value_objects import IdentificadorLibro

        resultado = ResultadoBusqueda(
            libro_id=IdentificadorLibro(bjv_id="12345"),
            titulo="Derecho Civil Mexicano",
            relevancia=0.95
        )
        assert resultado.libro_id.bjv_id == "12345"
        assert resultado.titulo == "Derecho Civil Mexicano"
        assert resultado.relevancia == 0.95

    def test_resultado_busqueda_is_immutable(self):
        """ResultadoBusqueda should be frozen dataclass."""
        from src.domain.bjv_entities import ResultadoBusqueda
        from src.domain.bjv_value_objects import IdentificadorLibro

        resultado = ResultadoBusqueda(
            libro_id=IdentificadorLibro(bjv_id="12345"),
            titulo="Test",
            relevancia=0.5
        )
        with pytest.raises(FrozenInstanceError):
            resultado.relevancia = 0.9

    def test_resultado_busqueda_with_fragmento(self):
        """ResultadoBusqueda should optionally include matching fragment."""
        from src.domain.bjv_entities import ResultadoBusqueda
        from src.domain.bjv_value_objects import IdentificadorLibro

        resultado = ResultadoBusqueda(
            libro_id=IdentificadorLibro(bjv_id="12345"),
            titulo="Test",
            relevancia=0.9,
            fragmento_coincidente="...el artículo 123 establece..."
        )
        assert resultado.fragmento_coincidente == "...el artículo 123 establece..."

    def test_resultado_busqueda_es_muy_relevante(self):
        """ResultadoBusqueda should identify highly relevant results."""
        from src.domain.bjv_entities import ResultadoBusqueda
        from src.domain.bjv_value_objects import IdentificadorLibro

        resultado = ResultadoBusqueda(
            libro_id=IdentificadorLibro(bjv_id="12345"),
            titulo="Test",
            relevancia=0.95
        )
        assert resultado.es_muy_relevante is True

    def test_resultado_busqueda_es_muy_relevante_false(self):
        """ResultadoBusqueda should identify low relevance results."""
        from src.domain.bjv_entities import ResultadoBusqueda
        from src.domain.bjv_value_objects import IdentificadorLibro

        resultado = ResultadoBusqueda(
            libro_id=IdentificadorLibro(bjv_id="12345"),
            titulo="Test",
            relevancia=0.5
        )
        assert resultado.es_muy_relevante is False


class TestPuntoControlScraping:
    """Tests for PuntoControlScraping entity - scraping checkpoint."""

    def test_punto_control_scraping_creation(self):
        """PuntoControlScraping should store checkpoint state."""
        from src.domain.bjv_entities import PuntoControlScraping

        checkpoint = PuntoControlScraping(
            id="checkpoint-001",
            ultima_pagina_procesada=42,
            ultimo_libro_id="12345",
            total_libros_descubiertos=500,
            total_libros_descargados=450
        )
        assert checkpoint.ultima_pagina_procesada == 42
        assert checkpoint.total_libros_descubiertos == 500

    def test_punto_control_scraping_is_immutable(self):
        """PuntoControlScraping should be frozen dataclass."""
        from src.domain.bjv_entities import PuntoControlScraping

        checkpoint = PuntoControlScraping(
            id="checkpoint-001",
            ultima_pagina_procesada=10,
            ultimo_libro_id="123",
            total_libros_descubiertos=100,
            total_libros_descargados=90
        )
        with pytest.raises(FrozenInstanceError):
            checkpoint.ultima_pagina_procesada = 20

    def test_punto_control_scraping_with_timestamp(self):
        """PuntoControlScraping should store timestamp."""
        from src.domain.bjv_entities import PuntoControlScraping

        now = datetime.now()
        checkpoint = PuntoControlScraping(
            id="checkpoint-001",
            ultima_pagina_procesada=10,
            ultimo_libro_id="123",
            total_libros_descubiertos=100,
            total_libros_descargados=90,
            timestamp=now
        )
        assert checkpoint.timestamp == now

    def test_punto_control_scraping_progreso_porcentaje(self):
        """PuntoControlScraping should calculate progress percentage."""
        from src.domain.bjv_entities import PuntoControlScraping

        checkpoint = PuntoControlScraping(
            id="checkpoint-001",
            ultima_pagina_procesada=10,
            ultimo_libro_id="123",
            total_libros_descubiertos=100,
            total_libros_descargados=75
        )
        assert checkpoint.progreso_porcentaje == 75.0

    def test_punto_control_scraping_progreso_zero_division(self):
        """PuntoControlScraping should handle zero discovered."""
        from src.domain.bjv_entities import PuntoControlScraping

        checkpoint = PuntoControlScraping(
            id="checkpoint-001",
            ultima_pagina_procesada=0,
            ultimo_libro_id=None,
            total_libros_descubiertos=0,
            total_libros_descargados=0
        )
        assert checkpoint.progreso_porcentaje == 0.0

    def test_punto_control_scraping_with_errores(self):
        """PuntoControlScraping should track errors."""
        from src.domain.bjv_entities import PuntoControlScraping

        checkpoint = PuntoControlScraping(
            id="checkpoint-001",
            ultima_pagina_procesada=10,
            ultimo_libro_id="123",
            total_libros_descubiertos=100,
            total_libros_descargados=90,
            total_errores=5
        )
        assert checkpoint.total_errores == 5

    def test_punto_control_scraping_libros_pendientes(self):
        """PuntoControlScraping should calculate pending books."""
        from src.domain.bjv_entities import PuntoControlScraping

        checkpoint = PuntoControlScraping(
            id="checkpoint-001",
            ultima_pagina_procesada=10,
            ultimo_libro_id="123",
            total_libros_descubiertos=100,
            total_libros_descargados=75
        )
        assert checkpoint.libros_pendientes == 25
