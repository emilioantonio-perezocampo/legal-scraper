"""
Tests for BJV Domain Events

RED phase: All tests should fail until implementation is complete.
Domain events in Spanish following DDD patterns.
All events are immutable frozen dataclasses.
"""
import pytest
from dataclasses import FrozenInstanceError
from datetime import datetime
from uuid import UUID


class TestBusquedaIniciada:
    """Tests for BusquedaIniciada event - search started."""

    def test_busqueda_iniciada_creation(self):
        """BusquedaIniciada should store search parameters."""
        from src.domain.bjv_events import BusquedaIniciada

        evento = BusquedaIniciada(
            busqueda_id="search-001",
            termino_busqueda="derecho civil",
            max_resultados=100
        )
        assert evento.busqueda_id == "search-001"
        assert evento.termino_busqueda == "derecho civil"
        assert evento.max_resultados == 100

    def test_busqueda_iniciada_is_immutable(self):
        """BusquedaIniciada should be frozen."""
        from src.domain.bjv_events import BusquedaIniciada

        evento = BusquedaIniciada(
            busqueda_id="search-001",
            termino_busqueda="test"
        )
        with pytest.raises(FrozenInstanceError):
            evento.termino_busqueda = "new term"

    def test_busqueda_iniciada_has_timestamp(self):
        """BusquedaIniciada should have automatic timestamp."""
        from src.domain.bjv_events import BusquedaIniciada

        evento = BusquedaIniciada(
            busqueda_id="search-001",
            termino_busqueda="test"
        )
        assert isinstance(evento.timestamp, datetime)

    def test_busqueda_iniciada_with_area_derecho(self):
        """BusquedaIniciada should optionally filter by area."""
        from src.domain.bjv_events import BusquedaIniciada
        from src.domain.bjv_value_objects import AreaDerecho

        evento = BusquedaIniciada(
            busqueda_id="search-001",
            termino_busqueda="contratos",
            area_derecho=AreaDerecho.CIVIL
        )
        assert evento.area_derecho == AreaDerecho.CIVIL


class TestLibroDescubierto:
    """Tests for LibroDescubierto event - book discovered."""

    def test_libro_descubierto_creation(self):
        """LibroDescubierto should store discovery info."""
        from src.domain.bjv_events import LibroDescubierto
        from src.domain.bjv_value_objects import IdentificadorLibro

        evento = LibroDescubierto(
            libro_id=IdentificadorLibro(bjv_id="12345"),
            titulo="Derecho Civil Mexicano",
            url="https://biblio.juridicas.unam.mx/bjv/detalle/12345"
        )
        assert evento.libro_id.bjv_id == "12345"
        assert evento.titulo == "Derecho Civil Mexicano"

    def test_libro_descubierto_is_immutable(self):
        """LibroDescubierto should be frozen."""
        from src.domain.bjv_events import LibroDescubierto
        from src.domain.bjv_value_objects import IdentificadorLibro

        evento = LibroDescubierto(
            libro_id=IdentificadorLibro(bjv_id="12345"),
            titulo="Test",
            url="https://example.com"
        )
        with pytest.raises(FrozenInstanceError):
            evento.titulo = "New Title"

    def test_libro_descubierto_has_timestamp(self):
        """LibroDescubierto should have automatic timestamp."""
        from src.domain.bjv_events import LibroDescubierto
        from src.domain.bjv_value_objects import IdentificadorLibro

        evento = LibroDescubierto(
            libro_id=IdentificadorLibro(bjv_id="12345"),
            titulo="Test",
            url="https://example.com"
        )
        assert isinstance(evento.timestamp, datetime)


class TestLibroDescargado:
    """Tests for LibroDescargado event - book downloaded."""

    def test_libro_descargado_creation(self):
        """LibroDescargado should store download info."""
        from src.domain.bjv_events import LibroDescargado
        from src.domain.bjv_value_objects import IdentificadorLibro

        evento = LibroDescargado(
            libro_id=IdentificadorLibro(bjv_id="12345"),
            ruta_archivo="/data/libros/12345.pdf",
            tamano_bytes=1048576
        )
        assert evento.libro_id.bjv_id == "12345"
        assert evento.ruta_archivo == "/data/libros/12345.pdf"
        assert evento.tamano_bytes == 1048576

    def test_libro_descargado_is_immutable(self):
        """LibroDescargado should be frozen."""
        from src.domain.bjv_events import LibroDescargado
        from src.domain.bjv_value_objects import IdentificadorLibro

        evento = LibroDescargado(
            libro_id=IdentificadorLibro(bjv_id="12345"),
            ruta_archivo="/data/test.pdf",
            tamano_bytes=1000
        )
        with pytest.raises(FrozenInstanceError):
            evento.ruta_archivo = "/new/path.pdf"

    def test_libro_descargado_tamano_mb_property(self):
        """LibroDescargado should calculate size in MB."""
        from src.domain.bjv_events import LibroDescargado
        from src.domain.bjv_value_objects import IdentificadorLibro

        evento = LibroDescargado(
            libro_id=IdentificadorLibro(bjv_id="12345"),
            ruta_archivo="/data/test.pdf",
            tamano_bytes=5242880  # 5 MB
        )
        assert evento.tamano_mb == 5.0


class TestLibroProcesado:
    """Tests for LibroProcesado event - book processed/parsed."""

    def test_libro_procesado_creation(self):
        """LibroProcesado should store processing info."""
        from src.domain.bjv_events import LibroProcesado
        from src.domain.bjv_value_objects import IdentificadorLibro

        evento = LibroProcesado(
            libro_id=IdentificadorLibro(bjv_id="12345"),
            total_paginas=350,
            total_fragmentos=120
        )
        assert evento.total_paginas == 350
        assert evento.total_fragmentos == 120

    def test_libro_procesado_is_immutable(self):
        """LibroProcesado should be frozen."""
        from src.domain.bjv_events import LibroProcesado
        from src.domain.bjv_value_objects import IdentificadorLibro

        evento = LibroProcesado(
            libro_id=IdentificadorLibro(bjv_id="12345"),
            total_paginas=100,
            total_fragmentos=50
        )
        with pytest.raises(FrozenInstanceError):
            evento.total_fragmentos = 100


class TestFragmentoIndexado:
    """Tests for FragmentoIndexado event - fragment indexed with embedding."""

    def test_fragmento_indexado_creation(self):
        """FragmentoIndexado should store indexing info."""
        from src.domain.bjv_events import FragmentoIndexado
        from src.domain.bjv_value_objects import IdentificadorLibro

        evento = FragmentoIndexado(
            fragmento_id="frag-001",
            libro_id=IdentificadorLibro(bjv_id="12345"),
            modelo_embedding="text-embedding-3-small"
        )
        assert evento.fragmento_id == "frag-001"
        assert evento.modelo_embedding == "text-embedding-3-small"

    def test_fragmento_indexado_is_immutable(self):
        """FragmentoIndexado should be frozen."""
        from src.domain.bjv_events import FragmentoIndexado
        from src.domain.bjv_value_objects import IdentificadorLibro

        evento = FragmentoIndexado(
            fragmento_id="frag-001",
            libro_id=IdentificadorLibro(bjv_id="12345"),
            modelo_embedding="test"
        )
        with pytest.raises(FrozenInstanceError):
            evento.modelo_embedding = "new-model"


class TestErrorScraping:
    """Tests for ErrorScraping event - scraping error occurred."""

    def test_error_scraping_creation(self):
        """ErrorScraping should store error info."""
        from src.domain.bjv_events import ErrorScraping
        from src.domain.bjv_value_objects import IdentificadorLibro

        evento = ErrorScraping(
            libro_id=IdentificadorLibro(bjv_id="12345"),
            mensaje_error="Connection timeout",
            codigo_error="TIMEOUT"
        )
        assert evento.mensaje_error == "Connection timeout"
        assert evento.codigo_error == "TIMEOUT"

    def test_error_scraping_is_immutable(self):
        """ErrorScraping should be frozen."""
        from src.domain.bjv_events import ErrorScraping
        from src.domain.bjv_value_objects import IdentificadorLibro

        evento = ErrorScraping(
            libro_id=IdentificadorLibro(bjv_id="12345"),
            mensaje_error="Error",
            codigo_error="ERR"
        )
        with pytest.raises(FrozenInstanceError):
            evento.mensaje_error = "New error"

    def test_error_scraping_es_recuperable(self):
        """ErrorScraping should identify recoverable errors."""
        from src.domain.bjv_events import ErrorScraping
        from src.domain.bjv_value_objects import IdentificadorLibro

        evento = ErrorScraping(
            libro_id=IdentificadorLibro(bjv_id="12345"),
            mensaje_error="Timeout",
            codigo_error="TIMEOUT",
            es_recuperable=True
        )
        assert evento.es_recuperable is True


class TestScrapingCompletado:
    """Tests for ScrapingCompletado event - scraping completed."""

    def test_scraping_completado_creation(self):
        """ScrapingCompletado should store completion stats."""
        from src.domain.bjv_events import ScrapingCompletado

        evento = ScrapingCompletado(
            total_libros_descubiertos=6840,
            total_libros_descargados=6500,
            total_errores=340,
            duracion_segundos=3600.5
        )
        assert evento.total_libros_descubiertos == 6840
        assert evento.total_libros_descargados == 6500
        assert evento.duracion_segundos == 3600.5

    def test_scraping_completado_is_immutable(self):
        """ScrapingCompletado should be frozen."""
        from src.domain.bjv_events import ScrapingCompletado

        evento = ScrapingCompletado(
            total_libros_descubiertos=100,
            total_libros_descargados=90,
            total_errores=10,
            duracion_segundos=60.0
        )
        with pytest.raises(FrozenInstanceError):
            evento.total_errores = 0

    def test_scraping_completado_tasa_exito(self):
        """ScrapingCompletado should calculate success rate."""
        from src.domain.bjv_events import ScrapingCompletado

        evento = ScrapingCompletado(
            total_libros_descubiertos=100,
            total_libros_descargados=80,
            total_errores=20,
            duracion_segundos=60.0
        )
        assert evento.tasa_exito == 80.0

    def test_scraping_completado_duracion_formateada(self):
        """ScrapingCompletado should format duration as HH:MM:SS."""
        from src.domain.bjv_events import ScrapingCompletado

        evento = ScrapingCompletado(
            total_libros_descubiertos=100,
            total_libros_descargados=100,
            total_errores=0,
            duracion_segundos=3661.0  # 1h 1m 1s
        )
        assert evento.duracion_formateada == "01:01:01"


class TestPuntoControlCreado:
    """Tests for PuntoControlCreado event - checkpoint created."""

    def test_punto_control_creado_creation(self):
        """PuntoControlCreado should store checkpoint info."""
        from src.domain.bjv_events import PuntoControlCreado

        evento = PuntoControlCreado(
            checkpoint_id="cp-001",
            pagina_actual=42,
            libros_procesados=500
        )
        assert evento.checkpoint_id == "cp-001"
        assert evento.pagina_actual == 42
        assert evento.libros_procesados == 500

    def test_punto_control_creado_is_immutable(self):
        """PuntoControlCreado should be frozen."""
        from src.domain.bjv_events import PuntoControlCreado

        evento = PuntoControlCreado(
            checkpoint_id="cp-001",
            pagina_actual=10,
            libros_procesados=100
        )
        with pytest.raises(FrozenInstanceError):
            evento.pagina_actual = 20


class TestScrapingPausado:
    """Tests for ScrapingPausado event - scraping paused."""

    def test_scraping_pausado_creation(self):
        """ScrapingPausado should store pause info."""
        from src.domain.bjv_events import ScrapingPausado

        evento = ScrapingPausado(
            razon="Rate limit reached",
            libros_pendientes=1000
        )
        assert evento.razon == "Rate limit reached"
        assert evento.libros_pendientes == 1000

    def test_scraping_pausado_is_immutable(self):
        """ScrapingPausado should be frozen."""
        from src.domain.bjv_events import ScrapingPausado

        evento = ScrapingPausado(
            razon="User request",
            libros_pendientes=500
        )
        with pytest.raises(FrozenInstanceError):
            evento.razon = "New reason"


class TestScrapingReanudado:
    """Tests for ScrapingReanudado event - scraping resumed."""

    def test_scraping_reanudado_creation(self):
        """ScrapingReanudado should store resume info."""
        from src.domain.bjv_events import ScrapingReanudado

        evento = ScrapingReanudado(
            checkpoint_id="cp-001",
            libros_restantes=500
        )
        assert evento.checkpoint_id == "cp-001"
        assert evento.libros_restantes == 500

    def test_scraping_reanudado_is_immutable(self):
        """ScrapingReanudado should be frozen."""
        from src.domain.bjv_events import ScrapingReanudado

        evento = ScrapingReanudado(
            checkpoint_id="cp-001",
            libros_restantes=100
        )
        with pytest.raises(FrozenInstanceError):
            evento.libros_restantes = 50
