"""
Tests for CAS Domain Events

Following RED-GREEN TDD: These tests define the expected behavior
for CAS/TAS jurisprudence scraper domain events.

Target: ~25 tests for events
"""
import pytest
from datetime import datetime


class TestBusquedaCASIniciada:
    """Tests for BusquedaCASIniciada event."""

    def test_creation_with_required_fields(self):
        """Create BusquedaCASIniciada with required fields."""
        from src.domain.cas_events import BusquedaCASIniciada

        event = BusquedaCASIniciada(
            termino_busqueda="transfer dispute",
        )
        assert event.termino_busqueda == "transfer dispute"

    def test_has_timestamp(self):
        """BusquedaCASIniciada has timestamp."""
        from src.domain.cas_events import BusquedaCASIniciada

        event = BusquedaCASIniciada(termino_busqueda="doping")
        assert event.timestamp is not None
        assert isinstance(event.timestamp, datetime)

    def test_has_filtros(self):
        """BusquedaCASIniciada can have filtros."""
        from src.domain.cas_events import BusquedaCASIniciada
        from src.domain.cas_value_objects import CategoriaDeporte

        event = BusquedaCASIniciada(
            termino_busqueda="contract",
            deporte=CategoriaDeporte.FUTBOL,
            anio=2023,
        )
        assert event.deporte == CategoriaDeporte.FUTBOL
        assert event.anio == 2023

    def test_immutability(self):
        """BusquedaCASIniciada should be immutable."""
        from src.domain.cas_events import BusquedaCASIniciada

        event = BusquedaCASIniciada(termino_busqueda="test")
        with pytest.raises(Exception):
            event.termino_busqueda = "modified"


class TestLaudoDescubierto:
    """Tests for LaudoDescubierto event."""

    def test_creation_with_required_fields(self):
        """Create LaudoDescubierto with required fields."""
        from src.domain.cas_events import LaudoDescubierto
        from src.domain.cas_value_objects import NumeroCaso

        event = LaudoDescubierto(
            numero_caso=NumeroCaso(valor="CAS 2023/A/12345"),
            titulo="Player X v. Club Y",
        )
        assert event.numero_caso.valor == "CAS 2023/A/12345"
        assert event.titulo == "Player X v. Club Y"

    def test_has_timestamp(self):
        """LaudoDescubierto has timestamp."""
        from src.domain.cas_events import LaudoDescubierto
        from src.domain.cas_value_objects import NumeroCaso

        event = LaudoDescubierto(
            numero_caso=NumeroCaso(valor="CAS 2023/A/12345"),
            titulo="Test Case",
        )
        assert event.timestamp is not None

    def test_has_url(self):
        """LaudoDescubierto can have URL."""
        from src.domain.cas_events import LaudoDescubierto
        from src.domain.cas_value_objects import NumeroCaso, URLLaudo

        event = LaudoDescubierto(
            numero_caso=NumeroCaso(valor="CAS 2023/A/12345"),
            titulo="Test Case",
            url=URLLaudo(valor="https://jurisprudence.tas-cas.org/doc.pdf"),
        )
        assert event.url is not None

    def test_immutability(self):
        """LaudoDescubierto should be immutable."""
        from src.domain.cas_events import LaudoDescubierto
        from src.domain.cas_value_objects import NumeroCaso

        event = LaudoDescubierto(
            numero_caso=NumeroCaso(valor="CAS 2023/A/12345"),
            titulo="Test",
        )
        with pytest.raises(Exception):
            event.titulo = "Modified"


class TestLaudoDescargado:
    """Tests for LaudoDescargado event."""

    def test_creation_with_required_fields(self):
        """Create LaudoDescargado with required fields."""
        from src.domain.cas_events import LaudoDescargado
        from src.domain.cas_value_objects import NumeroCaso

        event = LaudoDescargado(
            numero_caso=NumeroCaso(valor="CAS 2023/A/12345"),
            ruta_archivo="/data/cas/2023/12345.pdf",
        )
        assert event.ruta_archivo == "/data/cas/2023/12345.pdf"

    def test_has_tamano_bytes(self):
        """LaudoDescargado can have tamano_bytes."""
        from src.domain.cas_events import LaudoDescargado
        from src.domain.cas_value_objects import NumeroCaso

        event = LaudoDescargado(
            numero_caso=NumeroCaso(valor="CAS 2023/A/12345"),
            ruta_archivo="/data/cas/doc.pdf",
            tamano_bytes=1024000,
        )
        assert event.tamano_bytes == 1024000

    def test_has_timestamp(self):
        """LaudoDescargado has timestamp."""
        from src.domain.cas_events import LaudoDescargado
        from src.domain.cas_value_objects import NumeroCaso

        event = LaudoDescargado(
            numero_caso=NumeroCaso(valor="CAS 2023/A/12345"),
            ruta_archivo="/data/cas/doc.pdf",
        )
        assert event.timestamp is not None

    def test_immutability(self):
        """LaudoDescargado should be immutable."""
        from src.domain.cas_events import LaudoDescargado
        from src.domain.cas_value_objects import NumeroCaso

        event = LaudoDescargado(
            numero_caso=NumeroCaso(valor="CAS 2023/A/12345"),
            ruta_archivo="/data/cas/doc.pdf",
        )
        with pytest.raises(Exception):
            event.ruta_archivo = "/other/path.pdf"


class TestLaudoProcesado:
    """Tests for LaudoProcesado event."""

    def test_creation_with_required_fields(self):
        """Create LaudoProcesado with required fields."""
        from src.domain.cas_events import LaudoProcesado
        from src.domain.cas_value_objects import NumeroCaso

        event = LaudoProcesado(
            numero_caso=NumeroCaso(valor="CAS 2023/A/12345"),
            fragmentos_generados=15,
        )
        assert event.fragmentos_generados == 15

    def test_has_embeddings_generados(self):
        """LaudoProcesado can have embeddings_generados."""
        from src.domain.cas_events import LaudoProcesado
        from src.domain.cas_value_objects import NumeroCaso

        event = LaudoProcesado(
            numero_caso=NumeroCaso(valor="CAS 2023/A/12345"),
            fragmentos_generados=15,
            embeddings_generados=15,
        )
        assert event.embeddings_generados == 15

    def test_has_timestamp(self):
        """LaudoProcesado has timestamp."""
        from src.domain.cas_events import LaudoProcesado
        from src.domain.cas_value_objects import NumeroCaso

        event = LaudoProcesado(
            numero_caso=NumeroCaso(valor="CAS 2023/A/12345"),
            fragmentos_generados=10,
        )
        assert event.timestamp is not None


class TestErrorScrapingCAS:
    """Tests for ErrorScrapingCAS event."""

    def test_creation_with_required_fields(self):
        """Create ErrorScrapingCAS with required fields."""
        from src.domain.cas_events import ErrorScrapingCAS

        event = ErrorScrapingCAS(
            mensaje="Connection timeout",
            tipo_error="NetworkError",
        )
        assert event.mensaje == "Connection timeout"
        assert event.tipo_error == "NetworkError"

    def test_has_numero_caso_optional(self):
        """ErrorScrapingCAS can have numero_caso."""
        from src.domain.cas_events import ErrorScrapingCAS
        from src.domain.cas_value_objects import NumeroCaso

        event = ErrorScrapingCAS(
            mensaje="Failed to download",
            tipo_error="DownloadError",
            numero_caso=NumeroCaso(valor="CAS 2023/A/12345"),
        )
        assert event.numero_caso is not None

    def test_has_reintentable(self):
        """ErrorScrapingCAS has reintentable flag."""
        from src.domain.cas_events import ErrorScrapingCAS

        event = ErrorScrapingCAS(
            mensaje="Rate limited",
            tipo_error="RateLimitError",
            reintentable=True,
        )
        assert event.reintentable is True

    def test_has_timestamp(self):
        """ErrorScrapingCAS has timestamp."""
        from src.domain.cas_events import ErrorScrapingCAS

        event = ErrorScrapingCAS(mensaje="Error", tipo_error="GenericError")
        assert event.timestamp is not None


class TestScrapingCASCompletado:
    """Tests for ScrapingCASCompletado event."""

    def test_creation_with_required_fields(self):
        """Create ScrapingCASCompletado with required fields."""
        from src.domain.cas_events import ScrapingCASCompletado

        event = ScrapingCASCompletado(
            total_laudos_descubiertos=500,
            total_laudos_descargados=495,
            total_errores=5,
        )
        assert event.total_laudos_descubiertos == 500
        assert event.total_laudos_descargados == 495
        assert event.total_errores == 5

    def test_has_duracion_segundos(self):
        """ScrapingCASCompletado can have duracion_segundos."""
        from src.domain.cas_events import ScrapingCASCompletado

        event = ScrapingCASCompletado(
            total_laudos_descubiertos=100,
            total_laudos_descargados=100,
            total_errores=0,
            duracion_segundos=3600.5,
        )
        assert event.duracion_segundos == 3600.5

    def test_has_timestamp(self):
        """ScrapingCASCompletado has timestamp."""
        from src.domain.cas_events import ScrapingCASCompletado

        event = ScrapingCASCompletado(
            total_laudos_descubiertos=100,
            total_laudos_descargados=100,
            total_errores=0,
        )
        assert event.timestamp is not None


class TestScrapingCASPausado:
    """Tests for ScrapingCASPausado event."""

    def test_creation_with_required_fields(self):
        """Create ScrapingCASPausado with required fields."""
        from src.domain.cas_events import ScrapingCASPausado

        event = ScrapingCASPausado(
            razon="User requested pause",
            laudos_pendientes=50,
        )
        assert event.razon == "User requested pause"
        assert event.laudos_pendientes == 50

    def test_has_timestamp(self):
        """ScrapingCASPausado has timestamp."""
        from src.domain.cas_events import ScrapingCASPausado

        event = ScrapingCASPausado(razon="Pause", laudos_pendientes=10)
        assert event.timestamp is not None


class TestScrapingCASReanudado:
    """Tests for ScrapingCASReanudado event."""

    def test_creation_with_required_fields(self):
        """Create ScrapingCASReanudado with required fields."""
        from src.domain.cas_events import ScrapingCASReanudado

        event = ScrapingCASReanudado(
            checkpoint_id="chk-12345",
            laudos_restantes=50,
        )
        assert event.checkpoint_id == "chk-12345"
        assert event.laudos_restantes == 50

    def test_has_timestamp(self):
        """ScrapingCASReanudado has timestamp."""
        from src.domain.cas_events import ScrapingCASReanudado

        event = ScrapingCASReanudado(checkpoint_id="chk-123", laudos_restantes=10)
        assert event.timestamp is not None
