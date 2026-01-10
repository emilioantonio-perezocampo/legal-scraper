"""
Tests for CAS Actor Messages

Following RED-GREEN TDD: These tests define the expected behavior
for the CAS pipeline message types.

Target: ~35 tests for CAS messages
"""
import pytest
from datetime import datetime


class TestEstadoPipelineCAS:
    """Tests for EstadoPipelineCAS enum."""

    def test_has_iniciando_state(self):
        """Enum has INICIANDO state."""
        from src.infrastructure.actors.cas_messages import EstadoPipelineCAS
        assert hasattr(EstadoPipelineCAS, 'INICIANDO')

    def test_has_descubriendo_state(self):
        """Enum has DESCUBRIENDO state."""
        from src.infrastructure.actors.cas_messages import EstadoPipelineCAS
        assert hasattr(EstadoPipelineCAS, 'DESCUBRIENDO')

    def test_has_scrapeando_state(self):
        """Enum has SCRAPEANDO state."""
        from src.infrastructure.actors.cas_messages import EstadoPipelineCAS
        assert hasattr(EstadoPipelineCAS, 'SCRAPEANDO')

    def test_has_fragmentando_state(self):
        """Enum has FRAGMENTANDO state."""
        from src.infrastructure.actors.cas_messages import EstadoPipelineCAS
        assert hasattr(EstadoPipelineCAS, 'FRAGMENTANDO')

    def test_has_completado_state(self):
        """Enum has COMPLETADO state."""
        from src.infrastructure.actors.cas_messages import EstadoPipelineCAS
        assert hasattr(EstadoPipelineCAS, 'COMPLETADO')

    def test_has_error_state(self):
        """Enum has ERROR state."""
        from src.infrastructure.actors.cas_messages import EstadoPipelineCAS
        assert hasattr(EstadoPipelineCAS, 'ERROR')

    def test_has_pausado_state(self):
        """Enum has PAUSADO state."""
        from src.infrastructure.actors.cas_messages import EstadoPipelineCAS
        assert hasattr(EstadoPipelineCAS, 'PAUSADO')


class TestIniciarDescubrimientoCAS:
    """Tests for IniciarDescubrimientoCAS message."""

    def test_is_frozen_dataclass(self):
        """Message is a frozen dataclass."""
        from src.infrastructure.actors.cas_messages import IniciarDescubrimientoCAS
        msg = IniciarDescubrimientoCAS()
        with pytest.raises(Exception):
            msg.max_paginas = 10

    def test_default_max_paginas(self):
        """Default max_paginas is None (unlimited)."""
        from src.infrastructure.actors.cas_messages import IniciarDescubrimientoCAS
        msg = IniciarDescubrimientoCAS()
        assert msg.max_paginas is None

    def test_custom_max_paginas(self):
        """Can set custom max_paginas."""
        from src.infrastructure.actors.cas_messages import IniciarDescubrimientoCAS
        msg = IniciarDescubrimientoCAS(max_paginas=5)
        assert msg.max_paginas == 5

    def test_default_filtros(self):
        """Default filtros is None."""
        from src.infrastructure.actors.cas_messages import IniciarDescubrimientoCAS
        msg = IniciarDescubrimientoCAS()
        assert msg.filtros is None

    def test_custom_filtros(self):
        """Can set custom filtros dict."""
        from src.infrastructure.actors.cas_messages import IniciarDescubrimientoCAS
        filtros = {"year": 2023, "sport": "football"}
        msg = IniciarDescubrimientoCAS(filtros=filtros)
        assert msg.filtros == filtros


class TestLaudoDescubiertoCAS:
    """Tests for LaudoDescubiertoCAS message."""

    def test_is_frozen_dataclass(self):
        """Message is a frozen dataclass."""
        from src.infrastructure.actors.cas_messages import LaudoDescubiertoCAS
        msg = LaudoDescubiertoCAS(numero_caso="CAS 2023/A/12345", url="https://example.com")
        with pytest.raises(Exception):
            msg.numero_caso = "different"

    def test_has_numero_caso(self):
        """Message has numero_caso field."""
        from src.infrastructure.actors.cas_messages import LaudoDescubiertoCAS
        msg = LaudoDescubiertoCAS(numero_caso="CAS 2023/A/12345", url="https://example.com")
        assert msg.numero_caso == "CAS 2023/A/12345"

    def test_has_url(self):
        """Message has url field."""
        from src.infrastructure.actors.cas_messages import LaudoDescubiertoCAS
        msg = LaudoDescubiertoCAS(numero_caso="CAS 2023/A/12345", url="https://example.com/award")
        assert msg.url == "https://example.com/award"

    def test_default_titulo(self):
        """Default titulo is empty string."""
        from src.infrastructure.actors.cas_messages import LaudoDescubiertoCAS
        msg = LaudoDescubiertoCAS(numero_caso="CAS 2023/A/12345", url="https://example.com")
        assert msg.titulo == ""

    def test_custom_titulo(self):
        """Can set custom titulo."""
        from src.infrastructure.actors.cas_messages import LaudoDescubiertoCAS
        msg = LaudoDescubiertoCAS(
            numero_caso="CAS 2023/A/12345",
            url="https://example.com",
            titulo="Club A v. Club B"
        )
        assert msg.titulo == "Club A v. Club B"


class TestScrapearLaudoCAS:
    """Tests for ScrapearLaudoCAS message."""

    def test_is_frozen_dataclass(self):
        """Message is a frozen dataclass."""
        from src.infrastructure.actors.cas_messages import ScrapearLaudoCAS
        msg = ScrapearLaudoCAS(numero_caso="CAS 2023/A/12345", url="https://example.com")
        with pytest.raises(Exception):
            msg.url = "different"

    def test_has_required_fields(self):
        """Message has required fields."""
        from src.infrastructure.actors.cas_messages import ScrapearLaudoCAS
        msg = ScrapearLaudoCAS(numero_caso="CAS 2023/A/12345", url="https://example.com")
        assert msg.numero_caso == "CAS 2023/A/12345"
        assert msg.url == "https://example.com"

    def test_default_reintentos(self):
        """Default reintentos is 0."""
        from src.infrastructure.actors.cas_messages import ScrapearLaudoCAS
        msg = ScrapearLaudoCAS(numero_caso="CAS 2023/A/12345", url="https://example.com")
        assert msg.reintentos == 0


class TestLaudoScrapeadoCAS:
    """Tests for LaudoScrapeadoCAS message."""

    def test_is_frozen_dataclass(self):
        """Message is a frozen dataclass."""
        from datetime import date
        from src.infrastructure.actors.cas_messages import LaudoScrapeadoCAS
        from src.domain.cas_entities import LaudoArbitral
        from src.domain.cas_value_objects import NumeroCaso, FechaLaudo
        laudo = LaudoArbitral(
            id="test-id",
            numero_caso=NumeroCaso(valor="CAS 2023/A/12345"),
            titulo="Test v. Test",
            fecha=FechaLaudo(valor=date(2023, 1, 1))
        )
        msg = LaudoScrapeadoCAS(laudo=laudo)
        with pytest.raises(Exception):
            msg.laudo = None

    def test_has_laudo(self):
        """Message has laudo field."""
        from datetime import date
        from src.infrastructure.actors.cas_messages import LaudoScrapeadoCAS
        from src.domain.cas_entities import LaudoArbitral
        from src.domain.cas_value_objects import NumeroCaso, FechaLaudo
        laudo = LaudoArbitral(
            id="test-id",
            numero_caso=NumeroCaso(valor="CAS 2023/A/12345"),
            titulo="Test v. Test",
            fecha=FechaLaudo(valor=date(2023, 1, 1))
        )
        msg = LaudoScrapeadoCAS(laudo=laudo)
        assert msg.laudo == laudo


class TestFragmentarLaudoCAS:
    """Tests for FragmentarLaudoCAS message."""

    def test_is_frozen_dataclass(self):
        """Message is a frozen dataclass."""
        from src.infrastructure.actors.cas_messages import FragmentarLaudoCAS
        msg = FragmentarLaudoCAS(laudo_id="laudo-123", texto="Some text")
        with pytest.raises(Exception):
            msg.texto = "different"

    def test_has_required_fields(self):
        """Message has required fields."""
        from src.infrastructure.actors.cas_messages import FragmentarLaudoCAS
        msg = FragmentarLaudoCAS(laudo_id="laudo-123", texto="Award text content")
        assert msg.laudo_id == "laudo-123"
        assert msg.texto == "Award text content"


class TestFragmentosListosCAS:
    """Tests for FragmentosListosCAS message."""

    def test_is_frozen_dataclass(self):
        """Message is a frozen dataclass."""
        from src.infrastructure.actors.cas_messages import FragmentosListosCAS
        msg = FragmentosListosCAS(laudo_id="laudo-123", fragmentos=())
        with pytest.raises(Exception):
            msg.fragmentos = ()

    def test_has_laudo_id(self):
        """Message has laudo_id field."""
        from src.infrastructure.actors.cas_messages import FragmentosListosCAS
        msg = FragmentosListosCAS(laudo_id="laudo-123", fragmentos=())
        assert msg.laudo_id == "laudo-123"

    def test_has_fragmentos_tuple(self):
        """Message has fragmentos as tuple."""
        from src.infrastructure.actors.cas_messages import FragmentosListosCAS
        from src.domain.cas_entities import FragmentoLaudo
        fragmentos = (
            FragmentoLaudo(id="f1", laudo_id="laudo-123", texto="text1", posicion=0),
            FragmentoLaudo(id="f2", laudo_id="laudo-123", texto="text2", posicion=1),
        )
        msg = FragmentosListosCAS(laudo_id="laudo-123", fragmentos=fragmentos)
        assert len(msg.fragmentos) == 2


class TestErrorCASPipeline:
    """Tests for ErrorCASPipeline message."""

    def test_is_frozen_dataclass(self):
        """Message is a frozen dataclass."""
        from src.infrastructure.actors.cas_messages import ErrorCASPipeline
        msg = ErrorCASPipeline(mensaje="Error occurred", tipo_error="ParseError")
        with pytest.raises(Exception):
            msg.mensaje = "different"

    def test_has_mensaje(self):
        """Message has mensaje field."""
        from src.infrastructure.actors.cas_messages import ErrorCASPipeline
        msg = ErrorCASPipeline(mensaje="Parse failed", tipo_error="ParseError")
        assert msg.mensaje == "Parse failed"

    def test_has_tipo_error(self):
        """Message has tipo_error field."""
        from src.infrastructure.actors.cas_messages import ErrorCASPipeline
        msg = ErrorCASPipeline(mensaje="Error", tipo_error="BrowserTimeoutError")
        assert msg.tipo_error == "BrowserTimeoutError"

    def test_default_recuperable(self):
        """Default recuperable is True."""
        from src.infrastructure.actors.cas_messages import ErrorCASPipeline
        msg = ErrorCASPipeline(mensaje="Error", tipo_error="ParseError")
        assert msg.recuperable is True

    def test_custom_recuperable(self):
        """Can set recuperable to False."""
        from src.infrastructure.actors.cas_messages import ErrorCASPipeline
        msg = ErrorCASPipeline(mensaje="Fatal", tipo_error="FatalError", recuperable=False)
        assert msg.recuperable is False

    def test_default_contexto(self):
        """Default contexto is None."""
        from src.infrastructure.actors.cas_messages import ErrorCASPipeline
        msg = ErrorCASPipeline(mensaje="Error", tipo_error="ParseError")
        assert msg.contexto is None

    def test_custom_contexto(self):
        """Can set contexto dict."""
        from src.infrastructure.actors.cas_messages import ErrorCASPipeline
        ctx = {"url": "https://example.com", "case": "CAS 2023/A/12345"}
        msg = ErrorCASPipeline(mensaje="Error", tipo_error="ParseError", contexto=ctx)
        assert msg.contexto == ctx


class TestEstadisticasPipelineCAS:
    """Tests for EstadisticasPipelineCAS message."""

    def test_is_frozen_dataclass(self):
        """Message is a frozen dataclass."""
        from src.infrastructure.actors.cas_messages import EstadisticasPipelineCAS
        stats = EstadisticasPipelineCAS()
        with pytest.raises(Exception):
            stats.laudos_descubiertos = 10

    def test_default_values(self):
        """Default values are all zero."""
        from src.infrastructure.actors.cas_messages import EstadisticasPipelineCAS
        stats = EstadisticasPipelineCAS()
        assert stats.laudos_descubiertos == 0
        assert stats.laudos_scrapeados == 0
        assert stats.laudos_fragmentados == 0
        assert stats.errores == 0

    def test_custom_values(self):
        """Can set custom values."""
        from src.infrastructure.actors.cas_messages import EstadisticasPipelineCAS
        stats = EstadisticasPipelineCAS(
            laudos_descubiertos=100,
            laudos_scrapeados=95,
            laudos_fragmentados=90,
            errores=5
        )
        assert stats.laudos_descubiertos == 100
        assert stats.laudos_scrapeados == 95
        assert stats.laudos_fragmentados == 90
        assert stats.errores == 5

    def test_tasa_exito_calculation(self):
        """tasa_exito calculates success rate correctly."""
        from src.infrastructure.actors.cas_messages import EstadisticasPipelineCAS
        stats = EstadisticasPipelineCAS(
            laudos_descubiertos=100,
            laudos_scrapeados=80,
            errores=20
        )
        assert stats.tasa_exito == 0.8

    def test_tasa_exito_zero_discovered(self):
        """tasa_exito returns 0.0 when no awards discovered."""
        from src.infrastructure.actors.cas_messages import EstadisticasPipelineCAS
        stats = EstadisticasPipelineCAS()
        assert stats.tasa_exito == 0.0

    def test_con_incremento_returns_new_instance(self):
        """con_incremento returns new instance with incremented value."""
        from src.infrastructure.actors.cas_messages import EstadisticasPipelineCAS
        stats = EstadisticasPipelineCAS(laudos_descubiertos=10)
        new_stats = stats.con_incremento("laudos_descubiertos")
        assert new_stats.laudos_descubiertos == 11
        assert stats.laudos_descubiertos == 10  # Original unchanged

    def test_con_incremento_different_fields(self):
        """con_incremento works for different fields."""
        from src.infrastructure.actors.cas_messages import EstadisticasPipelineCAS
        stats = EstadisticasPipelineCAS()
        stats = stats.con_incremento("laudos_scrapeados")
        stats = stats.con_incremento("laudos_fragmentados")
        stats = stats.con_incremento("errores")
        assert stats.laudos_scrapeados == 1
        assert stats.laudos_fragmentados == 1
        assert stats.errores == 1


class TestPausarPipelineCAS:
    """Tests for PausarPipelineCAS message."""

    def test_is_frozen_dataclass(self):
        """Message is a frozen dataclass."""
        from src.infrastructure.actors.cas_messages import PausarPipelineCAS
        msg = PausarPipelineCAS()
        with pytest.raises(Exception):
            msg.razon = "different"

    def test_default_razon(self):
        """Default razon is 'usuario'."""
        from src.infrastructure.actors.cas_messages import PausarPipelineCAS
        msg = PausarPipelineCAS()
        assert msg.razon == "usuario"

    def test_custom_razon(self):
        """Can set custom razon."""
        from src.infrastructure.actors.cas_messages import PausarPipelineCAS
        msg = PausarPipelineCAS(razon="rate_limit")
        assert msg.razon == "rate_limit"


class TestReanudarPipelineCAS:
    """Tests for ReanudarPipelineCAS message."""

    def test_is_frozen_dataclass(self):
        """Message is a frozen dataclass."""
        from src.infrastructure.actors.cas_messages import ReanudarPipelineCAS
        msg = ReanudarPipelineCAS()
        with pytest.raises(Exception):
            msg.desde_checkpoint = True

    def test_default_desde_checkpoint(self):
        """Default desde_checkpoint is False."""
        from src.infrastructure.actors.cas_messages import ReanudarPipelineCAS
        msg = ReanudarPipelineCAS()
        assert msg.desde_checkpoint is False


class TestDetenerPipelineCAS:
    """Tests for DetenerPipelineCAS message."""

    def test_is_frozen_dataclass(self):
        """Message is a frozen dataclass."""
        from src.infrastructure.actors.cas_messages import DetenerPipelineCAS
        msg = DetenerPipelineCAS()
        with pytest.raises(Exception):
            msg.guardar_progreso = False

    def test_default_guardar_progreso(self):
        """Default guardar_progreso is True."""
        from src.infrastructure.actors.cas_messages import DetenerPipelineCAS
        msg = DetenerPipelineCAS()
        assert msg.guardar_progreso is True


class TestObtenerEstadoCAS:
    """Tests for ObtenerEstadoCAS message."""

    def test_is_frozen_dataclass(self):
        """Message is a frozen dataclass."""
        from src.infrastructure.actors.cas_messages import ObtenerEstadoCAS
        msg = ObtenerEstadoCAS()
        with pytest.raises(Exception):
            msg.incluir_estadisticas = False

    def test_default_incluir_estadisticas(self):
        """Default incluir_estadisticas is True."""
        from src.infrastructure.actors.cas_messages import ObtenerEstadoCAS
        msg = ObtenerEstadoCAS()
        assert msg.incluir_estadisticas is True
