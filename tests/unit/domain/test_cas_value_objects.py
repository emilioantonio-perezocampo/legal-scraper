"""
Tests for CAS Domain Value Objects

Following RED-GREEN TDD: These tests define the expected behavior
for CAS/TAS jurisprudence scraper value objects.

Target: ~35 tests for value objects
"""
import pytest
from datetime import date


class TestTipoProcedimiento:
    """Tests for TipoProcedimiento enum."""

    def test_arbitraje_ordinario_value(self):
        """ARBITRAJE_ORDINARIO has correct value."""
        from src.domain.cas_value_objects import TipoProcedimiento
        assert TipoProcedimiento.ARBITRAJE_ORDINARIO.value == "ordinary"

    def test_arbitraje_apelacion_value(self):
        """ARBITRAJE_APELACION has correct value."""
        from src.domain.cas_value_objects import TipoProcedimiento
        assert TipoProcedimiento.ARBITRAJE_APELACION.value == "appeal"

    def test_arbitraje_antidopaje_value(self):
        """ARBITRAJE_ANTIDOPAJE has correct value."""
        from src.domain.cas_value_objects import TipoProcedimiento
        assert TipoProcedimiento.ARBITRAJE_ANTIDOPAJE.value == "anti-doping"

    def test_mediacion_value(self):
        """MEDIACION has correct value."""
        from src.domain.cas_value_objects import TipoProcedimiento
        assert TipoProcedimiento.MEDIACION.value == "mediation"

    def test_opinion_consultiva_value(self):
        """OPINION_CONSULTIVA has correct value."""
        from src.domain.cas_value_objects import TipoProcedimiento
        assert TipoProcedimiento.OPINION_CONSULTIVA.value == "advisory"


class TestCategoriaDeporte:
    """Tests for CategoriaDeporte enum."""

    def test_futbol_value(self):
        """FUTBOL has correct value."""
        from src.domain.cas_value_objects import CategoriaDeporte
        assert CategoriaDeporte.FUTBOL.value == "football"

    def test_atletismo_value(self):
        """ATLETISMO has correct value."""
        from src.domain.cas_value_objects import CategoriaDeporte
        assert CategoriaDeporte.ATLETISMO.value == "athletics"

    def test_ciclismo_value(self):
        """CICLISMO has correct value."""
        from src.domain.cas_value_objects import CategoriaDeporte
        assert CategoriaDeporte.CICLISMO.value == "cycling"

    def test_natacion_value(self):
        """NATACION has correct value."""
        from src.domain.cas_value_objects import CategoriaDeporte
        assert CategoriaDeporte.NATACION.value == "swimming"

    def test_baloncesto_value(self):
        """BALONCESTO has correct value."""
        from src.domain.cas_value_objects import CategoriaDeporte
        assert CategoriaDeporte.BALONCESTO.value == "basketball"

    def test_tenis_value(self):
        """TENIS has correct value."""
        from src.domain.cas_value_objects import CategoriaDeporte
        assert CategoriaDeporte.TENIS.value == "tennis"

    def test_esqui_value(self):
        """ESQUI has correct value."""
        from src.domain.cas_value_objects import CategoriaDeporte
        assert CategoriaDeporte.ESQUI.value == "skiing"

    def test_otro_value(self):
        """OTRO has correct value."""
        from src.domain.cas_value_objects import CategoriaDeporte
        assert CategoriaDeporte.OTRO.value == "other"


class TestTipoMateria:
    """Tests for TipoMateria enum."""

    def test_dopaje_value(self):
        """DOPAJE has correct value."""
        from src.domain.cas_value_objects import TipoMateria
        assert TipoMateria.DOPAJE.value == "doping"

    def test_transferencia_value(self):
        """TRANSFERENCIA has correct value."""
        from src.domain.cas_value_objects import TipoMateria
        assert TipoMateria.TRANSFERENCIA.value == "transfer"

    def test_elegibilidad_value(self):
        """ELEGIBILIDAD has correct value."""
        from src.domain.cas_value_objects import TipoMateria
        assert TipoMateria.ELEGIBILIDAD.value == "eligibility"

    def test_disciplina_value(self):
        """DISCIPLINA has correct value."""
        from src.domain.cas_value_objects import TipoMateria
        assert TipoMateria.DISCIPLINA.value == "disciplinary"

    def test_contractual_value(self):
        """CONTRACTUAL has correct value."""
        from src.domain.cas_value_objects import TipoMateria
        assert TipoMateria.CONTRACTUAL.value == "contractual"

    def test_gobernanza_value(self):
        """GOBERNANZA has correct value."""
        from src.domain.cas_value_objects import TipoMateria
        assert TipoMateria.GOBERNANZA.value == "governance"

    def test_otro_value(self):
        """OTRO materia has correct value."""
        from src.domain.cas_value_objects import TipoMateria
        assert TipoMateria.OTRO.value == "other"


class TestNumeroCaso:
    """Tests for NumeroCaso value object."""

    def test_creation_with_valid_cas_format(self):
        """Create NumeroCaso with valid CAS format."""
        from src.domain.cas_value_objects import NumeroCaso
        numero = NumeroCaso(valor="CAS 2023/A/12345")
        assert numero.valor == "CAS 2023/A/12345"

    def test_creation_with_valid_tas_format(self):
        """Create NumeroCaso with valid TAS format."""
        from src.domain.cas_value_objects import NumeroCaso
        numero = NumeroCaso(valor="TAS 2023/A/12345")
        assert numero.valor == "TAS 2023/A/12345"

    def test_immutability(self):
        """NumeroCaso should be immutable (frozen dataclass)."""
        from src.domain.cas_value_objects import NumeroCaso
        numero = NumeroCaso(valor="CAS 2023/A/12345")
        with pytest.raises(Exception):  # FrozenInstanceError
            numero.valor = "CAS 2024/A/99999"

    def test_equality(self):
        """Two NumeroCaso with same valor are equal."""
        from src.domain.cas_value_objects import NumeroCaso
        n1 = NumeroCaso(valor="CAS 2023/A/12345")
        n2 = NumeroCaso(valor="CAS 2023/A/12345")
        assert n1 == n2

    def test_hash_equality(self):
        """Equal NumeroCaso have equal hashes."""
        from src.domain.cas_value_objects import NumeroCaso
        n1 = NumeroCaso(valor="CAS 2023/A/12345")
        n2 = NumeroCaso(valor="CAS 2023/A/12345")
        assert hash(n1) == hash(n2)

    def test_different_numbers_not_equal(self):
        """Different NumeroCaso are not equal."""
        from src.domain.cas_value_objects import NumeroCaso
        n1 = NumeroCaso(valor="CAS 2023/A/12345")
        n2 = NumeroCaso(valor="CAS 2023/A/12346")
        assert n1 != n2


class TestURLLaudo:
    """Tests for URLLaudo value object."""

    def test_creation_with_valid_url(self):
        """Create URLLaudo with valid CAS URL."""
        from src.domain.cas_value_objects import URLLaudo
        url = URLLaudo(valor="https://jurisprudence.tas-cas.org/Shared%20Documents/12345.pdf")
        assert "jurisprudence.tas-cas.org" in url.valor

    def test_immutability(self):
        """URLLaudo should be immutable."""
        from src.domain.cas_value_objects import URLLaudo
        url = URLLaudo(valor="https://jurisprudence.tas-cas.org/doc.pdf")
        with pytest.raises(Exception):
            url.valor = "https://other.com/doc.pdf"

    def test_equality(self):
        """Two URLLaudo with same valor are equal."""
        from src.domain.cas_value_objects import URLLaudo
        u1 = URLLaudo(valor="https://jurisprudence.tas-cas.org/doc.pdf")
        u2 = URLLaudo(valor="https://jurisprudence.tas-cas.org/doc.pdf")
        assert u1 == u2


class TestFechaLaudo:
    """Tests for FechaLaudo value object."""

    def test_creation_with_date(self):
        """Create FechaLaudo with date object."""
        from src.domain.cas_value_objects import FechaLaudo
        fecha = FechaLaudo(valor=date(2023, 6, 15))
        assert fecha.valor == date(2023, 6, 15)

    def test_immutability(self):
        """FechaLaudo should be immutable."""
        from src.domain.cas_value_objects import FechaLaudo
        fecha = FechaLaudo(valor=date(2023, 6, 15))
        with pytest.raises(Exception):
            fecha.valor = date(2024, 1, 1)

    def test_anio_property(self):
        """FechaLaudo.anio returns the year."""
        from src.domain.cas_value_objects import FechaLaudo
        fecha = FechaLaudo(valor=date(2023, 6, 15))
        assert fecha.anio == 2023

    def test_equality(self):
        """Two FechaLaudo with same date are equal."""
        from src.domain.cas_value_objects import FechaLaudo
        f1 = FechaLaudo(valor=date(2023, 6, 15))
        f2 = FechaLaudo(valor=date(2023, 6, 15))
        assert f1 == f2


class TestIdiomaLaudo:
    """Tests for IdiomaLaudo enum."""

    def test_ingles_value(self):
        """INGLES has correct value."""
        from src.domain.cas_value_objects import IdiomaLaudo
        assert IdiomaLaudo.INGLES.value == "en"

    def test_frances_value(self):
        """FRANCES has correct value."""
        from src.domain.cas_value_objects import IdiomaLaudo
        assert IdiomaLaudo.FRANCES.value == "fr"

    def test_espanol_value(self):
        """ESPANOL has correct value."""
        from src.domain.cas_value_objects import IdiomaLaudo
        assert IdiomaLaudo.ESPANOL.value == "es"


class TestEstadoLaudo:
    """Tests for EstadoLaudo enum."""

    def test_publicado_value(self):
        """PUBLICADO has correct value."""
        from src.domain.cas_value_objects import EstadoLaudo
        assert EstadoLaudo.PUBLICADO.value == "published"

    def test_pendiente_value(self):
        """PENDIENTE has correct value."""
        from src.domain.cas_value_objects import EstadoLaudo
        assert EstadoLaudo.PENDIENTE.value == "pending"

    def test_confidencial_value(self):
        """CONFIDENCIAL has correct value."""
        from src.domain.cas_value_objects import EstadoLaudo
        assert EstadoLaudo.CONFIDENCIAL.value == "confidential"
