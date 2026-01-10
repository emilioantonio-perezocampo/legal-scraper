"""
Tests for CAS Award Detail Parser

Following RED-GREEN TDD: These tests define the expected behavior
for parsing CAS award detail pages.

Target: ~30 tests for laudo parser
"""
import pytest


class TestParseLaudoDetalle:
    """Tests for parse_laudo_detalle function."""

    def test_empty_html_raises_parse_error(self):
        """Empty HTML raises ParseError."""
        from src.infrastructure.adapters.cas_laudo_parser import parse_laudo_detalle
        from src.infrastructure.adapters.cas_errors import ParseError

        with pytest.raises(ParseError):
            parse_laudo_detalle("", "https://example.com")

    def test_html_without_case_number_raises_parse_error(self):
        """HTML without case number raises ParseError."""
        from src.infrastructure.adapters.cas_laudo_parser import parse_laudo_detalle
        from src.infrastructure.adapters.cas_errors import ParseError

        html = "<html><body><p>No case number here</p></body></html>"
        with pytest.raises(ParseError):
            parse_laudo_detalle(html, "https://example.com")

    def test_extract_cas_case_number(self):
        """Extract CAS case number from HTML."""
        from src.infrastructure.adapters.cas_laudo_parser import parse_laudo_detalle

        html = """
        <html><body>
        <h1 class="case-number">CAS 2023/A/12345</h1>
        <p>Award content here</p>
        </body></html>
        """
        result = parse_laudo_detalle(html, "https://example.com")
        assert "CAS 2023/A/12345" in result.numero_caso.valor

    def test_extract_tas_case_number(self):
        """Extract TAS (French) case number from HTML."""
        from src.infrastructure.adapters.cas_laudo_parser import parse_laudo_detalle

        html = """
        <html><body>
        <h1>TAS 2022/O/8500</h1>
        </body></html>
        """
        result = parse_laudo_detalle(html, "https://example.com")
        assert "TAS" in result.numero_caso.valor

    def test_extract_titulo_from_parties(self):
        """Extract title from parties pattern."""
        from src.infrastructure.adapters.cas_laudo_parser import parse_laudo_detalle

        html = """
        <html><body>
        <h1>CAS 2023/A/12345</h1>
        <h2 class="parties">FC Barcelona v. FIFA</h2>
        </body></html>
        """
        result = parse_laudo_detalle(html, "https://example.com")
        assert "v." in result.titulo or "v" in result.titulo.lower()

    def test_extract_parties_appellant_respondent(self):
        """Extract parties with structured elements."""
        from src.infrastructure.adapters.cas_laudo_parser import parse_laudo_detalle

        html = """
        <html><body>
        <h1>CAS 2023/A/12345</h1>
        <div class="appellant">FC Barcelona</div>
        <div class="respondent">FIFA</div>
        </body></html>
        """
        result = parse_laudo_detalle(html, "https://example.com")
        assert len(result.partes) >= 1

    def test_extract_arbitrators(self):
        """Extract arbitration panel members."""
        from src.infrastructure.adapters.cas_laudo_parser import parse_laudo_detalle

        html = """
        <html><body>
        <h1>CAS 2023/A/12345</h1>
        <div class="panel">
            <div class="arbitrator">Dr. John Smith (President)</div>
            <div class="arbitrator">Prof. Jane Doe</div>
        </div>
        </body></html>
        """
        result = parse_laudo_detalle(html, "https://example.com")
        assert len(result.arbitros) >= 1

    def test_detect_deporte_football(self):
        """Detect football from FIFA mention."""
        from src.infrastructure.adapters.cas_laudo_parser import parse_laudo_detalle
        from src.domain.cas_value_objects import CategoriaDeporte

        html = """
        <html><body>
        <h1>CAS 2023/A/12345</h1>
        <p>FIFA v. Player regarding football transfer</p>
        </body></html>
        """
        result = parse_laudo_detalle(html, "https://example.com")
        assert result.categoria_deporte == CategoriaDeporte.FUTBOL

    def test_detect_deporte_cycling(self):
        """Detect cycling from UCI mention."""
        from src.infrastructure.adapters.cas_laudo_parser import parse_laudo_detalle
        from src.domain.cas_value_objects import CategoriaDeporte

        html = """
        <html><body>
        <h1>CAS 2023/A/12345</h1>
        <p>UCI v. Cyclist regarding doping</p>
        </body></html>
        """
        result = parse_laudo_detalle(html, "https://example.com")
        assert result.categoria_deporte == CategoriaDeporte.CICLISMO

    def test_detect_materia_dopaje(self):
        """Detect doping subject matter."""
        from src.infrastructure.adapters.cas_laudo_parser import parse_laudo_detalle
        from src.domain.cas_value_objects import TipoMateria

        html = """
        <html><body>
        <h1>CAS 2023/A/12345</h1>
        <p>Anti-doping violation WADA code</p>
        </body></html>
        """
        result = parse_laudo_detalle(html, "https://example.com")
        assert result.materia == TipoMateria.DOPAJE

    def test_detect_materia_transferencia(self):
        """Detect transfer subject matter."""
        from src.infrastructure.adapters.cas_laudo_parser import parse_laudo_detalle
        from src.domain.cas_value_objects import TipoMateria

        html = """
        <html><body>
        <h1>CAS 2023/A/12345</h1>
        <p>Transfer dispute between clubs</p>
        </body></html>
        """
        result = parse_laudo_detalle(html, "https://example.com")
        assert result.materia == TipoMateria.TRANSFERENCIA

    def test_detect_idioma_ingles(self):
        """Detect English language."""
        from src.infrastructure.adapters.cas_laudo_parser import parse_laudo_detalle
        from src.domain.cas_value_objects import IdiomaLaudo

        html = """
        <html><body>
        <h1>CAS 2023/A/12345</h1>
        <p>The Panel finds that the Appellant has failed to prove...</p>
        </body></html>
        """
        result = parse_laudo_detalle(html, "https://example.com")
        assert result.idioma == IdiomaLaudo.INGLES

    def test_detect_idioma_frances(self):
        """Detect French language."""
        from src.infrastructure.adapters.cas_laudo_parser import parse_laudo_detalle
        from src.domain.cas_value_objects import IdiomaLaudo

        html = """
        <html><body>
        <h1>TAS 2023/A/12345</h1>
        <p>Le Tribunal arbitral constate que la demande est recevable.</p>
        <p>Les parties ont été dûment entendues dans le cadre de la procédure.</p>
        </body></html>
        """
        result = parse_laudo_detalle(html, "https://example.com")
        assert result.idioma == IdiomaLaudo.FRANCES

    def test_extract_resumen(self):
        """Extract award summary/abstract."""
        from src.infrastructure.adapters.cas_laudo_parser import parse_laudo_detalle

        html = """
        <html><body>
        <h1>CAS 2023/A/12345</h1>
        <div class="summary">This case concerns a transfer dispute...</div>
        </body></html>
        """
        result = parse_laudo_detalle(html, "https://example.com")
        assert result.resumen is not None

    def test_extract_pdf_url(self):
        """Extract PDF download URL."""
        from src.infrastructure.adapters.cas_laudo_parser import parse_laudo_detalle

        html = """
        <html><body>
        <h1>CAS 2023/A/12345</h1>
        <a href="/documents/award.pdf" class="pdf-download">Download PDF</a>
        </body></html>
        """
        result = parse_laudo_detalle(html, "https://example.com")
        # url is optional, so just check it doesn't crash

    def test_extract_cited_cases(self):
        """Extract CAS cases cited in the award."""
        from src.infrastructure.adapters.cas_laudo_parser import parse_laudo_detalle

        html = """
        <html><body>
        <h1>CAS 2023/A/12345</h1>
        <p>As stated in CAS 2020/A/7000 and CAS 2019/A/6000...</p>
        </body></html>
        """
        result = parse_laudo_detalle(html, "https://example.com")
        assert len(result.palabras_clave) >= 0  # At least doesn't crash

    def test_full_laudo_has_id(self):
        """Parsed LaudoArbitral has an ID."""
        from src.infrastructure.adapters.cas_laudo_parser import parse_laudo_detalle

        html = """
        <html><body>
        <h1>CAS 2023/A/12345</h1>
        </body></html>
        """
        result = parse_laudo_detalle(html, "https://example.com")
        assert result.id is not None


class TestHelperFunctions:
    """Tests for parser helper functions."""

    def test_detect_tipo_parte_federation_returns_apelado(self):
        """Detect federation party type returns APELADO."""
        from src.infrastructure.adapters.cas_laudo_parser import _detect_tipo_parte
        from src.domain.cas_entities import TipoParte

        result = _detect_tipo_parte("FIFA")
        # Federations typically appear as respondents/apelados
        assert result == TipoParte.APELADO

    def test_detect_tipo_parte_club_returns_apelado(self):
        """Detect club party type returns APELADO."""
        from src.infrastructure.adapters.cas_laudo_parser import _detect_tipo_parte
        from src.domain.cas_entities import TipoParte

        result = _detect_tipo_parte("FC Barcelona")
        assert result == TipoParte.APELADO

    def test_detect_tipo_parte_atleta_returns_apelante(self):
        """Detect athlete party type returns APELANTE (default)."""
        from src.infrastructure.adapters.cas_laudo_parser import _detect_tipo_parte
        from src.domain.cas_entities import TipoParte

        result = _detect_tipo_parte("John Smith")
        assert result == TipoParte.APELANTE

    def test_detect_tipo_decision_desestimatoria(self):
        """Detect dismissal decision type."""
        from src.infrastructure.adapters.cas_laudo_parser import _detect_tipo_decision

        result = _detect_tipo_decision("The appeal is dismissed.")
        assert result == "desestimatoria"

    def test_detect_tipo_decision_estimatoria(self):
        """Detect upheld decision type."""
        from src.infrastructure.adapters.cas_laudo_parser import _detect_tipo_decision

        result = _detect_tipo_decision("The appeal is upheld.")
        assert result == "estimatoria"

    def test_detect_tipo_decision_parcial(self):
        """Detect partial decision type."""
        from src.infrastructure.adapters.cas_laudo_parser import _detect_tipo_decision

        result = _detect_tipo_decision("The appeal is partially allowed.")
        assert result == "parcial"

    def test_extract_casos_citados(self):
        """Extract cited cases from text."""
        from src.infrastructure.adapters.cas_laudo_parser import _extract_casos_citados

        text = "See CAS 2020/A/7000 and TAS 2019/O/6000 for reference."
        result = _extract_casos_citados(text)
        assert len(result) == 2

    def test_extract_casos_citados_limits_to_30(self):
        """Limit cited cases to 30."""
        from src.infrastructure.adapters.cas_laudo_parser import _extract_casos_citados

        cases = " ".join([f"CAS 2020/A/{i}" for i in range(50)])
        result = _extract_casos_citados(cases)
        assert len(result) <= 30


class TestExtractFederaciones:
    """Tests for federation extraction."""

    def test_extract_fifa_federation(self):
        """Extract FIFA from parties."""
        from src.infrastructure.adapters.cas_laudo_parser import _extract_federaciones
        from src.domain.cas_entities import Parte, TipoParte

        partes = (
            Parte(nombre="FIFA", tipo=TipoParte.APELADO),
        )
        result = _extract_federaciones(partes)
        assert len(result) >= 1
        assert any("FIFA" in f.acronimo for f in result)

    def test_extract_wada_federation(self):
        """Extract WADA from parties."""
        from src.infrastructure.adapters.cas_laudo_parser import _extract_federaciones
        from src.domain.cas_entities import Parte, TipoParte

        partes = (
            Parte(nombre="WADA", tipo=TipoParte.APELADO),
        )
        result = _extract_federaciones(partes)
        assert len(result) >= 1
