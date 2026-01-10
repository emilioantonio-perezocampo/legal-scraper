"""
RED Phase Tests: SCJN Document Parser

Tests for parsing SCJN document detail pages.
These tests must FAIL initially (RED phase).
"""
import pytest
from dataclasses import FrozenInstanceError

# These imports will fail until implementation exists
from src.infrastructure.adapters.scjn_document_parser import (
    DocumentDetailResult,
    ArticleResult,
    ReformResult,
    parse_document_detail,
    parse_articles,
    parse_reforms,
)
from src.infrastructure.adapters.errors import ParseError


# Sample HTML fixtures for document detail page
SAMPLE_DOCUMENT_DETAIL_HTML = """
<html>
<head><title>LEY FEDERAL DEL TRABAJO</title></head>
<body>
<div id="contenedor">
    <h1 class="titulo-ordenamiento">LEY FEDERAL DEL TRABAJO</h1>
    <div class="datos-ordenamiento">
        <div class="dato">
            <span class="etiqueta">Tipo de Ordenamiento:</span>
            <span class="valor">LEY FEDERAL</span>
        </div>
        <div class="dato">
            <span class="etiqueta">Ámbito:</span>
            <span class="valor">FEDERAL</span>
        </div>
        <div class="dato">
            <span class="etiqueta">Estatus:</span>
            <span class="valor">VIGENTE</span>
        </div>
        <div class="dato">
            <span class="etiqueta">Fecha de Publicación:</span>
            <span class="valor">01/04/1970</span>
        </div>
        <div class="dato">
            <span class="etiqueta">Fecha de Expedición:</span>
            <span class="valor">15/03/1970</span>
        </div>
    </div>
    <div id="contenido-ordenamiento">
        <div class="articulo" id="art1">
            <h3>Artículo 1</h3>
            <p>La presente Ley es de observancia general en toda la República.</p>
        </div>
        <div class="articulo" id="art2">
            <h3>Artículo 2</h3>
            <p>Las normas del trabajo tienden a conseguir el equilibrio entre los factores de la producción y la justicia social.</p>
        </div>
    </div>
</div>
</body>
</html>
"""

SAMPLE_DOCUMENT_WITH_REFORMS_HTML = """
<html>
<body>
<div id="contenedor">
    <h1 class="titulo-ordenamiento">LEY FEDERAL DEL TRABAJO</h1>
    <div class="datos-ordenamiento">
        <div class="dato">
            <span class="etiqueta">Tipo de Ordenamiento:</span>
            <span class="valor">LEY FEDERAL</span>
        </div>
        <div class="dato">
            <span class="etiqueta">Ámbito:</span>
            <span class="valor">FEDERAL</span>
        </div>
        <div class="dato">
            <span class="etiqueta">Estatus:</span>
            <span class="valor">VIGENTE</span>
        </div>
    </div>
    <div id="reformas">
        <table class="tabla-reformas">
            <tr class="reforma-row">
                <td><a href="wfOrdenamientoDetalle.aspx?q=ref001">Reforma 30/11/2012</a></td>
                <td>30/11/2012</td>
                <td>DOF 30-11-2012</td>
                <td><a href="AbrirDocReforma.aspx?q=ref001">PDF</a></td>
            </tr>
            <tr class="reforma-row">
                <td><a href="wfOrdenamientoDetalle.aspx?q=ref002">Reforma 01/05/2019</a></td>
                <td>01/05/2019</td>
                <td>DOF 01-05-2019</td>
                <td><a href="AbrirDocReforma.aspx?q=ref002">PDF</a></td>
            </tr>
        </table>
    </div>
</div>
</body>
</html>
"""

SAMPLE_ARTICLES_HTML = """
<div id="contenido-ordenamiento">
    <div class="articulo" id="art1">
        <h3>Artículo 1</h3>
        <p>La presente Ley es de observancia general en toda la República.</p>
    </div>
    <div class="articulo" id="art2">
        <h3>Artículo 2</h3>
        <p>Las normas del trabajo tienden a conseguir el equilibrio.</p>
    </div>
    <div class="articulo" id="art3">
        <h3>Artículo 3</h3>
        <p>El trabajo es un derecho y un deber sociales.</p>
    </div>
</div>
"""

SAMPLE_TRANSITORIOS_HTML = """
<div id="contenido-ordenamiento">
    <div class="articulo" id="art1">
        <h3>Artículo 1</h3>
        <p>Texto del artículo primero.</p>
    </div>
    <div class="articulo transitorio" id="trans1">
        <h3>TRANSITORIO PRIMERO</h3>
        <p>Esta Ley entrará en vigor el día de su publicación.</p>
    </div>
    <div class="articulo transitorio" id="trans2">
        <h3>TRANSITORIO SEGUNDO</h3>
        <p>Se derogan las disposiciones que se opongan a esta Ley.</p>
    </div>
</div>
"""

SAMPLE_ARTICLE_BIS_HTML = """
<div id="contenido-ordenamiento">
    <div class="articulo" id="art1">
        <h3>Artículo 1</h3>
        <p>Texto del artículo 1.</p>
    </div>
    <div class="articulo" id="art1bis">
        <h3>Artículo 1 Bis</h3>
        <p>Texto del artículo 1 bis.</p>
    </div>
    <div class="articulo" id="art2">
        <h3>Artículo 2</h3>
        <p>Texto del artículo 2.</p>
    </div>
</div>
"""

SAMPLE_UNICODE_HTML = """
<html>
<body>
<div id="contenedor">
    <h1 class="titulo-ordenamiento">CONSTITUCIÓN POLÍTICA DE LOS ESTADOS UNIDOS MEXICANOS</h1>
    <div class="datos-ordenamiento">
        <div class="dato">
            <span class="etiqueta">Tipo de Ordenamiento:</span>
            <span class="valor">CONSTITUCIÓN</span>
        </div>
    </div>
    <div id="contenido-ordenamiento">
        <div class="articulo">
            <h3>Artículo 1°</h3>
            <p>En los Estados Unidos Mexicanos todas las personas gozarán de los derechos humanos reconocidos en esta Constitución.</p>
        </div>
    </div>
</div>
</body>
</html>
"""

SAMPLE_EMPTY_DOCUMENT_HTML = """
<html>
<body>
<div id="contenedor">
    <h1 class="titulo-ordenamiento"></h1>
    <div class="datos-ordenamiento"></div>
    <div id="contenido-ordenamiento"></div>
</div>
</body>
</html>
"""

SAMPLE_NO_ARTICLES_HTML = """
<html>
<body>
<div id="contenedor">
    <h1 class="titulo-ordenamiento">ACUERDO GENERAL</h1>
    <div class="datos-ordenamiento">
        <div class="dato">
            <span class="etiqueta">Tipo de Ordenamiento:</span>
            <span class="valor">ACUERDO</span>
        </div>
    </div>
    <div id="contenido-ordenamiento">
        <p>Texto completo del acuerdo sin división en artículos.</p>
    </div>
</div>
</body>
</html>
"""


class TestDocumentDetailResult:
    """Tests for DocumentDetailResult dataclass."""

    def test_create_document_detail_result(self):
        """Should create result with all fields."""
        result = DocumentDetailResult(
            title="LEY FEDERAL DEL TRABAJO",
            short_title="LFT",
            category="LEY FEDERAL",
            scope="FEDERAL",
            status="VIGENTE",
            publication_date="01/04/1970",
            expedition_date="15/03/1970",
            full_text="Texto completo...",
            article_count=2,
        )
        assert result.title == "LEY FEDERAL DEL TRABAJO"
        assert result.category == "LEY FEDERAL"
        assert result.article_count == 2

    def test_document_detail_result_immutable(self):
        """Should be frozen/immutable."""
        result = DocumentDetailResult(
            title="Test",
            short_title="T",
            category="LEY",
            scope="FEDERAL",
            status="VIGENTE",
            publication_date=None,
            expedition_date=None,
            full_text="",
            article_count=0,
        )
        with pytest.raises(FrozenInstanceError):
            result.title = "Changed"


class TestArticleResult:
    """Tests for ArticleResult dataclass."""

    def test_create_article_result(self):
        """Should create article result."""
        article = ArticleResult(
            number="1",
            title="Artículo 1",
            content="La presente Ley es de observancia general.",
            is_transitory=False,
        )
        assert article.number == "1"
        assert article.is_transitory is False

    def test_article_result_immutable(self):
        """Should be frozen/immutable."""
        article = ArticleResult(
            number="1",
            title="Artículo 1",
            content="Test content",
            is_transitory=False,
        )
        with pytest.raises(FrozenInstanceError):
            article.content = "Changed"


class TestReformResult:
    """Tests for ReformResult dataclass."""

    def test_create_reform_result(self):
        """Should create reform result."""
        reform = ReformResult(
            q_param="ref001",
            title="Reforma 30/11/2012",
            publication_date="30/11/2012",
            gazette_reference="DOF 30-11-2012",
            has_pdf=True,
        )
        assert reform.q_param == "ref001"
        assert reform.has_pdf is True

    def test_reform_result_immutable(self):
        """Should be frozen/immutable."""
        reform = ReformResult(
            q_param="ref001",
            title="Reforma",
            publication_date=None,
            gazette_reference="",
            has_pdf=False,
        )
        with pytest.raises(FrozenInstanceError):
            reform.title = "Changed"


class TestParseDocumentDetailHappyPath:
    """Happy path tests for document detail parsing."""

    def test_parse_title(self):
        """Should parse document title."""
        result = parse_document_detail(SAMPLE_DOCUMENT_DETAIL_HTML)
        assert result.title == "LEY FEDERAL DEL TRABAJO"

    def test_parse_category(self):
        """Should parse document category."""
        result = parse_document_detail(SAMPLE_DOCUMENT_DETAIL_HTML)
        assert result.category == "LEY FEDERAL"

    def test_parse_scope(self):
        """Should parse document scope."""
        result = parse_document_detail(SAMPLE_DOCUMENT_DETAIL_HTML)
        assert result.scope == "FEDERAL"

    def test_parse_status(self):
        """Should parse document status."""
        result = parse_document_detail(SAMPLE_DOCUMENT_DETAIL_HTML)
        assert result.status == "VIGENTE"

    def test_parse_publication_date(self):
        """Should parse publication date."""
        result = parse_document_detail(SAMPLE_DOCUMENT_DETAIL_HTML)
        assert result.publication_date == "01/04/1970"

    def test_parse_expedition_date(self):
        """Should parse expedition date."""
        result = parse_document_detail(SAMPLE_DOCUMENT_DETAIL_HTML)
        assert result.expedition_date == "15/03/1970"


class TestParseDocumentDetailEdgeCases:
    """Edge case tests for document detail parsing."""

    def test_parse_empty_document(self):
        """Should handle document with empty fields."""
        result = parse_document_detail(SAMPLE_EMPTY_DOCUMENT_HTML)
        assert result.title == ""
        assert result.article_count == 0

    def test_parse_document_no_articles(self):
        """Should handle document without article structure."""
        result = parse_document_detail(SAMPLE_NO_ARTICLES_HTML)
        assert result.title == "ACUERDO GENERAL"
        assert result.article_count == 0


class TestParseDocumentDetailUnicode:
    """Unicode handling tests."""

    def test_parse_title_with_accents(self):
        """Should preserve Spanish accents in title."""
        result = parse_document_detail(SAMPLE_UNICODE_HTML)
        assert "CONSTITUCIÓN" in result.title
        assert "POLÍTICA" in result.title
        assert "MEXICANOS" in result.title


class TestParseDocumentDetailMalformed:
    """Tests for malformed HTML handling."""

    def test_parse_missing_container_raises_error(self):
        """Should raise ParseError if container is missing."""
        html = "<html><body><div>No container</div></body></html>"
        with pytest.raises(ParseError):
            parse_document_detail(html)


class TestParseArticles:
    """Tests for article parsing."""

    def test_parse_multiple_articles(self):
        """Should parse multiple articles."""
        articles = parse_articles(SAMPLE_ARTICLES_HTML)
        assert len(articles) == 3
        assert articles[0].number == "1"
        assert articles[1].number == "2"
        assert articles[2].number == "3"

    def test_parse_article_content(self):
        """Should extract article content."""
        articles = parse_articles(SAMPLE_ARTICLES_HTML)
        assert "observancia general" in articles[0].content

    def test_parse_article_title(self):
        """Should extract article title."""
        articles = parse_articles(SAMPLE_ARTICLES_HTML)
        assert "Artículo 1" in articles[0].title

    def test_parse_transitorios(self):
        """Should identify transitory articles."""
        articles = parse_articles(SAMPLE_TRANSITORIOS_HTML)
        # Find transitory articles
        transitories = [a for a in articles if a.is_transitory]
        assert len(transitories) == 2
        assert "PRIMERO" in transitories[0].number or "PRIMERO" in transitories[0].title

    def test_parse_article_bis(self):
        """Should handle 'Bis' articles."""
        articles = parse_articles(SAMPLE_ARTICLE_BIS_HTML)
        assert len(articles) == 3
        bis_articles = [a for a in articles if "bis" in a.number.lower() or "bis" in a.title.lower()]
        assert len(bis_articles) >= 1

    def test_parse_empty_articles(self):
        """Should return empty list for no articles."""
        html = "<div id='contenido-ordenamiento'></div>"
        articles = parse_articles(html)
        assert articles == []


class TestParseReforms:
    """Tests for reform/amendment parsing."""

    def test_parse_multiple_reforms(self):
        """Should parse multiple reforms."""
        reforms = parse_reforms(SAMPLE_DOCUMENT_WITH_REFORMS_HTML)
        assert len(reforms) == 2

    def test_parse_reform_q_param(self):
        """Should extract reform q_param."""
        reforms = parse_reforms(SAMPLE_DOCUMENT_WITH_REFORMS_HTML)
        assert reforms[0].q_param == "ref001"
        assert reforms[1].q_param == "ref002"

    def test_parse_reform_date(self):
        """Should extract reform date."""
        reforms = parse_reforms(SAMPLE_DOCUMENT_WITH_REFORMS_HTML)
        assert reforms[0].publication_date == "30/11/2012"
        assert reforms[1].publication_date == "01/05/2019"

    def test_parse_reform_gazette_reference(self):
        """Should extract gazette reference."""
        reforms = parse_reforms(SAMPLE_DOCUMENT_WITH_REFORMS_HTML)
        assert "DOF" in reforms[0].gazette_reference

    def test_parse_reform_has_pdf(self):
        """Should detect if reform has PDF."""
        reforms = parse_reforms(SAMPLE_DOCUMENT_WITH_REFORMS_HTML)
        assert reforms[0].has_pdf is True
        assert reforms[1].has_pdf is True

    def test_parse_no_reforms(self):
        """Should return empty list if no reforms."""
        reforms = parse_reforms(SAMPLE_DOCUMENT_DETAIL_HTML)
        assert reforms == []


class TestParseReformsEdgeCases:
    """Edge case tests for reform parsing."""

    def test_parse_reform_without_pdf(self):
        """Should handle reform without PDF link."""
        html = """
        <div id="reformas">
            <table class="tabla-reformas">
                <tr class="reforma-row">
                    <td><a href="wfOrdenamientoDetalle.aspx?q=ref001">Reforma</a></td>
                    <td>30/11/2012</td>
                    <td>DOF 30-11-2012</td>
                    <td></td>
                </tr>
            </table>
        </div>
        """
        reforms = parse_reforms(html)
        assert len(reforms) == 1
        assert reforms[0].has_pdf is False

    def test_parse_reform_missing_date(self):
        """Should handle reform with missing date."""
        html = """
        <div id="reformas">
            <table class="tabla-reformas">
                <tr class="reforma-row">
                    <td><a href="wfOrdenamientoDetalle.aspx?q=ref001">Reforma</a></td>
                    <td></td>
                    <td>DOF</td>
                    <td></td>
                </tr>
            </table>
        </div>
        """
        reforms = parse_reforms(html)
        assert len(reforms) == 1
        assert reforms[0].publication_date is None or reforms[0].publication_date == ""
