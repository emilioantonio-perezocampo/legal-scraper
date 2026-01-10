"""
RED Phase Tests: SCJN Search Parser

Tests for parsing SCJN search results HTML.
These tests must FAIL initially (RED phase).
"""
import pytest
from dataclasses import FrozenInstanceError

# These imports will fail until implementation exists
from src.infrastructure.adapters.scjn_search_parser import (
    SearchResultItem,
    parse_search_results,
    extract_pagination_info,
)
from src.infrastructure.adapters.errors import ParseError


# Sample HTML fixtures for testing
SAMPLE_SINGLE_RESULT_HTML = """
<html>
<body>
<div id="gridResultados">
    <table class="dxgvTable">
        <tr class="dxgvDataRow">
            <td><a href="wfOrdenamientoDetalle.aspx?q=abc123def456">LEY FEDERAL DEL TRABAJO</a></td>
            <td>01/04/1970</td>
            <td>15/03/1970</td>
            <td>VIGENTE</td>
            <td>LEY FEDERAL</td>
            <td>FEDERAL</td>
            <td><a href="wfExtracto.aspx?q=abc123">Extracto</a></td>
            <td><a href="AbrirDocReforma.aspx?q=abc123">PDF</a></td>
        </tr>
    </table>
</div>
</body>
</html>
"""

SAMPLE_MULTIPLE_RESULTS_HTML = """
<html>
<body>
<div id="gridResultados">
    <table class="dxgvTable">
        <tr class="dxgvDataRow">
            <td><a href="wfOrdenamientoDetalle.aspx?q=abc123">LEY FEDERAL DEL TRABAJO</a></td>
            <td>01/04/1970</td>
            <td>15/03/1970</td>
            <td>VIGENTE</td>
            <td>LEY FEDERAL</td>
            <td>FEDERAL</td>
            <td><a href="wfExtracto.aspx?q=abc123">Extracto</a></td>
            <td><a href="AbrirDocReforma.aspx?q=abc123">PDF</a></td>
        </tr>
        <tr class="dxgvDataRow">
            <td><a href="wfOrdenamientoDetalle.aspx?q=xyz789">CÓDIGO CIVIL FEDERAL</a></td>
            <td>26/05/1928</td>
            <td>25/05/1928</td>
            <td>VIGENTE</td>
            <td>CODIGO</td>
            <td>FEDERAL</td>
            <td><a href="wfExtracto.aspx?q=xyz789">Extracto</a></td>
            <td></td>
        </tr>
        <tr class="dxgvDataRow">
            <td><a href="wfOrdenamientoDetalle.aspx?q=def456">CONSTITUCIÓN POLÍTICA</a></td>
            <td>05/02/1917</td>
            <td>01/02/1917</td>
            <td>VIGENTE</td>
            <td>CONSTITUCION</td>
            <td>FEDERAL</td>
            <td></td>
            <td><a href="AbrirDocReforma.aspx?q=def456">PDF</a></td>
        </tr>
    </table>
</div>
</body>
</html>
"""

SAMPLE_EMPTY_RESULTS_HTML = """
<html>
<body>
<div id="gridResultados">
    <table class="dxgvTable">
    </table>
</div>
</body>
</html>
"""

SAMPLE_NO_ENCONTRADOS_HTML = """
<html>
<body>
<div id="gridResultados">
    <span class="dxgvEmptyDataRow">No se encontraron resultados</span>
</div>
</body>
</html>
"""

SAMPLE_UNICODE_TITLE_HTML = """
<html>
<body>
<div id="gridResultados">
    <table class="dxgvTable">
        <tr class="dxgvDataRow">
            <td><a href="wfOrdenamientoDetalle.aspx?q=uni123">CONSTITUCIÓN POLÍTICA DE LOS ESTADOS UNIDOS MEXICANOS</a></td>
            <td>05/02/1917</td>
            <td>01/02/1917</td>
            <td>VIGENTE</td>
            <td>CONSTITUCION</td>
            <td>FEDERAL</td>
            <td><a href="wfExtracto.aspx?q=uni123">Extracto</a></td>
            <td><a href="AbrirDocReforma.aspx?q=uni123">PDF</a></td>
        </tr>
    </table>
</div>
</body>
</html>
"""

SAMPLE_ABROGADA_HTML = """
<html>
<body>
<div id="gridResultados">
    <table class="dxgvTable">
        <tr class="dxgvDataRow">
            <td><a href="wfOrdenamientoDetalle.aspx?q=old123">LEY ANTIGUA</a></td>
            <td>01/01/1950</td>
            <td>01/01/1950</td>
            <td>ABROGADA</td>
            <td>LEY FEDERAL</td>
            <td>FEDERAL</td>
            <td><a href="wfExtracto.aspx?q=old123">Extracto</a></td>
            <td></td>
        </tr>
    </table>
</div>
</body>
</html>
"""

SAMPLE_PAGINATION_HTML = """
<html>
<body>
<div id="gridResultados">
    <table class="dxgvTable">
        <tr class="dxgvDataRow">
            <td><a href="wfOrdenamientoDetalle.aspx?q=page1">DOCUMENTO 1</a></td>
            <td>01/01/2020</td>
            <td>01/01/2020</td>
            <td>VIGENTE</td>
            <td>LEY</td>
            <td>FEDERAL</td>
            <td></td>
            <td></td>
        </tr>
    </table>
    <div class="dxpPagerItem">1</div>
    <div class="dxpPagerItem">2</div>
    <div class="dxpPagerItem">3</div>
    <div class="dxpPagerTotal">Página 1 de 10</div>
</div>
</body>
</html>
"""


class TestSearchResultItem:
    """Tests for SearchResultItem dataclass."""

    def test_create_result_item(self):
        """Result item should be creatable with all fields."""
        item = SearchResultItem(
            q_param="abc123",
            title="Ley Federal del Trabajo",
            category="LEY FEDERAL",
            publication_date="01/04/1970",
            expedition_date="15/03/1970",
            status="VIGENTE",
            scope="FEDERAL",
            has_pdf=True,
            has_extract=True,
        )
        assert item.q_param == "abc123"
        assert item.title == "Ley Federal del Trabajo"
        assert item.has_pdf is True

    def test_result_item_immutable(self):
        """Result item should be frozen/immutable."""
        item = SearchResultItem(
            q_param="abc",
            title="Test",
            category="LEY",
            publication_date=None,
            expedition_date=None,
            status="VIGENTE",
            scope="FEDERAL",
            has_pdf=False,
            has_extract=False,
        )
        with pytest.raises(FrozenInstanceError):
            item.title = "Changed"


class TestParseSearchResultsHappyPath:
    """Happy path tests for search result parsing."""

    def test_parse_single_result(self):
        """Should parse a single search result."""
        results = parse_search_results(SAMPLE_SINGLE_RESULT_HTML)
        assert len(results) == 1
        assert results[0].title == "LEY FEDERAL DEL TRABAJO"
        assert results[0].q_param == "abc123def456"

    def test_parse_multiple_results(self):
        """Should parse multiple search results."""
        results = parse_search_results(SAMPLE_MULTIPLE_RESULTS_HTML)
        assert len(results) == 3
        assert results[0].title == "LEY FEDERAL DEL TRABAJO"
        assert results[1].title == "CÓDIGO CIVIL FEDERAL"
        assert results[2].title == "CONSTITUCIÓN POLÍTICA"

    def test_parse_extracts_q_param(self):
        """Should correctly extract q_param from URL."""
        results = parse_search_results(SAMPLE_SINGLE_RESULT_HTML)
        assert results[0].q_param == "abc123def456"


class TestParseSearchResultsEmptyAndEdge:
    """Tests for empty and edge cases."""

    def test_parse_empty_results_returns_empty_list(self):
        """Empty results should return empty list."""
        results = parse_search_results(SAMPLE_EMPTY_RESULTS_HTML)
        assert results == []

    def test_parse_no_results_message(self):
        """'No se encontraron' message should return empty list."""
        results = parse_search_results(SAMPLE_NO_ENCONTRADOS_HTML)
        assert results == []


class TestParseSearchResultsMissingFields:
    """Tests for handling missing fields."""

    def test_parse_result_missing_pdf_link(self):
        """Should handle missing PDF link."""
        results = parse_search_results(SAMPLE_MULTIPLE_RESULTS_HTML)
        # Second result has no PDF link
        assert results[1].has_pdf is False
        assert results[0].has_pdf is True

    def test_parse_result_missing_extract_link(self):
        """Should handle missing extract link."""
        results = parse_search_results(SAMPLE_MULTIPLE_RESULTS_HTML)
        # Third result has no extract link
        assert results[2].has_extract is False
        assert results[0].has_extract is True


class TestParseSearchResultsQParam:
    """Tests for q_param extraction."""

    def test_extract_q_param_from_detail_link(self):
        """Should extract q_param from wfOrdenamientoDetalle link."""
        results = parse_search_results(SAMPLE_SINGLE_RESULT_HTML)
        assert results[0].q_param == "abc123def456"

    def test_extract_q_param_with_special_chars(self):
        """Should handle q_param with special characters (URL-encoded)."""
        # In real URLs, special chars like +/= are URL-encoded
        # %2B = +, %2F = /, %3D = =
        html = """
        <div id="gridResultados">
            <table class="dxgvTable">
                <tr class="dxgvDataRow">
                    <td><a href="wfOrdenamientoDetalle.aspx?q=abc%2B%2F%3Dxyz">TEST</a></td>
                    <td>01/01/2020</td>
                    <td>01/01/2020</td>
                    <td>VIGENTE</td>
                    <td>LEY</td>
                    <td>FEDERAL</td>
                    <td></td>
                    <td></td>
                </tr>
            </table>
        </div>
        """
        results = parse_search_results(html)
        # URL decoding: %2B→+, %2F→/, %3D→=
        assert results[0].q_param == "abc+/=xyz"


class TestParseSearchResultsStatusVariations:
    """Tests for different status values."""

    def test_parse_status_vigente(self):
        """Should parse VIGENTE status."""
        results = parse_search_results(SAMPLE_SINGLE_RESULT_HTML)
        assert results[0].status == "VIGENTE"

    def test_parse_status_abrogada(self):
        """Should parse ABROGADA status."""
        results = parse_search_results(SAMPLE_ABROGADA_HTML)
        assert results[0].status == "ABROGADA"

    def test_parse_status_unknown_defaults_to_raw(self):
        """Unknown status should be preserved as-is."""
        html = """
        <div id="gridResultados">
            <table class="dxgvTable">
                <tr class="dxgvDataRow">
                    <td><a href="wfOrdenamientoDetalle.aspx?q=abc">TEST</a></td>
                    <td>01/01/2020</td>
                    <td>01/01/2020</td>
                    <td>CUSTOM_STATUS</td>
                    <td>LEY</td>
                    <td>FEDERAL</td>
                    <td></td>
                    <td></td>
                </tr>
            </table>
        </div>
        """
        results = parse_search_results(html)
        assert results[0].status == "CUSTOM_STATUS"


class TestParseSearchResultsCategoryVariations:
    """Tests for different category values."""

    def test_parse_category_ley_federal(self):
        """Should parse LEY FEDERAL category."""
        results = parse_search_results(SAMPLE_SINGLE_RESULT_HTML)
        assert results[0].category == "LEY FEDERAL"

    def test_parse_category_codigo(self):
        """Should parse CODIGO category."""
        results = parse_search_results(SAMPLE_MULTIPLE_RESULTS_HTML)
        assert results[1].category == "CODIGO"

    def test_parse_category_constitucion(self):
        """Should parse CONSTITUCION category."""
        results = parse_search_results(SAMPLE_MULTIPLE_RESULTS_HTML)
        assert results[2].category == "CONSTITUCION"


class TestParseSearchResultsUnicode:
    """Tests for Unicode handling."""

    def test_parse_title_with_accents(self):
        """Should preserve Spanish accents in titles."""
        results = parse_search_results(SAMPLE_UNICODE_TITLE_HTML)
        assert "CONSTITUCIÓN" in results[0].title
        assert "POLÍTICA" in results[0].title

    def test_parse_title_with_ene(self):
        """Should preserve ñ in titles."""
        html = """
        <div id="gridResultados">
            <table class="dxgvTable">
                <tr class="dxgvDataRow">
                    <td><a href="wfOrdenamientoDetalle.aspx?q=abc">LEY DE NIÑOS Y NIÑAS</a></td>
                    <td>01/01/2020</td>
                    <td>01/01/2020</td>
                    <td>VIGENTE</td>
                    <td>LEY</td>
                    <td>FEDERAL</td>
                    <td></td>
                    <td></td>
                </tr>
            </table>
        </div>
        """
        results = parse_search_results(html)
        assert "NIÑOS" in results[0].title
        assert "NIÑAS" in results[0].title


class TestParseSearchResultsMalformedHTML:
    """Tests for malformed HTML handling."""

    def test_parse_missing_grid_raises_parse_error(self):
        """Should raise ParseError if grid is missing."""
        html = "<html><body><div>No grid here</div></body></html>"
        with pytest.raises(ParseError):
            parse_search_results(html)

    def test_parse_wrong_page_raises_parse_error(self):
        """Should raise ParseError for wrong page type."""
        html = "<html><body><h1>Error Page</h1></body></html>"
        with pytest.raises(ParseError):
            parse_search_results(html)


class TestExtractPaginationInfo:
    """Tests for pagination extraction."""

    def test_extract_first_page(self):
        """Should extract first page info."""
        current, total, next_cb = extract_pagination_info(SAMPLE_PAGINATION_HTML)
        assert current == 1
        assert total == 10

    def test_extract_pagination_from_pager_control(self):
        """Should extract info from DevExpress pager control."""
        html = """
        <div id="gridResultados">
            <div class="dxpPagerTotal">Página 5 de 20</div>
        </div>
        """
        current, total, next_cb = extract_pagination_info(html)
        assert current == 5
        assert total == 20

    def test_extract_single_page_results(self):
        """Single page results should show 1 of 1."""
        html = """
        <div id="gridResultados">
            <div class="dxpPagerTotal">Página 1 de 1</div>
        </div>
        """
        current, total, next_cb = extract_pagination_info(html)
        assert current == 1
        assert total == 1
        assert next_cb is None

    def test_extract_last_page_no_next(self):
        """Last page should have no next callback."""
        html = """
        <div id="gridResultados">
            <div class="dxpPagerTotal">Página 10 de 10</div>
        </div>
        """
        current, total, next_cb = extract_pagination_info(html)
        assert current == 10
        assert total == 10
        assert next_cb is None
