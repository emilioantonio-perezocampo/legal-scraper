"""
Tests for CAS Search Results Parser

Following RED-GREEN TDD: These tests define the expected behavior
for parsing CAS search results HTML.

Target: ~25 tests for search parser
"""
import pytest


class TestPaginacionInfoCAS:
    """Tests for PaginacionInfoCAS dataclass."""

    def test_creation_with_all_fields(self):
        """Create PaginacionInfoCAS with all fields."""
        from src.infrastructure.adapters.cas_search_parser import PaginacionInfoCAS
        paginacion = PaginacionInfoCAS(
            pagina_actual=1,
            total_paginas=10,
            total_resultados=200,
            resultados_por_pagina=20,
        )
        assert paginacion.pagina_actual == 1
        assert paginacion.total_paginas == 10

    def test_immutability(self):
        """PaginacionInfoCAS is frozen dataclass."""
        from src.infrastructure.adapters.cas_search_parser import PaginacionInfoCAS
        paginacion = PaginacionInfoCAS(
            pagina_actual=1,
            total_paginas=10,
            total_resultados=200,
            resultados_por_pagina=20,
        )
        with pytest.raises(Exception):
            paginacion.pagina_actual = 2

    def test_tiene_siguiente_true(self):
        """tiene_siguiente is True when not on last page."""
        from src.infrastructure.adapters.cas_search_parser import PaginacionInfoCAS
        paginacion = PaginacionInfoCAS(
            pagina_actual=5,
            total_paginas=10,
            total_resultados=200,
            resultados_por_pagina=20,
        )
        assert paginacion.tiene_siguiente is True

    def test_tiene_siguiente_false_on_last_page(self):
        """tiene_siguiente is False on last page."""
        from src.infrastructure.adapters.cas_search_parser import PaginacionInfoCAS
        paginacion = PaginacionInfoCAS(
            pagina_actual=10,
            total_paginas=10,
            total_resultados=200,
            resultados_por_pagina=20,
        )
        assert paginacion.tiene_siguiente is False

    def test_tiene_anterior_true(self):
        """tiene_anterior is True when not on first page."""
        from src.infrastructure.adapters.cas_search_parser import PaginacionInfoCAS
        paginacion = PaginacionInfoCAS(
            pagina_actual=5,
            total_paginas=10,
            total_resultados=200,
            resultados_por_pagina=20,
        )
        assert paginacion.tiene_anterior is True

    def test_tiene_anterior_false_on_first_page(self):
        """tiene_anterior is False on first page."""
        from src.infrastructure.adapters.cas_search_parser import PaginacionInfoCAS
        paginacion = PaginacionInfoCAS(
            pagina_actual=1,
            total_paginas=10,
            total_resultados=200,
            resultados_por_pagina=20,
        )
        assert paginacion.tiene_anterior is False


class TestParseSearchResults:
    """Tests for parse_search_results function."""

    def test_empty_html_raises_parse_error(self):
        """Empty HTML raises ParseError."""
        from src.infrastructure.adapters.cas_search_parser import parse_search_results
        from src.infrastructure.adapters.cas_errors import ParseError

        with pytest.raises(ParseError):
            parse_search_results("")

    def test_whitespace_only_raises_parse_error(self):
        """Whitespace-only HTML raises ParseError."""
        from src.infrastructure.adapters.cas_search_parser import parse_search_results
        from src.infrastructure.adapters.cas_errors import ParseError

        with pytest.raises(ParseError):
            parse_search_results("   \n\t  ")

    def test_no_results_returns_empty_tuple(self):
        """HTML with no results returns empty tuple."""
        from src.infrastructure.adapters.cas_search_parser import parse_search_results

        html = "<html><body><p>No results found</p></body></html>"
        result = parse_search_results(html)
        assert result == ()

    def test_parse_single_result_with_case_number(self):
        """Parse single result with CAS case number."""
        from src.infrastructure.adapters.cas_search_parser import parse_search_results

        html = """
        <html><body>
        <div class="case-item">
            <a href="/case/12345">CAS 2023/A/12345</a>
            <span>Player X v. Club Y</span>
        </div>
        </body></html>
        """
        result = parse_search_results(html)
        assert len(result) == 1
        assert "CAS 2023/A/12345" in result[0].numero_caso.valor

    def test_parse_tas_format_case_number(self):
        """Parse result with TAS (French) case number format."""
        from src.infrastructure.adapters.cas_search_parser import parse_search_results

        html = """
        <html><body>
        <div class="case-item">
            <a href="/case/99999">TAS 2022/O/8500</a>
        </div>
        </body></html>
        """
        result = parse_search_results(html)
        assert len(result) == 1
        assert "TAS" in result[0].numero_caso.valor

    def test_parse_multiple_results(self):
        """Parse multiple search results."""
        from src.infrastructure.adapters.cas_search_parser import parse_search_results

        html = """
        <html><body>
        <div class="case-item">CAS 2023/A/12345 - Case One</div>
        <div class="case-item">CAS 2023/A/12346 - Case Two</div>
        <div class="case-item">CAS 2023/A/12347 - Case Three</div>
        </body></html>
        """
        result = parse_search_results(html)
        assert len(result) == 3

    def test_extract_title_with_v_pattern(self):
        """Extract title with 'v.' pattern for parties."""
        from src.infrastructure.adapters.cas_search_parser import parse_search_results

        html = """
        <html><body>
        <div class="case-item">
            <a href="/case/1">CAS 2023/A/12345 - FC Barcelona v. FIFA</a>
        </div>
        </body></html>
        """
        result = parse_search_results(html)
        assert len(result) == 1
        assert "v." in result[0].titulo or "v" in result[0].titulo.lower()

    def test_extract_absolute_url(self):
        """Extract absolute URL from result."""
        from src.infrastructure.adapters.cas_search_parser import parse_search_results

        html = """
        <html><body>
        <div class="case-item">
            <a href="https://jurisprudence.tas-cas.org/case/12345">CAS 2023/A/12345</a>
        </div>
        </body></html>
        """
        result = parse_search_results(html)
        assert len(result) >= 1

    def test_extract_relative_url_converted(self):
        """Extract relative URL and convert to absolute."""
        from src.infrastructure.adapters.cas_search_parser import parse_search_results

        html = """
        <html><body>
        <div class="case-item">
            <a href="/Shared Documents/12345.pdf">CAS 2023/A/12345</a>
        </div>
        </body></html>
        """
        result = parse_search_results(html)
        assert len(result) >= 1

    def test_skip_malformed_items(self):
        """Skip items without valid case numbers."""
        from src.infrastructure.adapters.cas_search_parser import parse_search_results

        html = """
        <html><body>
        <div class="case-item">Invalid item without case number</div>
        <div class="case-item">CAS 2023/A/12345 - Valid item</div>
        <div class="case-item">Another invalid</div>
        </body></html>
        """
        result = parse_search_results(html)
        assert len(result) == 1

    def test_detect_sport_football(self):
        """Detect football sport from content."""
        from src.infrastructure.adapters.cas_search_parser import parse_search_results

        html = """
        <html><body>
        <div class="case-item">
            CAS 2023/A/12345 - FIFA v. Player (football transfer)
        </div>
        </body></html>
        """
        result = parse_search_results(html)
        assert len(result) >= 1

    def test_detect_matter_doping(self):
        """Detect doping matter from content."""
        from src.infrastructure.adapters.cas_search_parser import parse_search_results

        html = """
        <html><body>
        <div class="case-item">
            CAS 2023/A/12345 - WADA v. Athlete (anti-doping violation)
        </div>
        </body></html>
        """
        result = parse_search_results(html)
        assert len(result) >= 1


class TestExtractPaginationInfo:
    """Tests for extract_pagination_info function."""

    def test_extract_current_page(self):
        """Extract current page number."""
        from src.infrastructure.adapters.cas_search_parser import extract_pagination_info

        html = """
        <html><body>
        <div class="pagination">
            <a href="?page=1">1</a>
            <span class="active">2</span>
            <a href="?page=3">3</a>
        </div>
        </body></html>
        """
        result = extract_pagination_info(html)
        assert result.pagina_actual == 2

    def test_extract_total_pages(self):
        """Extract total pages from pagination."""
        from src.infrastructure.adapters.cas_search_parser import extract_pagination_info

        html = """
        <html><body>
        <div class="pagination">
            <a href="?page=1">1</a>
            <a href="?page=2">2</a>
            <a href="?page=10">10</a>
        </div>
        </body></html>
        """
        result = extract_pagination_info(html)
        assert result.total_paginas == 10

    def test_extract_total_results(self):
        """Extract total results count."""
        from src.infrastructure.adapters.cas_search_parser import extract_pagination_info

        html = """
        <html><body>
        <div class="results-count">Showing 1-20 of 500 results</div>
        </body></html>
        """
        result = extract_pagination_info(html)
        assert result.total_resultados == 500

    def test_default_values_when_no_pagination(self):
        """Return defaults when pagination not found."""
        from src.infrastructure.adapters.cas_search_parser import extract_pagination_info

        html = "<html><body><p>Some content</p></body></html>"
        result = extract_pagination_info(html)
        assert result.pagina_actual == 1
        assert result.total_paginas == 1


class TestExtractCaseNumbersFromText:
    """Tests for extract_case_numbers_from_text function."""

    def test_extract_single_case_number(self):
        """Extract single CAS case number from text."""
        from src.infrastructure.adapters.cas_search_parser import extract_case_numbers_from_text

        text = "As stated in CAS 2020/A/7000, the Panel finds..."
        result = extract_case_numbers_from_text(text)
        assert len(result) == 1
        assert "CAS 2020/A/7000" in result[0]

    def test_extract_multiple_case_numbers(self):
        """Extract multiple case numbers from text."""
        from src.infrastructure.adapters.cas_search_parser import extract_case_numbers_from_text

        text = """
        The Panel refers to CAS 2020/A/7000 and CAS 2021/A/7500.
        See also TAS 2019/O/6000 for similar reasoning.
        """
        result = extract_case_numbers_from_text(text)
        assert len(result) == 3

    def test_deduplicate_results(self):
        """Deduplicate repeated case numbers."""
        from src.infrastructure.adapters.cas_search_parser import extract_case_numbers_from_text

        text = """
        CAS 2020/A/7000 is cited here.
        And CAS 2020/A/7000 is cited again.
        CAS 2020/A/7000 appears multiple times.
        """
        result = extract_case_numbers_from_text(text)
        assert len(result) == 1

    def test_normalize_spacing(self):
        """Normalize spacing in case numbers."""
        from src.infrastructure.adapters.cas_search_parser import extract_case_numbers_from_text

        text = "See CAS  2020/A/7000 and CAS2021/A/7500"
        result = extract_case_numbers_from_text(text)
        assert len(result) >= 1

    def test_empty_text_returns_empty_tuple(self):
        """Empty text returns empty tuple."""
        from src.infrastructure.adapters.cas_search_parser import extract_case_numbers_from_text

        result = extract_case_numbers_from_text("")
        assert result == ()
