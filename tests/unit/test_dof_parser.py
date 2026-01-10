import pytest
from datetime import date
from bs4 import BeautifulSoup
from src.domain.entities import FederalLaw
# This doesn't exist yet - we are about to build it
from src.infrastructure.adapters.dof_parser import parse_dof_html

# A sample snippet of what DOF HTML actually looks like (simplified)
SAMPLE_HTML = """
<html>
    <body>
        <div id="DivDetalleNota">
            <h3 class="titulo">LEY FEDERAL DEL TRABAJO</h3>
            <span id="lblFecha">01/04/1970</span>
            <div class="Articulo">
                <p><strong>Artículo 1o.-</strong> La presente Ley es de observancia general...</p>
            </div>
            <div class="Articulo">
                <p><strong>Artículo 2o.-</strong> Las normas del trabajo tienden a conseguir...</p>
            </div>
        </div>
    </body>
</html>
"""

def test_parse_dof_html_extracts_law():
    """
    Test that our parsing logic correctly converts raw HTML into a Domain Entity.
    """
    # 1. Run the parser
    law = parse_dof_html(SAMPLE_HTML)

    # 2. Verify the entity
    assert isinstance(law, FederalLaw)
    assert law.title == "LEY FEDERAL DEL TRABAJO"
    assert law.publication_date == date(1970, 4, 1)
    
    # 3. Verify Articles
    assert len(law.articles) == 2
    assert law.articles[0].identifier == "Artículo 1o.-"
    assert "observancia general" in law.articles[0].content