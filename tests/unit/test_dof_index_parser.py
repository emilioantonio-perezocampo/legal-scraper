import pytest
from src.infrastructure.adapters.dof_index_parser import parse_dof_index

# A snippet of the DOF daily index page
# It contains links to specific notes (nota_detalle.php)
SAMPLE_INDEX_HTML = """
<html>
    <body>
       <div id="cuerpo_principal">
          <a href="nota_detalle.php?codigo=12345&fecha=29/12/2025" class="enlaces">
             ACUERDO por el que se modifican las reglas...
          </a>
          <br>
          <a href="nota_detalle.php?codigo=67890&fecha=29/12/2025" class="enlaces">
             DECRETO por el que se expide la Ley...
          </a>
          <a href="contactenos.php">Contacto</a>
       </div>
    </body>
</html>
"""

def test_parse_dof_index_finds_urls():
    """
    Test that we can extract the relevant article URLs from the index page.
    """
    links = parse_dof_index(SAMPLE_INDEX_HTML)

    assert len(links) == 2
    # Function returns list of dicts with 'url' and 'title' keys
    urls = [item['url'] for item in links]
    assert "https://dof.gob.mx/nota_detalle.php?codigo=12345&fecha=29/12/2025" in urls
    assert "https://dof.gob.mx/nota_detalle.php?codigo=67890&fecha=29/12/2025" in urls