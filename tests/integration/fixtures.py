"""
Integration test fixtures for SCJN scraper pipeline.

Provides MockSCJNServer for simulating SCJN website responses.
"""
import pytest
import pytest_asyncio
import asyncio
from typing import Dict, List, Tuple, Any
from aiohttp import web
from aiohttp.test_utils import TestServer
from pathlib import Path

from src.infrastructure.actors.rate_limiter import NoOpRateLimiter


class MockSCJNServer:
    """
    Mock SCJN website for integration testing.

    Simulates:
    - Search results pages with pagination
    - Document detail pages
    - PDF downloads
    - Rate limiting responses (429)
    - Various error conditions
    """

    def __init__(self):
        self.app = web.Application()
        self._setup_routes()
        self._request_log: List[Tuple[str, Any]] = []
        self._should_rate_limit = False
        self._error_on_q_param: Dict[str, int] = {}  # q_param -> status code
        self.base_url: str = ""

    def _setup_routes(self):
        """Set up HTTP routes."""
        self.app.router.add_get('/Buscador/Paginas/Buscar.aspx', self._handle_search)
        self.app.router.add_get('/Buscador/Paginas/wfOrdenamientoDetalle.aspx', self._handle_detail)
        self.app.router.add_get('/Buscador/Paginas/AbrirDocReforma.aspx', self._handle_pdf)

    async def _handle_search(self, request: web.Request) -> web.Response:
        """Handle search requests."""
        self._request_log.append(('search', dict(request.query)))

        if self._should_rate_limit:
            return web.Response(status=429, text="Rate limited")

        page = int(request.query.get('pagina', '1'))
        category = request.query.get('categoria', '')

        return web.Response(
            text=self._generate_search_html(page, category),
            content_type='text/html',
        )

    async def _handle_detail(self, request: web.Request) -> web.Response:
        """Handle document detail requests."""
        q_param = request.query.get('q', '')
        self._request_log.append(('detail', q_param))

        if q_param in self._error_on_q_param:
            return web.Response(status=self._error_on_q_param[q_param])

        return web.Response(
            text=self._generate_detail_html(q_param),
            content_type='text/html',
        )

    async def _handle_pdf(self, request: web.Request) -> web.Response:
        """Handle PDF download requests."""
        q_param = request.query.get('q', '')
        self._request_log.append(('pdf', q_param))

        return web.Response(
            body=self._generate_mock_pdf(),
            content_type='application/pdf',
        )

    def _generate_search_html(self, page: int, category: str = "") -> str:
        """Generate mock search results HTML."""
        results = []
        start = (page - 1) * 10

        for i in range(10):
            idx = start + i
            results.append(f'''
                <tr class="dxgvDataRow">
                    <td><a href="wfOrdenamientoDetalle.aspx?q=test_q_param_{idx}">LEY DE PRUEBA {idx}</a></td>
                    <td>01/01/2023</td>
                    <td>01/01/2023</td>
                    <td>VIGENTE</td>
                    <td>LEY FEDERAL</td>
                    <td>FEDERAL</td>
                </tr>
            ''')

        total_pages = 3 if page < 3 else page

        return f'''
        <html>
        <body>
        <div id="gridResultados">
            <table class="dxgvTable">
                {''.join(results)}
            </table>
            <div class="dxpPagerTotal">Página {page} de {total_pages}</div>
        </div>
        </body>
        </html>
        '''

    def _generate_detail_html(self, q_param: str) -> str:
        """Generate mock document detail HTML."""
        return f'''
        <html>
        <body>
        <div id="contenedor">
            <h1 class="titulo-ordenamiento">LEY DE PRUEBA - {q_param}</h1>
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
                    <span class="valor">01/01/2023</span>
                </div>
            </div>
            <div id="reformas">
                <table>
                    <tr class="dxgvDataRow">
                        <td><a href="AbrirDocReforma.aspx?q={q_param}_reform1">PDF</a></td>
                        <td>15/03/2023</td>
                        <td>DOF 15-03-2023</td>
                    </tr>
                </table>
            </div>
        </div>
        </body>
        </html>
        '''

    def _generate_mock_pdf(self) -> bytes:
        """Generate minimal valid PDF bytes."""
        return b'''%PDF-1.4
1 0 obj<</Type/Catalog/Pages 2 0 R>>endobj
2 0 obj<</Type/Pages/Kids[3 0 R]/Count 1>>endobj
3 0 obj<</Type/Page/MediaBox[0 0 612 792]/Parent 2 0 R/Resources<<>>>>endobj
xref
0 4
0000000000 65535 f
0000000009 00000 n
0000000052 00000 n
0000000101 00000 n
trailer<</Size 4/Root 1 0 R>>
startxref
178
%%EOF'''

    # Test control methods
    def enable_rate_limiting(self):
        """Enable 429 responses."""
        self._should_rate_limit = True

    def disable_rate_limiting(self):
        """Disable 429 responses."""
        self._should_rate_limit = False

    def set_error_for_q_param(self, q_param: str, status_code: int):
        """Set specific q_param to return an error."""
        self._error_on_q_param[q_param] = status_code

    def clear_errors(self):
        """Clear all error configurations."""
        self._error_on_q_param.clear()

    def get_request_log(self) -> List[Tuple[str, Any]]:
        """Get log of all requests."""
        return self._request_log.copy()

    def clear_request_log(self):
        """Clear request log."""
        self._request_log.clear()

    def get_search_request_count(self) -> int:
        """Get count of search requests."""
        return sum(1 for req_type, _ in self._request_log if req_type == 'search')

    def get_detail_request_count(self) -> int:
        """Get count of detail requests."""
        return sum(1 for req_type, _ in self._request_log if req_type == 'detail')

    def get_pdf_request_count(self) -> int:
        """Get count of PDF requests."""
        return sum(1 for req_type, _ in self._request_log if req_type == 'pdf')


@pytest_asyncio.fixture
async def mock_scjn_server():
    """Fixture: Running mock SCJN server."""
    server = MockSCJNServer()
    runner = web.AppRunner(server.app)
    await runner.setup()
    site = web.TCPSite(runner, 'localhost', 0)
    await site.start()

    # Get actual port
    port = site._server.sockets[0].getsockname()[1]
    server.base_url = f"http://localhost:{port}/Buscador/Paginas"

    yield server

    await runner.cleanup()


@pytest.fixture
def no_rate_limiter():
    """Fixture: Rate limiter that doesn't wait."""
    return NoOpRateLimiter()


@pytest.fixture
def temp_output_dir(tmp_path):
    """Fixture: Temporary output directory."""
    output_dir = tmp_path / "scjn_output"
    output_dir.mkdir()
    return output_dir


@pytest.fixture
def temp_checkpoint_dir(tmp_path):
    """Fixture: Temporary checkpoint directory."""
    checkpoint_dir = tmp_path / "checkpoints"
    checkpoint_dir.mkdir()
    return checkpoint_dir


# Shared test HTML fixtures
SAMPLE_SEARCH_HTML = '''
<html>
<body>
<div id="gridResultados">
    <table class="dxgvTable">
        <tr class="dxgvDataRow">
            <td><a href="wfOrdenamientoDetalle.aspx?q=ABC123">Ley Federal del Trabajo</a></td>
            <td>01/04/1970</td>
            <td>01/04/1970</td>
            <td>VIGENTE</td>
            <td>LEY FEDERAL</td>
            <td>FEDERAL</td>
        </tr>
        <tr class="dxgvDataRow">
            <td><a href="wfOrdenamientoDetalle.aspx?q=DEF456">Codigo Civil Federal</a></td>
            <td>26/05/1928</td>
            <td>26/05/1928</td>
            <td>VIGENTE</td>
            <td>CODIGO</td>
            <td>FEDERAL</td>
        </tr>
    </table>
    <div class="dxpPagerTotal">Página 1 de 1</div>
</div>
</body>
</html>
'''

SAMPLE_DETAIL_HTML = '''
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
        <div class="dato">
            <span class="etiqueta">Fecha de Publicación:</span>
            <span class="valor">01/04/1970</span>
        </div>
    </div>
</div>
</body>
</html>
'''
