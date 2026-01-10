"""
Test utilities for actor testing.

Provides mock coordinators, HTTP servers, and fixtures for testing actors.
"""
import asyncio
import pytest
from typing import List, Any, Type, Optional

from src.infrastructure.actors.base import BaseActor
from src.infrastructure.actors.messages import ActorMessage


class MockCoordinator(BaseActor):
    """
    Mock coordinator that captures messages for assertions.

    Use this in tests to verify actors send correct messages.
    """

    def __init__(self):
        super().__init__()
        self.received: List[Any] = []
        self._responses: dict = {}

    async def handle_message(self, message):
        """Capture message and return preset response if any."""
        self.received.append(message)

        # Return preset response for ask pattern
        msg_type = type(message)
        if msg_type in self._responses:
            return self._responses[msg_type]
        return None

    def set_response(self, message_type: Type, response: Any) -> None:
        """Set response for ask pattern tests."""
        self._responses[message_type] = response

    def get_messages(self, message_type: Type) -> List[Any]:
        """Get all messages of a specific type."""
        return [m for m in self.received if isinstance(m, message_type)]

    def get_last_message(self, message_type: Type) -> Optional[Any]:
        """Get most recent message of type."""
        messages = self.get_messages(message_type)
        return messages[-1] if messages else None

    def clear(self) -> None:
        """Clear received messages."""
        self.received.clear()

    def assert_received(self, message_type: Type, count: int = 1) -> None:
        """Assert specific message type was received."""
        actual = len(self.get_messages(message_type))
        assert actual == count, f"Expected {count} {message_type.__name__}, got {actual}"

    def assert_not_received(self, message_type: Type) -> None:
        """Assert message type was NOT received."""
        self.assert_received(message_type, count=0)


class MockHTTPServer:
    """
    Mock HTTP server for scraper testing.

    Registers URL patterns with responses for simulated HTTP requests.
    """

    def __init__(self):
        self.responses: dict = {}
        self.requests: List[str] = []

    def add_response(
        self,
        url_pattern: str,
        content: str,
        status: int = 200,
        content_type: str = "text/html",
    ) -> None:
        """Register a mock response for a URL pattern."""
        self.responses[url_pattern] = (content, status, content_type)

    def add_pdf_response(self, url_pattern: str, pdf_bytes: bytes) -> None:
        """Register a mock PDF response."""
        self.responses[url_pattern] = (pdf_bytes, 200, "application/pdf")

    def add_error_response(self, url_pattern: str, status: int, message: str = "") -> None:
        """Register an error response."""
        self.responses[url_pattern] = (message, status, "text/plain")

    async def get(self, url: str) -> tuple:
        """
        Simulate HTTP GET request.

        Returns:
            Tuple of (content, status_code, content_type)
        """
        self.requests.append(url)

        for pattern, (content, status, content_type) in self.responses.items():
            if pattern in url:
                return content, status, content_type

        return "", 404, "text/plain"

    def get_request_count(self, url_pattern: str = None) -> int:
        """Get count of requests, optionally filtered by pattern."""
        if url_pattern is None:
            return len(self.requests)
        return sum(1 for req in self.requests if url_pattern in req)

    def clear_requests(self) -> None:
        """Clear recorded requests."""
        self.requests.clear()


class MockEmbeddingModel:
    """
    Mock embedding model for testing EmbeddingActor.

    Returns deterministic vectors based on text hash.
    """

    def __init__(self, dimension: int = 384):
        self.dimension = dimension
        self.call_count = 0

    def encode(self, texts: List[str]) -> List[List[float]]:
        """Generate mock embeddings."""
        self.call_count += 1
        embeddings = []
        for text in texts:
            # Generate deterministic vector based on text
            hash_val = hash(text) % 1000000
            vector = [(hash_val + i) / 1000000.0 for i in range(self.dimension)]
            embeddings.append(vector)
        return embeddings


# ============ PYTEST FIXTURES ============

@pytest.fixture
async def mock_coordinator():
    """Fixture: Started mock coordinator."""
    coordinator = MockCoordinator()
    await coordinator.start()
    yield coordinator
    await coordinator.stop()


@pytest.fixture
def mock_http():
    """Fixture: Mock HTTP server."""
    return MockHTTPServer()


@pytest.fixture
def instant_rate_limiter():
    """Fixture: Rate limiter that doesn't wait."""
    from src.infrastructure.actors.rate_limiter import NoOpRateLimiter
    return NoOpRateLimiter()


@pytest.fixture
def mock_embedding_model():
    """Fixture: Mock embedding model."""
    return MockEmbeddingModel()


@pytest.fixture
def sample_search_html():
    """Fixture: Sample SCJN search results HTML."""
    return """
    <html>
    <body>
    <div id="gridResultados">
        <table class="dxgvTable">
            <tr class="dxgvDataRow">
                <td><a href="wfOrdenamientoDetalle.aspx?q=abc123%3D%3D">LEY FEDERAL DEL TRABAJO</a></td>
                <td>01/04/1970</td>
                <td>15/03/1970</td>
                <td>VIGENTE</td>
                <td>LEY FEDERAL</td>
                <td>FEDERAL</td>
                <td><a href="wfExtracto.aspx?q=abc123">Extracto</a></td>
                <td><a href="AbrirDocReforma.aspx?q=abc123">PDF</a></td>
            </tr>
            <tr class="dxgvDataRow">
                <td><a href="wfOrdenamientoDetalle.aspx?q=def456%3D%3D">CÓDIGO CIVIL FEDERAL</a></td>
                <td>26/05/1928</td>
                <td>25/05/1928</td>
                <td>VIGENTE</td>
                <td>CODIGO</td>
                <td>FEDERAL</td>
                <td></td>
                <td><a href="AbrirDocReforma.aspx?q=def456">PDF</a></td>
            </tr>
        </table>
        <div class="dxpPagerTotal">Página 1 de 5</div>
    </div>
    </body>
    </html>
    """


@pytest.fixture
def sample_document_html():
    """Fixture: Sample SCJN document detail HTML."""
    return """
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
        <div id="contenido-ordenamiento">
            <div class="articulo" id="art1">
                <h3>Artículo 1</h3>
                <p>La presente Ley es de observancia general en toda la República.</p>
            </div>
            <div class="articulo" id="art2">
                <h3>Artículo 2</h3>
                <p>Las normas del trabajo tienden a conseguir el equilibrio.</p>
            </div>
        </div>
    </div>
    </body>
    </html>
    """


@pytest.fixture
def sample_pdf_bytes():
    """Fixture: Minimal valid PDF bytes for testing."""
    # This is a minimal valid PDF structure
    return b"""%PDF-1.4
1 0 obj
<< /Type /Catalog /Pages 2 0 R >>
endobj
2 0 obj
<< /Type /Pages /Kids [3 0 R] /Count 1 >>
endobj
3 0 obj
<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 4 0 R >>
endobj
4 0 obj
<< /Length 44 >>
stream
BT /F1 12 Tf 100 700 Td (Test Document) Tj ET
endstream
endobj
xref
0 5
0000000000 65535 f
0000000009 00000 n
0000000058 00000 n
0000000115 00000 n
0000000206 00000 n
trailer
<< /Size 5 /Root 1 0 R >>
startxref
300
%%EOF"""


@pytest.fixture
def sample_legal_text():
    """Fixture: Sample Spanish legal text for chunking tests."""
    return """
TÍTULO PRIMERO
Principios Generales

Artículo 1.- La presente Ley es de observancia general en toda la República
y rige las relaciones de trabajo comprendidas en el artículo 123, Apartado A,
de la Constitución.

Artículo 2.- Las normas del trabajo tienden a conseguir el equilibrio entre
los factores de la producción y la justicia social, así como propiciar el
trabajo digno o decente en todas las relaciones laborales.

Se entiende por trabajo digno o decente aquél en el que se respeta plenamente
la dignidad humana del trabajador.

Artículo 3.- El trabajo es un derecho y un deber sociales. No es artículo de
comercio.

TÍTULO SEGUNDO
Relaciones Individuales de Trabajo

CAPÍTULO I
Disposiciones Generales

Artículo 20.- Se entiende por relación de trabajo, cualquiera que sea el acto
que le dé origen, la prestación de un trabajo personal subordinado a una
persona, mediante el pago de un salario.
"""
