"""
Tests for BJV actor messages.

Following RED-GREEN TDD: These tests define the expected behavior
for all BJV actor messages (commands and events).

Target: ~30 tests
"""
import pytest
from datetime import datetime


class TestDiscoveryMessages:
    """Tests for discovery-related messages."""

    def test_iniciar_busqueda_creation(self):
        """IniciarBusqueda can be created with required fields."""
        from src.infrastructure.actors.bjv_messages import IniciarBusqueda

        msg = IniciarBusqueda(
            correlation_id="corr-123",
            query="derecho civil",
        )

        assert msg.correlation_id == "corr-123"
        assert msg.query == "derecho civil"

    def test_iniciar_busqueda_defaults(self):
        """IniciarBusqueda has sensible defaults."""
        from src.infrastructure.actors.bjv_messages import IniciarBusqueda

        msg = IniciarBusqueda(correlation_id="corr-123")

        assert msg.query is None
        assert msg.area_derecho is None
        assert msg.anio_desde is None
        assert msg.anio_hasta is None
        assert msg.max_resultados == 100
        assert msg.timestamp is not None

    def test_iniciar_busqueda_immutable(self):
        """IniciarBusqueda is frozen (immutable)."""
        from src.infrastructure.actors.bjv_messages import IniciarBusqueda

        msg = IniciarBusqueda(correlation_id="corr-123")

        with pytest.raises(Exception):  # FrozenInstanceError
            msg.query = "new query"

    def test_iniciar_busqueda_with_filters(self):
        """IniciarBusqueda accepts all filter parameters."""
        from src.infrastructure.actors.bjv_messages import IniciarBusqueda

        msg = IniciarBusqueda(
            correlation_id="corr-123",
            query="contratos",
            area_derecho="civil",
            anio_desde=2020,
            anio_hasta=2024,
            max_resultados=50,
        )

        assert msg.anio_desde == 2020
        assert msg.anio_hasta == 2024
        assert msg.area_derecho == "civil"

    def test_procesar_pagina_resultados(self):
        """ProcesarPaginaResultados has required fields."""
        from src.infrastructure.actors.bjv_messages import ProcesarPaginaResultados

        msg = ProcesarPaginaResultados(
            correlation_id="corr-123",
            numero_pagina=2,
            url_pagina="https://biblio.juridicas.unam.mx/bjv/resultados?page=2",
        )

        assert msg.numero_pagina == 2
        assert "page=2" in msg.url_pagina

    def test_busqueda_completada(self):
        """BusquedaCompletada has result statistics."""
        from src.infrastructure.actors.bjv_messages import BusquedaCompletada

        msg = BusquedaCompletada(
            correlation_id="corr-123",
            total_descubiertos=150,
            total_paginas=8,
        )

        assert msg.total_descubiertos == 150
        assert msg.total_paginas == 8


class TestDownloadMessages:
    """Tests for download-related messages."""

    def test_descargar_libro(self):
        """DescargarLibro has book details."""
        from src.infrastructure.actors.bjv_messages import DescargarLibro

        msg = DescargarLibro(
            correlation_id="corr-123",
            libro_id="4567",
            url_detalle="https://biblio.juridicas.unam.mx/bjv/detalle-libro/4567",
        )

        assert msg.libro_id == "4567"
        assert msg.incluir_capitulos is True  # default

    def test_descargar_libro_sin_capitulos(self):
        """DescargarLibro can exclude chapters."""
        from src.infrastructure.actors.bjv_messages import DescargarLibro

        msg = DescargarLibro(
            correlation_id="corr-123",
            libro_id="4567",
            url_detalle="url",
            incluir_capitulos=False,
        )

        assert msg.incluir_capitulos is False

    def test_descargar_pdf(self):
        """DescargarPDF has PDF URL and metadata."""
        from src.infrastructure.actors.bjv_messages import DescargarPDF

        msg = DescargarPDF(
            correlation_id="corr-123",
            libro_id="4567",
            capitulo_id="cap-1",
            url_pdf="https://archivos.juridicas.unam.mx/bjv/libros/4567/cap1.pdf",
        )

        assert msg.libro_id == "4567"
        assert msg.capitulo_id == "cap-1"
        assert ".pdf" in msg.url_pdf

    def test_libro_listo(self):
        """LibroListo indicates book is ready for processing."""
        from src.infrastructure.actors.bjv_messages import LibroListo

        libro_mock = object()  # Placeholder for LibroBJV
        msg = LibroListo(
            correlation_id="corr-123",
            libro_id="4567",
            libro=libro_mock,
        )

        assert msg.libro_id == "4567"
        assert msg.libro is libro_mock

    def test_pdf_listo(self):
        """PDFListo indicates PDF is downloaded."""
        from src.infrastructure.actors.bjv_messages import PDFListo

        msg = PDFListo(
            correlation_id="corr-123",
            libro_id="4567",
            capitulo_id="cap-1",
            ruta_pdf="/data/4567/cap1.pdf",
        )

        assert msg.ruta_pdf == "/data/4567/cap1.pdf"


class TestProcessingMessages:
    """Tests for text processing messages."""

    def test_extraer_texto_pdf(self):
        """ExtraerTextoPDF has PDF path."""
        from src.infrastructure.actors.bjv_messages import ExtraerTextoPDF

        msg = ExtraerTextoPDF(
            correlation_id="corr-123",
            libro_id="4567",
            capitulo_id=None,
            ruta_pdf="/data/4567/libro.pdf",
        )

        assert msg.ruta_pdf == "/data/4567/libro.pdf"
        assert msg.capitulo_id is None

    def test_texto_extraido(self):
        """TextoExtraido has extracted text and metadata."""
        from src.infrastructure.actors.bjv_messages import TextoExtraido

        msg = TextoExtraido(
            correlation_id="corr-123",
            libro_id="4567",
            capitulo_id=None,
            texto="Contenido del libro...",
            total_paginas=120,
            confianza=0.95,
        )

        assert msg.total_paginas == 120
        assert msg.confianza == 0.95

    def test_fragmentar_texto(self):
        """FragmentarTexto has text to chunk."""
        from src.infrastructure.actors.bjv_messages import FragmentarTexto

        msg = FragmentarTexto(
            correlation_id="corr-123",
            libro_id="4567",
            capitulo_id=None,
            texto="Texto largo para fragmentar...",
        )

        assert msg.texto == "Texto largo para fragmentar..."

    def test_texto_fragmentado(self):
        """TextoFragmentado has fragments tuple."""
        from src.infrastructure.actors.bjv_messages import TextoFragmentado

        fragmentos = (object(), object())  # Mock fragments
        msg = TextoFragmentado(
            correlation_id="corr-123",
            libro_id="4567",
            total_fragmentos=2,
            fragmentos=fragmentos,
        )

        assert msg.total_fragmentos == 2
        assert len(msg.fragmentos) == 2

    def test_generar_embeddings(self):
        """GenerarEmbeddings has fragments to embed."""
        from src.infrastructure.actors.bjv_messages import GenerarEmbeddings

        fragmentos = (object(),)
        msg = GenerarEmbeddings(
            correlation_id="corr-123",
            libro_id="4567",
            fragmentos=fragmentos,
        )

        assert msg.fragmentos == fragmentos

    def test_embeddings_generados(self):
        """EmbeddingsGenerados confirms embedding count."""
        from src.infrastructure.actors.bjv_messages import EmbeddingsGenerados

        msg = EmbeddingsGenerados(
            correlation_id="corr-123",
            libro_id="4567",
            total_embeddings=15,
        )

        assert msg.total_embeddings == 15


class TestPersistenceMessages:
    """Tests for persistence messages."""

    def test_guardar_libro(self):
        """GuardarLibro has book to persist."""
        from src.infrastructure.actors.bjv_messages import GuardarLibro

        libro_mock = object()
        msg = GuardarLibro(
            correlation_id="corr-123",
            libro=libro_mock,
        )

        assert msg.libro is libro_mock

    def test_guardar_fragmentos(self):
        """GuardarFragmentos has fragments tuple."""
        from src.infrastructure.actors.bjv_messages import GuardarFragmentos

        fragmentos = (object(), object(), object())
        msg = GuardarFragmentos(
            correlation_id="corr-123",
            libro_id="4567",
            fragmentos=fragmentos,
        )

        assert len(msg.fragmentos) == 3

    def test_guardar_embeddings(self):
        """GuardarEmbeddings has embeddings tuple."""
        from src.infrastructure.actors.bjv_messages import GuardarEmbeddings

        embeddings = (object(),)
        msg = GuardarEmbeddings(
            correlation_id="corr-123",
            libro_id="4567",
            embeddings=embeddings,
        )

        assert msg.embeddings == embeddings

    def test_guardar_checkpoint(self):
        """GuardarCheckpoint has checkpoint data."""
        from src.infrastructure.actors.bjv_messages import GuardarCheckpoint

        checkpoint_mock = object()
        msg = GuardarCheckpoint(
            correlation_id="corr-123",
            checkpoint=checkpoint_mock,
        )

        assert msg.checkpoint is checkpoint_mock


class TestPipelineControlMessages:
    """Tests for pipeline control messages."""

    def test_iniciar_pipeline(self):
        """IniciarPipeline starts the pipeline."""
        from src.infrastructure.actors.bjv_messages import IniciarPipeline

        msg = IniciarPipeline(
            session_id="sess-abc",
            max_resultados=200,
            query="derecho penal",
        )

        assert msg.session_id == "sess-abc"
        assert msg.max_resultados == 200
        assert msg.query == "derecho penal"

    def test_iniciar_pipeline_defaults(self):
        """IniciarPipeline has sensible defaults."""
        from src.infrastructure.actors.bjv_messages import IniciarPipeline

        msg = IniciarPipeline(session_id="sess-abc")

        assert msg.max_resultados == 100
        assert msg.query is None

    def test_pausar_pipeline(self):
        """PausarPipeline pauses processing."""
        from src.infrastructure.actors.bjv_messages import PausarPipeline

        msg = PausarPipeline(correlation_id="corr-123")

        assert msg.correlation_id == "corr-123"

    def test_reanudar_pipeline(self):
        """ReanudarPipeline resumes processing."""
        from src.infrastructure.actors.bjv_messages import ReanudarPipeline

        msg = ReanudarPipeline(
            correlation_id="corr-123",
            session_id="sess-abc",
        )

        assert msg.session_id == "sess-abc"

    def test_detener_pipeline(self):
        """DetenerPipeline stops with checkpoint option."""
        from src.infrastructure.actors.bjv_messages import DetenerPipeline

        msg = DetenerPipeline(
            correlation_id="corr-123",
            guardar_checkpoint=True,
        )

        assert msg.guardar_checkpoint is True

    def test_obtener_estado(self):
        """ObtenerEstado queries pipeline state."""
        from src.infrastructure.actors.bjv_messages import ObtenerEstado

        msg = ObtenerEstado()

        assert msg.timestamp is not None


class TestErrorMessages:
    """Tests for error messages."""

    def test_error_procesamiento(self):
        """ErrorProcesamiento has error details."""
        from src.infrastructure.actors.bjv_messages import ErrorProcesamiento

        msg = ErrorProcesamiento(
            correlation_id="corr-123",
            libro_id="4567",
            actor_origen="BJVScraperActor",
            tipo_error="NetworkError",
            mensaje_error="Connection refused",
            recuperable=True,
        )

        assert msg.libro_id == "4567"
        assert msg.actor_origen == "BJVScraperActor"
        assert msg.tipo_error == "NetworkError"
        assert msg.recuperable is True

    def test_error_procesamiento_no_recuperable(self):
        """ErrorProcesamiento can be non-recoverable."""
        from src.infrastructure.actors.bjv_messages import ErrorProcesamiento

        msg = ErrorProcesamiento(
            correlation_id="corr-123",
            libro_id=None,
            actor_origen="BJVPDFActor",
            tipo_error="PDFCorruptedError",
            mensaje_error="Cannot open PDF",
            recuperable=False,
        )

        assert msg.recuperable is False
        assert msg.libro_id is None
