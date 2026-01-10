"""
Tests for CAS Domain Entities

Following RED-GREEN TDD: These tests define the expected behavior
for CAS/TAS jurisprudence scraper entities.

Target: ~45 tests for entities
"""
import pytest
from datetime import date, datetime
from uuid import uuid4


class TestArbitro:
    """Tests for Arbitro entity."""

    def test_creation_with_required_fields(self):
        """Create Arbitro with name and nationality."""
        from src.domain.cas_entities import Arbitro
        arbitro = Arbitro(
            nombre="Dr. John Smith",
            nacionalidad="United Kingdom",
        )
        assert arbitro.nombre == "Dr. John Smith"
        assert arbitro.nacionalidad == "United Kingdom"

    def test_optional_rol_field(self):
        """Arbitro can have optional rol field."""
        from src.domain.cas_entities import Arbitro
        arbitro = Arbitro(
            nombre="Dr. John Smith",
            nacionalidad="United Kingdom",
            rol="President",
        )
        assert arbitro.rol == "President"

    def test_immutability(self):
        """Arbitro should be immutable."""
        from src.domain.cas_entities import Arbitro
        arbitro = Arbitro(nombre="Dr. John Smith", nacionalidad="UK")
        with pytest.raises(Exception):
            arbitro.nombre = "Jane Doe"

    def test_equality(self):
        """Two Arbitros with same fields are equal."""
        from src.domain.cas_entities import Arbitro
        a1 = Arbitro(nombre="Dr. John Smith", nacionalidad="UK")
        a2 = Arbitro(nombre="Dr. John Smith", nacionalidad="UK")
        assert a1 == a2


class TestParte:
    """Tests for Parte entity."""

    def test_creation_with_required_fields(self):
        """Create Parte with name and type."""
        from src.domain.cas_entities import Parte, TipoParte
        parte = Parte(
            nombre="FC Barcelona",
            tipo=TipoParte.APELANTE,
        )
        assert parte.nombre == "FC Barcelona"
        assert parte.tipo == TipoParte.APELANTE

    def test_tipo_apelado(self):
        """Parte can be APELADO type."""
        from src.domain.cas_entities import Parte, TipoParte
        parte = Parte(nombre="FIFA", tipo=TipoParte.APELADO)
        assert parte.tipo == TipoParte.APELADO

    def test_tipo_demandante(self):
        """Parte can be DEMANDANTE type."""
        from src.domain.cas_entities import Parte, TipoParte
        parte = Parte(nombre="Player X", tipo=TipoParte.DEMANDANTE)
        assert parte.tipo == TipoParte.DEMANDANTE

    def test_tipo_demandado(self):
        """Parte can be DEMANDADO type."""
        from src.domain.cas_entities import Parte, TipoParte
        parte = Parte(nombre="Club Y", tipo=TipoParte.DEMANDADO)
        assert parte.tipo == TipoParte.DEMANDADO

    def test_optional_pais_field(self):
        """Parte can have optional pais field."""
        from src.domain.cas_entities import Parte, TipoParte
        parte = Parte(
            nombre="FC Barcelona",
            tipo=TipoParte.APELANTE,
            pais="Spain",
        )
        assert parte.pais == "Spain"

    def test_immutability(self):
        """Parte should be immutable."""
        from src.domain.cas_entities import Parte, TipoParte
        parte = Parte(nombre="FC Barcelona", tipo=TipoParte.APELANTE)
        with pytest.raises(Exception):
            parte.nombre = "Real Madrid"


class TestFederacion:
    """Tests for Federacion entity."""

    def test_creation_with_required_fields(self):
        """Create Federacion with nombre and acronimo."""
        from src.domain.cas_entities import Federacion
        fed = Federacion(
            nombre="Fédération Internationale de Football Association",
            acronimo="FIFA",
        )
        assert fed.nombre == "Fédération Internationale de Football Association"
        assert fed.acronimo == "FIFA"

    def test_optional_deporte_field(self):
        """Federacion can have optional deporte field."""
        from src.domain.cas_entities import Federacion
        from src.domain.cas_value_objects import CategoriaDeporte
        fed = Federacion(
            nombre="FIFA",
            acronimo="FIFA",
            deporte=CategoriaDeporte.FUTBOL,
        )
        assert fed.deporte == CategoriaDeporte.FUTBOL

    def test_immutability(self):
        """Federacion should be immutable."""
        from src.domain.cas_entities import Federacion
        fed = Federacion(nombre="FIFA", acronimo="FIFA")
        with pytest.raises(Exception):
            fed.acronimo = "UEFA"


class TestLaudoArbitral:
    """Tests for LaudoArbitral aggregate root."""

    def test_creation_with_required_fields(self):
        """Create LaudoArbitral with required fields."""
        from src.domain.cas_entities import LaudoArbitral
        from src.domain.cas_value_objects import NumeroCaso, FechaLaudo

        laudo = LaudoArbitral(
            id=str(uuid4()),
            numero_caso=NumeroCaso(valor="CAS 2023/A/12345"),
            fecha=FechaLaudo(valor=date(2023, 6, 15)),
            titulo="Player X v. Club Y",
        )
        assert laudo.numero_caso.valor == "CAS 2023/A/12345"
        assert laudo.titulo == "Player X v. Club Y"

    def test_has_tipo_procedimiento(self):
        """LaudoArbitral has tipo_procedimiento field."""
        from src.domain.cas_entities import LaudoArbitral
        from src.domain.cas_value_objects import (
            NumeroCaso, FechaLaudo, TipoProcedimiento
        )

        laudo = LaudoArbitral(
            id=str(uuid4()),
            numero_caso=NumeroCaso(valor="CAS 2023/A/12345"),
            fecha=FechaLaudo(valor=date(2023, 6, 15)),
            titulo="Test Case",
            tipo_procedimiento=TipoProcedimiento.ARBITRAJE_APELACION,
        )
        assert laudo.tipo_procedimiento == TipoProcedimiento.ARBITRAJE_APELACION

    def test_has_categoria_deporte(self):
        """LaudoArbitral has categoria_deporte field."""
        from src.domain.cas_entities import LaudoArbitral
        from src.domain.cas_value_objects import (
            NumeroCaso, FechaLaudo, CategoriaDeporte
        )

        laudo = LaudoArbitral(
            id=str(uuid4()),
            numero_caso=NumeroCaso(valor="CAS 2023/A/12345"),
            fecha=FechaLaudo(valor=date(2023, 6, 15)),
            titulo="Test Case",
            categoria_deporte=CategoriaDeporte.FUTBOL,
        )
        assert laudo.categoria_deporte == CategoriaDeporte.FUTBOL

    def test_has_materia(self):
        """LaudoArbitral has materia field."""
        from src.domain.cas_entities import LaudoArbitral
        from src.domain.cas_value_objects import NumeroCaso, FechaLaudo, TipoMateria

        laudo = LaudoArbitral(
            id=str(uuid4()),
            numero_caso=NumeroCaso(valor="CAS 2023/A/12345"),
            fecha=FechaLaudo(valor=date(2023, 6, 15)),
            titulo="Test Case",
            materia=TipoMateria.TRANSFERENCIA,
        )
        assert laudo.materia == TipoMateria.TRANSFERENCIA

    def test_has_partes_tuple(self):
        """LaudoArbitral has partes as tuple."""
        from src.domain.cas_entities import LaudoArbitral, Parte, TipoParte
        from src.domain.cas_value_objects import NumeroCaso, FechaLaudo

        partes = (
            Parte(nombre="Player X", tipo=TipoParte.APELANTE),
            Parte(nombre="Club Y", tipo=TipoParte.APELADO),
        )
        laudo = LaudoArbitral(
            id=str(uuid4()),
            numero_caso=NumeroCaso(valor="CAS 2023/A/12345"),
            fecha=FechaLaudo(valor=date(2023, 6, 15)),
            titulo="Test Case",
            partes=partes,
        )
        assert len(laudo.partes) == 2
        assert isinstance(laudo.partes, tuple)

    def test_has_arbitros_tuple(self):
        """LaudoArbitral has arbitros as tuple."""
        from src.domain.cas_entities import LaudoArbitral, Arbitro
        from src.domain.cas_value_objects import NumeroCaso, FechaLaudo

        arbitros = (
            Arbitro(nombre="Dr. John Smith", nacionalidad="UK", rol="President"),
            Arbitro(nombre="Prof. Jane Doe", nacionalidad="Germany"),
        )
        laudo = LaudoArbitral(
            id=str(uuid4()),
            numero_caso=NumeroCaso(valor="CAS 2023/A/12345"),
            fecha=FechaLaudo(valor=date(2023, 6, 15)),
            titulo="Test Case",
            arbitros=arbitros,
        )
        assert len(laudo.arbitros) == 3 or len(laudo.arbitros) == 2
        assert isinstance(laudo.arbitros, tuple)

    def test_has_federaciones_tuple(self):
        """LaudoArbitral has federaciones as tuple."""
        from src.domain.cas_entities import LaudoArbitral, Federacion
        from src.domain.cas_value_objects import NumeroCaso, FechaLaudo

        federaciones = (
            Federacion(nombre="FIFA", acronimo="FIFA"),
        )
        laudo = LaudoArbitral(
            id=str(uuid4()),
            numero_caso=NumeroCaso(valor="CAS 2023/A/12345"),
            fecha=FechaLaudo(valor=date(2023, 6, 15)),
            titulo="Test Case",
            federaciones=federaciones,
        )
        assert len(laudo.federaciones) == 1
        assert isinstance(laudo.federaciones, tuple)

    def test_has_url_laudo(self):
        """LaudoArbitral has url field."""
        from src.domain.cas_entities import LaudoArbitral
        from src.domain.cas_value_objects import NumeroCaso, FechaLaudo, URLLaudo

        laudo = LaudoArbitral(
            id=str(uuid4()),
            numero_caso=NumeroCaso(valor="CAS 2023/A/12345"),
            fecha=FechaLaudo(valor=date(2023, 6, 15)),
            titulo="Test Case",
            url=URLLaudo(valor="https://jurisprudence.tas-cas.org/doc.pdf"),
        )
        assert laudo.url is not None

    def test_has_idioma(self):
        """LaudoArbitral has idioma field."""
        from src.domain.cas_entities import LaudoArbitral
        from src.domain.cas_value_objects import NumeroCaso, FechaLaudo, IdiomaLaudo

        laudo = LaudoArbitral(
            id=str(uuid4()),
            numero_caso=NumeroCaso(valor="CAS 2023/A/12345"),
            fecha=FechaLaudo(valor=date(2023, 6, 15)),
            titulo="Test Case",
            idioma=IdiomaLaudo.INGLES,
        )
        assert laudo.idioma == IdiomaLaudo.INGLES

    def test_has_resumen(self):
        """LaudoArbitral has resumen field."""
        from src.domain.cas_entities import LaudoArbitral
        from src.domain.cas_value_objects import NumeroCaso, FechaLaudo

        laudo = LaudoArbitral(
            id=str(uuid4()),
            numero_caso=NumeroCaso(valor="CAS 2023/A/12345"),
            fecha=FechaLaudo(valor=date(2023, 6, 15)),
            titulo="Test Case",
            resumen="This case concerns a transfer dispute...",
        )
        assert laudo.resumen == "This case concerns a transfer dispute..."

    def test_has_texto_completo(self):
        """LaudoArbitral has texto_completo field."""
        from src.domain.cas_entities import LaudoArbitral
        from src.domain.cas_value_objects import NumeroCaso, FechaLaudo

        laudo = LaudoArbitral(
            id=str(uuid4()),
            numero_caso=NumeroCaso(valor="CAS 2023/A/12345"),
            fecha=FechaLaudo(valor=date(2023, 6, 15)),
            titulo="Test Case",
            texto_completo="ARBITRAL AWARD rendered by...",
        )
        assert laudo.texto_completo is not None

    def test_has_estado(self):
        """LaudoArbitral has estado field."""
        from src.domain.cas_entities import LaudoArbitral
        from src.domain.cas_value_objects import NumeroCaso, FechaLaudo, EstadoLaudo

        laudo = LaudoArbitral(
            id=str(uuid4()),
            numero_caso=NumeroCaso(valor="CAS 2023/A/12345"),
            fecha=FechaLaudo(valor=date(2023, 6, 15)),
            titulo="Test Case",
            estado=EstadoLaudo.PUBLICADO,
        )
        assert laudo.estado == EstadoLaudo.PUBLICADO

    def test_has_palabras_clave_tuple(self):
        """LaudoArbitral has palabras_clave as tuple of strings."""
        from src.domain.cas_entities import LaudoArbitral
        from src.domain.cas_value_objects import NumeroCaso, FechaLaudo

        laudo = LaudoArbitral(
            id=str(uuid4()),
            numero_caso=NumeroCaso(valor="CAS 2023/A/12345"),
            fecha=FechaLaudo(valor=date(2023, 6, 15)),
            titulo="Test Case",
            palabras_clave=("transfer", "contract", "breach"),
        )
        assert len(laudo.palabras_clave) == 3
        assert isinstance(laudo.palabras_clave, tuple)

    def test_immutability(self):
        """LaudoArbitral should be immutable."""
        from src.domain.cas_entities import LaudoArbitral
        from src.domain.cas_value_objects import NumeroCaso, FechaLaudo

        laudo = LaudoArbitral(
            id=str(uuid4()),
            numero_caso=NumeroCaso(valor="CAS 2023/A/12345"),
            fecha=FechaLaudo(valor=date(2023, 6, 15)),
            titulo="Test Case",
        )
        with pytest.raises(Exception):
            laudo.titulo = "Modified Title"

    def test_has_fecha_creacion(self):
        """LaudoArbitral has fecha_creacion field."""
        from src.domain.cas_entities import LaudoArbitral
        from src.domain.cas_value_objects import NumeroCaso, FechaLaudo

        laudo = LaudoArbitral(
            id=str(uuid4()),
            numero_caso=NumeroCaso(valor="CAS 2023/A/12345"),
            fecha=FechaLaudo(valor=date(2023, 6, 15)),
            titulo="Test Case",
        )
        assert laudo.fecha_creacion is not None
        assert isinstance(laudo.fecha_creacion, datetime)


class TestFragmentoLaudo:
    """Tests for FragmentoLaudo entity (text chunks for RAG)."""

    def test_creation_with_required_fields(self):
        """Create FragmentoLaudo with required fields."""
        from src.domain.cas_entities import FragmentoLaudo

        fragmento = FragmentoLaudo(
            id=str(uuid4()),
            laudo_id=str(uuid4()),
            texto="The Panel finds that the Appellant has failed...",
            posicion=0,
        )
        assert fragmento.texto is not None
        assert fragmento.posicion == 0

    def test_has_laudo_id(self):
        """FragmentoLaudo references parent laudo."""
        from src.domain.cas_entities import FragmentoLaudo

        laudo_id = str(uuid4())
        fragmento = FragmentoLaudo(
            id=str(uuid4()),
            laudo_id=laudo_id,
            texto="Sample text",
            posicion=0,
        )
        assert fragmento.laudo_id == laudo_id

    def test_has_seccion(self):
        """FragmentoLaudo can have seccion field."""
        from src.domain.cas_entities import FragmentoLaudo

        fragmento = FragmentoLaudo(
            id=str(uuid4()),
            laudo_id=str(uuid4()),
            texto="Sample text",
            posicion=0,
            seccion="FINDINGS",
        )
        assert fragmento.seccion == "FINDINGS"

    def test_immutability(self):
        """FragmentoLaudo should be immutable."""
        from src.domain.cas_entities import FragmentoLaudo

        fragmento = FragmentoLaudo(
            id=str(uuid4()),
            laudo_id=str(uuid4()),
            texto="Sample text",
            posicion=0,
        )
        with pytest.raises(Exception):
            fragmento.texto = "Modified text"


class TestEmbeddingLaudo:
    """Tests for EmbeddingLaudo entity (vector embeddings)."""

    def test_creation_with_required_fields(self):
        """Create EmbeddingLaudo with required fields."""
        from src.domain.cas_entities import EmbeddingLaudo

        embedding = EmbeddingLaudo(
            id=str(uuid4()),
            fragmento_id=str(uuid4()),
            vector=(0.1, 0.2, 0.3, 0.4),
            modelo="text-embedding-3-small",
        )
        assert embedding.vector is not None
        assert embedding.modelo == "text-embedding-3-small"

    def test_vector_is_tuple_of_floats(self):
        """EmbeddingLaudo vector is tuple of floats."""
        from src.domain.cas_entities import EmbeddingLaudo

        vector = tuple([0.1] * 1536)  # OpenAI embedding dimension
        embedding = EmbeddingLaudo(
            id=str(uuid4()),
            fragmento_id=str(uuid4()),
            vector=vector,
            modelo="text-embedding-3-small",
        )
        assert isinstance(embedding.vector, tuple)
        assert len(embedding.vector) == 1536

    def test_has_fragmento_id(self):
        """EmbeddingLaudo references parent fragmento."""
        from src.domain.cas_entities import EmbeddingLaudo

        fragmento_id = str(uuid4())
        embedding = EmbeddingLaudo(
            id=str(uuid4()),
            fragmento_id=fragmento_id,
            vector=(0.1, 0.2, 0.3),
            modelo="test-model",
        )
        assert embedding.fragmento_id == fragmento_id

    def test_immutability(self):
        """EmbeddingLaudo should be immutable."""
        from src.domain.cas_entities import EmbeddingLaudo

        embedding = EmbeddingLaudo(
            id=str(uuid4()),
            fragmento_id=str(uuid4()),
            vector=(0.1, 0.2, 0.3),
            modelo="test-model",
        )
        with pytest.raises(Exception):
            embedding.modelo = "different-model"


class TestResultadoBusquedaCAS:
    """Tests for ResultadoBusquedaCAS entity (search results)."""

    def test_creation_with_required_fields(self):
        """Create ResultadoBusquedaCAS with required fields."""
        from src.domain.cas_entities import ResultadoBusquedaCAS
        from src.domain.cas_value_objects import NumeroCaso

        resultado = ResultadoBusquedaCAS(
            numero_caso=NumeroCaso(valor="CAS 2023/A/12345"),
            titulo="Player X v. Club Y",
            relevancia=0.95,
        )
        assert resultado.relevancia == 0.95
        assert resultado.titulo == "Player X v. Club Y"

    def test_has_fragmento_relevante(self):
        """ResultadoBusquedaCAS can have fragmento_relevante."""
        from src.domain.cas_entities import ResultadoBusquedaCAS
        from src.domain.cas_value_objects import NumeroCaso

        resultado = ResultadoBusquedaCAS(
            numero_caso=NumeroCaso(valor="CAS 2023/A/12345"),
            titulo="Test Case",
            relevancia=0.85,
            fragmento_relevante="The Panel concludes that...",
        )
        assert resultado.fragmento_relevante is not None

    def test_immutability(self):
        """ResultadoBusquedaCAS should be immutable."""
        from src.domain.cas_entities import ResultadoBusquedaCAS
        from src.domain.cas_value_objects import NumeroCaso

        resultado = ResultadoBusquedaCAS(
            numero_caso=NumeroCaso(valor="CAS 2023/A/12345"),
            titulo="Test Case",
            relevancia=0.85,
        )
        with pytest.raises(Exception):
            resultado.relevancia = 0.99


class TestPuntoControlScrapingCAS:
    """Tests for PuntoControlScrapingCAS entity (checkpoints)."""

    def test_creation_with_required_fields(self):
        """Create PuntoControlScrapingCAS with required fields."""
        from src.domain.cas_entities import PuntoControlScrapingCAS

        checkpoint = PuntoControlScrapingCAS(
            id=str(uuid4()),
            pagina_actual=5,
            laudos_procesados=100,
        )
        assert checkpoint.pagina_actual == 5
        assert checkpoint.laudos_procesados == 100

    def test_has_filtros_activos(self):
        """PuntoControlScrapingCAS can have filtros_activos."""
        from src.domain.cas_entities import PuntoControlScrapingCAS

        checkpoint = PuntoControlScrapingCAS(
            id=str(uuid4()),
            pagina_actual=5,
            laudos_procesados=100,
            filtros_activos={"deporte": "football", "anio": "2023"},
        )
        assert checkpoint.filtros_activos["deporte"] == "football"

    def test_has_fecha_checkpoint(self):
        """PuntoControlScrapingCAS has fecha_checkpoint."""
        from src.domain.cas_entities import PuntoControlScrapingCAS

        checkpoint = PuntoControlScrapingCAS(
            id=str(uuid4()),
            pagina_actual=5,
            laudos_procesados=100,
        )
        assert checkpoint.fecha_checkpoint is not None
        assert isinstance(checkpoint.fecha_checkpoint, datetime)

    def test_has_laudos_pendientes_tuple(self):
        """PuntoControlScrapingCAS has laudos_pendientes as tuple."""
        from src.domain.cas_entities import PuntoControlScrapingCAS

        pending = ("CAS 2023/A/12345", "CAS 2023/A/12346")
        checkpoint = PuntoControlScrapingCAS(
            id=str(uuid4()),
            pagina_actual=5,
            laudos_procesados=100,
            laudos_pendientes=pending,
        )
        assert isinstance(checkpoint.laudos_pendientes, tuple)

    def test_immutability(self):
        """PuntoControlScrapingCAS should be immutable."""
        from src.domain.cas_entities import PuntoControlScrapingCAS

        checkpoint = PuntoControlScrapingCAS(
            id=str(uuid4()),
            pagina_actual=5,
            laudos_procesados=100,
        )
        with pytest.raises(Exception):
            checkpoint.pagina_actual = 10
