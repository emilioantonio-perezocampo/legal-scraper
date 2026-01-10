"""
Tests for BJV Domain Value Objects

RED phase: All tests should fail until implementation is complete.
Tests follow DDD patterns with Spanish domain terminology.
"""
import pytest
from dataclasses import FrozenInstanceError


class TestTipoContenido:
    """Tests for TipoContenido enum - content types in BJV."""

    def test_tipo_contenido_has_libro_value(self):
        """TipoContenido should have LIBRO member."""
        from src.domain.bjv_value_objects import TipoContenido
        assert TipoContenido.LIBRO.value == "libro"

    def test_tipo_contenido_has_capitulo_value(self):
        """TipoContenido should have CAPITULO member."""
        from src.domain.bjv_value_objects import TipoContenido
        assert TipoContenido.CAPITULO.value == "capitulo"

    def test_tipo_contenido_has_articulo_value(self):
        """TipoContenido should have ARTICULO member."""
        from src.domain.bjv_value_objects import TipoContenido
        assert TipoContenido.ARTICULO.value == "articulo"

    def test_tipo_contenido_has_revista_value(self):
        """TipoContenido should have REVISTA member."""
        from src.domain.bjv_value_objects import TipoContenido
        assert TipoContenido.REVISTA.value == "revista"


class TestAreaDerecho:
    """Tests for AreaDerecho enum - legal practice areas."""

    def test_area_derecho_has_civil(self):
        """AreaDerecho should have CIVIL member."""
        from src.domain.bjv_value_objects import AreaDerecho
        assert AreaDerecho.CIVIL.value == "civil"

    def test_area_derecho_has_penal(self):
        """AreaDerecho should have PENAL member."""
        from src.domain.bjv_value_objects import AreaDerecho
        assert AreaDerecho.PENAL.value == "penal"

    def test_area_derecho_has_constitucional(self):
        """AreaDerecho should have CONSTITUCIONAL member."""
        from src.domain.bjv_value_objects import AreaDerecho
        assert AreaDerecho.CONSTITUCIONAL.value == "constitucional"

    def test_area_derecho_has_administrativo(self):
        """AreaDerecho should have ADMINISTRATIVO member."""
        from src.domain.bjv_value_objects import AreaDerecho
        assert AreaDerecho.ADMINISTRATIVO.value == "administrativo"

    def test_area_derecho_has_mercantil(self):
        """AreaDerecho should have MERCANTIL member."""
        from src.domain.bjv_value_objects import AreaDerecho
        assert AreaDerecho.MERCANTIL.value == "mercantil"

    def test_area_derecho_has_laboral(self):
        """AreaDerecho should have LABORAL member."""
        from src.domain.bjv_value_objects import AreaDerecho
        assert AreaDerecho.LABORAL.value == "laboral"

    def test_area_derecho_has_fiscal(self):
        """AreaDerecho should have FISCAL member."""
        from src.domain.bjv_value_objects import AreaDerecho
        assert AreaDerecho.FISCAL.value == "fiscal"

    def test_area_derecho_has_internacional(self):
        """AreaDerecho should have INTERNACIONAL member."""
        from src.domain.bjv_value_objects import AreaDerecho
        assert AreaDerecho.INTERNACIONAL.value == "internacional"

    def test_area_derecho_has_general(self):
        """AreaDerecho should have GENERAL member for uncategorized content."""
        from src.domain.bjv_value_objects import AreaDerecho
        assert AreaDerecho.GENERAL.value == "general"


class TestISBN:
    """Tests for ISBN value object - book identifier."""

    def test_isbn_creation_with_valid_isbn13(self):
        """ISBN should accept valid ISBN-13."""
        from src.domain.bjv_value_objects import ISBN
        isbn = ISBN(valor="978-607-02-1234-5")
        assert isbn.valor == "978-607-02-1234-5"

    def test_isbn_creation_with_valid_isbn10(self):
        """ISBN should accept valid ISBN-10."""
        from src.domain.bjv_value_objects import ISBN
        isbn = ISBN(valor="607-02-1234-5")
        assert isbn.valor == "607-02-1234-5"

    def test_isbn_is_immutable(self):
        """ISBN should be frozen dataclass."""
        from src.domain.bjv_value_objects import ISBN
        isbn = ISBN(valor="978-607-02-1234-5")
        with pytest.raises(FrozenInstanceError):
            isbn.valor = "new-value"

    def test_isbn_equality_by_value(self):
        """Two ISBNs with same value should be equal."""
        from src.domain.bjv_value_objects import ISBN
        isbn1 = ISBN(valor="978-607-02-1234-5")
        isbn2 = ISBN(valor="978-607-02-1234-5")
        assert isbn1 == isbn2

    def test_isbn_normalized_property(self):
        """ISBN should provide normalized version without dashes."""
        from src.domain.bjv_value_objects import ISBN
        isbn = ISBN(valor="978-607-02-1234-5")
        assert isbn.normalizado == "9786070212345"


class TestAnioPublicacion:
    """Tests for AnioPublicacion value object - publication year."""

    def test_anio_publicacion_creation(self):
        """AnioPublicacion should store year value."""
        from src.domain.bjv_value_objects import AnioPublicacion
        anio = AnioPublicacion(valor=2023)
        assert anio.valor == 2023

    def test_anio_publicacion_is_immutable(self):
        """AnioPublicacion should be frozen dataclass."""
        from src.domain.bjv_value_objects import AnioPublicacion
        anio = AnioPublicacion(valor=2023)
        with pytest.raises(FrozenInstanceError):
            anio.valor = 2024

    def test_anio_publicacion_equality(self):
        """Two AnioPublicacion with same year should be equal."""
        from src.domain.bjv_value_objects import AnioPublicacion
        anio1 = AnioPublicacion(valor=2023)
        anio2 = AnioPublicacion(valor=2023)
        assert anio1 == anio2

    def test_anio_publicacion_es_reciente_true(self):
        """AnioPublicacion should identify recent publications (last 5 years)."""
        from src.domain.bjv_value_objects import AnioPublicacion
        anio = AnioPublicacion(valor=2023)
        assert anio.es_reciente is True

    def test_anio_publicacion_es_reciente_false(self):
        """AnioPublicacion should identify old publications."""
        from src.domain.bjv_value_objects import AnioPublicacion
        anio = AnioPublicacion(valor=1990)
        assert anio.es_reciente is False


class TestURLArchivo:
    """Tests for URLArchivo value object - file URL."""

    def test_url_archivo_creation(self):
        """URLArchivo should store URL and format."""
        from src.domain.bjv_value_objects import URLArchivo
        url = URLArchivo(
            url="https://biblio.juridicas.unam.mx/bjv/libro/123.pdf",
            formato="pdf"
        )
        assert url.url == "https://biblio.juridicas.unam.mx/bjv/libro/123.pdf"
        assert url.formato == "pdf"

    def test_url_archivo_is_immutable(self):
        """URLArchivo should be frozen dataclass."""
        from src.domain.bjv_value_objects import URLArchivo
        url = URLArchivo(url="https://example.com/file.pdf", formato="pdf")
        with pytest.raises(FrozenInstanceError):
            url.url = "new-url"

    def test_url_archivo_es_pdf_true(self):
        """URLArchivo should identify PDF files."""
        from src.domain.bjv_value_objects import URLArchivo
        url = URLArchivo(url="https://example.com/file.pdf", formato="pdf")
        assert url.es_pdf is True

    def test_url_archivo_es_pdf_false(self):
        """URLArchivo should identify non-PDF files."""
        from src.domain.bjv_value_objects import URLArchivo
        url = URLArchivo(url="https://example.com/file.epub", formato="epub")
        assert url.es_pdf is False

    def test_url_archivo_nombre_archivo(self):
        """URLArchivo should extract filename from URL."""
        from src.domain.bjv_value_objects import URLArchivo
        url = URLArchivo(
            url="https://biblio.juridicas.unam.mx/bjv/libro/documento.pdf",
            formato="pdf"
        )
        assert url.nombre_archivo == "documento.pdf"


class TestIdentificadorLibro:
    """Tests for IdentificadorLibro value object - book identifier in BJV system."""

    def test_identificador_libro_creation(self):
        """IdentificadorLibro should store BJV ID."""
        from src.domain.bjv_value_objects import IdentificadorLibro
        id_libro = IdentificadorLibro(bjv_id="12345")
        assert id_libro.bjv_id == "12345"

    def test_identificador_libro_is_immutable(self):
        """IdentificadorLibro should be frozen dataclass."""
        from src.domain.bjv_value_objects import IdentificadorLibro
        id_libro = IdentificadorLibro(bjv_id="12345")
        with pytest.raises(FrozenInstanceError):
            id_libro.bjv_id = "99999"

    def test_identificador_libro_equality(self):
        """Two IdentificadorLibro with same bjv_id should be equal."""
        from src.domain.bjv_value_objects import IdentificadorLibro
        id1 = IdentificadorLibro(bjv_id="12345")
        id2 = IdentificadorLibro(bjv_id="12345")
        assert id1 == id2

    def test_identificador_libro_url_property(self):
        """IdentificadorLibro should generate BJV URL."""
        from src.domain.bjv_value_objects import IdentificadorLibro
        id_libro = IdentificadorLibro(bjv_id="12345")
        assert id_libro.url == "https://biblio.juridicas.unam.mx/bjv/detalle/12345"

    def test_identificador_libro_hash_for_dict_key(self):
        """IdentificadorLibro should be hashable for use as dict key."""
        from src.domain.bjv_value_objects import IdentificadorLibro
        id_libro = IdentificadorLibro(bjv_id="12345")
        d = {id_libro: "test"}
        assert d[id_libro] == "test"
