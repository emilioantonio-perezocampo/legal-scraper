"""
Tests for BJV Repository Ports (Interfaces)

RED phase: All tests should fail until implementation is complete.
Repository ports define the contract for persistence/retrieval.
These are abstract interfaces - tests verify the protocol definition.
"""
import pytest
from abc import ABC
from typing import Protocol, runtime_checkable


class TestLibroRepositorio:
    """Tests for LibroRepositorio port - book persistence interface."""

    def test_libro_repositorio_is_protocol(self):
        """LibroRepositorio should be a Protocol."""
        from src.domain.bjv_repository_ports import LibroRepositorio
        assert hasattr(LibroRepositorio, '__protocol_attrs__') or issubclass(LibroRepositorio, Protocol)

    def test_libro_repositorio_has_guardar_method(self):
        """LibroRepositorio should define guardar method."""
        from src.domain.bjv_repository_ports import LibroRepositorio
        assert hasattr(LibroRepositorio, 'guardar')

    def test_libro_repositorio_has_obtener_por_id_method(self):
        """LibroRepositorio should define obtener_por_id method."""
        from src.domain.bjv_repository_ports import LibroRepositorio
        assert hasattr(LibroRepositorio, 'obtener_por_id')

    def test_libro_repositorio_has_buscar_method(self):
        """LibroRepositorio should define buscar method."""
        from src.domain.bjv_repository_ports import LibroRepositorio
        assert hasattr(LibroRepositorio, 'buscar')

    def test_libro_repositorio_has_listar_todos_method(self):
        """LibroRepositorio should define listar_todos method."""
        from src.domain.bjv_repository_ports import LibroRepositorio
        assert hasattr(LibroRepositorio, 'listar_todos')

    def test_libro_repositorio_has_existe_method(self):
        """LibroRepositorio should define existe method."""
        from src.domain.bjv_repository_ports import LibroRepositorio
        assert hasattr(LibroRepositorio, 'existe')


class TestFragmentoRepositorio:
    """Tests for FragmentoRepositorio port - text fragment persistence interface."""

    def test_fragmento_repositorio_is_protocol(self):
        """FragmentoRepositorio should be a Protocol."""
        from src.domain.bjv_repository_ports import FragmentoRepositorio
        assert hasattr(FragmentoRepositorio, '__protocol_attrs__') or issubclass(FragmentoRepositorio, Protocol)

    def test_fragmento_repositorio_has_guardar_method(self):
        """FragmentoRepositorio should define guardar method."""
        from src.domain.bjv_repository_ports import FragmentoRepositorio
        assert hasattr(FragmentoRepositorio, 'guardar')

    def test_fragmento_repositorio_has_obtener_por_libro_method(self):
        """FragmentoRepositorio should define obtener_por_libro method."""
        from src.domain.bjv_repository_ports import FragmentoRepositorio
        assert hasattr(FragmentoRepositorio, 'obtener_por_libro')

    def test_fragmento_repositorio_has_buscar_similares_method(self):
        """FragmentoRepositorio should define buscar_similares for vector search."""
        from src.domain.bjv_repository_ports import FragmentoRepositorio
        assert hasattr(FragmentoRepositorio, 'buscar_similares')


class TestCheckpointRepositorio:
    """Tests for CheckpointRepositorio port - scraping checkpoint persistence."""

    def test_checkpoint_repositorio_is_protocol(self):
        """CheckpointRepositorio should be a Protocol."""
        from src.domain.bjv_repository_ports import CheckpointRepositorio
        assert hasattr(CheckpointRepositorio, '__protocol_attrs__') or issubclass(CheckpointRepositorio, Protocol)

    def test_checkpoint_repositorio_has_guardar_method(self):
        """CheckpointRepositorio should define guardar method."""
        from src.domain.bjv_repository_ports import CheckpointRepositorio
        assert hasattr(CheckpointRepositorio, 'guardar')

    def test_checkpoint_repositorio_has_obtener_ultimo_method(self):
        """CheckpointRepositorio should define obtener_ultimo method."""
        from src.domain.bjv_repository_ports import CheckpointRepositorio
        assert hasattr(CheckpointRepositorio, 'obtener_ultimo')

    def test_checkpoint_repositorio_has_eliminar_method(self):
        """CheckpointRepositorio should define eliminar method."""
        from src.domain.bjv_repository_ports import CheckpointRepositorio
        assert hasattr(CheckpointRepositorio, 'eliminar')
