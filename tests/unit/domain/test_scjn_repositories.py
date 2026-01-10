"""
RED Phase Tests: SCJN Repository Interfaces (Ports)

Tests for SCJN repository port definitions following DDD/Hexagonal Architecture.
These are interface tests - they verify the contracts exist and are correct.
These tests must FAIL initially (RED phase).
"""
import pytest
from abc import ABC
from typing import Optional, List
import inspect

# These imports will fail until implementation exists
from src.domain.scjn_repositories import (
    ISCJNDocumentRepository,
    IEmbeddingRepository,
)
from src.domain.scjn_entities import SCJNDocument, DocumentEmbedding


class TestISCJNDocumentRepository:
    """Tests for ISCJNDocumentRepository port interface."""

    def test_interface_is_abstract(self):
        """Repository interface should be abstract."""
        assert issubclass(ISCJNDocumentRepository, ABC)

    def test_interface_cannot_be_instantiated(self):
        """Abstract interface should not be directly instantiable."""
        with pytest.raises(TypeError):
            ISCJNDocumentRepository()

    def test_has_save_method(self):
        """Interface should define save method."""
        assert hasattr(ISCJNDocumentRepository, 'save')
        method = getattr(ISCJNDocumentRepository, 'save')
        assert callable(method)

    def test_save_method_is_async(self):
        """save method should be a coroutine function."""
        method = getattr(ISCJNDocumentRepository, 'save')
        assert inspect.iscoroutinefunction(method)

    def test_has_find_by_id_method(self):
        """Interface should define find_by_id method."""
        assert hasattr(ISCJNDocumentRepository, 'find_by_id')
        method = getattr(ISCJNDocumentRepository, 'find_by_id')
        assert callable(method)

    def test_find_by_id_is_async(self):
        """find_by_id method should be a coroutine function."""
        method = getattr(ISCJNDocumentRepository, 'find_by_id')
        assert inspect.iscoroutinefunction(method)

    def test_has_find_by_q_param_method(self):
        """Interface should define find_by_q_param method."""
        assert hasattr(ISCJNDocumentRepository, 'find_by_q_param')
        method = getattr(ISCJNDocumentRepository, 'find_by_q_param')
        assert callable(method)

    def test_find_by_q_param_is_async(self):
        """find_by_q_param method should be a coroutine function."""
        method = getattr(ISCJNDocumentRepository, 'find_by_q_param')
        assert inspect.iscoroutinefunction(method)

    def test_has_exists_method(self):
        """Interface should define exists method."""
        assert hasattr(ISCJNDocumentRepository, 'exists')
        method = getattr(ISCJNDocumentRepository, 'exists')
        assert callable(method)

    def test_exists_is_async(self):
        """exists method should be a coroutine function."""
        method = getattr(ISCJNDocumentRepository, 'exists')
        assert inspect.iscoroutinefunction(method)


class TestIEmbeddingRepository:
    """Tests for IEmbeddingRepository port interface."""

    def test_interface_is_abstract(self):
        """Repository interface should be abstract."""
        assert issubclass(IEmbeddingRepository, ABC)

    def test_interface_cannot_be_instantiated(self):
        """Abstract interface should not be directly instantiable."""
        with pytest.raises(TypeError):
            IEmbeddingRepository()

    def test_has_save_method(self):
        """Interface should define save method."""
        assert hasattr(IEmbeddingRepository, 'save')
        method = getattr(IEmbeddingRepository, 'save')
        assert callable(method)

    def test_save_is_async(self):
        """save method should be a coroutine function."""
        method = getattr(IEmbeddingRepository, 'save')
        assert inspect.iscoroutinefunction(method)

    def test_has_save_batch_method(self):
        """Interface should define save_batch method."""
        assert hasattr(IEmbeddingRepository, 'save_batch')
        method = getattr(IEmbeddingRepository, 'save_batch')
        assert callable(method)

    def test_save_batch_is_async(self):
        """save_batch method should be a coroutine function."""
        method = getattr(IEmbeddingRepository, 'save_batch')
        assert inspect.iscoroutinefunction(method)

    def test_has_find_similar_method(self):
        """Interface should define find_similar method."""
        assert hasattr(IEmbeddingRepository, 'find_similar')
        method = getattr(IEmbeddingRepository, 'find_similar')
        assert callable(method)

    def test_find_similar_is_async(self):
        """find_similar method should be a coroutine function."""
        method = getattr(IEmbeddingRepository, 'find_similar')
        assert inspect.iscoroutinefunction(method)


class TestRepositoryImplementationContract:
    """Tests to verify concrete implementations follow the contract."""

    def test_document_repository_methods_have_correct_signatures(self):
        """Verify method signatures match expected patterns."""
        # Get all abstract methods
        abstract_methods = []
        for name, method in inspect.getmembers(ISCJNDocumentRepository):
            if getattr(method, '__isabstractmethod__', False):
                abstract_methods.append(name)

        expected_methods = ['save', 'find_by_id', 'find_by_q_param', 'exists']
        for method_name in expected_methods:
            assert method_name in abstract_methods, f"Missing abstract method: {method_name}"

    def test_embedding_repository_methods_have_correct_signatures(self):
        """Verify method signatures match expected patterns."""
        # Get all abstract methods
        abstract_methods = []
        for name, method in inspect.getmembers(IEmbeddingRepository):
            if getattr(method, '__isabstractmethod__', False):
                abstract_methods.append(name)

        expected_methods = ['save', 'save_batch', 'find_similar']
        for method_name in expected_methods:
            assert method_name in abstract_methods, f"Missing abstract method: {method_name}"
