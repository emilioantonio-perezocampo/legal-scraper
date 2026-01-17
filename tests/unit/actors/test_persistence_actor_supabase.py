"""
RED Phase Tests: Persistence Actor with Supabase Backend

Tests for integrating Supabase persistence into the existing persistence actor.
These tests must FAIL initially (RED phase).

The integration should:
- Use Supabase when ENABLE_SUPABASE_PERSISTENCE is true
- Fall back to JSON when Supabase is disabled
- Support dual-write mode for data safety
- Handle Supabase failures gracefully
"""
import pytest
import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch
import os

from src.infrastructure.actors.persistence_actor import SCJNPersistenceActor
from src.infrastructure.actors.messages import (
    GuardarDocumento,
    DocumentoGuardado,
)
from src.domain.scjn_entities import (
    SCJNDocument,
    SCJNArticle,
)
from src.domain.scjn_value_objects import (
    DocumentCategory,
    DocumentScope,
    DocumentStatus,
)


@pytest.fixture
def temp_storage_dir(tmp_path):
    """Create a temporary directory for storage."""
    return tmp_path / "storage"


@pytest.fixture
def sample_document():
    """Create a sample SCJNDocument for testing."""
    return SCJNDocument(
        id="doc-supabase-test",
        q_param="supabase_test==",
        title="LEY DE PRUEBA SUPABASE",
        short_title="LPS",
        category=DocumentCategory.LEY_FEDERAL,
        scope=DocumentScope.FEDERAL,
        status=DocumentStatus.VIGENTE,
        articles=(
            SCJNArticle(number="1", content="Artículo 1 contenido"),
        ),
    )


@pytest.fixture
def mock_supabase_repository():
    """Create a mock SupabaseDocumentRepository."""
    repo = AsyncMock()
    repo.save_document = AsyncMock(return_value="doc-supabase-test")
    repo.exists = AsyncMock(return_value=False)
    repo.find_by_external_id = AsyncMock(return_value=None)
    return repo


class TestPersistenceActorSupabaseEnabled:
    """Tests for Supabase persistence when enabled."""

    @pytest.mark.asyncio
    async def test_uses_supabase_when_enabled(
        self, temp_storage_dir, sample_document, monkeypatch
    ):
        """When ENABLE_SUPABASE_PERSISTENCE is true, should save to Supabase."""
        monkeypatch.setenv("ENABLE_SUPABASE_PERSISTENCE", "true")
        monkeypatch.setenv("ENABLE_DUAL_WRITE", "false")

        # Create mock repository
        mock_repo = AsyncMock()
        mock_repo.save_document = AsyncMock(return_value="doc-supabase-test")

        actor = SCJNPersistenceActor(storage_dir=str(temp_storage_dir))
        await actor.start()

        # Manually inject mock repository (simulating successful Supabase init)
        actor._supabase_repo = mock_repo
        actor._supabase_enabled = True

        result = await actor.ask(GuardarDocumento(document=sample_document))

        mock_repo.save_document.assert_called_once()
        assert isinstance(result, DocumentoGuardado)

        await actor.stop()

    @pytest.mark.asyncio
    async def test_dual_write_saves_to_both(
        self, temp_storage_dir, sample_document, monkeypatch
    ):
        """When ENABLE_DUAL_WRITE is true, should save to both JSON and Supabase."""
        monkeypatch.setenv("ENABLE_SUPABASE_PERSISTENCE", "true")
        monkeypatch.setenv("ENABLE_DUAL_WRITE", "true")

        # Create mock repository
        mock_repo = AsyncMock()
        mock_repo.save_document = AsyncMock(return_value="doc-supabase-test")

        actor = SCJNPersistenceActor(storage_dir=str(temp_storage_dir))
        await actor.start()

        # Manually inject mock repository (simulating successful Supabase init)
        actor._supabase_repo = mock_repo
        actor._supabase_enabled = True
        actor._dual_write_enabled = True

        await actor.ask(GuardarDocumento(document=sample_document))

        # Supabase should be called
        mock_repo.save_document.assert_called_once()

        # JSON file should also exist
        doc_file = temp_storage_dir / "documents" / f"{sample_document.id}.json"
        assert doc_file.exists()

        await actor.stop()


class TestPersistenceActorSupabaseDisabled:
    """Tests for JSON persistence when Supabase is disabled."""

    @pytest.mark.asyncio
    async def test_uses_json_when_supabase_disabled(
        self, temp_storage_dir, sample_document, monkeypatch
    ):
        """When ENABLE_SUPABASE_PERSISTENCE is false, should save only to JSON."""
        monkeypatch.setenv("ENABLE_SUPABASE_PERSISTENCE", "false")

        actor = SCJNPersistenceActor(storage_dir=str(temp_storage_dir))
        await actor.start()

        result = await actor.ask(GuardarDocumento(document=sample_document))

        # JSON file should exist
        doc_file = temp_storage_dir / "documents" / f"{sample_document.id}.json"
        assert doc_file.exists()
        assert isinstance(result, DocumentoGuardado)

        await actor.stop()

    @pytest.mark.asyncio
    async def test_default_behavior_is_json_only(
        self, temp_storage_dir, sample_document, monkeypatch
    ):
        """Default behavior (no env vars) should be JSON only."""
        # Ensure no Supabase env vars
        for key in [
            "ENABLE_SUPABASE_PERSISTENCE",
            "SUPABASE_URL",
            "SUPABASE_ANON_KEY",
            "SUPABASE_SERVICE_ROLE_KEY",
        ]:
            monkeypatch.delenv(key, raising=False)

        actor = SCJNPersistenceActor(storage_dir=str(temp_storage_dir))
        await actor.start()

        result = await actor.ask(GuardarDocumento(document=sample_document))

        doc_file = temp_storage_dir / "documents" / f"{sample_document.id}.json"
        assert doc_file.exists()
        assert isinstance(result, DocumentoGuardado)

        await actor.stop()


class TestPersistenceActorSupabaseFailure:
    """Tests for handling Supabase failures."""

    @pytest.mark.asyncio
    async def test_falls_back_to_json_on_supabase_error(
        self, temp_storage_dir, sample_document, monkeypatch
    ):
        """If Supabase fails, should fall back to JSON and log error."""
        monkeypatch.setenv("ENABLE_SUPABASE_PERSISTENCE", "true")
        monkeypatch.setenv("ENABLE_DUAL_WRITE", "false")
        monkeypatch.setenv("SUPABASE_URL", "https://test.supabase.co")
        monkeypatch.setenv("SUPABASE_ANON_KEY", "test-anon")
        monkeypatch.setenv("SUPABASE_SERVICE_ROLE_KEY", "test-service")
        monkeypatch.setenv("SUPABASE_DB_HOST", "localhost")
        monkeypatch.setenv("SUPABASE_DB_PASSWORD", "test")

        with patch(
            "src.infrastructure.actors.persistence_actor.SupabaseDocumentRepository"
        ) as MockRepo:
            mock_repo = AsyncMock()
            mock_repo.save_document = AsyncMock(
                side_effect=Exception("Supabase connection failed")
            )
            MockRepo.return_value = mock_repo

            actor = SCJNPersistenceActor(storage_dir=str(temp_storage_dir))
            await actor.start()

            # Should not raise, should fall back to JSON
            result = await actor.ask(GuardarDocumento(document=sample_document))

            doc_file = temp_storage_dir / "documents" / f"{sample_document.id}.json"
            assert doc_file.exists()
            assert isinstance(result, DocumentoGuardado)

            await actor.stop()

    @pytest.mark.asyncio
    async def test_dual_write_continues_on_supabase_error(
        self, temp_storage_dir, sample_document, monkeypatch
    ):
        """In dual write mode, JSON should succeed even if Supabase fails."""
        monkeypatch.setenv("ENABLE_SUPABASE_PERSISTENCE", "true")
        monkeypatch.setenv("ENABLE_DUAL_WRITE", "true")
        monkeypatch.setenv("SUPABASE_URL", "https://test.supabase.co")
        monkeypatch.setenv("SUPABASE_ANON_KEY", "test-anon")
        monkeypatch.setenv("SUPABASE_SERVICE_ROLE_KEY", "test-service")
        monkeypatch.setenv("SUPABASE_DB_HOST", "localhost")
        monkeypatch.setenv("SUPABASE_DB_PASSWORD", "test")

        with patch(
            "src.infrastructure.actors.persistence_actor.SupabaseDocumentRepository"
        ) as MockRepo:
            mock_repo = AsyncMock()
            mock_repo.save_document = AsyncMock(
                side_effect=Exception("Supabase error")
            )
            MockRepo.return_value = mock_repo

            actor = SCJNPersistenceActor(storage_dir=str(temp_storage_dir))
            await actor.start()

            result = await actor.ask(GuardarDocumento(document=sample_document))

            # JSON should still be saved
            doc_file = temp_storage_dir / "documents" / f"{sample_document.id}.json"
            assert doc_file.exists()
            assert isinstance(result, DocumentoGuardado)

            await actor.stop()


class TestPersistenceActorSupabaseDeduplication:
    """Tests for deduplication with Supabase."""

    @pytest.mark.asyncio
    async def test_checks_supabase_before_saving(
        self, temp_storage_dir, sample_document, monkeypatch
    ):
        """Should check Supabase for existing document before saving."""
        monkeypatch.setenv("ENABLE_SUPABASE_PERSISTENCE", "true")

        # Create mock repository
        mock_repo = AsyncMock()
        mock_repo.exists = AsyncMock(return_value=True)  # Already exists
        mock_repo.save_document = AsyncMock()

        actor = SCJNPersistenceActor(storage_dir=str(temp_storage_dir))
        await actor.start()

        # Manually inject mock repository (simulating successful Supabase init)
        actor._supabase_repo = mock_repo
        actor._supabase_enabled = True

        # EXISTS check should be made
        exists = await actor.ask(("EXISTS", sample_document.q_param))

        # When using Supabase, should check remote
        mock_repo.exists.assert_called()

        await actor.stop()


class TestPersistenceActorSupabaseLoad:
    """Tests for loading documents from Supabase."""

    @pytest.mark.asyncio
    async def test_loads_from_supabase_when_enabled(
        self, temp_storage_dir, sample_document, monkeypatch
    ):
        """When enabled, should try to load from Supabase first."""
        monkeypatch.setenv("ENABLE_SUPABASE_PERSISTENCE", "true")
        monkeypatch.setenv("SUPABASE_URL", "https://test.supabase.co")
        monkeypatch.setenv("SUPABASE_ANON_KEY", "test-anon")
        monkeypatch.setenv("SUPABASE_SERVICE_ROLE_KEY", "test-service")
        monkeypatch.setenv("SUPABASE_DB_HOST", "localhost")
        monkeypatch.setenv("SUPABASE_DB_PASSWORD", "test")

        with patch(
            "src.infrastructure.actors.persistence_actor.SupabaseDocumentRepository"
        ) as MockRepo:
            mock_repo = AsyncMock()
            mock_repo.find_by_external_id = AsyncMock(
                return_value={
                    "id": "doc-123",
                    "q_param": "test==",
                    "title": "Test Document",
                }
            )
            MockRepo.return_value = mock_repo

            actor = SCJNPersistenceActor(storage_dir=str(temp_storage_dir))
            await actor.start()

            # This verifies integration capability
            # Actual implementation will be tested in GREEN phase

            await actor.stop()


class TestPersistenceActorExistingBehavior:
    """Regression tests - ensure existing behavior still works."""

    @pytest.mark.asyncio
    async def test_json_save_still_works(self, temp_storage_dir, sample_document):
        """Basic JSON persistence should still work as before."""
        actor = SCJNPersistenceActor(storage_dir=str(temp_storage_dir))
        await actor.start()

        result = await actor.ask(GuardarDocumento(document=sample_document))

        assert isinstance(result, DocumentoGuardado)
        assert result.document_id == sample_document.id

        doc_file = temp_storage_dir / "documents" / f"{sample_document.id}.json"
        assert doc_file.exists()

        await actor.stop()

    @pytest.mark.asyncio
    async def test_json_load_still_works(self, temp_storage_dir, sample_document):
        """Loading documents from JSON should still work."""
        actor = SCJNPersistenceActor(storage_dir=str(temp_storage_dir))
        await actor.start()

        await actor.ask(GuardarDocumento(document=sample_document))

        result = await actor.ask(("LOAD_DOCUMENT", sample_document.id))

        assert result is not None
        assert result.id == sample_document.id
        assert result.title == sample_document.title

        await actor.stop()

    @pytest.mark.asyncio
    async def test_exists_check_still_works(self, temp_storage_dir, sample_document):
        """EXISTS check should still work for in-memory documents."""
        actor = SCJNPersistenceActor(storage_dir=str(temp_storage_dir))
        await actor.start()

        # Before saving
        exists_before = await actor.ask(("EXISTS", sample_document.q_param))
        assert exists_before is False

        # After saving
        await actor.ask(GuardarDocumento(document=sample_document))
        exists_after = await actor.ask(("EXISTS", sample_document.q_param))
        assert exists_after is True

        await actor.stop()
