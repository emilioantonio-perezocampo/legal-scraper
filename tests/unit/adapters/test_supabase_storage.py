"""
RED Phase Tests: SupabaseStorageAdapter

Tests for file storage operations using Supabase Storage.
These tests must FAIL initially (RED phase).

The adapter should:
- Upload PDF/HTML files to Supabase Storage buckets
- Generate signed URLs for secure file access
- Support tenant-scoped storage paths
- Check file existence before upload (deduplication)
- Delete files when documents are removed
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
import hashlib

from src.infrastructure.adapters.supabase_storage import SupabaseStorageAdapter


@pytest.fixture
def mock_supabase_client():
    """Create a mock Supabase client with storage capabilities."""
    client = MagicMock()

    # Mock storage bucket operations
    storage = MagicMock()
    bucket = MagicMock()

    # Setup upload mock
    bucket.upload.return_value = {"Key": "public/scjn/doc-123.pdf"}

    # Setup download mock
    bucket.download.return_value = b"PDF content bytes"

    # Setup create_signed_url mock
    bucket.create_signed_url.return_value = {
        "signedURL": "https://storage.example.com/signed/path?token=abc123"
    }

    # Setup list mock for exists check
    bucket.list.return_value = [{"name": "doc-123.pdf"}]

    # Setup remove mock
    bucket.remove.return_value = None

    storage.from_.return_value = bucket
    client.storage = storage

    return client, storage, bucket


@pytest.fixture
def sample_pdf_content():
    """Sample PDF content for testing."""
    return b"%PDF-1.4 sample content for testing purposes"


class TestSupabaseStorageAdapterUpload:
    """Tests for file upload operations."""

    @pytest.mark.asyncio
    async def test_upload_pdf_returns_storage_path(
        self, mock_supabase_client, sample_pdf_content
    ):
        """Given PDF content, when uploaded, returns storage path."""
        client, storage, bucket = mock_supabase_client

        adapter = SupabaseStorageAdapter(client=client)
        path = await adapter.upload(
            content=sample_pdf_content,
            source_type="scjn",
            doc_id="doc-123",
        )

        assert path is not None
        assert "doc-123" in path
        assert path.endswith(".pdf")

    @pytest.mark.asyncio
    async def test_upload_uses_source_type_prefix(
        self, mock_supabase_client, sample_pdf_content
    ):
        """Storage path should include source type prefix."""
        client, storage, bucket = mock_supabase_client

        adapter = SupabaseStorageAdapter(client=client)
        path = await adapter.upload(
            content=sample_pdf_content,
            source_type="bjv",
            doc_id="doc-456",
        )

        assert "bjv" in path

    @pytest.mark.asyncio
    async def test_upload_with_tenant_scopes_path(
        self, mock_supabase_client, sample_pdf_content
    ):
        """Given tenant_id, path should include tenant prefix."""
        client, storage, bucket = mock_supabase_client

        adapter = SupabaseStorageAdapter(client=client)
        path = await adapter.upload(
            content=sample_pdf_content,
            source_type="scjn",
            doc_id="doc-123",
            tenant_id="tenant-abc",
        )

        assert "tenant-abc" in path

    @pytest.mark.asyncio
    async def test_upload_sets_correct_content_type_for_pdf(
        self, mock_supabase_client, sample_pdf_content
    ):
        """PDF uploads should set content-type to application/pdf."""
        client, storage, bucket = mock_supabase_client

        adapter = SupabaseStorageAdapter(client=client)
        await adapter.upload(
            content=sample_pdf_content,
            source_type="scjn",
            doc_id="doc-123",
        )

        # Verify upload was called with correct content type
        bucket.upload.assert_called_once()
        call_kwargs = bucket.upload.call_args
        # Content type should be passed as an option
        assert "application/pdf" in str(call_kwargs)

    @pytest.mark.asyncio
    async def test_upload_uses_bucket_from_settings(
        self, mock_supabase_client, sample_pdf_content
    ):
        """Adapter should use the configured bucket name."""
        client, storage, bucket = mock_supabase_client

        adapter = SupabaseStorageAdapter(
            client=client, bucket_name="custom-bucket"
        )
        await adapter.upload(
            content=sample_pdf_content,
            source_type="scjn",
            doc_id="doc-123",
        )

        storage.from_.assert_called_with("custom-bucket")

    @pytest.mark.asyncio
    async def test_upload_creates_hash_prefix_path(
        self, mock_supabase_client, sample_pdf_content
    ):
        """Path should use hash prefix for even distribution."""
        client, storage, bucket = mock_supabase_client

        adapter = SupabaseStorageAdapter(client=client)
        path = await adapter.upload(
            content=sample_pdf_content,
            source_type="scjn",
            doc_id="doc-123",
        )

        # Should contain a hash prefix (first 2 chars of md5)
        parts = path.split("/")
        assert len(parts) >= 3  # At least: source/hash/filename


class TestSupabaseStorageAdapterDownload:
    """Tests for file download operations."""

    @pytest.mark.asyncio
    async def test_download_returns_file_content(self, mock_supabase_client):
        """Given storage path, returns file bytes."""
        client, storage, bucket = mock_supabase_client
        bucket.download.return_value = b"PDF content"

        adapter = SupabaseStorageAdapter(client=client)
        content = await adapter.download("public/scjn/ab/doc-123.pdf")

        assert content == b"PDF content"

    @pytest.mark.asyncio
    async def test_download_calls_bucket_download(self, mock_supabase_client):
        """Download should call the bucket's download method."""
        client, storage, bucket = mock_supabase_client

        adapter = SupabaseStorageAdapter(client=client)
        await adapter.download("public/scjn/ab/doc-123.pdf")

        bucket.download.assert_called_once_with("public/scjn/ab/doc-123.pdf")


class TestSupabaseStorageAdapterSignedUrl:
    """Tests for signed URL generation."""

    @pytest.mark.asyncio
    async def test_get_signed_url_returns_https_url(self, mock_supabase_client):
        """Signed URL should be HTTPS."""
        client, storage, bucket = mock_supabase_client

        adapter = SupabaseStorageAdapter(client=client)
        url = await adapter.get_signed_url("public/scjn/ab/doc-123.pdf")

        assert url.startswith("https://")

    @pytest.mark.asyncio
    async def test_get_signed_url_includes_token(self, mock_supabase_client):
        """Signed URL should include authentication token."""
        client, storage, bucket = mock_supabase_client

        adapter = SupabaseStorageAdapter(client=client)
        url = await adapter.get_signed_url("public/scjn/ab/doc-123.pdf")

        assert "token" in url or "signedURL" in str(bucket.create_signed_url.call_args)

    @pytest.mark.asyncio
    async def test_get_signed_url_respects_expiry(self, mock_supabase_client):
        """Signed URL should use specified expiry time."""
        client, storage, bucket = mock_supabase_client

        adapter = SupabaseStorageAdapter(client=client)
        await adapter.get_signed_url("path/to/file.pdf", expires_in=7200)

        # Verify expiry was passed to create_signed_url
        call_args = bucket.create_signed_url.call_args
        assert 7200 in call_args[0] or 7200 in str(call_args)

    @pytest.mark.asyncio
    async def test_get_signed_url_default_expiry_is_3600(self, mock_supabase_client):
        """Default expiry should be 3600 seconds (1 hour)."""
        client, storage, bucket = mock_supabase_client

        adapter = SupabaseStorageAdapter(client=client)
        await adapter.get_signed_url("path/to/file.pdf")

        call_args = bucket.create_signed_url.call_args
        assert 3600 in call_args[0] or 3600 in str(call_args)


class TestSupabaseStorageAdapterExists:
    """Tests for file existence checking."""

    @pytest.mark.asyncio
    async def test_exists_returns_true_when_file_present(self, mock_supabase_client):
        """exists() returns True when file exists in bucket."""
        client, storage, bucket = mock_supabase_client
        bucket.list.return_value = [{"name": "doc-123.pdf"}]

        adapter = SupabaseStorageAdapter(client=client)
        exists = await adapter.exists("public/scjn/ab/doc-123.pdf")

        assert exists is True

    @pytest.mark.asyncio
    async def test_exists_returns_false_when_file_missing(self, mock_supabase_client):
        """exists() returns False when file not in bucket."""
        client, storage, bucket = mock_supabase_client
        bucket.list.return_value = []

        adapter = SupabaseStorageAdapter(client=client)
        exists = await adapter.exists("public/scjn/ab/nonexistent.pdf")

        assert exists is False


class TestSupabaseStorageAdapterDelete:
    """Tests for file deletion."""

    @pytest.mark.asyncio
    async def test_delete_removes_file(self, mock_supabase_client):
        """delete() should call bucket's remove method."""
        client, storage, bucket = mock_supabase_client

        adapter = SupabaseStorageAdapter(client=client)
        await adapter.delete("public/scjn/ab/doc-123.pdf")

        bucket.remove.assert_called_once()

    @pytest.mark.asyncio
    async def test_delete_passes_correct_path(self, mock_supabase_client):
        """delete() should pass the exact path to remove."""
        client, storage, bucket = mock_supabase_client

        adapter = SupabaseStorageAdapter(client=client)
        await adapter.delete("public/scjn/ab/doc-123.pdf")

        call_args = bucket.remove.call_args
        assert "public/scjn/ab/doc-123.pdf" in str(call_args)


class TestSupabaseStorageAdapterConfiguration:
    """Tests for adapter configuration."""

    def test_adapter_requires_client(self):
        """Adapter should require a Supabase client."""
        with pytest.raises(TypeError):
            SupabaseStorageAdapter()

    def test_adapter_uses_default_bucket(self, mock_supabase_client):
        """Default bucket name should be 'legal-scraper-pdfs'."""
        client, storage, bucket = mock_supabase_client

        adapter = SupabaseStorageAdapter(client=client)
        assert adapter._bucket_name == "legal-scraper-pdfs"

    def test_adapter_accepts_custom_bucket(self, mock_supabase_client):
        """Adapter should accept custom bucket name."""
        client, storage, bucket = mock_supabase_client

        adapter = SupabaseStorageAdapter(
            client=client, bucket_name="my-custom-bucket"
        )
        assert adapter._bucket_name == "my-custom-bucket"


class TestSupabaseStorageAdapterPathGeneration:
    """Tests for storage path generation."""

    def test_generate_path_without_tenant(self, mock_supabase_client):
        """Path without tenant: {source_type}/{hash}/{doc_id}.pdf"""
        client, storage, bucket = mock_supabase_client

        adapter = SupabaseStorageAdapter(client=client)
        path = adapter._generate_path(
            source_type="scjn",
            doc_id="doc-123",
            extension=".pdf",
        )

        assert path.startswith("scjn/")
        assert "doc-123.pdf" in path

    def test_generate_path_with_tenant(self, mock_supabase_client):
        """Path with tenant: {tenant_id}/{source_type}/{hash}/{doc_id}.pdf"""
        client, storage, bucket = mock_supabase_client

        adapter = SupabaseStorageAdapter(client=client)
        path = adapter._generate_path(
            source_type="bjv",
            doc_id="doc-456",
            extension=".pdf",
            tenant_id="tenant-xyz",
        )

        assert path.startswith("tenant-xyz/")
        assert "bjv/" in path
        assert "doc-456.pdf" in path

    def test_generate_path_uses_hash_prefix(self, mock_supabase_client):
        """Path should include 2-char hash prefix for distribution."""
        client, storage, bucket = mock_supabase_client

        adapter = SupabaseStorageAdapter(client=client)
        path = adapter._generate_path(
            source_type="scjn",
            doc_id="doc-123",
            extension=".pdf",
        )

        # Hash prefix should be 2 hex chars
        parts = path.split("/")
        hash_prefix = parts[1]  # After source_type
        assert len(hash_prefix) == 2
        assert all(c in "0123456789abcdef" for c in hash_prefix)
