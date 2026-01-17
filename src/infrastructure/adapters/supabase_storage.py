"""
Supabase Storage Adapter for PDF/HTML file storage.

Handles file uploads, downloads, and signed URL generation
for scraped legal documents using Supabase Storage.

Features:
- Organized storage paths: {tenant}/{source_type}/{hash_prefix}/{doc_id}.pdf
- Hash-based path prefixes for even file distribution
- Signed URLs for secure temporary access
- Deduplication via existence checks

Usage:
    from supabase import create_client
    client = create_client(url, service_role_key)
    adapter = SupabaseStorageAdapter(client=client)

    # Upload a PDF
    path = await adapter.upload(content, "scjn", "doc-123")

    # Get signed URL for download
    url = await adapter.get_signed_url(path, expires_in=3600)
"""
from __future__ import annotations

import hashlib
from typing import Any, Optional


class SupabaseStorageAdapter:
    """
    Storage adapter for Supabase Storage buckets.

    Manages file uploads and downloads for scraped legal documents,
    organizing files by tenant, source type, and hash prefix.

    Attributes:
        DEFAULT_BUCKET: Default bucket name for legal documents
    """

    DEFAULT_BUCKET = "legal-scraper-pdfs"

    def __init__(
        self,
        client: Any,
        bucket_name: str = DEFAULT_BUCKET,
    ) -> None:
        """
        Initialize the storage adapter.

        Args:
            client: Supabase client from create_client()
            bucket_name: Name of the storage bucket to use
        """
        self._client = client
        self._bucket_name = bucket_name

    def _get_bucket(self):
        """Get the storage bucket object."""
        return self._client.storage.from_(self._bucket_name)

    def _generate_hash_prefix(self, doc_id: str) -> str:
        """
        Generate a 2-character hash prefix for even file distribution.

        Uses MD5 hash of doc_id to create a predictable prefix that
        distributes files evenly across 256 subdirectories.

        Args:
            doc_id: Document identifier

        Returns:
            2-character hex string (00-ff)
        """
        hash_bytes = hashlib.md5(doc_id.encode()).hexdigest()
        return hash_bytes[:2]

    def _generate_path(
        self,
        source_type: str,
        doc_id: str,
        extension: str = ".pdf",
        tenant_id: Optional[str] = None,
    ) -> str:
        """
        Generate a storage path for a file.

        Path structure:
        - Without tenant: {source_type}/{hash_prefix}/{doc_id}.pdf
        - With tenant: {tenant_id}/{source_type}/{hash_prefix}/{doc_id}.pdf

        Args:
            source_type: Document source (scjn, bjv, cas, dof)
            doc_id: Document identifier
            extension: File extension (default: .pdf)
            tenant_id: Optional tenant identifier

        Returns:
            Full storage path string
        """
        hash_prefix = self._generate_hash_prefix(doc_id)
        filename = f"{doc_id}{extension}"

        if tenant_id:
            return f"{tenant_id}/{source_type}/{hash_prefix}/{filename}"
        else:
            return f"{source_type}/{hash_prefix}/{filename}"

    async def upload(
        self,
        content: bytes,
        source_type: str,
        doc_id: str,
        content_type: str = "application/pdf",
        tenant_id: Optional[str] = None,
    ) -> str:
        """
        Upload a file to Supabase Storage.

        Args:
            content: File content as bytes
            source_type: Document source (scjn, bjv, cas, dof)
            doc_id: Document identifier
            content_type: MIME type (default: application/pdf)
            tenant_id: Optional tenant identifier for scoped storage

        Returns:
            Storage path of the uploaded file
        """
        extension = ".pdf" if content_type == "application/pdf" else ".html"
        path = self._generate_path(
            source_type=source_type,
            doc_id=doc_id,
            extension=extension,
            tenant_id=tenant_id,
        )

        bucket = self._get_bucket()
        bucket.upload(
            path,
            content,
            {"content-type": content_type},
        )

        return path

    async def download(self, path: str) -> bytes:
        """
        Download a file from Supabase Storage.

        Args:
            path: Storage path of the file

        Returns:
            File content as bytes
        """
        bucket = self._get_bucket()
        return bucket.download(path)

    async def get_signed_url(
        self,
        path: str,
        expires_in: int = 3600,
    ) -> str:
        """
        Generate a signed URL for secure file access.

        Args:
            path: Storage path of the file
            expires_in: URL validity in seconds (default: 3600 = 1 hour)

        Returns:
            HTTPS signed URL with authentication token
        """
        bucket = self._get_bucket()
        result = bucket.create_signed_url(path, expires_in)
        return result.get("signedURL", result.get("signedUrl", ""))

    async def exists(self, path: str) -> bool:
        """
        Check if a file exists in storage.

        Args:
            path: Storage path to check

        Returns:
            True if file exists, False otherwise
        """
        bucket = self._get_bucket()

        # Extract directory and filename from path
        parts = path.rsplit("/", 1)
        if len(parts) == 2:
            directory, filename = parts
        else:
            directory = ""
            filename = parts[0]

        # List files in directory and check for match
        files = bucket.list(directory)
        return any(f.get("name") == filename for f in files)

    async def delete(self, path: str) -> None:
        """
        Delete a file from storage.

        Args:
            path: Storage path of the file to delete
        """
        bucket = self._get_bucket()
        bucket.remove([path])
