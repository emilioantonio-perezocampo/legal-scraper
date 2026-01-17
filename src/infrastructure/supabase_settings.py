"""
Supabase Integration Settings

Configuration settings for connecting the legal-scraper to Supabase.
Supports both cloud-hosted and self-hosted Supabase instances.

Usage:
    settings = SupabaseSettings.from_env()
    supabase = create_client(settings.url, settings.service_role_key)
"""
from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class SupabaseSettings:
    """
    Configuration settings for Supabase integration.

    Attributes:
        url: Supabase project URL (e.g., https://project.supabase.co)
        anon_key: Public anonymous key for client-side operations
        service_role_key: Service role key for server-side operations (bypasses RLS)
        db_host: PostgreSQL host for direct database connections
        db_port: PostgreSQL port (default: 5432)
        db_password: PostgreSQL password for the postgres user
        db_name: Database name (default: postgres)
        persistence_enabled: Whether to persist documents to Supabase
        dual_write_enabled: Whether to write to both JSON and Supabase
        realtime_enabled: Whether to enable Realtime subscriptions
        pdf_bucket: Storage bucket name for PDF files
        allow_insecure: Allow HTTP URLs (for local development)
    """

    url: str
    anon_key: str
    service_role_key: str
    db_host: str
    db_port: int
    db_password: str
    db_name: str
    persistence_enabled: bool
    dual_write_enabled: bool
    realtime_enabled: bool
    pdf_bucket: str
    allow_insecure: bool

    @classmethod
    def from_env(cls) -> SupabaseSettings:
        """
        Load settings from environment variables.

        Required environment variables:
            - SUPABASE_URL
            - SUPABASE_ANON_KEY
            - SUPABASE_SERVICE_ROLE_KEY
            - SUPABASE_DB_PASSWORD

        Optional environment variables:
            - SUPABASE_DB_HOST (default: localhost)
            - SUPABASE_DB_PORT (default: 5432)
            - SUPABASE_DB_NAME (default: postgres)
            - ENABLE_SUPABASE_PERSISTENCE (default: false)
            - ENABLE_DUAL_WRITE (default: true)
            - ENABLE_REALTIME_UPDATES (default: false)
            - SCRAPER_PDF_BUCKET (default: legal-scraper-pdfs)
            - SUPABASE_ALLOW_INSECURE (default: false)

        Returns:
            SupabaseSettings instance with validated configuration

        Raises:
            ValueError: If required environment variables are missing or invalid
        """
        # Required variables
        url = os.environ.get("SUPABASE_URL")
        if not url:
            raise ValueError("Missing required environment variable: SUPABASE_URL")

        anon_key = os.environ.get("SUPABASE_ANON_KEY")
        if not anon_key:
            raise ValueError("Missing required environment variable: SUPABASE_ANON_KEY")

        service_role_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
        if not service_role_key:
            raise ValueError(
                "Missing required environment variable: SUPABASE_SERVICE_ROLE_KEY"
            )

        db_password = os.environ.get("SUPABASE_DB_PASSWORD")
        if not db_password:
            raise ValueError(
                "Missing required environment variable: SUPABASE_DB_PASSWORD"
            )

        # Optional variables with defaults
        db_host = os.environ.get("SUPABASE_DB_HOST", "localhost")
        db_port = int(os.environ.get("SUPABASE_DB_PORT", "5432"))
        db_name = os.environ.get("SUPABASE_DB_NAME", "postgres")
        pdf_bucket = os.environ.get("SCRAPER_PDF_BUCKET", "legal-scraper-pdfs")

        # Feature flags
        persistence_enabled = (
            os.environ.get("ENABLE_SUPABASE_PERSISTENCE", "false").lower() == "true"
        )
        dual_write_enabled = (
            os.environ.get("ENABLE_DUAL_WRITE", "true").lower() == "true"
        )
        realtime_enabled = (
            os.environ.get("ENABLE_REALTIME_UPDATES", "false").lower() == "true"
        )
        allow_insecure = (
            os.environ.get("SUPABASE_ALLOW_INSECURE", "false").lower() == "true"
        )

        # Validate HTTPS requirement
        if not allow_insecure and not url.startswith("https://"):
            raise ValueError(
                f"SUPABASE_URL must use HTTPS (got: {url}). "
                "Set SUPABASE_ALLOW_INSECURE=true for local development."
            )

        return cls(
            url=url,
            anon_key=anon_key,
            service_role_key=service_role_key,
            db_host=db_host,
            db_port=db_port,
            db_password=db_password,
            db_name=db_name,
            persistence_enabled=persistence_enabled,
            dual_write_enabled=dual_write_enabled,
            realtime_enabled=realtime_enabled,
            pdf_bucket=pdf_bucket,
            allow_insecure=allow_insecure,
        )

    @property
    def db_url(self) -> str:
        """
        Build a PostgreSQL connection URL.

        Returns:
            Connection URL in format: postgresql://postgres:password@host:port/database
        """
        return (
            f"postgresql://postgres:{self.db_password}@"
            f"{self.db_host}:{self.db_port}/{self.db_name}"
        )

    @property
    def asyncpg_url(self) -> str:
        """
        Build an asyncpg-compatible connection URL.

        This is the same format as db_url but explicitly named for asyncpg usage.

        Returns:
            Connection URL in format: postgresql://postgres:password@host:port/database
        """
        return self.db_url
