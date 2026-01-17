"""
RED Phase Tests: SupabaseSettings

Tests for Supabase configuration settings.
These tests must FAIL initially (RED phase).

The SupabaseSettings class should:
- Load configuration from environment variables
- Validate required fields
- Build database connection URLs
- Support both Supabase cloud and self-hosted instances
"""
import pytest

# This import will fail until implementation exists
from src.infrastructure.supabase_settings import SupabaseSettings


class TestSupabaseSettingsFromEnv:
    """Tests for loading settings from environment variables."""

    def test_from_env_loads_url(self, monkeypatch):
        """SUPABASE_URL should be loaded from environment."""
        monkeypatch.setenv("SUPABASE_URL", "https://example.supabase.co")
        monkeypatch.setenv("SUPABASE_ANON_KEY", "test-anon-key")
        monkeypatch.setenv("SUPABASE_SERVICE_ROLE_KEY", "test-service-key")
        monkeypatch.setenv("SUPABASE_DB_HOST", "localhost")
        monkeypatch.setenv("SUPABASE_DB_PASSWORD", "secret")

        settings = SupabaseSettings.from_env()

        assert settings.url == "https://example.supabase.co"

    def test_from_env_loads_anon_key(self, monkeypatch):
        """SUPABASE_ANON_KEY should be loaded from environment."""
        monkeypatch.setenv("SUPABASE_URL", "https://example.supabase.co")
        monkeypatch.setenv("SUPABASE_ANON_KEY", "test-anon-key")
        monkeypatch.setenv("SUPABASE_SERVICE_ROLE_KEY", "test-service-key")
        monkeypatch.setenv("SUPABASE_DB_HOST", "localhost")
        monkeypatch.setenv("SUPABASE_DB_PASSWORD", "secret")

        settings = SupabaseSettings.from_env()

        assert settings.anon_key == "test-anon-key"

    def test_from_env_loads_service_role_key(self, monkeypatch):
        """SUPABASE_SERVICE_ROLE_KEY should be loaded from environment."""
        monkeypatch.setenv("SUPABASE_URL", "https://example.supabase.co")
        monkeypatch.setenv("SUPABASE_ANON_KEY", "test-anon-key")
        monkeypatch.setenv("SUPABASE_SERVICE_ROLE_KEY", "test-service-key")
        monkeypatch.setenv("SUPABASE_DB_HOST", "localhost")
        monkeypatch.setenv("SUPABASE_DB_PASSWORD", "secret")

        settings = SupabaseSettings.from_env()

        assert settings.service_role_key == "test-service-key"

    def test_from_env_raises_if_url_missing(self, monkeypatch):
        """Should raise ValueError if SUPABASE_URL is missing."""
        monkeypatch.delenv("SUPABASE_URL", raising=False)
        monkeypatch.setenv("SUPABASE_ANON_KEY", "test-anon-key")
        monkeypatch.setenv("SUPABASE_SERVICE_ROLE_KEY", "test-service-key")

        with pytest.raises(ValueError, match="SUPABASE_URL"):
            SupabaseSettings.from_env()

    def test_from_env_raises_if_anon_key_missing(self, monkeypatch):
        """Should raise ValueError if SUPABASE_ANON_KEY is missing."""
        monkeypatch.setenv("SUPABASE_URL", "https://example.supabase.co")
        monkeypatch.delenv("SUPABASE_ANON_KEY", raising=False)
        monkeypatch.setenv("SUPABASE_SERVICE_ROLE_KEY", "test-service-key")

        with pytest.raises(ValueError, match="SUPABASE_ANON_KEY"):
            SupabaseSettings.from_env()

    def test_from_env_raises_if_service_role_key_missing(self, monkeypatch):
        """Should raise ValueError if SUPABASE_SERVICE_ROLE_KEY is missing."""
        monkeypatch.setenv("SUPABASE_URL", "https://example.supabase.co")
        monkeypatch.setenv("SUPABASE_ANON_KEY", "test-anon-key")
        monkeypatch.delenv("SUPABASE_SERVICE_ROLE_KEY", raising=False)

        with pytest.raises(ValueError, match="SUPABASE_SERVICE_ROLE_KEY"):
            SupabaseSettings.from_env()


class TestSupabaseSettingsDbUrl:
    """Tests for database URL construction."""

    def test_db_url_built_correctly(self, monkeypatch):
        """Database URL should be constructed from components."""
        monkeypatch.setenv("SUPABASE_URL", "https://example.supabase.co")
        monkeypatch.setenv("SUPABASE_ANON_KEY", "test-anon-key")
        monkeypatch.setenv("SUPABASE_SERVICE_ROLE_KEY", "test-service-key")
        monkeypatch.setenv("SUPABASE_DB_HOST", "db.example.com")
        monkeypatch.setenv("SUPABASE_DB_PASSWORD", "supersecret")

        settings = SupabaseSettings.from_env()

        assert "db.example.com" in settings.db_url
        assert "supersecret" in settings.db_url

    def test_db_url_defaults_to_localhost(self, monkeypatch):
        """Database host should default to localhost if not set."""
        monkeypatch.setenv("SUPABASE_URL", "https://example.supabase.co")
        monkeypatch.setenv("SUPABASE_ANON_KEY", "test-anon-key")
        monkeypatch.setenv("SUPABASE_SERVICE_ROLE_KEY", "test-service-key")
        monkeypatch.setenv("SUPABASE_DB_PASSWORD", "secret")
        monkeypatch.delenv("SUPABASE_DB_HOST", raising=False)

        settings = SupabaseSettings.from_env()

        assert "localhost" in settings.db_url

    def test_db_url_default_port(self, monkeypatch):
        """Database port should default to 5432."""
        monkeypatch.setenv("SUPABASE_URL", "https://example.supabase.co")
        monkeypatch.setenv("SUPABASE_ANON_KEY", "test-anon-key")
        monkeypatch.setenv("SUPABASE_SERVICE_ROLE_KEY", "test-service-key")
        monkeypatch.setenv("SUPABASE_DB_HOST", "localhost")
        monkeypatch.setenv("SUPABASE_DB_PASSWORD", "secret")
        monkeypatch.delenv("SUPABASE_DB_PORT", raising=False)

        settings = SupabaseSettings.from_env()

        assert ":5432/" in settings.db_url

    def test_db_url_custom_port(self, monkeypatch):
        """Database port should be configurable."""
        monkeypatch.setenv("SUPABASE_URL", "https://example.supabase.co")
        monkeypatch.setenv("SUPABASE_ANON_KEY", "test-anon-key")
        monkeypatch.setenv("SUPABASE_SERVICE_ROLE_KEY", "test-service-key")
        monkeypatch.setenv("SUPABASE_DB_HOST", "localhost")
        monkeypatch.setenv("SUPABASE_DB_PASSWORD", "secret")
        monkeypatch.setenv("SUPABASE_DB_PORT", "6543")

        settings = SupabaseSettings.from_env()

        assert ":6543/" in settings.db_url

    def test_db_url_raises_if_password_missing(self, monkeypatch):
        """Should raise ValueError if DB password is required but missing."""
        monkeypatch.setenv("SUPABASE_URL", "https://example.supabase.co")
        monkeypatch.setenv("SUPABASE_ANON_KEY", "test-anon-key")
        monkeypatch.setenv("SUPABASE_SERVICE_ROLE_KEY", "test-service-key")
        monkeypatch.setenv("SUPABASE_DB_HOST", "localhost")
        monkeypatch.delenv("SUPABASE_DB_PASSWORD", raising=False)

        with pytest.raises(ValueError, match="SUPABASE_DB_PASSWORD"):
            SupabaseSettings.from_env()


class TestSupabaseSettingsFeatureFlags:
    """Tests for feature flag configuration."""

    def test_persistence_enabled_by_default(self, monkeypatch):
        """ENABLE_SUPABASE_PERSISTENCE should default to False."""
        monkeypatch.setenv("SUPABASE_URL", "https://example.supabase.co")
        monkeypatch.setenv("SUPABASE_ANON_KEY", "test-anon-key")
        monkeypatch.setenv("SUPABASE_SERVICE_ROLE_KEY", "test-service-key")
        monkeypatch.setenv("SUPABASE_DB_HOST", "localhost")
        monkeypatch.setenv("SUPABASE_DB_PASSWORD", "secret")
        monkeypatch.delenv("ENABLE_SUPABASE_PERSISTENCE", raising=False)

        settings = SupabaseSettings.from_env()

        assert settings.persistence_enabled is False

    def test_persistence_can_be_enabled(self, monkeypatch):
        """ENABLE_SUPABASE_PERSISTENCE can be set to true."""
        monkeypatch.setenv("SUPABASE_URL", "https://example.supabase.co")
        monkeypatch.setenv("SUPABASE_ANON_KEY", "test-anon-key")
        monkeypatch.setenv("SUPABASE_SERVICE_ROLE_KEY", "test-service-key")
        monkeypatch.setenv("SUPABASE_DB_HOST", "localhost")
        monkeypatch.setenv("SUPABASE_DB_PASSWORD", "secret")
        monkeypatch.setenv("ENABLE_SUPABASE_PERSISTENCE", "true")

        settings = SupabaseSettings.from_env()

        assert settings.persistence_enabled is True

    def test_dual_write_enabled_by_default(self, monkeypatch):
        """ENABLE_DUAL_WRITE should default to True for safety."""
        monkeypatch.setenv("SUPABASE_URL", "https://example.supabase.co")
        monkeypatch.setenv("SUPABASE_ANON_KEY", "test-anon-key")
        monkeypatch.setenv("SUPABASE_SERVICE_ROLE_KEY", "test-service-key")
        monkeypatch.setenv("SUPABASE_DB_HOST", "localhost")
        monkeypatch.setenv("SUPABASE_DB_PASSWORD", "secret")
        monkeypatch.delenv("ENABLE_DUAL_WRITE", raising=False)

        settings = SupabaseSettings.from_env()

        assert settings.dual_write_enabled is True

    def test_realtime_enabled_by_default(self, monkeypatch):
        """ENABLE_REALTIME_UPDATES should default to False."""
        monkeypatch.setenv("SUPABASE_URL", "https://example.supabase.co")
        monkeypatch.setenv("SUPABASE_ANON_KEY", "test-anon-key")
        monkeypatch.setenv("SUPABASE_SERVICE_ROLE_KEY", "test-service-key")
        monkeypatch.setenv("SUPABASE_DB_HOST", "localhost")
        monkeypatch.setenv("SUPABASE_DB_PASSWORD", "secret")
        monkeypatch.delenv("ENABLE_REALTIME_UPDATES", raising=False)

        settings = SupabaseSettings.from_env()

        assert settings.realtime_enabled is False


class TestSupabaseSettingsStorage:
    """Tests for storage configuration."""

    def test_pdf_bucket_default(self, monkeypatch):
        """PDF bucket should have a sensible default."""
        monkeypatch.setenv("SUPABASE_URL", "https://example.supabase.co")
        monkeypatch.setenv("SUPABASE_ANON_KEY", "test-anon-key")
        monkeypatch.setenv("SUPABASE_SERVICE_ROLE_KEY", "test-service-key")
        monkeypatch.setenv("SUPABASE_DB_HOST", "localhost")
        monkeypatch.setenv("SUPABASE_DB_PASSWORD", "secret")
        monkeypatch.delenv("SCRAPER_PDF_BUCKET", raising=False)

        settings = SupabaseSettings.from_env()

        assert settings.pdf_bucket == "legal-scraper-pdfs"

    def test_pdf_bucket_configurable(self, monkeypatch):
        """PDF bucket should be configurable."""
        monkeypatch.setenv("SUPABASE_URL", "https://example.supabase.co")
        monkeypatch.setenv("SUPABASE_ANON_KEY", "test-anon-key")
        monkeypatch.setenv("SUPABASE_SERVICE_ROLE_KEY", "test-service-key")
        monkeypatch.setenv("SUPABASE_DB_HOST", "localhost")
        monkeypatch.setenv("SUPABASE_DB_PASSWORD", "secret")
        monkeypatch.setenv("SCRAPER_PDF_BUCKET", "custom-bucket")

        settings = SupabaseSettings.from_env()

        assert settings.pdf_bucket == "custom-bucket"


class TestSupabaseSettingsValidation:
    """Tests for URL and key validation."""

    def test_url_must_be_https(self, monkeypatch):
        """SUPABASE_URL must use HTTPS in production."""
        monkeypatch.setenv("SUPABASE_URL", "http://insecure.example.com")
        monkeypatch.setenv("SUPABASE_ANON_KEY", "test-anon-key")
        monkeypatch.setenv("SUPABASE_SERVICE_ROLE_KEY", "test-service-key")
        monkeypatch.setenv("SUPABASE_DB_HOST", "localhost")
        monkeypatch.setenv("SUPABASE_DB_PASSWORD", "secret")
        monkeypatch.setenv("SUPABASE_ALLOW_INSECURE", "false")

        with pytest.raises(ValueError, match="HTTPS"):
            SupabaseSettings.from_env()

    def test_url_allows_http_when_insecure_allowed(self, monkeypatch):
        """HTTP URLs allowed when SUPABASE_ALLOW_INSECURE is true."""
        monkeypatch.setenv("SUPABASE_URL", "http://localhost:8000")
        monkeypatch.setenv("SUPABASE_ANON_KEY", "test-anon-key")
        monkeypatch.setenv("SUPABASE_SERVICE_ROLE_KEY", "test-service-key")
        monkeypatch.setenv("SUPABASE_DB_HOST", "localhost")
        monkeypatch.setenv("SUPABASE_DB_PASSWORD", "secret")
        monkeypatch.setenv("SUPABASE_ALLOW_INSECURE", "true")

        settings = SupabaseSettings.from_env()

        assert settings.url == "http://localhost:8000"


class TestSupabaseSettingsAsyncpgUrl:
    """Tests for asyncpg connection URL."""

    def test_asyncpg_url_format(self, monkeypatch):
        """asyncpg URL should use postgresql:// scheme."""
        monkeypatch.setenv("SUPABASE_URL", "https://example.supabase.co")
        monkeypatch.setenv("SUPABASE_ANON_KEY", "test-anon-key")
        monkeypatch.setenv("SUPABASE_SERVICE_ROLE_KEY", "test-service-key")
        monkeypatch.setenv("SUPABASE_DB_HOST", "db.example.com")
        monkeypatch.setenv("SUPABASE_DB_PASSWORD", "secret")

        settings = SupabaseSettings.from_env()

        assert settings.asyncpg_url.startswith("postgresql://")

    def test_asyncpg_url_contains_credentials(self, monkeypatch):
        """asyncpg URL should contain postgres user and password."""
        monkeypatch.setenv("SUPABASE_URL", "https://example.supabase.co")
        monkeypatch.setenv("SUPABASE_ANON_KEY", "test-anon-key")
        monkeypatch.setenv("SUPABASE_SERVICE_ROLE_KEY", "test-service-key")
        monkeypatch.setenv("SUPABASE_DB_HOST", "db.example.com")
        monkeypatch.setenv("SUPABASE_DB_PASSWORD", "mypassword")

        settings = SupabaseSettings.from_env()

        assert "postgres:" in settings.asyncpg_url
        assert "mypassword" in settings.asyncpg_url
