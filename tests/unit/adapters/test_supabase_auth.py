"""
RED Phase Tests: SupabaseAuthBridge

Tests for JWT validation and user/tenant context extraction.
These tests must FAIL initially (RED phase).

The bridge should:
- Validate JWT tokens from Supabase Auth
- Extract user info (id, email, role)
- Extract tenant_id from app_metadata
- Provide client factories for RLS-scoped and admin access
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, timedelta

from src.infrastructure.adapters.supabase_auth import (
    SupabaseAuthBridge,
    AuthenticationError,
    TokenExpiredError,
)


@pytest.fixture
def mock_settings():
    """Create mock Supabase settings."""
    settings = MagicMock()
    settings.url = "https://test.supabase.co"
    settings.anon_key = "test-anon-key"
    settings.service_role_key = "test-service-role-key"
    return settings


@pytest.fixture
def mock_supabase_client():
    """Create a mock Supabase client with auth capabilities."""
    client = MagicMock()

    # Mock user response
    user = MagicMock()
    user.id = "user-123"
    user.email = "test@example.com"
    user.app_metadata = {
        "tenant_id": "tenant-abc",
        "role": "staff",
    }
    user.user_metadata = {
        "name": "Test User",
    }

    # Mock auth.get_user response
    user_response = MagicMock()
    user_response.user = user

    client.auth.get_user.return_value = user_response

    return client


@pytest.fixture
def valid_jwt_token():
    """A mock valid JWT token string."""
    return "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.valid.signature"


@pytest.fixture
def expired_jwt_token():
    """A mock expired JWT token string."""
    return "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.expired.signature"


class TestSupabaseAuthBridgeVerifyToken:
    """Tests for token verification."""

    @pytest.mark.asyncio
    async def test_verify_token_returns_user_info(
        self, mock_settings, mock_supabase_client, valid_jwt_token
    ):
        """Given valid JWT, returns user id, email, tenant_id."""
        bridge = SupabaseAuthBridge(
            settings=mock_settings, client=mock_supabase_client
        )
        user = await bridge.verify_token(valid_jwt_token)

        assert user["id"] == "user-123"
        assert user["email"] == "test@example.com"
        assert user["tenant_id"] == "tenant-abc"

    @pytest.mark.asyncio
    async def test_verify_token_includes_role(
        self, mock_settings, mock_supabase_client, valid_jwt_token
    ):
        """Given valid JWT with role, includes role in result."""
        bridge = SupabaseAuthBridge(
            settings=mock_settings, client=mock_supabase_client
        )
        user = await bridge.verify_token(valid_jwt_token)

        assert user["role"] == "staff"

    @pytest.mark.asyncio
    async def test_verify_token_raises_on_invalid(
        self, mock_settings, mock_supabase_client
    ):
        """Given invalid JWT, raises AuthenticationError."""
        mock_supabase_client.auth.get_user.side_effect = Exception("Invalid token")

        bridge = SupabaseAuthBridge(
            settings=mock_settings, client=mock_supabase_client
        )

        with pytest.raises(AuthenticationError, match="Invalid"):
            await bridge.verify_token("invalid-token")

    @pytest.mark.asyncio
    async def test_verify_token_raises_on_expired(
        self, mock_settings, mock_supabase_client, expired_jwt_token
    ):
        """Given expired JWT, raises TokenExpiredError."""
        mock_supabase_client.auth.get_user.side_effect = Exception(
            "JWT expired"
        )

        bridge = SupabaseAuthBridge(
            settings=mock_settings, client=mock_supabase_client
        )

        with pytest.raises((TokenExpiredError, AuthenticationError)):
            await bridge.verify_token(expired_jwt_token)

    @pytest.mark.asyncio
    async def test_verify_token_handles_missing_tenant(
        self, mock_settings, mock_supabase_client, valid_jwt_token
    ):
        """Given JWT without tenant_id, returns None for tenant_id."""
        mock_supabase_client.auth.get_user.return_value.user.app_metadata = {}

        bridge = SupabaseAuthBridge(
            settings=mock_settings, client=mock_supabase_client
        )
        user = await bridge.verify_token(valid_jwt_token)

        assert user["tenant_id"] is None


class TestSupabaseAuthBridgeExtractTenantId:
    """Tests for tenant ID extraction."""

    @pytest.mark.asyncio
    async def test_extract_tenant_id_from_app_metadata(
        self, mock_settings, mock_supabase_client, valid_jwt_token
    ):
        """Tenant ID should be extracted from app_metadata."""
        bridge = SupabaseAuthBridge(
            settings=mock_settings, client=mock_supabase_client
        )
        tenant_id = await bridge.extract_tenant_id(valid_jwt_token)

        assert tenant_id == "tenant-abc"

    @pytest.mark.asyncio
    async def test_extract_tenant_id_returns_none_when_missing(
        self, mock_settings, mock_supabase_client, valid_jwt_token
    ):
        """Returns None when tenant_id not in app_metadata."""
        mock_supabase_client.auth.get_user.return_value.user.app_metadata = {
            "role": "admin"
        }

        bridge = SupabaseAuthBridge(
            settings=mock_settings, client=mock_supabase_client
        )
        tenant_id = await bridge.extract_tenant_id(valid_jwt_token)

        assert tenant_id is None


class TestSupabaseAuthBridgeClientFactories:
    """Tests for Supabase client factory methods."""

    def test_get_client_for_user_uses_anon_key(
        self, mock_settings, mock_supabase_client, valid_jwt_token
    ):
        """User client should use anon key with RLS enforcement."""
        with patch(
            "src.infrastructure.adapters.supabase_auth.create_client"
        ) as mock_create:
            mock_create.return_value = MagicMock()

            bridge = SupabaseAuthBridge(
                settings=mock_settings, client=mock_supabase_client
            )
            bridge.get_client_for_user(valid_jwt_token)

            mock_create.assert_called_once()
            call_args = mock_create.call_args[0]
            assert call_args[1] == "test-anon-key"

    def test_get_admin_client_uses_service_role(
        self, mock_settings, mock_supabase_client
    ):
        """Admin client should use service_role key (bypasses RLS)."""
        with patch(
            "src.infrastructure.adapters.supabase_auth.create_client"
        ) as mock_create:
            mock_create.return_value = MagicMock()

            bridge = SupabaseAuthBridge(
                settings=mock_settings, client=mock_supabase_client
            )
            bridge.get_admin_client()

            mock_create.assert_called_once()
            call_args = mock_create.call_args[0]
            assert call_args[1] == "test-service-role-key"


class TestSupabaseAuthBridgeConfiguration:
    """Tests for bridge configuration."""

    def test_bridge_requires_settings(self, mock_supabase_client):
        """Bridge should require settings object."""
        with pytest.raises(TypeError):
            SupabaseAuthBridge(client=mock_supabase_client)

    def test_bridge_stores_settings(self, mock_settings, mock_supabase_client):
        """Bridge should store settings for client creation."""
        bridge = SupabaseAuthBridge(
            settings=mock_settings, client=mock_supabase_client
        )

        assert bridge._settings == mock_settings

    def test_bridge_stores_client(self, mock_settings, mock_supabase_client):
        """Bridge should store client for token validation."""
        bridge = SupabaseAuthBridge(
            settings=mock_settings, client=mock_supabase_client
        )

        assert bridge._client == mock_supabase_client


class TestSupabaseAuthBridgeUserInfo:
    """Tests for user info extraction."""

    @pytest.mark.asyncio
    async def test_extracts_user_metadata(
        self, mock_settings, mock_supabase_client, valid_jwt_token
    ):
        """User metadata should be included in result."""
        bridge = SupabaseAuthBridge(
            settings=mock_settings, client=mock_supabase_client
        )
        user = await bridge.verify_token(valid_jwt_token)

        assert "user_metadata" in user
        assert user["user_metadata"]["name"] == "Test User"

    @pytest.mark.asyncio
    async def test_extracts_app_metadata(
        self, mock_settings, mock_supabase_client, valid_jwt_token
    ):
        """App metadata should be included in result."""
        bridge = SupabaseAuthBridge(
            settings=mock_settings, client=mock_supabase_client
        )
        user = await bridge.verify_token(valid_jwt_token)

        assert "app_metadata" in user
        assert user["app_metadata"]["role"] == "staff"
