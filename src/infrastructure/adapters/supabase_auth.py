"""
Supabase Auth Bridge for JWT validation and client factory.

Provides authentication integration with Supabase Auth (GoTrue),
including JWT validation, user/tenant context extraction, and
client factories for RLS-scoped and admin access.

Features:
- JWT token validation against Supabase Auth
- Tenant ID extraction from app_metadata
- User-scoped client (with RLS) for normal operations
- Admin client (bypasses RLS) for administrative tasks

Usage:
    from supabase import create_client
    settings = SupabaseSettings.from_env()
    client = create_client(settings.url, settings.service_role_key)
    bridge = SupabaseAuthBridge(settings=settings, client=client)

    # Validate token and get user info
    user = await bridge.verify_token(jwt_token)
    print(f"User: {user['email']}, Tenant: {user['tenant_id']}")
"""
from __future__ import annotations

from typing import Any, Dict, Optional

try:
    from supabase import create_client
except ImportError:
    create_client = None  # type: ignore


class AuthenticationError(Exception):
    """Raised when token validation fails."""

    pass


class TokenExpiredError(AuthenticationError):
    """Raised when JWT token has expired."""

    pass


class SupabaseAuthBridge:
    """
    Authentication bridge for Supabase Auth integration.

    Handles JWT validation and provides factory methods for creating
    Supabase clients with appropriate access levels.

    Attributes:
        _settings: Supabase configuration settings
        _client: Supabase client for token validation
    """

    def __init__(
        self,
        settings: Any,
        client: Any,
    ) -> None:
        """
        Initialize the auth bridge.

        Args:
            settings: SupabaseSettings with URL and keys
            client: Supabase client for token validation
        """
        self._settings = settings
        self._client = client

    async def verify_token(self, token: str) -> Dict[str, Any]:
        """
        Verify a JWT token and extract user information.

        Args:
            token: JWT token from Supabase Auth

        Returns:
            Dictionary containing:
            - id: User UUID
            - email: User email address
            - tenant_id: Tenant UUID from app_metadata (or None)
            - role: User role from app_metadata (or None)
            - user_metadata: User's custom metadata
            - app_metadata: Application metadata (tenant_id, role, etc.)

        Raises:
            AuthenticationError: If token is invalid
            TokenExpiredError: If token has expired
        """
        try:
            response = self._client.auth.get_user(token)
            user = response.user

            app_metadata = getattr(user, "app_metadata", {}) or {}
            user_metadata = getattr(user, "user_metadata", {}) or {}

            return {
                "id": user.id,
                "email": user.email,
                "tenant_id": app_metadata.get("tenant_id"),
                "role": app_metadata.get("role"),
                "user_metadata": user_metadata,
                "app_metadata": app_metadata,
            }

        except Exception as e:
            error_msg = str(e).lower()
            if "expired" in error_msg:
                raise TokenExpiredError(f"Token expired: {e}") from e
            raise AuthenticationError(f"Invalid token: {e}") from e

    async def extract_tenant_id(self, token: str) -> Optional[str]:
        """
        Extract tenant ID from a JWT token.

        Args:
            token: JWT token from Supabase Auth

        Returns:
            Tenant UUID string, or None if not present
        """
        user_info = await self.verify_token(token)
        return user_info.get("tenant_id")

    def get_client_for_user(self, token: str) -> Any:
        """
        Get a Supabase client scoped to the user's RLS policies.

        This client uses the anon key and sets the user's JWT,
        ensuring all database operations respect Row Level Security.

        Args:
            token: User's JWT token

        Returns:
            Supabase client with user context
        """
        if create_client is None:
            raise ImportError("supabase package not installed")

        client = create_client(
            self._settings.url,
            self._settings.anon_key,
        )
        # Set the user's token for RLS
        client.auth.set_session(token, token)
        return client

    def get_admin_client(self) -> Any:
        """
        Get a Supabase client with admin (service role) access.

        This client bypasses RLS and should only be used for
        administrative operations that require full table access.

        Returns:
            Supabase client with service role key
        """
        if create_client is None:
            raise ImportError("supabase package not installed")

        return create_client(
            self._settings.url,
            self._settings.service_role_key,
        )
