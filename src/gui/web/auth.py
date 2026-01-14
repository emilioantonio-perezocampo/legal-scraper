import base64
import hashlib
import hmac
import os
import secrets
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict

import jwt


class AuthError(Exception):
    pass


@dataclass(frozen=True)
class AuthSettings:
    username: str
    password_hash: str
    jwt_secret: str
    token_ttl_minutes: int = 60
    disabled: bool = False
    cookie_name: str = "scraper_token"
    cookie_secure: bool = True

    @classmethod
    def from_env(cls) -> "AuthSettings":
        disabled = os.getenv("SCRAPER_AUTH_DISABLED", "").lower() in ("1", "true", "yes")
        if disabled:
            return cls(
                username="",
                password_hash="",
                jwt_secret="",
                token_ttl_minutes=0,
                disabled=True,
                cookie_secure=False,
            )

        username = os.getenv("SCRAPER_AUTH_USERNAME", "")
        password_hash = os.getenv("SCRAPER_AUTH_PASSWORD_HASH", "")
        jwt_secret = os.getenv("SCRAPER_AUTH_JWT_SECRET", "")
        ttl_str = os.getenv("SCRAPER_AUTH_TOKEN_TTL_MINUTES", "60")
        cookie_secure = os.getenv("SCRAPER_AUTH_COOKIE_SECURE", "true").lower() in ("1", "true", "yes")

        if not username or not password_hash or not jwt_secret:
            raise RuntimeError(
                "Missing auth configuration. Set SCRAPER_AUTH_USERNAME, "
                "SCRAPER_AUTH_PASSWORD_HASH, and SCRAPER_AUTH_JWT_SECRET "
                "(or SCRAPER_AUTH_DISABLED=true for local dev)."
            )

        try:
            ttl = int(ttl_str)
        except ValueError as exc:
            raise RuntimeError("SCRAPER_AUTH_TOKEN_TTL_MINUTES must be an integer.") from exc

        return cls(
            username=username,
            password_hash=password_hash,
            jwt_secret=jwt_secret,
            token_ttl_minutes=ttl,
            cookie_secure=cookie_secure,
        )


def _b64encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("utf-8").rstrip("=")


def _b64decode(raw: str) -> bytes:
    padded = raw + "=" * (-len(raw) % 4)
    return base64.urlsafe_b64decode(padded.encode("utf-8"))


def hash_password(password: str, iterations: int = 390000) -> str:
    salt = secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
    return f"pbkdf2_sha256${iterations}${_b64encode(salt)}${_b64encode(digest)}"


def verify_password(password: str, encoded: str) -> bool:
    try:
        scheme, iter_str, salt_b64, digest_b64 = encoded.split("$", 3)
        if scheme != "pbkdf2_sha256":
            return False
        iterations = int(iter_str)
        salt = _b64decode(salt_b64)
        expected = _b64decode(digest_b64)
    except (ValueError, TypeError):
        return False

    test_digest = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt,
        iterations,
    )
    return hmac.compare_digest(test_digest, expected)


def create_access_token(username: str, settings: AuthSettings) -> str:
    now = datetime.now(timezone.utc)
    exp = now + timedelta(minutes=settings.token_ttl_minutes)
    payload = {
        "sub": username,
        "iat": int(now.timestamp()),
        "exp": int(exp.timestamp()),
    }
    return jwt.encode(payload, settings.jwt_secret, algorithm="HS256")


def decode_access_token(token: str, settings: AuthSettings) -> Dict[str, Any]:
    try:
        payload = jwt.decode(token, settings.jwt_secret, algorithms=["HS256"])
    except jwt.ExpiredSignatureError as exc:
        raise AuthError("Token expired.") from exc
    except jwt.InvalidTokenError as exc:
        raise AuthError("Invalid token.") from exc
    return payload
