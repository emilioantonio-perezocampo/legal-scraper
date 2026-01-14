#!/usr/bin/env python3
import argparse
import base64
import getpass
import hashlib
import secrets


def _b64encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("utf-8").rstrip("=")


def hash_password(password: str, iterations: int) -> str:
    salt = secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
    return f"pbkdf2_sha256${iterations}${_b64encode(salt)}${_b64encode(digest)}"


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate PBKDF2 password hash.")
    parser.add_argument("--password", help="Password to hash (optional, prompts if omitted)")
    parser.add_argument("--iterations", type=int, default=390000)
    args = parser.parse_args()

    password = args.password or getpass.getpass("Password: ")
    if not password:
        print("Password cannot be empty.")
        return 1

    print(hash_password(password, args.iterations))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
