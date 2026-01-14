# Deployment (Reflex UI + FastAPI API)

This setup runs the API and the Reflex UI behind Caddy with HTTPS.

## 1. Configure authentication

Generate a password hash:

```bash
python scripts/generate_password_hash.py
```

Create a `.env` file in the repo root with:

```bash
SCRAPER_AUTH_USERNAME=admin
SCRAPER_AUTH_PASSWORD_HASH=pbkdf2_sha256$...
SCRAPER_AUTH_JWT_SECRET=replace_with_long_random_string
SCRAPER_AUTH_TOKEN_TTL_MINUTES=60
SCRAPER_AUTH_COOKIE_SECURE=true
```

For local development you can disable auth:

```bash
SCRAPER_AUTH_DISABLED=true
```

## 2. Update Caddyfile

Edit `Caddyfile` and replace `YOUR_DOMAIN.com` with your real domain or public IP.

## 3. Start the stack

```bash
docker compose -f compose.prod.yaml up -d
```

## 4. Make it reachable

- Point DNS to the VM public IP.
- Ensure ports 80 and 443 are open in your firewall.
