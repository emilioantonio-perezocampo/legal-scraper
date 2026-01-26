# Scraper GUI API Architecture

> **Version:** 1.0
> **Last Updated:** 2026-01-14
> **Maintainers:** Legal Scraper Team

## Overview

The Legal Scraper GUI provides a FastAPI-based REST API for controlling multiple legal document scrapers. The architecture follows a **Bridge Actor Pattern** where each scraper type (SCJN, BJV, CAS, DOF) has a dedicated bridge actor that translates HTTP requests into actor system messages.

```
┌─────────────────────────────────────────────────────────────────┐
│                        FastAPI Web Layer                         │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐        │
│  │  /api/   │  │/api/scjn │  │ /api/bjv │  │ /api/cas │        │
│  │ generic  │  │          │  │          │  │  (TODO)  │        │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘        │
└───────┼─────────────┼─────────────┼─────────────┼──────────────┘
        │             │             │             │
        ▼             ▼             ▼             ▼
┌─────────────────────────────────────────────────────────────────┐
│                      ScraperAPI Orchestrator                     │
│  ┌──────────────┐  ┌────────────┐  ┌────────────┐               │
│  │ScraperService│  │ SCJN Bridge│  │ BJV Bridge │  ...          │
│  └──────────────┘  └────────────┘  └────────────┘               │
└─────────────────────────────────────────────────────────────────┘
        │                   │              │
        ▼                   ▼              ▼
┌─────────────────────────────────────────────────────────────────┐
│                     Actor System (Pykka-based)                   │
│  ┌────────────┐  ┌────────────┐  ┌────────────┐  ┌──────────┐  │
│  │ Discovery  │  │ Coordinator│  │  Download  │  │Persistence│  │
│  │   Actor    │  │   Actor    │  │   Actor    │  │  Actor    │  │
│  └────────────┘  └────────────┘  └────────────┘  └──────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

## Authentication

### Type
- **Password Hashing:** PBKDF2-SHA256 (390,000 iterations)
- **Token Type:** JWT with HS256 signing
- **Token Storage:** HTTP-only cookie (`scraper_token`)

### Auth Flow

```
┌──────────┐     POST /api/auth/login      ┌──────────┐
│  Client  │ ───────────────────────────▶  │   API    │
│          │   {username, password}        │          │
│          │                               │          │
│          │  ◀─────────────────────────── │          │
│          │   Set-Cookie: scraper_token   │          │
│          │   {access_token, expires_in}  │          │
│          │                               │          │
│          │   GET /api/status             │          │
│          │ ───────────────────────────▶  │          │
│          │   Cookie: scraper_token       │          │
│          │                               │          │
│          │  ◀─────────────────────────── │          │
│          │   200 OK / 401 Unauthorized   │          │
└──────────┘                               └──────────┘
```

### Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `SCRAPER_AUTH_USERNAME` | Yes* | - | Admin username |
| `SCRAPER_AUTH_PASSWORD_HASH` | Yes* | - | PBKDF2 password hash |
| `SCRAPER_AUTH_JWT_SECRET` | Yes* | - | Secret for JWT signing |
| `SCRAPER_AUTH_TOKEN_TTL_MINUTES` | No | `60` | Token expiration |
| `SCRAPER_AUTH_COOKIE_SECURE` | No | `true` | Secure cookie flag |
| `SCRAPER_AUTH_DISABLED` | No | `false` | Disable auth (dev only) |

*Required unless `SCRAPER_AUTH_DISABLED=true`

### Generate Password Hash

```bash
python scripts/generate_password_hash.py --password "your-password"
```

---

## Endpoints

### Health & Auth Endpoints

| Method | Path | Auth | Purpose |
|--------|------|:----:|---------|
| GET | `/api/health` | ❌ | Health check |
| POST | `/api/auth/login` | ❌ | Authenticate user |
| POST | `/api/auth/logout` | ✅ | Invalidate session |

### Generic Scraper Endpoints

| Method | Path | Auth | Purpose |
|--------|------|:----:|---------|
| GET | `/api/status` | ✅ | Get current job status |
| POST | `/api/start` | ✅ | Start scraping job |
| POST | `/api/pause` | ✅ | Pause current job |
| POST | `/api/resume` | ✅ | Resume paused job |
| POST | `/api/cancel` | ✅ | Cancel current job |
| GET | `/api/logs` | ✅ | Get job logs |
| GET | `/api/events` | ✅ | SSE event stream |
| GET | `/api/config/options` | ✅ | Get configuration options |

### SCJN Endpoints

| Method | Path | Auth | Purpose |
|--------|------|:----:|---------|
| GET | `/api/scjn/status` | ✅ | Get SCJN job status |
| GET | `/api/scjn/categories` | ✅ | List legislation categories |
| POST | `/api/scjn/start` | ✅ | Start SCJN scraping |
| POST | `/api/scjn/pause` | ✅ | Pause SCJN job |
| POST | `/api/scjn/resume` | ✅ | Resume SCJN job |
| POST | `/api/scjn/cancel` | ✅ | Cancel SCJN job |
| GET | `/api/scjn/logs` | ✅ | Get SCJN logs |

**Request: Start SCJN Scraping**
```json
{
  "category": "LEY",          // Optional: LEY, CODIGO, REGLAMENTO, etc.
  "scope": "FEDERAL",         // Optional: FEDERAL, ESTATAL
  "max_results": 100          // Default: 100
}
```

### BJV Endpoints

| Method | Path | Auth | Purpose |
|--------|------|:----:|---------|
| GET | `/api/bjv/status` | ✅ | Get BJV job status |
| GET | `/api/bjv/areas` | ✅ | List legal areas |
| POST | `/api/bjv/start` | ✅ | Start BJV scraping |
| POST | `/api/bjv/pause` | ✅ | Pause BJV job |
| POST | `/api/bjv/resume` | ✅ | Resume BJV job |
| POST | `/api/bjv/cancel` | ✅ | Cancel BJV job |
| GET | `/api/bjv/logs` | ✅ | Get BJV logs |

**Request: Start BJV Scraping**
```json
{
  "termino_busqueda": "constitución",  // Search term
  "area_derecho": "constitucional",    // Optional: civil, penal, etc.
  "max_resultados": 50,                // Default: 50
  "incluir_capitulos": true,           // Include chapters
  "descargar_pdfs": false              // Download PDFs
}
```

### CAS Endpoints (TODO)

| Method | Path | Auth | Purpose |
|--------|------|:----:|---------|
| GET | `/api/cas/status` | ✅ | Get CAS job status |
| GET | `/api/cas/filters` | ✅ | List sports/matters |
| POST | `/api/cas/start` | ✅ | Start CAS scraping |
| POST | `/api/cas/pause` | ✅ | Pause CAS job |
| POST | `/api/cas/resume` | ✅ | Resume CAS job |
| POST | `/api/cas/cancel` | ✅ | Cancel CAS job |
| GET | `/api/cas/logs` | ✅ | Get CAS logs |

### DOF Endpoints (TODO)

| Method | Path | Auth | Purpose |
|--------|------|:----:|---------|
| GET | `/api/dof/status` | ✅ | Get DOF job status |
| GET | `/api/dof/sections` | ✅ | List DOF sections |
| POST | `/api/dof/start` | ✅ | Start DOF scraping |
| POST | `/api/dof/pause` | ✅ | Pause DOF job |
| POST | `/api/dof/resume` | ✅ | Resume DOF job |
| POST | `/api/dof/cancel` | ✅ | Cancel DOF job |
| GET | `/api/dof/logs` | ✅ | Get DOF logs |

---

## Bridge Actor Pattern

Each scraper type uses a **Bridge Actor** to translate HTTP requests into actor messages.

### Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         HTTP Layer                               │
│                                                                  │
│   POST /api/scjn/start ──┐                                      │
│   POST /api/scjn/pause ──┼──▶ ScraperAPI._scjn_bridge.ask()     │
│   POST /api/scjn/cancel ─┘                                      │
└──────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌──────────────────────────────────────────────────────────────────┐
│                    SCJNGuiBridgeActor                            │
│                                                                  │
│   Messages:                                                      │
│   ├── ("START_SEARCH", SCJNSearchConfig) → Start scraping       │
│   ├── "PAUSE_SEARCH"  → Pause current job                       │
│   ├── "RESUME_SEARCH" → Resume paused job                       │
│   ├── "STOP_SEARCH"   → Cancel job                              │
│   ├── "GET_STATUS"    → Get current status                      │
│   └── "GET_PROGRESS"  → Get progress info                       │
│                                                                  │
│   Events (async callback):                                       │
│   ├── SCJNJobStarted   → Job started                            │
│   ├── SCJNJobProgress  → Progress update                        │
│   ├── SCJNJobCompleted → Job finished                           │
│   └── SCJNJobFailed    → Job error                              │
└──────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌──────────────────────────────────────────────────────────────────┐
│                     Coordinator Actor                            │
│                                                                  │
│   Manages:                                                       │
│   ├── Discovery Actor  → Find documents                         │
│   ├── Download Actor   → Fetch content                          │
│   └── Persistence Actor → Save data                             │
└──────────────────────────────────────────────────────────────────┘
```

### Bridge Files

| Scraper | Bridge File | Status |
|---------|-------------|--------|
| SCJN | `src/gui/infrastructure/actors/scjn_bridge.py` | ✅ Complete |
| BJV | `src/gui/infrastructure/actors/bjv_bridge.py` | ✅ Complete |
| CAS | `src/gui/infrastructure/actors/cas_gui_bridge.py` | ✅ Complete |
| DOF | `src/gui/infrastructure/actors/dof_gui_bridge.py` | ❌ TODO |
| Generic | `src/gui/infrastructure/actors/gui_bridge.py` | ✅ Complete |

---

## Domain Model

### Target Sources

```python
class TargetSource(Enum):
    DOF = "dof"   # Diario Oficial de la Federación
    SCJN = "scjn" # Suprema Corte de Justicia de la Nación
    BJV = "bjv"   # Biblioteca Jurídica Virtual UNAM
    CAS = "cas"   # Court of Arbitration for Sport
```

### Job Status Lifecycle

```
                  ┌─────────┐
                  │  IDLE   │
                  └────┬────┘
                       │ start()
                       ▼
                  ┌─────────┐
              ┌───│ RUNNING │───┐
              │   └────┬────┘   │
         pause()       │        │ error
              │        │        │
              ▼        │        ▼
         ┌─────────┐   │   ┌─────────┐
         │ PAUSED  │   │   │ FAILED  │
         └────┬────┘   │   └─────────┘
              │        │
        resume()       │ complete()
              │        │
              └────────┼────────┘
                       │
                       ▼
                  ┌──────────┐
                  │COMPLETED │
                  └──────────┘
```

---

## Adding a New Scraper

Follow these steps to add a new scraper type to the GUI API:

### Step 1: Create the Bridge Actor

Create `src/gui/infrastructure/actors/{name}_gui_bridge.py`:

```python
from dataclasses import dataclass
from typing import Optional, Dict, Any
from src.infrastructure.actors.base import BaseActor

@dataclass
class MyScraperConfig:
    """Configuration for scraping job."""
    filter_a: Optional[str] = None
    filter_b: Optional[str] = None
    max_results: int = 100

class MyScraperGuiBridgeActor(BaseActor):
    """Bridge actor for MyScraperName."""

    async def start_job(self, config: MyScraperConfig) -> str:
        """Start a new scraping job."""
        pass

    async def pause_job(self) -> None:
        """Pause the current job."""
        pass

    async def resume_job(self) -> None:
        """Resume paused job."""
        pass

    async def stop_job(self) -> None:
        """Cancel current job."""
        pass
```

### Step 2: Export from `__init__.py`

Add to `src/gui/infrastructure/actors/__init__.py`:

```python
from .my_scraper_gui_bridge import (
    MyScraperGuiBridgeActor,
    MyScraperConfig,
)
```

### Step 3: Add Request/Response Models

Add to `src/gui/web/api.py`:

```python
class MyScraperStartRequest(BaseModel):
    filter_a: Optional[str] = None
    filter_b: Optional[str] = None
    max_results: int = Field(default=100, ge=1, le=1000)

class MyScraperStatusResponse(BaseModel):
    status: str
    job_id: Optional[str] = None
```

### Step 4: Initialize Bridge in ScraperAPI

```python
class ScraperAPI:
    def __init__(self, ...):
        ...
        self._my_scraper_bridge: Optional[MyScraperGuiBridgeActor] = None
        self._my_scraper_logs: List[dict] = []

    async def startup(self):
        ...
        self._my_scraper_bridge = MyScraperGuiBridgeActor(
            event_handler=self._handle_my_scraper_event
        )
        await self._my_scraper_bridge.start()
```

### Step 5: Add API Endpoints

```python
@app.get("/api/my_scraper/status", tags=["MyScraper"])
async def get_my_scraper_status():
    ...

@app.post("/api/my_scraper/start", tags=["MyScraper"])
async def start_my_scraper(request: MyScraperStartRequest):
    ...

# Add pause, resume, cancel, logs endpoints...
```

### Step 6: Add Tests

Create `tests/gui/web/test_my_scraper_api.py`:

```python
class TestMyScraperEndpoints:
    async def test_status_idle(self, client):
        ...
    async def test_start_success(self, client):
        ...
```

---

## Configuration

### Server Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `SCRAPER_HTTP_PORT` | `8081` | HTTP port |
| `SCRAPER_HTTPS_PORT` | `8444` | HTTPS port |

### Running the Server

```bash
# Development
python -m src.gui.web.api

# Production
uvicorn src.gui.web.api:create_app --factory --host 0.0.0.0 --port 8081
```

---

## File Structure

```
src/gui/
├── __init__.py
├── main.py                    # TUI entry point
├── launcher.py                # GUI launcher
├── domain/
│   ├── entities.py           # ScraperJob, JobProgress, etc.
│   ├── value_objects.py      # TargetSource, OutputFormat, etc.
│   └── events.py             # Domain events
├── application/
│   ├── services.py           # ScraperService, ConfigService
│   └── use_cases.py          # Business logic
├── infrastructure/
│   └── actors/
│       ├── gui_bridge.py     # Base bridge actor
│       ├── scjn_bridge.py    # SCJN-specific bridge
│       ├── bjv_bridge.py     # BJV-specific bridge
│       ├── cas_gui_bridge.py # CAS-specific bridge
│       ├── gui_controller.py # Main controller
│       └── gui_state.py      # State management
├── presentation/
│   ├── presenters.py         # Data formatters
│   └── views/                # View components
├── tui/
│   └── app.py               # Terminal UI (Textual)
└── web/
    ├── api.py               # FastAPI application
    └── auth.py              # JWT authentication
```

---

## See Also

- [SCJN Scraper Documentation](../SCJN_SITE_ANALYSIS.md)
- [Implementation Plan](../IMPLEMENTATION_PLAN.md)
- [Codebase Analysis](../CODEBASE_ANALYSIS.md)
