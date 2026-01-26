# Legal Scraper Architecture

## Overview

Multi-source legal document scraper using Domain-Driven Design (DDD), Actor Model architecture, and FastAPI. Extracts documents from Mexican and international legal sources using LLM-based extraction and HTML parsing.

---

## Data Sources

| Source | URL | Method | PDF Download | Status |
|--------|-----|--------|--------------|--------|
| **SCJN** | legislacion.scjn.gob.mx | LLM extraction (OpenRouter + Gemini 3 Flash) | `AbrirDocReforma.aspx?q={q_param}` | ✅ Working |
| **DOF** | dof.gob.mx | HTML parsing (BeautifulSoup) | Detail page PDF extraction | ✅ Working |
| **BJV** | biblio.juridicas.unam.mx | Crawl4AI + HTML regex | `url_pdf` field | ✅ Working |
| **CAS** | jurisprudence.tas-cas.org | Playwright browser (Angular SPA) | `URLLaudo` value object | ✅ Working |

---

## Architecture Layers

```
┌─────────────────────────────────────────────────────────────────┐
│                         API Layer                                │
│  FastAPI REST endpoints (port 8000)                              │
│  - /api/health, /api/dof/*, /api/scjn/*, /api/bjv/*, /api/cas/* │
└─────────────────────────────────────────────────────────────────┘
                                │
┌─────────────────────────────────────────────────────────────────┐
│                     GUI Bridge Actors                            │
│  SCJNGuiBridgeActor, BJVGuiBridgeActor, DofGuiBridgeActor, etc. │
└─────────────────────────────────────────────────────────────────┘
                                │
┌─────────────────────────────────────────────────────────────────┐
│                     Discovery Actors                             │
│  SCJNDiscoveryActor, BJVDiscoveryActor, CASDiscoveryActor,      │
│  DofDiscoveryActor                                               │
└─────────────────────────────────────────────────────────────────┘
                                │
┌─────────────────────────────────────────────────────────────────┐
│                 Infrastructure Adapters                          │
│  ┌─────────────┐  ┌────────────┐  ┌─────────────┐               │
│  │ LLM Parsers │  │ HTML Parse │  │ Browser     │               │
│  │ - SCJN LLM  │  │ - DOF HTML │  │ - CAS Playw │               │
│  │ - BJV LLM   │  │ - BJV regex│  │ - SCJN brow │               │
│  │ - Crawl4AI  │  │            │  │             │               │
│  └─────────────┘  └────────────┘  └─────────────┘               │
└─────────────────────────────────────────────────────────────────┘
                                │
┌─────────────────────────────────────────────────────────────────┐
│                   Persistence Layer                              │
│  - Supabase PostgreSQL (primary)                                 │
│  - JSON files (dual-write mode)                                  │
│  - Supabase Storage (PDFs)                                       │
└─────────────────────────────────────────────────────────────────┘
```

---

## Domain Model

### SCJN Documents
```
SCJNDocument
├── q_param (encrypted ID)
├── title
├── category (LEY, CODIGO, DECRETO, REGLAMENTO, ACUERDO)
├── scope (FEDERAL, ESTATAL, CDMX)
├── status (VIGENTE, ABROGADA, DEROGADA)
├── publication_date
├── articles[]
└── reforms[]
```

### DOF Publications
```
DofPublication
├── external_id
├── title
├── publication_date
├── jurisdiction
├── section
└── articles[]
```

### BJV Books
```
BJVLibro
├── libro_id
├── titulo
├── tipo_contenido (LIBRO, CAPITULO, ARTICULO)
├── autores_texto
├── anio
└── area
```

### CAS Awards
```
CASLaudo
├── numero_caso (CAS YYYY/A/NNNN)
├── titulo
├── fecha
├── categoria_deporte
├── tipo_procedimiento
└── partes
```

---

## Database Schema

### Tables

| Table | Purpose | Records |
|-------|---------|---------|
| `scraper_documents` | Parent table (all sources) | ~51 |
| `scjn_documents` | Supreme Court legislation | 5 |
| `dof_publicaciones` | Official Gazette publications | 46 |
| `bjv_libros` | UNAM library books | 0 |
| `cas_laudos` | Sports arbitration awards | 0 |
| `scraper_chunks` | Text chunks with embeddings | 0 |

### Schema Diagram
```sql
scraper_documents (parent)
├── id (UUID, PK)
├── source_type (scjn|dof|bjv|cas)
├── external_id
├── title
├── publication_date
└── tenant_id (optional)

scjn_documents (child)
├── id (UUID, FK → scraper_documents)
├── q_param
├── category
├── scope
├── status
└── source_url

dof_publicaciones (child)
├── id (UUID, FK → scraper_documents)
├── dof_date
├── section
├── jurisdiction
└── full_text
```

---

## PDF/Document Download

The scraper supports downloading PDF and Word documents from all sources.

### CLI Usage
```bash
# Download PDFs along with metadata
python -m src.gui.infrastructure.scraper_cli run scjn --use-crawl4ai --download-pdfs

# Download and upload to Supabase Storage
python -m src.gui.infrastructure.scraper_cli run bjv --use-crawl4ai --download-pdfs --upload-to-storage
```

### PDF URL Patterns by Source

| Source | URL Pattern | Notes |
|--------|-------------|-------|
| SCJN | `AbrirDocReforma.aspx?q={q_param}` | Uses encrypted q_param |
| DOF | Extracted from `nota_detalle.php` | PDF link in detail page |
| BJV | `url_pdf` field from book detail | Direct PDF URLs |
| CAS | `URLLaudo` value object | Award PDF links |

### Temporal Workflow Steps
1. **Extract documents** - Get metadata and PDF URLs
2. **Download PDFs** (optional) - Download to local directory
3. **Persist to Supabase** - Save metadata to database
4. **Upload to Storage** (optional) - Upload PDFs to Supabase Storage
5. **Trigger embedding** - Start embedding workflow

---

## External Services

| Service | Purpose | Port |
|---------|---------|------|
| Supabase PostgreSQL | Document persistence | 5432 |
| Supabase Storage | PDF/HTML file storage | - |
| Temporal | Workflow orchestration | 7233 |
| OpenRouter | LLM API (Gemini 3 Flash) | - |
| Crawl4AI | JavaScript rendering + LLM | - |

---

## Key Files

| File | Purpose |
|------|---------|
| `src/gui/web/api.py` | FastAPI endpoints |
| `src/infrastructure/adapters/scjn_llm_parser.py` | SCJN LLM extraction |
| `src/infrastructure/adapters/bjv_llm_parser.py` | BJV HTML + LLM parser |
| `src/infrastructure/adapters/cas_llm_parser.py` | CAS Playwright parser |
| `src/infrastructure/adapters/dof_index_parser.py` | DOF HTML parser |
| `src/infrastructure/adapters/crawl4ai_adapter.py` | Unified Crawl4AI wrapper |
| `src/infrastructure/adapters/supabase_repository.py` | Database persistence |
| `src/infrastructure/adapters/supabase_storage.py` | PDF/file storage |
| `src/gui/infrastructure/crawl4ai_activities.py` | Temporal activities (extraction, download) |
| `src/gui/infrastructure/crawl4ai_workflow.py` | Temporal workflow orchestration |
| `src/gui/infrastructure/scraper_cli.py` | CLI for running scrapers |
| `test_all_sources.py` | Source validation tests |

---

## Docker Services

| Service | Container | Purpose |
|---------|-----------|---------|
| API | legal-scraper-api | FastAPI REST API |
| Worker | legal-scraper-worker | Temporal worker |
| UI | legal-scraper-ui | Reflex dashboard |
| Caddy | caddy | Reverse proxy |

---

## Configuration

### Environment Variables

```bash
# LLM
OPENROUTER_API_KEY=sk-or-v1-...

# Supabase
SUPABASE_URL=http://kong:8000
SUPABASE_DB_HOST=legaltracking-dev-db-1
SUPABASE_DB_PORT=5432
SUPABASE_DB_PASSWORD=...

# Feature Flags
ENABLE_SUPABASE_PERSISTENCE=true
ENABLE_DUAL_WRITE=true

# Authentication
SCRAPER_AUTH_DISABLED=true  # For internal access
```

---

## Last Updated
January 26, 2026
