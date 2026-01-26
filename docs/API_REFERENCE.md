# Legal Scraper API Reference

## Base URL

```
http://localhost:8000
```

For external access via Caddy:
```
http://<hostname>/api
```

---

## Authentication

Authentication is optional and can be disabled with `SCRAPER_AUTH_DISABLED=true`.

### Login
```http
POST /api/auth/login
Content-Type: application/json

{
  "username": "admin",
  "password": "your-password"
}
```

**Response:**
```json
{
  "success": true,
  "access_token": "eyJ...",
  "token_type": "bearer"
}
```

### Using Token
```http
Authorization: Bearer <access_token>
```

---

## Health Check

### Check API Health
```http
GET /api/health
```

**Response:**
```json
{
  "status": "healthy",
  "version": "1.0.0"
}
```

---

## DOF (Diario Oficial de la Federación)

### Start DOF Scraper
```http
POST /api/dof/start
Content-Type: application/json

{
  "mode": "today",
  "output_directory": "dof_data"
}
```

**Mode Options:**
- `today` - Scrape today's publications
- `range` - Scrape date range (requires `start_date`, `end_date`)

**Response:**
```json
{
  "success": true,
  "job_id": "dof-gui-abc123",
  "message": "DOF scraper started"
}
```

### Get DOF Status
```http
GET /api/dof/status
```

**Response:**
```json
{
  "status": "running",
  "job_id": "dof-gui-abc123",
  "progress": {
    "total_documents": 10,
    "processed_documents": 5
  }
}
```

### Get DOF Logs
```http
GET /api/dof/logs
```

### Pause/Resume/Cancel
```http
POST /api/dof/pause
POST /api/dof/resume
POST /api/dof/cancel
```

---

## SCJN (Suprema Corte de Justicia)

### Start SCJN Scraper
```http
POST /api/scjn/start
Content-Type: application/json

{
  "category": null,
  "scope": null,
  "max_results": 10,
  "output_directory": "scjn_data"
}
```

**Filter Options:**
- `category`: LEY, CODIGO, DECRETO, REGLAMENTO, ACUERDO
- `scope`: FEDERAL, ESTATAL, CDMX

**Response:**
```json
{
  "success": true,
  "job_id": "scjn-gui-xyz789"
}
```

### Get SCJN Status
```http
GET /api/scjn/status
```

**Response:**
```json
{
  "status": "idle",
  "job_id": null,
  "connected": true,
  "progress": {
    "discovered_count": 0,
    "downloaded_count": 0,
    "pending_count": 0,
    "active_downloads": 0,
    "error_count": 0,
    "state": "idle"
  }
}
```

### Get SCJN Logs
```http
GET /api/scjn/logs
```

### Pause/Resume/Cancel
```http
POST /api/scjn/pause
POST /api/scjn/resume
POST /api/scjn/cancel
```

---

## BJV (Biblioteca Jurídica Virtual)

### Start BJV Scraper
```http
POST /api/bjv/start
Content-Type: application/json

{
  "termino_busqueda": "derecho civil",
  "area_derecho": null,
  "max_resultados": 10
}
```

**Response:**
```json
{
  "success": true,
  "job_id": "bjv-gui-def456"
}
```

### Get BJV Status
```http
GET /api/bjv/status
```

**Response:**
```json
{
  "status": "idle",
  "job_id": null,
  "connected": true,
  "progress": {
    "libros_descubiertos": 0,
    "libros_descargados": 0,
    "libros_pendientes": 0,
    "descargas_activas": 0,
    "errores": 0,
    "estado": "IDLE"
  }
}
```

### Get BJV Logs
```http
GET /api/bjv/logs
```

---

## CAS (Court of Arbitration for Sport)

### Start CAS Scraper
```http
POST /api/cas/start
Content-Type: application/json

{
  "sport": "Football",
  "max_results": 10
}
```

**Note:** CAS requires Playwright browser support. Limited functionality without browser adapter.

**Response:**
```json
{
  "success": true,
  "job_id": "cas-gui-ghi789"
}
```

### Get CAS Status
```http
GET /api/cas/status
```

### Get CAS Filters
```http
GET /api/cas/filters
```

Returns available filter options (sports, procedures, categories).

---

## Server-Sent Events (SSE)

### Real-time Progress Updates
```http
GET /api/sse/progress?source=scjn
```

Streams progress updates in SSE format:
```
event: progress
data: {"discovered": 5, "downloaded": 3, "errors": 0}

event: progress
data: {"discovered": 10, "downloaded": 8, "errors": 0}
```

---

## Error Responses

### 400 Bad Request
```json
{
  "detail": "Invalid request parameters"
}
```

### 401 Unauthorized
```json
{
  "detail": "Not authenticated"
}
```

### 500 Internal Server Error
```json
{
  "detail": "Internal server error",
  "error": "Error message details"
}
```

---

## Rate Limiting

Built-in rate limiting per source:
- DOF: 1 request/second
- SCJN: 2 requests/second
- BJV: 1 request/second
- CAS: 0.5 requests/second

---

## Example: Complete Workflow

```bash
# 1. Check health
curl -s http://localhost:8000/api/health

# 2. Start SCJN scraper
curl -X POST http://localhost:8000/api/scjn/start \
  -H "Content-Type: application/json" \
  -d '{"max_results": 10}'

# 3. Poll status until complete
curl -s http://localhost:8000/api/scjn/status

# 4. Check logs if needed
curl -s http://localhost:8000/api/scjn/logs
```

---

## Last Updated
January 26, 2026
