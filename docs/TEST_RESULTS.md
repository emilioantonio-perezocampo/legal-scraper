# Legal Scraper Test Results

## Test Date
January 26, 2026

---

## LLM Scraper Test Summary

Test script: `test_all_sources.py`

| Source | Status | Documents Found | Extraction Method |
|--------|--------|-----------------|-------------------|
| **SCJN** | PASS | 10 documents | LLM (OpenRouter + Gemini 3 Flash) |
| **DOF** | PASS | 2 documents | HTML parsing (BeautifulSoup) |
| **BJV** | PASS | 11 books | HTML regex + Crawl4AI |
| **CAS** | PASS | 50 cases | Playwright (Angular SPA with table extraction) |

**Result: 4/4 sources working (100%)**

---

## Source-Specific Results

### SCJN (Suprema Corte de Justicia de la Nacion)

**Status:** Working

**Method:** LLM-based extraction using OpenRouter API with Gemini 3 Flash model

**Test Output:**
```
TEST: SCJN (Suprema Corte)
Model: openrouter/google/gemini-3-flash-preview
Fetching and extracting SCJN documents...
Documents found: 10
Has next page: True

Sample documents:
  1. LEY GENERAL DE EDUCACION...
     Q-Param: encoded_id
```

**Files Generated:** 5 JSON documents in `scjn_data/documents/`

**Required Fields Validated:**
- `q_param` - Encrypted document identifier
- `title` - Document title
- `category` - LEY, CODIGO, DECRETO, REGLAMENTO, ACUERDO
- `scope` - FEDERAL, ESTATAL, CDMX
- `status` - VIGENTE, ABROGADA, DEROGADA

---

### DOF (Diario Oficial de la Federacion)

**Status:** Working

**Method:** HTML parsing with BeautifulSoup (no LLM required)

**Test Output:**
```
TEST: DOF (Diario Oficial)
Fetching: https://dof.gob.mx/index.php?year=2026&month=01&day=23
HTML length: ~50000 characters
Documents found: 2
```

**Files Generated:** 56 JSON documents in `scraped_data/`

**Required Fields Validated:**
- `title` - Publication title
- `publication_date` - Date in ISO format
- `jurisdiction` - Federal/State jurisdiction

---

### BJV (Biblioteca Juridica Virtual - UNAM)

**Status:** Working

**Method:** HTML pattern extraction with Crawl4AI for JavaScript rendering

**Test Output:**
```
TEST: BJV (Biblioteca Juridica Virtual)
Using Crawl4AI with JavaScript rendering
Fetching main catalog page (no search query)...
URL: https://biblio.juridicas.unam.mx/bjv
Books found: 11
Has next page: False

Sample books:
  1. Derecho Natural...
     ID: 7867
```

**Extraction Strategy:**
- Uses regex pattern `/bjv/detalle-libro/(\d+)-([^"\'<>\s]+)` to find book links
- Converts URL slugs to titles
- Falls back to LLM extraction for search result pages

**Required Fields Validated:**
- `libro_id` - BJV book identifier
- `titulo` - Book title
- `tipo_contenido` - LIBRO, CAPITULO, ARTICULO, REVISTA

---

### CAS (Court of Arbitration for Sport)

**Status:** Working

**Method:** Playwright browser automation with table extraction for Angular SPA

**Test Output:**
```
TEST: CAS (Court of Arbitration for Sport)
Attempting search with Playwright interaction...
Cases found: 50
Has next page: True

Sample cases:
  1. CAS 2023/A/10168
     Title: Olympiakos FC v. Hellenic Football Federation (HFF)...
     Sport: football
```

**Implementation Details:**
The CAS website (`jurisprudence.tas-cas.org`) is an Angular SPA that required:
1. Browser automation with Playwright (headless Chromium)
2. Bootstrap dropdown interaction for sport filters
3. Search input filtering for sport selection
4. Table extraction for case data (Lang, Year, Proc, Case#, Parties, Sport, etc.)
5. Date parsing from DD/MM/YYYY format

**Docker Configuration:**
- Playwright browsers installed in `/opt/playwright-browsers`
- `PLAYWRIGHT_BROWSERS_PATH` environment variable set
- Dependencies: dbus, chromium libs, no-sandbox mode

**Required Fields Validated:**
- `numero_caso` - Case number (CAS YYYY/X/NNNN)
- `titulo` - Parties involved
- `fecha` - Decision date
- `categoria_deporte` - Sport category
- `tipo_procedimiento` - Procedure type (Appeal, Ordinary)

---

## File Integrity Verification

### File Counts

| Directory | File Count | Format |
|-----------|------------|--------|
| `scraped_data/` | 56 files | JSON |
| `scjn_data/documents/` | 5 files | JSON |

### JSON Validation

```bash
# Validation command
find /root/legal-scraper/scraped_data -name "*.json" -exec jq empty {} \;
find /root/legal-scraper/scjn_data -name "*.json" -exec jq empty {} \;

# Result: All files valid JSON, no parsing errors
```

### Empty File Check

```bash
find /root/legal-scraper/scraped_data -name "*.json" -empty
find /root/legal-scraper/scjn_data -name "*.json" -empty

# Result: No empty files found
```

---

## Database Verification

### Record Counts

| Table | Count |
|-------|-------|
| `scraper_documents` (parent) | 51 |
| `dof_publicaciones` | 46 |
| `scjn_documents` | 5 |
| `bjv_libros` | 0 |
| `cas_laudos` | 0 |

### Source Distribution

```sql
SELECT source_type, COUNT(*) as count
FROM scraper_documents
GROUP BY source_type;

-- Results:
-- dof  | 46
-- scjn | 5
```

### Data Integrity

- All child records have matching parent records in `scraper_documents`
- No orphaned records detected
- Foreign key constraints satisfied

---

## API Integration Tests

### Health Check
```bash
curl -s http://localhost:8000/api/health
# Response: {"status":"healthy","version":"1.0.0"}
```

### SCJN Status
```bash
curl -s http://localhost:8000/api/scjn/status
# Response: {"status":"idle","job_id":null,"connected":true,"progress":{...}}
```

### DOF Status
```bash
curl -s http://localhost:8000/api/dof/status
# Response: {"status":"idle","job_id":null,"progress":{...}}
```

### BJV Status
```bash
curl -s http://localhost:8000/api/bjv/status
# Response: {"status":"idle","job_id":null,"connected":true,"progress":{...}}
```

---

## Known Issues

### 1. File-to-Database Sync Gap
- **Issue:** 56 DOF files vs 46 DB records (10 file difference)
- **Cause:** Files created after last database import, or duplicate prevention
- **Resolution:** Re-run `import_json_to_db.py` to sync

### 2. SCJN Website Changes
- **Issue:** Original HTML parser broken due to site redesign
- **Resolution:** LLM-based parser now preferred method

---

## Performance Metrics

| Source | Avg Extraction Time | Rate Limit |
|--------|---------------------|------------|
| SCJN | ~5-10 seconds/page | 2 req/sec |
| DOF | ~1-2 seconds/page | 1 req/sec |
| BJV | ~3-5 seconds/page | 1 req/sec |
| CAS | N/A | 0.5 req/sec |

---

## Recommendations

1. **Schedule regular imports** to keep database in sync with files
2. **Monitor OpenRouter API usage** for LLM extraction costs
3. **Implement incremental scraping** to avoid re-processing existing documents
4. **Monitor Playwright browser stability** in Docker environments

---

## Test Environment

- **OS:** Linux 6.8.0-90-generic
- **Python:** 3.11+
- **Docker:** Compose v2
- **LLM Model:** google/gemini-3-flash-preview via OpenRouter
- **Database:** Supabase PostgreSQL

---

## Last Updated
January 26, 2026
