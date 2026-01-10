# SCJN Website Analysis - legislacion.scjn.gob.mx

## Executive Summary

This document provides a technical analysis of the SCJN (Suprema Corte de Justicia de la Nación) legislation database at [legislacion.scjn.gob.mx](https://legislacion.scjn.gob.mx/Buscador/Paginas/Buscar.aspx) for the purpose of building a web scraper extension.

---

## 1. Website Overview

### 1.1 Authority

**Operator**: Centro de Documentación y Análisis, Archivos y Compilación de Leyes (CDAACL)
**Contact**: cdaacl@mail.scjn.gob.mx
**Phone**: (55) 4113-1100 ext. 4109, 1262

### 1.2 Collection Statistics

| Metric | Count |
|--------|-------|
| Total Legal Instruments | 60,000+ |
| Online with Full Text | 11,000+ |
| International Treaties | 2,800+ |
| Official Publications | 39,000+ (dating from 1871) |

---

## 2. Document Categories (Tipos de Ordenamientos)

### 2.1 Federal Scope

| Category | Spanish Name | Description |
|----------|--------------|-------------|
| Constitution | Constitución Política | Mexican Constitution with all reforms |
| Federal Laws | Leyes Federales | Federal legislation |
| Federal Codes | Códigos Federales | Procedural and substantive codes |
| Decrees | Decretos | Executive decrees |
| Regulations | Reglamentos | Administrative regulations |
| Agreements | Acuerdos | Official agreements |

### 2.2 State Scope

| Category | Description |
|----------|-------------|
| State Constitutions | Constituciones Estatales |
| State Laws | Leyes Estatales |
| State Codes | Códigos Estatales |

### 2.3 International Scope

| Category | Description |
|----------|-------------|
| International Treaties | Tratados Internacionales (2,800+) |
| International Agreements | Convenios Internacionales |

### 2.4 Document Status (Vigencia)

| Status | Spanish | Description |
|--------|---------|-------------|
| Active | VIGENTE | Currently in effect |
| Abrogated | ABROGADA | Superseded by new law |
| Derogated | DEROGADA | Partially or fully repealed |
| Substituted | SUSTITUIDA | Replaced by another |
| Extinct | EXTINTA | No longer applicable |

---

## 3. Technical Architecture

### 3.1 Technology Stack

| Component | Technology |
|-----------|------------|
| Framework | ASP.NET Web Forms |
| UI Components | DevExpress Controls |
| Client-Side | ASP.NET AJAX, jQuery |
| Grid System | ASPxClientGridView |
| Calendar | DevExpress Calendar |
| Analytics | Google Analytics (UA-126749635-1) |

### 3.2 URL Structure

**Base URL**: `https://legislacion.scjn.gob.mx/Buscador/Paginas/`

| Page | Purpose | URL Pattern |
|------|---------|-------------|
| Search | Main search interface | `Buscar.aspx` |
| Document Detail | Reform listing for a document | `wfOrdenamientoDetalle.aspx?q={encoded}` |
| Article View | Quick article lookup | `wfArticuladoFast.aspx?q={encoded}` |
| Extract View | Document summary/extract | `wfExtracto.aspx?q={encoded}` |
| PDF Document | Reform document PDF | `AbrirDocReforma.aspx?q={encoded}` |
| Article PDF | Full article text | `AbrirDocArticulo.aspx?q={encoded}` |
| Legislative Process | Amendment history | `wfProcesoLegislativo.aspx?q={encoded}` |
| Complete Process | Full legislative record | `wfProcesoLegislativoCompleto.aspx?q={encoded}` |
| Thematic Index | Subject matter index | `wfIndiceTematico.aspx` |
| Recent Reforms | Last 50 reforms | `wfReformas Resultados.aspx` |
| Update Dates | Last update info | `wfFechaDeActualizacion.aspx` |

### 3.3 Query Parameter Encoding

All document URLs use a `q=` parameter containing **Base64-encoded** encrypted identifiers.

**Example**:
```
q=b/EcoMjefuFeB6DOaNOimE2VCMjfIsnCECSIArvq0l5HCFlXkN9QRimN4pk8I165
```

**Note**: The encoded string appears to contain internal IDs like:
- `IdLey` (Law ID)
- `IdReforma` (Reform ID)
- `IdProc` (Process ID)

---

## 4. Search Interface Analysis

### 4.1 Search Form Elements

| Element | Type | Description |
|---------|------|-------------|
| Text Search | Input | Supports AND, OR, NOT, NEAR operators |
| Scope (Ámbito) | Dropdown | Federal, State, CDMX, International, Foreign |
| State | Dropdown | 31 states + national |
| Document Type | Dropdown | ~50+ types |
| Status | Dropdown | Active, Abrogated, etc. |
| Subject Matter | Dropdown | ~15 categories |
| Date Range | DatePicker | Start/End with DevExpress calendar |
| Results Per Page | Dropdown | 10, 20, 50 |

### 4.2 Search Results Structure

Results display in a grid with:
- Publication date
- Expedition date
- Category (DECRETO, LEY, etc.)
- Publication number
- Status (VIGENTE, etc.)
- Action links (Extract, PDF, Articles, Process)

---

## 5. Dynamic Content & AJAX

### 5.1 Page Request Manager

```javascript
Sys.WebForms.PageRequestManager  // ASP.NET AJAX
```

### 5.2 Grid Callbacks

```javascript
ASPxClientGridView  // DevExpress grid control
__doPostBack('ctl00$MainContentPlaceHolder$gridReformas$cell[X]_3$LinkButton[Y]','')
```

### 5.3 Content Loading

- Modal progress indicator during requests
- Background blur (opacity 0.6)
- `GetParteArticulo` function for lazy article loading

---

## 6. Scraping Challenges

### 6.1 Technical Challenges

| Challenge | Severity | Mitigation Strategy |
|-----------|----------|---------------------|
| ASP.NET ViewState | High | Maintain session, parse `__VIEWSTATE` |
| Encrypted URLs | High | Discover via navigation, cache mappings |
| AJAX Callbacks | Medium | Use browser automation (Playwright/Selenium) |
| Rate Limiting | Medium | Implement delays, respect robots.txt |
| No Public API | High | Web scraping required |

### 6.2 robots.txt

**Status**: Not found (404)
**Interpretation**: No explicit restrictions, but ethical scraping principles apply.

### 6.3 Rate Limiting Recommendations

| Scenario | Delay |
|----------|-------|
| Between pages | 2-3 seconds |
| Between documents | 1-2 seconds |
| Daily limit | ~1000 documents/day recommended |

---

## 7. Document Access Flow

### 7.1 Discovery Flow

```
┌─────────────────┐
│   Buscar.aspx   │  (Search Page)
│ Enter criteria  │
└────────┬────────┘
         │ Search
         ▼
┌─────────────────┐
│  Search Results │  (Grid of documents)
│  with q= links  │
└────────┬────────┘
         │ Click document
         ▼
┌─────────────────────────┐
│ wfOrdenamientoDetalle   │  (Document detail)
│ Lists all reforms/dates │
└────────┬────────────────┘
         │ Click reform
         ▼
┌─────────────────────────┐
│   wfExtracto.aspx       │  (Extract view)
│ OR AbrirDocReforma.aspx │  (PDF download)
└─────────────────────────┘
```

### 7.2 Direct Access (if q= known)

```python
# Once q= parameter is discovered, direct access is possible
doc_url = f"https://legislacion.scjn.gob.mx/Buscador/Paginas/wfOrdenamientoDetalle.aspx?q={encoded_id}"
pdf_url = f"https://legislacion.scjn.gob.mx/Buscador/Paginas/AbrirDocReforma.aspx?q={encoded_id}"
```

---

## 8. Data Extraction Points

### 8.1 Metadata Fields

| Field | Location | Extraction Method |
|-------|----------|-------------------|
| Title | Header section | CSS selector / regex |
| Publication Date | Grid cell / meta | Parse date format (dd/mm/yyyy) |
| Expedition Date | Grid cell | Parse date format |
| Category | Grid cell | Text extraction |
| Status (Vigencia) | Grid cell / badge | Text extraction |
| IdLey | Hidden field / URL | Parse from page |
| IdReforma | Hidden field / URL | Parse from page |

### 8.2 Content Fields

| Field | Source | Notes |
|-------|--------|-------|
| Full Text | wfExtracto.aspx | HTML content in div |
| PDF Image | AbrirDocReforma.aspx | Binary PDF download |
| Articles | wfArticuladoFast.aspx | Structured article list |
| Legislative History | wfProcesoLegislativo.aspx | Amendment chronology |

---

## 9. PDF Handling

### 9.1 PDF Sources

1. **AbrirDocReforma.aspx** - Official gazette image PDFs
2. **AbrirDocArticulo.aspx** - Article-specific PDFs

### 9.2 PDF Characteristics

| Aspect | Expected Characteristics |
|--------|--------------------------|
| Format | Scanned images + OCR text |
| Size | 100KB - 10MB typical |
| Quality | Variable (historical docs may be poor) |
| Text Layer | May or may not have OCR |

### 9.3 PDF Processing Requirements

1. Download binary content
2. Extract text (PyPDF2, pdfplumber)
3. OCR if needed (Tesseract)
4. Clean and normalize text
5. Chunk for embeddings

---

## 10. Recommended Scraping Architecture

### 10.1 Actor Design

```
┌─────────────────────┐
│SCJNDiscoveryActor   │  (Scout - discovers document URLs)
│ - Browse categories │
│ - Parse search      │
│ - Extract q= params │
└──────────┬──────────┘
           │ tell({url, q_param, metadata})
           ▼
┌─────────────────────┐
│ SCJNScraperActor    │  (Worker - fetches documents)
│ - Fetch HTML pages  │
│ - Download PDFs     │
│ - Extract text      │
└──────────┬──────────┘
           │ tell(("PROCESS_PDF", pdf_bytes))
           ▼
┌─────────────────────┐
│ PDFProcessorActor   │  (PDF Handler)
│ - Extract text      │
│ - OCR if needed     │
│ - Chunk content     │
└──────────┬──────────┘
           │ tell(("EMBED", chunks))
           ▼
┌─────────────────────┐
│ EmbeddingActor      │  (Vector Generator)
│ - Generate vectors  │
│ - Store embeddings  │
└──────────┬──────────┘
           │ tell(("SAVE", document))
           ▼
┌─────────────────────┐
│ PersistenceActor    │  (Storage - existing)
│ - Save JSON/PDF     │
│ - Store vectors     │
└─────────────────────┘
```

### 10.2 Browser Automation Need

Due to ASP.NET ViewState and AJAX callbacks, **browser automation is recommended** for:
- Initial search navigation
- Discovering q= parameters
- Handling dynamic content

**Recommended Tools**: Playwright (async support) or Selenium

---

## 11. Data Model Proposal

### 11.1 SCJN-Specific Entities

```python
@dataclass(frozen=True)
class SCJNDocument:
    """Aggregate root for SCJN legislation documents."""
    id: str
    q_param: str  # Encrypted URL parameter
    title: str
    category: DocumentCategory  # LEY, CODIGO, DECRETO, etc.
    scope: DocumentScope  # FEDERAL, STATE, INTERNATIONAL
    status: DocumentStatus  # VIGENTE, ABROGADA, etc.
    publication_date: Optional[date]
    expedition_date: Optional[date]
    state: Optional[str]  # For state-level documents
    subject_matters: tuple[str, ...]  # ADMINISTRATIVO, CIVIL, etc.
    reforms: tuple['SCJNReform', ...]

@dataclass(frozen=True)
class SCJNReform:
    """Represents a specific reform/amendment."""
    id: str
    q_param: str
    publication_date: date
    publication_number: str
    text_content: Optional[str]
    pdf_path: Optional[str]

@dataclass(frozen=True)
class SCJNArticle:
    """Individual article within a document."""
    number: str  # "1", "2 Bis", "Transitorio Primero"
    content: str
    reform_history: tuple[str, ...]  # References to reform IDs
```

### 11.2 Enumerations

```python
class DocumentCategory(Enum):
    CONSTITUCION = "CONSTITUCION"
    LEY = "LEY"
    CODIGO = "CODIGO"
    DECRETO = "DECRETO"
    REGLAMENTO = "REGLAMENTO"
    ACUERDO = "ACUERDO"
    TRATADO = "TRATADO"

class DocumentScope(Enum):
    FEDERAL = "FEDERAL"
    STATE = "ESTATAL"
    CDMX = "CDMX"
    INTERNATIONAL = "INTERNACIONAL"

class DocumentStatus(Enum):
    VIGENTE = "VIGENTE"
    ABROGADA = "ABROGADA"
    DEROGADA = "DEROGADA"
    SUSTITUIDA = "SUSTITUIDA"
    EXTINTA = "EXTINTA"
```

---

## 12. Risk Assessment

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| Website structure changes | Medium | High | Version detection, flexible parsers |
| IP blocking | Low | High | Rate limiting, proxy rotation |
| Encrypted URL changes | Low | Medium | Re-discovery mechanism |
| PDF OCR quality issues | Medium | Medium | Multiple extraction strategies |
| Session timeout | Medium | Low | Session refresh, cookies |

---

## 13. Ethical Considerations

1. **Public Information**: All documents are public legal records
2. **Educational Purpose**: Scraping for legal research is legitimate
3. **Rate Limiting**: Respect server capacity with delays
4. **Attribution**: Maintain source attribution in stored data
5. **No Authentication Bypass**: Only access public pages

---

## 14. Next Steps

1. **Phase 3**: Create detailed implementation plan
2. **Prototype**: Build minimal discovery actor with Playwright
3. **Test**: Verify PDF download and extraction
4. **Iterate**: Refine based on real-world responses

---

## Sources

- [SCJN Legislation Search](https://legislacion.scjn.gob.mx/Buscador/Paginas/Buscar.aspx)
- [SCJN Normativa Nacional e Internacional](https://www.scjn.gob.mx/normativa-nacional-internacional)
- [Centro de Documentación y Análisis](https://www.sitios.scjn.gob.mx/centrodedocumentacion/contenidos_normativa/Normativa%20Nacional%20e%20Internacional)
- [Servicio de Compilación de Leyes](https://www.scjn.gob.mx/normativa-nacional-internacional/servicio-compilacion-leyes)

---

*Document generated during Phase 2 of SCJN Legislation Scraper Extension analysis.*
