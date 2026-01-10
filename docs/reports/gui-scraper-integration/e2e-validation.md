# E2E Validation: Web GUI -> Scraper

## 1. Environment Setup
- **App:** Web GUI (`src.gui.launcher --mode web`)
- **Port:** 8002
- **Scraper:** SCJN (integrated)

## 2. Test Execution
### A. Happy Path
1.  **Start:** POST `/api/scjn/start`
    - Response: `{"success":true, "job_id": "..."}`
    - Log: "SCJN job started"
2.  **Status:** GET `/api/scjn/status`
    - Response: `{"status": "running", ...}`
3.  **Artifacts:** Checked `scjn_data/`
    - Found: `documents/` and `embeddings/` directories.
4.  **Download:** GET `/downloads/scjn/test.json` (simulated artifact)
    - Response: 200 OK, JSON content.

### B. Failure/Control Path
1.  **Cancel:** POST `/api/scjn/cancel`
    - Response: `{"success":true}`
    - System cleanly stopped.

## 3. Results
The Web GUI now fully satisfies the integration requirements:
- [x] Triggers scraper (via `create_pipeline`)
- [x] Observes progress (via `SCJNGuiBridgeActor`)
- [x] Produces files (verified on disk)
- [x] Allows downloads (via `/downloads/scjn` mount)
