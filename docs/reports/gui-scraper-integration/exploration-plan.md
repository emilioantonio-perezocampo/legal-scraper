# Exploration Plan

## Objective
Validate that the Web GUI (FastAPI) can trigger SCJN scraping and produce downloadable artifacts.

## Analysis of `api.py`
- **Endpoints:** `/api/scjn/start`, `/api/scjn/status`.
- **Missing:** No obvious download route or static mount for `scraped_data`.
- **Hypothesis:** The Web GUI currently allows triggering jobs but relies on filesystem access for results. This fails the "downloadable" requirement.

## Planned Steps
1.  **Start Web GUI:** `python -m src.gui.launcher --mode web --port 8001` (background).
2.  **Health Check:** `curl http://localhost:8001/api/health`.
3.  **Trigger Scrape:** `curl -X POST http://localhost:8001/api/scjn/start` with payload `{"max_results": 1, "output_directory": "_scratch/gui_test"}`.
4.  **Poll Status:** Loop `curl http://localhost:8001/api/scjn/status` until completed.
5.  **Verify Artifacts:** `ls _scratch/gui_test`.
6.  **Attempt Download:** Try accessing files via HTTP. If failed, I must implement a download route.

## Success Criteria for Exploration
- Confirm GUI starts.
- Confirm Scrape triggers.
- Confirm files exist on disk.
- Confirm files are *not* currently downloadable (proving the need for a fix).
