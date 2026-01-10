# Implementation Log: Web GUI Fix

## Changes
Modified `src/gui/web/api.py` to:
1.  **Initialize SCJN Pipeline:** imported `create_pipeline` from `src.scjn_main` and wired it into `ScraperAPI.startup()`. This ensures the bridge has a real coordinator to talk to.
2.  **Enable Downloads:** Added `app.mount("/downloads/scjn", ...)` to expose the `scjn_data` directory via HTTP.
3.  **Args Helper:** Added `ScraperArgs` dataclass to provide necessary configuration to `create_pipeline`.

## Rationale
The Web GUI was previously a "shell" that initialized the bridge but not the actual scraper backend, leading to "Coordinator not connected" errors. It also lacked any mechanism to retrieve the scraped files.

## Files Modified
- `src/gui/web/api.py`
