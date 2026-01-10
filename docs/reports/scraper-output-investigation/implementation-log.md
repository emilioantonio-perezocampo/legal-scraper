# Implementation Log

## Changes
Modified `src/scjn_main.py` to remove the invalid `burst_size` argument passed to the `RateLimiter` constructor.

### File: `src/scjn_main.py`
- **Before:**
    ```python
    rate_limiter = RateLimiter(
        requests_per_second=args.rate_limit,
        burst_size=max(1, int(args.rate_limit * 2)),
    )
    ```
- **After:**
    ```python
    rate_limiter = RateLimiter(
        requests_per_second=args.rate_limit,
    )
    ```

## Verification
1.  **Reproduction Command:** `python src/scjn_main.py discover --max-results 1 --output-dir _scratch/scraper-debug/scjn --skip-pdfs`
    - **Result:** Success (Exit Code 0). The script no longer crashes with `TypeError`.
    - **Artifacts:** `_scratch/scraper-debug/scjn/documents` and `_scratch/scraper-debug/scjn/embeddings` directories were created.

2.  **Regression Testing:** `pytest tests/integration/test_scjn_pipeline.py`
    - **Result:** All 16 tests passed. The change did not break existing pipeline tests.
