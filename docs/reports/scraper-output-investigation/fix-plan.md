# Fix Plan: Enable Scraper Execution

## Problem
The `src/scjn_main.py` entry point crashes immediately with `TypeError: RateLimiter.__init__() got an unexpected keyword argument 'burst_size'`.
This prevents users from running the scraper manually, leading to the perception that "no output files are produced" (because it crashes before writing).

## Root Cause
The `RateLimiter` class definition (`src/infrastructure/actors/rate_limiter.py`) does not accept `burst_size` in its constructor, but the main wiring code passes it.

## Proposed Fix
Remove the `burst_size` argument from the `RateLimiter` instantiation in `src/scjn_main.py`.

```python
# src/scjn_main.py

async def create_pipeline(args) -> SCJNCoordinatorActor:
    """Create and wire the full scraping pipeline."""
    # Create rate limiter
    rate_limiter = RateLimiter(
        requests_per_second=args.rate_limit,
        # burst_size argument removed
    )
```

## Verification
1.  Apply the fix.
2.  Run the reproduction command:
    `python src/scjn_main.py discover --max-results 1 --output-dir _scratch/scraper-debug/scjn --skip-pdfs`
3.  Verify that `_scratch/scraper-debug/scjn` contains a `.json` file.
