"""
Shared fixtures for integration tests.
"""
# Re-export all fixtures from scjn_fixtures.py
from tests.integration.scjn_fixtures import (
    mock_scjn_server,
    no_rate_limiter,
    temp_output_dir,
    temp_checkpoint_dir,
    SAMPLE_SEARCH_HTML,
    SAMPLE_DETAIL_HTML,
)
