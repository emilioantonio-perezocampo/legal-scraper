# GUI Domain Layer
"""
Domain layer containing:
- Entities: Core business objects with identity
- Value Objects: Immutable objects defined by their attributes
- Domain Events: Events that represent significant domain occurrences
"""
from .entities import (
    ScraperJob,
    JobStatus,
    JobProgress,
    ScraperConfiguration,
    DateRange,
    LogEntry,
    LogLevel,
)
from .value_objects import (
    TargetSource,
    OutputFormat,
    ScraperMode,
)

__all__ = [
    'ScraperJob',
    'JobStatus',
    'JobProgress',
    'ScraperConfiguration',
    'DateRange',
    'LogEntry',
    'LogLevel',
    'TargetSource',
    'OutputFormat',
    'ScraperMode',
]
