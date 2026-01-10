from dataclasses import dataclass, field
from datetime import date
from typing import List, Optional

@dataclass
class Article:
    """
    Represents a single article within a law.
    Value Object: Its identity is defined by its content and order within the law.
    """
    identifier: str  # e.g., "Art. 1", "Art. 42 Bis"
    content: str
    order: int  # To maintain sequence

@dataclass
class FederalLaw:
    """
    Represents a Federal Law (Aggregate Root).
    """
    title: str
    publication_date: date
    jurisdiction: str
    # A law starts with an empty list of articles by default
    articles: List[Article] = field(default_factory=list)