import pytest
from datetime import date
from src.domain.entities import FederalLaw, Article

def test_federal_law_initialization():
    """Test that we can create a basic FederalLaw entity."""
    law = FederalLaw(
        title="Ley Federal del Trabajo",
        publication_date=date(1970, 4, 1),
        jurisdiction="Federal"
    )
    assert law.title == "Ley Federal del Trabajo"

def test_law_can_hold_articles():
    """Test that a law is composed of articles."""
    article_1 = Article(
        identifier="Art. 1",
        content="The laws shall be respected...",
        order=1
    )
    
    law = FederalLaw(
        title="Constitution",
        publication_date=date(1917, 2, 5),
        jurisdiction="Federal",
        articles=[article_1]
    )

    assert len(law.articles) == 1
    assert law.articles[0].identifier == "Art. 1"