import pytest
import json
import os
from datetime import date
from src.domain.entities import FederalLaw, Article
# This will fail - PersistenceActor doesn't exist yet
from src.infrastructure.actors.persistence import PersistenceActor

@pytest.mark.asyncio
async def test_persistence_actor_saves_to_json(tmp_path):
    """
    Test that the actor receives a Domain Entity and saves it as a JSON file.
    We use 'tmp_path' so the file is created in a temporary test folder.
    """
    # 1. Setup: configure actor to save to a temp dir
    save_dir = tmp_path / "data"
    save_dir.mkdir()
    
    actor = PersistenceActor(output_dir=str(save_dir))
    await actor.start()

    # 2. Create a dummy law
    law = FederalLaw(
        title="Ley de Amparo",
        publication_date=date(2021, 6, 7),
        jurisdiction="Federal",
        articles=[Article(identifier="Art 1", content="Text...", order=1)]
    )

    # 3. Send the command to save
    # Note: We wrap the entity in a tuple or specific command to be explicit
    await actor.tell(("SAVE_LAW", law))

    # Give it a moment to write to disk
    import asyncio
    await asyncio.sleep(0.1)

    # 4. Assert: Check if file exists on disk
    expected_filename = "Ley_de_Amparo.json"
    expected_file = save_dir / expected_filename
    
    assert expected_file.exists()
    
    # Verify content
    with open(expected_file, "r", encoding="utf-8") as f:
        data = json.load(f)
        assert data["title"] == "Ley de Amparo"
        assert data["jurisdiction"] == "Federal"

    await actor.stop()