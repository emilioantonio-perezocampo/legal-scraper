import json
import os
import re  # <--- Added regex module
from datetime import date
from src.infrastructure.actors.base import BaseActor
from src.domain.entities import FederalLaw

class PersistenceActor(BaseActor):
    def __init__(self, output_dir: str = "data"):
        super().__init__()
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)

    async def handle_message(self, message):
        if isinstance(message, tuple) and message[0] == "SAVE_LAW":
            law = message[1]
            self._save_to_json(law)

    def _save_to_json(self, law: FederalLaw):
        # 1. Sanitize the title to make it safe for Windows/Linux filenames
        # Remove characters: \ / : * ? " < > |
        safe_title = re.sub(r'[\\/*?:"<>|]', "", law.title)
        
        # 2. Limit length to avoid "File name too long" errors
        safe_title = safe_title[:100] 
        
        # 3. Create filename
        filename = f"{safe_title.replace(' ', '_')}.json"
        filepath = os.path.join(self.output_dir, filename)
        
        # 4. Convert to Dict
        data = {
            "title": law.title,
            "publication_date": str(law.publication_date) if law.publication_date else None,
            "jurisdiction": law.jurisdiction,
            "articles": [
                {
                    "identifier": art.identifier,
                    "content": art.content,
                    "order": art.order
                }
                for art in law.articles
            ]
        }
        
        # 5. Write to disk
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
            
        # print(f"💾 Saved: {filename}") # Optional: Uncomment to see save logs