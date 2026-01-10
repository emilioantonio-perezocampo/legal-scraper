import aiohttp
from typing import Optional, Dict
from src.infrastructure.actors.base import BaseActor
from src.infrastructure.adapters.dof_parser import parse_dof_html

class DofScraperActor(BaseActor):
    def __init__(self, output_actor: Optional[BaseActor] = None):
        super().__init__()
        self.output_actor = output_actor

    async def handle_message(self, message):
        url = None
        external_title = None

        # --- Message Parsing Logic ---
        
        # 1. New "Rich" Message from Scout (Dict)
        if isinstance(message, dict) and "url" in message:
            url = message["url"]
            external_title = message.get("title")

        # 2. Legacy/Manual String Message
        elif isinstance(message, str):
            if "nota_detalle.php" in message:
                url = message.strip()
            elif message == "scrape_ley_federal_trabajo" or message == "START_SCRAPING":
                url = "https://dof.gob.mx/nota_detalle.php?codigo=5612823&fecha=01/03/2021"

        # --- Scraping Logic ---
        if url:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, ssl=False) as response:
                        if response.status == 200:
                            html = await response.text(encoding='utf-8')
                            
                            # Parse HTML content
                            federal_law = parse_dof_html(html)
                            
                            # OVERRIDE: If the Scout gave us a better title, use it!
                            # This fixes "Unknown Title" because the Index page usually has the correct text.
                            if external_title:
                                federal_law.title = external_title
                            
                            # Log success
                            safe_title = federal_law.title[:60] + "..." if len(federal_law.title) > 60 else federal_law.title
                            print(f"✅ Scraped: {safe_title}")

                            if self.output_actor:
                                await self.output_actor.tell(("SAVE_LAW", federal_law))
                            
                            return federal_law
                        else:
                            print(f"❌ Error fetching {url}: Status {response.status}")
            except Exception as e:
                print(f"❌ Exception scraping {url}: {e}")
        
        return None