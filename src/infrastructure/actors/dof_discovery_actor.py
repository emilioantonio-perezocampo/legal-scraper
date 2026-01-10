import asyncio
import aiohttp
from datetime import date, timedelta
from src.infrastructure.actors.base import BaseActor
from src.infrastructure.adapters.dof_index_parser import parse_dof_index

class DofDiscoveryActor(BaseActor):
    """
    The Scout. It visits DOF index pages (Past or Present), 
    finds documents, and assigns them to the Worker.
    """
    def __init__(self, worker_actor: BaseActor):
        super().__init__()
        self.worker_actor = worker_actor

    async def handle_message(self, message):
        # 1. Scrape Today (Daily Job)
        if message == "DISCOVER_TODAY":
            await self._scan_index(date.today())

        # 2. Scrape Specific Date
        elif isinstance(message, tuple) and message[0] == "DISCOVER_DATE":
            target_date = message[1]
            await self._scan_index(target_date)

        # 3. NEW: Scrape a Range of Dates (Historical Backfill)
        # Message format: ("DISCOVER_RANGE", start_date, end_date)
        elif isinstance(message, tuple) and message[0] == "DISCOVER_RANGE":
            start_date = message[1]
            end_date = message[2]
            await self._scan_range(start_date, end_date)

    async def _scan_range(self, start_date: date, end_date: date):
        """
        Loops through every day from start to end.
        """
        current_date = start_date
        total_days = (end_date - start_date).days + 1
        
        print(f"🗓️ STARTING HISTORICAL SCRAPE: {total_days} days ({start_date} to {end_date})")

        while current_date <= end_date:
            # Skip weekends? (Optional: DOF publishes usually Mon-Fri, but sometimes Sat/Sun)
            # For "All website", we check every day just in case.
            
            await self._scan_index(current_date)
            
            # Move to next day
            current_date += timedelta(days=1)
            
            # CRITICAL: Sleep between days to avoid IP Ban
            print("⏳ Cooling down for 2 seconds...")
            await asyncio.sleep(2)

        print("🏁 HISTORICAL SCRAPE COMPLETE.")

    async def _scan_index(self, target_date: date):
        url = f"https://dof.gob.mx/index.php?year={target_date.year}&month={target_date.month:02d}&day={target_date.day:02d}"
        
        print(f"🔭 Scanning Index: {target_date} -> {url}")

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, ssl=False) as response:
                    if response.status == 200:
                        html = await response.text(encoding='utf-8')
                        items = parse_dof_index(html)
                        
                        if items:
                            print(f"   found {len(items)} documents.")
                            for item in items:
                                await self.worker_actor.tell(item)
                        else:
                            print("   (No documents found, possibly weekend/holiday)")
                    else:
                        print(f"❌ Failed to fetch index: {response.status}")
        except Exception as e:
            print(f"❌ Error scanning {target_date}: {e}")