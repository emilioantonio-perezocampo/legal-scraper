import asyncio
import os
import signal
from datetime import date
from src.infrastructure.actors.scheduler import SchedulerActor
from src.infrastructure.actors.dof_actor import DofScraperActor
from src.infrastructure.actors.persistence import PersistenceActor
from src.infrastructure.actors.dof_discovery_actor import DofDiscoveryActor

async def main():
    print("🚀 Legal Scraper System Starting...")

    # 1. Initialize Persistence (The Vault)
    # Saves data to 'scraped_data' folder. Handles filename sanitization.
    persistence = PersistenceActor(output_dir="scraped_data")
    
    # 2. Initialize Worker (The Soldier)
    # Receives (URL + Title) -> Scrapes Content -> Pipes to Persistence
    dof_scraper = DofScraperActor(output_actor=persistence)
    
    # 3. Initialize Discovery (The Time Traveler Scout)
    # Can scan "Today", a specific date, or a historical range of dates.
    dof_discovery = DofDiscoveryActor(worker_actor=dof_scraper)
    
    # 4. Initialize Scheduler (The Commander)
    scheduler = SchedulerActor()

    # 5. Start All Actors
    await persistence.start()
    await dof_scraper.start()
    await dof_discovery.start()
    await scheduler.start()

    # 6. Register the Scout with the Scheduler
    # (Useful if you want to keep the daily 8:00 AM job active while backfilling)
    await scheduler.tell(("REGISTER_WORKER", dof_discovery))

    print("✅ System Online. Actors are listening.")
    
    # ---------------------------------------------------------
    # ⚡ TRIGGER: HISTORICAL BACKFILL (The "Full Website" Mode)
    # ---------------------------------------------------------
    # Change these dates to scrape whatever period you want.
    # For now, we test a 5-day window to ensure stability.
    
    start_date = date(2021, 3, 1)  # Start of March 2021
    end_date = date(2021, 3, 5)    # 5 days later
    
    print(f"⚡ Triggering Historical Scrape: {start_date} to {end_date}...")
    
    # Send the Range command to the Scout
    await dof_discovery.tell(("DISCOVER_RANGE", start_date, end_date))

    # ---------------------------------------------------------

    # 8. Keep-Alive Loop
    # Keeps the script running so the async actors can finish their work.
    stop_event = asyncio.Event()
    
    def signal_handler():
        print("\n🛑 Shutdown signal received...")
        stop_event.set()

    loop = asyncio.get_running_loop()
    if os.name != 'nt': # Linux/Docker signals
        loop.add_signal_handler(signal.SIGINT, signal_handler)
        loop.add_signal_handler(signal.SIGTERM, signal_handler)

    try:
        if os.name == 'nt':
            # Windows local dev loop
            while True:
                await asyncio.sleep(1)
        else:
            # Docker production wait
            await stop_event.wait()
            
    finally:
        print("Stubbing out actors...")
        await scheduler.stop()
        await dof_discovery.stop()
        await dof_scraper.stop()
        await persistence.stop()
        print("👋 Goodbye.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass