# ==========================================
# PASTE THIS ENTIRE BLOCK INTO Ipython
# ==========================================
from sqlalchemy import select

from sofascrape.conf.config import load_config
from sofascrape.db.manager import DatabaseManager
from sofascrape.db.models import Events, Match, MatchComponentAudit

# from sofascrape.pipeline.orchestrator import Orchestrator

# 1. Initialize our core tools
config = load_config()
db = DatabaseManager(config)
# pipeline = Orchestrator(db, config)


# 2. Define our Sandbox Function
def dev_queue_backfill(db_manager: DatabaseManager, season_id: int, component: str):
    """
    A standalone dev function to backfill a component for an entire season.
    (We can move this into manager.py later once you are happy with it!)
    """
    print(
        f"\n--- Starting Backfill for Season {season_id} | Component: {component} ---"
    )

    with db_manager.SessionLocal() as session:
        # Step A: Find all matches in this season
        stmt = select(Events.id).where(
            Events.season_id == season_id, Events.status_type == "finished"
        )
        match_ids = session.scalars(stmt).all()

        if not match_ids:
            print(f"⚠️ No matches found in the database for season {season_id}!")
            return 0

        print(f"Found {len(match_ids)} matches. Queuing '{component}' tasks...")

        queued_count = 0
        for m_id in match_ids:
            # Step B: Create a new pending task for this specific component
            task = MatchComponentAudit(
                match_id=m_id, component_name=component, status="PENDING"
            )
            # using .merge() just in case a task already exists, it won't crash
            session.merge(task)
            queued_count += 1

        session.commit()
        print(f"✅ Successfully queued {queued_count} tasks!")
        return queued_count


# ==========================================
# THE EXPERIMENT
# ==========================================

# Let's say your Season ID for Scottish League One is 62416 (from your previous logs)
# Change this to whatever season ID you currently have matches saved for!
TARGET_SEASON_ID = 77128

# 1. Queue up the backfill (Let's try grabbing the odds!)
tasks_added = dev_queue_backfill(db, TARGET_SEASON_ID, "odds")

# 2. Run the Orchestrator to process our new queue!
if tasks_added > 0:
    print("\n🚀 Firing up the Orchestrator...")
    pipeline.run_worker_loop()
else:
    print("No tasks to run. Did you sync matches for this season yet?")
