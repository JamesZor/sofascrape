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
        stmt = select(Events.match_id).where(
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


14035250

# ==========================================
# THE EXPERIMENT
# ==========================================

# Let's say your Season ID for Scottish League One is 62416 (from your previous logs)
# Change this to whatever season ID you currently have matches saved for!
TARGET_SEASON_ID = 77128

# 1. Queue up the backfill (Let's try grabbing the odds!)
tasks_added = dev_queue_backfill(db, TARGET_SEASON_ID, "odds")

# --- The MVP Orchestrator Sandbox


import time

from webdriver import ManagerWebdriver

from sofascrape.conf.config import load_config
from sofascrape.db.manager import DatabaseManager

# Import your specific odds scraper
# Adjust this import to match your actual class name in oddsComponent.py!
from sofascrape.football.oddsComponent import FootballOddsComponentScraper

# 1. Init infrastructure (if you haven't already in this session)
config = load_config()
db = DatabaseManager(config)


# 2. Grab a tiny batch of tasks so we can watch it work
def get_pending_tasks(db: DatabaseManager, limit: int = 2) -> list[MatchComponentAudit]:
    """Fetches a batch of pending or retryable tasks from the queue."""
    with db.SessionLocal() as session:
        # We want tasks that are PENDING, or QA_MISMATCH/API_ERROR that haven't hit the retry limit
        stmt = (
            select(MatchComponentAudit)
            .where(
                MatchComponentAudit.status.in_(["PENDING", "QA_MISMATCH", "API_ERROR"]),
                MatchComponentAudit.retry_count
                < 3,  # Hardcoded for now, could use config
            )
            .limit(limit)
        )
        # session.scalars() unpacks the SQLAlchemy rows into actual Python objects
        results = session.scalars(stmt).all()
        return results


tasks = get_pending_tasks(db=db, limit=3)
print(f"\n--- Mini-Orchestrator grabbed {len(tasks)} tasks ---")
print(tasks)

# here we want to process a task:
mw = ManagerWebdriver()
driver = mw.spawn_webdriver()

t1 = tasks[1]

print(f"\n▶ Processing Match {t1.match_id} | Component: {t1.component_name}")

# Instantiate your scraper
scraper = FootballOddsComponentScraper(
    matchid=t1.match_id, webdriver=driver, cfg=config
)


print("  - Fetch A...")
scraper.get_data()
scraper.parse_data()
data_a = scraper.data
raw_a = scraper.raw_data

# --- PAUSE ---
pause_time = config.pipeline.qa_pause_seconds
print(f"  - Anti-bot pause for {pause_time}s...")
time.sleep(pause_time)

# --- FETCH B ---
print("  - Fetch B...")
scraper.get_data()
scraper.parse_data()
data_b = scraper.data

# --- IN-MEMORY QA ---
# Pydantic models can be directly compared with '=='!
if data_a == data_b and data_a is not None:
    print("  ✅ QA Passed! Data is identical.")

    # We use the awesome routing method you built earlier!
    # db.save_component_data(
    #     match_id=task.match_id,
    #     component_name=task.component_name,
    #     parsed_data=data_a,
    #     raw_data=raw_a,
    # )
    # Mark queue as done
    # db.update_task_status(task.audit_id, status="SUCCESS")
    print("  💾 Saved to PostgreSQL.")
else:
    print("  ❌ QA Mismatch! Randomization caught.")
    # db.update_task_status(
    #     task.audit_id,
    #     status="QA_MISMATCH",
    #     error_message="Fetch A != Fetch B",
    #     increment_retry=True,
    # )


# TODO: 2026-04-07
def process_compenent(task, driver, config) -> None:

    print(f"\n▶ Processing Match {task.match_id} | Component: {task.component_name}")

    # Instantiate your scraper
    scraper = FootballOddsComponentScraper(
        matchid=task.match_id, webdriver=driver, cfg=config
    )

    try:
        # --- FETCH A ---
        print("  - Fetch A...")
        scraper.get_data()
        scraper.parse_data()
        data_a = scraper.data

        # --- PAUSE ---
        pause_time = config.pipeline.qa_pause_seconds
        print(f"  - Anti-bot pause for {pause_time}s...")
        time.sleep(pause_time)

        # --- FETCH B ---
        print("  - Fetch B...")
        scraper.get_data()
        scraper.parse_data()
        data_b = scraper.data

        # --- IN-MEMORY QA ---
        # Pydantic models can be directly compared with '=='!
        if data_a == data_b and data_a is not None:
            print("  ✅ QA Passed! Data is identical.")

            # We use the awesome routing method you built earlier!
            # db.save_component_data(
            #     match_id=task.match_id,
            #     component_name=task.component_name,
            #     parsed_data=data_a,
            #     raw_data=raw_a,
            # )
            # # Mark queue as done
            # db.update_task_status(task.audit_id, status="SUCCESS")
            print("  💾 Saved to PostgreSQL.")
        else:
            print("  ❌ QA Mismatch! Randomization caught.")
            # db.update_task_status(
            #     task.audit_id,
            #     status="QA_MISMATCH",
            #     error_message="Fetch A != Fetch B",
            #     increment_retry=True,
            # )

    except Exception as e:
        print(f"  ⚠️ Error scraping {task.match_id}: {e}")
        # db.update_task_status(
        #     task.audit_id,
        #     status="API_ERROR",
        #     error_message=str(e),
        #     increment_retry=True,
        # )


t1 = task[1]

process_compenent(t1, driver, config)

# -------

if tasks:
    # 3. Spin up ONE webdriver for this test loop
    print("Spawning Webdriver...")
    mw = ManagerWebdriver()
    driver = mw.spawn_webdriver()

    try:
        for task in tasks:
            print(
                f"\n▶ Processing Match {task.match_id} | Component: {task.component_name}"
            )

            # Instantiate your scraper
            scraper = FootballOddsComponentScraper(
                matchid=task.match_id, webdriver=driver, cfg=config
            )

            try:
                # --- FETCH A ---
                print("  - Fetch A...")
                scraper.get_data()
                scraper.parse_data()
                data_a = scraper.data
                raw_a = scraper.raw_data

                # --- PAUSE ---
                pause_time = config.pipeline.qa_pause_seconds
                print(f"  - Anti-bot pause for {pause_time}s...")
                time.sleep(pause_time)

                # --- FETCH B ---
                print("  - Fetch B...")
                scraper.get_data()
                scraper.parse_data()
                data_b = scraper.data

                # --- IN-MEMORY QA ---
                # Pydantic models can be directly compared with '=='!
                if data_a == data_b and data_a is not None:
                    print("  ✅ QA Passed! Data is identical.")

                    # We use the awesome routing method you built earlier!
                    db.save_component_data(
                        match_id=task.match_id,
                        component_name=task.component_name,
                        parsed_data=data_a,
                        raw_data=raw_a,
                    )
                    # Mark queue as done
                    db.update_task_status(task.audit_id, status="SUCCESS")
                    print("  💾 Saved to PostgreSQL.")
                else:
                    print("  ❌ QA Mismatch! Randomization caught.")
                    db.update_task_status(
                        task.audit_id,
                        status="QA_MISMATCH",
                        error_message="Fetch A != Fetch B",
                        increment_retry=True,
                    )

            except Exception as e:
                print(f"  ⚠️ Error scraping {task.match_id}: {e}")
                db.update_task_status(
                    task.audit_id,
                    status="API_ERROR",
                    error_message=str(e),
                    increment_retry=True,
                )

    finally:
        print("\nCleaning up webdriver...")
        driver.close()
