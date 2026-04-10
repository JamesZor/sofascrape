# ==========================================
# PASTE THIS INTO YOUR IPYTHON REPL
# ==========================================
from sqlalchemy import select
from webdriver import ManagerWebdriver

from sofascrape.conf.config import load_config
from sofascrape.db.manager import DatabaseManager
from sofascrape.db.models import Events, EventsStatusTypes

# IMPORTANT: Adjust this import based on where your scraper lives!
from sofascrape.general.events import EventsComponentScraper
from sofascrape.pipeline.orchestrator import Orchestrator

config = load_config()
db = DatabaseManager(config)

pipeline = Orchestrator(db, config)
# config for the test repl run
tournament_id = 56
season_id = 77129

pipeline.sync_events(tournament_id=tournament_id, season_id=season_id)


# -----------

# 1. Get the events from the DB
with db.SessionLocal() as session:
    matches: list[Events] = pipeline._get_events_season(
        session=session, season_id=season_id
    )

already_finished_ids: set[int] = set(m.match_id for m in matches)

# 2. Scrape the latest calendar from the API
driver = pipeline.mw.spawn_webdriver()

scraper = EventsComponentScraper(
    tournamentid=tournament_id, seasonid=season_id, webdriver=driver, cfg=config
)
parsed_events, raw_events_list = pipeline._scraper_process(scraper=scraper)
print(f"-> API returned {len(parsed_events)} total matches for the season.")


# 3. Calculate the Delta (What finished this weekend?)
newly_finished_ids = []

for p_event in parsed_events:
    # NOTE: Adjust '.status.type' based on how your schema parses the status string
    if p_event.status.type == EventsStatusTypes.FINISHED.value:
        if p_event.id not in already_finished_ids:
            newly_finished_ids.append(p_event.id)

print(f"-> Discovered {len(newly_finished_ids)} NEWLY finished matches!")


if parsed_events:
    print("-> Upserting calendar to the Events table...")
    db.upsert_events(
        tournament_id=tournament_id,
        parsed_events=parsed_events,
        raw_event=raw_events_list,
    )
    print("✅ Database Calendar Updated.")
else:
    print("⚠️ No events parsed from the API.")


# ---------
def dev_sync_events(
    db_manager: DatabaseManager, tournament_id: int, season_id: int, driver
):
    """
    A standalone dev function to update the calendar and find newly finished matches.
    """
    print(
        f"\n--- Starting Calendar Sync | Tournament {tournament_id} | Season {season_id} ---"
    )

    # 1. Ask the DB what is ALREADY finished
    with db_manager.SessionLocal() as session:
        stmt = select(Events.match_id).where(
            Events.season_id == season_id,
            Events.status_type == EventsStatusTypes.FINISHED,
        )
        already_finished_ids = set(session.scalars(stmt).all())

    print(f"-> DB currently knows about {len(already_finished_ids)} finished matches.")

    # 2. Scrape the latest calendar from the API
    print("-> Scraping latest events from SofaScore API...")
    scraper = EventsComponentScraper(
        tournamentid=tournament_id, seasonid=season_id, webdriver=driver, cfg=config
    )
    scraper.get_data()
    scraper.parse_data()

    # NOTE: Adjust '.events' based on your actual Pydantic schema
    parsed_events = scraper.data.events
    raw_events_list = scraper.raw_data.get("events", [])
    print(f"-> API returned {len(parsed_events)} total matches for the season.")

    # 3. Calculate the Delta (What finished this weekend?)
    newly_finished_ids = []

    for p_event in parsed_events:
        # NOTE: Adjust '.status.type' based on how your schema parses the status string
        if p_event.status.type == EventsStatusTypes.FINISHED.value:
            if p_event.id not in already_finished_ids:
                newly_finished_ids.append(p_event.id)

    print(f"-> Discovered {len(newly_finished_ids)} NEWLY finished matches!")

    # 4. Upsert everything to the Database
    # This updates future kick-off times and marks the new ones as FINISHED
    if parsed_events:
        print("-> Upserting calendar to the Events table...")
        db_manager.upsert_events(
            tournament_id=tournament_id,
            parsed_events=parsed_events,
            raw_event=raw_events_list,
        )
        print("✅ Database Calendar Updated.")
    else:
        print("⚠️ No events parsed from the API.")

    return newly_finished_ids


# ==========================================
# THE EXPERIMENT
# ==========================================
mw = ManagerWebdriver()
driver = mw.spawn_webdriver()

try:
    # Use your League One IDs!
    TOURNAMENT_ID = 56
    SEASON_ID = 77129

    new_matches = dev_sync_events(db, TOURNAMENT_ID, SEASON_ID, driver)

    print("\n--- TEST RESULTS ---")
    print(f"Newly Finished Match IDs: {new_matches}")

finally:
    driver.close()
