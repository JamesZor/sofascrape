from sqlalchemy import create_engine

from sofascrape.conf.config import load_config
from sofascrape.db.models import Base

# 1. Load the config we made in Phase 1
config = load_config()

# 2. Create the SQLAlchemy Engine (the actual connection to Postgres)
# We set echo=True so you can watch it generate the raw SQL!
engine = create_engine(config.database.url, echo=True)

# 3. Build the schema
print("Connecting to database and creating tables...")
Base.metadata.create_all(engine)
print("Schema creation complete!")

# ----- Reset the tables, beca

from sqlalchemy import create_engine

from sofascrape.conf.config import load_config
from sofascrape.db.models import Base

config = load_config()
engine = create_engine(config.database.url, echo=True)

# 1. Drop EVERYTHING (Say goodbye to our dummy data!)
Base.metadata.drop_all(engine)

# 2. Recreate everything with the new raw_data column
Base.metadata.create_all(engine)
print("Database schema successfully recreated!")


######################################################################################
# --- Nuclear options ---
######################################################################################

from sqlalchemy import create_engine, text

from sofascrape.conf.config import load_config
from sofascrape.db.models import Base

config = load_config()
engine = create_engine(config.database.url, echo=True)

print("Nuking the old database schema...")

# 1. The Postgres Nuclear Option
with engine.connect() as conn:
    # Force drop everything in the public schema (tables, types, constraints, etc.)
    conn.execute(text("DROP SCHEMA public CASCADE;"))

    # Recreate the empty public schema
    conn.execute(text("CREATE SCHEMA public;"))

    # In SQLAlchemy 2.0, you must explicitly commit these changes
    conn.commit()

# 2. Recreate everything based on your CURRENT db.models.py
print("Rebuilding fresh schema from models...")
Base.metadata.create_all(engine)

print("Database schema successfully recreated!")


# ----- reset insert dbs after reseting the db models tables

# src/dev_pipeline.py

import os

os.environ["DISPLAY"] = ":0"

from webdriver import ManagerWebdriver

from sofascrape.conf.config import load_config
from sofascrape.db.manager import DatabaseManager
from sofascrape.football.eventComponent import EventFootallComponentScraper
from sofascrape.football.graphComponent import FootballGraphComponentScraper
from sofascrape.football.incidentsComponent import FootballIncidentsComponentScraper
from sofascrape.football.lineupComonent import FootballLineupComponentScraper
from sofascrape.football.oddsComponent import FootballOddsComponentScraper
from sofascrape.football.statsComponent import FootballStatsComponentScraper
from sofascrape.general.events import EventsComponentScraper
from sofascrape.general.seasons import SeasonsComponentScraper

# Import all your scrapers
from sofascrape.general.tournament import TournamentComponentScraper


class SofaDevPipeline:
    def __init__(self):
        """Initializes the shared infrastructure (Config, DB, Webdriver)."""
        print("Initializing Configuration and Database...")
        self.config = load_config()
        self.db = DatabaseManager(self.config)

        print("Spawning Proxy-Rotating Webdriver...")
        self.mw = ManagerWebdriver()
        self.driver = self.mw.spawn_webdriver()

    def close(self):
        """Always call this to clean up the browser."""
        print("Closing webdriver...")
        self.driver.close()

    # --- Phase 1: General Tournament Data ---

    def test_tournament(self, target_id: int):
        print(f"\n[1/6] Scraping Tournament {target_id}...")
        scraper = TournamentComponentScraper(
            tournamentid=target_id, webdriver=self.driver, cfg=self.config
        )
        scraper.get_data()
        scraper.parse_data()
        self.db.upsert_tournament(scraper.data, scraper.raw_data)
        print(f"✅ Tournament {scraper.data.tournament.name} saved.")

    def test_seasons(self, target_id: int):
        print(f"\n[2/6] Scraping Seasons for Tournament {target_id}...")
        scraper = SeasonsComponentScraper(
            tournamentid=target_id, webdriver=self.driver, cfg=self.config
        )
        scraper.get_data()
        scraper.parse_data()
        self.db.upsert_seasons(
            scraper.tournamentid,
            scraper.data.seasons,
            scraper.raw_data.get("seasons", []),
        )
        print("✅ Seasons saved.")

    def test_events(self, target_id: int, season_id: int):
        print(
            f"\n[3/6] Scraping Events for Tournament {target_id}, Season {season_id}..."
        )
        scraper = EventsComponentScraper(
            tournamentid=target_id,
            seasonid=season_id,
            webdriver=self.driver,
            cfg=self.config,
        )
        scraper.get_data()
        scraper.parse_data()
        self.db.upsert_events(
            scraper.tournamentid,
            scraper.data.events,
            scraper.raw_data.get("events", []),
        )
        print("✅ Events saved.")

    # --- Phase 2: Specific Match Data ---

    def test_match_base(self, match_id: int):
        print(f"\n[4/6] Scraping Base Match Data for {match_id}...")
        scraper = EventFootallComponentScraper(
            matchid=match_id, webdriver=self.driver, cfg=self.config
        )
        scraper.get_data()
        scraper.parse_data()
        self.db.upsert_match(scraper.matchid, scraper.data, scraper.raw_data)
        print("✅ Match Base Data saved.")

    def test_match_lineups(self, match_id: int):
        print(f"\n[5/6] Scraping Match Lineups for {match_id}...")
        scraper = FootballLineupComponentScraper(
            matchid=match_id, webdriver=self.driver, cfg=self.config
        )
        scraper.get_data()
        scraper.parse_data()
        self.db.upsert_match_lineup(match_id=match_id, parsed_lineup=scraper.data)
        print("✅ Match Lineups saved.")

    def test_match_incidents(self, match_id: int):
        print(f"\n[6/6] Scraping Match Incidents for {match_id}...")
        scraper = FootballIncidentsComponentScraper(
            matchid=match_id, webdriver=self.driver, cfg=self.config
        )
        scraper.get_data()
        scraper.parse_data()
        self.db.upsert_match_incident(match_id=match_id, parsed_incidents=scraper.data)
        print("✅ Match Incidents saved.")

    def test_match_graph(self, match_id: int):
        print(f"\n[7/7] Scraping Match Graph (Momentum) for {match_id}...")
        scraper = FootballGraphComponentScraper(
            matchid=match_id, webdriver=self.driver, cfg=self.config
        )
        scraper.get_data()
        scraper.parse_data()
        self.db.upsert_match_graph(match_id=match_id, parsed_graph=scraper.data)
        print("✅ Match Graph saved.")

    def test_match_odds(self, match_id: int):
        print(f"\n[8] Scraping Match Odds for {match_id}...")
        scraper = FootballOddsComponentScraper(
            matchid=match_id, webdriver=self.driver, cfg=self.config
        )
        scraper.get_data()
        scraper.parse_data()
        self.db.upsert_match_odds(
            match_id=match_id,
            parsed_odds_markets=scraper.data,
            raw_odds=scraper.raw_data,
        )
        print("✅ Match Odds saved.")

    def test_match_statistics(self, match_id: int) -> None:
        print(f"\n[9] Scraping Match statistics for {match_id}...")
        scraper = FootballStatsComponentScraper(
            matchid=match_id, webdriver=self.driver, cfg=self.config
        )
        scraper.get_data()
        scraper.parse_data()
        self.db.upsert_match_statistics(match_id=match_id, parsed_stats=scraper.data)
        print("✅ Match Odds saved.")


# ==========================================
# Execution Block (Edit this part to test!)
# ==========================================
if __name__ == "__main__":
    # Test Parameters
    # TOURNAMENT_ID = 56
    # SEASON_ID = 77129
    # MATCH_ID = 14035506

    TOURNAMENT_ID = 54
    SEASON_ID = 77128
    MATCH_ID = 14035136

    pipeline = SofaDevPipeline()

    try:
        # Uncomment the pieces you want to test right now.
        # They will all share the exact same browser window!

        pipeline.test_tournament(target_id=TOURNAMENT_ID)
        pipeline.test_seasons(target_id=TOURNAMENT_ID)
        pipeline.test_events(target_id=TOURNAMENT_ID, season_id=SEASON_ID)

        pipeline.test_match_base(match_id=MATCH_ID)
        pipeline.test_match_lineups(match_id=MATCH_ID)
        pipeline.test_match_incidents(match_id=MATCH_ID)
        pipeline.test_match_statistics(match_id=MATCH_ID)
        pipeline.test_match_graph(match_id=MATCH_ID)
        pipeline.test_match_odds(match_id=MATCH_ID)

        # pipeline.test_match_statistics(match_id=MATCH_ID)

    except Exception as e:
        print(f"\n❌ Error during scraping: {e}")
        raise
    finally:
        # This guarantees the proxy browser closes even if your code fails
        pipeline.close()
