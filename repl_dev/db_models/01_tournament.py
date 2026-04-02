import os

from webdriver import ManagerWebdriver

os.environ["DISPLAY"] = ":0"

from sofascrape.conf.config import load_config
from sofascrape.db.manager import DatabaseManager
from sofascrape.general.tournament import TournamentComponentScraper

# 1. Load our new infrastructure
config = load_config()
db = DatabaseManager(config)

# 2. Spin up your custom proxy-rotating webdriver
print("Spawning Webdriver...")
mw = ManagerWebdriver()
driver = mw.spawn_webdriver()

try:
    # 3. Let's try to scrape the Scottish Premiership (Assuming ID is 36, change if needed!)
    # Actually, let's use Tournament ID 17 (Premier League) or whatever you know works.
    target_id = 54

    print(f"Instantiating Scraper for Tournament {target_id}...")
    scraper = TournamentComponentScraper(
        tournamentid=target_id, webdriver=driver, cfg=config
    )

    # 4. Execute the scrape steps
    scraper.get_data()
    print("Raw Data Fetched!")

    scraper.parse_data()
    print("Data Parsed into Pydantic successfully!")

    # Let's look at what we got:
    tournament_name = scraper.data.tournament.name
    print(f"\n--- Result ---")
    print(f"Found Tournament: {tournament_name}")
    print(scraper.data.model_dump_json(indent=6))  # Pretty print the JSON

    # 5. Let's test saving it to our brand new Postgres table!
    print(f"\nSaving {tournament_name} to Postgres database...")
    db.upsert_tournament(scraper.data, scraper.raw_data)
    print("Saved successfully!")

finally:
    # Always clean up the browser!
    print("Closing webdriver...")
    driver.close()
