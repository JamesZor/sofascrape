import os

from webdriver import ManagerWebdriver

os.environ["DISPLAY"] = ":0"

from sofascrape.conf.config import load_config
from sofascrape.db.manager import DatabaseManager
from sofascrape.general.seasons import SeasonsComponentScraper

# 1. Load our new infrastructure
config = load_config()
db = DatabaseManager(config)

# 2. Spin up your custom proxy-rotating webdriver
print("Spawning Webdriver...")
mw = ManagerWebdriver()
driver = mw.spawn_webdriver()


target_id = 56
# target_id = 54

print(f"Instantiating Scraper for Tournament {target_id}...")
scraper = SeasonsComponentScraper(tournamentid=target_id, webdriver=driver, cfg=config)

# 4. Execute the scrape steps
scraper.get_data()
print("Raw Data Fetched!")

scraper.parse_data()
print("Data Parsed into Pydantic successfully!")

# Let's look at what we got:
print(f"\n--- Result ---")
print(scraper.data.model_dump_json(indent=6))  # Pretty print the JSON

# 5. Let's test saving it to our brand new Postgres table!
# NOTE: need to ensure that we have tournamentid in db before, otherwise throws an error.
db.upsert_seasons(
    scraper.tournamentid,
    scraper.data.seasons,
    scraper.raw_data.get("seasons", []),
)

print("Saved successfully!")

driver.close()
