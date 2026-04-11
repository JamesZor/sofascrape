import os

os.environ["DISPLAY"] = ":0"

from webdriver import ManagerWebdriver

from sofascrape.conf.config import load_config
from sofascrape.db.manager import DatabaseManager
from sofascrape.football.statsComponent import FootballStatsComponentScraper

# 1. Load our new infrastructure
config = load_config()
db = DatabaseManager(config)

# 2. Spin up your custom proxy-rotating webdriver
print("Spawning Webdriver...")
mw = ManagerWebdriver()
driver = mw.spawn_webdriver()

# 3. Let's try to scrape the Scottish Premiership (Assuming ID is 36, change if needed!)
# Actually, let's use Tournament ID 17 (Premier League) or whatever you know works.
# match_id = 14035506 # Tournament id
match_id = 14035136

scraper = FootballStatsComponentScraper(matchid=match_id, webdriver=driver, cfg=config)

# 4. Execute the scrape steps
scraper.get_data()
print("Raw Data Fetched!")

scraper.parse_data()

# Let's look at what we got:
print(f"\n--- Result ---")
print(scraper.data.model_dump_json(indent=6))  # Pretty print the JSON

# 5. Let's test saving it to our brand new Postgres table!
# TODO:
db.upsert_match_statistics(match_id=match_id, parsed_stats=scraper.data)
# Always clean up the browser!
print("Closing webdriver...")
driver.close()
