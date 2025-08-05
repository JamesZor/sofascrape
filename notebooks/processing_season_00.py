"""
Notebook on the development of a data processor for season information.

- get the event ids for the season.
- run the scraper for the season
- save the data from the scraper

- since there could be issues with data rerun, save +1

- process, the saved seasons:
    test, hash the details to compare.
    save good data, flag missing, incorrect data.

- scrape the flag  and missing.



##### Get
seasons_ids_scraper = SeasonsComponentScraper(
    tournamentid=scot_details.value["id"], webdriver=d1
)
seasons_ids_scraper.process()

for season in seasons_ids_scraper.data.seasons:
    print(season.model_dump_json(indent=3))

{
   "name": "Premiership 25/26",
   "id": 77128,
   "year": "25/26"
}
{
"name": "Premiership 24/25",
   "id": 62408,
   "year": "24/25"
}
{
   "name": "Premiership 23/24",
   "id": 52588,
   "year": "23/24"
}
{
   "name": "Premiership 22/23",
   "id": 41957,
   "year": "22/23"
}
{
   "name": "Premiership 21/22",
   "id": 37029,
   "year": "21/22"
}
{
   "name": "Premiership 20/21",
   "id": 28212,
   "year": "20/21"
}
{
   "name": "Premiership 19/20",
   "id": 23987,
   "year": "19/20"
}
{
   "name": "Premiership 18/19",
   "id": 17364,
   "year": "18/19"
}
{
   "name": "Premiership 17/18",
   "id": 13448,
   "year": "17/18"
}
{
   "name": "Premiership 16/17",
   "id": 11743,
   "year": "16/17"
}
{
   "name": "Premiership 15/16",
   "id": 10379,
   "year": "15/16"
}
{
   "name": "Premiership 14/15",
   "id": 8190,
   "year": "14/15"
}
{
   "name": "Premier League 13/14",
   "id": 6309,
   "year": "13/14"
}
{
   "name": "Premier League 12/13",
   "id": 4728,
   "year": "12/13"
}
{
   "name": "Premier League 11/12",
   "id": 3392,
   "year": "11/12"
}
{
   "name": "Premier League 10/11",
   "id": 2752,
   "year": "10/11"
}
{
   "name": "Premier League 09/10",
   "id": 2140,
   "year": "09/10"
}
{
   "name": "Premier League 08/09",
   "id": 1570,
   "year": "08/09"
}
"""

#####
"""
#Scrape and save example:

    season_scraper = SeasonFootballScraper(
        tournamentid=scot_details.value["id"],
        seasonid=62408,
        managerwebdriver=pu.webmanager,
    )
    season_scraper.scrape(use_threading=True, max_workers=8)
    pu.save_pickle(
        "scot_s24_r1", data=season_scraper.data
"""

######

import logging
from datetime import datetime
from pathlib import Path

from sofascrape.football import SeasonFootballScraper
from sofascrape.general import SeasonsComponentScraper
from sofascrape.utils import FootballLeague, ProcessingUtils


# ===== LOGGING SETUP =====
def setup_run_logging(run_name: str = "scot_r2"):
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"{run_name}_{timestamp}.log"

    # Key change: Don't use basicConfig!
    # Instead, configure specific loggers:

    # Only your sofascrape package gets debug level
    sofascrape_logger = logging.getLogger("sofascrape")
    sofascrape_logger.setLevel(logging.DEBUG)

    # Add handlers only to your loggers
    handler = logging.FileHandler(log_file)
    handler.setFormatter(
        logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    )

    sofascrape_logger.addHandler(handler)
    sofascrape_logger.propagate = False  # This stops selenium logs!

    # Silence selenium explicitly
    logging.getLogger("selenium").setLevel(logging.ERROR)

    return str(log_file)


# ===== YOUR EXISTING CODE WITH LOGGING =====
# Setup logging (call this once at the start)
log_file = setup_run_logging("scot_r3_run1")  # Change run name for different runs

scot_details: FootballLeague = FootballLeague.SCOTLAND
pu = ProcessingUtils(type=FootballLeague.SCOTLAND, web_on=True)
d1 = pu.webmanager.spawn_webdriver()
logger = logging.getLogger("scot_r3")

if __name__ == "__main__":
    logger.info("Starting season scraper...")
    logger.info(f"Tournament ID: {scot_details.value['id']}")

    season_scraper = SeasonFootballScraper(
        tournamentid=scot_details.value["id"],
        seasonid=62408,
        managerwebdriver=pu.webmanager,
    )

    logger.info("Starting scrape with 8 workers...")
    season_scraper.scrape(use_threading=True, max_workers=8)

    # Log results
    if season_scraper.data:
        logger.info(
            f"Scraping completed! Success rate: {season_scraper.data.success_rate}"
        )
        logger.info(
            f"Total: {season_scraper.data.total_matches}, "
            f"Success: {season_scraper.data.successful_matches}, "
            f"Failed: {season_scraper.data.failed_matches}"
        )

    pu.save_pickle("scot_s24_r3", data=season_scraper.data)
    logger.info(f"Data saved! Log file: {log_file}")
