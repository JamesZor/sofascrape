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

#####
"""

import logging

from sofascrape.football import SeasonFootballScraper
from sofascrape.general import SeasonsComponentScraper
from sofascrape.utils import FootballLeague, ProcessingUtils

scot_details: FootballLeague = FootballLeague.SCOTLAND
pu = ProcessingUtils(type=FootballLeague.SCOTLAND, web_on=True)
d1 = pu.webmanager.spawn_webdriver()


if __name__ == "__main__":
    season_scraper = SeasonFootballScraper(
        tournamentid=scot_details.value["id"],
        seasonid=62408,
        managerwebdriver=pu.webmanager,
    )
    season_scraper.scrape(use_threading=True, max_workers=8)
    pu.save_pickle(
        "scot_s24_r1", data=season_scraper.data
    )  # schema.SeasonScrapingResults.
