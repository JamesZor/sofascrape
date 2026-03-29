from omegaconf import OmegaConf
from webdriver import ManagerWebdriver

from sofascrape.general import SeasonsComponentScraper

#

# - National League (ID: 72, Slug: national-league)
national_league_id = 72
"""
name='National League 25/26' id=78229 year='25/26'
name='National League 24/25' id=63807 year='24/25'
name='National League 23/24' id=52783 year='23/24'
name='National League 22/23' id=42666 year='22/23'
name='National League 21/22' id=37339 year='21/22'
name='National League 20/21' id=32516 year='20/21'
name='National League 19/20' id=24130 year='19/20'
name='National League 18/19' id=17826 year='18/19'
name='National League 17/18' id=13527 year='17/18'
name='National League 16/17' id=11903 year='16/17'
name='National League 15/16' id=10456 year='15/16'
name='National League 14/15' id=8454 year='14/15' 
"""

if __name__ == "__main__":

    mw = ManagerWebdriver()
    d1 = mw.spawn_webdriver()

    tour_scrape_pl = SeasonsComponentScraper(
        tournamentid=national_league_id, webdriver=d1
    )

    results = tour_scrape_pl.process()

    for season in results.seasons:
        print(season)
