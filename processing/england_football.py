import logging
import time
from dataclasses import asdict

from webdriver import ManagerWebdriver

import sofascrape.schemas.general as sofaschema
from sofascrape.general import SeasonsComponentScraper
from sofascrape.loader.footballDataManager import FootballDataManager
from sofascrape.quality.core.dataclasses import SeasonConsensusResult
from sofascrape.quality.manager import SeasonQualityManager

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    force=True,
)
# Use the CORRECT logger name from the debug output
# manager_logger =logging.getLogger("sofascrape.quality.manager")
# manager_logger.setLevel(logging.w)
logging.getLogger("sofascrape.quality.manager").setLevel(logging.WARNING)
# Silence the noisy ones
logging.getLogger("webdriver").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("selenium").setLevel(logging.WARNING)
logging.getLogger("webdriver.core").setLevel(logging.WARNING)
logging.getLogger("webdriver.core.mywebdriver").setLevel(logging.WARNING)
logging.getLogger("webdriver.core.options").setLevel(logging.WARNING)
logging.getLogger("webdriver.core.proxy_manager").setLevel(logging.WARNING)

logging.getLogger("sofascrape.football.season").setLevel(logging.WARNING)
logging.getLogger("sofascrape.football.LineupComonent").setLevel(logging.WARNING)
logging.getLogger("sofascrape.football.base").setLevel(logging.WARNING)
logging.getLogger("sofascrape.football.matchScraper").setLevel(logging.WARNING)
logging.getLogger("sofascrape.football.incidentsComponent").setLevel(logging.WARNING)
logging.getLogger("sofascrape.football.statsComponent").setLevel(logging.WARNING)
logging.getLogger("sofascrape.football.graphComponent").setLevel(logging.WARNING)
logging.getLogger("sofascrape.football.lineupComonent").setLevel(logging.WARNING)
logging.getLogger("sofascrape.football.LineupComonent").setLevel(logging.WARNING)
logging.getLogger("sofascrape.football.eventComponent").setLevel(logging.WARNING)
logging.getLogger("sofascrape.abstract.base").setLevel(logging.WARNING)

"""
  - Premier League (ID: 1, Slug: premier-league)
  - Championship (ID: 2, Slug: championship)
  - League One (ID: 3, Slug: league-one)
  - League Two (ID: 84, Slug: league-two)

"""

"""
 No stats 
name='Premier League 25/26' id=76986 year='25/26'
name='Premier League 24/25' id=61627 year='24/25' - missing 18
name='Premier League 23/24' id=52186 year='23/24' -missing 2 
name='Premier League 22/23' id=41886 year='22/23'
name='Premier League 21/22' id=37036 year='21/22'
name='Premier League 20/21' id=29415 year='20/21'
name='Premier League 19/20' id=23776 year='19/20'
name='Premier League 18/19' id=17359 year='18/19'
name='Premier League 17/18' id=13380 year='17/18'
name='Premier League 16/17' id=11733 year='16/17'

"""

"""
no stats 
name='Championship 25/26' id=77347 year='25/26'
name='Championship 24/25' id=61961 year='24/25' - missing 4, 
name='Championship 23/24' id=52367 year='23/24' - missing 10 matches 
name='Championship 22/23' id=42401 year='22/23' - missing 1 
name='Championship 21/22' id=37154 year='21/22' - missing 10
name='Championship 20/21' id=29438 year='20/21' - missing 7 
name='Championship 19/20' id=23976 year='19/20'
name='Championship 18/19' id=17473 year='18/19'
name='Championship 17/18' id=13429 year='17/18'
name='Championship 16/17' id=11784 year='16/17'

"""


def get_seaons_ids(tournament_id: int):
    mw = ManagerWebdriver()
    tournamentscraper = SeasonsComponentScraper(
        tournamentid=tournament_id, webdriver=mw.spawn_webdriver()
    )
    results = tournamentscraper.process()
    assert results is not None, "Error getting the data."
    for season in results.seasons[:10]:
        print(season)


def run_two_scrapes(tournament_id: int, season_id: int):
    season_quaility_manager = SeasonQualityManager(
        tournament_id=tournament_id, season_id=season_id
    )
    season_quaility_manager.execute_scraping_run()
    time.sleep(60)
    season_quaility_manager.execute_scraping_run()

    consensus: SeasonConsensusResult = (
        season_quaility_manager.build_consensus_analysis()
    )
    print(consensus.season_summary)


def display_consensus(tournament_id: int, season_id: int):
    season_quaility_manager = SeasonQualityManager(
        tournament_id=tournament_id, season_id=season_id
    )

    consensus = season_quaility_manager.storage.load_most_current_consensus()
    print(consensus.season_summary)
    print()
    print(consensus.get_retry_dict())


# def rerun(tournament_id: int, season_id: int):
#    season_quaility_manager = SeasonQualityManager(
#        tournament_id=tournament_id, season_id=season_id
#    )
#
#    cons


if __name__ == "__main__":
    # get_seaons_ids(1)
    t_id = 1
    s_id = 52186

    display_consensus(tournament_id=t_id, season_id=s_id)

    # run_two_scrapes(tournament_id=1, season_id=52186)
