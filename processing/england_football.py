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


if __name__ == "__main__":
    get_seaons_ids(2)
