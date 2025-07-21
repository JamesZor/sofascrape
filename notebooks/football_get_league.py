import logging
from enum import Enum
from typing import List, Optional, Set

from webdriver import ManagerWebdriver

import sofascrape.schemas.general as schemas
from sofascrape.abstract.base import BaseLeagueScraper
from sofascrape.general import SeasonsComponentScraper

logger = logging.getLogger(__name__)


class FootballSeasonYears(Enum):
    """start year for that season"""

    S25 = "25/26"
    S24 = "24/25"
    S23 = "23/24"
    S22 = "22/23"
    S21 = "21/22"
    S20 = "20/21"
    S19 = "19/20"
    S18 = "18/19"
    S17 = "17/18"

    @classmethod
    def recent_season(cls, count: int = 3) -> Set[str]:
        all_season = [season.value for season in cls]
        return set(all_season[:count])

    @classmethod
    def window_season(cls, start_year: int, end_year: int) -> Set[str]:
        """start_year and end_year format XX, i.e 23 ( Not 2023)"""
        all_season = [
            season.value
            for season in cls
            if (
                (int(season.value.split("/")[0]) >= start_year)
                and (int(season.value.split("/")[0]) <= end_year)
            )
        ]
        return set(all_season)


class LeagueFootballScraper(BaseLeagueScraper):
    """Gets the list of seasonids and processes them"""

    def __init__(self, tournamentid: int):
        super().__init__(tournamentid=tournamentid)
        self.season_scraper: Optional[SeasonsComponentScraper] = None
        self.set_years_to_process()

    def set_years_to_process(self, start_year: int = 23, end_year: int = 24) -> None:
        self.years_to_process: Set[str] = FootballSeasonYears.window_season(
            start_year=start_year, end_year=end_year
        )

    def get_seasons(self) -> None:
        try:
            driver = self.mw.spawn_webdriver()
            self.season_scraper = SeasonsComponentScraper(
                tournamentid=self.tournamentid, webdriver=driver, cfg=self.cfg
            )
            self.season_scraper.process()  # data in .data.seasons : List[SeasonsSchemas]

        except Exception as e:
            logger.error(
                f"Failed to get seasons list for tournamentid:{self.tournamentid}: error - {str(e)}."
            )

            driver.close()

    def test_print(self) -> None:
        if self.season_scraper is not None:
            for season in self.season_scraper.data.seasons:
                if season.year in self.years_to_process:
                    print(season.model_dump_json(indent=2))

    def scrape(self):
        pass


if __name__ == "__main__":
    tournamentid = 1
    ls = LeagueFootballScraper(tournamentid=tournamentid)
    ls.get_seasons()
    ls.test_print()
