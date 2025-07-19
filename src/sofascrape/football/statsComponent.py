import logging
from typing import Dict

from pydantic import ValidationError
from webdriver import MyWebDriver

import sofascrape.schemas.general as schemas
from sofascrape.abstract.base import BaseComponentScraper

logger = logging.getLogger(__name__)


class FootballStatsComponentScraper(BaseComponentScraper):
    """For football statistics of an event."""

    def __init__(self, webdriver: MyWebDriver, matchid: int) -> None:
        super().__init__(webdriver=webdriver)
        self.matchid: int = matchid
        self.page_url: str = self.cfg.links.football_stats.format(match_id=matchid)

    def get_data(self):
        """Fetch statistics data from the API."""
        try:
            stats_data = self.webdriver.get_page(self.page_url)

            if not isinstance(stats_data, Dict):
                raise ValueError(f"Data is not a dict, {type(stats_data)=}.")
            self.raw_data = stats_data

        except Exception as e:
            logger.error(
                f"Failed to get stats data, {self.matchid = }: with error {str(e)}."
            )

    def parse_data(self) -> None:
        """Parse raw statistics data into structured format using Pydantic.

        Raises:
            ValueError: If raw_data is None
            ValidationError: If data doesn't match expected schema
        """
        try:
            self.data = schemas.FootballStatsSchema.model_validate(self.raw_data)
            logger.info(f"Successfully parsed stats data for event {self.matchid =}.")

        except ValidationError as e:
            logger.warning(
                f"Stats validation failed for event, {self.matchid =}, {str(e)}."
            )
            raise
        except Exception as e:
            logger.error(
                f"Unexpected error parsing match stats event {self.matchid=}, {str(e)}."
            )
            raise
