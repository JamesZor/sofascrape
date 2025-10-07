import logging
from typing import Dict, Optional

from omegaconf import DictConfig
from pydantic import ValidationError
from webdriver import (  # Make sure to import ManagerWebdriver for testing
    ManagerWebdriver,
    MyWebDriver,
)

from sofascrape.abstract.base import BaseComponentScraper
from sofascrape.schemas.odds import OddsSchema


class FootballOddsComponentScraper(BaseComponentScraper):
    """Scraper for football odds of an event."""

    def __init__(
        self, webdriver: MyWebDriver, matchid: int, cfg: Optional[DictConfig] = None
    ) -> None:
        super().__init__(webdriver=webdriver, cfg=cfg)
        self.matchid: int = matchid
        self.page_url: str = self.cfg.links.football_odds.format(match_id=matchid)

    def get_data(self):
        """Fetch odds data from the API."""
        try:
            odds_data = self.webdriver.get_page(self.page_url)

            if not isinstance(odds_data, Dict):
                raise ValueError(f"Data is not a dict, got {type(odds_data)=}.")
            self.raw_data = odds_data
        except Exception as e:
            logging.error(
                f"Failed to get odds data for match {self.matchid}: with error {str(e)}."
            )
            # Re-raise the exception to ensure the process fails correctly
            raise

    def parse_data(self) -> None:
        """Parse raw odds data into structured format using Pydantic."""
        if self.raw_data is None:
            raise ValueError("raw_data is not set, cannot parse.")

        try:
            self.data = OddsSchema.model_validate(self.raw_data)
            logging.info(f"Successfully parsed odds data for event {self.matchid}.")
        except ValidationError as e:
            logging.warning(
                f"Odds validation failed for event {self.matchid}: {str(e)}."
            )
            raise
        except Exception as e:
            logging.error(
                f"Unexpected error parsing match odds for event {self.matchid}: {str(e)}."
            )
            raise
