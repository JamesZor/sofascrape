# src/sofascrape/football/oddsComponent.py

import logging
from typing import Dict, Optional

from omegaconf import DictConfig
from pydantic import ValidationError
from webdriver import MyWebDriver

from sofascrape.abstract.base import BaseComponentScraper
from sofascrape.schemas import odds as SchemaOdds

logger = logging.getLogger(__name__)


class FootballOddsComponentScraper(BaseComponentScraper):
    """Scraper for football odds of an event."""

    def __init__(
        self, webdriver: MyWebDriver, matchid: int, cfg: Optional[DictConfig] = None
    ) -> None:
        super().__init__(webdriver=webdriver, cfg=cfg)
        self.matchid: int = matchid
        self.page_url: str = self.cfg.links.football_odds.format(match_id=self.matchid)

    def get_data(self):
        """Fetch odds data from the API, gracefully handling 404 errors."""
        try:
            odds_data = self.webdriver.get_page(self.page_url)

            if isinstance(odds_data, dict) and "error" in odds_data:
                if odds_data.get("error", {}).get("code") == 404:
                    logger.warning(
                        f"No odds data found (404) for match {self.matchid}."
                    )
                    self.raw_data = None  # Explicitly set raw_data to None
                    return  # Exit the function gracefully
                else:
                    # Handle other potential errors if needed
                    error_msg = odds_data.get("error", {}).get(
                        "message", "Unknown API error"
                    )
                    raise ValueError(error_msg)

            if not isinstance(odds_data, Dict):
                raise ValueError(f"Data is not a dict, got {type(odds_data)=}.")

            self.raw_data = odds_data

        except Exception as e:
            logger.error(
                f"Failed to get odds data for match {self.matchid}: with error {str(e)}."
            )
            self.raw_data = None

    def parse_data(self) -> None:
        """Parse raw odds data into structured format using Pydantic."""
        # This method will now only be called if raw_data is not None
        if self.raw_data is None:
            return

        try:
            self.data = SchemaOdds.OddsSchema.model_validate(self.raw_data)
            logger.info(f"Successfully parsed odds data for event {self.matchid}.")
        except ValidationError as e:
            logger.warning(
                f"Odds validation failed for event {self.matchid}: {str(e)}."
            )
            raise
