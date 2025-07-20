import logging
from typing import Dict, Optional

from omegaconf import DictConfig
from pydantic import ValidationError
from webdriver import MyWebDriver

import sofascrape.schemas.general as schemas
from sofascrape.abstract.base import BaseComponentScraper

logger = logging.getLogger(__name__)


class FootballLineupComponentScraper(BaseComponentScraper):
    """For football lineup of an event."""

    def __init__(
        self, webdriver: MyWebDriver, matchid: int, cfg: Optional[DictConfig] = None
    ) -> None:
        super().__init__(webdriver=webdriver, cfg=cfg)
        self.matchid: int = matchid
        self.page_url: str = self.cfg.links.football_lineup.format(match_id=matchid)

    def get_data(self):
        """Fetch lineup data from the API."""
        try:
            lineup_data = self.webdriver.get_page(self.page_url)

            if not isinstance(lineup_data, Dict):
                raise ValueError(f"Data is not a dict, {type(lineup_data)=}.")
            self.raw_data = lineup_data

        except Exception as e:
            logger.error(
                f"Failed to get lineup data, {self.matchid = }: with error {str(e)}."
            )

    def parse_data(self) -> None:
        """Parse raw lineup data into structured format using Pydantic.

        Raises:
            ValueError: If raw_data is None
            ValidationError: If data doesn't match expected schema
        """
        try:
            self.data = schemas.FootballLineupSchema.model_validate(self.raw_data)
            logger.info(f"Successfully parsed lineup data for event {self.matchid =}.")

        except ValidationError as e:
            logger.warning(
                f"Lineup validation failed for event, {self.matchid =}, {str(e)}."
            )
            raise
        except Exception as e:
            logger.error(
                f"Unexpected error parsing match lineup event {self.matchid=}, {str(e)}."
            )
            raise
